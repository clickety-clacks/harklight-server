defmodule AttentionServer.NativeSignIn do
  @moduledoc false

  alias AttentionServer.{BrowserAuth, DeviceMigration, DeviceProof, Engine, JSON}
  alias AttentionServer.Store.RecordTable
  alias Exqlite.Sqlite3

  @window_ms 10 * 60 * 1_000
  @hosted_methods ~w(google apple email_code passkey)

  @schema """
  CREATE TABLE harklight_native_transactions (
    transaction_id TEXT PRIMARY KEY NOT NULL,
    verifier_hash TEXT NOT NULL,
    expires_ms INTEGER NOT NULL,
    person_id TEXT,
    method TEXT,
    authorized_until_ms INTEGER,
    redeemed_device TEXT,
    redeemed_session TEXT,
    redeemed_key TEXT,
    redeemed_label TEXT,
    redeemed_old_proof TEXT,
    old_device_result TEXT
  );
  CREATE INDEX harklight_native_transactions_expiry
    ON harklight_native_transactions (expires_ms);
  CREATE TABLE harklight_device_sessions (
    session_hash TEXT PRIMARY KEY NOT NULL,
    device_id TEXT NOT NULL UNIQUE,
    person_id TEXT NOT NULL,
    public_key TEXT NOT NULL,
    signed_in_at_ms INTEGER NOT NULL,
    pending INTEGER NOT NULL CHECK (pending IN (0, 1))
  );
  """

  def create_tables(db), do: Sqlite3.execute(db, @schema)

  def begin(db, verifier_hash, now) do
    if DeviceProof.hash?(verifier_hash) do
      reference = BrowserAuth.new_secret()

      with :ok <-
             RecordTable.run(
               db,
               """
               DELETE FROM harklight_native_transactions
               WHERE redeemed_device IS NULL AND expires_ms <= ? AND COALESCE(authorized_until_ms, 0) <= ?
               """,
               [now, now]
             ),
           :ok <-
             RecordTable.run(
               db,
               """
               INSERT INTO harklight_native_transactions (transaction_id, verifier_hash, expires_ms)
               VALUES (?, ?, ?)
               """,
               [reference, verifier_hash, now + @window_ms]
             ) do
        {:ok, reference}
      end
    else
      {:error, :invalid_verifier_hash}
    end
  end

  def authorize_hosted(db, ref, person, method, now) when method in @hosted_methods,
    do: authorize(db, ref, person, method, now)

  def authorize_hosted(_, _, _, _, _), do: {:error, :invalid_request}

  defp authorize(db, ref, person, method, now) when is_binary(ref) and byte_size(ref) == 43 do
    with :ok <-
           RecordTable.run(
             db,
             """
             UPDATE harklight_native_transactions
             SET person_id = ?, method = ?, authorized_until_ms = ?
             WHERE transaction_id = ? AND expires_ms > ? AND person_id IS NULL
             """,
             [person, method, now + @window_ms, ref, now]
           ) do
      case Sqlite3.changes(db) do
        {:ok, 1} -> {:ok, ref}
        {:ok, 0} -> {:error, :invalid_sign_in}
        error -> error
      end
    end
  end

  defp authorize(_, _, _, _, _), do: {:error, :invalid_request}

  def proof(request, reference_field) do
    with %{^reference_field => reference, "public_key" => public_key, "label" => label,
           "session_hash" => session_hash, "signature" => signature} <- request,
         true <- DeviceProof.secret?(reference) and DeviceProof.hash?(session_hash),
         true <- is_binary(label) and String.valid?(label) and byte_size(label) in 1..160,
         {:ok, key} <- DeviceProof.native_key(public_key) do
      if DeviceProof.verify(key, DeviceProof.sign_in_bytes(reference, session_hash), signature) do
        {:ok, %{reference: reference, key: public_key, label: label, hash: session_hash}}
      else
        {:error, :unauthorized}
      end
    else
      _ -> {:error, :invalid_request}
    end
  end

  def prepare_redemption(db, engine, %{"verifier" => verifier} = request, now, mode) do
    with true <- DeviceProof.secret?(verifier),
         {:ok, proof} <- proof(request, "transaction"),
         {:ok, old_proof} <- old_proof(request, mode),
         {:ok, [[person, method, expires, device, redeemed_hash, redeemed_key, redeemed_label,
                 redeemed_old_proof, old_device_result]]} <-
           RecordTable.select(
             db,
             """
             SELECT person_id, method, authorized_until_ms, redeemed_device, redeemed_session,
                    redeemed_key, redeemed_label, redeemed_old_proof, old_device_result
             FROM harklight_native_transactions
             WHERE transaction_id = ? AND verifier_hash = ? AND person_id IS NOT NULL
             """,
             [proof.reference, DeviceProof.digest(verifier)]
           ) do
      proof = Map.put(proof, :old_digest, old_proof && DeviceProof.digest(JSON.canonical(old_proof)))

      cond do
        device ->
          duplicate(db, person, method, device, proof,
            {redeemed_hash, redeemed_key, redeemed_label, redeemed_old_proof}, old_device_result)

        expires > now ->
          prepare_device(db, engine, person, method, proof, old_proof, now)

        true ->
          {:error, :unauthorized}
      end
    else
      false -> {:error, :invalid_request}
      {:ok, []} -> {:error, :unauthorized}
      error -> error
    end
  end

  def prepare_redemption(_, _, _, _, _), do: {:error, :invalid_request}

  defp old_proof(%{"old_proof" => %{"device_id" => id, "public_key" => key, "signature" => signature} = proof}, :self_hosted)
       when is_binary(id) and is_binary(key) and is_binary(signature) and map_size(proof) == 3 do
    if DeviceProof.hash?(id), do: {:ok, proof}, else: {:error, :invalid_request}
  end

  defp old_proof(%{"old_proof" => _}, _), do: {:error, :invalid_request}
  defp old_proof(_, _), do: {:ok, nil}

  defp duplicate(db, person, method, device, %{hash: hash, key: key, label: label, old_digest: digest},
         {hash, key, label, digest}, old_device) do
    case lookup_session(db, hash) do
      {:ok, %{device_id: ^device, person_id: ^person}} ->
        {:ok, with_old_device(result(person, device, method, true), old_device)}

      _ ->
        {:error, :unauthorized}
    end
  end

  defp duplicate(_, _, _, _, _, _, _), do: {:error, :unauthorized}

  defp prepare_device(db, engine, person, method, proof, old_proof, now) do
    old_device = old_device_status(engine, person, method, proof, old_proof)

    with {:ok, next, device, status} <- recipient(engine, person, proof.label, old_device),
         {:ok, previous} <- device_session_hash(db, device) do
      redemption = Map.merge(proof, %{person: person, device: device, old_device: status,
                                      previous: previous, now: now})
      response = with_old_device(result(person, device, method, false), status)
      {:ok, next, redemption, response}
    end
  end

  defp old_device_status(_engine, _person, _method, _proof, nil), do: nil

  defp old_device_status(engine, person, method, proof,
         %{"device_id" => id, "public_key" => public_key, "signature" => signature})
       when method in ["owner_code", "passkey"] do
    with {:ok, key} <- DeviceProof.legacy_key(public_key, id),
         true <- DeviceProof.verify(key, DeviceProof.sign_in_bytes(proof.reference, proof.hash), signature),
         %{person: ^person, revoked: revoked} <- engine.devices[id] do
      if revoked, do: "removed", else: {:attached, id}
    else
      _ -> "unknown"
    end
  end

  defp old_device_status(_, _, _, _, _), do: "unknown"

  defp recipient(engine, _person, _label, {:attached, device}),
    do: {:ok, engine, device, "attached"}

  defp recipient(engine, person, label, status) do
    device = :crypto.strong_rand_bytes(32) |> Base.encode16(case: :lower)

    with {:ok, next} <- Engine.setup_device(engine, person, device) do
      next = Engine.put_row(next, :devices, device, %{next.devices[device] | label: label})
      {:ok, next, device, status}
    end
  end

  defp with_old_device(result, nil), do: result
  defp with_old_device(result, status), do: Map.put(result, "old_device", status)

  def result(person, device, method, duplicate),
    do: %{"person_id" => person, "device_id" => device, "method" => method, "duplicate" => duplicate}

  def commit_redemption(db, redemption) do
    %{reference: transaction, person: person, device: device, hash: hash,
      key: key, label: label, now: now, old_digest: old_digest, old_device: old_device} = redemption

    with :ok <-
           RecordTable.run(
             db,
             """
             UPDATE harklight_native_transactions
             SET redeemed_device = ?, redeemed_session = ?, redeemed_key = ?, redeemed_label = ?,
                 redeemed_old_proof = ?, old_device_result = ?
             WHERE transaction_id = ? AND redeemed_device IS NULL
             """,
             [device, hash, key, label, old_digest, old_device, transaction]
           ),
         {:ok, 1} <- Sqlite3.changes(db),
         :ok <- replace_recovered_session(db, person, device, old_device) do
      add_session(db, hash, device, person, key, now, false)
    else
      {:ok, 0} -> {:error, :unauthorized}
      error -> error
    end
  end

  defp replace_recovered_session(db, person, device, "attached") do
    with :ok <- remove_device_sessions(db, person, device),
         do: DeviceMigration.set_phase(db, device, "activated")
  end

  defp replace_recovered_session(_, _, _, _), do: :ok

  def add_session(db, hash, device, person, key, now, pending?) do
    RecordTable.run(
      db,
      """
      INSERT INTO harklight_device_sessions
        (session_hash, device_id, person_id, public_key, signed_in_at_ms, pending)
      VALUES (?, ?, ?, ?, ?, ?)
      """,
      [hash, device, person, key, now, if(pending?, do: 1, else: 0)]
    )
  end

  def device_session_hash(db, device) do
    case RecordTable.select(
           db,
           "SELECT session_hash FROM harklight_device_sessions WHERE device_id = ?",
           [device]
         ) do
      {:ok, [[hash]]} -> {:ok, hash}
      {:ok, []} -> {:ok, nil}
      error -> error
    end
  end

  def signed_in_devices(db, person) do
    with {:ok, rows} <-
           RecordTable.select(
             db,
             "SELECT device_id, signed_in_at_ms FROM harklight_device_sessions WHERE person_id = ?",
             [person]
           ) do
      {:ok, Map.new(rows, fn [id, time] -> {id, time} end)}
    end
  end

  def remove_device_sessions(db, person, device) do
    RecordTable.run(
      db,
      "DELETE FROM harklight_device_sessions WHERE person_id = ? AND device_id = ?",
      [person, device]
    )
  end

  def lookup_session(db, hash) do
    with {:ok, rows} <-
           RecordTable.select(
             db,
             """
             SELECT device_id, person_id, public_key, pending FROM harklight_device_sessions
             WHERE session_hash = ?
             """,
             [hash]
           ) do
      case rows do
        [[device, person, key, pending]] ->
          {:ok, %{device_id: device, person_id: person, public_key: key, pending: pending == 1}}

        [] ->
          {:error, :unauthorized}
      end
    end
  end
end
