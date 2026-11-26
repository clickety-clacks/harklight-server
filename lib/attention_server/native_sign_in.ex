defmodule AttentionServer.NativeSignIn do
  @moduledoc false

  alias AttentionServer.{BrowserAuth, DeviceProof, Engine}
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
    redeemed_label TEXT
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

  def prepare_redemption(db, engine, %{"verifier" => verifier} = request, now) do
    with true <- DeviceProof.secret?(verifier),
         {:ok, proof} <- proof(request, "transaction"),
         {:ok, [[person, method, expires, device, redeemed_hash, redeemed_key, redeemed_label]]} <-
           RecordTable.select(
             db,
             """
             SELECT person_id, method, authorized_until_ms, redeemed_device, redeemed_session,
                    redeemed_key, redeemed_label
             FROM harklight_native_transactions
             WHERE transaction_id = ? AND verifier_hash = ? AND person_id IS NOT NULL
             """,
             [proof.reference, DeviceProof.digest(verifier)]
           ) do
      if device do
        duplicate(db, person, method, device, proof.hash, proof.key, proof.label,
          {redeemed_hash, redeemed_key, redeemed_label})
      else
        if expires > now,
          do: prepare_device(engine, proof.reference, person, method, proof.hash, proof.key, proof.label, now),
          else: {:error, :unauthorized}
      end
    else
      false -> {:error, :invalid_request}
      {:ok, []} -> {:error, :unauthorized}
      error -> error
    end
  end

  def prepare_redemption(_, _, _, _), do: {:error, :invalid_request}

  defp duplicate(db, person, method, device, hash, key, label, {hash, key, label}) do
    case lookup_session(db, hash) do
      {:ok, %{device_id: ^device, person_id: ^person}} ->
        {:ok, result(person, device, method, true)}

      _ ->
        {:error, :unauthorized}
    end
  end

  defp duplicate(_, _, _, _, _, _, _, _), do: {:error, :unauthorized}

  defp prepare_device(engine, transaction, person, method, hash, key, label, now) do
    device = :crypto.strong_rand_bytes(32) |> Base.encode16(case: :lower)

    with {:ok, next} <- Engine.setup_device(engine, person, device) do
      next = Engine.put_row(next, :devices, device, %{next.devices[device] | label: label})
      redemption = %{reference: transaction, person: person, device: device, hash: hash,
                     key: key, label: label, now: now}
      {:ok, next, redemption, result(person, device, method, false)}
    end
  end

  def result(person, device, method, duplicate),
    do: %{"person_id" => person, "device_id" => device, "method" => method, "duplicate" => duplicate}

  def commit_redemption(db, redemption) do
    %{reference: transaction, person: person, device: device, hash: hash,
      key: key, label: label, now: now} = redemption

    with :ok <-
           RecordTable.run(
             db,
             """
             UPDATE harklight_native_transactions
             SET redeemed_device = ?, redeemed_session = ?, redeemed_key = ?, redeemed_label = ?
             WHERE transaction_id = ? AND redeemed_device IS NULL
             """,
             [device, hash, key, label, transaction]
           ),
         {:ok, 1} <- Sqlite3.changes(db) do
      add_session(db, hash, device, person, key, now, false)
    else
      {:ok, 0} -> {:error, :unauthorized}
      error -> error
    end
  end

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
