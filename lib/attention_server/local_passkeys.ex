defmodule AttentionServer.LocalPasskeys do
  @moduledoc false

  alias AttentionServer.{BrowserAuth, DeviceProof, Engine, NativeSignIn}
  alias AttentionServer.Store.RecordTable
  alias Exqlite.Sqlite3

  @challenge_seconds 300

  @schema """
  CREATE TABLE harklight_passkeys (
    credential_id BLOB PRIMARY KEY NOT NULL,
    person_id TEXT NOT NULL,
    owner_key TEXT NOT NULL,
    owner_revision INTEGER NOT NULL,
    public_key BLOB NOT NULL
  );
  CREATE TABLE harklight_passkey_challenges (
    reference TEXT PRIMARY KEY NOT NULL,
    purpose TEXT NOT NULL CHECK (purpose IN ('register', 'assert')),
    browser_hash BLOB,
    native_reference TEXT,
    challenge BLOB NOT NULL,
    expires_ms INTEGER NOT NULL
  );
  """

  def create_tables(db), do: Sqlite3.execute(db, @schema)

  def registration_options(db, origin, browser_hash, person, now) do
    with {:ok, host} <- rp_host(origin) do
      challenge =
        Wax.new_registration_challenge(
          origin: origin,
          rp_id: host,
          attestation: "none",
          trusted_attestation_types: [:none],
          timeout: @challenge_seconds,
          user_verification: "required"
        )

      with {:ok, reference} <- save_challenge(db, "register", browser_hash, nil, challenge, now) do
        {:ok,
         %{
           "reference" => reference,
           "publicKey" => %{
             "challenge" => encode(challenge.bytes),
             "rp" => %{"id" => host, "name" => "Harklight"},
             "user" => %{"id" => encode(:crypto.hash(:sha256, person)), "name" => "Harklight", "displayName" => "Harklight"},
             "pubKeyCredParams" => [%{"type" => "public-key", "alg" => -7}, %{"type" => "public-key", "alg" => -257}],
             "authenticatorSelection" => %{"residentKey" => "required", "userVerification" => "required"},
             "attestation" => "none",
             "timeout" => @challenge_seconds * 1000
           }
         }}
      end
    end
  end

  def assertion_options(db, origin, transaction, now) do
    with {:ok, host} <- rp_host(origin),
         :ok <- valid_native_reference(db, transaction, now) do
      challenge =
        Wax.new_authentication_challenge(
          origin: origin,
          rp_id: host,
          timeout: @challenge_seconds,
          user_verification: "required"
        )

      with {:ok, reference} <- save_challenge(db, "assert", nil, transaction, challenge, now) do
        {:ok,
         %{
           "reference" => reference,
           "publicKey" => %{
             "challenge" => encode(challenge.bytes),
             "rpId" => host,
             "userVerification" => "required",
             "timeout" => @challenge_seconds * 1000
           }
         }}
      end
    end
  end

  def register(db, browser_hash, principal, request, now) do
    with %{"reference" => reference, "attestation" => attestation, "client_data" => client} <- request,
         {:ok, attestation} <- decode(attestation),
         {:ok, client} <- decode(client),
         {:ok, challenge, nil} <- challenge(db, reference, "register", browser_hash, now),
         {:ok, {data, _result}} <- Wax.register(attestation, client, challenge) do
      id = data.attested_credential_data.credential_id
      cose = data.attested_credential_data.credential_public_key

      consume_challenge(db, reference, fn ->
        with :ok <-
               RecordTable.run(
                 db,
                 "INSERT INTO harklight_passkeys VALUES (?, ?, ?, ?, ?)",
                 [{:blob, id}, principal.person, principal.key, principal.revision,
                  {:blob, :erlang.term_to_binary(cose)}]
               ) do
          {:ok, %{"registered" => true}}
        end
      end)
    else
      _ -> {:error, :invalid_passkey}
    end
  end

  def authenticate(db, engine, request, now, native_allowed?) do
    with %{"reference" => reference, "credential_id" => id, "authenticator_data" => data,
           "signature" => signature, "client_data" => client} <- request,
         {:ok, id} <- decode(id),
         {:ok, data} <- decode(data),
         {:ok, signature} <- decode(signature),
         {:ok, client} <- decode(client),
         {:ok, challenge, native_reference} <- challenge(db, reference, "assert", nil, now),
         true <- is_nil(native_reference) or native_allowed?,
         {:ok, [[person, owner, revision, encoded_key]]} <-
           RecordTable.select(
             db,
             "SELECT person_id, owner_key, owner_revision, public_key FROM harklight_passkeys WHERE credential_id = ?",
             [{:blob, id}]
           ),
         {:ok, ^revision} <- Engine.owner_revision(engine, person, owner),
         key = :erlang.binary_to_term(encoded_key, [:safe]),
         {:ok, _authenticator} <- Wax.authenticate(id, data, signature, client, challenge, [{id, key}]) do
      consume_challenge(db, reference, fn ->
        finish_assertion(db, native_reference, person, owner, revision, now)
      end)
    else
      _ -> {:error, :invalid_passkey}
    end
  end

  defp valid_native_reference(_db, nil, _now), do: :ok

  defp valid_native_reference(db, transaction, now) do
    if DeviceProof.secret?(transaction) and NativeSignIn.pending_transaction?(db, transaction, now),
      do: :ok,
      else: {:error, :invalid_passkey}
  end

  defp finish_assertion(db, nil, person, owner, revision, _now) do
    cookie = BrowserAuth.new_secret()

    with :ok <- BrowserAuth.add_session(db, BrowserAuth.hash(cookie), person, owner, revision, "self_hosted"),
         do: {:ok, %{cookie: cookie, method: "passkey"}}
  end

  defp finish_assertion(db, transaction, person, _owner, _revision, now) do
    case NativeSignIn.authorize_local_passkey(db, transaction, person, now) do
      {:ok, reference} -> {:ok, %{transaction: reference}}
      _ -> {:error, :invalid_passkey}
    end
  end

  defp rp_host(origin) when is_binary(origin) do
    case URI.parse(origin) do
      %URI{scheme: "https", host: host} when is_binary(host) -> {:ok, host}
      _ -> {:error, :passkeys_unavailable}
    end
  end

  defp rp_host(_), do: {:error, :passkeys_unavailable}

  defp save_challenge(db, purpose, browser_hash, native_reference, challenge, now) do
    reference = BrowserAuth.new_secret()
    browser = if browser_hash, do: {:blob, browser_hash}

    with :ok <- RecordTable.run(db, "DELETE FROM harklight_passkey_challenges WHERE expires_ms <= ?", [now]),
         :ok <-
           RecordTable.run(
             db,
             "INSERT INTO harklight_passkey_challenges VALUES (?, ?, ?, ?, ?, ?)",
             [reference, purpose, browser, native_reference, {:blob, :erlang.term_to_binary(challenge)},
              now + @challenge_seconds * 1000]
           ) do
      {:ok, reference}
    end
  end

  defp challenge(db, reference, purpose, hash, now) do
    browser = if hash, do: {:blob, hash}

    with true <- DeviceProof.secret?(reference),
         {:ok, [[encoded, native_reference]]} <-
           RecordTable.select(
             db,
             """
             SELECT challenge, native_reference FROM harklight_passkey_challenges
             WHERE reference = ? AND purpose = ? AND browser_hash IS ? AND expires_ms > ?
             """,
             [reference, purpose, browser, now]
           ) do
      {:ok, :erlang.binary_to_term(encoded, [:safe]), native_reference}
    else
      _ -> {:error, :invalid_passkey}
    end
  end

  defp consume_challenge(db, reference, finish) do
    RecordTable.transaction(db, fn ->
      with :ok <- RecordTable.run(db, "DELETE FROM harklight_passkey_challenges WHERE reference = ?", [reference]),
           {:ok, 1} <- Sqlite3.changes(db) do
        finish.()
      else
        _ -> {:error, :invalid_passkey}
      end
    end)
  end

  defp encode(bytes), do: Base.url_encode64(bytes, padding: false)
  defp decode(bytes) when is_binary(bytes), do: Base.url_decode64(bytes, padding: false)
  defp decode(_), do: :error
end
