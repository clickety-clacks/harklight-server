defmodule AttentionServer.LocalSignIn do
  @moduledoc false

  alias AttentionServer.{BrowserAuth, DeviceProof, Engine, NativeSignIn}
  alias AttentionServer.Store.RecordTable
  alias Exqlite.Sqlite3

  @code_window_ms 10 * 60 * 1_000

  @schema """
  CREATE TABLE harklight_owner_codes (
    code_hash TEXT PRIMARY KEY NOT NULL,
    person_id TEXT NOT NULL,
    owner_key TEXT NOT NULL,
    owner_revision INTEGER NOT NULL,
    expires_ms INTEGER NOT NULL
  );
  """

  def create_tables(db), do: Sqlite3.execute(db, @schema)

  def issue(db, engine, person, owner, now) do
    with {:ok, revision} <- Engine.owner_revision(engine, person, owner) do
      code = :crypto.strong_rand_bytes(7) |> Base.encode32(padding: false) |> binary_part(0, 10)
      expires = now + @code_window_ms

      RecordTable.transaction(db, fn ->
        with :ok <- RecordTable.run(db, "DELETE FROM harklight_owner_codes WHERE expires_ms <= ?", [now]),
             :ok <-
               RecordTable.run(
                 db,
                 "INSERT INTO harklight_owner_codes VALUES (?, ?, ?, ?, ?)",
                 [DeviceProof.digest(code), person, owner, revision, expires]
               ) do
          {:ok, %{"code" => code, "expires_at" => expires}}
        end
      end)
    end
  end

  def redeem(db, engine, code, transaction, now) when is_binary(code) and byte_size(code) <= 32 do
    normalized = code |> String.trim() |> String.upcase()
    hash = DeviceProof.digest(normalized)

    RecordTable.transaction(db, fn ->
      with {:ok, [[person, owner, revision]]} <-
             RecordTable.select(
               db,
               """
               SELECT person_id, owner_key, owner_revision FROM harklight_owner_codes
               WHERE code_hash = ? AND expires_ms > ?
               """,
               [hash, now]
             ),
           {:ok, ^revision} <- Engine.owner_revision(engine, person, owner),
           {:ok, result} <- grant(db, transaction, person, owner, revision, now),
           :ok <- RecordTable.run(db, "DELETE FROM harklight_owner_codes WHERE code_hash = ?", [hash]) do
        {:ok, result}
      else
        {:ok, _} -> {:error, :invalid_sign_in}
        {:error, :forbidden} -> {:error, :invalid_sign_in}
        error -> error
      end
    end)
  end

  def redeem(_, _, _, _, _), do: {:error, :invalid_sign_in}

  defp grant(db, nil, person, owner, revision, _now) do
    cookie = BrowserAuth.new_secret()

    with :ok <- BrowserAuth.add_session(db, BrowserAuth.hash(cookie), person, owner, revision, "self_hosted"),
         do: {:ok, %{cookie: cookie, method: "owner_code"}}
  end

  defp grant(db, transaction, person, _owner, _revision, now) do
    with {:ok, ref} <- NativeSignIn.authorize_local_code(db, transaction, person, now),
         do: {:ok, %{transaction: ref}}
  end
end
