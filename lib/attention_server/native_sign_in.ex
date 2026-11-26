defmodule AttentionServer.NativeSignIn do
  @moduledoc false

  alias AttentionServer.BrowserAuth
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
    authorized_until_ms INTEGER
  );
  CREATE INDEX harklight_native_transactions_expiry
    ON harklight_native_transactions (expires_ms);
  """

  def create_tables(db), do: Sqlite3.execute(db, @schema)

  def begin(db, verifier_hash, now) do
    if lowercase_hash?(verifier_hash) do
      reference = BrowserAuth.new_secret()

      with :ok <-
             RecordTable.run(
               db,
               """
               DELETE FROM harklight_native_transactions
               WHERE expires_ms <= ? AND COALESCE(authorized_until_ms, 0) <= ?
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

  defp lowercase_hash?(value) when is_binary(value) and byte_size(value) == 64,
    do: match?({:ok, _}, Base.decode16(value, case: :lower))

  defp lowercase_hash?(_), do: false
end
