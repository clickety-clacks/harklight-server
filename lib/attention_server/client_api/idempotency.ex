defmodule AttentionServer.ClientAPI.Idempotency do
  @moduledoc """
  The client API v2 `Idempotency-Key` store (docs/client-api-v2.md §2 and §5).

  A committed write is stored as `{person, key} → (digest, status, receipt)`.
  The scope is the **person**, not the client: a device and a browser of the
  same person share one key space, as the spec asks.

  - Only successes are stored, as today's engine keeps operation receipts
    only on success: a refused request retried with the same key is evaluated
    again.
  - The receipt is compact: what the write changed (an item, its new entry,
    settings, or a staged file's FileCreated). A repeat with the same digest
    renders those resources from current state, answers with the stored
    status and `Idempotent-Replayed: true`, and applies nothing.
  - A repeat with a different digest is `409 idempotency_conflict`.
  - The same key sent at the same moment by two different clients of one
    person may apply twice: begin and finish are separate relay steps, and
    the engine's own receipts deduplicate per client. A key is minted on one
    client, so this is not a production pattern (accepted, review C4).

  Receipts are kept for at least the change log's 30-day retention window, and
  older ones are pruned as new ones are written. The table lives in the
  store's SQLite database beside the change log and exists only while
  `client_api_v2` is on.

  Writes also pass the key to the engine as today's operation id, so a retry
  from the same client is deduplicated by the engine's own receipts inside the
  commit, even when a crash comes between the commit and the receipt here.

  Call these from the Relay process, so they are serialized with commits.
  """

  alias AttentionServer.{JSON, Message}
  alias Exqlite.Sqlite3

  @retention_ms 30 * 24 * 60 * 60 * 1000

  @schema [
    """
    CREATE TABLE IF NOT EXISTS client_idempotency (
      person_id TEXT NOT NULL,
      idempotency_key TEXT NOT NULL,
      digest TEXT NOT NULL,
      status INTEGER NOT NULL,
      body BLOB NOT NULL,
      at_ms INTEGER NOT NULL,
      PRIMARY KEY (person_id, idempotency_key)
    )
    """,
    "CREATE INDEX IF NOT EXISTS client_idempotency_at ON client_idempotency (at_ms)"
  ]

  @type receipt :: %{digest: String.t(), status: pos_integer(), body: map()}

  @doc "How long receipts are kept, in milliseconds."
  def retention_ms, do: @retention_ms

  @doc "Whether `key` is an acceptable `Idempotency-Key`: a UUIDv4."
  @spec valid_key?(term()) :: boolean()
  def valid_key?(key), do: Message.uuid_v4?(key)

  @doc """
  The digest of a request: its method, its route (with the item it names) and
  its body, canonically encoded. Two requests with the same digest are the
  same request.
  """
  @spec digest(String.t(), [String.t()], map()) :: String.t()
  def digest(method, path, body) when is_binary(method) and is_list(path) and is_map(body) do
    canonical = JSON.canonical(%{"method" => method, "path" => path, "body" => body})
    Base.encode16(:crypto.hash(:sha256, canonical), case: :lower)
  end

  @doc "The stored receipt for `{person, key}`, if any."
  @spec lookup(struct(), String.t(), String.t()) ::
          {:ok, receipt() | nil} | {:error, :disabled | term()}
  def lookup(%AttentionServer.Store{change_log: true, db: db}, person, key)
      when is_binary(person) and is_binary(key) do
    with :ok <- ensure(db),
         {:ok, row} <-
           one(
             db,
             "SELECT digest, status, body FROM client_idempotency " <>
               "WHERE person_id = ? AND idempotency_key = ? AND at_ms >= ?",
             [person, key, now() - @retention_ms]
           ) do
      case row do
        nil ->
          {:ok, nil}

        [digest, status, body] ->
          case JSON.decode_line(body <> "\n") do
            {:ok, decoded} when is_map(decoded) ->
              {:ok, %{digest: digest, status: status, body: decoded}}

            _ ->
              {:error, :corrupt_receipt}
          end
      end
    end
  end

  def lookup(%AttentionServer.Store{}, _person, _key), do: {:error, :disabled}

  @doc """
  Stores the outcome of `{person, key}`. A receipt that already exists is kept:
  the first outcome is the one every repeat gets. Receipts past retention are
  pruned in the same step.
  """
  @spec save(struct(), String.t(), String.t(), String.t(), pos_integer(), map()) ::
          :ok | {:error, term()}
  def save(%AttentionServer.Store{change_log: true, db: db}, person, key, digest, status, body)
      when is_binary(person) and is_binary(key) and is_binary(digest) and is_integer(status) and
             is_map(body) do
    at = now()

    with :ok <- ensure(db),
         :ok <-
           run(db, "DELETE FROM client_idempotency WHERE at_ms < ?", [at - @retention_ms]),
         :ok <-
           run(
             db,
             "INSERT INTO client_idempotency (person_id, idempotency_key, digest, status, body, at_ms) " <>
               "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (person_id, idempotency_key) DO NOTHING",
             [person, key, digest, status, JSON.encode!(body), at]
           ) do
      :ok
    end
  end

  def save(%AttentionServer.Store{}, _person, _key, _digest, _status, _body),
    do: {:error, :disabled}

  defp now, do: System.system_time(:millisecond)

  defp ensure(db) do
    Enum.reduce_while(@schema, :ok, fn sql, :ok ->
      case Sqlite3.execute(db, sql) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp one(db, sql, params) do
    statement(db, sql, params, fn stmt ->
      case Sqlite3.step(db, stmt) do
        {:row, row} -> {:ok, row}
        :done -> {:ok, nil}
        error -> error
      end
    end)
  end

  defp run(db, sql, params) do
    case statement(db, sql, params, &Sqlite3.step(db, &1)) do
      :done -> :ok
      error -> error
    end
  end

  defp statement(db, sql, params, fun) do
    with {:ok, stmt} <- Sqlite3.prepare(db, sql) do
      result =
        case Sqlite3.bind(stmt, params) do
          :ok -> fun.(stmt)
          error -> error
        end

      _ = Sqlite3.release(db, stmt)
      result
    end
  end
end
