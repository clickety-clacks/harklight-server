defmodule AttentionServer.Store.RecordTable do
  @moduledoc false

  alias AttentionServer.Store.RecordCodec
  alias Exqlite.Sqlite3

  @schema """
  CREATE TABLE relay_record_generation (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    generation INTEGER NOT NULL
  );
  CREATE TABLE relay_records (
    key BLOB PRIMARY KEY NOT NULL,
    payload BLOB NOT NULL,
    checksum BLOB NOT NULL,
    account INTEGER NOT NULL DEFAULT 0
  ) WITHOUT ROWID;
  CREATE INDEX relay_records_account ON relay_records (key) WHERE account = 1;
  """

  # Called once, inside the format conversion transaction.
  def create(db, generation) do
    with :ok <- Sqlite3.execute(db, @schema) do
      run(db, "INSERT INTO relay_record_generation VALUES (1, ?)", [generation])
    end
  end

  def generation(db) do
    case select(db, "SELECT generation FROM relay_record_generation WHERE id = 1") do
      {:ok, [[generation]]} -> {:ok, generation}
      {:ok, _} -> {:error, :invalid_state}
      error -> error
    end
  end

  def rows(db) do
    with {:ok, rows} <- select(db, "SELECT key, payload, checksum FROM relay_records") do
      {:ok, Enum.map(rows, fn [key, payload, hash] -> {key, payload, hash} end)}
    end
  end

  # The consumer runs before the statement is released. It can stop early
  # without retaining the remaining records or leaking the SQLite cursor.
  def scan(db, consume) do
    statement(db, "SELECT key, payload, checksum FROM relay_records", [], fn stmt ->
      consume.(Stream.unfold(stmt, &next_row(db, &1, fn [key, payload, hash] -> {key, payload, hash} end)))
    end)
  end

  def scan_keys(db, consume) do
    statement(db, "SELECT key FROM relay_records", [], fn stmt ->
      consume.(Stream.unfold(stmt, &next_row(db, &1, fn [key] -> key end)))
    end)
  end

  defp next_row(_db, :done, _convert), do: nil

  defp next_row(db, stmt, convert) do
    case Sqlite3.step(db, stmt) do
      {:row, values} -> {convert.(values), stmt}
      :done -> nil
      error -> {error, :done}
    end
  end

  # append receives this connection and the new generation and performs SQL
  # only. It must return {:ok, metadata} or {:error, reason}. Callers publish
  # the Engine and notify subscribers after commit returns success.
  def commit(db, expected_generation, records, append) do
    transaction(db, fn -> write_generation(db, expected_generation, records, append) end)
  end

  def seed(db, generation, rows) do
    with {:ok, _count} <- seed_count(db, generation, rows), do: :ok
  end

  def seed_count(db, generation, rows) do
    with :ok <- create(db, generation) do
      write_records(db, Stream.map(rows, &{:insert, &1}))
    end
  end

  # Only a tagged success commits. A bare SQL success does not publish the
  # caller's result.
  def transaction(db, write) do
    with :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE") do
      try do
        case finish(db, write.()) do
          {:ok, _} = result ->
            result

          error ->
            _ = Sqlite3.execute(db, "ROLLBACK")
            error
        end
      catch
        kind, reason ->
          _ = Sqlite3.execute(db, "ROLLBACK")
          :erlang.raise(kind, reason, __STACKTRACE__)
      end
    end
  end

  # Composes records with a caller's transaction when its preconditions must
  # be checked before any row is changed.
  def write_generation(db, expected_generation, records, append) do
    with :ok <-
           run(
             db,
             "UPDATE relay_record_generation SET generation = generation + 1 WHERE id = 1 AND generation = ?",
             [expected_generation]
           ),
         :ok <- require_changed_generation(db),
         {:ok, _count} <- write_records(db, records),
         {:ok, metadata} <- append_rows(append, db, expected_generation + 1) do
      {:ok, metadata}
    end
  end

  defp finish(db, {:ok, _} = result) do
    with :ok <- Sqlite3.execute(db, "COMMIT"), do: result
  end

  defp finish(_db, {:error, _} = error), do: error
  defp finish(_db, _other), do: {:error, :invalid_transaction_result}

  defp append_rows(append, db, generation) do
    case append.(db, generation) do
      {:ok, _} = result -> result
      {:error, _} = error -> error
      _ -> {:error, :invalid_append_result}
    end
  end

  defp require_changed_generation(db) do
    case Sqlite3.changes(db) do
      {:ok, 1} -> :ok
      {:ok, 0} -> {:error, :stale_generation}
      error -> error
    end
  end

  defp write_records(db, records) do
    Enum.reduce_while(records, {:ok, 0}, fn record, {:ok, count} ->
      case write_record(db, record) do
        :ok -> {:cont, {:ok, count + 1}}
        error -> {:halt, error}
      end
    end)
  end

  defp write_record(db, {:insert, {key, payload, checksum}}) do
    account = case RecordCodec.decode_key(key) do
      {:ok, address} -> if(RecordCodec.account?(address), do: 1, else: 0)
      _ -> 0
    end

    run(db, "INSERT INTO relay_records (key, payload, checksum, account) VALUES (?, ?, ?, ?)", [
      {:blob, key},
      {:blob, payload},
      {:blob, checksum},
      account
    ])
  end

  defp write_record(db, {:update, {key, payload, checksum}}) do
    with :ok <-
           run(db, "UPDATE relay_records SET payload = ?, checksum = ? WHERE key = ?", [
             {:blob, payload},
             {:blob, checksum},
             {:blob, key}
           ]),
         do: require_record(db)
  end

  defp write_record(db, {:delete, key}) do
    with :ok <- run(db, "DELETE FROM relay_records WHERE key = ?", [{:blob, key}]),
         do: require_record(db)
  end

  defp require_record(db) do
    case Sqlite3.changes(db) do
      {:ok, 1} -> :ok
      {:ok, 0} -> {:error, :missing_record}
      error -> error
    end
  end

  def select(db, sql, params \\ []) do
    statement(db, sql, params, &Sqlite3.fetch_all(db, &1))
  end

  def run(db, sql, params) do
    case statement(db, sql, params, &Sqlite3.step(db, &1)) do
      :done -> :ok
      error -> error
    end
  end

  defp statement(db, sql, params, fun) do
    with {:ok, stmt} <- Sqlite3.prepare(db, sql) do
      try do
        with :ok <- Sqlite3.bind(stmt, params), do: fun.(stmt)
      after
        Sqlite3.release(db, stmt)
      end
    end
  end
end
