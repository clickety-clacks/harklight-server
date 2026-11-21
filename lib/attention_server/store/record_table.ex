defmodule AttentionServer.Store.RecordTable do
  @moduledoc false

  alias Exqlite.Sqlite3

  @schema """
  CREATE TABLE relay_record_generation (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    generation INTEGER NOT NULL
  );
  CREATE TABLE relay_records (
    key BLOB PRIMARY KEY NOT NULL,
    payload BLOB NOT NULL,
    checksum BLOB NOT NULL
  ) WITHOUT ROWID;
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

  # append performs SQL only, on this same connection. Callers publish the
  # new Engine and notify subscribers after the transaction returns success.
  def commit(db, expected_generation, records, append) do
    transaction(db, fn -> persist(db, expected_generation, records, append) end)
  end

  def seed(db, generation, rows) do
    with :ok <- create(db, generation) do
      write_records(db, Stream.map(rows, &{:put, &1}))
    end
  end

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

  defp persist(db, expected_generation, records, append) do
    with :ok <-
           run(
             db,
             "UPDATE relay_record_generation SET generation = generation + 1 WHERE id = 1 AND generation = ?",
             [expected_generation]
           ),
         :ok <- require_changed_generation(db),
         :ok <- write_records(db, records),
         {:ok, metadata} <- append.() do
      {:ok, metadata}
    end
  end

  defp finish(db, {:ok, _} = result) do
    with :ok <- Sqlite3.execute(db, "COMMIT"), do: result
  end

  defp finish(_db, error), do: error

  defp require_changed_generation(db) do
    case Sqlite3.changes(db) do
      {:ok, 1} -> :ok
      {:ok, 0} -> {:error, :stale_generation}
      error -> error
    end
  end

  defp write_records(db, records) do
    Enum.reduce_while(records, :ok, fn record, :ok ->
      case write_record(db, record) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp write_record(db, {:put, {key, payload, checksum}}) do
    run(
      db,
      """
      INSERT INTO relay_records (key, payload, checksum) VALUES (?, ?, ?)
      ON CONFLICT (key) DO UPDATE SET payload = excluded.payload, checksum = excluded.checksum
      """,
      [{:blob, key}, {:blob, payload}, {:blob, checksum}]
    )
  end

  defp write_record(db, {:delete, key}) do
    run(db, "DELETE FROM relay_records WHERE key = ?", [{:blob, key}])
  end

  defp select(db, sql) do
    statement(db, sql, [], &Sqlite3.fetch_all(db, &1))
  end

  defp run(db, sql, params) do
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
