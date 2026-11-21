defmodule AttentionServer.Store.KeyedMigration do
  @moduledoc false

  alias AttentionServer.{Conversation, LargeData, Store}
  alias AttentionServer.Store.{RecordCodec, RecordTable}
  alias Exqlite.Sqlite3

  @format 30
  @derived [:advisory_id_index, :open_default_index, :route_b_index, :touched_keys]

  # The caller supplies the fully normalized and verified legacy Store,
  # including its generation-matched progress overlay.
  def convert(%Store{db: db, engine: engine, version: version} = store) do
    persisted = LargeData.persist_refs(engine)
    expected = canonical(persisted)
    generation = version + 1

    RecordTable.transaction(db, fn ->
      with :ok <- verify_previous_progress(store),
           :ok <-
             Sqlite3.execute(
               db,
               "CREATE INDEX relay_large_data_refs_id ON relay_large_data_refs (id)"
             ),
           :ok <- RecordTable.seed(db, generation, RecordCodec.encode(persisted)),
           {:ok, restored} <- read_records(db),
           true <- canonical(restored) === expected,
           :ok <- write_anchor(db, version, generation),
           :ok <- Sqlite3.execute(db, "DELETE FROM relay_device_progress") do
        {:ok, generation}
      else
        false -> {:error, :migration_readback_mismatch}
        error -> error
      end
    end)
  end

  def load(db) do
    with {:ok, [[version, payload, checksum]]} <-
           query(db, "SELECT version, payload, checksum FROM relay_state WHERE id = 1", []),
         true <- :crypto.hash(:sha256, payload) == checksum,
         {:ok, identity} <- decode_anchor(payload),
         :ok <- check_identity(identity),
         {:ok, generation} <- RecordTable.generation(db),
         true <- generation >= version,
         {:ok, engine} <- read_records(db) do
      {:ok, engine, generation}
    else
      false -> {:error, :invalid_state}
      {:ok, _} -> {:error, :invalid_state}
      error -> error
    end
  end

  defp decode_anchor(payload) do
    case :erlang.binary_to_term(payload, [:safe]) do
      {:attention_relay_keyed_state, @format, identity} -> {:ok, identity}
      {:attention_relay_keyed_state, _, _} -> {:error, :unsupported_keyed_format}
      _ -> {:error, :not_keyed}
    end
  rescue
    ArgumentError -> {:error, :invalid_state}
  end

  defp check_identity({codec, written_major}) do
    case RecordCodec.identity() do
      {^codec, ^written_major} -> :ok
      {^codec, running_major} -> {:error, {:unsupported_otp_major, written_major, running_major}}
      _ -> {:error, :unsupported_record_codec}
    end
  end

  defp check_identity(_), do: {:error, :unsupported_record_codec}

  defp read_records(db) do
    with {:ok, rows} <- RecordTable.rows(db), do: RecordCodec.decode(rows)
  end

  defp canonical(engine) do
    engine
    |> Conversation.index_origin_feeds()
    |> Map.from_struct()
    |> Map.drop(@derived)
  end

  defp verify_previous_progress(store) do
    with {:ok, rows} <-
           query(
             store.db,
             "SELECT version FROM relay_device_progress WHERE id = 1 AND snapshot_version = ?",
             [store.version]
           ),
         true <-
           rows == [[store.progress_version]] or (rows == [] and store.progress_version == 0) do
      :ok
    else
      false -> {:error, :stale_progress}
      error -> error
    end
  end

  defp write_anchor(db, previous, generation) do
    payload =
      :erlang.term_to_binary({:attention_relay_keyed_state, @format, RecordCodec.identity()})

    params = [generation, {:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}, previous]

    with :done <-
           statement(
             db,
             "UPDATE relay_state SET version = ?, payload = ?, checksum = ? WHERE id = 1 AND version = ?",
             params,
             &Sqlite3.step(db, &1)
           ),
         {:ok, 1} <- Sqlite3.changes(db) do
      :ok
    else
      {:ok, 0} -> {:error, :stale_generation}
      error -> error
    end
  end

  defp query(db, sql, params), do: statement(db, sql, params, &Sqlite3.fetch_all(db, &1))

  defp statement(db, sql, params, execute) do
    with {:ok, stmt} <- Sqlite3.prepare(db, sql) do
      try do
        with :ok <- Sqlite3.bind(stmt, params), do: execute.(stmt)
      after
        Sqlite3.release(db, stmt)
      end
    end
  end
end
