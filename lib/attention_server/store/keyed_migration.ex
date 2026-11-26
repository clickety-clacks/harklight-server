defmodule AttentionServer.Store.KeyedMigration do
  @moduledoc false

  alias AttentionServer.{LargeData, Store}
  alias AttentionServer.Store.{DerivedIndex, RecordCodec, RecordTable}
  alias Exqlite.Sqlite3

  @previous_format 31
  @format 32

  def report_stage(stage) do
    :telemetry.execute([:harklight, :store, :conversion, stage], %{time: System.monotonic_time()}, %{})
  end

  def bootstrap(db, engine) do
    payload = encode_anchor()

    with :ok <- large_data_index(db),
         :ok <- RecordTable.seed(db, 0, RecordCodec.encode(engine)),
         :ok <- DerivedIndex.create(db, 0) do
      RecordTable.run(db, "INSERT INTO relay_state (id, version, payload, checksum) VALUES (1, 0, ?, ?)", [
        {:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}
      ])
    end
  end

  # Prepare the validated in-memory readback before committing the anchor.
  # Runtime selection of the converter belongs to the guarded Store opener.
  def convert_store(%Store{db: db, engine: engine, version: version} = store, prepare)
      when is_function(prepare, 3) do
    RecordTable.transaction(db, fn -> convert_locked(store, engine, version, prepare) end)
  end

  def convert_locked(%Store{db: db} = store, engine, version, prepare) do
    engine = split_item_availability(engine)
    generation = version + 1
    rows = RecordCodec.encode(engine, &LargeData.persist_refs/1)

    with :ok <- verify_previous_progress(store),
         :ok <- Store.verify_retained_uploads(store),
         :ok <- large_data_index(db),
         {:ok, count} <- RecordTable.seed_count(db, generation, rows),
         :ok <- report_stage(:records_written),
         :ok <- DerivedIndex.create(db, generation),
         :ok <- report_stage(:indexes_written),
         :ok <- verify_records(db, engine, count),
         :ok <- report_stage(:records_verified),
         {:ok, prepared} <- prepare.(store, engine, generation),
         :ok <- write_anchor(db, version, generation),
         :ok <- advance_current_logs(db, version, generation),
         :ok <- Sqlite3.execute(db, "DELETE FROM relay_device_progress"),
         :ok <- report_stage(:anchor_written) do
      {:ok, prepared}
    end
  end

  defp split_item_availability(engine) do
    availability =
      Enum.reduce(engine.messages, engine.item_availability, fn
        {{person, _, _} = key, %{origin: origin, session_handle: handle, accepted_seq: seq}}, rows
        when is_binary(handle) ->
          case get_in(engine.origin_sessions, [
                 {person, origin, handle},
                 "client_availability",
                 seq
               ]) do
            nil -> rows
            published -> Map.put(rows, key, published)
          end

        _, rows ->
          rows
      end)

    sessions =
      Map.new(engine.origin_sessions, fn {key, session} ->
        {key, Map.delete(session, "client_availability")}
      end)

    %{engine | origin_sessions: sessions, item_availability: availability}
  end

  # The caller owns the stopped-store conversion transaction. FORMAT32 adds
  # empty keyed collections, so existing row and feed generations stay put.
  def upgrade_anchor(db) do
    supported_identity = RecordCodec.identity()

    with {:ok, [[version, payload, checksum]]} <-
           query(db, "SELECT version, payload, checksum FROM relay_state WHERE id = 1", []),
         true <- :crypto.hash(:sha256, payload) == checksum,
         {:ok, identity} <- decode_anchor(payload, [@previous_format]),
         :ok <- check_identity(identity, supported_identity),
         {:ok, generation} <- RecordTable.generation(db),
         true <- generation >= version,
         :ok <- write_anchor(db, version, version) do
      {:ok, :converted}
    else
      false -> {:error, :invalid_state}
      {:ok, _} -> {:error, :invalid_state}
      error -> error
    end
  end

  def load(db), do: load(db, [@format])

  def load_preview(db), do: load(db, [30, @previous_format, @format])

  defp load(db, formats) do
    # Load the codec's identity atom before decoding the anchor with :safe.
    supported_identity = RecordCodec.identity()

    with {:ok, [[version, payload, checksum]]} <-
           query(db, "SELECT version, payload, checksum FROM relay_state WHERE id = 1", []),
         true <- :crypto.hash(:sha256, payload) == checksum,
         {:ok, identity} <- decode_anchor(payload, formats),
         :ok <- check_identity(identity, supported_identity),
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

  defp decode_anchor(payload, formats) do
    case :erlang.binary_to_term(payload, [:safe]) do
      {:attention_relay_keyed_state, format, identity} ->
        if format in formats, do: {:ok, identity}, else: {:error, :unsupported_keyed_format}

      _ -> {:error, :not_keyed}
    end
  rescue
    ArgumentError -> {:error, :invalid_state}
  end

  defp check_identity(identity, identity), do: :ok

  defp check_identity({codec, written_major}, {codec, running_major}),
    do: {:error, {:unsupported_otp_major, written_major, running_major}}

  defp check_identity(_, _), do: {:error, :unsupported_record_codec}

  defp read_records(db) do
    Code.ensure_loaded!(AttentionServer.SharedPush)
    RecordTable.scan(db, &RecordCodec.decode/1)
  end

  defp verify_records(db, expected, count) do
    result =
      RecordTable.scan(db, fn rows ->
        Enum.reduce_while(rows, 0, fn row, verified ->
          if RecordCodec.matches_row?(expected, row, &LargeData.persist_refs/1),
            do: {:cont, verified + 1},
            else: {:halt, :mismatch}
        end)
      end)

    case result do
      ^count -> :ok
      _ -> {:error, :migration_readback_mismatch}
    end
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
    payload = encode_anchor()

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

  defp encode_anchor do
    :erlang.term_to_binary({:attention_relay_keyed_state, @format, RecordCodec.identity()})
  end

  defp large_data_index(db),
    do: Sqlite3.execute(db, "CREATE INDEX relay_large_data_refs_id ON relay_large_data_refs (id)")

  # Conversion preserves the facts and feed rows. Only logs already current
  # at the legacy snapshot generation can follow its new generation.
  defp advance_current_logs(db, previous, generation) do
    Enum.reduce_while(~w(client_change_log_state agent_change_log_state), :ok, fn table, :ok ->
      case advance_current_log(db, table, previous, generation) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp advance_current_log(db, table, previous, generation) do
    case query(db, "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?", [table]) do
      {:ok, []} ->
        :ok

      {:ok, [[1]]} ->
        sql = "UPDATE #{table} SET store_version = ? WHERE id = 1 AND store_version = ?"

        with :done <- statement(db, sql, [generation, previous], &Sqlite3.step(db, &1)),
             do: :ok

      error ->
        error
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
