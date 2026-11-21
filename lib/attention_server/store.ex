defmodule AttentionServer.Store do
  @moduledoc """
  A transactional SQLite snapshot for the bounded relay core. Origin availability
  and support updates are committed with the complete state before success replies.

  The server serializes decisions in one process. SQLite commits the complete
  new decision state and a content-free audit row before any success reply.
  A version compare prevents a second process using a stale snapshot from
  silently replacing committed work. This is not a public database API.
  """

  alias AttentionServer.{
    APNS,
    BrowserRecipient,
    BounceDecision,
    BrowserSignIn,
    Conversation,
    Diagnostics,
    Engine,
    GuidedJoin,
    LargeData
  }

  alias Exqlite.Sqlite3

  alias AttentionServer.Store.{
    KeyedMigration,
    KeyedReference,
    KeyedValidation,
    Persistence,
    RecordCodec,
    RecordTable,
    SnapshotEncoding
  }

  @format_version 29
  @inbox_index_chunk_size 20
  # `large_base` is the durable engine of the last snapshot commit,
  # `large_persisted` its persisted form and `large_index` its large-data
  # reference rows. A commit reuses them for every unchanged subterm.
  defstruct [
    :db,
    :engine,
    :version,
    :upload_dir,
    :large_base,
    :large_persisted,
    :large_index,
    mode: :legacy,
    source_bindings: %{},
    upload_links: %{},
    authority_index: %{},
    progress_version: 0,
    encoding_cache: %{},
    change_log: false,
    # Persons whose client resources changed since the Relay last ranked them.
    change_log_touched: MapSet.new()
  ]

  @progress_fields [:cursors, :conversation_cursors, :conversation_opened]

  @schema """
  CREATE TABLE IF NOT EXISTS relay_device_progress (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    snapshot_version INTEGER NOT NULL,
    version INTEGER NOT NULL,
    payload BLOB NOT NULL,
    checksum BLOB NOT NULL
  );
  CREATE TABLE IF NOT EXISTS relay_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL,
    payload BLOB NOT NULL,
    checksum BLOB NOT NULL
  );
  CREATE TABLE IF NOT EXISTS relay_large_data_refs (
    path BLOB PRIMARY KEY NOT NULL,
    marker TEXT NOT NULL,
    id TEXT NOT NULL,
    size INTEGER NOT NULL,
    sha256 TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS relay_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    at_ms INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS relay_inbox_snapshots (
    person_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    startup_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    since_seq INTEGER NOT NULL,
    high_water INTEGER NOT NULL,
    status TEXT NOT NULL,
    item_count INTEGER NOT NULL,
    receipt_count INTEGER NOT NULL,
    operation_count INTEGER NOT NULL,
    cursor_secret BLOB NOT NULL,
    receipt_next_offset INTEGER NOT NULL,
    receipts_complete INTEGER NOT NULL,
    PRIMARY KEY (person_id, device_id, startup_id),
    UNIQUE (person_id, device_id, snapshot_id)
  );
  CREATE TABLE IF NOT EXISTS relay_inbox_item_pages (
    person_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    start_offset INTEGER NOT NULL,
    payload BLOB NOT NULL,
    checksum BLOB NOT NULL,
    PRIMARY KEY (person_id, device_id, snapshot_id, start_offset)
  );
  CREATE TABLE IF NOT EXISTS relay_inbox_receipt_sequences (
    person_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    start_offset INTEGER NOT NULL,
    payload BLOB NOT NULL,
    checksum BLOB NOT NULL,
    PRIMARY KEY (person_id, device_id, snapshot_id, start_offset)
  );
  CREATE TABLE IF NOT EXISTS relay_inbox_operation_receipts (
    person_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    op_id TEXT NOT NULL,
    PRIMARY KEY (person_id, device_id, snapshot_id, op_id)
  );
  CREATE TABLE IF NOT EXISTS relay_inbox_batches (
    person_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    input_digest BLOB NOT NULL,
    result BLOB NOT NULL,
    result_checksum BLOB NOT NULL,
    PRIMARY KEY (person_id, device_id, snapshot_id, batch_id)
  );
  CREATE TABLE IF NOT EXISTS relay_inbox_receipt_pages (
    person_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    start_offset INTEGER NOT NULL,
    next_offset INTEGER,
    result BLOB NOT NULL,
    result_checksum BLOB NOT NULL,
    PRIMARY KEY (person_id, device_id, snapshot_id, start_offset)
  );
  """

  def open(nil), do: {:ok, %__MODULE__{db: nil, engine: Engine.new(), version: 0}}

  @doc """
  Turns on the client API v2 change log for this durable store: creates its
  tables, reconciles it with the snapshot, and from then on writes its rows in
  every commit's transaction. A store that is never enabled is unchanged.
  """
  def enable_change_log(%__MODULE__{db: nil}), do: {:error, :not_durable}

  def enable_change_log(%__MODULE__{db: db, version: version} = store) do
    with :ok <- AttentionServer.ChangeLog.enable(db, version),
         :ok <- AttentionServer.AgentChangeLog.enable(db, store.engine, version) do
      engine = AttentionServer.RouteBIndex.enable_agent_feed(store.engine)
      {:ok, %{store | change_log: true, engine: engine}}
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  def open(":memory:") do
    Diagnostics.failure(:server_storage, :store_open, :invalid_store_path, :unavailable)
    {:error, :invalid_store_path}
  end

  def open(path) when is_binary(path) and path != "", do: open(path, :legacy)

  def open(_) do
    Diagnostics.failure(:server_storage, :store_open, :invalid_store_path, :unavailable)
    {:error, :invalid_store_path}
  end

  def open(path, :keyed_cutover)
      when is_binary(path) and path != "" and path != ":memory:" do
    case cutover_mode(path) do
      {:ok, :keyed} ->
        open(path, :keyed)

      {:ok, :legacy} ->
        with {:ok, legacy} <- open(path, :legacy),
             {:ok, _} <- convert_and_close(legacy) do
          open(path, :keyed)
        end

      error ->
        error
    end
  end

  def open(path, mode) when is_binary(path) and path != "" and mode in [:legacy, :keyed] do
    upload_dir = Path.expand(path) <> ".uploads"

    case Sqlite3.open(path) do
      {:ok, db} ->
        case initialize(db, upload_dir, mode) do
          {:ok, engine, version} ->
            case finalize_open(db, upload_dir, engine, version, mode) do
              {:ok, store} ->
                {:ok, store}

              {:error, reason} ->
                Diagnostics.failure(:server_storage, :store_open, reason, :unavailable)
                _ = Sqlite3.close(db)
                {:error, :storage_unavailable}

              other ->
                Diagnostics.failure(:server_storage, :store_open, other, :unavailable)
                _ = Sqlite3.close(db)
                {:error, :storage_unavailable}
            end

          error ->
            Diagnostics.failure(:server_storage, :store_open, error, :unavailable)
            _ = Sqlite3.close(db)
            {:error, {:storage_unavailable, error}}
        end

      error ->
        Diagnostics.failure(:server_storage, :store_open, error, :unavailable)
        {:error, {:storage_unavailable, error}}
    end
  end

  defp cutover_mode(path) do
    with {:ok, db} <- Sqlite3.open(path) do
      try do
        case one(
               db,
               "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name IN ('relay_records', 'relay_record_generation') LIMIT 1",
               []
             ) do
          {:ok, [1]} -> {:ok, :keyed}
          {:ok, nil} -> {:ok, :legacy}
          error -> error
        end
      after
        Sqlite3.close(db)
      end
    end
  end

  defp convert_and_close(legacy) do
    try do
      KeyedMigration.convert(legacy)
    after
      close(legacy)
    end
  end

  defp finalize_open(db, upload_dir, engine, version, mode) do
    engine = engine |> Engine.rebuild_default_index() |> Conversation.index_origin_feeds()

    with {:ok, reference_rows} <- load_large_data_references(db),
         :ok <- File.mkdir_p(upload_dir),
         :ok <- File.chmod(upload_dir, 0o700),
         {:ok, engine} <- LargeData.attach_paths(engine, upload_dir, reference_rows),
         :ok <- LargeData.verify_all(engine),
         store = %__MODULE__{
           db: db,
           engine: engine,
           version: version,
           upload_dir: upload_dir,
           mode: mode
         },
         :ok <- prune_untracked_uploads(store),
         :ok <- LargeData.prune(upload_dir, engine),
         :ok <- sync_large_data_references(db, LargeData.persisted_references(engine)) do
      finalize_mode(store)
    end
  end

  defp finalize_mode(%__MODULE__{mode: :keyed} = store) do
    engine = AttentionServer.RouteBIndex.enable_keyed_lookups(store.engine)

    {:ok,
     %{
       store
       | engine: engine,
         source_bindings: engine.route_b_index.source_bindings,
         upload_links: KeyedValidation.upload_index(engine),
         authority_index: KeyedValidation.authority_index(engine)
     }}
  end

  defp finalize_mode(%__MODULE__{mode: :legacy} = store) do
    with {:ok, store} <- load_progress(store) do
      persisted = LargeData.persist_refs(store.engine)

      {_payload, cache} =
        Persistence.run(fn -> SnapshotEncoding.encode(persisted, @format_version) end)

      {:ok, %{store | encoding_cache: cache}}
    end
  end

  def close(%__MODULE__{db: nil}), do: :ok

  def close(%__MODULE__{db: db}) do
    Diagnostics.result(:server_storage, :store_close, Sqlite3.close(db), :unavailable)
  end

  @doc "Copy, verify and sync upload bytes in the connection process, outside the relay."
  def stage_upload(upload_dir, metadata, source_path)
      when is_binary(upload_dir) and is_binary(source_path) do
    staged = Path.join(upload_dir, metadata.id <> ".staged")

    result =
      with :ok <- File.cp(source_path, staged),
           {:ok, %{size: size}} <- File.stat(staged),
           true <- size == metadata.size,
           {:ok, hash} <- hash_file(staged, operation_id(metadata)),
           true <- hash == metadata.sha256,
           :ok <- sync_file(staged, operation_id(metadata)) do
        {:ok, staged}
      else
        false ->
          Diagnostics.failure(
            :server_storage,
            :upload_stage,
            :upload_integrity_mismatch,
            :unavailable,
            operation_id(metadata)
          )

          {:error, :storage_unavailable}

        error ->
          Diagnostics.failure(
            :server_storage,
            :upload_stage,
            error,
            :unavailable,
            operation_id(metadata)
          )

          {:error, :storage_unavailable}
      end

    if match?({:error, _}, result) do
      case File.rm(staged) do
        :ok ->
          :ok

        {:error, :enoent} ->
          :ok

        {:error, reason} ->
          Diagnostics.failure(
            :server_storage,
            :upload_delete,
            reason,
            :unavailable,
            operation_id(metadata)
          )
      end
    end

    result
  end

  def stage_upload(_, _, _), do: {:error, :storage_unavailable}

  @doc "Atomically publish verified bytes from the connection process, outside the relay."
  def publish_staged_upload(upload_dir, metadata, staged)
      when is_binary(upload_dir) and is_binary(staged) do
    target = Path.join(upload_dir, metadata.id)

    result =
      with :ok <- File.rename(staged, target) do
        case LargeData.sync_directory(upload_dir) do
          :ok ->
            :ok

          error ->
            _ = File.rm(target)
            error
        end
      end

    case result do
      :ok ->
        {:ok, target}

      other ->
        Diagnostics.failure(
          :server_storage,
          :upload_publish,
          other,
          :unavailable,
          operation_id(metadata)
        )

        {:error, :storage_unavailable}
    end
  end

  @doc "Commit authority metadata for bytes already published at their final path."
  def put_published_upload(%__MODULE__{db: nil}, _metadata),
    do: {:error, :storage_unavailable}

  def put_published_upload(%__MODULE__{mode: :keyed, engine: engine} = store, metadata) do
    next = put_in(engine.uploads[metadata.id], metadata)
    keys = MapSet.new([{:row, :uploads, metadata.id}])
    audit = %{person: metadata.person, actor: metadata.uploader, action: "file_upload"}

    case commit(store, {next, keys}, audit) do
      {:ok, committed} -> {:ok, committed}
      _ -> {:error, :storage_unavailable}
    end
  end

  def put_published_upload(%__MODULE__{} = store, metadata) do
    next = %{store.engine | uploads: Map.put(store.engine.uploads, metadata.id, metadata)}

    if valid_state?(next, :incremental) do
      case commit(store, next, %{
             person: metadata.person,
             actor: metadata.uploader,
             action: "file_upload"
           }) do
        {:ok, committed} -> {:ok, committed}
        _ -> {:error, :storage_unavailable}
      end
    else
      Diagnostics.failure(
        :server_storage,
        :state_commit,
        :invalid_state,
        :unavailable,
        operation_id(metadata)
      )

      {:error, :storage_unavailable}
    end
  end

  def get_upload(%__MODULE__{db: nil}, _id), do: {:error, :storage_unavailable}

  def get_upload(%__MODULE__{} = store, id) do
    case Map.get(store.engine.uploads, id) do
      nil ->
        {:error, :not_found}

      %{size: size} = metadata ->
        path = Path.join(store.upload_dir, id)

        case File.stat(path) do
          {:ok, %{size: ^size}} ->
            {:ok, path}

          {:ok, _} ->
            Diagnostics.failure(
              :server_storage,
              :upload_verify,
              :upload_integrity_mismatch,
              :unavailable,
              operation_id(metadata)
            )

            {:error, :storage_unavailable}

          {:error, reason} ->
            Diagnostics.failure(
              :server_storage,
              :upload_verify,
              reason,
              :unavailable,
              operation_id(metadata)
            )

            {:error, :storage_unavailable}

          other ->
            Diagnostics.failure(
              :server_storage,
              :upload_verify,
              other,
              :unavailable,
              operation_id(metadata)
            )

            {:error, :storage_unavailable}
        end
    end
  end

  @doc "Verify fetched bytes in the calling connection, never in the relay process."
  def verify_upload(path, %{size: size, sha256: hash} = metadata) do
    with {:ok, %{size: ^size}} <- File.stat(path),
         {:ok, ^hash} <- hash_file(path, operation_id(metadata)) do
      :ok
    else
      {:error, reason} ->
        Diagnostics.failure(
          :server_storage,
          :upload_verify,
          reason,
          :unavailable,
          operation_id(metadata)
        )

        {:error, :storage_unavailable}

      _ ->
        Diagnostics.failure(
          :server_storage,
          :upload_verify,
          :upload_integrity_mismatch,
          :unavailable,
          operation_id(metadata)
        )

        {:error, :storage_unavailable}
    end
  end

  @doc "Forget only unreferenced uploads in the durable snapshot before unlinking their bytes."
  def delete_uploads(%__MODULE__{} = store, [], _audit), do: {:ok, store}

  def delete_uploads(%__MODULE__{} = store, ids, audit) when is_list(ids) do
    next = %{store.engine | uploads: Map.drop(store.engine.uploads, ids)}

    proposal =
      if store.mode == :keyed,
        do: {next, existing_upload_rows(store.engine, ids)},
        else: next

    case commit(store, proposal, audit) do
      {:ok, updated} ->
        result =
          Enum.reduce_while(ids, :ok, fn id, :ok ->
            case File.rm(Path.join(updated.upload_dir, id)) do
              :ok ->
                {:cont, :ok}

              {:error, :enoent} ->
                {:cont, :ok}

              {:error, reason} ->
                Diagnostics.failure(
                  :server_storage,
                  :upload_delete,
                  reason,
                  :unavailable,
                  operation_id(audit)
                )

                {:halt, {:error, :storage_unavailable}}
            end
          end)

        case result do
          :ok -> {:ok, updated}
          {:error, reason} -> {:error, reason, updated}
        end

      {:error, reason, _} ->
        {:error, reason, store}
    end
  end

  defp existing_upload_rows(engine, ids) do
    for id <- ids, Map.has_key?(engine.uploads, id), into: MapSet.new(), do: {:row, :uploads, id}
  end

  @doc "Remove upload files whose authority metadata was already removed by a prior commit."
  def prune_untracked_uploads(%__MODULE__{upload_dir: nil}), do: :ok

  def prune_untracked_uploads(%__MODULE__{} = store) do
    with {:ok, names} <- File.ls(store.upload_dir) do
      names
      |> Enum.filter(fn name ->
        cond do
          AttentionServer.Message.uuid_v4?(name) ->
            not Map.has_key?(store.engine.uploads, name)

          String.ends_with?(name, ".staged") ->
            AttentionServer.Message.uuid_v4?(String.trim_trailing(name, ".staged"))

          true ->
            false
        end
      end)
      |> Enum.reduce_while(:ok, fn name, :ok ->
        case File.rm(Path.join(store.upload_dir, name)) do
          :ok ->
            {:cont, :ok}

          {:error, :enoent} ->
            {:cont, :ok}

          {:error, reason} ->
            Diagnostics.failure(:server_storage, :upload_delete, reason, :unavailable)
            {:halt, {:error, :storage_unavailable}}
        end
      end)
    else
      {:error, reason} ->
        Diagnostics.failure(:server_storage, :store_open, reason, :unavailable)
        {:error, :storage_unavailable}
    end
  end

  defp sync_file(path, operation_id) do
    with {:ok, file} <- :file.open(String.to_charlist(path), [:read, :write, :binary]) do
      result = :file.sync(file)

      case :file.close(file) do
        :ok ->
          :ok

        {:error, reason} ->
          Diagnostics.failure(
            :server_storage,
            :upload_stage,
            reason,
            :unavailable,
            operation_id
          )
      end

      result
    end
  end

  defp hash_file(path, operation_id) do
    case File.open(path, [:read, :binary]) do
      {:ok, file} ->
        result = hash_chunks(file, :crypto.hash_init(:sha256))

        case File.close(file) do
          :ok ->
            :ok

          {:error, reason} ->
            Diagnostics.failure(
              :server_storage,
              :upload_verify,
              reason,
              :unavailable,
              operation_id
            )
        end

        result

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp hash_chunks(file, state) do
    case IO.binread(file, 65_536) do
      :eof -> {:ok, Base.encode16(:crypto.hash_final(state), case: :lower)}
      bytes when is_binary(bytes) -> hash_chunks(file, :crypto.hash_update(state, bytes))
      {:error, reason} -> {:error, reason}
    end
  end

  def inbox_startup(%__MODULE__{db: nil}, _person, _device, _startup_id),
    do: {:error, :storage_unavailable}

  def inbox_startup(%__MODULE__{db: db}, person, device, startup_id) do
    with {:ok, row} <-
           one(
             db,
             "SELECT person_id, device_id, startup_id, snapshot_id, since_seq, high_water, status, item_count, receipt_count, operation_count, cursor_secret, receipt_next_offset, receipts_complete FROM relay_inbox_snapshots WHERE person_id = ? AND device_id = ? AND startup_id = ?",
             [person, device, startup_id]
           ) do
      decode_inbox_snapshot(row)
    end
  end

  def inbox_snapshot(%__MODULE__{db: nil}, _person, _device, _snapshot_id),
    do: {:error, :storage_unavailable}

  def inbox_snapshot(%__MODULE__{db: db}, person, device, snapshot_id) do
    with {:ok, row} <-
           one(
             db,
             "SELECT person_id, device_id, startup_id, snapshot_id, since_seq, high_water, status, item_count, receipt_count, operation_count, cursor_secret, receipt_next_offset, receipts_complete FROM relay_inbox_snapshots WHERE person_id = ? AND device_id = ? AND snapshot_id = ?",
             [person, device, snapshot_id]
           ) do
      decode_inbox_snapshot(row)
    end
  end

  def inbox_items(%__MODULE__{db: nil}, _snapshot, _offset),
    do: {:error, :storage_unavailable}

  def inbox_items(%__MODULE__{db: db}, snapshot, offset) do
    cond do
      offset == 0 and snapshot.item_count == 0 ->
        {:ok, []}

      not (is_integer(offset) and offset >= 0 and offset < snapshot.item_count) ->
        {:error, :invalid_page_cursor}

      rem(offset, @inbox_index_chunk_size) != 0 ->
        {:error, :invalid_page_cursor}

      true ->
        expected_count = min(@inbox_index_chunk_size, snapshot.item_count - offset)

        with {:ok, row} <-
               one(
                 db,
                 "SELECT payload, checksum FROM relay_inbox_item_pages WHERE person_id = ? AND device_id = ? AND snapshot_id = ? AND start_offset = ?",
                 [snapshot.person_id, snapshot.device_id, snapshot.snapshot_id, offset]
               ),
             {:ok, items} <- decode_inbox_index_chunk(row, expected_count, :items) do
          {:ok, items}
        end
    end
  end

  def inbox_receipt_sequences(%__MODULE__{db: nil}, _snapshot, _offset, _count),
    do: {:error, :storage_unavailable}

  def inbox_receipt_sequences(%__MODULE__{} = store, snapshot, offset, count)
      when is_integer(offset) and is_integer(count) and offset >= 0 and count in 1..20 do
    if offset + count > snapshot.receipt_count do
      {:error, :invalid_page_cursor}
    else
      read_receipt_sequences(store, snapshot, offset, count, [])
    end
  end

  def inbox_receipt_sequences(_store, _snapshot, _offset, _count),
    do: {:error, :invalid_page_cursor}

  def inbox_operation_at_snapshot?(%__MODULE__{db: nil}, _snapshot, _op_id),
    do: {:error, :storage_unavailable}

  def inbox_operation_at_snapshot?(%__MODULE__{db: db}, snapshot, op_id)
      when is_binary(op_id) do
    with {:ok, row} <-
           one(
             db,
             "SELECT 1 FROM relay_inbox_operation_receipts WHERE person_id = ? AND device_id = ? AND snapshot_id = ? AND op_id = ?",
             [snapshot.person_id, snapshot.device_id, snapshot.snapshot_id, op_id]
           ) do
      {:ok, not is_nil(row)}
    end
  end

  def begin_inbox_snapshot(%__MODULE__{db: nil}, _attrs),
    do: {:error, :storage_unavailable}

  def begin_inbox_snapshot(%__MODULE__{db: db}, attrs) do
    result =
      with :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
           {:ok, row} <-
             one(
               db,
               "SELECT person_id, device_id, startup_id, snapshot_id, since_seq, high_water, status, item_count, receipt_count, operation_count, cursor_secret, receipt_next_offset, receipts_complete FROM relay_inbox_snapshots WHERE person_id = ? AND device_id = ? AND startup_id = ?",
               [attrs.person_id, attrs.device_id, attrs.startup_id]
             ),
           true <- is_nil(row),
           :ok <-
             run(
               db,
               "INSERT INTO relay_inbox_snapshots (person_id, device_id, startup_id, snapshot_id, since_seq, high_water, status, item_count, receipt_count, operation_count, cursor_secret, receipt_next_offset, receipts_complete) VALUES (?, ?, ?, ?, ?, ?, 'building', 0, 0, 0, ?, 0, 0)",
               [
                 attrs.person_id,
                 attrs.device_id,
                 attrs.startup_id,
                 attrs.snapshot_id,
                 attrs.since,
                 attrs.high_water,
                 {:blob, attrs.cursor_secret}
               ]
             ),
           {:ok, saved_row} <-
             one(
               db,
               "SELECT person_id, device_id, startup_id, snapshot_id, since_seq, high_water, status, item_count, receipt_count, operation_count, cursor_secret, receipt_next_offset, receipts_complete FROM relay_inbox_snapshots WHERE person_id = ? AND device_id = ? AND startup_id = ?",
               [attrs.person_id, attrs.device_id, attrs.startup_id]
             ),
           {:ok, snapshot} <- decode_inbox_snapshot(saved_row),
           :ok <- Sqlite3.execute(db, "COMMIT") do
        {:ok, snapshot}
      else
        false -> {:error, :checkpoint_conflict}
        error -> error
      end

    case result do
      {:ok, snapshot} ->
        {:ok, snapshot}

      error ->
        _ = Sqlite3.execute(db, "ROLLBACK")
        error
    end
  end

  def save_inbox_snapshot_chunk(%__MODULE__{db: nil}, _snapshot, _kind, _offset, _values),
    do: {:error, :storage_unavailable}

  def save_inbox_snapshot_chunk(
        %__MODULE__{db: db},
        snapshot,
        kind,
        offset,
        values
      )
      when is_integer(offset) and offset >= 0 and rem(offset, @inbox_index_chunk_size) == 0 and
             is_list(values) and length(values) in 1..@inbox_index_chunk_size do
    result =
      with :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
           {:ok, :building} <- building_inbox_snapshot(db, snapshot) do
        case insert_inbox_snapshot_chunk(db, snapshot, kind, offset, values) do
          :ok -> Sqlite3.execute(db, "COMMIT")
          error -> error
        end
      end

    case result do
      :ok ->
        :ok

      error ->
        _ = Sqlite3.execute(db, "ROLLBACK")
        error
    end
  end

  def save_inbox_snapshot_chunk(_store, _snapshot, _kind, _offset, _values),
    do: {:error, :snapshot_unavailable}

  def finish_inbox_snapshot(%__MODULE__{db: nil}, _snapshot, _counts),
    do: {:error, :storage_unavailable}

  def finish_inbox_snapshot(%__MODULE__{db: db}, snapshot, counts) do
    item_count = Map.get(counts, :items)
    receipt_count = Map.get(counts, :receipts)
    operation_count = Map.get(counts, :operation_ids)

    if Enum.all?([item_count, receipt_count, operation_count], &(is_integer(&1) and &1 >= 0)) do
      result =
        with :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
             {:ok, :building} <- building_inbox_snapshot(db, snapshot),
             :ok <-
               run(
                 db,
                 "UPDATE relay_inbox_snapshots SET status = 'pending', item_count = ?, receipt_count = ?, operation_count = ?, receipts_complete = ? WHERE person_id = ? AND device_id = ? AND snapshot_id = ? AND status = 'building'",
                 [
                   item_count,
                   receipt_count,
                   operation_count,
                   if(receipt_count == 0, do: 1, else: 0),
                   snapshot.person_id,
                   snapshot.device_id,
                   snapshot.snapshot_id
                 ]
               ),
             {:ok, 1} <- Sqlite3.changes(db),
             :ok <- Sqlite3.execute(db, "COMMIT") do
          :ok
        end

      case result do
        :ok ->
          :ok

        error ->
          _ = Sqlite3.execute(db, "ROLLBACK")
          error
      end
    else
      {:error, :snapshot_unavailable}
    end
  end

  def discard_building_inbox_snapshots(%__MODULE__{db: nil}), do: :ok

  def discard_building_inbox_snapshots(%__MODULE__{db: db}) do
    with :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
         :ok <- delete_building_snapshot_rows(db),
         :ok <- Sqlite3.execute(db, "COMMIT") do
      :ok
    else
      error ->
        _ = Sqlite3.execute(db, "ROLLBACK")
        error
    end
  end

  def discard_browser_inbox_snapshots(%__MODULE__{db: nil}), do: :ok

  def discard_browser_inbox_snapshots(%__MODULE__{db: db}) do
    discard_inbox_snapshots_matching(db, "device_id LIKE 'browser:%'", [])
  end

  def discard_inbox_snapshots_for_device(%__MODULE__{db: nil}, _person, _device), do: :ok

  def discard_inbox_snapshots_for_device(%__MODULE__{db: db}, person, device) do
    discard_inbox_snapshots_matching(db, "person_id = ? AND device_id = ?", [person, device])
  end

  def discard_inbox_snapshot(%__MODULE__{db: nil}, _snapshot), do: :ok

  def discard_inbox_snapshot(%__MODULE__{db: db}, snapshot) do
    with :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
         :ok <- delete_snapshot_rows(db, snapshot.snapshot_id),
         :ok <- Sqlite3.execute(db, "COMMIT") do
      :ok
    else
      error ->
        _ = Sqlite3.execute(db, "ROLLBACK")
        error
    end
  end

  def inbox_batch(%__MODULE__{db: nil}, _person, _device, _snapshot_id, _batch_id),
    do: {:error, :storage_unavailable}

  def inbox_batch(%__MODULE__{db: db}, person, device, snapshot_id, batch_id) do
    with {:ok, row} <-
           one(
             db,
             "SELECT input_digest, result, result_checksum FROM relay_inbox_batches WHERE person_id = ? AND device_id = ? AND snapshot_id = ? AND batch_id = ?",
             [person, device, snapshot_id, batch_id]
           ) do
      decode_inbox_batch(row)
    end
  end

  def save_inbox_batch(
        %__MODULE__{db: nil},
        _person,
        _device,
        _snapshot_id,
        _batch_id,
        _digest,
        _result
      ),
      do: {:error, :storage_unavailable}

  def save_inbox_batch(%__MODULE__{db: db}, person, device, snapshot_id, batch_id, digest, result) do
    payload = encode_term(result)
    checksum = :crypto.hash(:sha256, payload)

    case inbox_batch(%__MODULE__{db: db}, person, device, snapshot_id, batch_id) do
      {:ok, nil} ->
        with :ok <-
               run(
                 db,
                 "INSERT INTO relay_inbox_batches (person_id, device_id, snapshot_id, batch_id, input_digest, result, result_checksum) VALUES (?, ?, ?, ?, ?, ?, ?)",
                 [
                   person,
                   device,
                   snapshot_id,
                   batch_id,
                   {:blob, digest},
                   {:blob, payload},
                   {:blob, checksum}
                 ]
               ) do
          {:ok, result}
        end

      {:ok, %{digest: ^digest, result: saved}} ->
        {:ok, saved}

      {:ok, _different} ->
        {:error, :checkpoint_conflict}

      error ->
        error
    end
  end

  def inbox_receipt_page(%__MODULE__{db: nil}, _person, _device, _snapshot_id, _offset),
    do: {:error, :storage_unavailable}

  def inbox_receipt_page(%__MODULE__{db: db}, person, device, snapshot_id, offset) do
    with {:ok, row} <-
           one(
             db,
             "SELECT result, result_checksum FROM relay_inbox_receipt_pages WHERE person_id = ? AND device_id = ? AND snapshot_id = ? AND start_offset = ?",
             [person, device, snapshot_id, offset]
           ) do
      decode_inbox_receipt_page(row)
    end
  end

  def save_inbox_receipt_page(
        %__MODULE__{db: nil},
        _person,
        _device,
        _snapshot_id,
        _offset,
        _next_offset,
        _result
      ),
      do: {:error, :storage_unavailable}

  def save_inbox_receipt_page(
        %__MODULE__{db: db},
        person,
        device,
        snapshot_id,
        offset,
        next_offset,
        result
      ) do
    payload = encode_term(result)
    checksum = :crypto.hash(:sha256, payload)

    with :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
         {:ok, existing} <-
           one(
             db,
             "SELECT result, result_checksum FROM relay_inbox_receipt_pages WHERE person_id = ? AND device_id = ? AND snapshot_id = ? AND start_offset = ?",
             [person, device, snapshot_id, offset]
           ),
         {:ok, saved} <-
           existing_or_insert_receipt_page(
             db,
             existing,
             person,
             device,
             snapshot_id,
             offset,
             next_offset,
             result,
             payload,
             checksum
           ),
         :ok <- Sqlite3.execute(db, "COMMIT") do
      {:ok, saved}
    else
      error ->
        _ = Sqlite3.execute(db, "ROLLBACK")
        error
    end
  end

  def inbox_checkpoint_status(%__MODULE__{db: nil}, _person, _device, _snapshot_id),
    do: {:error, :storage_unavailable}

  def inbox_checkpoint_status(%__MODULE__{db: db}, person, device, snapshot_id) do
    with {:ok, row} <-
           one(
             db,
             "SELECT status, since_seq, high_water, receipts_complete FROM relay_inbox_snapshots WHERE person_id = ? AND device_id = ? AND snapshot_id = ?",
             [person, device, snapshot_id]
           ) do
      case row do
        nil ->
          {:error, :invalid_snapshot}

        [status, since, high, receipts_complete] ->
          {:ok,
           %{
             status: status,
             since: since,
             high_water: high,
             receipts_complete: receipts_complete == 1
           }}
      end
    end
  end

  def accept_inbox_checkpoint(
        %__MODULE__{db: nil} = store,
        _engine,
        _person,
        _device,
        _snapshot_id,
        _high,
        _batches,
        _audit
      ),
      do: {:error, :storage_unavailable, store}

  def accept_inbox_checkpoint(
        %__MODULE__{mode: :keyed, db: db} = store,
        engine,
        person,
        device,
        snapshot_id,
        high,
        batches,
        audit
      ) do
    address = {:row, :cursors, {person, device}}

    with {:ok, ^person} <- Engine.authorized_person(engine, device),
         {:ok, next} <- Engine.checkpoint_cursor(engine, device, high),
         true <- valid_authority_reference?(next, address) do
      records =
        if Map.get(engine.cursors, {person, device}) == high,
          do: [],
          else: [{:put, RecordCodec.encode_row({:row, :cursors, {person, device}}, high)}]

      result =
        Persistence.run(fn ->
          RecordTable.transaction(db, fn ->
            with {:ok, generation} when generation == store.version <- RecordTable.generation(db),
                 {:ok, duplicate?} <-
                   checkpoint_status(db, person, device, snapshot_id, high, batches) do
              if duplicate? do
                {:ok, true}
              else
                RecordTable.write_generation(db, generation, records, fn db, _ ->
                  with :ok <- accept_checkpoint_row(db, person, device, snapshot_id),
                       :ok <- write_audit(db, audit),
                       do: {:ok, false}
                end)
              end
            else
              {:ok, _stale} -> {:error, :storage_unavailable}
              error -> error
            end
          end)
        end)

      case result do
        {:ok, true} ->
          {:ok, store, true}

        {:ok, false} ->
          {authority, _} =
            KeyedValidation.authority_changes(store.authority_index, engine, next, [address])

          committed = %{
            store
            | engine: next,
              authority_index: authority,
              version: store.version + 1
          }

          Persistence.committed(committed)
          {:ok, committed, false}

        {:error, reason} ->
          {:error, reason, store}
      end
    else
      false -> {:error, :forbidden, store}
      {:ok, _other_person} -> {:error, :forbidden, store}
      {:error, reason} -> {:error, reason, store}
    end
  end

  def accept_inbox_checkpoint(
        %__MODULE__{} = store,
        engine,
        person,
        device,
        snapshot_id,
        high,
        batches,
        audit
      ) do
    with {:ok, next_engine} <- Engine.checkpoint_cursor(engine, device, high),
         %__MODULE__{db: db} <- store,
         :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
         :ok <- check_progress_version(store),
         {:ok, duplicate?} <-
           checkpoint_status(db, person, device, snapshot_id, high, batches),
         :ok <-
           if(duplicate?,
             do: :ok,
             else: checkpoint_commit(store, next_engine, person, device, snapshot_id, audit)
           ),
         :ok <- Sqlite3.execute(db, "COMMIT") do
      next_store =
        if duplicate?,
          do: store,
          else: %{store | engine: next_engine, progress_version: store.progress_version + 1}

      {:ok, next_store, duplicate?}
    else
      {:error, reason} ->
        _ = if store.db, do: Sqlite3.execute(store.db, "ROLLBACK")
        {:error, reason, store}

      {:error, reason, _} ->
        _ = if store.db, do: Sqlite3.execute(store.db, "ROLLBACK")
        {:error, reason, store}
    end
  end

  defp checkpoint_status(db, person, device, snapshot_id, high, batches) do
    with {:ok, row} <-
           one(
             db,
             "SELECT status, high_water, receipts_complete FROM relay_inbox_snapshots WHERE person_id = ? AND device_id = ? AND snapshot_id = ?",
             [person, device, snapshot_id]
           ) do
      checkpoint_ready(db, row, person, device, snapshot_id, high, batches)
    end
  end

  defp initialize(db, upload_dir, :legacy) do
    with :ok <- set_durability(db),
         :ok <- Sqlite3.execute(db, @schema) do
      load_or_create(db, upload_dir)
    end
  end

  defp initialize(db, _upload_dir, :keyed) do
    with {:ok, engine, generation} <- KeyedMigration.load(db),
         engine = engine |> BounceDecision.rebuild_indexes() |> Engine.rebuild_route_b_index(),
         true <- valid_state?(engine),
         :ok <- set_durability(db) do
      {:ok, engine, generation}
    else
      false -> {:error, :invalid_state}
      error -> error
    end
  end

  defp set_durability(db),
    do: Sqlite3.execute(db, "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")

  @doc """
  Commits engine state and both change logs. Publishers can supply canonical
  entry keys whose projection changed while their stored facts stayed equal.
  """
  def commit(store, next, audit, projected_entries \\ [])

  def commit(
        %__MODULE__{mode: :keyed} = store,
        {%Engine{} = next, %MapSet{} = touched},
        audit,
        projected_entries
      ) do
    projected_entries = if store.change_log, do: projected_entries, else: []

    if MapSet.size(touched) == 0 and projected_entries == [],
      do: {:ok, store},
      else: commit_keyed(store, next, touched, audit, projected_entries)
  end

  def commit(%__MODULE__{mode: :keyed} = store, _next, _audit, _projected_entries),
    do: {:error, :storage_unavailable, store}

  def commit(store, next, audit, projected_entries) do
    projected_entries = if store.change_log, do: projected_entries, else: []

    case do_commit(store, next, audit, projected_entries) do
      {:ok, committed} = result ->
        Persistence.committed(committed)
        result

      other ->
        other
    end
  end

  def commit_inbox_batch(store, next, touched, audit, snapshot_id, batch_id, digest, result) do
    commit_keyed(store, next, touched, audit, [], fn ->
      case save_inbox_batch(
             store,
             audit.person,
             audit.actor,
             snapshot_id,
             batch_id,
             digest,
             result
           ) do
        {:ok, _saved} -> :ok
        error -> error
      end
    end)
  end

  defp commit_keyed(
         store,
         next,
         touched,
         audit,
         projected_entries,
         write_metadata \\ fn -> :ok end
       ) do
    items = for {:row, :messages, key} <- touched, do: key

    {authority_index, affected} =
      KeyedValidation.authority_changes(store.authority_index, store.engine, next, touched)

    result =
      with true <-
             Enum.all?(MapSet.union(touched, affected), &valid_authority_reference?(next, &1)),
           true <- Enum.all?(touched, &valid_push_address_change?(store.engine, next, &1)),
           true <- Enum.all?(touched, &valid_transition_fact?(store.engine, next, &1)),
           :ok <- KeyedValidation.sequences(store.engine, next, touched),
           {:ok, upload_links, entries} <-
             KeyedValidation.upload_changes(store.upload_links, store.engine, next, touched),
           true <-
             Enum.all?(entries, fn {person, entry} ->
               valid_entry_uploads?(next, person, entry)
             end),
           true <-
             Enum.all?(touched, fn
               {:row, :uploads, id} ->
                 case Map.fetch(next.uploads, id) do
                   {:ok, upload} -> valid_upload?(next, id, upload)
                   :error -> not Map.has_key?(upload_links, id)
                 end

               _ ->
                 true
             end),
           true <- Enum.all?(items, &valid_item_change?(store.engine, next, &1)),
           {:ok, index} <-
             KeyedValidation.source_changes(
               store.source_bindings,
               store.engine.messages,
               next.messages,
               items
             ),
           {:ok, prepared} <-
             KeyedReference.prepare(store.engine, next, touched, store.upload_dir) do
        %{engine: next, records: records, delete_paths: deleted, reference_rows: refs} = prepared
        log = prepare_change_logs(store, next, touched, projected_entries)
        %{db: db, version: version} = store

        case Persistence.run(fn ->
               RecordTable.commit(db, version, records, fn db, generation ->
                 with :ok <- write_reference_delta(db, deleted, refs),
                      :ok <- write_audit(db, audit),
                      :ok <- write_metadata.(),
                      {:ok, heads, agent_heads} <- append_change_logs(db, log, generation) do
                   {:ok, {heads, agent_heads}}
                 end
               end)
             end) do
          {:ok, {heads, agent_heads}} ->
            prune_reference_candidates(db, store.upload_dir, prepared.orphan_candidates)
            AttentionServer.ChangeLog.notify(heads)
            AttentionServer.ChangeLog.notify(agent_heads)
            if log, do: AttentionServer.ChangeLog.notify_closing(log.closings)

            {:ok,
             %{
               store
               | engine: put_in(next.route_b_index.source_bindings, index),
                 source_bindings: index,
                 upload_links: upload_links,
                 authority_index: authority_index,
                 version: store.version + 1,
                 change_log_touched:
                   MapSet.union(store.change_log_touched, MapSet.new(Map.keys(heads)))
             }}

          error ->
            error
        end
      end

    case result do
      {:ok, committed} ->
        Persistence.committed(committed)
        {:ok, committed}

      _ ->
        {:error, :storage_unavailable, store}
    end
  end

  defp write_reference_delta(db, deleted_paths, reference_rows) do
    with :ok <-
           write_reference_rows(deleted_paths, fn path ->
             run(db, "DELETE FROM relay_large_data_refs WHERE path = ?", [{:blob, path}])
           end) do
      write_reference_rows(reference_rows, &insert_reference(db, &1))
    end
  end

  defp write_reference_rows(rows, write) do
    Enum.reduce_while(rows, :ok, fn row, :ok ->
      case write.(row) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp prune_reference_candidates(db, directory, candidates) do
    Enum.each(Enum.uniq(candidates), fn id ->
      if one(db, "SELECT 1 FROM relay_large_data_refs WHERE id = ? LIMIT 1", [id]) == {:ok, nil} do
        _ = File.rm(Path.join(directory, id))
      end
    end)
  end

  defp write_audit(db, audit) do
    run(db, "INSERT INTO relay_audit (person_id, actor_id, action, at_ms) VALUES (?, ?, ?, ?)", [
      audit.person,
      audit.actor,
      audit.action,
      System.system_time(:millisecond)
    ])
  end

  defp do_commit(%__MODULE__{engine: old} = store, next, _audit, []) when old == next,
    do: {:ok, store}

  defp do_commit(%__MODULE__{db: nil} = store, next, _audit, _projected_entries),
    do: {:ok, %{store | engine: next}}

  defp do_commit(%__MODULE__{} = store, next, audit, projected_entries) do
    cond do
      projected_entries == [] and progress_only?(store.engine, next) and valid_progress?(next) ->
        commit_progress(store, next, audit)

      valid_state?(next, :incremental, store.engine.messages) ->
        commit_durable(store, next, audit, projected_entries)

      true ->
        Diagnostics.failure(
          :server_storage,
          :state_commit,
          :invalid_state,
          :unavailable,
          operation_id(audit)
        )

        {:error, :storage_unavailable, store}
    end
  end

  defp commit_durable(
         %__MODULE__{db: db, version: version} = store,
         next,
         audit,
         projected_entries \\ []
       ) do
    # Large-data preparation runs here, against the previous durable base this
    # process already holds, so only changed subterms are walked and nothing
    # large is copied to or from the worker. The worker needs the generation and
    # connection, not a second copy of the old engine. The caller keeps its
    # committed snapshot until this returns.
    generation = %{
      store
      | engine: nil,
        large_base: nil,
        large_persisted: nil,
        large_index: nil
    }

    # The change log's rows are found here, against the engine this process
    # already holds, and written in the snapshot's transaction.
    log = prepare_change_logs(store, next, nil, projected_entries)

    result =
      with {:ok, durable_next, persisted, index} <- prepare_large_data(store, next) do
        old_ids =
          if store.large_index,
            do: LargeData.reference_index_ids(store.large_index),
            else: LargeData.refs(store.engine)

        next_ids = LargeData.reference_index_ids(index)
        rows = LargeData.reference_index_rows(index)

        case Persistence.run(fn ->
               persist_snapshot(generation, persisted, rows, audit, old_ids, next_ids, log)
             end) do
          {:ok, cache, heads, agent_heads} ->
            {:ok, cache, heads, agent_heads, durable_next, persisted, index}

          error ->
            error
        end
      end

    case result do
      {:ok, cache, heads, agent_heads, durable_next, persisted, index} ->
        AttentionServer.ChangeLog.notify(heads)
        AttentionServer.ChangeLog.notify(agent_heads)
        if log, do: AttentionServer.ChangeLog.notify_closing(log.closings)

        {:ok,
         %{
           store
           | engine: durable_next,
             version: version + 1,
             progress_version: 0,
             encoding_cache: cache,
             large_base: durable_next,
             large_persisted: persisted,
             large_index: index,
             change_log_touched:
               MapSet.union(store.change_log_touched, MapSet.new(Map.keys(heads)))
         }}

      error ->
        Diagnostics.failure(
          :server_storage,
          :state_commit,
          error,
          :outcome_unknown,
          operation_id(audit)
        )

        _ = Sqlite3.execute(db, "ROLLBACK")
        {:error, :storage_unavailable, store}
    end
  end

  defp prepare_large_data(%__MODULE__{upload_dir: upload_dir, large_base: base} = store, next)
       when is_binary(upload_dir) and not is_nil(base) do
    case LargeData.commit_incremental(
           next,
           base,
           store.large_persisted,
           store.large_index,
           upload_dir
         ) do
      {:ok, durable_next, persisted, index, _created} -> {:ok, durable_next, persisted, index}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_large_data(%__MODULE__{upload_dir: upload_dir}, next) do
    with {:ok, durable_next, _created} <- LargeData.externalize(next, upload_dir),
         true <- LargeData.valid_refs?(durable_next) do
      {:ok, durable_next, LargeData.persist_refs(durable_next),
       LargeData.reference_index(durable_next)}
    else
      false -> {:error, :invalid_large_data}
      error -> error
    end
  end

  defp persist_snapshot(
         %{db: db, version: version, upload_dir: upload_dir} = store,
         persisted,
         reference_rows,
         audit,
         old_ids,
         next_ids,
         log
       ) do
    with {payload, cache} <-
           SnapshotEncoding.encode(persisted, @format_version, store.encoding_cache),
         checksum = :crypto.hash(:sha256, payload),
         :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
         :ok <- check_progress_version(store),
         :ok <- write_large_data_references(db, reference_rows),
         :ok <-
           run(
             db,
             "UPDATE relay_state SET version = version + 1, payload = ?, checksum = ? WHERE id = 1 AND version = ?",
             [{:blob, payload}, {:blob, checksum}, version]
           ),
         {:ok, 1} <- Sqlite3.changes(db),
         :ok <- Sqlite3.execute(db, "DELETE FROM relay_device_progress"),
         :ok <-
           run(
             db,
             "INSERT INTO relay_audit (person_id, actor_id, action, at_ms) VALUES (?, ?, ?, ?)",
             [audit.person, audit.actor, audit.action, System.system_time(:millisecond)]
           ),
         {:ok, heads, agent_heads} <- append_change_logs(db, log, version + 1),
         :ok <- Sqlite3.execute(db, "COMMIT") do
      if is_binary(upload_dir) do
        case LargeData.delete_unreferenced(upload_dir, old_ids, next_ids, persisted) do
          :ok ->
            :ok

          error ->
            Diagnostics.failure(
              :server_storage,
              :large_data_delete,
              error,
              :unavailable,
              operation_id(audit)
            )
        end
      end

      {:ok, cache, heads, agent_heads}
    end
  end

  defp prepare_change_logs(%__MODULE__{change_log: false}, _next, _touched, _projected_entries),
    do: nil

  defp prepare_change_logs(store, next, touched, projected_entries) do
    changed =
      if touched do
        AttentionServer.ChangeLog.changed_resources(
          store.engine,
          next,
          touched,
          projected_entries
        )
      else
        AttentionServer.ChangeLog.changed_resources(store.engine, next, projected_entries)
      end

    pending = AttentionServer.ChangeLog.changes(store.engine, next, changed)

    seeded =
      Map.new(pending, fn {person, _} ->
        seeded =
          if touched do
            Map.get(store.engine.persons, person, 0) > 0 or
              Map.get(store.engine.conversation_highs, person, 0) > 0 or
              Map.has_key?(store.engine.bounce_levels, person)
          else
            AttentionServer.ChangeLog.seeded?(store.engine, person)
          end

        {person, seeded}
      end)

    %{
      pending: pending,
      seeded: seeded,
      closings:
        if(touched,
          do: AttentionServer.ChangeLog.closings(store.engine, next, touched),
          else: AttentionServer.ChangeLog.closings(store.engine, next)
        ),
      created: AttentionServer.ChangeLog.new_resources(store.engine, next, changed),
      agent_pending: AttentionServer.AgentChangeLog.changes(store.engine, next, changed)
    }
  end

  defp append_change_logs(_db, nil, _version), do: {:ok, %{}, %{}}

  defp append_change_logs(db, log, version) do
    with {:ok, heads} <-
           AttentionServer.ChangeLog.append(db, log.pending, log.seeded, version, log.created),
         {:ok, agent_heads} <-
           AttentionServer.AgentChangeLog.append(db, log.agent_pending, version) do
      {:ok, heads, agent_heads}
    end
  end

  # ACKs must be durable before their replies, but do not change retained history.
  # The overlay is tied to the snapshot generation. Older releases ignore it;
  # if they write a newer snapshot, its progress supersedes the old overlay.
  defp progress_only?(old, next) do
    Map.merge(next, Map.take(old, @progress_fields)) === old
  end

  defp valid_progress?(engine) do
    is_map(engine.cursors) and
      Enum.all?(engine.cursors, fn {{person, key}, seq} ->
        owns_or_browser?(engine, person, key) and is_integer(seq) and seq >= 0 and
          seq <= engine.persons[person]
      end) and
      is_map(engine.conversation_cursors) and
      Enum.all?(engine.conversation_cursors, fn {{person, key}, seq} ->
        owns_or_browser?(engine, person, key) and is_integer(seq) and seq >= 0 and
          seq <= Map.get(engine.conversation_highs, person, 0)
      end) and
      is_struct(engine.conversation_opened, MapSet) and
      Enum.all?(engine.conversation_opened, fn {person, key} ->
        owns_or_browser?(engine, person, key)
      end)
  rescue
    _ -> false
  end

  defp load_progress(store) do
    case one(
           store.db,
           "SELECT version, payload, checksum FROM relay_device_progress WHERE id = 1 AND snapshot_version = ?",
           [store.version]
         ) do
      {:ok, nil} ->
        {:ok, store}

      {:ok, [version, payload, checksum]} ->
        with {:ok, progress} <- decode_checked_term(payload, checksum),
             true <-
               is_map(progress) and Enum.sort(Map.keys(progress)) == Enum.sort(@progress_fields),
             engine = Map.merge(store.engine, progress),
             true <- valid_progress?(engine) do
          {:ok, %{store | engine: engine, progress_version: version}}
        else
          _ -> {:error, :storage_unavailable}
        end

      _ ->
        {:error, :storage_unavailable}
    end
  end

  defp check_progress_version(store) do
    with {:ok, [snapshot_version]} <-
           one(store.db, "SELECT version FROM relay_state WHERE id = 1", []),
         true <- snapshot_version == store.version,
         {:ok, row} <-
           one(
             store.db,
             "SELECT version FROM relay_device_progress WHERE id = 1 AND snapshot_version = ?",
             [store.version]
           ),
         true <-
           (case row do
              nil -> 0
              [version] -> version
            end) == store.progress_version do
      :ok
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  defp write_progress(store, next) do
    payload = encode_term(Map.take(next, @progress_fields))

    with :ok <- check_progress_version(store) do
      run(
        store.db,
        "INSERT OR REPLACE INTO relay_device_progress (id, snapshot_version, version, payload, checksum) VALUES (1, ?, ?, ?, ?)",
        [
          store.version,
          store.progress_version + 1,
          {:blob, payload},
          {:blob, :crypto.hash(:sha256, payload)}
        ]
      )
    end
  end

  defp commit_progress(store, next, audit) do
    result =
      with :ok <- Sqlite3.execute(store.db, "BEGIN IMMEDIATE"),
           :ok <- write_progress(store, next),
           :ok <-
             run(
               store.db,
               "INSERT INTO relay_audit (person_id, actor_id, action, at_ms) VALUES (?, ?, ?, ?)",
               [audit.person, audit.actor, audit.action, System.system_time(:millisecond)]
             ),
           :ok <- Sqlite3.execute(store.db, "COMMIT") do
        {:ok, %{store | engine: next, progress_version: store.progress_version + 1}}
      end

    case result do
      {:ok, _} ->
        result

      error ->
        Diagnostics.failure(
          :server_storage,
          :state_commit,
          error,
          :outcome_unknown,
          operation_id(audit)
        )

        _ = Sqlite3.execute(store.db, "ROLLBACK")
        {:error, :storage_unavailable, store}
    end
  end

  defp load_or_create(db, upload_dir) do
    case one(db, "SELECT version, payload, checksum FROM relay_state WHERE id = 1", []) do
      {:ok, nil} ->
        engine = Engine.new()
        payload = Persistence.run(fn -> encode_state(engine) end)

        with :ok <-
               run(
                 db,
                 "INSERT OR IGNORE INTO relay_state (id, version, payload, checksum) VALUES (1, 0, ?, ?)",
                 [
                   {:blob, payload},
                   {:blob, :crypto.hash(:sha256, payload)}
                 ]
               ) do
          load_or_create(db, upload_dir)
        end

      {:ok, [version, payload, checksum]} when is_integer(version) and is_binary(payload) ->
        if :crypto.hash(:sha256, payload) == checksum do
          try do
            :ok = AttentionServer.Store.StoredTerms.load_atoms()

            case :erlang.binary_to_term(payload, [:safe]) do
              {:attention_relay_state, format, %Engine{} = stored}
              when format in [
                     1,
                     2,
                     3,
                     4,
                     5,
                     6,
                     8,
                     9,
                     10,
                     11,
                     12,
                     13,
                     14,
                     15,
                     16,
                     17,
                     18,
                     19,
                     20,
                     21,
                     22,
                     23,
                     24,
                     25,
                     26,
                     27,
                     28,
                     29
                   ] ->
                original = struct(Engine, Map.from_struct(stored))

                # A FORMAT27 store can have newer durable ACKs in the progress
                # table. Load them before normalization advances the snapshot
                # generation, and fold them into that same migration transaction.
                with {:ok, original_store} <-
                       load_progress(%__MODULE__{
                         db: db,
                         engine: original,
                         version: version,
                         upload_dir: upload_dir
                       }) do
                  original = original_store.engine

                  engine =
                    original
                    |> then(fn state ->
                      if format < 3, do: normalize_enrollments(state), else: state
                    end)
                    |> then(fn state ->
                      if format < 8, do: normalize_pending_v8(state), else: state
                    end)
                    |> then(fn state ->
                      if format < 9, do: normalize_agents_v9(state), else: state
                    end)
                    |> then(fn state ->
                      if format < 10, do: normalize_push_v10(state), else: state
                    end)
                    |> then(fn state ->
                      if format < 11, do: normalize_push_reasons_v11(state), else: state
                    end)
                    |> then(fn state ->
                      if format < 13, do: normalize_device_metadata(state), else: state
                    end)
                    |> then(fn state ->
                      if format < 15 do
                        invitations =
                          Map.new(state.join_invitations, fn {hash, item} ->
                            {hash, Map.put_new(item, :request_id, nil)}
                          end)

                        %{state | join_invitations: invitations}
                      else
                        state
                      end
                    end)
                    |> then(fn state ->
                      if format < 16, do: normalize_resumable_pending(state), else: state
                    end)
                    |> then(fn state ->
                      if format < 17, do: normalize_push_presentation(state), else: state
                    end)
                    |> then(fn state ->
                      if format < 19, do: normalize_conversation_push(state), else: state
                    end)
                    |> then(fn state ->
                      if format < 20, do: Map.put(state, :uploads, %{}), else: state
                    end)
                    |> then(fn state ->
                      if format < 21 do
                        now = System.system_time(:millisecond)

                        uploads =
                          Map.new(state.uploads, fn {id, upload} ->
                            {id, Map.put_new(upload, :last_activity_ms, now)}
                          end)

                        %{state | uploads: uploads}
                      else
                        state
                      end
                    end)
                    |> then(fn state ->
                      if format < 22, do: Map.put(state, :hosted_accounts, %{}), else: state
                    end)
                    |> then(fn state ->
                      if format < 23 do
                        %{state | algorithm_choices: %{}, jev_endpoint: nil, jev_model: nil}
                      else
                        state
                      end
                    end)
                    |> then(fn state ->
                      if format < 24, do: %{state | chat_read_positions: %{}}, else: state
                    end)
                    |> then(fn state ->
                      if format < 26 do
                        %{
                          state
                          | bounce_levels: %{},
                            bounce_revisions: %{},
                            opened_revisions: MapSet.new(),
                            advisory_feeds: %{}
                        }
                      else
                        state
                      end
                    end)
                    |> then(fn state ->
                      if format < 28, do: normalize_device_focus_permissions(state), else: state
                    end)
                    |> then(fn state ->
                      if format < 29, do: Map.put(state, :delivery_states, %{}), else: state
                    end)

                  engine =
                    engine
                    |> BounceDecision.rebuild_indexes()
                    |> Engine.rebuild_route_b_index()

                  cond do
                    not valid_state?(engine) ->
                      {:error, :invalid_state}

                    format == @format_version and
                        same_state_except_bounce_indexes?(engine, original) ->
                      {:ok, engine, version}

                    true ->
                      case commit_durable(
                             original_store,
                             engine,
                             %{
                               person: "system",
                               actor: "schema_migration",
                               action:
                                 cond do
                                   format < 3 -> "enrollment_revision_init"
                                   format < 4 -> "owner_state_init"
                                   format < 5 -> "pending_state_init"
                                   format < 6 -> "maintainer_state_init"
                                   format < 7 -> "pending_v8_readback_init"
                                   format < 9 -> "agent_authority_init"
                                   format < 10 -> "push_state_init"
                                   format < 11 -> "push_reason_sanitize"
                                   format < 12 -> "join_invitation_init"
                                   format < 13 -> "device_metadata_init"
                                   format < 14 -> "browser_sign_in_init"
                                   format < 15 -> "invitation_request_init"
                                   format < 16 -> "resumable_pending_init"
                                   format < 17 -> "push_presentation_init"
                                   format < 18 -> "conversation_feed_init"
                                   format < 23 -> "prioritization_init"
                                   format < 24 -> "chat_read_positions_init"
                                   format < 26 -> "bounce_state_init"
                                   format < 27 -> "bounce_schedule_index_init"
                                   format < 28 -> "device_focus_permissions_init"
                                   format < 29 -> "hosted_delivery_state_init"
                                   true -> "state_format"
                                 end
                             }
                           ) do
                        {:ok, migrated} -> {:ok, migrated.engine, migrated.version}
                        _ -> {:error, :migration_failed}
                      end
                  end
                end

              _ ->
                {:error, :invalid_state}
            end
          rescue
            error ->
              Diagnostics.failure(:server_storage, :store_open, error, :unavailable)
              {:error, :invalid_state}
          end
        else
          {:error, :invalid_checksum}
        end

      error ->
        error
    end
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

  defp all(db, sql, params) do
    statement(db, sql, params, fn stmt -> collect_rows(db, stmt, []) end)
  end

  defp load_large_data_references(db) do
    all(
      db,
      "SELECT path, marker, id, size, sha256 FROM relay_large_data_refs ORDER BY path",
      []
    )
  end

  defp sync_large_data_references(db, rows) do
    result =
      with :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
           :ok <- write_large_data_references(db, rows),
           :ok <- Sqlite3.execute(db, "COMMIT") do
        :ok
      end

    if result != :ok, do: Sqlite3.execute(db, "ROLLBACK")
    result
  end

  defp write_large_data_references(db, rows) do
    with :ok <- Sqlite3.execute(db, "DELETE FROM relay_large_data_refs") do
      write_reference_rows(rows, &insert_reference(db, &1))
    end
  end

  defp insert_reference(db, [path, marker, id, size, sha256]) do
    run(
      db,
      "INSERT INTO relay_large_data_refs (path, marker, id, size, sha256) VALUES (?, ?, ?, ?, ?)",
      [{:blob, path}, marker, id, size, sha256]
    )
  end

  defp insert_reference(_db, _row), do: {:error, :invalid_large_data_reference}

  defp collect_rows(db, stmt, acc) do
    case Sqlite3.step(db, stmt) do
      {:row, row} -> collect_rows(db, stmt, [row | acc])
      :done -> {:ok, Enum.reverse(acc)}
      error -> error
    end
  end

  defp run(db, sql, params) do
    case statement(db, sql, params, fn stmt -> Sqlite3.step(db, stmt) end) do
      :done -> :ok
      error -> error
    end
  end

  defp building_inbox_snapshot(db, snapshot) do
    with {:ok, row} <-
           one(
             db,
             "SELECT status FROM relay_inbox_snapshots WHERE person_id = ? AND device_id = ? AND snapshot_id = ?",
             [snapshot.person_id, snapshot.device_id, snapshot.snapshot_id]
           ) do
      case row do
        ["building"] -> {:ok, :building}
        _ -> {:error, :snapshot_unavailable}
      end
    end
  end

  defp insert_inbox_snapshot_chunk(db, snapshot, :items, offset, values) do
    if Enum.all?(values, fn
         {sender, id} -> is_binary(sender) and is_binary(id)
         _ -> false
       end) do
      insert_checked_chunk(db, "relay_inbox_item_pages", snapshot, offset, values)
    else
      {:error, :snapshot_unavailable}
    end
  end

  defp insert_inbox_snapshot_chunk(db, snapshot, :receipts, offset, values) do
    if Enum.all?(values, &(is_integer(&1) and &1 > 0)) do
      insert_checked_chunk(db, "relay_inbox_receipt_sequences", snapshot, offset, values)
    else
      {:error, :snapshot_unavailable}
    end
  end

  defp insert_inbox_snapshot_chunk(db, snapshot, :operation_ids, _offset, values) do
    if Enum.all?(values, &(is_binary(&1) and &1 != "")) do
      Enum.reduce_while(values, :ok, fn op_id, :ok ->
        case run(
               db,
               "INSERT INTO relay_inbox_operation_receipts (person_id, device_id, snapshot_id, op_id) VALUES (?, ?, ?, ?)",
               [snapshot.person_id, snapshot.device_id, snapshot.snapshot_id, op_id]
             ) do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end
      end)
    else
      {:error, :snapshot_unavailable}
    end
  end

  defp insert_inbox_snapshot_chunk(_db, _snapshot, _kind, _offset, _values),
    do: {:error, :snapshot_unavailable}

  defp insert_checked_chunk(db, table, snapshot, offset, values) do
    payload = encode_term(values)

    run(
      db,
      "INSERT INTO #{table} (person_id, device_id, snapshot_id, start_offset, payload, checksum) VALUES (?, ?, ?, ?, ?, ?)",
      [
        snapshot.person_id,
        snapshot.device_id,
        snapshot.snapshot_id,
        offset,
        {:blob, payload},
        {:blob, :crypto.hash(:sha256, payload)}
      ]
    )
  end

  defp delete_building_snapshot_rows(db) do
    with :ok <- delete_for_building_snapshots(db, "relay_inbox_receipt_pages"),
         :ok <- delete_for_building_snapshots(db, "relay_inbox_batches"),
         :ok <- delete_for_building_snapshots(db, "relay_inbox_operation_receipts"),
         :ok <- delete_for_building_snapshots(db, "relay_inbox_receipt_sequences"),
         :ok <- delete_for_building_snapshots(db, "relay_inbox_item_pages"),
         :ok <-
           run(
             db,
             "DELETE FROM relay_inbox_snapshots WHERE status = 'building'",
             []
           ) do
      :ok
    end
  end

  defp delete_for_building_snapshots(db, table) do
    run(
      db,
      "DELETE FROM #{table} WHERE snapshot_id IN (SELECT snapshot_id FROM relay_inbox_snapshots WHERE status = 'building')",
      []
    )
  end

  defp discard_inbox_snapshots_matching(db, where, params) do
    tables = [
      "relay_inbox_receipt_pages",
      "relay_inbox_batches",
      "relay_inbox_operation_receipts",
      "relay_inbox_receipt_sequences",
      "relay_inbox_item_pages"
    ]

    with :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
         :ok <-
           Enum.reduce_while(tables, :ok, fn table, :ok ->
             result =
               run(
                 db,
                 "DELETE FROM #{table} WHERE #{where}",
                 params
               )

             if result == :ok, do: {:cont, :ok}, else: {:halt, result}
           end),
         :ok <- run(db, "DELETE FROM relay_inbox_snapshots WHERE #{where}", params),
         :ok <- Sqlite3.execute(db, "COMMIT") do
      :ok
    else
      error ->
        _ = Sqlite3.execute(db, "ROLLBACK")
        error
    end
  end

  defp delete_snapshot_rows(db, snapshot_id) do
    with :ok <- delete_one_building_snapshot(db, "relay_inbox_receipt_pages", snapshot_id),
         :ok <- delete_one_building_snapshot(db, "relay_inbox_batches", snapshot_id),
         :ok <- delete_one_building_snapshot(db, "relay_inbox_operation_receipts", snapshot_id),
         :ok <- delete_one_building_snapshot(db, "relay_inbox_receipt_sequences", snapshot_id),
         :ok <- delete_one_building_snapshot(db, "relay_inbox_item_pages", snapshot_id),
         :ok <-
           run(
             db,
             "DELETE FROM relay_inbox_snapshots WHERE snapshot_id = ? AND status = 'building'",
             [snapshot_id]
           ) do
      :ok
    end
  end

  defp delete_one_building_snapshot(db, table, snapshot_id) do
    run(
      db,
      "DELETE FROM #{table} WHERE snapshot_id = ? AND snapshot_id IN (SELECT snapshot_id FROM relay_inbox_snapshots WHERE snapshot_id = ? AND status = 'building')",
      [snapshot_id, snapshot_id]
    )
  end

  defp read_receipt_sequences(_store, _snapshot, _offset, 0, acc),
    do: {:ok, Enum.reverse(acc)}

  defp read_receipt_sequences(%__MODULE__{db: db} = store, snapshot, offset, remaining, acc) do
    chunk_offset = div(offset, @inbox_index_chunk_size) * @inbox_index_chunk_size
    expected_count = min(@inbox_index_chunk_size, snapshot.receipt_count - chunk_offset)

    with {:ok, row} <-
           one(
             db,
             "SELECT payload, checksum FROM relay_inbox_receipt_sequences WHERE person_id = ? AND device_id = ? AND snapshot_id = ? AND start_offset = ?",
             [snapshot.person_id, snapshot.device_id, snapshot.snapshot_id, chunk_offset]
           ),
         {:ok, chunk} <- decode_inbox_index_chunk(row, expected_count, :receipts),
         inside_offset when inside_offset < length(chunk) <- offset - chunk_offset do
      count = min(remaining, length(chunk) - inside_offset)
      part = Enum.slice(chunk, inside_offset, count)

      read_receipt_sequences(
        store,
        snapshot,
        offset + count,
        remaining - count,
        Enum.reverse(part, acc)
      )
    else
      _ -> {:error, :snapshot_unavailable}
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

  defp decode_inbox_snapshot(nil), do: {:ok, nil}

  defp decode_inbox_snapshot([
         person,
         device,
         startup,
         id,
         since,
         high,
         status,
         item_count,
         receipt_count,
         operation_count,
         secret,
         receipt_next,
         receipts_complete
       ]) do
    with true <- valid_count?(item_count),
         true <- valid_count?(receipt_count),
         true <- valid_count?(operation_count),
         true <- is_binary(secret) and byte_size(secret) == 32 do
      {:ok,
       %{
         person_id: person,
         device_id: device,
         startup_id: startup,
         snapshot_id: id,
         since: since,
         high_water: high,
         status: status,
         item_count: item_count,
         receipt_count: receipt_count,
         operation_count: operation_count,
         cursor_secret: secret,
         receipt_next_offset: receipt_next,
         receipts_complete: receipts_complete == 1
       }}
    else
      _ -> {:error, :snapshot_unavailable}
    end
  end

  defp decode_inbox_snapshot(_), do: {:error, :snapshot_unavailable}

  defp decode_inbox_index_chunk([payload, checksum], expected_count, :items) do
    with {:ok, chunk} <- decode_checked_term(payload, checksum),
         true <- is_list(chunk) and length(chunk) == expected_count,
         true <-
           Enum.all?(chunk, fn
             {sender, id} -> is_binary(sender) and is_binary(id)
             _ -> false
           end) do
      {:ok, chunk}
    else
      _ -> {:error, :snapshot_unavailable}
    end
  end

  defp decode_inbox_index_chunk([payload, checksum], expected_count, :receipts) do
    with {:ok, chunk} <- decode_checked_term(payload, checksum),
         true <- is_list(chunk) and length(chunk) == expected_count,
         true <- Enum.all?(chunk, &(is_integer(&1) and &1 > 0)) do
      {:ok, chunk}
    else
      _ -> {:error, :snapshot_unavailable}
    end
  end

  defp decode_inbox_index_chunk(_, _expected_count, _kind),
    do: {:error, :snapshot_unavailable}

  defp valid_count?(value), do: is_integer(value) and value >= 0

  defp decode_inbox_batch(nil), do: {:ok, nil}

  defp decode_inbox_batch([digest, result, checksum]) do
    with {:ok, decoded} <- decode_checked_term(result, checksum),
         true <- is_binary(digest) and byte_size(digest) == 32 do
      {:ok, %{digest: digest, result: decoded}}
    else
      _ -> {:error, :snapshot_unavailable}
    end
  end

  defp decode_inbox_batch(_), do: {:error, :snapshot_unavailable}

  defp decode_inbox_receipt_page(nil), do: {:ok, nil}

  defp decode_inbox_receipt_page([result, checksum]),
    do: decode_checked_term(result, checksum)

  defp decode_inbox_receipt_page(_), do: {:error, :snapshot_unavailable}

  defp existing_or_insert_receipt_page(
         db,
         existing,
         person,
         device,
         snapshot_id,
         offset,
         next_offset,
         result,
         payload,
         checksum
       ) do
    case decode_inbox_receipt_page(existing) do
      {:ok, value} when not is_nil(value) ->
        {:ok, value}

      {:ok, nil} ->
        insert_inbox_receipt_page(
          db,
          person,
          device,
          snapshot_id,
          offset,
          next_offset,
          result,
          payload,
          checksum
        )

      error ->
        error
    end
  end

  defp insert_inbox_receipt_page(
         db,
         person,
         device,
         snapshot_id,
         offset,
         next_offset,
         result,
         payload,
         checksum
       ) do
    with {:ok, [expected_offset]} <-
           one(
             db,
             "SELECT receipt_next_offset FROM relay_inbox_snapshots WHERE person_id = ? AND device_id = ? AND snapshot_id = ? AND status != 'replaced'",
             [person, device, snapshot_id]
           ),
         true <- expected_offset == offset,
         :ok <-
           run(
             db,
             "INSERT INTO relay_inbox_receipt_pages (person_id, device_id, snapshot_id, start_offset, next_offset, result, result_checksum) VALUES (?, ?, ?, ?, ?, ?, ?)",
             [
               person,
               device,
               snapshot_id,
               offset,
               next_offset,
               {:blob, payload},
               {:blob, checksum}
             ]
           ),
         :ok <-
           run(
             db,
             "UPDATE relay_inbox_snapshots SET receipt_next_offset = ?, receipts_complete = ? WHERE person_id = ? AND device_id = ? AND snapshot_id = ? AND status != 'replaced'",
             [
               if(is_nil(next_offset), do: offset + length(result["events"]), else: next_offset),
               if(is_nil(next_offset), do: 1, else: 0),
               person,
               device,
               snapshot_id
             ]
           ) do
      {:ok, result}
    else
      false -> {:error, :invalid_page_cursor}
      {:ok, nil} -> {:error, :invalid_snapshot}
      error -> error
    end
  end

  defp checkpoint_ready(_db, nil, _person, _device, _snapshot_id, _high, _batches),
    do: {:error, :invalid_snapshot}

  defp checkpoint_ready(
         db,
         [status, high, receipts_complete],
         person,
         device,
         snapshot_id,
         high,
         batches
       ) do
    with {:ok, rows} <-
           all(
             db,
             "SELECT batch_id FROM relay_inbox_batches WHERE person_id = ? AND device_id = ? AND snapshot_id = ? ORDER BY batch_id",
             [person, device, snapshot_id]
           ),
         true <- Enum.map(rows, &hd/1) == Enum.sort(batches) do
      case {status, receipts_complete} do
        {"accepted", _} -> {:ok, true}
        {"pending", 1} -> {:ok, false}
        {"pending", _} -> {:error, :checkpoint_pending}
        {"replaced", _} -> {:error, :snapshot_replaced}
        _ -> {:error, :snapshot_unavailable}
      end
    else
      false -> {:error, :checkpoint_conflict}
      error -> error
    end
  end

  defp checkpoint_ready(
         _db,
         [_status, _saved_high, _complete],
         _person,
         _device,
         _snapshot_id,
         _high,
         _batches
       ),
       do: {:error, :checkpoint_conflict}

  defp checkpoint_commit(store, next_engine, person, device, snapshot_id, audit) do
    with :ok <- write_progress(store, next_engine),
         :ok <- accept_checkpoint_row(store.db, person, device, snapshot_id),
         do: write_audit(store.db, audit)
  end

  defp accept_checkpoint_row(db, person, device, snapshot_id) do
    with :ok <-
           run(
             db,
             "UPDATE relay_inbox_snapshots SET status = 'replaced' WHERE person_id = ? AND device_id = ? AND status IN ('pending', 'accepted') AND snapshot_id != ?",
             [person, device, snapshot_id]
           ),
         :ok <-
           run(
             db,
             "UPDATE relay_inbox_snapshots SET status = 'accepted' WHERE person_id = ? AND device_id = ? AND snapshot_id = ? AND status = 'pending'",
             [person, device, snapshot_id]
           ),
         {:ok, 1} <- Sqlite3.changes(db) do
      :ok
    else
      {:ok, 0} -> {:error, :snapshot_unavailable}
      error -> error
    end
  end

  defp decode_checked_term(payload, checksum)
       when is_binary(payload) and is_binary(checksum) do
    if :crypto.hash(:sha256, payload) == checksum do
      try do
        {:ok, :erlang.binary_to_term(payload, [:safe])}
      rescue
        _ -> {:error, :snapshot_unavailable}
      end
    else
      {:error, :snapshot_unavailable}
    end
  end

  defp decode_checked_term(_, _), do: {:error, :snapshot_unavailable}

  defp encode_term(value), do: :erlang.term_to_binary(value, [{:compressed, 6}])

  defp valid_free_order?(engine) do
    is_map(engine.free_order_highs) and
      Enum.all?(engine.free_order_highs, fn {person, high} ->
        valid_order_high?(engine, person, high)
      end) and
      Enum.all?(engine.messages, fn {{person, _, _}, record} ->
        valid_arrival?(record, Map.get(engine.free_order_highs, person, 0))
      end) and
      Enum.all?(engine.conversation_events, fn {{person, _}, event} ->
        valid_arrival?(event, Map.get(engine.free_order_highs, person, 0))
      end)
  end

  defp valid_order_high?(engine, person, high),
    do: Map.has_key?(engine.persons, person) and is_integer(high) and high >= 0

  defp valid_arrival?(record, high) when is_map(record) do
    case Map.get(record, :free_order) do
      nil -> true
      order -> is_integer(order) and order > 0 and order <= high
    end
  end

  defp valid_arrival?(_, _), do: false

  defp encode_state(engine) do
    persisted =
      engine
      |> Map.from_struct()
      |> Map.delete(:open_default_index)
      |> Map.delete(:advisory_id_index)
      |> Map.delete(:route_b_index)
      |> Map.delete(:touched_keys)
      |> Map.put(:__struct__, Engine)

    :erlang.term_to_binary({:attention_relay_state, @format_version, persisted}, [
      {:compressed, 6}
    ])
  end

  defp normalize_enrollments(engine) do
    devices =
      Map.new(engine.devices, fn {key, record} ->
        {key, Map.put_new(record, :enrollment_revision, 1)}
      end)

    agents =
      Map.new(engine.agents, fn {key, record} ->
        {key, Map.put_new(record, :enrollment_revision, 1)}
      end)

    %{engine | devices: devices, agents: agents}
  end

  defp normalize_device_metadata(engine) do
    devices =
      Map.new(engine.devices, fn {key, record} ->
        {key, record |> Map.put_new(:label, nil) |> Map.put_new(:enrolled_at, nil)}
      end)

    %{engine | devices: devices}
  end

  defp normalize_device_focus_permissions(engine) do
    devices =
      Map.new(engine.devices, fn {key, record} ->
        {key, record |> Map.put_new(:see_focus, false) |> Map.put_new(:push_view, false)}
      end)

    %{engine | devices: devices}
  end

  defp normalize_agents_v9(engine) do
    agents =
      Map.new(engine.agents, fn {key, record} ->
        migrated =
          record
          |> Map.put_new(:read_grants, MapSet.new())
          |> Map.put_new(:respond_grants, MapSet.new())
          |> Map.put_new(:grant_revision, 1)
          |> Map.put_new(:granted_by, nil)

        history = [
          %{
            revision: 1,
            actor: nil,
            read_grants: migrated.read_grants,
            respond_grants: migrated.respond_grants,
            revoked: migrated.revoked,
            action: "fixture_migration",
            at_ms: System.system_time(:millisecond)
          }
        ]

        {key, Map.put_new(migrated, :grant_history, history)}
      end)

    %{engine | agents: agents, agent_cursor_key: :crypto.strong_rand_bytes(32)}
  end

  defp normalize_push_v10(engine),
    do: %{engine | push_bindings: Map.get(engine, :push_bindings, %{})}

  defp normalize_push_reasons_v11(engine) do
    bindings =
      Map.new(engine.push_bindings, fn {key, binding} ->
        case Map.get(binding, :last_outcome) do
          outcome when is_map(outcome) ->
            sanitized =
              case Map.fetch(outcome, "reason") do
                {:ok, reason} ->
                  if APNS.safe_reason?(reason), do: outcome, else: Map.delete(outcome, "reason")

                :error ->
                  outcome
              end

            {key, Map.put(binding, :last_outcome, sanitized)}

          _ ->
            {key, binding}
        end
      end)

    %{engine | push_bindings: bindings}
  end

  defp normalize_push_presentation(engine) do
    bindings =
      Map.new(engine.push_bindings, fn {{person, _device} = key, binding} ->
        # Completed old hints have no pending presentation to preserve. For an
        # in-flight old hint, retain the unresolved-delivery alert possibility.
        accepted_high =
          if binding.pending_high == 0,
            do: engine.persons[person],
            else: Map.get(engine.cursors, key, 0)

        {key, Map.put(binding, :accepted_high, accepted_high)}
      end)

    %{engine | push_bindings: bindings}
  end

  defp normalize_conversation_push(engine) do
    bindings =
      Map.new(engine.push_bindings, fn {key, binding} ->
        {key,
         binding
         |> Map.put_new(:pending_conversation_high, 0)
         |> Map.put_new(:accepted_conversation_high, 0)
         |> Map.put_new(:claim_conversation_high, nil)}
      end)

    %{engine | push_bindings: bindings}
  end

  # Reviewed format-six snapshots predate bounded S8 readback. Keep their
  # immutable request deadline and conservatively deny old approval retries
  # because their approving owner key was never recorded.
  defp normalize_pending_v8(engine) do
    pending =
      Map.new(engine.pending_enrollments, fn {code, item} ->
        {code,
         item
         |> Map.put(:status_expires_at, item.expires_at + 86_400_000)
         |> then(fn request ->
           if request.state == :approved,
             do: Map.put(request, :approved_by, nil),
             else: request
         end)}
      end)

    %{engine | pending_enrollments: pending, pending_history: %{}}
  end

  # A legacy expired receipt lacks person and certificate. Recover person only
  # from one exact consumed invitation; resume must supply the same-SPKI cert.
  defp normalize_resumable_pending(engine) do
    pending =
      Map.new(engine.pending_enrollments, fn {code, item} ->
        {code,
         item
         |> Map.put_new(:request_generation, 1)
         |> Map.put_new(:joined, item.state == :pending and not is_nil(item.person))}
      end)

    {pending, history} =
      Enum.reduce(engine.pending_history, {pending, %{}}, fn {code, item}, {current, past} ->
        matches =
          Enum.filter(engine.join_invitations, fn {_hash, invite} ->
            invite.code == code and invite.key == item.key and
              invite.secret_hash == item.secret_hash and
              Map.has_key?(engine.persons, invite.person)
          end)

        key_unbound? =
          not Map.has_key?(engine.devices, item.key) and
            not Map.has_key?(engine.agents, item.key) and
            not MapSet.member?(engine.retired_owner_keys, item.key) and
            not Enum.any?(engine.owners, fn {_person, owner} -> owner.key == item.key end) and
            not Enum.any?(current, fn {_pending_code, pending_item} ->
              pending_item.key == item.key
            end)

        case {item.state, matches, key_unbound?} do
          {:expired, [{_hash, invite}], true} ->
            recovered = %{
              state: :pending,
              person: invite.person,
              key: item.key,
              certificate: nil,
              legacy_recovery: true,
              label: "New device",
              secret_hash: item.secret_hash,
              expires_at: item.expires_at,
              status_expires_at: item.status_expires_at,
              request_generation: 1,
              joined: true
            }

            {Map.put(current, code, recovered), past}

          {:approved, [{_hash, _invite}], _} ->
            {current, Map.put(past, code, Map.put(item, :joined, true))}

          {state, [{_hash, _invite}], _} when state in [:cancelled, :declined] ->
            {current, Map.put(past, code, Map.put(item, :joined, true))}

          _ ->
            {current, Map.put(past, code, item)}
        end
      end)

    %{engine | pending_enrollments: pending, pending_history: history}
  end

  # The durable hot path checks bounce index containers without rescanning their
  # retained rows. Store open performs the full cross-map bounce validation.
  defp valid_state?(engine), do: valid_state?(engine, :full)

  defp valid_state?(engine, bounce_validation), do: valid_state?(engine, bounce_validation, %{})

  defp valid_state?(%Engine{} = engine, bounce_validation, previous_messages) do
    try do
      valid_instance?(engine.relay_instance_id) and
        valid_cursor_key?(engine.agent_cursor_key) and
        is_map(engine.open_default_index) and
        Enum.all?(engine.open_default_index, fn {key, at} ->
          is_tuple(key) and tuple_size(key) == 3 and is_integer(at)
        end) and
        is_integer(engine.pending_clock_high_water) and engine.pending_clock_high_water >= 0 and
        Enum.all?(engine.persons, fn {person, high} ->
          is_binary(person) and is_integer(high) and high >= 0
        end) and
        valid_owners?(engine) and
        valid_pending?(engine) and
        valid_pending_history?(engine) and
        GuidedJoin.valid_state?(engine) and
        BrowserSignIn.valid_state?(engine) and
        valid_maintainers?(engine) and
        valid_hosted_accounts?(engine) and
        valid_delivery_states?(engine) and
        valid_prioritization?(engine) and
        valid_push_bindings?(engine) and
        AttentionServer.Conversation.valid_state?(engine) and
        BounceDecision.valid_state?(engine, bounce_validation) and
        valid_uploads?(engine) and
        valid_upload_references?(engine) and
        LargeData.valid_refs?(engine) and
        Enum.all?(engine.devices, fn {_, device} -> valid_device?(engine, device) end) and
        Enum.all?(engine.agents, fn {key, agent} -> valid_agent?(engine, key, agent) end) and
        engine.agents |> Map.values() |> Enum.map(&{&1.person, &1.id}) |> Enum.uniq() |> length() ==
          map_size(engine.agents) and
        Enum.all?(engine.sender_grants, fn {{person, key}, _} -> owns?(engine, person, key) end) and
        Enum.all?(engine.subscriptions, fn {{person, key}, _} -> owns?(engine, person, key) end) and
        Enum.all?(engine.cursors, fn {{person, key}, seq} ->
          owns_or_browser?(engine, person, key) and is_integer(seq) and seq >= 0 and
            seq <= engine.persons[person]
        end) and
        valid_free_order?(engine) and
        is_map(engine.conversation_highs) and
        Enum.all?(engine.conversation_highs, fn {person, high} ->
          Map.has_key?(engine.persons, person) and is_integer(high) and high >= 0
        end) and
        is_map(engine.conversation_events) and
        Enum.all?(engine.conversation_events, fn {{person, seq}, event} ->
          is_integer(seq) and seq > 0 and seq <= Map.get(engine.conversation_highs, person, 0) and
            event.kind == :conversation and elem(event.key, 0) == person and
            Map.has_key?(engine.messages, event.key)
        end) and
        map_size(engine.conversation_events) ==
          Enum.reduce(engine.conversation_highs, 0, fn {_person, high}, sum -> sum + high end) and
        is_map(engine.chat_read_positions) and
        Enum.all?(engine.chat_read_positions, fn {key, position} ->
          valid_chat_position?(engine, key, position)
        end) and
        is_map(engine.reply_group_history) and
        Enum.all?(engine.reply_group_history, fn {key, history} ->
          valid_reply_history?(engine, key, history)
        end) and
        is_map(engine.conversation_cursors) and
        Enum.all?(engine.conversation_cursors, fn {{person, key}, seq} ->
          owns_or_browser?(engine, person, key) and is_integer(seq) and seq >= 0 and
            seq <= Map.get(engine.conversation_highs, person, 0)
        end) and
        is_struct(engine.conversation_opened, MapSet) and
        Enum.all?(engine.conversation_opened, fn {person, key} ->
          owns_or_browser?(engine, person, key)
        end) and
        Enum.all?(engine.receipts, fn {{person, key, _op}, _} ->
          actor_owns?(engine, person, key)
        end) and
        Enum.all?(engine.source_report_grants, fn {{person, key, source}, grant} ->
          owns?(engine, person, key) and nonempty?(source) and nonempty?(grant.delegation_ref) and
            is_boolean(grant.revoked) and is_struct(grant.senders, MapSet) and
            Enum.all?(grant.senders, &AttentionServer.Message.sender_id?/1)
        end) and
        Enum.all?(engine.enrollment_read_grants, fn {id, grant} ->
          valid_enrollment_read_grant?(engine, id, grant)
        end) and
        Enum.all?(engine.messages, fn {key, record} ->
          valid_item?(engine, key, record) and terminal_unchanged?(previous_messages, key, record)
        end) and
        unique_source_bindings?(engine.messages) and
        Enum.all?(engine.events, fn {{person, seq}, event} ->
          valid_seq?(engine, person, seq) and elem(event.key, 0) == person and
            event.kind in [:delivery, :lifecycle] and Map.has_key?(engine.messages, event.key)
        end) and
        map_size(engine.events) ==
          Enum.reduce(engine.persons, 0, fn {_person, high}, sum -> sum + high end)
    rescue
      _ -> false
    end
  end

  defp valid_state?(_, _, _), do: false

  defp same_state_except_bounce_indexes?(loaded, stored) do
    %{
      loaded
      | bounce_deadline_index: stored.bounce_deadline_index,
        bounce_waiting_routes: stored.bounce_waiting_routes,
        bounce_discussed_messages: stored.bounce_discussed_messages,
        bounce_origin_entries: stored.bounce_origin_entries,
        advisory_id_index: stored.advisory_id_index,
        route_b_index: stored.route_b_index
    } == stored
  end

  defp valid_prioritization?(engine) do
    is_map(engine.algorithm_choices) and
      Enum.all?(engine.algorithm_choices, fn {person, choice} ->
        valid_algorithm_choice?(engine, person, choice)
      end) and
      valid_jev_endpoint?(engine.jev_endpoint) and valid_jev_model?(engine.jev_model)
  end

  defp valid_algorithm_choice?(engine, person, choice),
    do: Map.has_key?(engine.persons, person) and choice in ["baseline", "jev-set"]

  defp valid_jev_endpoint?(nil), do: true
  defp valid_jev_endpoint?(value), do: AttentionServer.JevClient.valid_endpoint?(value)

  defp valid_jev_model?(nil), do: true
  defp valid_jev_model?(value), do: AttentionServer.JevClient.valid_model?(value)

  defp valid_cursor_key?(key), do: is_binary(key) and byte_size(key) == 32

  defp valid_chat_position?(engine, {person, sender, id} = key, position) do
    Map.has_key?(engine.persons, person) and Map.has_key?(engine.messages, key) and
      is_binary(sender) and is_binary(id) and is_integer(position) and position > 0 and
      origin_reply_at?(engine, key, position)
  end

  defp valid_chat_position?(_, _, _), do: false

  defp valid_reply_history?(engine, key, %{scores: scores, score: score}) when is_map(scores) do
    Map.has_key?(engine.messages, key) and valid_reply_score?(score) and
      Enum.all?(scores, fn {seq, score} ->
        valid_reply_score?(score) and seq > Map.get(engine.chat_read_positions, key, 0) and
          origin_reply_at?(engine, key, seq)
      end)
  end

  defp valid_reply_history?(_, _, _), do: false

  defp valid_reply_score?(score), do: is_number(score) and score >= 0 and score <= 1

  defp origin_reply_at?(engine, {person, _, _} = key, seq) do
    match?(
      %{key: ^key, payload: %{"kind" => "reply", "author" => "origin"}},
      Map.get(engine.conversation_events, {person, seq})
    )
  end

  defp valid_uploads?(engine) do
    is_map(engine.uploads) and
      Enum.all?(engine.uploads, fn {id, upload} -> valid_upload?(engine, id, upload) end)
  end

  defp valid_upload?(engine, id, upload) do
    AttentionServer.Message.uuid_v4?(id) and upload.id == id and
      Map.has_key?(engine.persons, upload.person) and
      owns_or_browser?(engine, upload.person, upload.uploader) and
      AttentionServer.Message.uuid?(upload.op_id) and
      AttentionServer.Message.sender_id?(upload.sender_id) and
      is_binary(upload.message_id) and upload.message_id != "" and
      is_integer(upload.revision) and upload.revision > 0 and
      Map.get(upload, :uploader_kind) in [nil, "agent", "client"] and
      AttentionServer.ConversationFiles.valid_name?(upload.name) and
      is_integer(upload.size) and upload.size >= 0 and
      is_integer(upload.last_activity_ms) and upload.last_activity_ms >= 0 and
      is_binary(upload.sha256) and
      Regex.match?(~r/\A[0-9a-f]{64}\z/, upload.sha256)
  end

  defp valid_upload_references?(engine) do
    Enum.all?(engine.origin_feeds, fn {{person, _origin}, feed} ->
      Enum.all?(feed.entries, fn {_seq, entry} -> valid_entry_uploads?(engine, person, entry) end)
    end)
  end

  defp valid_entry_uploads?(engine, person, entry) do
    Enum.all?(Map.get(entry, "files", []), fn file ->
      case file["tier"] do
        "inline" ->
          true

        "upload" ->
          case Map.get(engine.uploads, file["upload_id"]) do
            %{person: ^person} = upload ->
              upload.op_id == entry["op_id"] and
                upload.sender_id == entry["sender_id"] and
                upload.message_id == entry["message_id"] and
                upload.revision == entry["request_revision"] and
                upload.name == file["name"] and upload.size == file["size"] and
                upload.sha256 == file["sha256"]

            _ ->
              false
          end

        _ ->
          false
      end
    end)
  end

  defp valid_conversation_record?(record) do
    handle = Map.get(record, :session_handle)
    vendor = Map.get(record, :vendor_session_id)
    revision = Map.get(record, :revision, 1)
    revisions = Map.get(record, :revisions, %{1 => record.message})
    files_by_revision = Map.get(record, :files_by_revision, %{})

    case handle do
      nil ->
        is_nil(vendor) and revision == 1

      _ ->
        AttentionServer.Conversation.valid_session?(handle, vendor) and
          is_integer(revision) and revision >= 1 and is_map(revisions) and
          is_map(files_by_revision) and
          Enum.all?(files_by_revision, fn {n, files} ->
            is_integer(n) and n >= 1 and n <= revision and
              AttentionServer.ConversationFiles.valid_stored?(files)
          end) and
          map_size(revisions) == revision and revisions[revision] == record.message and
          Enum.all?(1..revision, fn n ->
            case Map.fetch(revisions, n) do
              {:ok, message} ->
                AttentionServer.Message.validate(message) == :ok and
                  message["sender_id"] == record.message["sender_id"] and
                  message["message_id"] == record.message["message_id"]

              :error ->
                false
            end
          end)
    end
  end

  defp valid_origin_result?(record) do
    case Map.get(record, :origin_result) do
      nil ->
        true

      result ->
        is_binary(Map.get(record, :session_handle)) and
          match?(%{state: "answered"}, record.terminal) and
          result.terminal_seq == record.terminal.seq and
          result.status in ~w(received applied refused) and
          is_integer(result.revision) and result.revision > 0 and
          AttentionServer.Message.uuid_v4?(result.result_ref)
    end
  end

  defp valid_push_bindings?(engine) do
    bindings_valid? =
      Enum.all?(engine.push_bindings, fn
        {{person, device}, binding} when is_map(binding) ->
          owns?(engine, person, device) and valid_push_binding?(binding, engine, person, device)

        _ ->
          false
      end)

    addresses =
      for {_key, %{active: true, address: address}} <- engine.push_bindings, do: address

    bindings_valid? and length(addresses) == MapSet.size(MapSet.new(addresses))
  end

  defp valid_push_binding?(binding, engine, person, device) do
    active = Map.get(binding, :active)
    revision = Map.get(binding, :revision)
    binding_id = Map.get(binding, :binding_id)
    address = Map.get(binding, :address)
    state = Map.get(binding, :state)
    pending_high = Map.get(binding, :pending_high)
    accepted_high = Map.get(binding, :accepted_high)
    pending_conversation_high = Map.get(binding, :pending_conversation_high)
    accepted_conversation_high = Map.get(binding, :accepted_conversation_high)
    deadline_at = Map.get(binding, :deadline_at)
    next_at = Map.get(binding, :next_at)
    attempt_count = Map.get(binding, :attempt_count)
    claim_id = Map.get(binding, :claim_id)
    claim_high = Map.get(binding, :claim_high)
    claim_conversation_high = Map.get(binding, :claim_conversation_high)
    claim_at = Map.get(binding, :claim_at)

    is_boolean(active) and is_integer(revision) and revision > 0 and
      revision < 9_223_372_036_854_775_808 and
      state in ~w(ready queued sending waiting_retry blocked_configuration expired canceled endpoint_invalid) and
      is_integer(pending_high) and pending_high >= 0 and pending_high <= engine.persons[person] and
      is_integer(accepted_high) and accepted_high >= 0 and
      accepted_high <= engine.persons[person] and
      is_integer(pending_conversation_high) and pending_conversation_high >= 0 and
      pending_conversation_high <= Map.get(engine.conversation_highs, person, 0) and
      is_integer(accepted_conversation_high) and accepted_conversation_high >= 0 and
      accepted_conversation_high <= Map.get(engine.conversation_highs, person, 0) and
      (is_nil(deadline_at) or is_integer(deadline_at)) and
      (is_nil(next_at) or is_integer(next_at)) and
      is_integer(attempt_count) and attempt_count >= 0 and
      (is_nil(binding.last_attempt_at) or is_integer(binding.last_attempt_at)) and
      valid_push_claim?(state, claim_id, claim_high, claim_conversation_high, claim_at) and
      valid_push_outcome?(Map.get(binding, :last_outcome)) and
      if(active,
        do:
          not get_in(engine.devices, [device, :revoked]) and
            binding_id?(binding_id) and APNS.profile_id?(Map.get(binding, :profile_id)) and
            is_binary(address) and Regex.match?(~r/\A[0-9a-f]{64}\z/, address) and
            is_integer(binding.registered_at) and binding.registered_at > 0 and
            ((pending_high == 0 and pending_conversation_high == 0) or is_integer(deadline_at)) and
            state not in ["canceled", "endpoint_invalid"],
        else:
          (state == "canceled" and is_nil(binding_id) and is_nil(address) and
             is_nil(Map.get(binding, :profile_id)) and is_nil(Map.get(binding, :registered_at)) and
             pending_high == 0 and pending_conversation_high == 0 and is_nil(deadline_at) and
             is_nil(next_at)) or
            (state == "endpoint_invalid" and binding_id?(binding_id) and is_nil(address) and
               APNS.profile_id?(Map.get(binding, :profile_id)) and
               is_integer(binding.registered_at) and pending_high == 0 and
               pending_conversation_high == 0 and is_nil(deadline_at))
      )
  rescue
    _ -> false
  end

  defp valid_push_claim?("sending", claim_id, claim_high, claim_conversation_high, claim_at),
    do:
      AttentionServer.Message.uuid_v4?(claim_id) and is_integer(claim_high) and claim_high >= 0 and
        is_integer(claim_conversation_high) and claim_conversation_high >= 0 and
        (claim_high > 0 or claim_conversation_high > 0) and is_integer(claim_at)

  defp valid_push_claim?(_state, claim_id, claim_high, claim_conversation_high, claim_at),
    do:
      is_nil(claim_id) and is_nil(claim_high) and is_nil(claim_conversation_high) and
        is_nil(claim_at)

  defp valid_push_outcome?(nil), do: true

  defp valid_push_outcome?(%{"provider" => "apns", "state" => state} = outcome) do
    state in ~w(provider_accepted endpoint_invalid retry blocked_configuration expired) and
      Enum.all?(outcome, fn
        {"provider", "apns"} -> true
        {"state", value} -> value == state
        {"http_status", value} -> is_integer(value) and value in 100..599
        {"reason", value} -> APNS.safe_reason?(value)
        {"request_id", value} -> AttentionServer.Message.uuid_v4?(value)
        _ -> false
      end)
  end

  defp valid_push_outcome?(_), do: false

  defp binding_id?(value), do: AttentionServer.Message.uuid_v4?(value)

  defp valid_device?(engine, device) do
    Map.has_key?(engine.persons, device.person) and
      is_integer(device.enrollment_revision) and device.enrollment_revision > 0 and
      (is_nil(device.label) or (is_binary(device.label) and byte_size(device.label) >= 1)) and
      (is_nil(device.enrolled_at) or (is_integer(device.enrolled_at) and device.enrolled_at >= 0))
  end

  defp valid_agent?(engine, key, agent) do
    hex_key?(key) and
      Map.has_key?(engine.persons, agent.person) and
      not Map.has_key?(engine.devices, key) and
      AttentionServer.Message.uuid_v4?(agent.id) and
      is_boolean(agent.revoked) and is_struct(agent.senders, MapSet) and
      is_integer(agent.enrollment_revision) and agent.enrollment_revision > 0 and
      Enum.all?(agent.senders, &AttentionServer.Message.sender_id?/1) and
      is_struct(agent.read_grants, MapSet) and is_struct(agent.respond_grants, MapSet) and
      is_integer(agent.grant_revision) and agent.grant_revision > 0 and
      (is_nil(agent.granted_by) or hex_key?(agent.granted_by)) and
      valid_agent_history?(agent) and
      AttentionServer.AgentAccess.valid_patterns?(MapSet.to_list(agent.read_grants)) and
      AttentionServer.AgentAccess.valid_patterns?(MapSet.to_list(agent.respond_grants))
  end

  defp valid_agent_history?(agent) do
    history = agent.grant_history

    is_list(history) and length(history) == agent.grant_revision and
      Enum.with_index(history, 1)
      |> Enum.all?(fn {entry, revision} ->
        entry.revision == revision and
          (is_nil(entry.actor) or hex_key?(entry.actor)) and
          entry.action in ~w(register replace revoke fixture fixture_revoke fixture_migration) and
          is_integer(entry.at_ms) and entry.at_ms > 0 and is_boolean(entry.revoked) and
          is_struct(entry.read_grants, MapSet) and is_struct(entry.respond_grants, MapSet) and
          AttentionServer.AgentAccess.valid_patterns?(MapSet.to_list(entry.read_grants)) and
          AttentionServer.AgentAccess.valid_patterns?(MapSet.to_list(entry.respond_grants))
      end) and
      case List.last(history) do
        %{actor: actor, read_grants: read, respond_grants: respond, revoked: revoked} ->
          actor == agent.granted_by and read == agent.read_grants and
            respond == agent.respond_grants and revoked == agent.revoked

        _ ->
          false
      end
  end

  defp valid_owners?(engine) do
    owners = engine.owners
    retired = engine.retired_owner_keys

    is_map(owners) and is_struct(retired, MapSet) and
      Enum.all?(owners, fn {person, owner} -> valid_owner?(engine, person, owner) end) and
      owners |> Map.values() |> Enum.map(& &1.key) |> Enum.uniq() |> length() == map_size(owners) and
      Enum.all?(retired, fn key ->
        hex_key?(key) and not Map.has_key?(engine.devices, key) and
          not Map.has_key?(engine.agents, key)
      end)
  end

  defp valid_owner?(engine, person, owner) do
    Map.has_key?(engine.persons, person) and
      hex_key?(owner.key) and is_integer(owner.revision) and owner.revision > 0 and
      not Map.has_key?(engine.devices, owner.key) and
      not Map.has_key?(engine.agents, owner.key) and
      not MapSet.member?(engine.retired_owner_keys, owner.key)
  end

  defp valid_pending?(engine) do
    pending = engine.pending_enrollments

    is_map(pending) and is_map(engine.pending_history) and
      Enum.all?(pending, fn {code, item} -> valid_pending_item?(engine, code, item) end) and
      pending |> Map.values() |> Enum.map(& &1.secret_hash) |> Enum.uniq() |> length() ==
        map_size(pending)
  end

  defp valid_pending_item?(engine, code, item) do
    is_binary(code) and Regex.match?(~r/\A[A-Z2-7]{10}\z/, code) and
      hex_key?(item.key) and
      ((is_binary(item.certificate) and
          AttentionServer.DeviceIdentity.from_certificate(item.certificate) == {:ok, item.key}) or
         (is_nil(item.certificate) and Map.get(item, :legacy_recovery) == true and
            not is_nil(item.person))) and
      is_binary(item.label) and byte_size(item.label) >= 1 and
      is_binary(item.secret_hash) and byte_size(item.secret_hash) == 32 and
      valid_pending_deadlines?(item.expires_at, item.status_expires_at) and
      is_integer(item.request_generation) and item.request_generation > 0 and
      Map.get(item, :joined, false) in [true, false] and
      case item do
        %{state: :pending, person: nil} ->
          true

        %{state: :pending, person: person} ->
          Map.has_key?(engine.persons, person)

        %{state: :approved, person: person, key: key} ->
          get_in(engine.devices, [key, :person]) == person and
            (is_nil(Map.get(item, :approved_by)) or
               hex_key?(item.approved_by))

        _ ->
          false
      end
  end

  # Current requests carry no deadlines; requests stored earlier keep theirs,
  # which are no longer enforced.
  defp valid_pending_deadlines?(nil, nil), do: true

  defp valid_pending_deadlines?(expires_at, status_expires_at),
    do:
      is_integer(expires_at) and expires_at > 0 and
        status_expires_at == expires_at + 86_400_000

  defp valid_pending_history?(engine) do
    history = engine.pending_history

    if is_map(history) do
      secrets =
        Enum.map(Map.values(engine.pending_enrollments), & &1.secret_hash) ++
          Enum.map(Map.values(history), & &1.secret_hash)

      Enum.all?(history, fn {code, receipt} ->
        valid_pending_resolution?(engine, code, receipt)
      end) and
        Enum.all?(history, fn {code, _} -> not Map.has_key?(engine.pending_enrollments, code) end) and
        length(Enum.uniq(secrets)) == length(secrets)
    else
      false
    end
  end

  defp valid_pending_resolution?(engine, code, receipt) do
    is_binary(code) and Regex.match?(~r/\A[A-Z2-7]{10}\z/, code) and
      case receipt do
        %{
          state: :expired,
          secret_hash: hash,
          key: key,
          expires_at: expires_at,
          status_expires_at: status_expires_at
        } = exact ->
          map_size(exact) == 5 and is_binary(hash) and byte_size(hash) == 32 and
            hex_key?(key) and valid_pending_deadlines?(expires_at, status_expires_at)

        %{
          state: :cancelled,
          secret_hash: hash,
          person: person,
          key: key,
          label: label,
          expires_at: expires_at,
          status_expires_at: status_expires_at
        } = exact ->
          map_size(exact) in [7, 8] and Map.get(exact, :joined, false) in [true, false] and
            is_binary(hash) and byte_size(hash) == 32 and
            (is_nil(person) or Map.has_key?(engine.persons, person)) and hex_key?(key) and
            is_binary(label) and byte_size(label) >= 1 and
            valid_pending_deadlines?(expires_at, status_expires_at)

        %{
          state: :declined,
          secret_hash: hash,
          person: person,
          key: key,
          label: label,
          expires_at: expires_at,
          status_expires_at: status_expires_at,
          declined_by: owner
        } = exact ->
          map_size(exact) in [8, 9] and Map.get(exact, :joined, false) in [true, false] and
            is_binary(hash) and byte_size(hash) == 32 and
            Map.has_key?(engine.persons, person) and hex_key?(key) and hex_key?(owner) and
            is_binary(label) and byte_size(label) >= 1 and
            valid_pending_deadlines?(expires_at, status_expires_at)

        %{
          state: :approved,
          secret_hash: hash,
          person: person,
          key: key,
          label: label,
          expires_at: expires_at,
          status_expires_at: status_expires_at,
          approved_by: approved_by
        } = exact ->
          map_size(exact) in [8, 9, 10] and
            Map.get(exact, :joined, false) in [true, false] and
            is_integer(Map.get(exact, :request_generation, 1)) and
            Map.get(exact, :request_generation, 1) > 0 and
            is_binary(hash) and byte_size(hash) == 32 and
            Map.has_key?(engine.persons, person) and hex_key?(key) and
            is_binary(label) and byte_size(label) >= 1 and
            valid_pending_deadlines?(expires_at, status_expires_at) and
            (is_nil(approved_by) or hex_key?(approved_by)) and
            get_in(engine.devices, [key, :person]) == person

        _ ->
          false
      end
  end

  defp valid_maintainers?(engine) do
    is_map(engine.maintainer_grants) and
      Enum.all?(engine.maintainer_grants, fn {id, grant} ->
        valid_maintainer?(engine, id, grant)
      end)
  end

  defp valid_maintainer?(engine, id, grant) do
    AttentionServer.Message.uuid_v4?(id) and
      Map.has_key?(engine.persons, grant.target_person) and
      Map.has_key?(engine.persons, grant.grantee_person) and
      is_struct(grant.scopes, MapSet) and MapSet.size(grant.scopes) in 1..4 and
      MapSet.subset?(grant.scopes, MapSet.new(~w(devices senders view_items configure))) and
      is_integer(grant.granted_at) and grant.granted_at > 0 and
      hex_key?(grant.granted_by) and
      ((is_nil(grant.revoked_at) and is_nil(grant.revoked_by)) or
         (is_integer(grant.revoked_at) and grant.revoked_at >= grant.granted_at and
            hex_key?(grant.revoked_by)))
  end

  defp valid_enrollment_read_grant?(engine, grant_id, grant) do
    AttentionServer.Message.uuid_v4?(grant_id) and
      Map.has_key?(engine.persons, grant.person) and
      owns?(engine, grant.person, grant.connector) and
      nonempty?(grant.source_instance_id) and nonempty?(grant.issuer) and
      is_boolean(grant.revoked) and valid_grant_target?(engine, grant.person, grant.target)
  end

  defp valid_hosted_accounts?(engine) do
    accounts = engine.hosted_accounts

    is_map(accounts) and
      Enum.all?(accounts, fn {ref, record} -> valid_hosted_account?(engine, ref, record) end) and
      accounts |> Map.values() |> Enum.map(& &1.person) |> Enum.uniq() |> length() ==
        map_size(accounts)
  end

  defp valid_hosted_account?(engine, ref, record) do
    is_binary(ref) and byte_size(ref) >= 1 and map_size(record) == 3 and
      Map.has_key?(engine.persons, record.person) and
      Map.has_key?(engine.owners, record.person) and
      (is_nil(record.label) or (is_binary(record.label) and byte_size(record.label) >= 1)) and
      is_boolean(record.test)
  end

  defp valid_delivery_states?(engine) do
    is_map(engine.delivery_states) and
      Enum.all?(engine.delivery_states, fn {person, state} ->
        valid_delivery_state?(engine, person, state)
      end)
  end

  defp valid_delivery_state?(engine, person, state),
    do:
      Map.has_key?(engine.persons, person) and is_binary(person) and
        state in ["STOPPED", "RESTORED"]

  defp valid_instance?(id),
    do: is_binary(id) and byte_size(id) == 32 and Regex.match?(~r/\A[0-9a-f]{32}\z/, id)

  defp owns?(engine, person, key),
    do: get_in(engine.devices, [key, :person]) == person and Map.has_key?(engine.persons, person)

  defp owns_or_browser?(engine, person, key),
    do:
      owns?(engine, person, key) or
        (Map.has_key?(engine.persons, person) and BrowserRecipient.valid_for?(key, person))

  defp actor_owns?(engine, person, key),
    do:
      owns_or_browser?(engine, person, key) or
        get_in(engine.agents, [key, :person]) == person

  defp actor_kind?(engine, person, terminal) do
    case terminal.actor_kind do
      :device ->
        owns?(engine, person, terminal.actor) and
          Map.get(terminal, :actor_id, terminal.actor) == terminal.actor

      :agent ->
        get_in(engine.agents, [terminal.actor, :person]) == person and
          get_in(engine.agents, [terminal.actor, :id]) == terminal.actor_id

      :browser ->
        BrowserRecipient.valid_for?(terminal.actor, person) and
          Map.get(terminal, :actor_id, terminal.actor) == terminal.actor

      _ ->
        false
    end
  end

  defp valid_terminal_actor?(_engine, _person, %{actor_kind: :server} = terminal) do
    is_nil(terminal.actor) and is_nil(Map.get(terminal, :actor_id)) and
      terminal.state == "answered" and terminal.by == "default" and
      terminal.answer_present == true
  end

  defp valid_terminal_actor?(engine, person, terminal),
    do:
      actor_owns?(engine, person, terminal.actor) and
        actor_kind?(engine, person, terminal)

  defp valid_seq?(engine, person, seq),
    do: is_integer(seq) and seq > 0 and seq <= Map.get(engine.persons, person, 0)

  defp valid_authority_reference?(engine, {:row, :messages, key}) do
    case Map.fetch(engine.messages, key) do
      {:ok, item} -> valid_item?(engine, key, item)
      :error -> true
    end
  end

  defp valid_authority_reference?(engine, {:row, :uploads, key}) do
    case Map.fetch(engine.uploads, key) do
      {:ok, upload} -> valid_upload?(engine, key, upload)
      :error -> true
    end
  end

  defp valid_authority_reference?(engine, {:row, :receipts, {person, actor, _} = key}),
    do: not Map.has_key?(engine.receipts, key) or actor_owns?(engine, person, actor)

  defp valid_authority_reference?(engine, {:row, :agents, key}) do
    case Map.fetch(engine.agents, key) do
      {:ok, agent} -> valid_agent?(engine, key, agent)
      :error -> true
    end
  end

  defp valid_authority_reference?(engine, {:row, :devices, key}) do
    case Map.fetch(engine.devices, key) do
      {:ok, device} -> valid_device?(engine, device)
      :error -> true
    end
  end

  defp valid_authority_reference?(engine, {:row, :owners, person}) do
    case Map.fetch(engine.owners, person) do
      {:ok, owner} -> valid_owner?(engine, person, owner)
      :error -> true
    end
  end

  defp valid_authority_reference?(engine, {:member, :retired_owner_keys, key}),
    do:
      not MapSet.member?(engine.retired_owner_keys, key) or
        (hex_key?(key) and not Map.has_key?(engine.devices, key) and
           not Map.has_key?(engine.agents, key))

  defp valid_authority_reference?(engine, {:row, :hosted_accounts, reference}) do
    case Map.fetch(engine.hosted_accounts, reference) do
      {:ok, record} -> valid_hosted_account?(engine, reference, record)
      :error -> true
    end
  end

  defp valid_authority_reference?(engine, {:row, :delivery_states, person}) do
    case Map.fetch(engine.delivery_states, person) do
      {:ok, state} -> valid_delivery_state?(engine, person, state)
      :error -> true
    end
  end

  defp valid_authority_reference?(engine, {:row, :algorithm_choices, person}) do
    case Map.fetch(engine.algorithm_choices, person) do
      {:ok, choice} -> valid_algorithm_choice?(engine, person, choice)
      :error -> true
    end
  end

  defp valid_authority_reference?(engine, {:row, :free_order_highs, person}) do
    case Map.fetch(engine.free_order_highs, person) do
      {:ok, high} -> valid_order_high?(engine, person, high)
      :error -> true
    end
  end

  defp valid_authority_reference?(engine, {:row, :bounce_levels, person}) do
    case Map.fetch(engine.bounce_levels, person) do
      {:ok, config} -> BounceDecision.valid_configuration?(engine, person, config)
      :error -> true
    end
  end

  defp valid_authority_reference?(engine, {:row, :maintainer_grants, id}) do
    case Map.fetch(engine.maintainer_grants, id) do
      {:ok, grant} -> valid_maintainer?(engine, id, grant)
      :error -> true
    end
  end

  defp valid_authority_reference?(engine, {:row, :enrollment_read_grants, id}) do
    case Map.fetch(engine.enrollment_read_grants, id) do
      {:ok, grant} -> valid_enrollment_read_grant?(engine, id, grant)
      :error -> true
    end
  end

  defp valid_authority_reference?(engine, {:row, :pending_enrollments, code}) do
    case Map.fetch(engine.pending_enrollments, code) do
      {:ok, item} ->
        not Map.has_key?(engine.pending_history, code) and valid_pending_item?(engine, code, item)

      :error ->
        true
    end
  end

  defp valid_authority_reference?(engine, {:row, :pending_history, code}) do
    case Map.fetch(engine.pending_history, code) do
      {:ok, receipt} ->
        not Map.has_key?(engine.pending_enrollments, code) and
          valid_pending_resolution?(engine, code, receipt)

      :error ->
        true
    end
  end

  defp valid_authority_reference?(engine, {:row, :browser_sign_ins, hash}) do
    case Map.fetch(engine.browser_sign_ins, hash) do
      {:ok, record} -> BrowserSignIn.valid_record?(engine, hash, record)
      :error -> true
    end
  end

  defp valid_authority_reference?(engine, {:row, :join_invitations, hash}) do
    case Map.fetch(engine.join_invitations, hash) do
      {:ok, record} -> GuidedJoin.valid_record?(engine, hash, record)
      :error -> true
    end
  end

  defp valid_authority_reference?(engine, {:row, field, {person, actor} = key})
       when field in [:sender_grants, :subscriptions, :origin_feeds],
       do: not Map.has_key?(Map.fetch!(engine, field), key) or owns?(engine, person, actor)

  defp valid_authority_reference?(engine, {:row, :push_bindings, {person, device} = key}) do
    case Map.fetch(engine.push_bindings, key) do
      {:ok, binding} ->
        owns?(engine, person, device) and valid_push_binding?(binding, engine, person, device)

      :error ->
        true
    end
  end

  defp valid_authority_reference?(
         engine,
         {:row, :source_report_grants, {person, actor, _} = key}
       ),
       do: not Map.has_key?(engine.source_report_grants, key) or owns?(engine, person, actor)

  defp valid_authority_reference?(engine, {:row, :origin_sessions, key}) do
    case Map.fetch(engine.origin_sessions, key) do
      {:ok, session} -> Conversation.valid_origin_session?(engine, key, session)
      :error -> true
    end
  end

  defp valid_authority_reference?(engine, {:row, field, {person, actor} = key})
       when field in [:cursors, :conversation_cursors] do
    high =
      if field == :cursors,
        do: Map.get(engine.persons, person, 0),
        else: Map.get(engine.conversation_highs, person, 0)

    case Map.fetch(Map.fetch!(engine, field), key) do
      {:ok, seq} ->
        owns_or_browser?(engine, person, actor) and is_integer(seq) and seq >= 0 and seq <= high

      :error ->
        true
    end
  end

  defp valid_authority_reference?(engine, {:member, :conversation_opened, {person, actor} = key}),
    do:
      not MapSet.member?(engine.conversation_opened, key) or
        owns_or_browser?(engine, person, actor)

  defp valid_authority_reference?(engine, {:row, :origin_receipts, {person, origin, _} = key}) do
    case Map.fetch(engine.origin_receipts, key) do
      {:ok, receipt} ->
        owns?(engine, person, origin) and Conversation.valid_origin_receipt?(engine, key, receipt)

      :error ->
        true
    end
  end

  defp valid_authority_reference?(engine, {:origin_feed, scope, {:delivery_ack, seq}}) do
    valid_authority_reference?(engine, {:row, :origin_feeds, scope}) and
      Conversation.valid_delivery_ack?(engine, scope, seq)
  end

  defp valid_authority_reference?(engine, {:origin_feed, scope, _}),
    do: valid_authority_reference?(engine, {:row, :origin_feeds, scope})

  defp valid_authority_reference?(_engine, _address), do: true

  defp valid_transition_fact?(_old, next, {:field, :agent_cursor_key}),
    do: valid_cursor_key?(next.agent_cursor_key)

  defp valid_transition_fact?(_old, next, {:field, :jev_endpoint}),
    do: valid_jev_endpoint?(next.jev_endpoint)

  defp valid_transition_fact?(_old, next, {:field, :jev_model}),
    do: valid_jev_model?(next.jev_model)

  defp valid_transition_fact?(_old, next, {:field, :pending_clock_high_water}),
    do: is_integer(next.pending_clock_high_water) and next.pending_clock_high_water >= 0

  defp valid_transition_fact?(_old, next, {:field, :relay_instance_id}),
    do: valid_instance?(next.relay_instance_id)

  # Published ordering and reply facts are append-only. Lowering their high
  # water mark or replacing a reply could invalidate untouched references.
  defp valid_transition_fact?(old, next, {:row, :free_order_highs, person}) do
    case Map.fetch(next.free_order_highs, person) do
      {:ok, high} ->
        is_integer(high) and high >= Map.get(old.free_order_highs, person, 0)

      :error ->
        not Map.has_key?(old.free_order_highs, person)
    end
  end

  defp valid_transition_fact?(old, next, {:row, :conversation_events, {person, _} = key}) do
    event = Map.get(next.conversation_events, key)

    case Map.fetch(old.conversation_events, key) do
      {:ok, published} -> event === published
      :error -> is_nil(event) or valid_arrival?(event, Map.get(next.free_order_highs, person, 0))
    end
  end

  defp valid_transition_fact?(_old, next, {:row, :chat_read_positions, key}) do
    case Map.fetch(next.chat_read_positions, key) do
      {:ok, position} ->
        valid_chat_position?(next, key, position) and valid_scored_reply?(next, key)

      :error ->
        true
    end
  end

  defp valid_transition_fact?(_old, next, {:row, :reply_group_history, key}),
    do: valid_scored_reply?(next, key)

  defp valid_transition_fact?(_old, _next, _address), do: true

  defp valid_scored_reply?(engine, key) do
    case Map.fetch(engine.reply_group_history, key) do
      {:ok, history} -> valid_reply_history?(engine, key, history)
      :error -> true
    end
  end

  defp valid_push_address_change?(old, next, {:row, :push_bindings, key}) do
    case Map.get(next.push_bindings, key) do
      %{active: true, address: address} ->
        previous = AttentionServer.PushIndex.owner(old.route_b_index.push_bindings, address)
        current = AttentionServer.PushIndex.owner(next.route_b_index.push_bindings, address)

        current == key and
          (previous == key or
             not match?(%{active: true, address: ^address}, Map.get(next.push_bindings, previous)))

      _ ->
        true
    end
  end

  defp valid_push_address_change?(_old, _next, _address), do: true

  defp valid_item_change?(old, next, {person, _, _} = key) do
    case Map.fetch(next.messages, key) do
      {:ok, record} ->
        terminal = record.terminal

        valid_item?(next, key, record) and
          valid_arrival?(record, Map.get(next.free_order_highs, person, 0)) and
          terminal_unchanged?(old.messages, key, record) and
          (is_nil(terminal) or
             match?(%{kind: :lifecycle, key: ^key}, next.events[{person, terminal.seq}]))

      :error ->
        not Map.has_key?(old.messages, key)
    end
  rescue
    _ -> false
  end

  defp terminal_unchanged?(messages, key, record) do
    case Map.get(messages, key) do
      %{terminal: prior} when not is_nil(prior) -> prior === record.terminal
      _ -> true
    end
  end

  defp valid_item?(engine, {person, sender, id}, record) do
    owns?(engine, person, record.origin) and
      record.message["sender_id"] == sender and record.message["message_id"] == id and
      valid_seq?(engine, person, record.accepted_seq) and
      valid_conversation_record?(record) and valid_origin_result?(record) and
      (is_nil(record.terminal) or
         (valid_terminal_actor?(engine, person, record.terminal) and
            valid_seq?(engine, person, record.terminal.seq))) and
      valid_source_record?(record)
  end

  defp valid_source_record?(record) do
    binding = Map.get(record, :source_binding)
    result = Map.get(record, :source_result)

    binding_ok =
      is_nil(binding) or
        (Enum.all?(~w(source_instance_id request_id request_revision), &nonempty?(binding[&1])) and
           binding["acceptance_seq"] == record.accepted_seq)

    result_ok =
      is_nil(result) or
        (not is_nil(binding) and not is_nil(record.terminal) and
           result.terminal_seq == record.terminal.seq and
           result.status in ~w(received applied refused) and
           is_integer(result.revision) and result.revision > 0 and
           nonempty?(result.result_ref))

    binding_ok and result_ok
  end

  defp unique_source_bindings?(messages) do
    tuples =
      Enum.flat_map(messages, fn {_key, record} ->
        case Map.get(record, :source_binding) do
          nil ->
            []

          binding ->
            [{binding["source_instance_id"], binding["request_id"], binding["request_revision"]}]
        end
      end)

    length(tuples) == MapSet.size(MapSet.new(tuples))
  end

  defp valid_grant_target?(engine, person, target) when is_map(target) do
    if Enum.sort(Map.keys(target)) != ~w(id key_fingerprint kind),
      do: false,
      else: grant_target?(engine, person, target)
  end

  defp valid_grant_target?(_, _, _), do: false

  defp grant_target?(engine, person, %{"kind" => "device", "id" => id, "key_fingerprint" => key}) do
    id == key and hex_key?(key) and get_in(engine.devices, [key, :person]) == person
  end

  defp grant_target?(engine, person, %{"kind" => "agent", "id" => id, "key_fingerprint" => key}) do
    AttentionServer.Message.uuid_v4?(id) and hex_key?(key) and
      get_in(engine.agents, [key, :person]) == person and
      get_in(engine.agents, [key, :id]) == id
  end

  defp grant_target?(_, _, _), do: false

  defp hex_key?(value),
    do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{64}\z/, value)

  defp nonempty?(value), do: is_binary(value) and value != ""

  defp operation_id(%{op_id: value}) do
    if AttentionServer.Message.uuid?(value), do: value
  end

  defp operation_id(%{operation_id: value}) do
    if AttentionServer.Message.uuid?(value), do: value
  end

  defp operation_id(_), do: nil
end
