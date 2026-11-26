defmodule AttentionServer.Store do
  @moduledoc """
  A transactional SQLite snapshot for the bounded relay core. Origin availability
  and support updates are committed with the complete state before success replies.

  The server serializes decisions in one process. SQLite commits the complete
  new decision state and a content-free audit row before any success reply.
  A version compare prevents a second process using a stale snapshot from
  silently replacing committed work. This is not a public database API.
  """

  require Logger

  alias AttentionServer.{
    AgentChangeLog,
    APNS,
    BrowserRecipient,
    BrowserAuth,
    BounceDecision,
    BrowserSignIn,
    Conversation,
    DeviceMigration,
    Diagnostics,
    Engine,
    GuidedJoin,
    LargeData,
    NativeSignIn,
    SharedAnswers,
    SharedMessages
  }

  alias AttentionServer.ClientAPI.Idempotency
  alias Exqlite.Sqlite3

  alias AttentionServer.Store.{
    AuthSchema,
    ClientDataMigration,
    ConversionPreflight,
    DerivedIndex,
    KeyedMigration,
    KeyedReference,
    KeyedValidation,
    Persistence,
    RecordCodec,
    RecordTable,
    SnapshotEncoding
  }

  @format_version 30
  @client_data_migration "retained_client_data_v1"
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
    :auth_schema,
    mode: :snapshot,
    source_bindings: %{},
    upload_links: %{},
    authority_index: %{},
    unique_index: %{},
    blob_owners: %{},
    progress_version: 0,
    encoding_cache: %{},
    change_log: false,
    # Persons whose client resources changed since the Relay last ranked them.
    change_log_touched: MapSet.new()
  ]

  @progress_fields [:cursors, :conversation_cursors, :conversation_opened]

  @schema """
  CREATE TABLE IF NOT EXISTS relay_data_migrations (
    name TEXT PRIMARY KEY NOT NULL
  );
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

  def activate_auth(%__MODULE__{auth_schema: %{}} = store, _mode, _devices), do: {:ok, store}

  def activate_auth(%__MODULE__{mode: :keyed, engine: engine} = store, mode, devices)
      when mode in [:self_hosted, :saas] do
    next = if mode == :self_hosted, do: Engine.replace_rows(engine, :browser_sign_ins, %{}), else: engine
    audit = %{person: "system", actor: "schema_migration", action: "sign_in_s1", auth_activation: devices}

    with {:ok, committed} <- commit(store, next, audit),
         {:ok, %{} = schema} <- AuthSchema.read(committed.db) do
      {:ok, %{committed | auth_schema: schema}}
    else
      {:error, reason, _store} -> {:error, reason}
      {:error, _} = error -> error
      _ -> {:error, :invalid_auth_schema}
    end
  end

  def activate_auth(_store, _mode, _devices), do: {:error, :auth_requires_format31}

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

  @doc false
  def migrate_retained_client_data(%__MODULE__{mode: :snapshot, change_log: true} = store) do
    case retained_data_migration(store.db) do
      {:ok, [@client_data_migration]} ->
        {:ok, store}

      {:ok, nil} ->
        {engine, touched} = ClientDataMigration.prepare(store.engine)

        if MapSet.size(touched) == 0 do
          with :ok <- record_data_migration(store.db, @client_data_migration), do: {:ok, store}
        else
          case commit(store, engine, retained_data_audit()) do
            {:ok, migrated} -> {:ok, migrated}
            {:error, reason, _} -> {:error, reason}
          end
        end

      error ->
        error
    end
  end

  def migrate_retained_client_data(%__MODULE__{}), do: {:error, :storage_unavailable}

  defp retained_data_migration(db) do
    one(db, "SELECT name FROM relay_data_migrations WHERE name = ?", [@client_data_migration])
  end

  defp retained_data_audit,
    do: %{person: "system", actor: "schema_migration", action: @client_data_migration}

  def open(nil), do: {:ok, %__MODULE__{db: nil, engine: Engine.new(), version: 0}}

  def open(":memory:") do
    Diagnostics.failure(:server_storage, :store_open, :invalid_store_path, :unavailable)
    {:error, :invalid_store_path}
  end

  def open(path) when is_binary(path) and path != "" do
    if File.exists?(path) do
      with {:ok, mode} <- cutover_mode(path), do: open(path, mode)
    else
      open(path, :snapshot)
    end
  end

  def open(_) do
    Diagnostics.failure(:server_storage, :store_open, :invalid_store_path, :unavailable)
    {:error, :invalid_store_path}
  end

  def open_relay(path) when is_binary(path) and path not in ["", ":memory:"],
    do: open(path, :relay)

  def open_relay(path), do: open(path)

  def track_relay_writes(%__MODULE__{mode: :keyed} = store) do
    put_in(store.engine.touched_keys, MapSet.new())
  end

  def track_relay_writes(store), do: store

  def open(path, :keyed_cutover)
      when is_binary(path) and path != "" and path != ":memory:" do
    case cutover_mode(path) do
      {:ok, :keyed} ->
        open(path, :keyed)

      {:ok, :snapshot} ->
        with {:ok, keyed} <- convert_snapshot_store(path), do: finish_open(keyed)

      error ->
        error
    end
  end

  def open(path, mode)
      when is_binary(path) and path != "" and mode in [:snapshot, :keyed, :relay] do
    with {:ok, store} <- open_store(path, mode), do: finish_open(store)
  end

  @doc """
  Reads a stopped, checkpointed FORMAT30, FORMAT31 or FORMAT32 store without opening
  the original with SQLite. A disposable private copy contains any SQLite
  sidecars, and no normal startup or migration work runs.
  """
  def read_stopped_engine(path) when is_binary(path) do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)
    dir = Path.join(System.tmp_dir!(), "harklight-preview-" <> suffix)

    with :ok <- File.mkdir(dir) do
      try do
        copy = Path.join(dir, "preview.sqlite3")

        with :ok <- File.chmod(dir, 0o700),
             :ok <- require_checkpoint(path),
             :ok <- File.cp(path, copy),
             :ok <- require_checkpoint(path),
             {:ok, db} <- Sqlite3.open(copy, mode: :readonly) do
          try do
            read_preview_engine(db)
          after
            Sqlite3.close(db)
          end
        end
      after
        File.rm_rf(dir)
      end
    end
  end

  defp require_checkpoint(path) do
    Enum.reduce_while(["-wal", "-journal"], :ok, fn suffix, :ok ->
      case File.stat(path <> suffix) do
        {:error, :enoent} -> {:cont, :ok}
        {:ok, %{size: 0}} -> {:cont, :ok}
        {:ok, _} -> {:halt, {:error, :store_not_checkpointed}}
        error -> {:halt, error}
      end
    end)
  end

  defp read_preview_engine(db) do
    case one(db, "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'relay_records'", []) do
      {:ok, [1]} ->
        with {:ok, decoded, _generation} <- KeyedMigration.load_preview(db),
             do: validate_keyed_engine(decoded)

      {:ok, nil} ->
        with {:ok, engine, _version} <- load_or_create(db, nil, :preview), do: {:ok, engine}

      error ->
        error
    end
  end

  defp open_store(path, mode) do
    upload_dir = Path.expand(path) <> ".uploads"

    case Sqlite3.open(path) do
      {:ok, db} ->
        initialized =
          with {:ok, auth_schema} <- AuthSchema.read(db),
               {:ok, engine, version} <- initialize(db, upload_dir, mode) do
            {:ok, auth_schema, engine, version}
          end

        case initialized do
          {:ok, auth_schema, engine, version} ->
            store_mode = if mode == :relay, do: :keyed, else: mode

            case finalize_open(db, upload_dir, engine, version, store_mode) do
              {:ok, store} ->
                {:ok, %{store | auth_schema: auth_schema}}

              {:error, reason} ->
                Diagnostics.failure(:server_storage, :store_open, reason, :unavailable)
                _ = Sqlite3.close(db)
                {:error, :storage_unavailable}

              other ->
                Diagnostics.failure(:server_storage, :store_open, other, :unavailable)
                _ = Sqlite3.close(db)
                {:error, :storage_unavailable}
            end

          {:error, :conversion_required} = error ->
            _ = Sqlite3.close(db)
            error

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
          {:ok, nil} -> {:ok, :snapshot}
          error -> error
        end
      after
        Sqlite3.close(db)
      end
    end
  end

  def convert_stopped(path, budget, expected_format) when is_binary(path) and path != "" do
    preflight = &ConversionPreflight.check(&1, budget, expected_format)

    convert_snapshot_store(path, preflight, fn _store, _engine, _generation -> {:ok, :converted} end)
  end

  defp convert_snapshot_store(
         path,
         preflight \\ fn _db -> :ok end,
         prepare \\ &prepare_keyed_conversion/3
       ) do
    upload_dir = Path.expand(path) <> ".uploads"

    with {:ok, db} <- Sqlite3.open(path, mode: :readwrite) do
      try do
        result =
          RecordTable.transaction(db, fn ->
            with {:ok, auth_schema} <- AuthSchema.read(db) do
              prepare_with_auth = fn store, engine, generation ->
                case prepare.(store, engine, generation) do
                  {:ok, %__MODULE__{} = converted} ->
                    {:ok, %{converted | auth_schema: auth_schema}}

                  result ->
                    result
                end
              end

              case preflight.(db) do
                :ok -> convert_locked(db, upload_dir, prepare_with_auth)
                {:ok, _info} -> convert_locked(db, upload_dir, prepare_with_auth)
                {:already_converted, 31} -> KeyedMigration.upgrade_anchor(db)
                {:already_converted, 32} -> {:ok, :already_converted}
                error -> error
              end
            end
          end)

        case result do
          {:ok, :already_converted} = success ->
            _ = Sqlite3.close(db)
            success

          {:ok, :converted} = success ->
            :ok = KeyedMigration.report_stage(:committed)
            _ = Sqlite3.close(db)
            success

          {:ok, _} = success ->
            :ok = KeyedMigration.report_stage(:committed)
            success

          error ->
            Diagnostics.failure(:server_storage, :store_open, error, :unavailable)
            _ = Sqlite3.close(db)
            error
        end
      catch
        kind, reason ->
          _ = Sqlite3.close(db)
          :erlang.raise(kind, reason, __STACKTRACE__)
      end
    end
  end

  # Normalise in memory so a failed install preserves the old anchor and
  # progress rows at their original generation.
  defp convert_locked(db, upload_dir, prepare) do
    with :ok <- KeyedMigration.report_stage(:preflight_passed),
         :ok <- Sqlite3.execute(db, @schema),
         {:ok, legacy} <- load_or_create(db, upload_dir, :convert),
         :ok <- KeyedMigration.report_stage(:snapshot_loaded),
         {:ok, rows} <- load_large_data_references(db),
         {:ok, engine} <- attach_conversion_refs(legacy.engine, upload_dir, rows),
         {:ok, legacy} <- prepare_cutover_client_data(%{legacy | engine: engine}),
         :ok <- LargeData.verify_all(legacy.engine),
         :ok <- write_large_data_references(db, LargeData.persisted_references(legacy.engine)) do
      KeyedMigration.convert_locked(
        legacy,
        legacy.engine,
        legacy.version,
        prepare
      )
    end
  end

  # The cutover owns the transaction. Prepare the retained data before writing
  # records so its changes, log entries and marker roll back with the anchor.
  defp prepare_cutover_client_data(%__MODULE__{db: db} = snapshot) do
    case retained_data_migration(db) do
      {:ok, [@client_data_migration]} ->
        {:ok, snapshot}

      {:ok, nil} ->
        {engine, touched} = ClientDataMigration.prepare(snapshot.engine)

        with :ok <- record_cutover_client_changes(snapshot, engine, touched),
             :ok <- record_data_migration(db, @client_data_migration) do
          {:ok, %{snapshot | engine: engine}}
        end

      error ->
        error
    end
  end

  defp record_cutover_client_changes(%__MODULE__{db: db, version: version} = snapshot, engine, touched) do
    if MapSet.size(touched) == 0 do
      :ok
    else
      before = AttentionServer.RouteBIndex.enable_agent_feed(snapshot.engine)
      next = %{engine | route_b_index: before.route_b_index}
      log = AttentionServer.ChangeLog.commit_log_touched(before, next, touched, [])

      with :ok <- AttentionServer.ChangeLog.enable_in_transaction(db, version),
           :ok <- AttentionServer.AgentChangeLog.enable_in_transaction(db, before, version),
           :ok <- write_audit(db, retained_data_audit()),
           {:ok, _, _} <- append_change_logs(db, log, version + 1) do
        :ok
      end
    end
  end

  defp attach_conversion_refs(engine, _directory, []), do: {:ok, engine}

  defp attach_conversion_refs(engine, directory, rows),
    do: LargeData.attach_paths(engine, directory, rows)

  defp prepare_keyed_conversion(legacy, engine, generation) do
    finalize_mode(%__MODULE__{
      db: legacy.db,
      upload_dir: legacy.upload_dir,
      engine: rebuild_open_indexes(engine),
      version: generation,
      mode: :keyed
    })
  end

  defp finalize_open(db, upload_dir, engine, version, mode) do
    engine = rebuild_open_indexes(engine)

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
         {:ok, store} <- finalize_mode(store),
         :ok <- sync_large_data_references(store, LargeData.persisted_references(engine)) do
      {:ok, store}
    end
  end

  defp finish_open(store) do
    # Registry refresh and conversion can still refuse the open. Reacquire the
    # writer lock after those steps succeed and check that the file inventory
    # is still current before performing irreversible maintenance.
    result =
      RecordTable.transaction(store.db, fn ->
        with :ok <- check_open_generation(store),
             :ok <- prune_untracked_uploads(store),
             :ok <- LargeData.prune(store.upload_dir, store.engine) do
          {:ok, :pruned}
        end
      end)

    case result do
      {:error, :stale_generation} ->
        # Another writer committed after this open loaded its state, so the
        # loaded engine is no longer current. Refuse the open.
        Diagnostics.failure(:server_storage, :store_open, :stale_generation, :unavailable)
        _ = close(store)
        {:error, :storage_unavailable}

      _ ->
        Diagnostics.result(:upload_cleanup, :startup_cleanup, result, :unavailable)
        {:ok, store}
    end
  end

  defp check_open_generation(%__MODULE__{mode: :keyed, db: db, version: version}) do
    case RecordTable.generation(db) do
      {:ok, ^version} -> :ok
      _ -> {:error, :stale_generation}
    end
  end

  defp check_open_generation(%__MODULE__{db: db, version: version}) do
    case one(db, "SELECT version FROM relay_state WHERE id = 1", []) do
      {:ok, [^version]} -> :ok
      _ -> {:error, :stale_generation}
    end
  end

  defp rebuild_open_indexes(engine) do
    engine |> Engine.rebuild_default_index() |> Conversation.index_origin_feeds()
  end

  defp finalize_mode(%__MODULE__{mode: :keyed} = store) do
    engine = AttentionServer.RouteBIndex.enable_keyed_lookups(store.engine)

    {:ok,
     %{
       store
       | engine: engine,
         source_bindings: engine.route_b_index.source_bindings,
         upload_links: KeyedValidation.upload_index(engine),
         authority_index: KeyedValidation.authority_index(engine),
         unique_index: KeyedValidation.unique_index(engine),
         blob_owners: KeyedReference.blob_owners(engine)
     }}
  end

  defp finalize_mode(%__MODULE__{mode: :snapshot} = store) do
    with {:ok, store} <- load_progress(store) do
      persisted = LargeData.persist_refs(store.engine)

      {_payload, cache} = SnapshotEncoding.encode(persisted, @format_version)
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

    case File.rename(staged, target) do
      :ok ->
        {:ok, target}

      {:error, reason} ->
        Diagnostics.failure(
          :server_storage,
          :upload_publish,
          reason,
          :unavailable,
          operation_id(metadata)
        )

        {:error, :storage_unavailable}

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
    next = %{engine | uploads: Map.put(engine.uploads, metadata.id, metadata)}
    touched = MapSet.new([{:row, :uploads, metadata.id}])
    audit = %{person: metadata.person, actor: metadata.uploader, action: "file_upload"}

    case commit(store, {next, touched}, audit) do
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

  @doc false
  def verify_retained_uploads(%__MODULE__{engine: engine} = store) do
    if valid_upload_references?(engine) do
      check_retained_uploads(store, &verify_upload/2)
    else
      {:error, :retained_upload_unavailable}
    end
  end

  defp check_retained_uploads(%__MODULE__{engine: engine, upload_dir: directory}, verify) do
    engine
    |> KeyedValidation.upload_index()
    |> Enum.reduce_while(:ok, fn {id, _address}, :ok ->
      with true <- AttentionServer.Message.uuid_v4?(id),
           {:ok, upload} <- Map.fetch(engine.uploads, id),
           :ok <- verify.(Path.join(directory, id), upload) do
        {:cont, :ok}
      else
        _ -> {:halt, {:error, :retained_upload_unavailable}}
      end
    end)
  end

  # Name the retained uploads that halt orphan cleanup so an operator can
  # find and repair them. Only well-formed upload ids are logged.
  defp log_unavailable_retained_uploads(%__MODULE__{engine: engine, upload_dir: directory}) do
    unavailable =
      for {id, _owners} <- KeyedValidation.upload_index(engine),
          not retained_upload_available?(engine, directory, id),
          do: id

    {ids, malformed} = Enum.split_with(unavailable, &AttentionServer.Message.uuid_v4?/1)

    Logger.warning(
      "event=harklight_unavailable_retained_upload retained_uploads=#{length(unavailable)} " <>
        "malformed_ids=#{length(malformed)} upload_ids=#{ids |> Enum.sort() |> Enum.join(",")}"
    )
  end

  defp retained_upload_available?(engine, directory, id) do
    with true <- AttentionServer.Message.uuid_v4?(id),
         {:ok, upload} <- Map.fetch(engine.uploads, id),
         :ok <- verify_upload_size(Path.join(directory, id), upload) do
      true
    else
      _ -> false
    end
  end

  defp verify_upload_size(path, %{size: size}) do
    case File.stat(path) do
      {:ok, %{type: :regular, size: ^size}} -> :ok
      _ -> {:error, :retained_upload_unavailable}
    end
  end

  @doc "Forget only unreferenced uploads in the durable snapshot before unlinking their bytes."
  def delete_uploads(%__MODULE__{} = store, [], _audit), do: {:ok, store}

  def delete_uploads(%__MODULE__{} = store, ids, audit) when is_list(ids) do
    next = %{store.engine | uploads: Map.drop(store.engine.uploads, ids)}

    proposal =
      if store.mode == :keyed do
        touched =
          for id <- ids,
              Map.has_key?(store.engine.uploads, id),
              into: MapSet.new(),
              do: {:row, :uploads, id}

        {next, touched}
      else
        next
      end

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

  @doc "Remove upload files whose authority metadata was already removed by a prior commit."
  def prune_untracked_uploads(%__MODULE__{upload_dir: nil}), do: :ok

  def prune_untracked_uploads(%__MODULE__{} = store) do
    with :ok <- check_retained_uploads(store, &verify_upload_size/2),
         {:ok, names} <- File.ls(store.upload_dir) do
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
      {:error, :retained_upload_unavailable} ->
        # Fetching this attachment already fails independently. Leave files in
        # place while the retained upload inventory cannot be verified.
        Diagnostics.failure(:upload_cleanup, :startup_cleanup, :upload_integrity_mismatch)
        log_unavailable_retained_uploads(store)
        :ok

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

  defp initialize(db, upload_dir, :snapshot) do
    with :ok <- set_durability(db),
         :ok <- Sqlite3.execute(db, @schema) do
      load_or_create(db, upload_dir)
    end
  end

  defp initialize(db, upload_dir, :relay) do
    with {:ok, kind} <- relay_layout(db) do
      case kind do
        :rows -> initialize(db, upload_dir, :keyed)
        :snapshot -> {:error, :conversion_required}
        :empty -> initialize_new_relay(db, upload_dir)
      end
    end
  end

  defp initialize(db, _upload_dir, :keyed) do
    with {:ok, engine, generation} <- KeyedMigration.load(db),
         {:ok, engine} <- validate_keyed_engine(engine),
         :ok <- set_durability(db),
         :ok <- DerivedIndex.ensure(db, generation) do
      {:ok, engine, generation}
    end
  end

  defp validate_keyed_engine(decoded) do
    engine = decoded |> BounceDecision.rebuild_indexes() |> Engine.rebuild_route_b_index()
    if valid_state?(engine), do: {:ok, engine}, else: {:error, :invalid_state}
  rescue
    _ -> {:error, :invalid_state}
  end

  defp relay_layout(db) do
    case one(
           db,
           "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = 'relay_state'",
           []
         ) do
      {:ok, [0]} ->
        {:ok, :empty}

      {:ok, [1]} ->
        case one(
               db,
               """
               SELECT EXISTS (SELECT 1 FROM relay_state WHERE id = 1),
                      EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'relay_records')
               """,
               []
             ) do
          {:ok, [1, 1]} -> {:ok, :rows}
          {:ok, [1, 0]} -> {:ok, :snapshot}
          {:ok, [0, 0]} -> {:ok, :empty}
          error -> error
        end

      error ->
        error
    end
  end

  defp initialize_new_relay(db, upload_dir) do
    with :ok <- set_durability(db) do
      case RecordTable.transaction(db, fn -> bootstrap_if_empty(db) end) do
        {:ok, :existing} -> initialize(db, upload_dir, :keyed)
        {:ok, engine} -> {:ok, engine, 0}
        error -> error
      end
    end
  end

  defp bootstrap_if_empty(db) do
    case relay_layout(db) do
      {:ok, :rows} ->
        {:ok, :existing}

      {:ok, :snapshot} ->
        {:error, :conversion_required}

      {:ok, :empty} ->
        engine = Engine.new()

        with :ok <- Sqlite3.execute(db, @schema),
             :ok <- KeyedMigration.bootstrap(db, engine) do
          {:ok, engine}
        end

      error ->
        error
    end
  end

  defp set_durability(db),
    do: Sqlite3.execute(db, "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")

  @doc """
  Commits engine state and its change-log rows. A publisher can supply the
  canonical entry keys whose projection changed without changing their facts.
  """
  def commit(store, next, audit, projected_entries \\ [])

  def commit(
        %__MODULE__{mode: :keyed} = store,
        %Engine{touched_keys: %MapSet{} = touched} = next,
        audit,
        projected_entries
      ) do
    commit(store, {%{next | touched_keys: MapSet.new()}, touched}, audit, projected_entries)
  end

  def commit(
        %__MODULE__{mode: :keyed, change_log: true} = store,
        %Engine{} = next,
        %{item_refresh: keys} = audit,
        []
      ) do
    touched = MapSet.new(keys, &{:row, :item_availability, &1})

    commit(store, {next, touched}, audit)
  end

  # A keyed store commits only the records named by the transition's touched
  # addresses. Callers that cannot supply them are refused.
  def commit(
        %__MODULE__{mode: :keyed} = store,
        {%Engine{} = next, %MapSet{} = touched},
        audit,
        projected_entries
      ) do
    projected_entries = if store.change_log, do: projected_entries, else: []

    cond do
      MapSet.size(touched) > 0 or projected_entries != [] or
          Map.has_key?(audit, :native_redemption) or Map.has_key?(audit, :app_receipt_capture) or
          Map.has_key?(audit, :auth_activation) ->
        commit_keyed(store, next, touched, audit, projected_entries)

      KeyedValidation.untouched_change(store.engine, next, touched) == :ok ->
        {:ok, store}

      true ->
        keyed_commit_failure(store, {:error, {:untouched_change, :empty}}, :unavailable, audit)
    end
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

  defp rank_change_persons(touched, _heads, %{item_refresh: _}), do: touched

  defp rank_change_persons(touched, heads, _audit),
    do: MapSet.union(touched, MapSet.new(Map.keys(heads)))

  # Keyed commits validate per address by default. Exceptional transitions
  # still use the whole-state validator, which also checks the state at open.
  defp commit_keyed(
         %__MODULE__{db: db, version: version} = store,
         next,
         touched,
         audit,
         projected
       ) do
    old = store.engine
    {next, touched} = AttentionServer.SharedPush.enqueue(old, next, touched, audit)
    items = for {:row, :messages, key} <- touched, do: key

    with :ok <- KeyedValidation.untouched_change(old, next, touched),
         :ok <- keyed_validation(store, next, touched),
         :ok <- KeyedValidation.sequences(old, next, touched),
         {:ok, upload_links, _related} <-
           KeyedValidation.upload_changes(store.upload_links, old, next, touched),
         {authority_index, _affected} <-
           KeyedValidation.authority_changes(store.authority_index, old, next, touched),
         {unique_index, _conflict} <-
           KeyedValidation.unique_changes(store.unique_index, old, next, touched),
         {:ok, source_bindings} <-
           KeyedValidation.source_changes(
             store.source_bindings,
             old.messages,
             next.messages,
             items
           ),
         {:ok, prepared} <- KeyedReference.prepare(old, next, touched, store.upload_dir),
         {:ok, blob_owners} <- update_blob_owners(store.blob_owners, prepared) do
      log =
        case audit do
          %{item_refresh: keys} -> AttentionServer.ChangeLog.refresh_items(old, keys)
          _ -> keyed_change_log(store, next, touched, projected)
        end

      log =
        log
        |> shared_send_log(next, audit)
        |> shared_mutation_log(next, audit)
        |> shared_freeze_log(old, next, touched)
        |> shared_chat_log(next, audit)
      %{records: records, delete_paths: delete_paths, reference_rows: references} = prepared
      derived = DerivedIndex.changes(old, next, touched)

      result =
        Persistence.run(fn ->
          result =
            RecordTable.commit(db, version, records, fn db, generation ->
              with :ok <- DerivedIndex.write(db, derived),
                   :ok <- write_reference_delta(db, delete_paths, references),
                   :ok <- write_audit(db, audit),
                   :ok <- write_auth_activation(db, prepared.engine, audit),
                   :ok <- write_auth_changes(db, audit),
                   {:ok, heads, agent_heads} <- append_change_logs(db, log, generation),
                   :ok <- write_shared_send_receipt(store, next, audit),
                   :ok <- write_shared_chat_receipt(store, next, audit),
                   :ok <- DerivedIndex.advance(db, generation),
                   :ok <- write_app_receipt(audit, %{store | engine: prepared.engine, version: generation}) do
                {:ok, {heads, agent_heads}}
              end
            end)

          if match?({:ok, _}, result) do
            :telemetry.execute([:harklight, :store, :commit, :committed], %{generation: version + 1}, %{})
          end

          result
        end)

      case result do
        {:ok, {heads, agent_heads}} ->
          prune_released_blobs(
            store.upload_dir,
            prepared.orphan_candidates,
            blob_owners,
            prepared.engine,
            audit
          )

          AttentionServer.ChangeLog.notify(heads)
          AttentionServer.ChangeLog.notify(agent_heads)
          if log, do: AttentionServer.ChangeLog.notify_closing(log.closings)

          engine = prepared.engine

          committed = %{
            store
            | engine: put_in(engine.route_b_index.source_bindings, source_bindings),
              version: version + 1,
              source_bindings: source_bindings,
              upload_links: upload_links,
              authority_index: authority_index,
              unique_index: unique_index,
              blob_owners: blob_owners,
              change_log_touched: rank_change_persons(store.change_log_touched, heads, audit)
          }

          Persistence.committed(committed)
          {:ok, committed}

        error ->
          LargeData.cleanup_created(prepared.created)
          keyed_commit_failure(store, error, :outcome_unknown, audit)
      end
    else
      error -> keyed_commit_failure(store, error, :unavailable, audit)
    end
  end

  # `config :attention_server, :keyed_validation` selects the keyed commit
  # validator: :incremental (production) uses the per-address verdict,
  # :full runs the whole-state check, and :both runs both and raises when
  # they disagree. See docs/feature-flags.md.
  defp keyed_validation(store, next, touched) do
    case Application.get_env(:attention_server, :keyed_validation, :full) do
      :incremental ->
        if incremental_verdict(store, next, touched) == :ok, do: :ok, else: :invalid_state

      :both ->
        full = valid_state?(next)
        incremental = incremental_verdict(store, next, touched)

        if full != (incremental == :ok) do
          raise "keyed validation mismatch: " <>
                  if(full,
                    do: "full accepted, incremental refused #{inspect(incremental)}",
                    else: "incremental accepted, full refused"
                  ) <> " for touched #{inspect(Enum.sort(touched), limit: 50)}"
        end

        if full, do: :ok, else: :invalid_state

      _ ->
        if valid_state?(next), do: :ok, else: :invalid_state
    end
  end

  @doc false
  # The per-address keyed verdict: :ok or {:error, detail}, where detail names
  # the refusing address or concern. The committed engine satisfied the full
  # validator, and untouched_change/3 has proved every changed record is
  # touched. A regular transition (preflight passes, and no message is removed
  # or re-originated, no published conversation event is rewritten or
  # removed, no free-order high water drops) is checked per touched address
  # plus the rows whose validity reads a touched record. Any other transition
  # falls back to the whole-state check, so the verdict always equals it.
  def incremental_verdict(%__MODULE__{engine: old} = store, %Engine{} = next, %MapSet{} = touched) do
    case irregular_transition(old, next, touched) do
      nil ->
        regular_verdict(store, old, next, touched)

      reason ->
        if valid_state?(next), do: :ok, else: {:error, {:irregular, reason}}
    end
  end

  def incremental_verdict(_store, _next, _touched), do: {:error, :invalid_state}

  defp irregular_transition(old, next, touched) do
    case KeyedValidation.preflight(old, next, touched) do
      :ok -> Enum.find_value(touched, &irregular_change(old, next, &1))
      _ -> :preflight
    end
  rescue
    _ -> :exception
  end

  # Delivery acks read the message origin, and events, chat positions and
  # reply scores require the message, so these stay in place.
  defp irregular_change(old, next, {:row, :messages, key}) do
    case {Map.fetch(old.messages, key), Map.fetch(next.messages, key)} do
      {{:ok, _}, :error} ->
        :message_removed

      {{:ok, prior}, {:ok, record}} ->
        if Map.get(prior, :origin) === Map.get(record, :origin), do: nil, else: :message_origin

      _ ->
        nil
    end
  end

  # Chat positions, reply scores and delivery acks read published events.
  defp irregular_change(old, next, {:row, :conversation_events, key}) do
    case Map.fetch(old.conversation_events, key) do
      {:ok, published} ->
        if Map.fetch(next.conversation_events, key) === {:ok, published},
          do: nil,
          else: :conversation_event_rewritten

      :error ->
        nil
    end
  end

  # Every message and conversation event arrival reads its person's high.
  defp irregular_change(old, next, {:row, :free_order_highs, person}) do
    case {Map.fetch(old.free_order_highs, person), Map.fetch(next.free_order_highs, person)} do
      {:error, _} -> nil
      {{:ok, prior}, {:ok, high}} when is_integer(high) and high >= prior -> nil
      _ -> :free_order_lowered
    end
  end

  defp irregular_change(_old, _next, _address), do: nil

  defp regular_verdict(store, old, next, touched) do
    items = for {:row, :messages, key} <- touched, do: key

    with :ok <- tagged(:sequences, fn -> KeyedValidation.sequences(old, next, touched) end),
         :ok <-
           tagged(:source_bindings, fn ->
             KeyedValidation.source_changes(
               store.source_bindings,
               old.messages,
               next.messages,
               items
             )
           end),
         {:ok, {unique_index, conflict}} <-
           rescued(:unique, fn ->
             KeyedValidation.unique_changes(store.unique_index, old, next, touched)
           end),
         {:ok, addresses} <-
           tagged(:dependents, fn ->
             keyed_addresses(store, unique_index, old, next, touched)
           end),
         :ok <- first_invalid_address(next, addresses),
         :ok <- unique_verdict(conflict) do
      derived_verdict(old, next, touched)
    end
  end

  defp rescued(tag, check) do
    {:ok, check.()}
  rescue
    _ -> {:error, tag}
  end

  # A value the full validator requires to be unique (owner keys, the shared
  # principal key space, pending secrets, hosted persons, agent ids, active
  # push addresses), now carried by a touched row and another row.
  defp unique_verdict(nil), do: :ok
  defp unique_verdict({namespace, _value}), do: {:error, {:unique, namespace}}

  defp tagged(tag, check) do
    case check.() do
      :ok -> :ok
      {:ok, %MapSet{} = addresses} -> {:ok, addresses}
      {:ok, _} -> :ok
      _ -> {:error, tag}
    end
  rescue
    _ -> {:error, tag}
  end

  # Touched addresses, plus the untouched rows whose validity reads a touched
  # record: the authority index names rows owned by a removed or lowered
  # person or by a device or agent whose identity changed, and rows bound to a
  # pending code; the upload index names entries and message revisions that
  # reference a touched upload, so removing an upload rechecks them; the
  # uniqueness index names the hosted accounts of a touched owner row; a
  # touched device rechecks its push bindings, which read `revoked`; and
  # bounce state is rechecked where it reads any of these (bounce_dependents/3).
  defp keyed_addresses(store, unique_index, old, next, touched) do
    {_index, affected} =
      KeyedValidation.authority_changes(store.authority_index, old, next, touched)

    {:ok, _links, related} =
      KeyedValidation.upload_changes(store.upload_links, old, next, touched)

    addresses =
      touched
      |> MapSet.union(affected)
      |> MapSet.union(related)
      |> MapSet.union(MapSet.new(Enum.flat_map(touched, &local_dependents/1)))
      |> MapSet.union(
        MapSet.new(Enum.flat_map(touched, &record_dependents(unique_index, old, next, &1)))
      )

    {:ok,
     MapSet.union(
       addresses,
       MapSet.new(Enum.flat_map(addresses, &bounce_dependents(old, next, &1)))
     )}
  end

  # Bounce state (#30-#34) that reads another record. A message row is read
  # by its bounce_revisions row (revision), its opened revisions, its waiting
  # item and deadline entry (origin and session) and, when its session
  # changes, its advisory entries. A bounce_revisions row is read by its
  # opened revisions, waiting item and deadline entry, and by the advisory
  # entries of a bounced selection it changes. A waiting route is read by the
  # deadline entries of the items it held or holds, and an origin session by
  # its advisory feed header. Origin sessions of a removed or reassigned
  # device arrive here through the authority index. A touched advisory fact
  # rechecks its feed header, the entries a lowered high drops and the
  # receipts of a rewritten entry. Removing or re-originating a message falls
  # back to the whole-state check (irregular_change/3); origin feed highs
  # never drop on a regular transition (KeyedValidation.preflight/3).
  defp bounce_dependents(old, next, {:row, :messages, key}) do
    prior = Map.get(old.messages, key)
    record = Map.get(next.messages, key)

    entries =
      if is_map(prior) and is_map(record) and
           Map.get(prior, :session_handle) !== Map.get(record, :session_handle),
         do: advisory_entries(next, key, prior),
         else: []

    [{:row, :bounce_revisions, key} | bounce_item_dependents(old, key)] ++ entries
  end

  defp bounce_dependents(old, next, {:row, :bounce_revisions, key}) do
    prior = Map.get(old.messages, key)

    entries =
      if bounced_selection_changed?(
           Map.get(old.bounce_revisions, key),
           Map.get(next.bounce_revisions, key)
         ),
         do: advisory_entries(next, key, prior),
         else: []

    bounce_item_dependents(old, key) ++ entries
  end

  defp bounce_dependents(old, next, {:row, :bounce_waiting_routes, session_key}) do
    for engine <- [old, next],
        %MapSet{} = items <- [Map.get(engine.bounce_waiting_routes, session_key)],
        {key, _sub_revision} <- items,
        uniq: true,
        do: {:bounce_queue, key}
  end

  defp bounce_dependents(_old, _next, {:row, :origin_sessions, feed_key}),
    do: [{:advisory, feed_key, :high}]

  defp bounce_dependents(old, next, {:advisory, feed_key, part}),
    do: [{:advisory, feed_key, :high} | advisory_dependents(old, next, feed_key, part)]

  defp bounce_dependents(_old, _next, _address), do: []

  # Every existing opened revision of a message names a substantive revision
  # of its old bounce row, and an untouched waiting route can hold only the
  # message's old pending item under its old session, so those are what a
  # change to the message or its bounce row can invalidate. A touched route
  # is checked whole.
  defp bounce_item_dependents(old, key) do
    opened =
      case Map.get(old.bounce_revisions, key) do
        %{revisions: revisions} when is_map(revisions) ->
          for sub_revision <- Map.keys(revisions),
              do: {:member, :opened_revisions, Tuple.insert_at(key, 3, sub_revision)}

        _ ->
          []
      end

    waiting =
      case BounceDecision.waiting_item(old, key) do
        {session_key, item} -> [{:bounce_route_item, session_key, item}]
        nil -> []
      end

    [{:bounce_queue, key} | opened ++ waiting]
  end

  # A valid entry for a message sits in the feed of the message's old origin
  # and session; only a changed session or bounced selection reads them all.
  defp advisory_entries(next, {person, _sender, _message_id} = key, prior) when is_map(prior) do
    feed_key = {person, Map.get(prior, :origin), Map.get(prior, :session_handle)}

    for sequence <- BounceDecision.advisory_sequences(next, feed_key, key),
        do: {:advisory, feed_key, {:entry, sequence}}
  end

  defp advisory_entries(_next, _key, _prior), do: []

  defp bounced_selection_changed?(%{revisions: revisions}, state) when is_map(revisions) do
    Enum.any?(revisions, fn
      {sub_revision, %{selection: %{"decision" => "bounced"} = selection}} ->
        not match?(%{revisions: %{^sub_revision => %{selection: ^selection}}}, state)

      _ ->
        false
    end)
  end

  defp bounced_selection_changed?(_prior, _state), do: false

  defp advisory_dependents(old, next, feed_key, :high) do
    with %{high: before} when is_integer(before) <- Map.get(old.advisory_feeds, feed_key),
         %{high: high} when is_integer(high) and high >= 0 and high < before <-
           Map.get(next.advisory_feeds, feed_key) do
      for sequence <- (high + 1)..before//1, do: {:advisory, feed_key, {:entry, sequence}}
    else
      _ -> []
    end
  end

  defp advisory_dependents(old, next, feed_key, {:entry, sequence}) do
    case get_in(old.advisory_feeds, [feed_key, :entries, sequence]) do
      %{"advisory_id" => advisory_id} = entry ->
        if get_in(next.advisory_feeds, [feed_key, :entries, sequence]) === entry,
          do: [],
          else: [{:advisory, feed_key, {:receipt, advisory_id}}]

      _ ->
        []
    end
  end

  defp advisory_dependents(_old, _next, _feed_key, _part), do: []

  # Hosted accounts read only whether their person has an owner row (#18).
  defp record_dependents(unique_index, _old, _next, {:row, :owners, person}),
    do: MapSet.to_list(KeyedValidation.unique_rows(unique_index, :hosted_person, person))

  # An active binding needs its device not revoked (#23). A changed device
  # person is already covered by the authority index.
  defp record_dependents(_unique_index, old, next, {:row, :devices, key}) do
    for engine <- [old, next],
        %{person: person} <- [Map.get(engine.devices, key)],
        uniq: true,
        do: {:row, :push_bindings, {person, key}}
  end

  defp record_dependents(_unique_index, _old, _next, _address), do: []

  defp local_dependents({:row, :persons, person}), do: [{:row, :conversation_highs, person}]

  defp local_dependents({:row, :messages, key}), do: [{:row, :item_availability, key}]

  defp local_dependents({:row, :chat_read_positions, key}),
    do: [{:row, :reply_group_history, key}]

  defp local_dependents({:row, :pending_enrollments, code}), do: [{:row, :pending_history, code}]
  defp local_dependents({:row, :pending_history, code}), do: [{:row, :pending_enrollments, code}]

  defp local_dependents({:origin_feed, {person, origin} = scope, {stage, seq}})
       when stage in [:entry, :delivery_ack],
       do: [
         {:row, :origin_feeds, scope},
         {:row, :origin_receipts, {person, origin, seq}},
         {:row, :origin_actions, {person, origin, seq}}
       ]

  defp local_dependents({:origin_feed, scope, :high}), do: [{:row, :origin_feeds, scope}]
  defp local_dependents(_address), do: []

  defp first_invalid_address(next, addresses) do
    case Enum.find(addresses, &(not valid_keyed_address?(next, &1))) do
      nil -> :ok
      address -> {:error, {:address, address}}
    end
  end

  defp valid_keyed_address?(engine, address) do
    keyed_address_valid?(engine, address)
  rescue
    _ -> false
  end

  # The full validator's per-row predicates, applied to one address of the
  # next engine. An absent row is valid here; what referenced it is rechecked
  # through keyed_addresses/4.
  defp keyed_address_valid?(engine, {:field, :relay_instance_id}),
    do: valid_instance?(engine.relay_instance_id)

  defp keyed_address_valid?(engine, {:field, :agent_cursor_key}),
    do: valid_cursor_key?(engine.agent_cursor_key)

  defp keyed_address_valid?(engine, {:field, field})
       when field in [:pending_clock_high_water, :browser_sign_in_clock_high_water],
       do: nonnegative_integer?(Map.fetch!(engine, field))

  defp keyed_address_valid?(engine, {:field, :jev_endpoint}),
    do: valid_jev_endpoint?(engine.jev_endpoint)

  defp keyed_address_valid?(engine, {:field, :jev_model}), do: valid_jev_model?(engine.jev_model)

  # Person highs and event rows: preflight and KeyedValidation.sequences/3.
  defp keyed_address_valid?(_engine, {:row, field, _key})
       when field in [:persons, :events],
       do: true

  defp keyed_address_valid?(engine, {:row, :conversation_events, {person, _} = key}) do
    case Map.fetch(engine.conversation_events, key) do
      {:ok, event} -> valid_arrival?(event, Map.get(engine.free_order_highs, person, 0))
      :error -> true
    end
  end

  defp keyed_address_valid?(engine, {:member, :retired_owner_keys, key}),
    do:
      not MapSet.member?(engine.retired_owner_keys, key) or
        valid_retired_owner_key?(engine, key)

  defp keyed_address_valid?(engine, {:member, :conversation_opened, {person, key} = member}),
    do:
      not MapSet.member?(engine.conversation_opened, member) or
        owns_or_browser?(engine, person, key)

  # Origin feed facts: preflight checks the high, contiguity, new entries,
  # delivery acks and the owning device of every touched scope.
  defp keyed_address_valid?(engine, {:row, :origin_feeds, {person, origin} = scope}) do
    not Map.has_key?(engine.origin_feeds, scope) or
      get_in(engine.devices, [origin, :person]) == person
  end

  defp keyed_address_valid?(engine, {:origin_feed, {person, _origin} = scope, {:entry, seq}}) do
    case get_in(engine.origin_feeds, [scope, :entries, seq]) do
      nil -> true
      entry -> valid_entry_uploads?(engine, person, entry)
    end
  end

  defp keyed_address_valid?(_engine, {:origin_feed, _scope, _fact}), do: true

  # Bounce state (#30-#34): see bounce_dependents/3 for what rechecks these.
  defp keyed_address_valid?(engine, {:row, :bounce_revisions, key}) do
    case Map.fetch(engine.bounce_revisions, key) do
      {:ok, state} -> BounceDecision.valid_revision_state?(engine, key, state)
      :error -> true
    end
  end

  defp keyed_address_valid?(engine, {:row, :bounce_waiting_routes, session_key}) do
    case Map.fetch(engine.bounce_waiting_routes, session_key) do
      {:ok, items} -> BounceDecision.valid_waiting_route?(engine, session_key, items)
      :error -> true
    end
  end

  defp keyed_address_valid?(engine, {:member, :opened_revisions, member}),
    do:
      not MapSet.member?(engine.opened_revisions, member) or
        BounceDecision.valid_opened_revision?(engine, member)

  defp keyed_address_valid?(engine, {:advisory, feed_key, part}),
    do: BounceDecision.valid_advisory_fact?(engine, feed_key, part)

  # Not record addresses: one item of a waiting route, and a message's share
  # of the derived deadline index.
  defp keyed_address_valid?(engine, {:bounce_route_item, session_key, item}) do
    case Map.get(engine.bounce_waiting_routes, session_key) do
      %MapSet{} = items ->
        not MapSet.member?(items, item) or
          BounceDecision.valid_waiting_item?(engine, session_key, item)

      _ ->
        true
    end
  end

  defp keyed_address_valid?(engine, {:bounce_queue, key}),
    do: BounceDecision.valid_queue_entry?(engine, key)

  defp keyed_address_valid?(engine, {:row, field, key}) do
    case Map.fetch(Map.fetch!(engine, field), key) do
      {:ok, row} -> valid_keyed_row?(engine, field, key, row)
      :error -> keyed_row_field?(field)
    end
  end

  defp keyed_address_valid?(_engine, _address), do: false

  @keyed_row_fields [
    :owners,
    :people_profiles,
    :pending_enrollments,
    :pending_history,
    :join_invitations,
    :browser_sign_ins,
    :maintainer_grants,
    :hosted_accounts,
    :hosted_organizations,
    :local_organizations,
    :delivery_states,
    :algorithm_choices,
    :alert_choices,
    :push_bindings,
    :origin_sessions,
    :item_availability,
    :origin_receipts,
    :origin_actions,
    :bounce_levels,
    :uploads,
    :devices,
    :agents,
    :sender_grants,
    :subscriptions,
    :cursors,
    :conversation_cursors,
    :receipts,
    :source_report_grants,
    :enrollment_read_grants,
    :messages,
    :shared_messages,
    :shared_recipients,
    :shared_entries,
    :shared_seen,
    :shared_answers,
    :free_order_highs,
    :conversation_highs,
    :chat_read_positions,
    :reply_group_history
  ]

  defp keyed_row_field?(field), do: field in @keyed_row_fields

  defp valid_keyed_row?(engine, :owners, person, owner), do: valid_owner?(engine, person, owner)

  defp valid_keyed_row?(engine, :people_profiles, person, profile),
    do: Map.has_key?(engine.persons, person) and AttentionServer.ClientAPI.PeopleProfiles.valid?(profile)

  defp valid_keyed_row?(engine, :pending_enrollments, code, item),
    do: not Map.has_key?(engine.pending_history, code) and valid_pending_item?(engine, code, item)

  defp valid_keyed_row?(engine, :pending_history, code, receipt),
    do:
      not Map.has_key?(engine.pending_enrollments, code) and
        valid_pending_resolution?(engine, code, receipt)

  defp valid_keyed_row?(engine, :join_invitations, hash, invite),
    do: GuidedJoin.valid_record?(engine, hash, invite)

  defp valid_keyed_row?(engine, :browser_sign_ins, hash, record),
    do: BrowserSignIn.valid_record?(engine, hash, record)

  defp valid_keyed_row?(engine, :maintainer_grants, id, grant),
    do: valid_maintainer?(engine, id, grant)

  defp valid_keyed_row?(engine, :hosted_accounts, ref, record),
    do: valid_hosted_account?(engine, ref, record)

  defp valid_keyed_row?(engine, :hosted_organizations, ref, record),
    do: Engine.valid_hosted_organization?(engine, ref, record)

  defp valid_keyed_row?(engine, :local_organizations, key, members),
    do: Engine.valid_local_organization?(engine, key, members)

  defp valid_keyed_row?(engine, :delivery_states, person, state),
    do: valid_delivery_state?(engine, person, state)

  defp valid_keyed_row?(engine, :algorithm_choices, person, choice),
    do: valid_algorithm_choice?(engine, person, choice)

  defp valid_keyed_row?(engine, :alert_choices, person, choice),
    do: valid_alert_choice?(engine, person, choice)

  defp valid_keyed_row?(engine, :push_bindings, key, binding),
    do: valid_push_binding_row?(engine, key, binding)

  defp valid_keyed_row?(engine, :origin_sessions, key, session),
    do: Conversation.valid_origin_session?(engine, key, session)

  defp valid_keyed_row?(engine, :item_availability, key, value),
    do: valid_item_availability?(engine, key, value)

  defp valid_keyed_row?(engine, :origin_receipts, key, receipt),
    do: Conversation.valid_origin_receipt?(engine, key, receipt)

  defp valid_keyed_row?(engine, :origin_actions, key, action),
    do: Conversation.valid_origin_action?(engine, key, action)

  defp valid_keyed_row?(engine, :bounce_levels, person, config),
    do: BounceDecision.valid_configuration?(engine, person, config)

  defp valid_keyed_row?(engine, :uploads, id, upload), do: valid_upload?(engine, id, upload)
  defp valid_keyed_row?(engine, :devices, _key, device), do: valid_device?(engine, device)
  defp valid_keyed_row?(engine, :agents, key, agent), do: valid_agent?(engine, key, agent)

  defp valid_keyed_row?(engine, field, {person, key}, _row)
       when field in [:sender_grants, :subscriptions],
       do: owns?(engine, person, key)

  defp valid_keyed_row?(engine, :cursors, key, seq), do: valid_cursor?(engine, key, seq)

  defp valid_keyed_row?(engine, :conversation_cursors, key, seq),
    do: valid_conversation_cursor?(engine, key, seq)

  defp valid_keyed_row?(engine, :receipts, {person, key, _op}, _receipt),
    do: receipt_actor?(engine, person, key)

  defp valid_keyed_row?(engine, :source_report_grants, key, grant),
    do: valid_source_report_grant?(engine, key, grant)

  defp valid_keyed_row?(engine, :enrollment_read_grants, id, grant),
    do: valid_enrollment_read_grant?(engine, id, grant)

  # Includes the revision files' upload references, which the reviewed draft
  # lacked.
  defp valid_keyed_row?(engine, :messages, {person, _, _} = key, record),
    do:
      valid_item?(engine, key, record) and
        valid_arrival?(record, Map.get(engine.free_order_highs, person, 0))

  defp valid_keyed_row?(engine, :shared_messages, key, record),
    do: AttentionServer.SharedMessages.valid_message?(engine, key, record)

  defp valid_keyed_row?(engine, :shared_recipients, key, record),
    do: AttentionServer.SharedMessages.valid_recipient?(engine, key, record)

  defp valid_keyed_row?(engine, :shared_entries, key, record),
    do: SharedMessages.valid_entry?(engine, key, record)

  defp valid_keyed_row?(engine, :shared_seen, key, record),
    do: SharedMessages.valid_seen?(engine, key, record)

  defp valid_keyed_row?(engine, :shared_answers, key, record),
    do: SharedAnswers.valid?(engine, key, record)

  defp valid_keyed_row?(engine, :free_order_highs, person, high),
    do: valid_order_high?(engine, person, high)

  defp valid_keyed_row?(engine, :conversation_highs, person, high),
    do: valid_conversation_high?(engine, person, high)

  defp valid_keyed_row?(engine, :chat_read_positions, key, position),
    do: valid_chat_position?(engine, key, position)

  defp valid_keyed_row?(engine, :reply_group_history, key, history),
    do: valid_reply_history?(engine, key, history)

  defp valid_keyed_row?(_engine, _field, _key, _row), do: false

  # Memory-only indexes are not addressable. The engine updates them with
  # the records they derive from, so each is compared, for the touched
  # records only, with what a rebuild for those records gives: the open
  # default of a touched message (#3), and the bounce discussion,
  # origin-revision and advisory id indexes (#34). The bounce deadline index
  # is checked per message as {:bounce_queue, key}.
  defp derived_verdict(old, next, touched) do
    [
      {:open_default_index, fn -> valid_open_defaults?(next, touched) end},
      {:bounce_discussed_messages,
       fn -> BounceDecision.valid_discussed_change?(old, next, touched) end},
      {:bounce_origin_entries,
       fn -> BounceDecision.valid_origin_entries_change?(next, touched) end},
      {:advisory_id_index,
       fn -> BounceDecision.valid_advisory_index_change?(old, next, touched) end}
    ]
    |> Enum.find_value(:ok, fn {index, check} ->
      if not derived_valid?(check), do: {:error, {:derived, index}}
    end)
  end

  defp derived_valid?(check) do
    check.()
  rescue
    _ -> false
  end

  # The whole-state check pins only the shape of this memory-only index, so
  # the per-address check pins the same shape for touched messages and does
  # not compare due times. Engine.put_message maintains the values.
  defp valid_open_defaults?(engine, touched) do
    is_map(engine.open_default_index) and
      Enum.all?(touched, fn
        {:row, :messages, key} ->
          case Map.fetch(engine.open_default_index, key) do
            {:ok, at} -> is_tuple(key) and tuple_size(key) == 3 and is_integer(at)
            :error -> true
          end

        _ ->
          true
      end)
  end

  defp update_blob_owners(owners, prepared) do
    case KeyedReference.update_blob_owners(owners, prepared.blob_delta) do
      {:ok, _} = updated ->
        updated

      error ->
        LargeData.cleanup_created(prepared.created)
        error
    end
  end

  defp keyed_commit_failure(store, error, outcome, audit) do
    reason =
      case error do
        {:error, {:untouched_change, _address}} -> :untouched_change
        {:error, reason} -> reason
        reason -> reason
      end

    Diagnostics.failure(:server_storage, :state_commit, reason, outcome, operation_id(audit))
    {:error, :storage_unavailable, store}
  end

  defp keyed_change_log(%__MODULE__{change_log: false}, _next, _touched, _projected), do: nil

  # A keyed commit derives its log rows from the touched addresses, so the
  # work follows the change. Under `keyed_validation: :both` (the test
  # environment) the whole-map log is computed as well and any differing part
  # raises, as keyed_validation/3 does for the validator. :full and
  # :incremental use the touched rows alone.
  defp keyed_change_log(store, next, touched, projected) do
    log = AttentionServer.ChangeLog.commit_log_touched(store.engine, next, touched, projected)

    if Application.get_env(:attention_server, :keyed_validation, :full) == :both do
      whole = AttentionServer.ChangeLog.commit_log(store.engine, next, projected)

      case AttentionServer.ChangeLog.commit_log_mismatches(log, whole) do
        [] ->
          :ok

        mismatches ->
          raise "keyed change log mismatch in " <>
                  Enum.map_join(mismatches, ", ", &inspect(elem(&1, 0))) <>
                  ": " <>
                  inspect(
                    Enum.map(mismatches, fn {part, a, b} ->
                      %{part: part, touched: a, whole: b}
                    end),
                    limit: 50
                  ) <> " for touched #{inspect(Enum.sort(touched), limit: 50)}"
      end
    end

    log
  end

  defp write_reference_delta(db, deleted_paths, reference_rows) do
    with :ok <-
           each_ok(deleted_paths, fn path ->
             run(db, "DELETE FROM relay_large_data_refs WHERE path = ?", [{:blob, path}])
           end) do
      each_ok(reference_rows, fn
        [path, marker, id, size, sha256] ->
          run(
            db,
            "INSERT INTO relay_large_data_refs (path, marker, id, size, sha256) VALUES (?, ?, ?, ?, ?)",
            [{:blob, path}, marker, id, size, sha256]
          )

        _row ->
          {:error, :invalid_large_data_reference}
      end)
    end
  end

  defp each_ok(rows, write) do
    Enum.reduce_while(rows, :ok, fn row, :ok ->
      case write.(row) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp shared_send_log(log, engine, %{shared_send_receipt: {scope, _, _, id, outcomes}}) do
    session = Map.get(Map.fetch!(engine.shared_messages, id), "session")
    log
    |> AgentChangeLog.append_shared_message(scope, id, session)
    |> AttentionServer.ChangeLog.append_shared_send(id, outcomes)
  end

  defp shared_send_log(log, _engine, _audit), do: log

  defp shared_mutation_log(log, engine, %{shared_mutation_receipt: {scope, _, _, id}}) do
    session = Map.get(Map.fetch!(engine.shared_messages, id), "session")
    log = AgentChangeLog.append_shared_message(log, scope, id, session)

    Enum.reduce(SharedMessages.live_recipients(engine, id), log, fn person, acc ->
      AttentionServer.ChangeLog.append_shared_freeze(acc, person, id)
    end)
  end

  defp shared_mutation_log(log, _engine, _audit), do: log

  defp shared_freeze_log(nil, _old, _next, _touched), do: nil

  defp shared_freeze_log(log, old, next, touched) do
    Enum.reduce(touched, log, fn
      {:row, :shared_recipients, {id, person} = key}, acc ->
        case {old.shared_recipients[key], next.shared_recipients[key]} do
          {%{frozen: _}, _} -> acc
          {%{}, %{frozen: _}} -> AttentionServer.ChangeLog.append_shared_freeze(acc, person, id)
          _ -> acc
        end

      {:row, :shared_messages, id}, acc ->
        case {old.shared_messages[id], next.shared_messages[id]} do
          {%{sender_frozen_revision: _}, _} -> acc
          {%{}, %{sender_frozen_revision: _, sender_person: person, sender_key: key} = record} ->
            AgentChangeLog.append_shared_message(acc, {person, key}, id, Map.get(record, "session"))

          _ -> acc
        end

      _, acc ->
        acc
    end)
  end

  defp shared_chat_log(log, engine, %{shared_chat: %{operation: operation, message_id: id} = chat}) do
    record = Map.fetch!(engine.shared_messages, id)
    session = Map.get(record, "session")

    case operation do
      :reply ->
        entry = Map.fetch!(engine.shared_entries, chat.entry_id)
        inbox_log =
          Enum.reduce(SharedMessages.live_recipients(engine, id), log, fn person, acc ->
            AttentionServer.ChangeLog.append_shared_entry(acc, person, id, entry.id)
          end)

        if Map.has_key?(record, :sender_frozen_revision),
          do: inbox_log,
          else: AgentChangeLog.append_shared_entry(inbox_log, {record.sender_person, record.sender_key}, entry.id, session)

      :answer ->
        shared_answer_log(log, engine, record, chat.answer_id)

      :shared_answer_receipt ->
        AgentChangeLog.append_shared_answer(log, {record.sender_person, record.sender_key}, chat.answer_id, session)

      :seen ->
        log =
          Enum.reduce(SharedMessages.live_recipients(engine, id), log, fn person, acc ->
            AttentionServer.ChangeLog.append_shared_freeze(acc, person, id)
          end)

        if Map.has_key?(record, :sender_frozen_revision),
          do: log,
          else: AgentChangeLog.append_shared_seen(log, {record.sender_person, record.sender_key}, chat.event_id, session)

      :read_position ->
        AttentionServer.ChangeLog.append_shared_freeze(log, chat.person, id)

      :chat_receipt ->
        entry = Map.fetch!(engine.shared_entries, chat.entry_id)
        AgentChangeLog.append_shared_entry(log, {record.sender_person, record.sender_key}, entry.id, session)
    end
  end

  defp shared_chat_log(log, engine, %{shared_default_answers: ids}) do
    Enum.reduce(ids, log, fn id, acc ->
      answer = Map.fetch!(engine.shared_answers, id)
      shared_answer_log(acc, engine, Map.fetch!(engine.shared_messages, answer.shared_id), id)
    end)
  end

  defp shared_chat_log(log, _engine, _audit), do: log

  defp shared_answer_log(log, engine, record, answer_id) do
    log =
      Enum.reduce(SharedMessages.live_recipients(engine, record.id), log, fn person, acc ->
        AttentionServer.ChangeLog.append_shared_freeze(acc, person, record.id)
      end)

    if Map.has_key?(record, :sender_frozen_revision) do
      log
    else
      scope = {record.sender_person, record.sender_key}
      session = record["session"]
      log
      |> AgentChangeLog.append_shared_message(scope, record.id, session)
      |> AgentChangeLog.append_shared_answer(scope, answer_id, session)
    end
  end

  defp write_shared_chat_receipt(store, engine, %{shared_chat: %{key: key} = chat})
       when is_binary(key) do
    with {:ok, wire} <- AttentionServer.ClientAPI.SharedChat.result(store, engine, chat) do
      Idempotency.save(store, chat.receipt_scope, key, chat.digest, 200, wire)
    end
  end

  defp write_shared_chat_receipt(_store, _engine, _audit), do: :ok

  defp write_shared_send_receipt(store, engine, %{
         shared_send_receipt: {{person, origin} = scope, key, digest, id, outcomes}
       }) do
    record = Map.fetch!(engine.shared_messages, id)

    with {:ok, rev} <- AgentChangeLog.head(store, scope),
         {:ok, resource_rev} <- AgentChangeLog.rev(store, scope, {:shared_message, id}) do
      body = %{
        "rev" => rev,
        "message" => SharedMessages.sender_message(engine, record, resource_rev),
        "recipients" => outcomes
      }

      Idempotency.save(store, Idempotency.agent_scope(person, origin), key, digest, 200, body)
    end
  end

  defp write_shared_send_receipt(_store, _engine, %{shared_mutation_receipt: {_, nil, _, _}}),
    do: :ok

  defp write_shared_send_receipt(store, engine, %{
         shared_mutation_receipt: {{person, origin} = scope, key, digest, id}
       }) do
    record = Map.fetch!(engine.shared_messages, id)

    with {:ok, rev} <- AgentChangeLog.head(store, scope),
         {:ok, resource_rev} <- AgentChangeLog.rev(store, scope, {:shared_message, id}) do
      body = %{"rev" => rev, "message" => SharedMessages.sender_message(engine, record, resource_rev)}
      Idempotency.save(store, Idempotency.agent_scope(person, origin), key, digest, 200, body)
    end
  end

  defp write_shared_send_receipt(_store, _engine, _audit), do: :ok

  defp write_auth_activation(db, engine, %{auth_activation: devices}) do
    with {:ok, %{} = _schema} <- AuthSchema.initialize_in_transaction(db, engine, devices),
         do: :ok
  end

  defp write_auth_activation(_db, _engine, _audit), do: :ok

  defp write_app_receipt(%{app_receipt_capture: capture}, store), do: capture.(store)
  defp write_app_receipt(_audit, _store), do: :ok

  defp write_auth_changes(db, %{person: person, actor: owner, browser_session: {hash, revision, issuer}}),
    do: BrowserAuth.add_session(db, hash, person, owner, revision, issuer)

  defp write_auth_changes(db, %{native_redemption: redemption}),
    do: NativeSignIn.commit_redemption(db, redemption)

  defp write_auth_changes(db, %{person: person, auth_device_removal: device}) do
    with :ok <- NativeSignIn.remove_device_sessions(db, person, device),
         do: DeviceMigration.set_phase(db, device, "removed")
  end

  defp write_auth_changes(_db, _audit), do: :ok

  defp write_audit(db, audit) do
    run(db, "INSERT INTO relay_audit (person_id, actor_id, action, at_ms) VALUES (?, ?, ?, ?)", [
      audit.person,
      audit.actor,
      audit.action,
      System.system_time(:millisecond)
    ])
  end

  # A blob whose last owning record was rewritten or removed in this commit
  # is deleted after the commit is durable. A failed unlink leaves an orphan
  # for the next open's cleanup.
  defp prune_released_blobs(nil, _candidates, _owners, _engine, _audit), do: :ok

  defp prune_released_blobs(directory, candidates, owners, engine, audit) do
    released = candidates |> Enum.uniq() |> Enum.reject(&Map.has_key?(owners, &1))
    result = LargeData.delete_released(directory, released, engine)

    Diagnostics.result(
      :server_storage,
      :large_data_delete,
      result,
      :unavailable,
      operation_id(audit)
    )
  end

  # These pages only update the availability checkpoint on existing sessions.
  # Full snapshot validation would traverse unrelated Items and their bodies.
  defp do_commit(%__MODULE__{change_log: true} = store, next, %{item_refresh: _} = audit, []),
    do: commit_durable(store, next, audit)

  defp do_commit(%__MODULE__{engine: old} = store, next, audit, [])
       when old == next and not is_map_key(audit, :native_redemption),
    do: {:ok, store}

  defp do_commit(%__MODULE__{db: nil} = store, next, _audit, _projected_entries),
    do: {:ok, %{store | engine: next}}

  defp do_commit(%__MODULE__{} = store, next, audit, projected_entries) do
    cond do
      projected_entries == [] and progress_only?(store.engine, next) and valid_progress?(next) ->
        commit_progress(store, next, audit)

      valid_state?(next, :incremental) ->
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
    generation = Map.take(store, [:db, :version, :progress_version])

    # The change log's rows are found here, against the engine this process
    # already holds, and written in the snapshot's transaction.
    log =
      if store.change_log do
        case audit do
          %{item_refresh: keys} -> AttentionServer.ChangeLog.refresh_items(store.engine, keys)
          _ -> AttentionServer.ChangeLog.commit_log(store.engine, next, projected_entries)
        end
      end

    result =
      with {:ok, durable_next, persisted, index} <- prepare_large_data(store, next) do
        # The cache shares records and compressed fragments with this process's
        # previous snapshot. A task would flatten those terms on the way in
        # and copy a full replacement cache back. Only the encoded binary and
        # transaction inputs need to reach the worker.
        {payload, cache} =
          SnapshotEncoding.encode(persisted, @format_version, store.encoding_cache)

        rows = LargeData.reference_index_rows(index)

        with {:ok, heads, agent_heads} <-
               Persistence.run(fn -> persist_snapshot(generation, payload, rows, audit, log) end) do
          cleanup_snapshot_blobs(store, index, persisted, audit)
          {:ok, cache, heads, agent_heads, durable_next, persisted, index}
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
             change_log_touched: rank_change_persons(store.change_log_touched, heads, audit)
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

  defp cleanup_snapshot_blobs(%{upload_dir: dir} = store, index, persisted, audit)
       when is_binary(dir) do
    old_ids =
      if store.large_index,
        do: LargeData.reference_index_ids(store.large_index),
        else: LargeData.refs(store.engine)

    next_ids = LargeData.reference_index_ids(index)

    case LargeData.delete_unreferenced(dir, old_ids, next_ids, persisted) do
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

  defp cleanup_snapshot_blobs(_store, _index, _persisted, _audit), do: :ok

  defp persist_snapshot(%{db: db, version: version} = store, payload, reference_rows, audit, log) do
    checksum = :crypto.hash(:sha256, payload)

    with :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
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
         :ok <- record_committed_migration(db, audit),
         :ok <- write_auth_changes(db, audit),
         :ok <- Sqlite3.execute(db, "COMMIT") do
      {:ok, heads, agent_heads}
    end
  end

  defp record_committed_migration(db, %{actor: "schema_migration", action: @client_data_migration}),
    do: record_data_migration(db, @client_data_migration)

  defp record_committed_migration(_db, _audit), do: :ok

  defp record_data_migration(db, name),
    do: run(db, "INSERT OR IGNORE INTO relay_data_migrations (name) VALUES (?)", [name])

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

  defp load_or_create(db, upload_dir, purpose \\ :open) do
    case one(db, "SELECT version, payload, checksum FROM relay_state WHERE id = 1", []) do
      {:ok, nil} when purpose == :open ->
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

      {:ok, nil} ->
        {:error, :invalid_state}

      {:ok, [version, payload, checksum]} when is_integer(version) and is_binary(payload) ->
        if :crypto.hash(:sha256, payload) == checksum do
          try do
            # Safe decoding accepts only atoms already loaded by this VM.
            # Load the modules that define persisted record fields before a
            # cold release reads snapshots containing invitations, browser
            # sign-ins, JSON decimal values or provider state.
            for module <- [
                  Engine,
                  BounceDecision,
                  Conversation,
                  GuidedJoin,
                  BrowserSignIn,
                  AttentionServer.JSON.DecimalToken,
                  APNS,
                  AttentionServer.AgentAccess,
                  AttentionServer.Message
                ] do
              {:module, ^module} = Code.ensure_loaded(module)
            end

            _ = RecordCodec.identity()

            case :erlang.binary_to_term(payload, [:safe]) do
              {:attention_relay_keyed_state, 32, _identity} ->
                Logger.error(
                  "Harklight Store uses FORMAT32 rows. To run an older release, restore the pre-upgrade backup."
                )

                {:error, :format32_requires_keyed_mode}

              {:attention_relay_keyed_state, 31, _identity} ->
                Logger.error(
                  "Harklight Store uses FORMAT31 rows. To run a snapshot release, restore the pre-upgrade backup."
                )

                {:error, :format31_requires_keyed_mode}

              {:attention_relay_state, @format_version, %Engine{} = stored} ->
                load_format30(db, upload_dir, version, stored, purpose)

              {:attention_relay_state, format, %Engine{}} when is_integer(format) ->
                {:error, {:unsupported_store_format, format}}

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

  defp load_format30(db, upload_dir, version, stored, purpose) do
    original = struct(Engine, Map.from_struct(stored))

    with {:ok, store} <- load_progress(%__MODULE__{db: db, engine: original, version: version, upload_dir: upload_dir}) do
      engine =
        store.engine
        |> BrowserSignIn.normalize_snapshot_records()
        |> BounceDecision.rebuild_indexes()
        |> Engine.rebuild_route_b_index()

      cond do
        not valid_state?(engine) -> {:error, :invalid_state}
        purpose == :convert -> {:ok, %{store | engine: engine}}
        true -> {:ok, engine, version}
      end
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

  defp sync_large_data_references(%__MODULE__{db: db} = store, rows) do
    case RecordTable.transaction(db, fn ->
           with :ok <- check_open_generation(store),
                :ok <- write_large_data_references(db, rows) do
             {:ok, :synced}
           end
         end) do
      {:ok, :synced} -> :ok
      error -> error
    end
  end

  defp write_large_data_references(db, rows) do
    with :ok <- Sqlite3.execute(db, "DELETE FROM relay_large_data_refs") do
      Enum.reduce_while(rows, :ok, fn
        [path, marker, id, size, sha256], :ok ->
          case run(
                 db,
                 "INSERT INTO relay_large_data_refs (path, marker, id, size, sha256) VALUES (?, ?, ?, ?, ?)",
                 [{:blob, path}, marker, id, size, sha256]
               ) do
            :ok -> {:cont, :ok}
            error -> {:halt, error}
          end

        _row, :ok ->
          {:halt, {:error, :invalid_large_data_reference}}
      end)
    end
  end

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

  defp valid_arrival?(record, high) do
    case Map.get(record, :free_order) do
      nil -> true
      order -> is_integer(order) and order > 0 and order <= high
    end
  end

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


  defp valid_item_availability?(engine, key, {status, stamp})
       when status in ["listening", "not_listening", "unsupported", "gone"] do
    match?(%{session_handle: handle} when is_binary(handle), Map.get(engine.messages, key)) and
      (is_nil(stamp) or (is_integer(stamp) and stamp > 0))
  end

  defp valid_item_availability?(_engine, _key, _value), do: false

  # The durable hot path checks bounce index containers without rescanning their
  # retained rows. Store open performs the full cross-map bounce validation.
  defp valid_state?(%Engine{} = engine), do: valid_state?(engine, :full)

  defp valid_state?(%Engine{} = engine, bounce_validation) do
    try do
      valid_instance?(engine.relay_instance_id) and
        valid_cursor_key?(engine.agent_cursor_key) and
        valid_open_default_index?(engine) and
        nonnegative_integer?(engine.pending_clock_high_water) and
        Enum.all?(engine.persons, fn {person, high} ->
          is_binary(person) and is_integer(high) and high >= 0
        end) and
        valid_owners?(engine) and
        valid_people_profiles?(engine) and
        valid_pending?(engine) and
        valid_pending_history?(engine) and
        GuidedJoin.valid_state?(engine) and
        BrowserSignIn.valid_state?(engine) and
        valid_maintainers?(engine) and
        valid_hosted_accounts?(engine) and
        Enum.all?(engine.hosted_organizations, fn {ref, record} ->
          Engine.valid_hosted_organization?(engine, ref, record)
        end) and
        Enum.all?(engine.local_organizations, fn {key, members} ->
          Engine.valid_local_organization?(engine, key, members)
        end) and
        Enum.all?(engine.shared_messages, fn {key, record} ->
          AttentionServer.SharedMessages.valid_message?(engine, key, record)
        end) and
        Enum.all?(engine.shared_recipients, fn {key, record} ->
          AttentionServer.SharedMessages.valid_recipient?(engine, key, record)
        end) and
        Enum.all?(engine.shared_entries, fn {key, record} ->
          SharedMessages.valid_entry?(engine, key, record)
        end) and
        Enum.all?(engine.shared_seen, fn {key, record} ->
          SharedMessages.valid_seen?(engine, key, record)
        end) and
        Enum.all?(engine.shared_answers, fn {key, record} ->
          SharedAnswers.valid?(engine, key, record)
        end) and
        valid_delivery_states?(engine) and
        valid_prioritization?(engine) and
        valid_alert_choices?(engine) and
        valid_push_bindings?(engine) and
        AttentionServer.Conversation.valid_state?(engine) and
        Enum.all?(engine.item_availability, fn {key, value} ->
          valid_item_availability?(engine, key, value)
        end) and
        BounceDecision.valid_state?(engine, bounce_validation) and
        valid_uploads?(engine) and
        valid_upload_references?(engine) and
        LargeData.valid_refs?(engine) and
        Enum.all?(engine.devices, fn {_key, device} -> valid_device?(engine, device) end) and
        Enum.all?(engine.agents, fn {key, agent} -> valid_agent?(engine, key, agent) end) and
        unique_agent_ids?(engine) and
        Enum.all?(engine.sender_grants, fn {{person, key}, _} -> owns?(engine, person, key) end) and
        Enum.all?(engine.subscriptions, fn {{person, key}, _} -> owns?(engine, person, key) end) and
        Enum.all?(engine.cursors, fn {key, seq} -> valid_cursor?(engine, key, seq) end) and
        valid_free_order?(engine) and
        is_map(engine.conversation_highs) and
        Enum.all?(engine.conversation_highs, fn {person, high} ->
          valid_conversation_high?(engine, person, high)
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
        Enum.all?(engine.conversation_cursors, fn {key, seq} ->
          valid_conversation_cursor?(engine, key, seq)
        end) and
        is_struct(engine.conversation_opened, MapSet) and
        Enum.all?(engine.conversation_opened, fn {person, key} ->
          owns_or_browser?(engine, person, key)
        end) and
        Enum.all?(engine.receipts, fn {{person, key, _op}, _} ->
          receipt_actor?(engine, person, key)
        end) and
        Enum.all?(engine.source_report_grants, fn {key, grant} ->
          valid_source_report_grant?(engine, key, grant)
        end) and
        Enum.all?(engine.enrollment_read_grants, fn {grant_id, grant} ->
          valid_enrollment_read_grant?(engine, grant_id, grant)
        end) and
        Enum.all?(engine.messages, fn {key, record} -> valid_item?(engine, key, record) end) and
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

  defp valid_state?(_, _), do: false
  defp valid_state?(_), do: false

  defp valid_prioritization?(engine) do
    is_map(engine.algorithm_choices) and
      Enum.all?(engine.algorithm_choices, fn {person, choice} ->
        valid_algorithm_choice?(engine, person, choice)
      end) and
      valid_jev_endpoint?(engine.jev_endpoint) and valid_jev_model?(engine.jev_model)
  end

  defp valid_algorithm_choice?(engine, person, choice),
    do: Map.has_key?(engine.persons, person) and choice in ["baseline", "jev-set"]

  defp valid_alert_choices?(engine) do
    is_map(engine.alert_choices) and
      Enum.all?(engine.alert_choices, fn {person, choice} ->
        valid_alert_choice?(engine, person, choice)
      end)
  end

  defp valid_alert_choice?(engine, person, choice),
    do: Map.has_key?(engine.persons, person) and choice == "all"

  defp valid_jev_endpoint?(endpoint),
    do: is_nil(endpoint) or AttentionServer.JevClient.valid_endpoint?(endpoint)

  defp valid_jev_model?(model),
    do: is_nil(model) or AttentionServer.JevClient.valid_model?(model)

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
      (is_nil(upload[:shared_entry_id]) or is_binary(upload[:shared_entry_id])) and
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

  defp valid_cursor_key?(key), do: is_binary(key) and byte_size(key) == 32

  defp nonnegative_integer?(value), do: is_integer(value) and value >= 0

  defp valid_open_default_index?(engine) do
    is_map(engine.open_default_index) and
      Enum.all?(engine.open_default_index, fn {key, at} ->
        is_tuple(key) and tuple_size(key) == 3 and is_integer(at)
      end)
  end

  defp valid_cursor?(engine, {person, key}, seq) do
    owns_or_browser?(engine, person, key) and is_integer(seq) and seq >= 0 and
      seq <= engine.persons[person]
  end

  defp valid_conversation_cursor?(engine, {person, key}, seq) do
    owns_or_browser?(engine, person, key) and is_integer(seq) and seq >= 0 and
      seq <= Map.get(engine.conversation_highs, person, 0)
  end

  defp valid_conversation_high?(engine, person, high),
    do: Map.has_key?(engine.persons, person) and is_integer(high) and high >= 0

  defp valid_chat_position?(engine, {person, sender, id} = key, position) do
    Map.has_key?(engine.persons, person) and Map.has_key?(engine.messages, key) and
      is_binary(sender) and is_binary(id) and is_integer(position) and position > 0 and
      origin_reply_at?(engine, key, position)
  end

  defp valid_reply_history?(engine, key, history) do
    Map.has_key?(engine.messages, key) and is_map(history.scores) and
      is_number(history.score) and history.score >= 0 and history.score <= 1 and
      Enum.all?(history.scores, fn {seq, score} ->
        is_number(score) and score >= 0 and score <= 1 and
          seq > Map.get(engine.chat_read_positions, key, 0) and
          origin_reply_at?(engine, key, seq)
      end)
  end

  defp origin_reply_at?(engine, {person, _, _} = key, seq) do
    match?(
      %{key: ^key, payload: %{"kind" => "reply", "author" => "origin"}},
      Map.get(engine.conversation_events, {person, seq})
    )
  end

  defp valid_source_report_grant?(engine, {person, key, source}, grant) do
    owns?(engine, person, key) and nonempty?(source) and nonempty?(grant.delegation_ref) and
      is_boolean(grant.revoked) and is_struct(grant.senders, MapSet) and
      Enum.all?(grant.senders, &AttentionServer.Message.sender_id?/1)
  end

  defp valid_enrollment_read_grant?(engine, grant_id, grant) do
    AttentionServer.Message.uuid_v4?(grant_id) and
      Map.has_key?(engine.persons, grant.person) and
      owns?(engine, grant.person, grant.connector) and
      nonempty?(grant.source_instance_id) and nonempty?(grant.issuer) and
      is_boolean(grant.revoked) and valid_grant_target?(engine, grant.person, grant.target)
  end

  defp valid_item?(engine, {person, sender, id} = key, record) do
    (owns?(engine, person, record.origin) or
       (record.origin == sender and system_notice_actor?(engine, person, sender))) and
      record.message["sender_id"] == sender and record.message["message_id"] == id and
      valid_seq?(engine, person, record.accepted_seq) and
      valid_conversation_record?(engine, key, record) and valid_origin_result?(record) and
      (is_nil(record.terminal) or
         (valid_terminal_actor?(engine, person, record.terminal) and
            valid_seq?(engine, person, record.terminal.seq))) and
      valid_source_record?(record)
  end

  defp valid_conversation_record?(engine, key, record) do
    handle = Map.get(record, :session_handle)
    vendor = Map.get(record, :vendor_session_id)
    revision = Map.get(record, :revision, 1)
    revisions = Map.get(record, :revisions, %{1 => record.message})
    files_by_revision = Map.get(record, :files_by_revision, %{})

    files_valid? =
      is_map(files_by_revision) and
        Enum.all?(files_by_revision, fn {n, files} ->
          is_integer(n) and n >= 1 and n <= revision and
            AttentionServer.ConversationFiles.valid_stored?(files) and
            valid_revision_uploads?(engine, key, n, files)
        end)

    files_valid? and
      case handle do
        nil ->
          is_nil(vendor) and revision == 1

        _ ->
          AttentionServer.Conversation.valid_session?(handle, vendor) and
            is_integer(revision) and revision >= 1 and is_map(revisions) and
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

  defp valid_revision_uploads?(engine, {person, sender, message_id}, revision, files) do
    Enum.all?(files, fn
      %{"tier" => "upload", "upload_id" => id} = file ->
        case Map.get(engine.uploads, id) do
          %{person: ^person, sender_id: ^sender, message_id: ^message_id} = upload ->
            # A later revision can carry a file from this message's earlier revision.
            is_integer(upload.revision) and upload.revision <= revision and
              upload.name == file["name"] and upload.size == file["size"] and
              upload.sha256 == file["sha256"]

          _ ->
            false
        end

      _inline ->
        true
    end)
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
      Enum.all?(engine.push_bindings, fn {key, binding} ->
        valid_push_binding_row?(engine, key, binding)
      end)

    bindings_valid? and unique_push_addresses?(engine)
  end

  defp valid_push_binding_row?(engine, {person, device}, binding) when is_map(binding),
    do: owns?(engine, person, device) and valid_push_binding?(binding, engine, person, device)

  defp valid_push_binding_row?(_engine, _key, _binding), do: false

  defp unique_push_addresses?(engine) do
    addresses =
      for {_key, %{active: true, address: address}} <- engine.push_bindings, do: address

    length(addresses) == MapSet.size(MapSet.new(addresses))
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

    is_boolean(active) and is_boolean(Map.get(binding, :shared_messages, false)) and
      AttentionServer.SharedPush.valid?(binding) and
      is_integer(revision) and revision > 0 and
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
      valid_push_claim?(state, claim_id, claim_high, claim_conversation_high, claim_at, Map.get(binding, :claim_shared, false)) and
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

  defp valid_push_claim?("sending", claim_id, claim_high, claim_conversation_high, claim_at, shared?),
    do:
      AttentionServer.Message.uuid_v4?(claim_id) and is_integer(claim_high) and claim_high >= 0 and
        is_integer(claim_conversation_high) and claim_conversation_high >= 0 and
        (claim_high > 0 or claim_conversation_high > 0 or shared?) and is_integer(claim_at)

  defp valid_push_claim?(_state, claim_id, claim_high, claim_conversation_high, claim_at, _shared?),
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
      (is_nil(device.label) or
         (is_binary(device.label) and byte_size(device.label) >= 1)) and
      (is_nil(device.enrolled_at) or
         (is_integer(device.enrolled_at) and device.enrolled_at >= 0))
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

  defp unique_agent_ids?(engine) do
    engine.agents |> Map.values() |> Enum.map(&{&1.person, &1.id}) |> Enum.uniq() |> length() ==
      map_size(engine.agents)
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
      Enum.all?(retired, &valid_retired_owner_key?(engine, &1))
  end

  defp valid_owner?(engine, person, owner) do
    Map.has_key?(engine.persons, person) and
      hex_key?(owner.key) and is_integer(owner.revision) and owner.revision > 0 and
      not Map.has_key?(engine.devices, owner.key) and
      not Map.has_key?(engine.agents, owner.key) and
      not MapSet.member?(engine.retired_owner_keys, owner.key)
  end

  defp valid_retired_owner_key?(engine, key) do
    hex_key?(key) and not Map.has_key?(engine.devices, key) and
      not Map.has_key?(engine.agents, key)
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
      Enum.all?(history, fn {code, receipt} ->
        valid_pending_resolution?(engine, code, receipt)
      end) and
        Enum.all?(history, fn {code, _} -> not Map.has_key?(engine.pending_enrollments, code) end) and
        unique_pending_secrets?(engine)
    else
      false
    end
  end

  defp unique_pending_secrets?(engine) do
    secrets =
      Enum.map(Map.values(engine.pending_enrollments), & &1.secret_hash) ++
        Enum.map(Map.values(engine.pending_history), & &1.secret_hash)

    length(Enum.uniq(secrets)) == length(secrets)
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

  defp valid_people_profiles?(engine) do
    profiles = engine.people_profiles
    handles = MapSet.new(Map.values(profiles), & &1["handle"])

    MapSet.size(handles) == map_size(profiles) and
      Enum.all?(profiles, fn {person, profile} ->
        valid_keyed_row?(engine, :people_profiles, person, profile)
      end)
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
      (is_nil(record.label) or
         (is_binary(record.label) and byte_size(record.label) >= 1)) and
      is_boolean(record.test)
  end

  defp valid_delivery_states?(engine) do
    is_map(engine.delivery_states) and
      Enum.all?(engine.delivery_states, fn {person, state} ->
        valid_delivery_state?(engine, person, state)
      end)
  end

  defp valid_delivery_state?(engine, person, state) do
    is_binary(person) and Map.has_key?(engine.persons, person) and
      state in ["STOPPED", "RESTORED"]
  end

  defp valid_instance?(id),
    do: is_binary(id) and byte_size(id) == 32 and Regex.match?(~r/\A[0-9a-f]{32}\z/, id)

  defp owns?(engine, person, key),
    do: get_in(engine.devices, [key, :person]) == person and Map.has_key?(engine.persons, person)

  defp owns_or_browser?(engine, person, key),
    do:
      owns?(engine, person, key) or
        (Map.has_key?(engine.persons, person) and BrowserRecipient.valid_for?(key, person))

  defp system_notice_actor?(engine, person, key),
    do:
      key == AttentionServer.Message.hosted_notice_sender() and
        Map.has_key?(engine.persons, person)

  defp receipt_actor?(engine, person, key),
    do: system_notice_actor?(engine, person, key) or actor_owns?(engine, person, key)

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
