defmodule AttentionServer.Store.KeyedMigrationTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias AttentionServer.{Engine, Store}
  alias AttentionServer.Store.{KeyedMigration, RecordCodec, RecordTable}
  alias Exqlite.Sqlite3

  @moduletag :tmp_dir

  setup %{tmp_dir: directory} do
    path = Path.join(directory, "relay.sqlite3")
    assert {:ok, store} = Store.open(path)
    %{path: path, store: store}
  end

  test "a legacy store has no keyed state", %{store: store} do
    assert {:error, :not_keyed} = KeyedMigration.load(store.db)
    assert :ok = Store.close(store)
  end

  test "the shipped fit envelope admits a small snapshot and guard makes conversion idempotent", %{
    path: path,
    store: store
  } do
    assert :ok = Store.close(store)
    gib = 1024 * 1024 * 1024
    budget = %{memory: 2 * gib, disk: 2 * gib, uploads: 4096}

    assert {:error, :source_format_mismatch} = Store.convert_stopped(path, budget, 29)
    assert {:ok, :converted} = Store.convert_stopped(path, budget, 30)
    assert {:ok, :already_converted} = Store.convert_stopped(path, budget, 30)
    assert {:ok, reopened} = Store.open(path, :keyed)
    assert reopened.version == 1
    assert :ok = Store.close(reopened)
  end

  for stage <- ~w(preflight_passed snapshot_loaded records_written indexes_written records_verified anchor_written committed)a do
    test "a killed converter recovers at #{stage}", %{path: path, store: store} do
      stage = unquote(stage)
      assert {:ok, engine} = Engine.setup_person(store.engine, "reader")
      audit = %{person: "reader", actor: "fixture", action: "seed"}
      assert {:ok, legacy} = Store.commit(store, engine, audit)
      assert {:ok, original} = RecordTable.select(legacy.db, "SELECT version, checksum FROM relay_state")
      assert :ok = Store.close(legacy)

      gib = 1024 * 1024 * 1024
      budget = %{memory: 2 * gib, disk: 2 * gib, uploads: 4096}
      {worker, monitor} = spawn_monitor(fn ->
        receive do
          :start -> Store.convert_stopped(path, budget, 30)
        end
      end)

      handler = {__MODULE__, make_ref()}
      event = [:harklight, :store, :conversion, stage]
      assert :ok = :telemetry.attach(handler, event, &__MODULE__.pause_converter/4, {worker, self()})

      on_exit(fn ->
        :telemetry.detach(handler)
        if Process.alive?(worker), do: Process.exit(worker, :kill)
      end)

      send(worker, :start)
      assert_receive {^worker, ^stage}, 5_000
      Process.exit(worker, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^worker, :killed}, 2_000

      if stage == :committed do
        assert {:ok, :already_converted} = Store.convert_stopped(path, budget, 30)
      else
        assert {:ok, db} = Sqlite3.open(path)
        assert {:ok, ^original} = RecordTable.select(db, "SELECT version, checksum FROM relay_state")
        assert {:ok, []} = RecordTable.select(db, "SELECT name FROM sqlite_schema WHERE name = 'relay_records'")
        assert :ok = Sqlite3.close(db)
        assert {:ok, :converted} = Store.convert_stopped(path, budget, 30)
      end

      assert {:ok, converted} = Store.open(path, :keyed)
      assert converted.engine.persons == %{"reader" => 0}
      assert :ok = Store.close(converted)
    end
  end

  def pause_converter([:harklight, :store, :conversion, stage], _measurement, _metadata, {worker, observer})
      when worker == self() do
    send(observer, {worker, stage})

    receive do
      :continue -> :ok
    end
  end

  def pause_converter(_event, _measurement, _metadata, _config), do: :ok

  test "reads verified keyed records at their current generation", %{store: store} do
    assert {:ok, engine} = Engine.setup_person(store.engine, "reader")
    assert {:ok, 1} = stage_keyed(store.db, engine, RecordCodec.identity())
    assert {:ok, ^engine, 1} = KeyedMigration.load(store.db)

    row = RecordCodec.encode_row({:field, :pending_clock_high_water}, 12)

    assert {:ok, :saved} =
             RecordTable.commit(store.db, 1, [{:update, row}], fn _, _ -> {:ok, :saved} end)

    assert {:ok, %{pending_clock_high_water: 12}, 2} = KeyedMigration.load(store.db)
    assert :ok = Store.close(store)
  end

  test "legacy startup names FORMAT31 and leaves the converted database intact", %{
    path: path,
    store: store
  } do
    assert {:ok, 1} = stage_keyed(store.db, store.engine, RecordCodec.identity())
    assert :ok = Store.close(store)
    before_refusal = File.read!(path)

    warning =
      capture_log(fn ->
        assert {:error, {:storage_unavailable, {:error, :format31_requires_keyed_mode}}} =
                 Store.open(path, :legacy)
      end)

    assert warning =~ "FORMAT31"
    assert warning =~ "restore the pre-upgrade backup"
    assert File.read!(path) == before_refusal
  end

  test "checks the producing OTP major before reading records", %{store: store} do
    {codec, current} = RecordCodec.identity()
    different = "another OTP major"
    assert {:ok, 1} = stage_keyed(store.db, store.engine, {codec, different})
    assert :ok = Sqlite3.execute(store.db, "DROP TABLE relay_records")

    assert {:error, {:unsupported_otp_major, ^different, ^current}} =
             KeyedMigration.load(store.db)

    assert :ok = Store.close(store)
  end

  test "rejects an anchor checksum or a generation older than the anchor", %{store: store} do
    assert {:ok, 1} = stage_keyed(store.db, store.engine, RecordCodec.identity())
    assert :ok = Sqlite3.execute(store.db, "UPDATE relay_record_generation SET generation = 0")
    assert {:error, :invalid_state} = KeyedMigration.load(store.db)

    assert :ok = Sqlite3.execute(store.db, "UPDATE relay_record_generation SET generation = 1")
    assert :ok = Sqlite3.execute(store.db, "UPDATE relay_state SET checksum = zeroblob(32)")
    assert {:error, :invalid_state} = KeyedMigration.load(store.db)
    assert :ok = Store.close(store)
  end

  test "a fresh VM can decode the keyed anchor before application startup", %{
    path: path,
    store: store
  } do
    assert {:ok, 1} = stage_keyed(store.db, store.engine, RecordCodec.identity())
    assert :ok = Store.close(store)

    script = """
    path = System.fetch_env!("KEYED_COLD_OPEN_TEST_PATH")
    {:ok, db} = Exqlite.Sqlite3.open(path)
    {:ok, %AttentionServer.Engine{}, 1} = AttentionServer.Store.KeyedMigration.load(db)
    :ok = Exqlite.Sqlite3.close(db)
    """

    {output, status} =
      System.cmd("mix", ["run", "--no-start", "--no-compile", "-e", script],
        env: [
          {"MIX_ENV", "test"},
          {"ERL_FLAGS", "+S 2:2"},
          {"ATTENTION_SERVER_ENABLE", "0"},
          {"KEYED_COLD_OPEN_TEST_PATH", path}
        ],
        stderr_to_stdout: true
      )

    assert status == 0, output
  end

  test "folds saved progress and advances each current change log with the anchor", %{
    store: store
  } do
    assert {:ok, store} = Store.enable_change_log(store)
    assert {:ok, engine} = Engine.setup_person(store.engine, "reader")
    assert {:ok, engine} = Engine.setup_device(engine, "reader", "device")
    audit = %{person: "reader", actor: "device", action: "prepare"}
    assert {:ok, store} = Store.commit(store, engine, audit)
    next = Engine.put_member(store.engine, :conversation_opened, {"reader", "device"})
    assert {:ok, store} = Store.commit(store, next, audit)
    assert store.progress_version == 1
    generation = store.version + 1

    assert {:ok, ^generation} =
             KeyedMigration.convert_store(store, fn legacy, restored, version ->
               assert {:error, :not_keyed} = KeyedMigration.load(legacy.db)
               assert MapSet.member?(restored.conversation_opened, {"reader", "device"})
               assert version == generation
               {:ok, version}
             end)

    assert {:ok, engine, ^generation} = KeyedMigration.load(store.db)
    assert MapSet.member?(engine.conversation_opened, {"reader", "device"})
    assert [[0]] = rows(store.db, "SELECT COUNT(*) FROM relay_device_progress")
    assert [[^generation]] = rows(store.db, "SELECT store_version FROM client_change_log_state")
    assert [[^generation]] = rows(store.db, "SELECT store_version FROM agent_change_log_state")
    assert :ok = Store.close(store)
  end

  test "a preparation failure rolls back the new keyed tables and anchor", %{
    path: path,
    store: store
  } do
    assert_raise RuntimeError, "index build failed", fn ->
      KeyedMigration.convert_store(store, fn legacy, _, _ ->
        assert {:error, :not_keyed} = KeyedMigration.load(legacy.db)
        raise "index build failed"
      end)
    end

    assert [] =
             rows(store.db, "SELECT name FROM sqlite_schema WHERE name LIKE 'relay_record%'")

    assert {:error, :not_keyed} = KeyedMigration.load(store.db)
    assert :ok = Store.close(store)
    assert {:ok, legacy} = Store.open(path)
    assert legacy.version == store.version
    assert legacy.engine == store.engine
    assert :ok = Store.close(legacy)
  end

  defp rows(db, sql) do
    assert {:ok, statement} = Sqlite3.prepare(db, sql)

    try do
      assert {:ok, rows} = Sqlite3.fetch_all(db, statement)
      rows
    after
      :ok = Sqlite3.release(db, statement)
    end
  end

  defp stage_keyed(db, engine, identity) do
    payload = :erlang.term_to_binary({:attention_relay_keyed_state, 31, identity})
    digest = :crypto.hash(:sha256, payload)

    RecordTable.transaction(db, fn ->
      with :ok <- RecordTable.seed(db, 1, RecordCodec.encode(engine)),
           {:ok, statement} <-
             Sqlite3.prepare(db, "UPDATE relay_state SET version = 1, payload = ?, checksum = ?") do
        try do
          with :ok <- Sqlite3.bind(statement, [{:blob, payload}, {:blob, digest}]),
               :done <- Sqlite3.step(db, statement),
               do: {:ok, 1}
        after
          Sqlite3.release(db, statement)
        end
      end
    end)
  end
end
