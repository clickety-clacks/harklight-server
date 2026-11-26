defmodule AttentionServer.Store.RowRelayStartupTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Engine, JSON, Relay, Store}
  alias AttentionServer.AgentAPI.FeedActivity
  alias AttentionServer.ClientAPI.Reads
  alias AttentionServer.Store.{DerivedIndex, RecordCodec, RecordTable}
  alias Exqlite.Sqlite3

  @moduletag :tmp_dir

  test "a fresh Relay starts with a FORMAT32 anchor and no legacy snapshot", %{tmp_dir: dir} do
    path = Path.join(dir, "relay.sqlite3")
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = GenServer.stop(relay)
    assert {:ok, store} = Store.open(path, :keyed)
    assert {:attention_relay_keyed_state, 32, _} = snapshot_anchor(store)
    assert [[1, 1, 0]] = query(store, "SELECT index_version, liveness_version, generation FROM relay_index_state")
    assert :ok = Store.close(store)
  end

  test "Relay startup leaves a legacy store for the stopped-store converter", %{tmp_dir: dir} do
    path = Path.join(dir, "relay.sqlite3")
    assert {:ok, store} = Store.open(path, :snapshot)
    original = snapshot_anchor(store)
    assert :ok = Store.close(store)
    assert {:error, :conversion_required} = Store.open_relay(path)
    assert {:ok, store} = Store.open(path, :snapshot)
    assert snapshot_anchor(store) == original
    assert :ok = Store.close(store)
  end

  test "Relay starts from the converted row store and commits ordinary mutations", %{
    tmp_dir: directory
  } do
    path = Path.join(directory, "relay.sqlite3")

    assert {:ok, legacy} = Store.open(path, :snapshot)
    assert {:ok, engine} = Engine.setup_person(legacy.engine, "person")
    assert {:ok, engine} = Engine.setup_device(engine, "person", "origin")
    audit = %{person: "person", actor: "origin", action: "test_fixture"}
    assert {:ok, legacy} = Store.commit(legacy, engine, audit)
    assert :ok = Store.close(legacy)

    assert {:ok, converted} = Store.open(path, :keyed_cutover)
    assert {:attention_relay_keyed_state, 32, _} = snapshot_anchor(converted)
    assert :ok = Store.close(converted)

    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")
    frame = forward_frame()
    assert {:ok, response} = Relay.request_line(relay, "origin", JSON.encode!(frame) <> "\n")
    assert {:ok, %{"type" => "ok", "seq" => 1}} = JSON.decode_line(response)
    assert :ok = GenServer.stop(relay)

    assert {:ok, cold} = Store.open(path, :keyed)
    assert cold.engine.sender_grants[{"person", "origin"}] == MapSet.new(["app:test"])
    assert {:attention_relay_keyed_state, 32, _} = snapshot_anchor(cold)
    assert [["person", 1, nil, 1]] = query(cold, "SELECT person, seq, state, live FROM relay_items")
    assert :ok = Store.close(cold)
  end

  test "stopped FORMAT31 upgrade retains records and is repeatable", %{tmp_dir: dir} do
    path = Path.join(dir, "relay.sqlite3")
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = GenServer.stop(relay)
    assert {:ok, store} = Store.open(path, :keyed)

    before = store.engine
    generation = store.version
    payload = :erlang.term_to_binary({:attention_relay_keyed_state, 31, RecordCodec.identity()})
    checksum = :crypto.hash(:sha256, payload)

    assert :ok =
             RecordTable.run(
               store.db,
               "UPDATE relay_state SET payload = ?, checksum = ? WHERE id = 1",
               [{:blob, payload}, {:blob, checksum}]
             )

    assert :ok = Store.close(store)
    budget = %{memory: 1024 * 1024 * 1024, disk: 1024 * 1024 * 1024}
    assert {:ok, :converted} = Store.convert_stopped(path, budget, 31)
    assert {:ok, :already_converted} = Store.convert_stopped(path, budget, 31)

    assert {:ok, upgraded} = Store.open(path, :keyed)
    assert {:attention_relay_keyed_state, 32, _} = snapshot_anchor(upgraded)
    assert upgraded.version == generation
    assert upgraded.engine == before
    assert :ok = Store.close(upgraded)
  end

  test "cutover moves published availability without changing its rendering", %{tmp_dir: dir} do
    path = Path.join(dir, "relay.sqlite3")
    assert {:ok, store} = Store.open(path, :snapshot)
    assert {:ok, engine} = Engine.setup_person(store.engine, "person")
    assert {:ok, engine} = Engine.setup_device(engine, "person", "origin")
    assert {:ok, engine} = Engine.setup_sender(engine, "origin", "app:test")
    session = "00000000-0000-4000-8000-000000000001"

    command = Map.delete(forward_frame(), "v")
    assert {engine, {:ok, %{"seq" => 1}}} = Engine.request_command(engine, "origin", command, 1)
    key = {"person", "app:test", "item"}
    scope = {"person", "origin", session}
    engine = put_in(engine.origin_sessions[scope]["client_availability"], %{1 => {"listening", nil}})
    assert FeedActivity.item_availability(engine, key) == %{"state" => "listening"}

    audit = %{person: "person", actor: "origin", action: "test_fixture"}
    assert {:ok, store} = Store.commit(store, engine, audit)
    assert :ok = Store.close(store)
    assert {:ok, converted} = Store.open(path, :keyed_cutover)

    refute Map.has_key?(converted.engine.origin_sessions[scope], "client_availability")
    assert converted.engine.item_availability[key] == {"listening", nil}
    assert FeedActivity.item_availability(converted.engine, key) == %{"state" => "listening"}
    assert :ok = Store.close(converted)
  end

  test "row-mode Inbox refuses a rank reset until the complete ranking is stored", %{
    tmp_dir: dir
  } do
    path = Path.join(dir, "relay.sqlite3")
    assert {:ok, store} = Store.open(path, :snapshot)
    assert {:ok, engine} = Engine.setup_person(store.engine, "person")
    assert {:ok, engine} = Engine.setup_device(engine, "person", "origin")
    audit = %{person: "person", actor: "origin", action: "test_fixture"}
    assert {:ok, store} = Store.commit(store, engine, audit)
    assert :ok = Store.close(store)
    assert {:ok, converted} = Store.open(path, :keyed_cutover)
    assert :ok = Store.close(converted)
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert {:ok, _} = Reads.inbox(relay, "origin")

    :sys.replace_state(relay, fn state ->
      assert :ok = Sqlite3.execute(state.store.db, "DELETE FROM client_rank_state")
      state
    end)

    assert {:error, :storage_unavailable} = Reads.inbox(relay, "origin")
    assert :ok = GenServer.stop(relay)
  end

  test "stored records alone rebuild the incremental derived rows through an inherited chain", %{tmp_dir: dir} do
    path = Path.join(dir, "relay.sqlite3")
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "person")
    assert :ok = Relay.setup_device(relay, "person", "origin")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    for {id, previous, op} <- [{"old", nil, 10}, {"middle", "old", 11}, {"new", "middle", 12}] do
      message = Map.put(forward_frame()["message"], "message_id", id)
      message = if previous, do: Map.put(message, "supersedes", [previous]), else: message
      assert %{"type" => "ok"} = issue(relay, Map.put(forward_frame(), "message", message), op)
    end

    assert {:ok, reader} = Sqlite3.open(path, mode: :readonly)
    db = %{db: reader}
    sql = "SELECT message_id, state, live FROM relay_items ORDER BY seq"
    assert [["old", "superseded", 1], ["middle", "superseded", 1], ["new", nil, 1]] = query(db, sql)

    assert %{"type" => "ok"} = issue(relay, %{
      "type" => "lifecycle", "sender_id" => "app:test", "message_id" => "new", "state" => "cleared", "at" => 1
    }, 13)

    assert [["old", "superseded", 0], ["middle", "superseded", 0], ["new", "cleared", 0]] = query(db, sql)
    tables = ~w(relay_items relay_entries relay_feed relay_advisories relay_sessions relay_authority relay_upload_refs)
    expected = Map.new(tables, &{&1, Enum.sort(query(db, "SELECT * FROM #{&1}"))})
    assert :ok = Sqlite3.close(reader)
    assert :ok = GenServer.stop(relay)

    assert {:ok, db} = Sqlite3.open(path)
    assert :ok = Sqlite3.execute(db, "DELETE FROM relay_items; UPDATE relay_index_state SET generation = 0")
    assert {:ok, generation} = RecordTable.generation(db)
    assert :ok = DerivedIndex.ensure(db, generation)
    assert :ok = Sqlite3.close(db)

    assert {:ok, rebuilt} = Store.open(path, :keyed)
    assert Map.new(tables, &{&1, Enum.sort(query(rebuilt, "SELECT * FROM #{&1}"))}) == expected
    assert :ok = Store.close(rebuilt)
  end

  for {phase, change} <- [
        records: "INSERT ON relay_items",
        indexes: "INSERT ON relay_audit",
        logs: "UPDATE ON relay_index_state"
      ] do
    test "failure after writing #{phase} rolls back the entire send and permits an exact retry", %{tmp_dir: dir} do
      path = Path.join(dir, "relay.sqlite3")
      assert {:ok, relay} = Relay.start_link(store_path: path)
      assert :ok = Relay.setup_person(relay, "person")
      assert :ok = Relay.setup_device(relay, "person", "origin")
      assert :ok = Relay.setup_sender(relay, "origin", "app:test")

      assert {:ok, db} = Sqlite3.open(path)
      tables = ~w(relay_records relay_items relay_feed relay_audit client_changes agent_changes relay_index_state)
      before = Map.new(tables, &{&1, query(%{db: db}, "SELECT count(*) FROM #{&1}")})
      generation = query(%{db: db}, "SELECT generation FROM relay_record_generation")

      assert :ok = Sqlite3.execute(db, """
             CREATE TRIGGER refuse_phase BEFORE #{unquote(change)}
             BEGIN SELECT RAISE(ABORT, 'synthetic commit refusal'); END
             """)

      assert %{"type" => "error", "reason" => "storage_unavailable"} = issue(relay, forward_frame(), 20)
      assert Map.new(tables, &{&1, query(%{db: db}, "SELECT count(*) FROM #{&1}")}) == before
      assert query(%{db: db}, "SELECT generation FROM relay_record_generation") == generation
      assert query(%{db: db}, "SELECT generation FROM relay_index_state") == generation
      assert :sys.get_state(relay).store.engine.messages == %{}
      assert :ok = Sqlite3.execute(db, "DROP TRIGGER refuse_phase")

      assert %{"type" => "ok", "seq" => 1} = issue(relay, forward_frame(), 20)
      assert %{"type" => "ok", "seq" => 1, "duplicate" => true} = issue(relay, forward_frame(), 20)
      assert [[1]] = query(%{db: db}, "SELECT count(*) FROM relay_items WHERE live = 1")
      assert :ok = Sqlite3.close(db)
      assert :ok = GenServer.stop(relay)
    end
  end

  test "a killed worker after COMMIT leaves one send across Relay restart and retry", %{tmp_dir: dir} do
    path = Path.join(dir, "relay.sqlite3")
    assert {:ok, relay} = Relay.start_link(store_path: path)
    Process.unlink(relay)
    monitor = Process.monitor(relay)
    assert :ok = Relay.setup_person(relay, "person")
    assert :ok = Relay.setup_device(relay, "person", "origin")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    observer = self()
    handler = {__MODULE__, make_ref()}
    event = [:harklight, :store, :commit, :committed]
    assert :ok = :telemetry.attach(handler, event, &__MODULE__.pause_worker/4, {relay, observer})

    on_exit(fn ->
      :telemetry.detach(handler)
      if Process.alive?(relay), do: Process.exit(relay, :kill)
    end)

    spawn(fn ->
      try do
        send(observer, {:reply, issue(relay, forward_frame(), 30)})
      catch
        :exit, _ -> send(observer, :reply_lost)
      end
    end)

    assert_receive {:committed, worker}, 5_000
    Process.exit(worker, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^relay, :killed}, 2_000
    assert_receive :reply_lost

    assert {:ok, restarted} = Relay.start_link(store_path: path)
    assert %{"type" => "ok", "seq" => 1, "duplicate" => true} = issue(restarted, forward_frame(), 30)
    assert {:ok, %{"items" => [%{"item_id" => "i1"}]}} = Reads.inbox(restarted, "origin")
    assert :ok = GenServer.stop(restarted)

    assert {:ok, cold} = Store.open(path, :keyed)
    assert [[1]] = query(cold, "SELECT count(*) FROM relay_items WHERE live = 1")
    assert :ok = Store.close(cold)
  end

  def pause_worker(_event, _measurement, _metadata, {relay, observer}) do
    {:links, links} = Process.info(self(), :links)

    if relay in links do
      send(observer, {:committed, self()})

      receive do
        :continue -> :ok
      end
    end
  end

  defp issue(relay, frame, n) do
    id = "00000000-0000-4000-8000-" <> String.pad_leading(Integer.to_string(n), 12, "0")
    frame = Map.merge(frame, %{"op_id" => id, "v" => 1})
    assert {:ok, reply} = Relay.request_line(relay, "origin", JSON.encode_line(frame))
    assert {:ok, decoded} = JSON.decode_line(reply)
    decoded
  end

  defp forward_frame do
    %{
      "v" => 1,
      "type" => "forward",
      "op_id" => "00000000-0000-4000-8000-000000000002",
      "session_handle" => "00000000-0000-4000-8000-000000000001",
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => "item",
        "created_at" => 1,
        "title" => "Fixture"
      }
    }
  end

  defp query(store, sql) do
    assert {:ok, stmt} = Sqlite3.prepare(store.db, sql)

    try do
      assert {:ok, rows} = Sqlite3.fetch_all(store.db, stmt)
      rows
    after
      Sqlite3.release(store.db, stmt)
    end
  end

  defp snapshot_anchor(store) do
    assert {:ok, stmt} = Sqlite3.prepare(store.db, "SELECT payload FROM relay_state WHERE id = 1")

    try do
      assert {:row, [payload]} = Sqlite3.step(store.db, stmt)
      :erlang.binary_to_term(payload, [:safe])
    after
      Sqlite3.release(store.db, stmt)
    end
  end
end
