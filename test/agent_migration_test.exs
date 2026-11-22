defmodule AttentionServer.AgentMigrationTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{Engine, Store}
  alias Exqlite.Sqlite3

  test "format-eight fixture agent gains empty service grants and a durable cursor key once" do
    dir =
      Path.join(
        System.tmp_dir!(),
        "attention-agent-migrate-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")
    {:ok, initial} = Store.open(path)
    assert :ok = Store.close(initial)

    key = String.duplicate("a", 64)
    id = "00000000-0000-4000-8000-0000000000aa"
    {:ok, person_state} = Engine.setup_person(initial.engine, "A")
    {:ok, with_agent} = Engine.setup_agent_fixture(person_state, "A", id, key, ["app:test"])

    old_agent =
      Map.drop(with_agent.agents[key], [
        :read_grants,
        :respond_grants,
        :grant_revision,
        :granted_by,
        :grant_history
      ])

    old_engine = %{with_agent | agents: %{key => old_agent}, agent_cursor_key: nil}
    payload = :erlang.term_to_binary({:attention_relay_state, 8, old_engine}, [{:compressed, 6}])

    assert {:ok, db} = Sqlite3.open(path)
    assert {:ok, stmt} = Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ?")
    assert :ok = Sqlite3.bind(stmt, [{:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}])
    assert :done = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)

    assert {:ok, migrated} = Store.open(path)
    assert migrated.version == initial.version + 1
    assert byte_size(migrated.engine.agent_cursor_key) == 32

    assert %{
             read_grants: read,
             respond_grants: respond,
             grant_revision: 1,
             grant_history: [%{action: "fixture_migration", actor: nil}]
           } = migrated.engine.agents[key]

    assert read == MapSet.new() and respond == MapSet.new()
    assert :ok = Store.close(migrated)

    assert {:ok, again} = Store.open(path)
    assert again.version == migrated.version
    assert again.engine.agent_cursor_key == migrated.engine.agent_cursor_key
    assert :ok = Store.close(again)
  end

  test "format-27 upgrade gives existing devices two disabled focus permissions" do
    dir =
      Path.join(
        System.tmp_dir!(),
        "attention-focus-migrate-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")
    {:ok, current} = Store.open(path)

    key = String.duplicate("a", 64)
    {:ok, engine} = Engine.setup_person(current.engine, "Owner")
    {:ok, engine} = Engine.setup_device(engine, "Owner", key)

    legacy_devices =
      Map.new(engine.devices, fn {device_key, device} ->
        {device_key, Map.drop(device, [:see_focus, :push_view])}
      end)

    legacy_engine = %{engine | devices: legacy_devices}
    assert :ok = Store.close(current)

    payload =
      :erlang.term_to_binary({:attention_relay_state, 27, legacy_engine}, [{:compressed, 6}])

    assert {:ok, db} = Sqlite3.open(path)
    assert {:ok, stmt} = Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ?")

    assert :ok =
             Sqlite3.bind(stmt, [
               {:blob, payload},
               {:blob, :crypto.hash(:sha256, payload)}
             ])

    assert :done = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)

    assert {:ok, migrated} = Store.open(path)
    assert %{see_focus: false, push_view: false} = migrated.engine.devices[key]
    assert :ok = Store.close(migrated)
  end

  test "format-17 state migrates without losing an existing inbox startup" do
    dir =
      Path.join(
        System.tmp_dir!(),
        "attention-format-17-migrate-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")
    {:ok, store} = Store.open(path)

    {:ok, engine} = Engine.setup_person(store.engine, "A")
    key = String.duplicate("a", 64)
    {:ok, engine} = Engine.setup_device(engine, "A", key)

    # A finished, not yet accepted 0.1 paging startup, as its tables stored it.
    assert {:ok, stmt} =
             Sqlite3.prepare(
               store.db,
               "INSERT INTO relay_inbox_snapshots (person_id, device_id, startup_id, snapshot_id, since_seq, high_water, status, item_count, receipt_count, operation_count, cursor_secret, receipt_next_offset, receipts_complete) VALUES (?, ?, 'format-17-startup', 'format-17-snapshot', 0, 0, 'pending', 0, 0, 0, ?, 0, 1)"
             )

    assert :ok = Sqlite3.bind(stmt, ["A", key, {:blob, :crypto.strong_rand_bytes(32)}])
    assert :done = Sqlite3.step(store.db, stmt)
    assert :ok = Sqlite3.release(store.db, stmt)
    assert :ok = Store.close(store)

    legacy_engine =
      engine
      |> Map.from_struct()
      |> Map.drop([
        :conversation_highs,
        :conversation_events,
        :conversation_cursors,
        :conversation_opened,
        :origin_feeds,
        :origin_sessions,
        :origin_receipts,
        :origin_actions,
        :uploads
      ])
      |> Map.put(:__struct__, Engine)

    payload =
      :erlang.term_to_binary({:attention_relay_state, 17, legacy_engine}, [{:compressed, 6}])

    assert {:ok, db} = Sqlite3.open(path)
    assert {:ok, stmt} = Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ?")

    assert :ok =
             Sqlite3.bind(stmt, [
               {:blob, payload},
               {:blob, :crypto.hash(:sha256, payload)}
             ])

    assert :done = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)

    assert {:ok, migrated} = Store.open(path)
    assert migrated.version == 1
    assert migrated.engine.persons == %{"A" => 0}

    assert {:ok, stmt} =
             Sqlite3.prepare(
               migrated.db,
               "SELECT snapshot_id, high_water, status FROM relay_inbox_snapshots WHERE person_id = ? AND device_id = ? AND startup_id = 'format-17-startup'"
             )

    assert :ok = Sqlite3.bind(stmt, ["A", key])
    assert {:ok, [["format-17-snapshot", 0, "pending"]]} = Sqlite3.fetch_all(migrated.db, stmt)
    assert :ok = Sqlite3.release(migrated.db, stmt)
    assert :ok = Store.close(migrated)
  end
end
