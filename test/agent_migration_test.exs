defmodule AttentionServer.AgentMigrationTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{Engine, InboxPaging, Store}
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

    attrs = %{
      person_id: "A",
      device_id: key,
      startup_id: "format-17-startup",
      snapshot_id: "format-17-snapshot",
      since: 0,
      high_water: 0,
      cursor_secret: :crypto.strong_rand_bytes(32)
    }

    assert {:ok, snapshot} = Store.begin_inbox_snapshot(store, attrs)

    assert :ok =
             Store.finish_inbox_snapshot(store, snapshot, %{
               items: 0,
               receipts: 0,
               operation_ids: 0
             })

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

    assert {:ok,
            %{snapshot_id: "format-17-snapshot", high_water: 0, status: "pending"} =
              migrated_snapshot} = Store.inbox_startup(migrated, "A", key, "format-17-startup")

    frame = %{
      "v" => 1,
      "type" => "inbox_start",
      "ref" => "format-17-restart",
      "paging_version" => 1,
      "startup_id" => "format-17-startup",
      "since" => 0
    }

    assert {:ready, ^migrated_snapshot} =
             InboxPaging.start_plan(migrated, migrated.engine, key, frame)

    assert {:ok, %{"type" => "ok", "high_water" => 0, "items" => [], "next_cursor" => nil},
            ^migrated_snapshot, false} = InboxPaging.start(migrated, migrated.engine, key, frame)

    assert :ok = Store.close(migrated)
  end
end
