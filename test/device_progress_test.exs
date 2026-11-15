defmodule AttentionServer.DeviceProgressTest do
  use ExUnit.Case, async: false
  alias AttentionServer.{Engine, JSON, Relay, Store}
  alias Exqlite.Sqlite3

  setup do
    dir = Path.join(System.tmp_dir!(), "device-progress-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "state.sqlite3")
    {:ok, store} = Store.open(path)
    {:ok, engine} = Engine.setup_person(store.engine, "A")
    {:ok, engine} = Engine.setup_device(engine, "A", "origin")
    {:ok, engine} = Engine.setup_device(engine, "A", "reader")
    {:ok, engine} = Engine.setup_sender(engine, "origin", "app:test")

    engine =
      Enum.reduce(1..8, engine, fn n, engine ->
        engine =
          apply_frame(engine, "origin", %{
            "type" => "forward",
            "op_id" => op(n),
            "session_handle" => op(1000 + n),
            "message" => %{
              "sender_id" => "app:test",
              "message_id" => "m#{n}",
              "created_at" => 1,
              "title" => "Synthetic"
            }
          })

        apply_frame(engine, "reader", %{
          "type" => "discussion",
          "op_id" => op(100 + n),
          "sender_id" => "app:test",
          "message_id" => "m#{n}",
          "request_revision" => 1,
          "text" => "Synthetic question"
        })
      end)

    {:ok, store} = Store.commit(store, engine, audit())
    Store.close(store)
    %{path: path}
  end

  test "cold replay and every ACK preserve the FORMAT29 snapshot and survive restart", %{
    path: path
  } do
    before = snapshot(path)
    {:ok, relay} = Relay.start_link(store_path: path)
    {:ok, session} = Relay.open_session(relay, "reader")
    replay = request(relay, session, %{"type" => "resync", "since" => 0})

    assert Enum.map(Enum.filter(replay, &(&1["type"] == "delivery")), & &1["seq"]) ==
             Enum.to_list(1..8)

    conversation = request(relay, session, %{"type" => "conversation_open", "since" => 0})

    assert Enum.map(
             Enum.filter(conversation, &(&1["type"] == "conversation")),
             & &1["conversation_seq"]
           ) == Enum.to_list(1..8)

    for n <- 1..8 do
      assert [%{"type" => "ok", "seq" => ^n}] =
               request(relay, session, %{"type" => "ack", "seq" => n})

      assert [%{"type" => "ok", "conversation_seq" => ^n}] =
               request(relay, session, %{"type" => "conversation_ack", "conversation_seq" => n})
    end

    version = :sys.get_state(relay).store.progress_version

    assert [%{"type" => "ok"}] =
             request(relay, session, %{"type" => "conversation_ack", "conversation_seq" => 8})

    assert :sys.get_state(relay).store.progress_version == version

    for n <- [7, 9] do
      assert [%{"type" => "error", "reason" => "invalid_cursor"}] =
               request(relay, session, %{"type" => "conversation_ack", "conversation_seq" => n})
    end

    GenServer.stop(relay)
    assert snapshot(path) == before
    {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, 8} = Relay.acknowledged_cursor(restarted, "reader")
    assert {:ok, 8} = Relay.conversation_cursor(restarted, "reader")

    assert MapSet.member?(
             :sys.get_state(restarted).store.engine.conversation_opened,
             {"A", "reader"}
           )

    # A later ordinary mutation folds progress into the snapshot atomically.
    :ok = Relay.setup_sender(restarted, "origin", "app:next")
    GenServer.stop(restarted)
    {:ok, folded} = Store.open(path)
    assert folded.progress_version == 0
    assert folded.engine.conversation_cursors[{"A", "reader"}] == 8
    assert folded.engine.cursors[{"A", "reader"}] == 8
    Store.close(folded)
  end

  test "paged cold replay checkpoints preserve snapshot bytes and survive restart", %{path: path} do
    before = snapshot(path)
    {:ok, relay} = Relay.start_link(store_path: path)
    {:ok, session} = Relay.open_websocket_session(relay, "reader", nil, nil)

    [%{"type" => "ok", "snapshot_id" => id, "high_water" => 8, "items" => items}] =
      request(relay, session, %{
        "type" => "inbox_start",
        "paging_version" => 1,
        "startup_id" => "cold",
        "since" => 0
      })

    assert length(items) == 8

    frame = %{
      "type" => "inbox_checkpoint_ack",
      "paging_version" => 1,
      "snapshot_id" => id,
      "high_water" => 8,
      "reconciled_batches" => [],
      "pending_manifest_complete" => true
    }

    assert [%{"checkpoint_accepted" => true}] = request(relay, session, frame)

    assert [%{"checkpoint_accepted" => true, "duplicate" => true}] =
             request(relay, session, frame)

    GenServer.stop(relay)
    assert snapshot(path) == before
    {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, 8} = Relay.acknowledged_cursor(restarted, "reader")
    {:ok, session} = Relay.open_websocket_session(restarted, "reader", nil, nil)

    assert [%{"snapshot_id" => ^id}] =
             request(restarted, session, %{
               "type" => "inbox_start",
               "paging_version" => 1,
               "startup_id" => "cold",
               "since" => 0
             })

    assert [%{"checkpoint_accepted" => true, "duplicate" => true}] =
             request(restarted, session, frame)

    GenServer.stop(restarted)
    assert snapshot(path) == before
  end

  test "stale ACK and full-snapshot writers cannot overwrite accepted progress", %{path: path} do
    {:ok, first} = Store.open(path)
    {:ok, stale} = Store.open(path)
    {:ok, next, _} = Engine.acknowledge_conversation(first.engine, "reader", 4, 8)
    assert {:ok, accepted} = Store.commit(first, next, audit())
    {:ok, late, _} = Engine.acknowledge_conversation(stale.engine, "reader", 3, 8)
    assert {:error, :storage_unavailable, _} = Store.commit(stale, late, audit())
    {:ok, full} = Engine.setup_sender(stale.engine, "origin", "app:late")
    assert {:error, :storage_unavailable, _} = Store.commit(stale, full, audit())
    Store.close(accepted)
    Store.close(stale)
    {:ok, reopened} = Store.open(path)
    assert reopened.engine.conversation_cursors[{"A", "reader"}] == 4
    Store.close(reopened)
  end

  test "failed ACK transaction never advances the durable cursor", %{path: path} do
    {:ok, store} = Store.open(path)

    :ok =
      Sqlite3.execute(
        store.db,
        "CREATE TRIGGER reject_progress BEFORE INSERT ON relay_device_progress BEGIN SELECT RAISE(ABORT, 'test failure'); END"
      )

    {:ok, next, _} = Engine.acknowledge_conversation(store.engine, "reader", 4, 8)
    assert {:error, :storage_unavailable, _} = Store.commit(store, next, audit())
    Store.close(store)
    {:ok, reopened} = Store.open(path)
    assert Map.get(reopened.engine.conversation_cursors, {"A", "reader"}, 0) == 0
    Store.close(reopened)
  end

  test "FORMAT27 migration folds acknowledged progress before advancing the snapshot", %{
    path: path
  } do
    {:ok, store} = Store.open(path)
    {:ok, next, _} = Engine.acknowledge_conversation(store.engine, "reader", 6, 8)
    {:ok, store} = Store.commit(store, next, audit())
    Store.close(store)
    as_format27(path)

    {:ok, migrated} = Store.open(path)
    assert migrated.engine.conversation_cursors[{"A", "reader"}] == 6
    assert migrated.progress_version == 0
    assert migrated.engine.delivery_states == %{}
    Store.close(migrated)
    [_, payload, _] = snapshot(path)
    assert {:attention_relay_state, 29, _} = :erlang.binary_to_term(payload)
    {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, 6} = Relay.conversation_cursor(restarted, "reader")
    {:ok, session} = Relay.open_session(restarted, "reader")
    request(restarted, session, %{"type" => "resync", "since" => 0})
    request(restarted, session, %{"type" => "conversation_open", "since" => 6})

    assert [%{"type" => "ok"}] =
             request(restarted, session, %{"type" => "conversation_ack", "conversation_seq" => 6})

    assert [%{"reason" => "invalid_cursor"}] =
             request(restarted, session, %{"type" => "conversation_ack", "conversation_seq" => 5})

    assert [%{"type" => "ok"}] =
             request(restarted, session, %{"type" => "conversation_ack", "conversation_seq" => 8})

    GenServer.stop(restarted)
  end

  test "failed migration preserves FORMAT27 snapshot and its ACK overlay for retry", %{path: path} do
    {:ok, store} = Store.open(path)
    {:ok, next, _} = Engine.acknowledge_conversation(store.engine, "reader", 6, 8)
    {:ok, store} = Store.commit(store, next, audit())
    Store.close(store)
    as_format27(path)
    before = snapshot(path)
    {:ok, db} = Sqlite3.open(path)

    :ok =
      Sqlite3.execute(
        db,
        "CREATE TRIGGER reject_migration BEFORE UPDATE ON relay_state BEGIN SELECT RAISE(ABORT, 'test failure'); END"
      )

    assert {:error, _} = Store.open(path)
    assert snapshot(path) == before
    :ok = Sqlite3.execute(db, "DROP TRIGGER reject_migration")
    Sqlite3.close(db)
    {:ok, migrated} = Store.open(path)
    assert migrated.engine.conversation_cursors[{"A", "reader"}] == 6
    Store.close(migrated)
  end

  defp as_format27(path) do
    [_, payload, _] = snapshot(path)
    {:attention_relay_state, 29, engine} = :erlang.binary_to_term(payload)
    # Fields introduced by main must be supplied by its existing normalizations.
    engine = Map.delete(engine, :delivery_states)
    payload = :erlang.term_to_binary({:attention_relay_state, 27, engine}, [:compressed])
    {:ok, db} = Sqlite3.open(path)

    {:ok, stmt} =
      Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ? WHERE id=1")

    :ok = Sqlite3.bind(stmt, [{:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}])
    :done = Sqlite3.step(db, stmt)
    Sqlite3.release(db, stmt)
    Sqlite3.close(db)
  end

  defp snapshot(path) do
    {:ok, db} = Sqlite3.open(path)

    {:ok, stmt} =
      Sqlite3.prepare(db, "SELECT version, payload, checksum FROM relay_state WHERE id = 1")

    {:row, row} = Sqlite3.step(db, stmt)
    Sqlite3.release(db, stmt)
    Sqlite3.close(db)
    row
  end

  defp audit, do: %{person: "A", actor: "reader", action: "test"}
  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp apply_frame(engine, key, frame) do
    {next, {:ok, line}} =
      Engine.request_line(engine, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(line)
    next
  end

  defp request(relay, session, frame) do
    {:ok, lines} =
      Relay.session_request_line(relay, session, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    Enum.map(lines, fn line ->
      {:ok, result} = JSON.decode_line(line)
      result
    end)
  end
end
