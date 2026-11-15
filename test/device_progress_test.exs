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

  test "cold replay and every ACK preserve the FORMAT27 snapshot and survive restart", %{
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

    for n <- [0, 7, 8] do
      assert [%{"type" => "ok", "conversation_seq" => 8}] =
               request(relay, session, %{"type" => "conversation_ack", "conversation_seq" => n})

      assert [%{"type" => "ok", "seq" => 8}] =
               request(relay, session, %{"type" => "ack", "seq" => n})
    end

    assert :sys.get_state(relay).store.progress_version == version

    for n <- [-1, 9, nil, "7"] do
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

  test "ACK recovery clamps to the stream high, not connection progress", %{path: path} do
    {:ok, store} = Store.open(path)

    for {field, ack, reply_field} <- [
          {:cursors, &Engine.acknowledge/4, "seq"},
          {:conversation_cursors, &Engine.acknowledge_conversation/4, "conversation_seq"}
        ] do
      engine = Map.update!(store.engine, field, &Map.put(&1, {"A", "reader"}, 4))
      # An earlier connection already acknowledged 4, even if this one sent 0.
      assert {:ok, ^engine, %{^reply_field => 4}} = ack.(engine, "reader", 2, 0)
      assert {:ok, ^engine, %{^reply_field => 4}} = ack.(engine, "reader", 4, 0)
      assert {:error, :invalid_cursor} = ack.(engine, "reader", 5, 0)
      assert {:ok, next, %{^reply_field => 5}} = ack.(engine, "reader", 5, 5)
      assert Map.fetch!(next, field)[{"A", "reader"}] == 5

      assert Map.fetch!(next, field)[{"A", "origin"}] ==
               Map.fetch!(engine, field)[{"A", "origin"}]

      # Synthetic inconsistent in-memory state exercises the recovery rule;
      # it does not claim that normal Store.open admits an invalid snapshot.
      ahead = Map.update!(engine, field, &Map.put(&1, {"A", "reader"}, 100))

      for seq <- [0, 8, 100] do
        assert {:ok, recovered, %{^reply_field => 8}} = ack.(ahead, "reader", seq, 0)
        assert Map.fetch!(recovered, field)[{"A", "reader"}] == 8
      end

      for seq <- [-1, nil, "4", 101] do
        assert {:error, :invalid_cursor} = ack.(ahead, "reader", seq, 101)
      end

      assert {:error, :forbidden} = ack.(engine, "missing", 0, 0)
    end

    Store.close(store)
  end

  test "conversation open and ordinary ACK durably recover cursors above replay high", %{
    path: path
  } do
    before = snapshot(path)
    {:ok, relay} = Relay.start_link(store_path: path)
    {:ok, session} = Relay.open_session(relay, "reader")
    request(relay, session, %{"type" => "resync", "since" => 0})
    inject_cursor(relay, :conversation_cursors, 100)

    assert %{"type" => "conversation_resynced", "conversation_seq" => 8} =
             request(relay, session, %{"type" => "conversation_open", "since" => 0})
             |> List.last()

    assert {:ok, 8} = Relay.conversation_cursor(relay, "reader")

    assert [%{"type" => "ok", "conversation_seq" => 8}] =
             request(relay, session, %{"type" => "conversation_ack", "conversation_seq" => 1})

    inject_cursor(relay, :cursors, 100)

    assert [%{"type" => "ok", "seq" => 8}] =
             request(relay, session, %{"type" => "ack", "seq" => 1})

    GenServer.stop(relay)
    assert snapshot(path) == before
    {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, 8} = Relay.conversation_cursor(restarted, "reader")
    assert {:ok, 8} = Relay.acknowledged_cursor(restarted, "reader")
    {:ok, session} = Relay.open_session(restarted, "reader")

    assert [%{"type" => "ok", "seq" => 8}] =
             request(restarted, session, %{"type" => "ack", "seq" => 0})

    GenServer.stop(restarted)
  end

  test "failed clamp commit does not report recovery", %{path: path} do
    {:ok, relay} = Relay.start_link(store_path: path)
    {:ok, session} = Relay.open_session(relay, "reader")
    request(relay, session, %{"type" => "resync", "since" => 0})
    request(relay, session, %{"type" => "conversation_open", "since" => 0})
    inject_cursor(relay, :conversation_cursors, 100)

    :ok =
      Sqlite3.execute(
        :sys.get_state(relay).store.db,
        "CREATE TRIGGER reject_clamp BEFORE INSERT ON relay_device_progress BEGIN SELECT RAISE(ABORT, 'test failure'); END"
      )

    assert [%{"type" => "error", "reason" => "storage_unavailable"}] =
             request(relay, session, %{"type" => "conversation_ack", "conversation_seq" => 8})

    assert {:ok, 100} = Relay.conversation_cursor(relay, "reader")
    :ok = Sqlite3.execute(:sys.get_state(relay).store.db, "DROP TRIGGER reject_clamp")

    assert [%{"type" => "ok", "conversation_seq" => 8}] =
             request(relay, session, %{"type" => "conversation_ack", "conversation_seq" => 8})

    GenServer.stop(relay)
    {:ok, reopened} = Store.open(path)
    assert reopened.engine.conversation_cursors[{"A", "reader"}] == 8
    Store.close(reopened)
  end

  test "older pending checkpoint preserves a newer device ACK across modes and restart", %{
    path: path
  } do
    {:ok, relay} = Relay.start_link(store_path: path)
    {:ok, paging} = Relay.open_websocket_session(relay, "reader", nil, nil)

    start = %{
      "type" => "inbox_start",
      "paging_version" => 1,
      "startup_id" => "retained",
      "since" => 0
    }

    assert [%{"snapshot_id" => id, "high_water" => 8}] = request(relay, paging, start)

    frame = %{
      "v" => 1,
      "type" => "forward",
      "op_id" => op(999),
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => "later",
        "created_at" => 2,
        "title" => "Later"
      }
    }

    assert {:ok, line} = Relay.request_line(relay, "origin", JSON.encode!(frame) <> "\n")
    assert {:ok, %{"type" => "ok", "seq" => 9}} = JSON.decode_line(line)

    # Opening a fresh session fences the paging connection but retains its snapshot.
    {:ok, legacy} = Relay.open_session(relay, "reader")
    request(relay, legacy, %{"type" => "resync", "since" => 0})

    assert [%{"type" => "ok", "seq" => 9}] =
             request(relay, legacy, %{"type" => "ack", "seq" => 9})

    {:ok, resumed} = Relay.open_websocket_session(relay, "reader", nil, nil)
    assert [%{"snapshot_id" => ^id, "high_water" => 8}] = request(relay, resumed, start)

    checkpoint = %{
      "type" => "inbox_checkpoint_ack",
      "paging_version" => 1,
      "snapshot_id" => id,
      "high_water" => 8,
      "reconciled_batches" => [],
      "pending_manifest_complete" => true
    }

    assert [%{"checkpoint_accepted" => true, "high_water" => 8} = reply] =
             request(relay, resumed, checkpoint)

    refute Map.has_key?(reply, "duplicate")
    assert {:ok, 9} = Relay.acknowledged_cursor(relay, "reader")

    assert [%{"checkpoint_accepted" => true, "high_water" => 8, "duplicate" => true}] =
             request(relay, resumed, checkpoint)

    assert {:ok, 9} = Relay.acknowledged_cursor(relay, "reader")
    GenServer.stop(relay)

    {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, 9} = Relay.acknowledged_cursor(restarted, "reader")
    {:ok, resumed} = Relay.open_websocket_session(restarted, "reader", nil, nil)
    assert [%{"snapshot_id" => ^id}] = request(restarted, resumed, start)

    assert [%{"checkpoint_accepted" => true, "high_water" => 8, "duplicate" => true}] =
             request(restarted, resumed, checkpoint)

    request(restarted, resumed, %{
      "type" => "inbox_attach",
      "paging_version" => 1,
      "snapshot_id" => id,
      "mode" => "replay",
      "since" => 8
    })

    assert [%{"type" => "ok", "seq" => 9}] =
             request(restarted, resumed, %{"type" => "ack", "seq" => 8})

    GenServer.stop(restarted)
  end

  defp inject_cursor(relay, field, value) do
    :sys.replace_state(relay, fn state ->
      engine = Map.update!(state.store.engine, field, &Map.put(&1, {"A", "reader"}, value))
      %{state | store: %{state.store | engine: engine}}
    end)
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
