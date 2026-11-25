defmodule AttentionServer.DeviceProgressTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{Engine, JSON, Relay, RelayReaders, Store}
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

  # Protocol 0.1 sessions no longer acknowledge progress through the Relay. A
  # cold Relay start and its replay reads leave the FORMAT30 snapshot alone,
  # and the stored progress writer (opens and every cursor a 0.1 session
  # stored) still commits beside the snapshot, survives restart and folds into
  # it on the next mutation.
  test "cold replay and every ACK preserve the FORMAT30 snapshot and survive restart", %{
    path: path
  } do
    before = snapshot(path)
    {:ok, relay} = Relay.start_link(store_path: path)
    {:ok, replay, 8} = RelayReaders.events(relay, "reader", 0)

    assert Enum.map(Enum.filter(replay, &(&1["type"] == "delivery")), & &1["seq"]) ==
             Enum.to_list(1..8)

    {:ok, conversation, 8} = RelayReaders.conversation_events(relay, "reader", 0)

    assert Enum.map(
             Enum.filter(conversation, &(&1["type"] == "conversation")),
             & &1["conversation_seq"]
           ) == Enum.to_list(1..8)

    GenServer.stop(relay)
    assert snapshot(path) == before

    {:ok, store} = Store.open(path)
    opened = Engine.put_member(store.engine, :conversation_opened, {"A", "reader"})
    {:ok, store} = Store.commit(store, opened, audit())

    store =
      Enum.reduce(1..8, store, fn n, store ->
        {:ok, store} = Store.commit(store, cursor(store.engine, n), audit())
        {:ok, store} = Store.commit(store, conversation_cursor(store.engine, n), audit())
        store
      end)

    version = store.progress_version
    {:ok, store} = Store.commit(store, conversation_cursor(store.engine, 8), audit())
    assert store.progress_version == version

    Store.close(store)
    assert snapshot(path) == before
    {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, 8} = Relay.acknowledged_cursor(restarted, "reader")
    assert {:ok, 8} = RelayReaders.conversation_cursor(restarted, "reader")

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

  test "stale ACK and full-snapshot writers cannot overwrite accepted progress", %{path: path} do
    {:ok, first} = Store.open(path)
    {:ok, stale} = Store.open(path)
    assert {:ok, accepted} = Store.commit(first, conversation_cursor(first.engine, 4), audit())
    late = conversation_cursor(stale.engine, 3)
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

    next = conversation_cursor(store.engine, 4)
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
    {:ok, store} = Store.commit(store, conversation_cursor(store.engine, 6), audit())
    Store.close(store)
    as_format27(path)

    {:ok, migrated} = Store.open(path)
    assert migrated.engine.conversation_cursors[{"A", "reader"}] == 6
    assert migrated.progress_version == 0
    assert migrated.engine.delivery_states == %{}
    Store.close(migrated)
    [_, payload, _] = snapshot(path)
    assert {:attention_relay_state, 30, _} = :erlang.binary_to_term(payload)
    {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, 6} = RelayReaders.conversation_cursor(restarted, "reader")
    GenServer.stop(restarted)

    # Later stored progress commits on top of the migrated cursor.
    {:ok, store} = Store.open(path)
    assert store.engine.conversation_cursors[{"A", "reader"}] == 6
    {:ok, store} = Store.commit(store, conversation_cursor(store.engine, 8), audit())
    Store.close(store)
    {:ok, reopened} = Store.open(path)
    assert reopened.engine.conversation_cursors[{"A", "reader"}] == 8
    Store.close(reopened)
  end

  test "failed migration preserves FORMAT27 snapshot and its ACK overlay for retry", %{path: path} do
    {:ok, store} = Store.open(path)
    {:ok, store} = Store.commit(store, conversation_cursor(store.engine, 6), audit())
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
    {:attention_relay_state, 30, engine} = :erlang.binary_to_term(payload)
    # Fields introduced by main must be supplied by its existing normalizations.
    engine = Map.drop(engine, [:delivery_states, :alert_choices])
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

  # The reader's ordinary-feed and conversation cursors, as a 0.1 session
  # stored them.
  defp cursor(engine, seq),
    do: %{engine | cursors: Map.put(engine.cursors, {"A", "reader"}, seq)}

  defp conversation_cursor(engine, seq),
    do: %{
      engine
      | conversation_cursors: Map.put(engine.conversation_cursors, {"A", "reader"}, seq)
    }

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp apply_frame(engine, key, frame) do
    {next, {:ok, line}} =
      Engine.request_line(engine, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(line)
    next
  end
end
