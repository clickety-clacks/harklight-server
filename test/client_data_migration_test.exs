defmodule AttentionServer.ClientDataMigrationTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{ChangeLog, Engine, PreChangeLogStore, Relay, Store}
  alias AttentionServer.ClientAPI.Resources
  alias AttentionServer.Store.RecordTable
  alias Exqlite.Sqlite3

  @moduletag :tmp_dir
  @key {"A", "app:test", "seen"}
  @session "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @discussion_feed_seq 2
  @seen %{
    "schema" => "attention-client.actions/1",
    "actions" => [
      %{"id" => "seen", "label" => "Mark as read", "kind" => "value", "value" => "seen-legacy"}
    ]
  }

  @budget %{memory: 4 * 1024 * 1024 * 1024, disk: 4 * 1024 * 1024 * 1024, uploads: 0}

  test "stopped conversion rewrites retained data atomically before the Relay serves it", %{
    tmp_dir: directory
  } do
    path = Path.join(directory, "relay.sqlite3")
    seed_retained_snapshot(path)
    assert {:ok, before} = Store.open(path, :snapshot)
    assert {:ok, before} = Store.enable_change_log(before)
    assert before.engine.messages[@key].message["message_type"] == nil
    assert {:ok, head} = ChangeLog.head(before, "A")

    assert :ok = Sqlite3.execute(before.db, """
    CREATE TRIGGER refuse_migration_marker BEFORE INSERT ON relay_data_migrations
    BEGIN SELECT RAISE(ABORT, 'synthetic migration marker refusal'); END
    """)

    assert :ok = Store.close(before)
    original = File.read!(path)
    assert {:error, _} = Store.convert_stopped(path, @budget, 30)
    assert File.read!(path) == original

    assert {:ok, snapshot} = Store.open(path, :snapshot)
    assert {:ok, snapshot} = Store.enable_change_log(snapshot)
    assert {:ok, ^head} = ChangeLog.head(snapshot, "A")
    assert {:ok, []} = RecordTable.select(snapshot.db, "SELECT name FROM relay_data_migrations")
    assert snapshot.engine.messages[@key].message["message_type"] == nil
    assert :ok = Sqlite3.execute(snapshot.db, "DROP TRIGGER refuse_migration_marker")
    assert :ok = Store.close(snapshot)

    assert {:ok, :converted} = Store.convert_stopped(path, @budget, 30)
    assert {:ok, :already_converted} = Store.convert_stopped(path, @budget, 30)
    assert {:ok, converted} = Store.open(path, :keyed)
    record = converted.engine.messages[@key]
    assert record.message["message_type"] == "fyi"
    assert record.message["answer_ui"] == @seen
    assert record.revisions[1] == record.message

    assert %{"message" => %{"message_type" => "fyi"}} =
             converted.engine.origin_feeds[{"A", "origin"}].entries[1]

    assert [%{"entry_id" => entry_id, "delivery" => delivery}] =
             Enum.filter(Resources.entries(converted.engine, @key), &(&1["author"] == "person"))

    assert %{"delivered" => true, "acted" => false, "receipt_expected" => false} = delivery
    refute Map.has_key?(delivery, "legacy_caption")
    refute Map.has_key?(delivery, "outcome")

    seq = entry_id |> String.trim_leading("e") |> String.to_integer()
    assert converted.engine.conversation_events[{"A", seq}].payload["receipt_expected"] == false
    assert converted.engine.origin_receipts == before.engine.origin_receipts
    assert {:ok, [["retained_client_data_v1"]]} =
             RecordTable.select(converted.db, "SELECT name FROM relay_data_migrations")

    assert {:ok, converted} = Store.enable_change_log(converted)
    assert {:ok, next_head} = ChangeLog.head(converted, "A")
    assert next_head > head
    target = ChangeLog.entry_resource(seq)
    assert {:ok, changes, false, ^next_head} = ChangeLog.read(converted, "A", head, 100)
    assert Enum.any?(changes, &match?(%{kind: :entry, resource_id: ^target}, &1))
    assert :ok = Store.close(converted)

    assert {:ok, relay} = Relay.start_link(store_path: path)
    restarted = :sys.get_state(relay).store
    assert {:ok, rows, false, _} = ChangeLog.read(restarted, "A", next_head, 100)
    refute Enum.any?(rows, &match?(%{kind: :entry, resource_id: ^target}, &1))

    assert {:ok, with_receipt, _} =
             Engine.origin_ack2(restarted.engine, "origin", @discussion_feed_seq, %{
               "op_id" => "00000000-0000-4000-8000-000000000003",
               "outcome" => "handed"
             })

    assert [%{"delivery" => %{"outcome" => "handed", "receipt_expected" => false}}] =
             Enum.filter(Resources.entries(with_receipt, @key), &(&1["entry_id"] == entry_id))

    GenServer.stop(relay)
  end

  test "FORMAT31 cutover preserves a marker already committed in FORMAT30", %{tmp_dir: directory} do
    path = Path.join(directory, "relay.sqlite3")
    seed_retained_snapshot(path)
    assert {:ok, snapshot} = Store.open(path, :snapshot)
    assert {:ok, snapshot} = Store.enable_change_log(snapshot)
    assert {:ok, migrated} = Store.migrate_retained_client_data(snapshot)
    assert migrated.engine.messages[@key].message["message_type"] == "fyi"
    assert {:ok, head} = ChangeLog.head(migrated, "A")
    assert :ok = Store.close(migrated)

    assert {:ok, :converted} = Store.convert_stopped(path, @budget, 30)
    assert {:ok, converted} = Store.open(path, :keyed)
    assert {:ok, [["retained_client_data_v1"]]} =
             RecordTable.select(converted.db, "SELECT name FROM relay_data_migrations")

    [{seq, entry}] =
      for {{"A", seq}, %{payload: %{"receipt_expected" => false}} = entry} <-
            converted.engine.conversation_events,
          do: {seq, entry}

    target = ChangeLog.entry_resource(seq)
    assert :ok = Store.close(converted)

    assert {:ok, relay} = Relay.start_link(store_path: path)
    restarted = :sys.get_state(relay).store
    assert restarted.engine.conversation_events[{"A", seq}] == entry
    assert {:ok, rows, false, _} = ChangeLog.read(restarted, "A", head, 100)
    refute Enum.any?(rows, &match?(%{kind: :entry, resource_id: ^target}, &1))
    GenServer.stop(relay)
  end

  defp seed_retained_snapshot(path) do
    PreChangeLogStore.write(path, fn store ->
      store =
        store
        |> PreChangeLogStore.setup_person("A")
        |> PreChangeLogStore.setup_device("A", "origin")
        |> PreChangeLogStore.setup_device("A", "reader")
        |> PreChangeLogStore.setup_sender("origin", "app:test")

      {%{"type" => "ok"}, store} =
        PreChangeLogStore.request(store, "origin", %{
          "type" => "forward",
          "op_id" => "00000000-0000-4000-8000-000000000001",
          "session_handle" => @session,
          "message" => %{
            "sender_id" => "app:test",
            "message_id" => "seen",
            "created_at" => 1,
            "title" => "Historical FYI",
            "answer_ui" => @seen
          }
        })

      {%{"type" => "ok"}, store} =
        PreChangeLogStore.request(store, "reader", %{
          "type" => "discussion",
          "op_id" => "00000000-0000-4000-8000-000000000002",
          "sender_id" => "app:test",
          "message_id" => "seen",
          "request_revision" => 1,
          "text" => "Retained reply"
        })

      [%{"feed_seq" => @discussion_feed_seq}] =
        for entry <- Map.values(store.engine.origin_feeds[{"A", "origin"}].entries),
            entry["kind"] == "discussion",
            do: entry

      PreChangeLogStore.origin_ack(store, "origin", @discussion_feed_seq)
    end)
  end
end
