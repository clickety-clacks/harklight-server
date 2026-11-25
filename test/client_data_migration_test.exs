defmodule AttentionServer.ClientDataMigrationTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{ChangeLog, PreChangeLogStore, Relay, Store}
  alias AttentionServer.ClientAPI.Resources
  alias AttentionServer.Store.RecordTable
  alias Exqlite.Sqlite3

  @moduletag :tmp_dir
  @key {"A", "app:test", "seen"}
  @session "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @seen %{
    "schema" => "attention-client.actions/1",
    "actions" => [
      %{"id" => "seen", "label" => "Seen", "kind" => "value", "value" => %{"acknowledgment" => "seen"}}
    ]
  }

  test "FORMAT31 startup retries an atomic retained-data migration and then skips it", %{
    tmp_dir: directory
  } do
    path = Path.join(directory, "relay.sqlite3")

    seed_retained_snapshot(path)

    assert {:ok, before} = Store.open(path)
    assert {:ok, before} = Store.enable_change_log(before)
    assert before.engine.messages[@key].message["message_type"] == nil
    assert {:ok, head} = ChangeLog.head(before, "A")
    assert :ok = Store.close(before)
    assert :ok = PreChangeLogStore.convert_fixture(path)

    assert {:ok, staged} = Store.open(path, :keyed)
    assert {:ok, staged} = Store.enable_change_log(staged)
    assert {:ok, ^head} = ChangeLog.head(staged, "A")
    assert :ok = Sqlite3.execute(staged.db, """
    CREATE TRIGGER refuse_migration_marker BEFORE INSERT ON relay_data_migrations
    BEGIN SELECT RAISE(ABORT, 'synthetic migration marker refusal'); END
    """)

    assert {:error, _} = Store.migrate_retained_client_data(staged)
    assert {:ok, []} = RecordTable.select(staged.db, "SELECT name FROM relay_data_migrations")
    assert RecordTable.generation(staged.db) == {:ok, staged.version}
    assert {:ok, ^head} = ChangeLog.head(staged, "A")
    assert :ok = Sqlite3.execute(staged.db, "DROP TRIGGER refuse_migration_marker")
    assert :ok = Store.close(staged)

    assert {:ok, reopened} = Store.open(path, :keyed)
    assert reopened.engine.messages[@key].message["message_type"] == nil
    assert :ok = Store.close(reopened)

    assert {:ok, relay} = Relay.start_link(store_path: path)
    after_start = :sys.get_state(relay).store
    record = after_start.engine.messages[@key]
    assert record.message["message_type"] == "fyi"
    assert record.revisions[1] == record.message

    assert %{"message" => %{"message_type" => "fyi"}} =
             after_start.engine.origin_feeds[{"A", "origin"}].entries[1]

    assert [%{"delivery" => %{"delivered" => true, "acted" => false} = delivery}] =
             Enum.filter(Resources.entries(after_start.engine, @key), &(&1["author"] == "person"))

    refute Map.has_key?(delivery, "legacy_caption")
    refute Map.has_key?(delivery, "outcome")

    assert after_start.engine.origin_receipts == before.engine.origin_receipts
    assert {:ok, next_head} = ChangeLog.head(after_start, "A")
    assert next_head > head
    assert {:ok, [["retained_client_data_v1"]]} =
             RecordTable.select(after_start.db, "SELECT name FROM relay_data_migrations")
    assert {:ok, statement} = Sqlite3.prepare(after_start.db, "SELECT payload FROM relay_state")
    assert {:row, [payload]} = Sqlite3.step(after_start.db, statement)
    assert :ok = Sqlite3.release(after_start.db, statement)
    assert {:attention_relay_keyed_state, 31, _} = :erlang.binary_to_term(payload, [:safe])
    assert {:ok, persisted, _} = AttentionServer.Store.KeyedMigration.load(after_start.db)
    assert persisted.messages[@key].message["message_type"] == "fyi"
    GenServer.stop(relay)

    assert {:ok, relay} = Relay.start_link(store_path: path)
    restarted = :sys.get_state(relay).store
    assert restarted.version == after_start.version
    assert {:ok, ^next_head} = ChangeLog.head(restarted, "A")
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

    assert :ok = PreChangeLogStore.convert_fixture(path)
    assert {:ok, converted} = Store.open(path, :keyed)
    assert {:ok, [["retained_client_data_v1"]]} =
             RecordTable.select(converted.db, "SELECT name FROM relay_data_migrations")

    version = converted.version
    assert :ok = Store.close(converted)

    assert {:ok, relay} = Relay.start_link(store_path: path)
    restarted = :sys.get_state(relay).store
    assert restarted.version == version
    assert {:ok, ^head} = ChangeLog.head(restarted, "A")
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

      [%{"feed_seq" => seq}] =
        for entry <- Map.values(store.engine.origin_feeds[{"A", "origin"}].entries),
            entry["kind"] == "discussion",
            do: entry

      PreChangeLogStore.origin_ack(store, "origin", seq)
    end)
  end

end
