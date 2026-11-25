defmodule AttentionServer.ClientDataMigrationTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{ChangeLog, PreChangeLogStore, Relay, Store}
  alias AttentionServer.ClientAPI.Resources
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

  test "first startup migrates retained captions and Seen-only messages in FORMAT30", %{
    tmp_dir: directory
  } do
    path = Path.join(directory, "relay.sqlite3")

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

    assert {:ok, before} = Store.open(path)
    assert {:ok, before} = Store.enable_change_log(before)
    assert before.engine.messages[@key].message["message_type"] == nil
    assert {:ok, head} = ChangeLog.head(before, "A")
    assert :ok = Store.close(before)

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
    assert {:ok, statement} = Sqlite3.prepare(after_start.db, "SELECT payload FROM relay_state")
    assert {:row, [payload]} = Sqlite3.step(after_start.db, statement)
    assert :ok = Sqlite3.release(after_start.db, statement)
    assert {:attention_relay_state, 30, persisted} = :erlang.binary_to_term(payload, [:safe])
    assert persisted.messages[@key].message["message_type"] == "fyi"
    GenServer.stop(relay)

    assert {:ok, relay} = Relay.start_link(store_path: path)
    restarted = :sys.get_state(relay).store
    assert restarted.version == after_start.version
    assert {:ok, ^next_head} = ChangeLog.head(restarted, "A")
    GenServer.stop(relay)
  end
end
