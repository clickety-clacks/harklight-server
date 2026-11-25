defmodule AttentionServer.ClientDataMigrationTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{ChangeLog, Engine, PreChangeLogStore, Relay, Store}
  alias AttentionServer.ClientAPI.Resources
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

      [%{"feed_seq" => @discussion_feed_seq}] =
        for entry <- Map.values(store.engine.origin_feeds[{"A", "origin"}].entries),
            entry["kind"] == "discussion",
            do: entry

      PreChangeLogStore.origin_ack(store, "origin", @discussion_feed_seq)
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
    assert record.message["answer_ui"] == @seen
    assert record.revisions[1] == record.message

    assert %{"message" => %{"message_type" => "fyi"}} =
             after_start.engine.origin_feeds[{"A", "origin"}].entries[1]

    assert [%{"entry_id" => entry_id, "delivery" => delivery}] =
             Enum.filter(Resources.entries(after_start.engine, @key), &(&1["author"] == "person"))

    assert %{"delivered" => true, "acted" => false, "receipt_expected" => false} = delivery
    refute Map.has_key?(delivery, "legacy_caption")
    refute Map.has_key?(delivery, "outcome")

    assert after_start.engine.origin_receipts == before.engine.origin_receipts
    assert {:ok, next_head} = ChangeLog.head(after_start, "A")
    assert next_head > head
    target_seq = entry_id |> String.trim_leading("e") |> String.to_integer()
    target = ChangeLog.entry_resource(target_seq)
    assert {:ok, changes, false, ^next_head} = ChangeLog.read(after_start, "A", head, 100)
    assert Enum.any?(changes, &match?(%{kind: :entry, resource_id: ^target}, &1))
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

    assert {:ok, with_receipt, _} =
             Engine.origin_ack2(restarted.engine, "origin", @discussion_feed_seq, %{
               "op_id" => "00000000-0000-4000-8000-000000000003",
               "outcome" => "handed"
             })

    assert [%{"delivery" => %{"outcome" => "handed", "receipt_expected" => false}}] =
             Enum.filter(Resources.entries(with_receipt, @key), &(&1["entry_id"] == entry_id))

    GenServer.stop(relay)
  end
end
