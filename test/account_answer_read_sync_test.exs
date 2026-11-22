defmodule AttentionServer.AccountAnswerReadSyncTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{JSON, Relay, RelayReaders}
  alias AttentionServer.ClientAPI.{Reads, Resources}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  setup do
    dir =
      Path.join(System.tmp_dir!(), "harklight-account-sync-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{path: Path.join(dir, "state.sqlite3")}
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp request(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp no_comment_request(relay, key, frame) do
    assert {:ok, line} = Relay.request_line(relay, key, JSON.encode!(frame) <> "\n")
    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  # The read position a device of the account reads in the v2 item.
  defp read_position(relay, device, id) do
    {:ok, item} =
      Resources.item_id(:sys.get_state(relay).store.engine, {"A", "app:test", id})

    assert {:ok, %{"item" => %{"chat" => %{"read_position" => position}}}} =
             Reads.item(relay, device, item)

    position
  end

  defp forward(relay, id, op_id) do
    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(op_id),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => id,
                 "created_at" => op_id,
                 "title" => "A request"
               }
             })
  end

  defp agent_reply(relay, id, discussion_op, reply_op) do
    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(discussion_op),
               "sender_id" => "app:test",
               "message_id" => id,
               "request_revision" => 1,
               "text" => "A question"
             })

    entries = Map.values(:sys.get_state(relay).store.engine.origin_feeds[{"A", "origin"}].entries)

    discussion =
      entries
      |> Enum.sort_by(& &1["feed_seq"])
      |> Enum.filter(&(&1["kind"] == "discussion" and &1["message_id"] == id))
      |> List.last()

    assert is_integer(discussion["feed_seq"])

    assert %{"type" => "ok", "seq" => seq} =
             request(relay, "origin", %{
               "type" => "conversation_reply",
               "op_id" => op(reply_op),
               "sender_id" => "app:test",
               "message_id" => id,
               "request_revision" => 1,
               "in_reply_to" => discussion["feed_seq"],
               "text" => "An answer"
             })

    seq
  end

  test "all account phones receive the same terminal outcome while legacy answer stays origin-only",
       %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_device(relay, "A", "reader2")
    assert :ok = Relay.setup_device(relay, "A", "offline")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "B", "foreign")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    forward(relay, "seen", 1)

    assert {:ok, [%{"type" => "delivery", "message_id" => "seen"}], 1} =
             RelayReaders.events(relay, "reader2", 0)

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(2),
               "sender_id" => "app:test",
               "message_id" => "seen",
               "request_revision" => 1,
               "answer" => nil
             })

    assert {:ok, [seen], 2} = RelayReaders.events(relay, "reader2", 1)
    assert seen["answer_outcome"] == %{"kind" => "seen"}
    refute Map.has_key?(seen, "answer")

    assert seen["type"] == "lifecycle"
    assert seen["message_id"] == "seen"

    assert {:ok, [origin_seen], 2} = RelayReaders.events(relay, "origin", 1)
    assert Map.has_key?(origin_seen, "answer")
    assert origin_seen["answer_outcome"] == seen["answer_outcome"]

    forward(relay, "reply", 3)

    assert {:ok, [%{"type" => "delivery", "message_id" => "reply"}], 3} =
             RelayReaders.events(relay, "reader2", 2)

    assert %{"type" => "ok"} =
             request(relay, "reader2", %{
               "type" => "answer",
               "op_id" => op(4),
               "sender_id" => "app:test",
               "message_id" => "reply",
               "request_revision" => 1,
               "answer" => "On it, 5 min"
             })

    assert {:ok, [reply], 4} = RelayReaders.events(relay, "reader", 3)
    assert reply["answer_outcome"] == %{"kind" => "reply", "text" => "On it, 5 min"}
    refute Map.has_key?(reply, "answer")

    assert {:ok, [%{"type" => "lifecycle", "message_id" => "reply"} = reply2], 4} =
             RelayReaders.events(relay, "reader2", 3)

    assert reply2["answer_outcome"] == reply["answer_outcome"]

    forward(relay, "no-comment", 5)

    assert {:ok, [%{"type" => "delivery", "message_id" => "no-comment"}], 5} =
             RelayReaders.events(relay, "reader2", 4)

    assert %{"type" => "ok", "terminal_kind" => "no_comment"} =
             no_comment_request(relay, "reader", %{
               "type" => "answer_no_comment",
               "request_id" => "no-comment",
               "request_revision" => 1
             })

    assert {:ok, [no_comment], 6} = RelayReaders.events(relay, "reader2", 5)
    assert no_comment["answer_outcome"] == %{"kind" => "no_comment"}
    assert no_comment["terminal_kind"] == "no_comment"

    assert no_comment["type"] == "lifecycle"
    assert no_comment["message_id"] == "no-comment"

    assert {:ok, replay, 6} = RelayReaders.events(relay, "offline", 0)

    outcomes =
      replay
      |> Enum.filter(&(&1["type"] == "lifecycle"))
      |> Map.new(&{&1["message_id"], &1["answer_outcome"]})

    assert outcomes == %{
             "seen" => %{"kind" => "seen"},
             "reply" => %{"kind" => "reply", "text" => "On it, 5 min"},
             "no-comment" => %{"kind" => "no_comment"}
           }

    refute Enum.any?(replay, &Map.has_key?(&1, "answer"))

    # The v2 inbox shows every device of the account the same outcomes, and
    # another person's device none of them.
    for device <- ["origin", "reader", "reader2", "offline"] do
      assert {:ok, %{"items" => items}} = Reads.inbox(relay, device)

      assert Map.new(items, &{&1["message"]["message_id"], &1["terminal"]["answer"]}) ==
               outcomes
    end

    assert {:ok, [], 0} = RelayReaders.events(relay, "foreign", 0)
    assert {:ok, %{"items" => []}} = Reads.inbox(relay, "foreign")

    GenServer.stop(relay)
  end

  test "read position validates reply provenance, replays once, and survives restart", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_device(relay, "A", "reader2")
    assert :ok = Relay.setup_device(relay, "B", "foreign")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")
    forward(relay, "thread", 11)

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(12),
               "sender_id" => "app:test",
               "message_id" => "thread",
               "request_revision" => 1,
               "text" => "Please explain"
             })

    assert %{"type" => "ok", "seq" => 2} =
             request(relay, "origin", %{
               "type" => "conversation_reply",
               "op_id" => op(13),
               "sender_id" => "app:test",
               "message_id" => "thread",
               "request_revision" => 1,
               "in_reply_to" => 2,
               "text" => "Here is the answer"
             })

    assert read_position(relay, "reader2", "thread") == nil

    read = %{
      "type" => "conversation_read",
      "op_id" => op(14),
      "sender_id" => "app:test",
      "message_id" => "thread",
      "read_through_conversation_seq" => 2
    }

    assert %{"type" => "error", "reason" => "not_found"} =
             request(relay, "reader", %{read | "op_id" => op(15), "message_id" => "absent"})

    assert %{"type" => "error", "reason" => "not_person"} = request(relay, "origin", read)
    assert %{"type" => "error", "reason" => "not_found"} = request(relay, "foreign", read)

    assert %{
             "type" => "error",
             "reason" => "invalid_field",
             "field" => "read_through_conversation_seq"
           } =
             request(relay, "reader", %{
               read
               | "op_id" => op(16),
                 "read_through_conversation_seq" => 1
             })

    assert %{
             "type" => "ok",
             "read_through_conversation_seq" => 2,
             "seq" => read_seq,
             "event_id" => event_id
           } = request(relay, "reader", read)

    assert is_binary(event_id) and event_id != ""

    # Another device of the account gets the read position as a fact.
    assert {:ok, [event], ^read_seq} =
             RelayReaders.conversation_events(relay, "reader2", read_seq - 1)

    assert event["kind"] == "read_position"
    assert event["conversation_seq"] == read_seq
    assert event["event_id"] == event_id
    assert event["read_through_conversation_seq"] == 2
    refute Map.has_key?(event, "request_revision")
    refute Map.has_key?(event, "author")

    assert %{"type" => "ok", "duplicate" => true, "seq" => ^read_seq, "event_id" => ^event_id} =
             request(relay, "reader", read)

    assert %{"type" => "error", "reason" => "operation_conflict"} =
             request(relay, "reader", %{read | "read_through_conversation_seq" => 1})

    assert read_position(relay, "reader2", "thread") == Resources.entry_id(2)

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)

    assert {:ok, [replayed], ^read_seq} =
             RelayReaders.conversation_events(restarted, "reader2", read_seq - 1)

    assert replayed["event_id"] == event_id
    assert read_position(restarted, "reader2", "thread") == Resources.entry_id(2)

    assert %{"type" => "ok", "duplicate" => true, "seq" => ^read_seq, "event_id" => ^event_id} =
             request(restarted, "reader", read)

    GenServer.stop(restarted)
  end

  test "a lower valid reply is a no-op and account positions stay independent by thread", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_device(relay, "A", "reader2")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    forward(relay, "alpha", 21)
    first_alpha = agent_reply(relay, "alpha", 22, 23)
    later_alpha = agent_reply(relay, "alpha", 24, 25)
    forward(relay, "beta", 26)
    beta_reply = agent_reply(relay, "beta", 27, 28)

    assert %{"type" => "ok", "seq" => read_seq, "read_through_conversation_seq" => ^later_alpha} =
             request(relay, "reader", %{
               "type" => "conversation_read",
               "op_id" => op(29),
               "sender_id" => "app:test",
               "message_id" => "alpha",
               "read_through_conversation_seq" => later_alpha
             })

    assert %{"type" => "ok", "read_through_conversation_seq" => ^later_alpha} =
             no_op =
             request(relay, "reader", %{
               "type" => "conversation_read",
               "op_id" => op(30),
               "sender_id" => "app:test",
               "message_id" => "alpha",
               "read_through_conversation_seq" => first_alpha
             })

    refute Map.has_key?(no_op, "seq")
    refute Map.has_key?(no_op, "event_id")
    assert {:ok, [], ^read_seq} = RelayReaders.conversation_events(relay, "reader2", read_seq)

    assert read_position(relay, "reader2", "alpha") == Resources.entry_id(later_alpha)
    assert read_position(relay, "reader2", "beta") == nil

    assert %{"type" => "ok", "read_through_conversation_seq" => ^beta_reply} =
             request(relay, "reader", %{
               "type" => "conversation_read",
               "op_id" => op(31),
               "sender_id" => "app:test",
               "message_id" => "beta",
               "read_through_conversation_seq" => beta_reply
             })

    assert read_position(relay, "reader2", "alpha") == Resources.entry_id(later_alpha)
    assert read_position(relay, "reader2", "beta") == Resources.entry_id(beta_reply)

    GenServer.stop(relay)
  end
end
