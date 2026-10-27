defmodule AttentionServer.AccountAnswerReadSyncTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{JSON, Relay}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  setup do
    dir = Path.join(System.tmp_dir!(), "harklight-account-sync-#{System.unique_integer([:positive])}")
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

  defp session_request(relay, session, frame) do
    assert {:ok, lines} =
             Relay.session_request_line(relay, session, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    Enum.map(lines, fn line ->
      assert {:ok, response} = JSON.decode_line(line)
      response
    end)
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

  test "all account phones receive the same terminal outcome while legacy answer stays origin-only",
       %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_device(relay, "A", "reader2")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    forward(relay, "seen", 1)
    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(2),
               "sender_id" => "app:test",
               "message_id" => "seen",
               "request_revision" => 1,
               "answer" => nil
             })

    assert {:ok, [seen], 2} = Relay.events(relay, "reader2", 1)
    assert seen["answer_outcome"] == %{"kind" => "seen"}
    refute Map.has_key?(seen, "answer")
    assert {:ok, [origin_seen], 2} = Relay.events(relay, "origin", 1)
    assert Map.has_key?(origin_seen, "answer")
    assert origin_seen["answer_outcome"] == seen["answer_outcome"]

    forward(relay, "reply", 3)
    assert %{"type" => "ok"} =
             request(relay, "reader2", %{
               "type" => "answer",
               "op_id" => op(4),
               "sender_id" => "app:test",
               "message_id" => "reply",
               "request_revision" => 1,
               "answer" => "On it, 5 min"
             })

    assert {:ok, [reply], 4} = Relay.events(relay, "reader", 3)
    assert reply["answer_outcome"] == %{"kind" => "reply", "text" => "On it, 5 min"}
    refute Map.has_key?(reply, "answer")

    forward(relay, "no-comment", 5)
    assert %{"type" => "ok", "terminal_kind" => "no_comment"} =
             no_comment_request(relay, "reader", %{
               "type" => "answer_no_comment",
               "request_id" => "no-comment",
               "request_revision" => 1
             })

    assert {:ok, [no_comment], 6} = Relay.events(relay, "reader2", 5)
    assert no_comment["answer_outcome"] == %{"kind" => "no_comment"}
    assert no_comment["terminal_kind"] == "no_comment"
    GenServer.stop(relay)
  end

  test "read position validates reply provenance, replays once, and survives restart", %{path: path} do
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

    assert {:ok, reader_session} = Relay.open_session(relay, "reader")
    assert List.last(session_request(relay, reader_session, %{"type" => "resync", "since" => 0}))[
             "type"
           ] == "resynced"

    assert {:ok, other_session} = Relay.open_session(relay, "reader2")
    assert List.last(session_request(relay, other_session, %{"type" => "resync", "since" => 0}))[
             "type"
           ] == "resynced"

    assert %{"type" => "conversation_resynced", "chat_read_positions" => []} =
             List.last(
               session_request(relay, other_session, %{"type" => "conversation_open", "since" => 0})
             )

    read = %{
      "type" => "conversation_read",
      "op_id" => op(14),
      "sender_id" => "app:test",
      "message_id" => "thread",
      "read_through_conversation_seq" => 2
    }

    assert [%{"type" => "error", "reason" => "not_found"}] =
             session_request(relay, reader_session, %{read | "op_id" => op(15), "message_id" => "absent"})

    assert %{"type" => "error", "reason" => "not_person"} = request(relay, "origin", read)
    assert %{"type" => "error", "reason" => "not_found"} = request(relay, "foreign", read)

    assert [%{"type" => "error", "reason" => "invalid_field", "field" => "read_through_conversation_seq"}] =
             session_request(relay, reader_session, %{read | "op_id" => op(16), "read_through_conversation_seq" => 1})

    assert [%{"type" => "ok", "read_through_conversation_seq" => 2, "seq" => read_seq,
             "event_id" => event_id}] = session_request(relay, reader_session, read)
    assert is_binary(event_id) and event_id != ""

    assert {:ok, [read_line]} = Relay.session_drain(relay, other_session)
    assert {:ok, event} = JSON.decode_line(read_line)
    assert event["kind"] == "read_position"
    assert event["conversation_seq"] == read_seq
    assert event["event_id"] == event_id
    assert event["read_through_conversation_seq"] == 2
    refute Map.has_key?(event, "request_revision")
    refute Map.has_key?(event, "author")

    assert [%{"type" => "ok", "duplicate" => true, "seq" => ^read_seq, "event_id" => ^event_id}] =
             session_request(relay, reader_session, read)
    assert [%{"type" => "error", "reason" => "operation_conflict"}] =
             session_request(relay, reader_session, %{read | "read_through_conversation_seq" => 1})

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, [replayed], ^read_seq} = Relay.conversation_events(restarted, "reader2", read_seq - 1)
    assert replayed["event_id"] == event_id
    assert {:ok, reopened} = Relay.open_session(restarted, "reader2")
    assert List.last(session_request(restarted, reopened, %{"type" => "resync", "since" => 0}))[
             "type"
           ] == "resynced"

    assert %{
             "type" => "conversation_resynced",
             "chat_read_positions" => [
               %{
                 "sender_id" => "app:test",
                 "message_id" => "thread",
                 "read_through_conversation_seq" => 2
               }
             ]
           } =
             List.last(
               session_request(restarted, reopened, %{
                 "type" => "conversation_open",
                 "since" => read_seq
               })
             )

    GenServer.stop(restarted)
  end
end
