defmodule AttentionServer.RouteBBoundedReplayTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{JSON, Relay}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  setup do
    dir =
      Path.join(System.tmp_dir!(), "harklight-route-b-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{path: Path.join(dir, "state.sqlite3")}
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp request(relay, key, frame) do
    assert {:ok, line} = Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")
    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp session_request(relay, session, frame) do
    assert {:ok, lines} =
             Relay.session_request_line(
               relay,
               session,
               JSON.encode!(Map.put(frame, "v", 1)) <> "\n"
             )

    Enum.map(lines, fn line ->
      assert {:ok, decoded} = JSON.decode_line(line)
      decoded
    end)
  end

  defp create_message(relay, id, operation) do
    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(operation),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => id,
                 "created_at" => operation,
                 "title" => "Question #{id}",
                 "body" => "Body #{id}"
               }
             })
  end

  defp setup_people(relay) do
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "subscribe_remote",
               "op_id" => op(1),
               "revision" => 1,
               "sender_ids" => ["app:test"]
             })
  end

  test "inbox_list pages current server order without changing sync or cursors", %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    setup_people(relay)

    create_message(relay, "resolved-old", 2)
    create_message(relay, "resolved-new", 3)
    create_message(relay, "active-a", 4)
    create_message(relay, "active-b", 5)

    for {message_id, operation} <- [{"resolved-old", 6}, {"resolved-new", 7}] do
      assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(operation),
               "sender_id" => "app:test",
               "message_id" => message_id,
               "answer" => nil
             })
    end

    assert {:ok, session} = Relay.open_session(relay, "reader")
    before = :sys.get_state(relay)
    engine = before.store.engine
    view = before.priority_views["A"]

    resolved_order =
      engine.messages
      |> Enum.filter(fn {{person, _, _}, record} -> person == "A" and not is_nil(record.terminal) end)
      |> Enum.sort_by(fn {{_, sender, id}, record} -> {-record.accepted_seq, sender, id} end)
      |> Enum.map(fn {{_, sender, id}, _} -> {sender, id} end)

    expected = view.message_order ++ resolved_order
    assert length(expected) == 4

    assert [first] =
             session_request(relay, session, %{
               "type" => "inbox_list",
               "ref" => "first-page",
             "limit" => 1
             })

    assert first["type"] == "ok"
    assert first["ref"] == "first-page"
    assert Enum.map(first["rows"], &{&1["sender_id"], &1["message_id"]}) == Enum.take(expected, 1)

    badge_count =
      Enum.count(engine.messages, fn {{person, _, _}, record} ->
        person == "A" and is_nil(record.terminal)
      end)

    assert first["unresolved_count"] == badge_count
    assert first["unresolved_count"] == 2
    assert first["view_rev"] == view.view_rev
    assert is_binary(first["next_cursor"])
    assert Enum.all?(first["rows"], &(&1["as"] == "message"))
    assert Enum.all?(first["rows"], &(&1["message"]["body"] =~ "Body"))

    after_first = :sys.get_state(relay)
    assert after_first.store.engine == engine
    assert after_first.sessions[session].sync_mode == nil
    refute after_first.sessions[session].ready

    assert [second] =
             session_request(relay, session, %{
               "type" => "inbox_list",
               "limit" => 1,
               "cursor" => first["next_cursor"]
             })

    assert Enum.map(second["rows"], &{&1["sender_id"], &1["message_id"]}) == [Enum.at(expected, 1)]
    assert is_binary(second["next_cursor"])
    assert second["unresolved_count"] == first["unresolved_count"]
    assert :sys.get_state(relay).store.engine == engine

    assert [third] =
             session_request(relay, session, %{
               "type" => "inbox_list",
               "limit" => 1,
               "cursor" => second["next_cursor"]
             })

    assert Enum.map(third["rows"], &{&1["sender_id"], &1["message_id"]}) == [Enum.at(expected, 2)]
    assert is_binary(third["next_cursor"])
    assert %{"terminal" => %{"state" => "answered"}} = hd(third["rows"])

    assert [fourth] =
             session_request(relay, session, %{
               "type" => "inbox_list",
               "limit" => 1,
               "cursor" => third["next_cursor"]
             })

    assert Enum.map(fourth["rows"], &{&1["sender_id"], &1["message_id"]}) == [Enum.at(expected, 3)]
    refute Map.has_key?(fourth, "next_cursor")

    assert [%{"type" => "ok", "rows" => all_rows}] =
             session_request(relay, session, %{"type" => "inbox_list", "limit" => 500})

    assert length(all_rows) == 4

    assert [%{"type" => "error", "reason" => "invalid_field", "field" => "limit"}] =
             session_request(relay, session, %{"type" => "inbox_list", "limit" => 0})

    assert [%{"type" => "error", "reason" => "invalid_cursor"}] =
             session_request(relay, session, %{"type" => "inbox_list", "cursor" => "bad"})

    assert [%{"type" => "ok"}, %{"type" => "resynced"}] =
             session_request(relay, session, %{"type" => "resync", "since" => first["seq"]})

    assert :sys.get_state(relay).sessions[session].sync_mode == :legacy

    assert [%{"type" => "ok", "rows" => legacy_rows}] =
             session_request(relay, session, %{"type" => "inbox_list", "limit" => 20})

    assert length(legacy_rows) == 4
    assert :sys.get_state(relay).sessions[session].sync_mode == :legacy
    assert {:ok, 0} = Relay.acknowledged_cursor(relay, "reader")
    GenServer.stop(relay)

    assert {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, restarted_session} = Relay.open_session(restarted, "reader")

    assert [%{"type" => "ok", "unresolved_count" => 2, "rows" => restarted_rows}] =
             session_request(restarted, restarted_session, %{"type" => "inbox_list"})

    assert length(restarted_rows) == 4
    GenServer.stop(restarted)
  end

  test "conversation_page returns current row and bounded frames for one item", %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path, agent_fixture_mode: true)
    setup_people(relay)
    create_message(relay, "chat", 2)

    assert %{"type" => "ok", "event_id" => discussion_id} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(3),
               "sender_id" => "app:test",
               "message_id" => "chat",
               "request_revision" => 1,
               "text" => "Could you clarify?"
             })

    assert %{"type" => "ok", "seq" => reply_seq} =
             request(relay, "origin", %{
               "type" => "conversation_reply",
               "op_id" => op(4),
               "sender_id" => "app:test",
               "message_id" => "chat",
               "request_revision" => 1,
               "in_reply_to" => String.to_integer(discussion_id),
               "text" => "Here is more detail"
             })

    assert %{"type" => "ok", "read_through_conversation_seq" => ^reply_seq} =
             request(relay, "reader", %{
               "type" => "conversation_read",
               "op_id" => op(5),
               "sender_id" => "app:test",
               "message_id" => "chat",
               "read_through_conversation_seq" => reply_seq
             })

    assert %{"type" => "ok", "opened" => true} =
             request(relay, "reader", %{
               "type" => "message_open",
               "op_id" => op(6),
               "sender_id" => "app:test",
               "message_id" => "chat",
               "request_revision" => 1
             })

    assert {:ok, session} = Relay.open_session(relay, "reader")
    state_before = :sys.get_state(relay)
    engine_before = state_before.store.engine

    assert [page] =
             session_request(relay, session, %{
               "type" => "conversation_page",
               "ref" => "chat-page",
               "sender_id" => "app:test",
               "message_id" => "chat",
               "limit" => 1
             })

    assert page["type"] == "ok"
    assert page["ref"] == "chat-page"
    assert length(page["frames"]) == 1
    assert page["older"]
    assert page["frames"] == [List.last(elem(Relay.conversation_events(relay, "reader", 0), 1))]

    row = page["row"]
    assert row["sender_id"] == "app:test"
    assert row["message_id"] == "chat"
    assert row["origin"] == "origin"
    assert row["session_handle"] == @handle
    assert row["request_revision"] == 1
    assert row["message"]["title"] == "Question chat"
    assert row["files"] == []
    assert row["as"] == "message"
    assert row["opened"]
    assert row["chat_started"]
    assert row["chat_read_position"] == reply_seq
    assert row["latest_origin_reply_seq"] == reply_seq
    assert row["conversation_seq"] == page["frames"][0]["conversation_seq"]
    assert is_map(row["availability"])
    assert is_map(row["priority_view"])

    assert [older] =
             session_request(relay, session, %{
               "type" => "conversation_page",
               "sender_id" => "app:test",
               "message_id" => "chat",
               "before" => page["frames"][0]["conversation_seq"],
               "limit" => 100
             })

    assert Enum.map(older["frames"], & &1["conversation_seq"]) ==
             Enum.sort(Enum.map(older["frames"], & &1["conversation_seq"]))

    assert [item_only] =
             session_request(relay, session, %{
               "type" => "conversation_page",
               "sender_id" => "app:test",
               "message_id" => "chat",
               "limit" => 0
             })

    assert item_only["frames"] == []
    assert item_only["older"]

    assert [%{"type" => "error", "reason" => "not_found"}] =
             session_request(relay, session, %{
               "type" => "conversation_page",
               "sender_id" => "app:test",
               "message_id" => "absent",
               "limit" => 10
             })

    assert [%{"type" => "ok"}, %{"type" => "resynced"}] =
             session_request(relay, session, %{
               "type" => "resync",
               "since" => page["seq"]
             })

    assert [%{"type" => "ok", "row" => legacy_row}] =
             session_request(relay, session, %{
               "type" => "conversation_page",
               "sender_id" => "app:test",
               "message_id" => "chat",
               "limit" => 0
             })

    assert legacy_row["chat_started"]
    assert :sys.get_state(relay).sessions[session].sync_mode == :legacy

    state_after = :sys.get_state(relay)
    assert state_after.store.engine == engine_before
    assert state_after.sessions[session].sync_mode == nil
    refute state_after.sessions[session].ready
    GenServer.stop(relay)
  end

  test "bounded conversation_open catches up from list highs and enables view notices", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    setup_people(relay)
    create_message(relay, "first", 2)

    assert {:ok, session} = Relay.open_session(relay, "reader")

    assert [%{"type" => "ok", "seq" => listed_seq, "conversation_seq" => listed_conversation_seq,
              "view_rev" => listed_view_rev}] =
             session_request(relay, session, %{"type" => "inbox_list", "limit" => 20})

    create_message(relay, "between", 3)

    assert %{"type" => "ok", "event_id" => discussion_id} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(4),
               "sender_id" => "app:test",
               "message_id" => "first",
               "request_revision" => 1,
               "text" => "Committed between page and subscribe"
             })

    assert [resync_ok, delivery, resynced] =
             session_request(relay, session, %{"type" => "resync", "since" => listed_seq})

    assert resync_ok["type"] == "ok"
    assert delivery["type"] == "delivery"
    assert delivery["seq"] > listed_seq
    assert resynced["type"] == "resynced"

    assert [open_ok, catchup, conversation_resynced] =
             session_request(relay, session, %{
               "type" => "conversation_open",
               "since" => listed_conversation_seq,
               "bounded" => true
             })

    assert open_ok["type"] == "ok"
    assert catchup["type"] == "conversation"
    assert catchup["event_id"] == discussion_id
    assert conversation_resynced["type"] == "conversation_resynced"
    refute Map.has_key?(conversation_resynced, "chat_read_positions")
    assert conversation_resynced["view_rev"] != listed_view_rev

    assert %{"type" => "ok", "opened" => true} =
             request(relay, "reader", %{
               "type" => "message_open",
               "op_id" => op(5),
               "sender_id" => "app:test",
               "message_id" => "first",
               "request_revision" => 1
             })

    create_message(relay, "after-subscribe", 6)
    assert {:ok, notice_lines} = Relay.session_drain(relay, session)
    notices = Enum.map(notice_lines, fn line -> {:ok, frame} = JSON.decode_line(line); frame end)
    assert Enum.any?(notices, &(&1["type"] == "delivery"))
    assert Enum.any?(notices, &(&1["type"] == "view_changed"))
    assert {:ok, 0} = Relay.acknowledged_cursor(relay, "reader")
    assert {:ok, 0} = Relay.conversation_cursor(relay, "reader")

    GenServer.stop(relay)
  end
end
