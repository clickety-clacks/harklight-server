defmodule AttentionServer.RouteBBoundedReplayTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{Engine, JSON, Relay}

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

  defp route_b_response(relay, session, frame, logical_frames) do
    raw = JSON.encode!(Map.put(frame, "v", 1)) <> "\n"
    {dispatch_us, result} =
      :timer.tc(fn -> Relay.session_request_line(relay, session, raw) end)
    assert {:ok, lines} = result

    decoded =
      Enum.map(lines, fn line ->
        assert {:ok, response} = JSON.decode_line(line)
        response
      end)

    %{
      dispatch_us: dispatch_us,
      wire_frames: length(lines),
      logical_frames: logical_frames.(decoded),
      bytes: Enum.reduce(lines, 0, &(byte_size(&1) + &2)),
      normalized_bytes:
        decoded
        |> Enum.map(&normalize_route_b_sequences/1)
        |> Enum.reduce(0, &(byte_size(JSON.encode!(&1)) + 1 + &2)),
      decoded: decoded
    }
  end

  defp normalize_route_b_sequences(value) when is_list(value),
    do: Enum.map(value, &normalize_route_b_sequences/1)

  defp normalize_route_b_sequences(value) when is_map(value) do
    Map.new(value, fn {key, nested} ->
      if key in ["seq", "conversation_seq", "latest_origin_reply_seq"] and
           is_integer(nested) do
        {key, 0}
      else
        {key, normalize_route_b_sequences(nested)}
      end
    end)
  end

  defp normalize_route_b_sequences(value), do: value

  defp median_request(relay, session, frame, logical_frames) do
    samples =
      Enum.map(1..7, fn _ -> route_b_response(relay, session, frame, logical_frames) end)
    first = hd(samples)

    assert Enum.all?(samples, fn sample ->
             Map.take(sample, [:wire_frames, :logical_frames, :bytes, :normalized_bytes]) ==
               Map.take(first, [:wire_frames, :logical_frames, :bytes, :normalized_bytes])
           end)

    median_dispatch_us = samples |> Enum.map(& &1.dispatch_us) |> Enum.sort() |> Enum.at(3)
    %{first | dispatch_us: median_dispatch_us}
  end

  defp scale_metric(metric) do
    %{
      "median_dispatch_us" => metric.dispatch_us,
      "wire_frames" => metric.wire_frames,
      "logical_frames" => metric.logical_frames,
      "payload_rows" =>
        Enum.reduce(metric.decoded, 0, &(length(Map.get(&1, "rows", [])) + &2)),
      "conversation_frames" =>
        Enum.reduce(metric.decoded, 0, &(length(Map.get(&1, "frames", [])) + &2)),
      "bytes" => metric.bytes,
      "normalized_bytes" => metric.normalized_bytes
    }
  end

  defp synthetic_history_specs(history_frames, old_keys, visible_key) do
    old_event_count = history_frames - 50
    old_count_per_key = div(old_event_count, length(old_keys))
    extra_old_keys = rem(old_event_count, length(old_keys))

    old_specs =
      old_keys
      |> Enum.with_index()
      |> Enum.flat_map(fn {key, index} ->
        count = old_count_per_key + if(index < extra_old_keys, do: 1, else: 0)

        kinds =
          [:discussion, :reply, :availability, :read_position] ++
            Enum.take(Stream.cycle([:discussion, :reply]), count - 4)

        Enum.map(kinds, &{key, &1})
      end)

    visible_specs =
      Enum.map(0..49, fn index ->
        kind = if rem(index, 2) == 0, do: :discussion, else: :reply
        {visible_key, kind}
      end)

    old_specs ++ visible_specs
  end

  defp seed_synthetic_history(relay, history_frames, old_keys, visible_key) do
    :sys.replace_state(relay, fn state ->
      engine = state.store.engine
      specs = synthetic_history_specs(history_frames, old_keys, visible_key)

      {events, read_positions, discussed_messages, first_replies} =
        specs
        |> Enum.with_index(1)
        |> Enum.reduce({%{}, %{}, engine.bounce_discussed_messages, %{}}, fn
          {{key, kind}, seq}, {events, read_positions, discussed, first_replies} ->
            event_id =
              "00000000-0000-4000-8000-" <>
                String.pad_leading(Integer.to_string(seq), 12, "0")

            payload =
              case kind do
                :discussion ->
                  %{
                    "kind" => "discussion",
                    "author" => "person",
                    "event_id" => event_id,
                    "request_revision" => 1,
                    "text" => "Synthetic discussion"
                  }

                :reply ->
                  %{
                    "kind" => "reply",
                    "author" => "origin",
                    "event_id" => event_id,
                    "request_revision" => 1,
                    "text" => "Synthetic reply"
                  }

                :availability ->
                  %{
                    "kind" => "availability",
                    "availability" => "available",
                    "last_detached_at" => nil,
                    "listening_support" => nil,
                    "unsupported_reason" => nil
                  }

                :read_position ->
                  reply_seq = Map.fetch!(first_replies, key)

                  %{
                    "kind" => "read_position",
                    "event_id" => "read:" <> event_id,
                    "read_through_conversation_seq" => reply_seq
                  }
              end

            first_replies =
              if kind == :reply, do: Map.put_new(first_replies, key, seq), else: first_replies

            read_positions =
              if kind == :read_position,
                do: Map.put(read_positions, key, Map.fetch!(first_replies, key)),
                else: read_positions

            discussed = if kind == :discussion, do: MapSet.put(discussed, key), else: discussed
            event = %{kind: :conversation, key: key, payload: payload}

            {Map.put(events, {"A", seq}, event), read_positions, discussed, first_replies}
        end)

      engine =
        %{
          engine
          | conversation_highs: Map.put(engine.conversation_highs, "A", history_frames),
            conversation_events: events,
            chat_read_positions: read_positions,
            bounce_discussed_messages: discussed_messages
        }
        |> Engine.rebuild_route_b_index()

      %{state | store: %{state.store | engine: engine}}
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

  @tag timeout: 300_000
  test "Route B request output stays bounded across synthetic history sizes", %{path: path} do
    runs =
      for history_frames <- [1_000, 10_000, 50_000] do
        store_path = path <> ".#{history_frames}"
        assert {:ok, relay} = Relay.start_link(store_path: store_path)
        setup_people(relay)

        old_ids = Enum.map(1..5, &"old-#{&1}")

        for {message_id, operation} <- Enum.zip(old_ids, 2..6) do
          create_message(relay, message_id, operation)
        end

        for {message_id, operation} <- Enum.zip(old_ids, 7..11) do
          assert %{"type" => "ok"} =
                   request(relay, "reader", %{
                     "type" => "answer",
                     "op_id" => op(operation),
                     "sender_id" => "app:test",
                     "message_id" => message_id,
                     "answer" => nil
                   })
        end

        current_ids = Enum.map(1..19, &"current-#{&1}") ++ ["visible-chat"]

        for {message_id, operation} <- Enum.zip(current_ids, 12..31) do
          create_message(relay, message_id, operation)
        end

        old_keys = Enum.map(old_ids, &{"A", "app:test", &1})
        visible_key = {"A", "app:test", "visible-chat"}
        seed_synthetic_history(relay, history_frames, old_keys, visible_key)

        seeded_engine = :sys.get_state(relay).store.engine

        assert Enum.all?(old_keys, fn key ->
                 summary = seeded_engine.route_b_index.conversation_summary_by_item[key]

                 not is_nil(seeded_engine.messages[key].terminal) and summary.chat_started and
                   summary.latest_origin_reply_seq > 0 and
                   summary.availability.payload["availability"] == "available" and
                   Map.get(seeded_engine.chat_read_positions, key, 0) > 0
               end)

        assert {:ok, session} = Relay.open_session(relay, "reader")

        list_metric =
          median_request(relay, session, %{"type" => "inbox_list", "limit" => 20}, fn [reply] ->
            1 + length(reply["rows"])
          end)

        list_reply = hd(list_metric.decoded)
        assert list_reply["type"] == "ok"
        assert length(list_reply["rows"]) == 20
        assert list_reply["unresolved_count"] == 20
        assert list_reply["conversation_seq"] == history_frames
        assert Enum.all?(list_reply["rows"], &(&1["message_id"] not in old_ids))

        page_metric =
          median_request(
            relay,
            session,
            %{
              "type" => "conversation_page",
              "sender_id" => "app:test",
              "message_id" => "visible-chat",
              "limit" => 50
            },
            fn [reply] -> 2 + length(reply["frames"]) end
          )

        page_reply = hd(page_metric.decoded)
        assert page_reply["type"] == "ok"
        assert length(page_reply["frames"]) == 50
        assert page_reply["conversation_seq"] == history_frames
        assert page_reply["row"]["chat_read_position"] == 0

        resync_metric =
          route_b_response(
            relay,
            session,
            %{"type" => "resync", "since" => list_reply["seq"]},
            fn replies -> length(replies) end
          )

        assert Enum.map(resync_metric.decoded, & &1["type"]) == ["ok", "resynced"]

        first_subscribe_metric =
          route_b_response(
            relay,
            session,
            %{"type" => "conversation_open", "since" => history_frames, "bounded" => true},
            &length/1
          )

        assert Enum.map(first_subscribe_metric.decoded, & &1["type"]) == [
                 "ok",
                 "conversation_resynced"
               ]

        conversation_resynced = List.last(first_subscribe_metric.decoded)
        refute Map.has_key?(conversation_resynced, "chat_read_positions")

        assert :ok = Relay.close_session(relay, session)
        assert {:ok, reconnect_session} = Relay.open_session(relay, "reader")

        assert [%{"type" => "ok"}, %{"type" => "resynced"}] =
                 session_request(
                   relay,
                   reconnect_session,
                   %{"type" => "resync", "since" => list_reply["seq"]}
                 )

        reconnect_subscribe_metric =
          route_b_response(
            relay,
            reconnect_session,
            %{"type" => "conversation_open", "since" => history_frames, "bounded" => true},
            &length/1
          )

        assert Enum.map(reconnect_subscribe_metric.decoded, & &1["type"]) == [
                 "ok",
                 "conversation_resynced"
               ]

        metric = %{
          "history_frames" => history_frames,
          "inbox_list" => scale_metric(list_metric),
          "conversation_page" => scale_metric(page_metric),
          "resync" => scale_metric(resync_metric),
          "first_subscribe_commit_and_reply" => scale_metric(first_subscribe_metric),
          "reconnect_subscribe" => scale_metric(reconnect_subscribe_metric)
        }

        IO.puts("ROUTE_B_SCALE " <> JSON.encode!(metric))
        GenServer.stop(relay)
        metric
      end

    for field <- [
          "inbox_list",
          "conversation_page",
          "resync",
          "first_subscribe_commit_and_reply",
          "reconnect_subscribe"
        ] do
      logical_frames = Enum.map(runs, &get_in(&1, [field, "logical_frames"]))
      normalized_bytes = Enum.map(runs, &get_in(&1, [field, "normalized_bytes"]))
      assert length(Enum.uniq(logical_frames)) == 1
      assert length(Enum.uniq(normalized_bytes)) == 1
    end
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
    assert row["conversation_seq"] == hd(page["frames"])["conversation_seq"]
    assert is_map(row["availability"])
    assert is_map(row["priority_view"])

    assert [older] =
             session_request(relay, session, %{
               "type" => "conversation_page",
               "sender_id" => "app:test",
               "message_id" => "chat",
               "before" => hd(page["frames"])["conversation_seq"],
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
