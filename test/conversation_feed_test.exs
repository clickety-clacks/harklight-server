defmodule AttentionServer.ConversationFeedTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{BounceDecision, BouncePolicy, Engine, JSON, OriginWire, Relay, Store}
  alias Exqlite.Sqlite3

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  defp state_snapshot(path) do
    assert {:ok, db} = Sqlite3.open(path)

    assert {:ok, state_stmt} =
             Sqlite3.prepare(
               db,
               "SELECT version, payload, checksum FROM relay_state WHERE id = 1"
             )

    assert {:row, [version, payload, checksum]} = Sqlite3.step(db, state_stmt)
    assert :ok = Sqlite3.release(db, state_stmt)

    assert {:ok, overlay_stmt} =
             Sqlite3.prepare(db, "SELECT count(*) FROM relay_state_overlay_events")

    assert {:row, [overlay_count]} = Sqlite3.step(db, overlay_stmt)
    assert :ok = Sqlite3.release(db, overlay_stmt)
    assert :ok = Sqlite3.close(db)
    {version, payload, checksum, overlay_count}
  end

  setup do
    dir =
      Path.join(System.tmp_dir!(), "harklight-conversation-#{System.unique_integer([:positive])}")

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

  defp cold_overlay_reopen(path) do
    {output, status} =
      System.cmd(
        "mix",
        ["run", "--no-start", "--no-compile", "test/support/cold_overlay_reopen.exs"],
        cd: Path.expand("..", __DIR__),
        env: [
          {"MIX_ENV", "test"},
          {"COLD_STORE_PATH", path},
          {"COLD_PARENT_OS_PID", System.pid()},
          {"COLD_PERSON", "A"},
          {"COLD_ORIGIN", "origin"},
          {"COLD_HANDLE", @handle},
          {"COLD_SUPPORT_OP", op(92)}
        ],
        stderr_to_stdout: true
      )

    assert status == 0, "cold-reopen process failed (exit #{status}):\n#{output}"
    assert output =~ "COLD_OVERLAY_REOPEN_AND_USE_OK"
  end

  test "message_open stores one account-wide revision fact without changing feed or lifecycle cursors",
       %{
         path: path
       } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    forward = %{
      "type" => "forward",
      "op_id" => op(980),
      "session_handle" => @handle,
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => "opened-on-any-device",
        "created_at" => 1,
        "title" => "Open this message"
      }
    }

    assert %{"type" => "ok", "seq" => 1} = request(relay, "origin", forward)
    lifecycle_before = Relay.events(relay, "reader", 0)
    origin_before = Relay.origin_feed(relay, "origin", 0)
    conversation_before = Relay.conversation_events(relay, "reader", 0)
    state_before = :sys.get_state(relay).store.engine

    open_op_id = op(981)

    opened = %{
      "type" => "message_open",
      "op_id" => open_op_id,
      "sender_id" => "app:test",
      "message_id" => "opened-on-any-device",
      "request_revision" => 1
    }

    assert %{
             "type" => "ok",
             "op_id" => ^open_op_id,
             "sender_id" => "app:test",
             "message_id" => "opened-on-any-device",
             "request_revision" => 1,
             "opened" => true
           } = request(relay, "reader", opened)

    state_after = :sys.get_state(relay).store.engine

    assert state_after.opened_revisions ==
             MapSet.new([{"A", "app:test", "opened-on-any-device", 1}])

    assert state_after.events == state_before.events
    assert state_after.conversation_events == state_before.conversation_events
    assert state_after.origin_feeds == state_before.origin_feeds
    assert state_after.cursors == state_before.cursors
    assert Relay.events(relay, "reader", 0) == lifecycle_before
    assert Relay.origin_feed(relay, "origin", 0) == origin_before
    assert Relay.conversation_events(relay, "reader", 0) == conversation_before

    assert request(relay, "reader", opened)["type"] == "ok"

    assert %{"type" => "ok", "opened" => true} =
             request(relay, "origin", %{opened | "op_id" => op(982)})

    assert MapSet.size(:sys.get_state(relay).store.engine.opened_revisions) == 1

    assert %{"type" => "error", "reason" => "invalid_field", "field" => "request_revision"} =
             request(relay, "reader", %{opened | "op_id" => op(983), "request_revision" => 2})

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)

    assert MapSet.size(:sys.get_state(restarted).store.engine.opened_revisions) == 1
    assert request(restarted, "reader", opened)["type"] == "ok"
    assert Relay.events(restarted, "reader", 0) == lifecycle_before
    assert Relay.origin_feed(restarted, "origin", 0) == origin_before
    GenServer.stop(restarted)
  end

  test "due advisory is durable on its original cursor and never advances the ordinary feed", %{
    path: path
  } do
    clock = :atomics.new(1, [])
    :atomics.put(clock, 1, System.system_time(:millisecond))
    clock_fn = fn -> :atomics.get(clock, 1) end
    assert {:ok, relay} = Relay.start_link(store_path: path, clock: clock_fn)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    {:ok, levels} =
      BouncePolicy.normalize_levels([
        %{"name" => "Follow up", "lower_bound" => 0, "bounce_after_days" => 1}
      ])

    state = :sys.get_state(relay)

    {:ok, engine, _config} =
      BounceDecision.configure_levels(state.store.engine, "A", levels, clock_fn.())

    {:ok, configured_store} =
      Store.commit(state.store, engine, %{person: "A", actor: "owner", action: "test_levels"})

    :sys.replace_state(relay, &%{&1 | store: configured_store})

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(970),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "durable-advisory",
                 "created_at" => 1,
                 "title" => "Original request",
                 "answer_ui" => %{
                   "schema" => "attention-client.actions/1",
                   "actions" => [
                     %{
                       "id" => "seen",
                       "label" => "Seen",
                       "kind" => "value",
                       "value" => %{"acknowledgment" => "seen"}
                     }
                   ]
                 },
                 "message_type" => "fyi"
               }
             })

    assert {:ok, %{"listening_support" => %{"advisory_v1" => true}}} =
             Relay.origin_support(relay, "origin", %{
               "type" => "support",
               "op_id" => op(971),
               "session_handle" => @handle,
               "route" => "claude-code",
               "cli_version" => "test-only",
               "unsupported_reason" => nil,
               "advisory_v1" => true
             })

    assert :ok = Relay.origin_stream_attach(relay, "origin", @handle, self(), false, true)
    assert {:ok, ordinary_before} = Relay.origin_feed(relay, "origin", 0)
    state = :sys.get_state(relay)
    record = state.store.engine.messages[{"A", "app:test", "durable-advisory"}]
    due_at = record.accepted_at + 86_400_000

    selected =
      BounceDecision.record_scores(
        state.store.engine,
        "A",
        [%{"sender_id" => "app:test", "message_id" => "durable-advisory", "score" => 0.9}],
        record.accepted_at
      )

    {:ok, selected_store} =
      Store.commit(state.store, selected, %{
        person: "system",
        actor: "bounce_decision",
        action: "test_score_selection"
      })

    :sys.replace_state(relay, &%{&1 | store: selected_store})
    :atomics.put(clock, 1, due_at + 25)
    generation = :sys.get_state(relay).bounce_deadline_generation
    send(relay, {:bounce_due, generation})
    assert_receive {:origin_advisory_changed, @handle, 1}, 2_000

    assert {:ok, first_fetch} = Relay.origin_advisory_feed(relay, "origin", @handle, 0)
    assert first_fetch["cursor"] == 1
    assert [advisory] = first_fetch["entries"]
    assert advisory["delivered"] == false
    assert advisory["origin_cursor_at_decision"] == ordinary_before["cursor"]
    assert {:ok, ordinary_after} = Relay.origin_feed(relay, "origin", 0)
    assert ordinary_after == ordinary_before

    assert :sys.get_state(relay).store.engine.messages[{"A", "app:test", "durable-advisory"}].terminal ==
             nil

    assert {:ok, first_ack} =
             Relay.origin_advisory_ack(relay, "origin", @handle, advisory["advisory_id"])

    assert first_ack["delivered"] and not first_ack["already_delivered"]
    assert {:ok, fetched_after_ack} = Relay.origin_advisory_feed(relay, "origin", @handle, 0)
    assert hd(fetched_after_ack["entries"])["delivered"]
    assert {:ok, ordinary_after_ack} = Relay.origin_feed(relay, "origin", 0)
    assert ordinary_after_ack == ordinary_before

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path, clock: clock_fn)

    assert {:ok, replayed} = Relay.origin_advisory_feed(restarted, "origin", @handle, 0)
    assert replayed["cursor"] == 1
    assert hd(replayed["entries"])["advisory_id"] == advisory["advisory_id"]
    assert hd(replayed["entries"])["delivered"]
    assert {:ok, ordinary_after_restart} = Relay.origin_feed(restarted, "origin", 0)
    assert ordinary_after_restart == ordinary_before

    assert {:ok, duplicate_ack} =
             Relay.origin_advisory_ack(restarted, "origin", @handle, advisory["advisory_id"])

    assert duplicate_ack["delivered"] and duplicate_ack["already_delivered"]
    GenServer.stop(restarted)
  end

  test "authorized agent answer keeps its author through terminal replay and restart", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path, agent_fixture_mode: true)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    agent_key = String.duplicate("c", 64)
    assert :ok = Relay.setup_agent_fixture(relay, "A", op(701), agent_key, ["app:test"])

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(702),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "agent-terminal",
                 "created_at" => 1,
                 "title" => "Approve?"
               }
             })

    assert {:ok, %{"seq" => 2}} =
             Relay.fixture_agent_answer(relay, agent_key, %{
               "type" => "answer",
               "op_id" => op(703),
               "sender_id" => "app:test",
               "message_id" => "agent-terminal",
               "acceptance_seq" => 1,
               "deliberate" => true,
               "intent_ref" => "agent intent",
               "answer" => nil
             })

    assert {:ok, %{"entries" => [terminal]}} = Relay.origin_feed(relay, "origin", 1)
    assert terminal["author"] == "agent"
    assert terminal["terminal_state"] == "answered"
    assert terminal["has_answer"] == true and Map.has_key?(terminal, "answer")
    assert {:ok, [lifecycle], 2} = Relay.events(relay, "reader", 1)
    assert lifecycle["author"] == "agent"
    assert lifecycle["state"] == "answered"
    GenServer.stop(relay)

    assert {:ok, restarted} = Relay.start_link(store_path: path, agent_fixture_mode: true)

    assert {:ok, %{"entries" => [%{"author" => "agent"}]}} =
             Relay.origin_feed(restarted, "origin", 1)

    assert {:ok, [%{"author" => "agent", "state" => "answered"}], 2} =
             Relay.events(restarted, "reader", 1)

    GenServer.stop(restarted)
  end

  test "legacy replay stays contiguous while a capable device replays and acknowledges separately",
       %{
         path: path
       } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "old")
    assert :ok = Relay.setup_device(relay, "A", "new")
    assert :ok = Relay.setup_device(relay, "B", "other")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(601),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "split",
                 "created_at" => 1,
                 "title" => "Question"
               }
             })

    assert %{"type" => "ok", "seq" => 1, "event_id" => "2"} =
             request(relay, "new", %{
               "type" => "discussion",
               "op_id" => op(602),
               "sender_id" => "app:test",
               "message_id" => "split",
               "request_revision" => 1,
               "text" => "More detail?"
             })

    assert {:ok, [ordinary], 1} = Relay.events(relay, "old", 0)
    assert ordinary["type"] == "delivery"
    assert {:ok, [conversation], 1} = Relay.conversation_events(relay, "new", 0)
    assert conversation["conversation_seq"] == 1
    assert conversation["event_id"] == "2"
    assert {:ok, [], 0} = Relay.conversation_events(relay, "other", 0)

    assert {:ok, old_session} = Relay.open_session(relay, "old")

    assert [
             %{"type" => "ok"},
             %{"type" => "delivery", "seq" => 1},
             %{"type" => "resynced", "seq" => 1}
           ] =
             session_request(relay, old_session, %{"type" => "resync", "since" => 0})

    assert [%{"type" => "ok", "seq" => 1}] =
             session_request(relay, old_session, %{"type" => "ack", "seq" => 1})

    assert [%{"type" => "error", "reason" => "conversation_open_required"}] =
             session_request(relay, old_session, %{
               "type" => "conversation_ack",
               "conversation_seq" => 1
             })

    assert {:ok, new_session} = Relay.open_session(relay, "new")

    assert [
             %{"type" => "ok"},
             %{"type" => "delivery", "seq" => 1},
             %{"type" => "resynced", "seq" => 1}
           ] =
             session_request(relay, new_session, %{"type" => "resync", "since" => 0})

    assert [%{"type" => "ok"}, %{"type" => "conversation_resynced", "conversation_seq" => 1}] =
             session_request(relay, new_session, %{"type" => "conversation_open", "since" => 1})

    assert [%{"type" => "error", "reason" => "invalid_cursor"}] =
             session_request(relay, new_session, %{
               "type" => "conversation_ack",
               "conversation_seq" => 1
             })

    assert [
             %{"type" => "ok"},
             %{"type" => "conversation", "conversation_seq" => 1},
             %{"type" => "conversation_resynced", "conversation_seq" => 1}
           ] =
             session_request(relay, new_session, %{"type" => "conversation_open", "since" => 0})

    assert [%{"type" => "ok", "conversation_seq" => 1}] =
             session_request(relay, new_session, %{
               "type" => "conversation_ack",
               "conversation_seq" => 1
             })

    assert {:ok, 1} = Relay.conversation_cursor(relay, "new")
    assert {:ok, 0} = Relay.acknowledged_cursor(relay, "new")
    assert {:ok, 1} = Relay.acknowledged_cursor(relay, "old")

    assert %{"type" => "ok", "seq" => 2} =
             request(relay, "old", %{
               "type" => "answer",
               "op_id" => op(603),
               "sender_id" => "app:test",
               "message_id" => "split",
               "answer" => nil
             })

    assert {:ok, [terminal_line]} = Relay.session_drain(relay, old_session)
    assert {:ok, %{"type" => "lifecycle", "seq" => 2}} = JSON.decode_line(terminal_line)

    assert {:ok, [new_terminal], 2} = Relay.events(relay, "new", 1)
    assert new_terminal["type"] == "lifecycle"
    assert new_terminal["event_id"] == "3"
    assert new_terminal["author"] == "person"
    assert new_terminal["by"] == "answer"
    assert {:ok, [], 1} = Relay.conversation_events(relay, "new", 1)

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, 1} = Relay.conversation_cursor(restarted, "new")
    assert {:ok, [replayed], 1} = Relay.conversation_events(restarted, "new", 0)
    assert replayed["event_id"] == "2"
    assert {:ok, [_, _], 2} = Relay.events(restarted, "old", 0)
    GenServer.stop(restarted)
  end

  test "conversation wake reaches only opened devices and does not suppress a later new alert", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path, push_enabled: true)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "old")
    assert :ok = Relay.setup_device(relay, "A", "new")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(611),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "wake-one",
                 "created_at" => 1,
                 "title" => "First question"
               }
             })

    for device <- ["old", "new"] do
      assert {:ok, session} = Relay.open_session(relay, device)

      assert [%{"type" => "ok"}, %{"type" => "delivery"}, %{"type" => "resynced"}] =
               session_request(relay, session, %{"type" => "resync", "since" => 0})

      assert [%{"type" => "ok", "seq" => 1}] =
               session_request(relay, session, %{"type" => "ack", "seq" => 1})

      if device == "new" do
        assert [%{"type" => "ok"}, %{"type" => "conversation_resynced"}] =
                 session_request(relay, session, %{"type" => "conversation_open", "since" => 0})
      end

      assert %{"type" => "ok"} =
               request(relay, device, %{
                 "type" => "push_register",
                 "op_id" => if(device == "old", do: op(612), else: op(613)),
                 "revision" => 1,
                 "profile_id" => "harklight-sandbox",
                 "address_kind" => "apns_token",
                 "address" =>
                   if(device == "old",
                     do: String.duplicate("ab", 32),
                     else: String.duplicate("cd", 32)
                   )
               })
    end

    assert %{"type" => "ok"} =
             request(relay, "new", %{
               "type" => "discussion",
               "op_id" => op(614),
               "sender_id" => "app:test",
               "message_id" => "wake-one",
               "request_revision" => 1,
               "text" => "Can you clarify?"
             })

    bindings = :sys.get_state(relay).store.engine.push_bindings
    assert bindings[{"A", "old"}].pending_conversation_high == 0
    assert bindings[{"A", "new"}].pending_conversation_high == 1

    now_ms = System.system_time(:millisecond) + 300_001
    assert {:ok, claim} = Relay.claim_push(relay, now_ms)
    assert claim.device == "new"
    assert claim.conversation_high == 1
    assert claim.presentation == :background
    assert :ok = Relay.finish_push(relay, claim, %{state: "provider_accepted"}, now_ms + 1)

    assert %{"type" => "ok", "seq" => 2} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(615),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "wake-two",
                 "created_at" => 2,
                 "title" => "New unresolved question"
               }
             })

    # Both devices open a window on the new request; each gets the alert,
    # including the one whose silent conversation wake just went out.
    assert {:ok, first_alert} = Relay.claim_push(relay, now_ms + 300_001)
    assert first_alert.high == 2
    assert first_alert.presentation == :alert

    assert :ok =
             Relay.finish_push(
               relay,
               first_alert,
               %{state: "provider_accepted"},
               now_ms + 300_002
             )

    assert {:ok, second_alert} = Relay.claim_push(relay, now_ms + 300_003)
    assert second_alert.high == 2
    assert second_alert.presentation == :alert
    assert Enum.sort([first_alert.device, second_alert.device]) == ["new", "old"]
    assert {:ok, nil} = Relay.claim_push(relay, now_ms + 300_004)
    GenServer.stop(relay)
  end

  test "inline file bytes replay exactly and an over-limit turn changes no request state", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    bytes = :binary.copy(<<0, 255, 42, 10>>, 65_536)
    file = %{"name" => "exact.bin", "data_base64" => Base.encode64(bytes)}

    message = %{
      "sender_id" => "app:test",
      "message_id" => "file-request",
      "created_at" => 1,
      "title" => "Please inspect this file"
    }

    send = %{
      "type" => "forward",
      "op_id" => op(501),
      "session_handle" => @handle,
      "message" => message,
      "files" => [file]
    }

    assert %{"type" => "error", "reason" => "invalid_field", "field" => "files"} =
             request(
               relay,
               "origin",
               Map.drop(send, ["session_handle"]) |> Map.put("op_id", op(500))
             )

    assert %{"type" => "ok", "seq" => 1} = request(relay, "origin", send)
    assert {:ok, [origin_delivery], 1} = Relay.events(relay, "origin", 0)
    refute Map.has_key?(origin_delivery, "files")

    assert {:ok, [recipient_delivery], 1} = Relay.events(relay, "reader", 0)
    assert [stored] = recipient_delivery["files"]
    assert Base.decode64!(stored["data_base64"]) == bytes
    assert stored["size"] == byte_size(bytes)
    assert stored["sha256"] == Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

    oversized = %{"name" => "oversized.bin", "data_base64" => Base.encode64(<<0>> <> bytes)}

    assert %{"type" => "error", "reason" => "invalid_field", "field" => "files"} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(502),
               "sender_id" => "app:test",
               "message_id" => "file-request",
               "request_revision" => 1,
               "text" => "Please clarify",
               "files" => [oversized]
             })

    assert {:ok, %{"cursor" => 1, "entries" => [_]}} =
             Relay.origin_feed(relay, "origin", 0)

    revised_message = Map.put(message, "title", "Updated question")

    assert %{"type" => "error", "reason" => "invalid_field", "field" => "files"} =
             request(relay, "origin", %{
               "type" => "conversation_revise",
               "op_id" => op(503),
               "sender_id" => "app:test",
               "message_id" => "file-request",
               "base_revision" => 1,
               "message" => revised_message,
               "files" => [oversized]
             })

    assert {:ok, [unchanged], 1} = Relay.events(relay, "reader", 0)
    assert unchanged["message"] == message
    assert unchanged["files"] == recipient_delivery["files"]

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, [replayed], 1} = Relay.events(restarted, "reader", 0)
    assert replayed["files"] == recipient_delivery["files"]

    assert %{"type" => "ok", "seq" => 1, "duplicate" => true} =
             request(restarted, "origin", send)

    GenServer.stop(restarted)
  end

  test "file transfer and agent use stay separate across lost reply and restart", %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    message = %{
      "sender_id" => "app:test",
      "message_id" => "file-turn",
      "created_at" => 1,
      "title" => "Need a file"
    }

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(511),
               "session_handle" => @handle,
               "message" => message
             })

    file = %{"name" => "image.bin", "data_base64" => Base.encode64(<<0, 1, 255>>)}

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(512),
               "sender_id" => "app:test",
               "message_id" => "file-turn",
               "request_revision" => 1,
               "text" => "Inspect the attachment",
               "files" => [file]
             })

    assert {:ok, %{"entries" => [_, pending]}} = Relay.origin_feed(relay, "origin", 0)
    assert pending["delivered"] == false
    assert pending["file_transfers"] == %{}
    assert pending["file_use"] == %{}
    assert Base.decode64!(hd(pending["files"])["data_base64"]) == <<0, 1, 255>>

    assert {:error, :file_transfers} = Relay.origin_ack(relay, "origin", 2)

    transfers = [%{"name" => "image.bin", "status" => "not_transferred", "reason" => "disk full"}]

    assert {:ok, %{"status" => "delivered", "file_transfers" => recorded}} =
             Relay.origin_ack(relay, "origin", 2, transfers)

    assert recorded["image.bin"] == %{"status" => "not_transferred", "reason" => "disk full"}

    assert {:ok, %{"status" => "already_delivered"}} =
             Relay.origin_ack(relay, "origin", 2, transfers)

    assert {:error, :transfer_conflict} =
             Relay.origin_ack(relay, "origin", 2, [
               %{"name" => "image.bin", "status" => "transferred"}
             ])

    assert {:ok, %{"entries" => [_, delivered]}} = Relay.origin_feed(relay, "origin", 0)
    assert delivered["delivered"] == true
    assert delivered["file_use"] == %{}

    outcome = %{"status" => "unusable", "reason" => "not transferred"}

    assert {:ok, %{"status" => "unusable"}} =
             Relay.origin_file_use(relay, "origin", 2, "image.bin", outcome, op(513))

    assert {:ok, %{"duplicate" => true}} =
             Relay.origin_file_use(relay, "origin", 2, "image.bin", outcome, op(513))

    assert {:error, :file_use_conflict} =
             Relay.origin_file_use(
               relay,
               "origin",
               2,
               "image.bin",
               %{"status" => "used"},
               op(514)
             )

    assert {:ok, [discussion, delivery_ack, receipt, transfer, use], 5} =
             Relay.conversation_events(relay, "reader", 0)

    assert delivery_ack["kind"] == "delivery_ack"
    assert delivery_ack["stage"] == "ack1"
    assert delivery_ack["event_id"] == discussion["event_id"]

    assert Enum.map([discussion, receipt, transfer, use], & &1["kind"]) ==
             ~w(discussion receipt file_transfer file_use)

    assert Enum.map([receipt, transfer, use], & &1["event_id"]) == ["2", "2", "2"]
    assert transfer["file"] == "image.bin"
    assert transfer["status"] == "not_transferred"
    assert use["status"] == "unusable"

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, %{"entries" => [_, replayed]}} = Relay.origin_feed(restarted, "origin", 0)
    assert replayed["file_transfers"] == recorded
    assert replayed["file_use"] == %{"image.bin" => outcome}
    GenServer.stop(restarted)
  end

  test "format-17 requests, grants and receipts survive conversation-feed initialization once", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    message = %{
      "sender_id" => "app:test",
      "message_id" => "legacy-request",
      "created_at" => 1,
      "title" => "Still available after upgrade?"
    }

    forward = %{"type" => "forward", "op_id" => op(90), "message" => message}
    assert %{"type" => "ok", "seq" => 1} = request(relay, "origin", forward)
    GenServer.stop(relay)

    assert {:ok, before} = Store.open(path)
    prior_version = before.version
    prior = before.engine
    assert :ok = Store.close(before)

    legacy =
      Map.drop(prior, [:origin_feeds, :origin_sessions, :origin_receipts, :origin_actions])

    payload = :erlang.term_to_binary({:attention_relay_state, 17, legacy}, [{:compressed, 6}])
    assert {:ok, db} = Sqlite3.open(path)
    assert {:ok, stmt} = Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ?")
    assert :ok = Sqlite3.bind(stmt, [{:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}])
    assert :done = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)

    assert {:ok, migrated} = Store.open(path)
    assert migrated.version == prior_version + 1
    assert migrated.engine.persons == prior.persons
    assert migrated.engine.devices == prior.devices
    assert migrated.engine.sender_grants == prior.sender_grants
    assert migrated.engine.messages == prior.messages
    assert migrated.engine.events == prior.events
    assert migrated.engine.receipts == prior.receipts
    assert migrated.engine.origin_feeds == %{}
    assert migrated.engine.origin_sessions == %{}
    assert migrated.engine.origin_receipts == %{}
    assert migrated.engine.origin_actions == %{}
    assert :ok = Store.close(migrated)

    assert {:ok, restarted} = Relay.start_link(store_path: path)

    assert %{"type" => "ok", "seq" => 1, "duplicate" => true} =
             request(restarted, "origin", forward)

    assert {:ok, %{"cursor" => 0, "entries" => []}} = Relay.origin_feed(restarted, "origin", 0)
    GenServer.stop(restarted)

    assert {:ok, again} = Store.open(path)
    assert again.version == prior_version + 1
    assert :ok = Store.close(again)
  end

  test "a sent request and one exact null answer replay to only its origin after restart", %{
    path: path
  } do
    vendor_session_id = String.duplicate("session-", 40)
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_device(relay, "B", "other")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    message = %{
      "sender_id" => "app:test",
      "message_id" => "request-1",
      "created_at" => 1,
      "title" => "private title"
    }

    assert %{"type" => "ok", "seq" => 1, "session_handle" => @handle, "revision" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(1),
               "session_handle" => @handle,
               "vendor_session_id" => vendor_session_id,
               "route" => "claude-code",
               "cli_version" => "test-only",
               "unsupported_reason" => nil,
               "message" => message
             })

    assert %{
             "type" => "ok",
             "seq" => 1,
             "session_handle" => @handle,
             "revision" => 1,
             "duplicate" => true
           } =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(1),
               "session_handle" => @handle,
               "vendor_session_id" => vendor_session_id,
               "route" => "claude-code",
               "cli_version" => "test-only",
               "unsupported_reason" => nil,
               "message" => message
             })

    assert {:ok,
            %{
              "cursor" => 1,
              "entries" => [%{"kind" => "revision", "author" => "origin", "op_id" => first_op}]
            }} =
             Relay.origin_feed(relay, "origin", 0)

    assert first_op == op(1)

    assert %{"type" => "error", "reason" => "request_conflict"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(2),
               "session_handle" => "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
               "message" => message
             })

    assert %{"type" => "ok", "seq" => 2} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(3),
               "sender_id" => "app:test",
               "message_id" => "request-1",
               "answer" => nil
             })

    assert {:ok, %{"cursor" => 2, "entries" => [terminal]}} =
             Relay.origin_feed(relay, "origin", 1)

    assert terminal["kind"] == "terminal"
    assert terminal["author"] == "person"
    assert terminal["terminal_state"] == "answered"
    assert terminal["has_answer"] == true
    assert Map.has_key?(terminal, "answer") and is_nil(terminal["answer"])
    assert {:ok, %{"cursor" => 0, "entries" => []}} = Relay.origin_feed(relay, "other", 0)
    assert {:error, :not_found} = Relay.origin_ack(relay, "other", 2)

    assert {:ok,
            %{
              "availability" => "not_listening",
              "open_requests" => 0,
              "pending_undelivered" => 1,
              "pending_delivered_unacted" => 0
            }} = Relay.origin_status(relay, "origin", @handle)

    assert {:error, :not_found} = Relay.origin_status(relay, "other", @handle)

    assert {:ok, %{"status" => "delivered", "acted" => false}} =
             Relay.origin_ack(relay, "origin", 2)

    assert {:ok, [], 2} = Relay.events(relay, "reader", 2)

    assert {:ok, [delivery_ack, receipt_change], 2} =
             Relay.conversation_events(relay, "reader", 0)

    assert delivery_ack["kind"] == "delivery_ack"
    assert delivery_ack["stage"] == "ack1"
    assert delivery_ack["event_id"] == terminal["event_id"]
    assert receipt_change["type"] == "conversation"
    assert receipt_change["kind"] == "receipt"
    assert receipt_change["event_id"] == terminal["event_id"]
    assert receipt_change["request_revision"] == 1
    assert receipt_change["delivered"] == true
    refute Map.has_key?(receipt_change, "acted")
    refute Map.has_key?(receipt_change, "answer")

    assert {:ok, [^delivery_ack, origin_receipt], 2} =
             Relay.conversation_events(relay, "origin", 0)

    assert origin_receipt["delivered"] == true
    refute Map.has_key?(origin_receipt, "acted")
    refute Map.has_key?(origin_receipt, "answer")

    assert {:ok, %{"status" => "already_delivered", "acted" => false}} =
             Relay.origin_ack(relay, "origin", 2)

    assert {:ok, [], 2} = Relay.conversation_events(relay, "reader", 2)

    assert {:ok,
            %{
              "pending_undelivered" => 0,
              "pending_delivered_unacted" => 1
            }} = Relay.origin_status(relay, "origin", @handle)

    GenServer.stop(relay)
    assert {:ok, stored} = Store.open(path)

    assert stored.engine.origin_sessions[{"A", "origin", @handle}].vendor_session_id ==
             vendor_session_id

    assert :ok = Store.close(stored)
    assert {:ok, restarted} = Relay.start_link(store_path: path)

    assert {:ok, %{"cursor" => 2, "entries" => [replayed]}} =
             Relay.origin_feed(restarted, "origin", 1)

    assert Map.drop(replayed, ~w(delivered acted)) == Map.drop(terminal, ~w(delivered acted))
    assert replayed["delivered"] == true
    assert replayed["acted"] == false

    assert {:ok, %{"status" => "already_delivered", "acted" => false}} =
             Relay.origin_ack(restarted, "origin", 2)

    result = %{
      "type" => "conversation_result",
      "op_id" => op(4),
      "sender_id" => "app:test",
      "message_id" => "request-1",
      "in_reply_to" => 2,
      "status" => "received"
    }

    assert %{"type" => "ok", "status" => "received", "result_revision" => 1} =
             request(restarted, "origin", result)

    assert %{"type" => "ok", "status" => "received", "duplicate" => true} =
             request(restarted, "origin", result)

    assert {:ok,
            %{
              "entries" => [%{"acted" => true}, %{"kind" => "result", "op_id" => result_op}]
            }} =
             Relay.origin_feed(restarted, "origin", 1)

    assert result_op == op(4)

    assert {:ok, %{"entries" => [_, %{"in_reply_to" => "2"}]}} =
             Relay.origin_feed(restarted, "origin", 1)

    assert {:ok, %{"source_status" => "source_reported", "status" => "received"}} =
             Engine.source_status(
               :sys.get_state(restarted).store.engine,
               "A",
               "app:test",
               "request-1"
             )

    assert %{"type" => "ok", "status" => "applied", "result_revision" => 2} =
             request(restarted, "origin", %{result | "op_id" => op(5), "status" => "applied"})

    assert %{"type" => "error", "reason" => "receipt_conflict"} =
             request(restarted, "origin", %{result | "op_id" => op(6), "status" => "refused"})

    assert {:ok, %{"cursor" => 4, "entries" => [%{"kind" => "result"}, %{"kind" => "result"}]}} =
             Relay.origin_feed(restarted, "origin", 2)

    assert {:ok,
            %{
              "pending_undelivered" => 0,
              "pending_delivered_unacted" => 0
            }} = Relay.origin_status(restarted, "origin", @handle)

    assert :ok = Relay.origin_session_gone(restarted, "origin", @handle)

    assert {:ok, %{"availability" => "gone"}} =
             Relay.origin_status(restarted, "origin", @handle)

    assert {:ok, %{"listening_support" => %{"route" => "claude-code"}}} =
             Relay.origin_status(restarted, "origin", @handle)

    GenServer.stop(restarted)
    assert {:ok, again} = Relay.start_link(store_path: path)

    assert {:ok, %{"availability" => "gone", "pending_delivered_unacted" => 0}} =
             Relay.origin_status(again, "origin", @handle)

    GenServer.stop(again)
  end

  test "person discussion reaches the original feed, reply is one acted turn, and relay hides person text from origin",
       %{
         path: path
       } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    message = %{
      "sender_id" => "app:test",
      "message_id" => "r1",
      "created_at" => 1,
      "title" => "May I proceed?"
    }

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(11),
               "session_handle" => @handle,
               "message" => message
             })

    assert %{"type" => "ok", "seq" => 1, "event_id" => "2"} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(12),
               "sender_id" => "app:test",
               "message_id" => "r1",
               "request_revision" => 1,
               "text" => "Please explain the rollout plan"
             })

    assert {:ok, [origin_change], 1} = Relay.conversation_events(relay, "origin", 0)
    refute String.contains?(JSON.encode!(origin_change), "rollout plan")
    assert {:ok, [recipient_change], 1} = Relay.conversation_events(relay, "reader", 0)
    assert recipient_change["text"] == "Please explain the rollout plan"
    assert recipient_change["event_id"] == "2"

    assert %{"type" => "error", "reason" => "not_origin"} =
             request(relay, "reader", %{
               "type" => "conversation_reply",
               "op_id" => op(13),
               "sender_id" => "app:test",
               "message_id" => "r1",
               "request_revision" => 1,
               "in_reply_to" => 2,
               "text" => "A forged response"
             })

    reply = %{
      "type" => "conversation_reply",
      "op_id" => op(14),
      "sender_id" => "app:test",
      "message_id" => "r1",
      "request_revision" => 1,
      "in_reply_to" => 2,
      "text" => "First the canary, then the full rollout"
    }

    assert %{"type" => "ok", "seq" => 2, "event_id" => "3"} =
             request(relay, "origin", reply)

    assert %{"type" => "ok", "seq" => 2, "duplicate" => true} =
             request(relay, "origin", reply)

    assert %{"type" => "error", "reason" => "already_acted"} =
             request(relay, "origin", %{reply | "op_id" => op(15)})

    assert {:ok, [discussion_fact, reply_fact, acted_fact], 3} =
             Relay.conversation_events(relay, "reader", 0)

    assert discussion_fact["event_id"] == "2"
    assert reply_fact["event_id"] == "3"
    assert acted_fact["kind"] == "acted"
    assert acted_fact["event_id"] == "2"
    assert acted_fact["acting_event_id"] == "3"

    assert {:ok, %{"cursor" => 3, "entries" => [discussion, own_reply]}} =
             Relay.origin_feed(relay, "origin", 1)

    assert discussion["kind"] == "discussion"
    assert discussion["acted"] == true
    assert discussion["delivered"] == false
    assert own_reply["kind"] == "reply"
    assert own_reply["author"] == "origin"
    assert own_reply["op_id"] == op(14)
    assert own_reply["in_reply_to"] == "2"

    assert {:ok, %{"status" => "delivered", "acted" => true}} =
             Relay.origin_ack(relay, "origin", 2)

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)

    assert {:ok, %{"entries" => [%{"acted" => true, "delivered" => true}, _]}} =
             Relay.origin_feed(restarted, "origin", 1)

    GenServer.stop(restarted)
  end

  test "revision preserves earlier discussion, refuses stale final answer, and survives restart",
       %{
         path: path
       } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    first = %{
      "sender_id" => "app:test",
      "message_id" => "revised-request",
      "created_at" => 1,
      "title" => "Deploy now?",
      "choices" => ["Yes", "No"]
    }

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(21),
               "session_handle" => @handle,
               "message" => first
             })

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(22),
               "sender_id" => "app:test",
               "message_id" => "revised-request",
               "request_revision" => 1,
               "text" => "Which environment?"
             })

    updated = %{first | "title" => "Deploy to production now?"}

    revise = %{
      "type" => "conversation_revise",
      "op_id" => op(23),
      "sender_id" => "app:test",
      "message_id" => "revised-request",
      "base_revision" => 1,
      "in_reply_to" => 2,
      "message" => updated
    }

    assert %{"type" => "ok", "seq" => 2, "revision" => 2} =
             request(relay, "origin", revise)

    assert %{"type" => "ok", "seq" => 2, "revision" => 2, "duplicate" => true} =
             request(relay, "origin", revise)

    assert %{"type" => "error", "reason" => "stale_revision", "current_revision" => 2} =
             request(relay, "origin", %{revise | "op_id" => op(24)})

    assert %{"type" => "error", "reason" => "stale_revision", "current_revision" => 2} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(25),
               "sender_id" => "app:test",
               "message_id" => "revised-request",
               "request_revision" => 1,
               "answer" => "Yes"
             })

    assert %{"type" => "error", "reason" => "stale_revision", "current_revision" => 2} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(26),
               "sender_id" => "app:test",
               "message_id" => "revised-request",
               "answer" => "Yes"
             })

    assert %{"type" => "ok", "seq" => 4} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(27),
               "sender_id" => "app:test",
               "message_id" => "revised-request",
               "request_revision" => 1,
               "text" => "Still asking about the first wording"
             })

    assert {:ok, %{"entries" => [_, old_discussion, revision, newer_discussion]}} =
             Relay.origin_feed(relay, "origin", 0)

    assert old_discussion["request_revision"] == 1
    assert old_discussion["acted"] == true
    assert revision["request_revision"] == 2
    assert revision["message"] == updated
    assert revision["op_id"] == op(23)
    assert revision["in_reply_to"] == "2"
    assert newer_discussion["request_revision"] == 1

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)

    assert {:ok, [initial], 1} = Relay.events(restarted, "reader", 0)

    assert {:ok, [_, revised, acted, _, old_ack, new_ack], 6} =
             Relay.conversation_events(restarted, "reader", 0)

    assert Enum.map([old_ack, new_ack], & &1["event_id"]) == [
             old_discussion["event_id"],
             newer_discussion["event_id"]
           ]

    assert Enum.all?(
             [old_ack, new_ack],
             &(&1["kind"] == "delivery_ack" and &1["stage"] == "ack1")
           )

    assert initial["type"] == "delivery"
    assert initial["request_revision"] == 1
    assert initial["message"] == first
    assert revised["type"] == "conversation"
    assert revised["request_revision"] == 2
    assert revised["message"] == updated
    assert acted["kind"] == "acted"
    assert acted["event_id"] == "2"
    assert acted["acting_event_id"] == "3"

    assert %{"type" => "ok", "seq" => 2} =
             request(restarted, "reader", %{
               "type" => "answer",
               "op_id" => op(28),
               "sender_id" => "app:test",
               "message_id" => "revised-request",
               "request_revision" => 2,
               "answer" => "Yes"
             })

    assert %{"type" => "error", "reason" => "closed"} =
             request(restarted, "origin", %{revise | "op_id" => op(29), "base_revision" => 2})

    assert {:ok, %{"entries" => [terminal]}} = Relay.origin_feed(restarted, "origin", 4)
    assert terminal["terminal_state"] == "answered"
    assert terminal["request_revision"] == 2
    assert terminal["answer"] == "Yes"
    assert {:ok, [terminal_change], 2} = Relay.events(restarted, "reader", 1)
    assert terminal_change["type"] == "lifecycle"
    assert terminal_change["event_id"] == terminal["event_id"]
    assert terminal_change["request_revision"] == 2
    GenServer.stop(restarted)
  end

  test "No comment refuses missing and stale revisions, then accepts the current revision", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    first = %{
      "sender_id" => "app:test",
      "message_id" => "no-comment-revision",
      "created_at" => 1,
      "title" => "Deploy to staging?",
      "choices" => ["Yes", "No"]
    }

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(31),
               "session_handle" => @handle,
               "message" => first
             })

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(32),
               "sender_id" => "app:test",
               "message_id" => "no-comment-revision",
               "request_revision" => 1,
               "text" => "Which environment?"
             })

    updated = %{first | "title" => "Deploy to production?"}

    assert %{"type" => "ok", "revision" => 2} =
             request(relay, "origin", %{
               "type" => "conversation_revise",
               "op_id" => op(33),
               "sender_id" => "app:test",
               "message_id" => "no-comment-revision",
               "base_revision" => 1,
               "in_reply_to" => 2,
               "message" => updated
             })

    assert %{"type" => "error", "reason" => "invalid_field"} =
             no_comment_request(relay, "reader", %{
               "type" => "answer_no_comment",
               "request_id" => "no-comment-revision"
             })

    assert %{"type" => "error", "reason" => "invalid_field"} =
             no_comment_request(relay, "reader", %{
               "type" => "answer_no_comment",
               "request_id" => "no-comment-revision",
               "request_revision" => 0
             })

    assert %{
             "type" => "error",
             "reason" => "stale_revision",
             "current_revision" => 2
           } =
             no_comment_request(relay, "reader", %{
               "type" => "answer_no_comment",
               "request_id" => "no-comment-revision",
               "request_revision" => 1
             })

    assert {:ok, %{"entries" => before_terminal}} = Relay.origin_feed(relay, "origin", 0)
    refute Enum.any?(before_terminal, &(&1["kind"] == "terminal"))

    assert %{"type" => "ok", "terminal_kind" => "no_comment"} =
             no_comment_request(relay, "reader", %{
               "type" => "answer_no_comment",
               "request_id" => "no-comment-revision",
               "request_revision" => 2
             })

    assert {:ok, %{"entries" => entries}} = Relay.origin_feed(relay, "origin", 0)
    terminal = List.last(entries)
    assert terminal["kind"] == "terminal"
    assert terminal["terminal_kind"] == "no_comment"
    assert terminal["request_revision"] == 2
    assert terminal["has_answer"] == false
    refute Map.has_key?(terminal, "answer")

    GenServer.stop(relay)
  end

  test "answered requests keep chat availability but refuse new chat from either side",
       %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    for {message_id, title, operation} <- [
          {"answered-chat", "Answer this?", 801},
          {"no-comment-chat", "Skip this answer?", 802}
        ] do
      assert %{"type" => "ok"} =
               request(relay, "origin", %{
                 "type" => "forward",
                 "op_id" => op(operation),
                 "session_handle" => @handle,
                 "message" => %{
                   "sender_id" => "app:test",
                   "message_id" => message_id,
                   "created_at" => operation,
                   "title" => title
                 }
               })
    end

    prior_discussions =
      for {message_id, operation, text} <- [
            {"answered-chat", 810, "Before the answer"},
            {"no-comment-chat", 811, "Before No comment"}
          ],
          into: %{} do
        frame = %{
          "type" => "discussion",
          "op_id" => op(operation),
          "sender_id" => "app:test",
          "message_id" => message_id,
          "request_revision" => 1,
          "text" => text
        }

        assert %{"type" => "ok"} = discussion = request(relay, "reader", frame)

        reply_frame = %{
          "v" => 1,
          "type" => "reply",
          "op_id" => op(operation + 1),
          "sender_id" => "app:test",
          "message_id" => message_id,
          "request_revision" => 1,
          "in_reply_to" => discussion["event_id"],
          "text" => "Already sent before answer",
          "ref" => "existing-reply-#{message_id}"
        }

        assert %{"type" => "ok"} = reply = OriginWire.command(relay, "origin", reply_frame)

        pending_frame = %{
          frame
          | "op_id" => op(operation + 2),
            "text" => "Pending before answer"
        }

        assert %{"type" => "ok"} = pending = request(relay, "reader", pending_frame)

        {message_id,
         %{
           frame: frame,
           event: discussion,
           reply_frame: reply_frame,
           reply: reply,
           pending: pending
         }}
      end

    assert :ok = Relay.origin_stream_attach(relay, "origin", @handle, self())

    answer =
      request(relay, "reader", %{
        "type" => "answer",
        "op_id" => op(803),
        "sender_id" => "app:test",
        "message_id" => "answered-chat",
        "request_revision" => 1,
        "answer" => "Yes"
      })

    assert answer["type"] == "ok"

    assert_receive {:origin_state_changed, answer_seq}
    assert is_integer(answer_seq)

    no_comment =
      no_comment_request(relay, "reader", %{
        "type" => "answer_no_comment",
        "request_id" => "no-comment-chat",
        "request_revision" => 1
      })

    assert no_comment["type"] == "ok"
    assert no_comment["terminal_kind"] == "no_comment"

    assert_receive {:origin_state_changed, no_comment_seq}
    assert no_comment_seq > answer_seq

    assert {:ok, status} = Relay.origin_status(relay, "origin", @handle)
    assert status["open_requests"] == 0
    assert status["open_chats"] == 2

    feed_before_refusals = Relay.origin_feed(relay, "origin", 0)

    assert %{
             "v" => 1,
             "type" => "error",
             "reason" => "closed",
             "ref" => "person-answer-ref"
           } =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(804),
               "ref" => "person-answer-ref",
               "sender_id" => "app:test",
               "message_id" => "answered-chat",
               "request_revision" => 1,
               "text" => "Can we discuss that answer?"
             })

    assert %{
             "v" => 1,
             "type" => "error",
             "reason" => "closed",
             "ref" => "agent-answer-ref"
           } =
             OriginWire.command(relay, "origin", %{
               "v" => 1,
               "type" => "reply",
               "op_id" => op(805),
               "ref" => "agent-answer-ref",
               "sender_id" => "app:test",
               "message_id" => "answered-chat",
               "request_revision" => 1,
               "in_reply_to" => prior_discussions["answered-chat"].pending["event_id"],
               "text" => "Yes, here is more detail."
             })

    assert %{
             "v" => 1,
             "type" => "error",
             "reason" => "closed",
             "ref" => "person-no-comment-ref"
           } =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(806),
               "ref" => "person-no-comment-ref",
               "sender_id" => "app:test",
               "message_id" => "no-comment-chat",
               "request_revision" => 1,
               "text" => "Can I explain why I chose no comment?"
             })

    assert %{
             "v" => 1,
             "type" => "error",
             "reason" => "closed",
             "ref" => "agent-no-comment-ref"
           } =
             OriginWire.command(relay, "origin", %{
               "v" => 1,
               "type" => "reply",
               "op_id" => op(807),
               "ref" => "agent-no-comment-ref",
               "sender_id" => "app:test",
               "message_id" => "no-comment-chat",
               "request_revision" => 1,
               "in_reply_to" => prior_discussions["no-comment-chat"].pending["event_id"],
               "text" => "Of course; what would you like to explain?"
             })

    assert %{
             "v" => 1,
             "type" => "error",
             "reason" => "invalid_field",
             "field" => "request_revision",
             "ref" => "stale-revision-ref"
           } =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(816),
               "ref" => "stale-revision-ref",
               "sender_id" => "app:test",
               "message_id" => "answered-chat",
               "request_revision" => 2,
               "text" => "This revision is invalid"
             })

    assert %{
             "v" => 1,
             "type" => "error",
             "reason" => "not_found",
             "ref" => "missing-discussion-ref"
           } =
             OriginWire.command(relay, "origin", %{
               "v" => 1,
               "type" => "reply",
               "op_id" => op(817),
               "ref" => "missing-discussion-ref",
               "sender_id" => "app:test",
               "message_id" => "no-comment-chat",
               "request_revision" => 1,
               "in_reply_to" => "9999",
               "text" => "This discussion does not exist"
             })

    assert Relay.origin_feed(relay, "origin", 0) == feed_before_refusals

    for {_message_id, %{reply_frame: frame, reply: reply}} <- prior_discussions do
      assert %{"type" => "ok", "duplicate" => true, "event_id" => event_id} =
               OriginWire.command(relay, "origin", frame)

      assert event_id == reply["event_id"]
    end

    assert Relay.origin_feed(relay, "origin", 0) == feed_before_refusals

    {:ok, %{"entries" => answered_feed}} = feed_before_refusals

    for {message_id, operation} <- [{"answered-chat", 830}, {"no-comment-chat", 831}] do
      terminal_entry =
        Enum.find(answered_feed, fn entry ->
          entry["kind"] == "terminal" and entry["message_id"] == message_id
        end)

      assert terminal_entry

      assert %{
               "v" => 1,
               "type" => "ok",
               "status" => "received",
               "ref" => ref
             } =
               OriginWire.command(relay, "origin", %{
                 "v" => 1,
                 "type" => "result",
                 "op_id" => op(operation),
                 "ref" => "result-#{message_id}",
                 "sender_id" => "app:test",
                 "message_id" => message_id,
                 "in_reply_to" => terminal_entry["event_id"],
                 "status" => "received"
               })

      assert ref == "result-#{message_id}"
    end

    assert {:ok, %{"entries" => entries}} = Relay.origin_feed(relay, "origin", 0)

    assert Enum.map(entries, & &1["channel"]) == [
             "reply",
             "reply",
             "chat",
             "chat",
             "chat",
             "chat",
             "chat",
             "chat",
             "reply",
             "reply",
             "reply",
             "reply"
           ]

    reply_entries = Enum.filter(entries, &(&1["channel"] == "reply"))

    assert Enum.map(reply_entries, & &1["kind"]) == [
             "revision",
             "revision",
             "terminal",
             "terminal",
             "result",
             "result"
           ]

    terminal_entries = Enum.filter(reply_entries, &(&1["kind"] == "terminal"))

    assert Enum.all?(terminal_entries, &(&1["terminal_state"] == "answered"))

    assert Enum.find(terminal_entries, &(&1["message_id"] == "answered-chat"))["answer"] ==
             "Yes"

    no_comment = Enum.find(terminal_entries, &(&1["message_id"] == "no-comment-chat"))
    assert no_comment["terminal_kind"] == "no_comment"
    refute Map.has_key?(no_comment, "answer")

    chat_entries = Enum.filter(entries, &(&1["channel"] == "chat"))

    refute Enum.any?(
             chat_entries,
             &(&1["kind"] == "terminal" and &1["terminal_state"] == "answered")
           )

    assert Enum.all?(chat_entries, &(&1["kind"] in ["discussion", "reply"]))

    assert Enum.map(Enum.filter(chat_entries, &(&1["kind"] == "discussion")), & &1["text"]) == [
             "Before the answer",
             "Pending before answer",
             "Before No comment",
             "Pending before answer"
           ]

    assert Enum.map(Enum.filter(chat_entries, &(&1["kind"] == "reply")), & &1["text"]) == [
             "Already sent before answer",
             "Already sent before answer"
           ]

    assert {:ok, conversation_events, _high} = Relay.conversation_events(relay, "reader", 0)
    refute Enum.any?(conversation_events, &Map.has_key?(&1, "answer"))

    for {_message_id, %{frame: frame, event: event}} <- prior_discussions do
      assert event["type"] == "ok"

      assert %{"type" => "ok", "duplicate" => true, "event_id" => event_id} =
               request(relay, "reader", frame)

      assert event_id == event["event_id"]

      assert Enum.any?(conversation_events, fn entry ->
               entry["event_id"] == event_id and entry["text"] == frame["text"]
             end)
    end

    for {_message_id, %{reply: reply, reply_frame: frame}} <- prior_discussions do
      assert Enum.any?(conversation_events, fn entry ->
               entry["event_id"] == reply["event_id"] and entry["text"] == frame["text"]
             end)
    end

    GenServer.stop(relay)
  end

  test "answer closes only its request while an unanswered sibling stays chat-ready", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    for {message_id, operation} <- [{"answered", 840}, {"sibling", 841}] do
      assert %{"type" => "ok"} =
               request(relay, "origin", %{
                 "type" => "forward",
                 "op_id" => op(operation),
                 "session_handle" => @handle,
                 "message" => %{
                   "sender_id" => "app:test",
                   "message_id" => message_id,
                   "created_at" => operation,
                   "title" => "Request #{message_id}"
                 }
               })
    end

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(842),
               "sender_id" => "app:test",
               "message_id" => "answered",
               "request_revision" => 1,
               "answer" => "Done"
             })

    sibling_discussion_frame = %{
      "type" => "discussion",
      "op_id" => op(843),
      "sender_id" => "app:test",
      "message_id" => "sibling",
      "request_revision" => 1,
      "text" => "This sibling is still open"
    }

    assert %{"type" => "ok"} =
             sibling_discussion =
             request(relay, "reader", sibling_discussion_frame)

    assert %{"type" => "ok", "ref" => "sibling-reply-ref"} =
             OriginWire.command(relay, "origin", %{
               "v" => 1,
               "type" => "reply",
               "op_id" => op(844),
               "ref" => "sibling-reply-ref",
               "sender_id" => "app:test",
               "message_id" => "sibling",
               "request_revision" => 1,
               "in_reply_to" => sibling_discussion["event_id"],
               "text" => "The sibling reply is still open"
             })

    assert %{
             "v" => 1,
             "type" => "error",
             "reason" => "closed",
             "ref" => "answered-ref"
           } =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(845),
               "ref" => "answered-ref",
               "sender_id" => "app:test",
               "message_id" => "answered",
               "request_revision" => 1,
               "text" => "This request is answered"
             })

    assert {:ok, status} = Relay.origin_status(relay, "origin", @handle)
    assert status["open_requests"] == 1
    assert status["open_chats"] == 2

    assert {:ok, history, _high} = Relay.conversation_events(relay, "reader", 0)
    assert Enum.any?(history, &(&1["text"] == "This sibling is still open"))
    assert Enum.any?(history, &(&1["text"] == "The sibling reply is still open"))

    GenServer.stop(relay)
  end

  test "origin withdrawal acts on a named turn and closes once without a second answer", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_device(relay, "A", "other_origin")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    original = %{
      "sender_id" => "app:test",
      "message_id" => "withdrawn",
      "created_at" => 1,
      "title" => "Proceed?"
    }

    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(31),
               "session_handle" => @handle,
               "message" => original
             })

    assert {:ok, [acceptance], 1} = Relay.events(relay, "reader", 0)
    assert acceptance["type"] == "delivery"
    assert acceptance["message"] == original
    refute Map.has_key?(acceptance, "state")
    refute Map.has_key?(acceptance, "retracted_at")
    refute Map.has_key?(acceptance, "terminal")

    discussion = %{
      "type" => "discussion",
      "op_id" => op(32),
      "sender_id" => "app:test",
      "message_id" => "withdrawn",
      "request_revision" => 1,
      "text" => "Is this still needed?"
    }

    assert %{"type" => "ok", "seq" => 1} = request(relay, "reader", discussion)

    withdraw = %{
      "type" => "conversation_withdraw",
      "op_id" => op(33),
      "sender_id" => "app:test",
      "message_id" => "withdrawn",
      "in_reply_to" => 2
    }

    assert %{"type" => "error", "reason" => "not_origin"} =
             request(relay, "other_origin", %{withdraw | "op_id" => op(34)})

    assert %{
             "type" => "ok",
             "seq" => 2,
             "state" => "retracted",
             "retracted_at" => retracted_at
           } = request(relay, "origin", withdraw)

    assert is_integer(retracted_at) and retracted_at > 0

    assert %{
             "type" => "ok",
             "seq" => 2,
             "state" => "retracted",
             "retracted_at" => ^retracted_at,
             "duplicate" => true
           } = request(relay, "origin", withdraw)

    assert %{
             "type" => "error",
             "reason" => "already_retracted",
             "terminal" => %{
               "state" => "retracted",
               "retracted_at" => ^retracted_at,
               "terminal_seq" => 2
             }
           } = request(relay, "origin", %{withdraw | "op_id" => op(39)})

    assert %{"type" => "error", "reason" => "not_origin"} =
             request(relay, "other_origin", %{withdraw | "op_id" => op(40)})

    assert %{"type" => "error", "reason" => "not_found"} =
             request(relay, "origin", %{withdraw | "op_id" => op(41), "message_id" => "missing"})

    assert %{"type" => "error", "reason" => "closed"} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(35),
               "sender_id" => "app:test",
               "message_id" => "withdrawn",
               "request_revision" => 1,
               "text" => "A late response"
             })

    assert %{"type" => "ok", "seq" => 1, "duplicate" => true} =
             request(relay, "reader", discussion)

    assert {:ok, [page_delivery, lifecycle], 2} = Relay.events(relay, "reader", 0)
    assert page_delivery["seq"] == acceptance["seq"]
    assert page_delivery["request_revision"] == acceptance["request_revision"]
    assert page_delivery["session_handle"] == acceptance["session_handle"]
    assert page_delivery["message"] == original
    assert page_delivery["state"] == "retracted"
    assert page_delivery["retracted_at"] == retracted_at
    assert lifecycle["seq"] == 2
    assert {:ok, [snapshot_lifecycle], 2} = Relay.events(relay, "reader", 1)
    assert snapshot_lifecycle == lifecycle

    assert {:ok, [origin_reference | _], 2} = Relay.events(relay, "origin", 0)
    assert origin_reference["type"] == "delivery"
    refute Map.has_key?(origin_reference, "message")
    refute Map.has_key?(origin_reference, "state")
    refute Map.has_key?(origin_reference, "retracted_at")

    assert {:ok, %{"entries" => [_, discussion, terminal]}} =
             Relay.origin_feed(relay, "origin", 0)

    assert discussion["acted"] == true
    assert terminal["kind"] == "terminal"
    assert terminal["channel"] == "reply"
    assert terminal["author"] == "origin"
    assert terminal["terminal_state"] == "retracted"
    assert terminal["retracted_at"] == retracted_at
    refute terminal["has_answer"]
    refute Map.has_key?(terminal, "answer")
    assert {:ok, [lifecycle], 2} = Relay.events(relay, "reader", 1)
    assert lifecycle["event_id"] == terminal["event_id"]
    assert lifecycle["author"] == "origin"
    assert lifecycle["state"] == "cleared"
    assert lifecycle["terminal_reason"] == "retracted"
    assert lifecycle["at"] == retracted_at
    assert lifecycle["retracted_at"] == retracted_at

    assert {:ok, [_, retraction, acted, delivery_ack], 4} =
             Relay.conversation_events(relay, "reader", 0)

    assert delivery_ack["kind"] == "delivery_ack"
    assert delivery_ack["event_id"] == discussion["event_id"]
    assert retraction["kind"] == "retraction"
    assert retraction["terminal_state"] == "retracted"
    assert retraction["retracted_at"] == retracted_at
    assert retraction["event_id"] == terminal["event_id"]
    assert retraction["request_revision"] == 1
    assert retraction["session_handle"] == @handle
    assert acted["kind"] == "acted"
    assert acted["acting_event_id"] == terminal["event_id"]

    assert %{"type" => "error", "reason" => "unknown_type"} =
             request(relay, "origin", %{
               "type" => "answer_receipt",
               "receipt_version" => 1,
               "sender_id" => "app:test",
               "message_id" => "withdrawn",
               "terminal_seq" => 2
             })

    queued_answer = %{
      "type" => "answer",
      "op_id" => op(36),
      "sender_id" => "app:test",
      "message_id" => "withdrawn",
      "request_revision" => 1,
      "answer" => "stale"
    }

    assert %{
             "type" => "error",
             "reason" => "retracted",
             "terminal" => %{
               "state" => "retracted",
               "retracted_at" => ^retracted_at,
               "terminal_seq" => 2
             }
           } = request(relay, "reader", queued_answer)

    assert %{
             "type" => "error",
             "reason" => "retracted",
             "terminal" => %{
               "state" => "retracted",
               "retracted_at" => ^retracted_at,
               "terminal_seq" => 2
             }
           } = request(relay, "reader", queued_answer)

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, [replayed], 2} = Relay.events(restarted, "reader", 1)
    assert replayed["state"] == "cleared"
    assert replayed["terminal_reason"] == "retracted"
    assert replayed["at"] == retracted_at
    assert replayed["retracted_at"] == retracted_at

    assert {:ok, [_, %{"kind" => "retraction", "event_id" => retraction_id}, _, ^delivery_ack], 4} =
             Relay.conversation_events(restarted, "reader", 0)

    assert retraction_id == terminal["event_id"]

    assert {:ok, %{"entries" => [_, _, persisted_terminal]}} =
             Relay.origin_feed(restarted, "origin", 0)

    assert persisted_terminal["terminal_state"] == "retracted"
    assert persisted_terminal["retracted_at"] == retracted_at

    assert %{
             "type" => "ok",
             "seq" => 2,
             "state" => "retracted",
             "retracted_at" => ^retracted_at,
             "duplicate" => true
           } = request(restarted, "origin", withdraw)

    assert %{"type" => "ok", "seq" => 3} =
             request(restarted, "origin", %{
               "type" => "forward",
               "op_id" => op(37),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "ordinary-clear",
                 "created_at" => 2,
                 "title" => "No longer needed"
               }
             })

    assert %{"type" => "ok", "seq" => 4} =
             request(restarted, "origin", %{
               "type" => "lifecycle",
               "op_id" => op(38),
               "sender_id" => "app:test",
               "message_id" => "ordinary-clear",
               "state" => "cleared",
               "at" => 3
             })

    assert {:ok, [ordinary_clear], 4} = Relay.events(restarted, "reader", 3)
    assert ordinary_clear["state"] == "cleared"
    refute Map.has_key?(ordinary_clear, "terminal_reason")
    refute Map.has_key?(ordinary_clear, "retracted_at")
    GenServer.stop(restarted)
  end

  test "a committed origin discussion reply remains nonterminal and retryable after retraction",
       %{
         path: path
       } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(50),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "discussion-reply",
                 "created_at" => 1,
                 "title" => "Question"
               }
             })

    discussion = %{
      "type" => "discussion",
      "op_id" => op(51),
      "sender_id" => "app:test",
      "message_id" => "discussion-reply",
      "request_revision" => 1,
      "text" => "Please clarify."
    }

    assert %{"type" => "ok", "seq" => 1} = request(relay, "reader", discussion)

    reply = %{
      "type" => "conversation_reply",
      "op_id" => op(52),
      "sender_id" => "app:test",
      "message_id" => "discussion-reply",
      "in_reply_to" => 2,
      "request_revision" => 1,
      "text" => "The usual route."
    }

    assert %{"type" => "ok", "seq" => 2} = request(relay, "origin", reply)

    assert %{"type" => "ok", "seq" => 2, "state" => "retracted"} =
             request(relay, "origin", %{
               "type" => "conversation_withdraw",
               "op_id" => op(53),
               "sender_id" => "app:test",
               "message_id" => "discussion-reply"
             })

    assert %{"type" => "ok", "seq" => 2, "duplicate" => true} =
             request(relay, "origin", reply)

    assert %{"type" => "error", "reason" => "closed"} =
             request(relay, "origin", %{reply | "op_id" => op(54), "text" => "A late reply"})

    assert {:ok, %{"entries" => entries}} = Relay.origin_feed(relay, "origin", 0)
    assert Enum.map(entries, & &1["kind"]) == ["revision", "discussion", "reply", "terminal"]
    assert List.last(entries)["terminal_state"] == "retracted"

    GenServer.stop(relay)
  end

  test "a committed terminal answer wins over a later origin retraction", %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(60),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "answer-wins",
                 "created_at" => 1,
                 "title" => "Question"
               }
             })

    assert %{"type" => "ok", "seq" => 2} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(61),
               "sender_id" => "app:test",
               "message_id" => "answer-wins",
               "request_revision" => 1,
               "answer" => "Done"
             })

    assert %{"type" => "error", "reason" => "closed"} =
             request(relay, "origin", %{
               "type" => "conversation_withdraw",
               "op_id" => op(62),
               "sender_id" => "app:test",
               "message_id" => "answer-wins"
             })

    assert {:ok, [lifecycle], 2} = Relay.events(relay, "reader", 1)
    assert lifecycle["state"] == "answered"

    assert {:ok, %{"entries" => [_, terminal]}} = Relay.origin_feed(relay, "origin", 0)
    assert terminal["terminal_state"] == "answered"
    assert terminal["answer"] == "Done"
    GenServer.stop(relay)
  end

  test "a person's open request replays current origin availability across stream loss and restart",
       %{
         path: path
       } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_device(relay, "B", "other")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(51),
               "session_handle" => @handle,
               "route" => "claude-code",
               "cli_version" => "test-only",
               "unsupported_reason" => nil,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "availability",
                 "created_at" => 1,
                 "title" => "Private question"
               }
             })

    assert {:ok, [initial], 1} = Relay.events(relay, "reader", 0)
    assert initial["availability"] == "not_listening"
    assert initial["session_handle"] == @handle
    assert initial["request_revision"] == 1
    assert initial["event_id"] == "1"
    assert initial["listening_support"]["route"] == "claude-code"
    assert initial["listening_support"]["origin"] == "origin"
    assert is_integer(initial["listening_support"]["reported_at"])

    assert :ok = Relay.origin_stream_attach(relay, "origin", @handle, self())
    assert {:ok, [listening], 1} = Relay.conversation_events(relay, "reader", 0)
    assert listening["type"] == "conversation"
    assert listening["kind"] == "availability"
    assert listening["availability"] == "listening"
    assert listening["session_handle"] == @handle
    refute Map.has_key?(listening, "message")
    assert {:ok, [origin_listening], 1} = Relay.conversation_events(relay, "origin", 0)
    assert origin_listening["availability"] == "listening"
    refute Map.has_key?(origin_listening, "message")

    assert :ok = Relay.origin_stream_detach(relay, "origin", @handle, self())
    assert {:ok, [detached], 2} = Relay.conversation_events(relay, "reader", 1)
    assert detached["availability"] == "not_listening"
    assert is_integer(detached["last_detached_at"])

    assert :ok = Relay.origin_session_gone(relay, "origin", @handle)
    assert :ok = Relay.origin_session_gone(relay, "origin", @handle)
    assert {:ok, [gone], 3} = Relay.conversation_events(relay, "reader", 2)
    assert gone["availability"] == "gone"
    assert gone["request_revision"] == 1
    assert {:ok, [], 0} = Relay.events(relay, "other", 0)

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, [initial], 1} = Relay.events(restarted, "reader", 0)
    assert {:ok, changes, 3} = Relay.conversation_events(restarted, "reader", 0)
    replayed = [initial | changes]

    assert Enum.map(replayed, & &1["availability"]) ==
             ~w(not_listening listening not_listening gone)

    assert {:ok, %{"availability" => "gone"}} =
             Relay.origin_status(restarted, "origin", @handle)

    GenServer.stop(restarted)
  end

  test "transient availability persists as an overlay and replays after process restart", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(81),
               "session_handle" => @handle,
               "route" => "other",
               "cli_version" => "test-only",
               "unsupported_reason" => nil,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "overlay-replay",
                 "created_at" => 1,
                 "title" => "Availability survives restart"
               }
             })

    {before_version, before_payload, before_checksum, 0} = state_snapshot(path)
    assert :crypto.hash(:sha256, before_payload) == before_checksum

    assert :ok = Relay.origin_stream_attach(relay, "origin", @handle, self())
    {after_version, after_payload, after_checksum, 1} = state_snapshot(path)

    assert after_version == before_version + 1
    assert after_payload == before_payload
    assert after_checksum != :crypto.hash(:sha256, after_payload)
    assert {:ok, [listening], 1} = Relay.conversation_events(relay, "reader", 0)
    assert listening["availability"] == "listening"

    assert :ok = Relay.setup_person(relay, "B")
    {folded_version, folded_payload, folded_checksum, 0} = state_snapshot(path)
    assert folded_version == after_version + 1
    assert folded_payload != before_payload
    assert :crypto.hash(:sha256, folded_payload) == folded_checksum

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)

    assert {:ok, [initial], 1} = Relay.events(restarted, "reader", 0)
    assert initial["availability"] == "not_listening"
    assert {:ok, [replayed], 1} = Relay.conversation_events(restarted, "reader", 0)
    assert replayed["availability"] == "listening"

    assert {:ok, %{"availability" => "not_listening"}} =
             Relay.origin_status(restarted, "origin", @handle)

    assert {:error, :person_exists} = Relay.setup_person(restarted, "B")

    {_version, restored_payload, restored_checksum, 0} = state_snapshot(path)
    assert :crypto.hash(:sha256, restored_payload) == restored_checksum

    GenServer.stop(restarted)
  end

  @tag :cold_overlay
  test "support writes persist their receipt and replay through the sparse overlay", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    forward = %{
      "type" => "forward",
      "op_id" => op(91),
      "session_handle" => @handle,
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => "support-overlay",
        "created_at" => 1,
        "title" => "Support survives restart"
      }
    }

    assert %{"type" => "ok", "seq" => 1} = request(relay, "origin", forward)

    support = %{
      "type" => "support",
      "op_id" => op(92),
      "session_handle" => @handle,
      "route" => "claude-code",
      "cli_version" => "test-only",
      "unsupported_reason" => nil
    }

    {before_version, before_payload, before_checksum, 0} = state_snapshot(path)
    assert :crypto.hash(:sha256, before_payload) == before_checksum

    assert {:error, :not_found} = Relay.origin_support(relay, "reader", support)

    assert state_snapshot(path) == {before_version, before_payload, before_checksum, 0}

    assert {:ok, %{"listening_support" => %{"route" => "claude-code"} = declared}} =
             Relay.origin_support(relay, "origin", support)

    assert is_integer(declared["reported_at"])
    {support_version, support_payload, support_checksum, 1} = state_snapshot(path)
    assert support_version == before_version + 1
    assert support_payload == before_payload
    assert support_checksum != :crypto.hash(:sha256, support_payload)

    assert :ok = Relay.origin_stream_attach(relay, "origin", @handle, self())
    {attach_version, attach_payload, _attach_checksum, 2} = state_snapshot(path)
    assert attach_version == support_version + 1
    assert attach_payload == before_payload

    GenServer.stop(relay)
    cold_overlay_reopen(path)
    assert {:ok, restarted} = Relay.start_link(store_path: path)

    assert {:ok, %{"listening_support" => ^declared}} =
             Relay.origin_status(restarted, "origin", @handle)

    assert {:ok, [supported, listening], 2} =
             Relay.conversation_events(restarted, "reader", 0)

    assert supported["kind"] == "availability"
    assert supported["listening_support"] == declared
    assert listening["availability"] == "listening"
    assert listening["listening_support"] == declared

    duplicate_state = state_snapshot(path)

    assert %{"type" => "ok", "seq" => 1, "duplicate" => true} =
             request(restarted, "origin", forward)

    assert state_snapshot(path) == duplicate_state

    assert {:ok, %{"duplicate" => true, "listening_support" => ^declared}} =
             Relay.origin_support(restarted, "origin", support)

    assert state_snapshot(path) == duplicate_state

    assert {:error, :operation_conflict} =
             Relay.origin_support(restarted, "origin", %{support | "route" => "codex"})

    assert :ok = Relay.setup_person(restarted, "B")
    {folded_version, folded_payload, folded_checksum, 0} = state_snapshot(path)
    assert folded_version > attach_version
    assert folded_payload != before_payload
    assert :crypto.hash(:sha256, folded_payload) == folded_checksum

    GenServer.stop(restarted)
    assert {:ok, folded_restart} = Relay.start_link(store_path: path)

    assert {:ok, %{"duplicate" => true, "listening_support" => ^declared}} =
             Relay.origin_support(folded_restart, "origin", support)

    assert {:ok, %{"listening_support" => ^declared}} =
             Relay.origin_status(folded_restart, "origin", @handle)

    GenServer.stop(folded_restart)
  end
end
