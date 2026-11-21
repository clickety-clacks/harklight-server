defmodule AttentionServer.ClientAPIAgentReadsTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    AgentChangeLog,
    ClientAPIFeedClient,
    ClientAPISchema,
    JSON,
    Relay,
    Server,
    TLSFixtures
  }

  alias AttentionServer.ClientAPI.Resources

  @first_session "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @other_session "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    defs = ClientAPISchema.contract_defs(Path.expand("../docs/client-api-v2.md", __DIR__))
    %{certs: certs, defs: defs}
  end

  setup %{certs: certs} = context do
    dir = Path.join(System.tmp_dir!(), "agent-reads-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    flags = Path.join(dir, "flags.json")
    File.write!(flags, JSON.encode!(%{"client_api_v2" => Map.get(context, :flag, true)}))

    server =
      start_supervised!(
        {Server,
         store_path: Path.join(dir, "relay.sqlite3"),
         feature_flags_path: flags,
         certfile: Path.join(certs, "server.pem"),
         keyfile: Path.join(certs, "server.key"),
         cacertfile: Path.join(certs, "ca.pem"),
         port: 0,
         enrollment_enabled: false,
         ip: {127, 0, 0, 1}}
      )

    relay = Server.relay(server)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")

    keys =
      Map.new([{"a1", "A"}, {"a2", "A"}, {"b1", "B"}], fn {name, person} ->
        assert {:ok, key} =
                 Relay.setup_device_certificate(
                   relay,
                   person,
                   TLSFixtures.certificate_der!(certs, name)
                 )

        {name, key}
      end)

    assert :ok = Relay.setup_sender(relay, keys["a1"], "app:test")
    assert :ok = Relay.setup_sender(relay, keys["a2"], "app:test")
    assert :ok = Relay.setup_sender(relay, keys["b1"], "app:test")

    %{server: server, relay: relay, keys: keys, port: Server.port(server)}
  end

  test "lists only the enrolled agent's messages, newest first, and filters by session", ctx do
    forward(ctx, "a1", 1, @first_session)
    forward(ctx, "a1", 2, @other_session)
    forward(ctx, "a2", 3, @first_session)
    forward(ctx, "b1", 4, @first_session)

    response = get(ctx, "a1", "/v2/agent/messages")
    assert response.status == 200

    assert %{"rev" => rev, "messages" => [newer, older]} =
             contract(ctx, "SentMessagesPage", response)

    assert rev > 0
    assert [newer["session"], older["session"]] == [@other_session, @first_session]

    scoped = get(ctx, "a1", "/v2/agent/messages?session=" <> @first_session)
    assert contract(ctx, "SentMessagesPage", scoped)["messages"] == [older]

    legacy =
      get(ctx, "a1", "/v2/agent/messages?sender=app%3Atest&legacy_message_id=item-1")

    assert older["legacy_message_id"] == "item-1"
    assert contract(ctx, "SentMessagesPage", legacy)["messages"] == [older]

    combined = "/v2/agent/messages?sender=app%3Atest&legacy_message_id=item-1&session="

    assert contract(ctx, "SentMessagesPage", get(ctx, "a1", combined <> @first_session))[
             "messages"
           ] == [older]

    assert contract(ctx, "SentMessagesPage", get(ctx, "a1", combined <> @other_session))[
             "messages"
           ] == []

    assert get(ctx, "a1", "/v2/agent/messages?session=invalid").status == 400
  end

  test "message pages bind the token to the agent and the filters", ctx do
    for n <- 1..51, do: forward(ctx, "a1", n, @first_session)
    filter = "/v2/agent/messages?session=" <> @first_session

    assert %{"messages" => messages, "next_page" => page} =
             contract(ctx, "SentMessagesPage", get(ctx, "a1", filter))

    assert Enum.map(messages, & &1["legacy_message_id"]) ==
             Enum.map(51..2//-1, &"item-#{&1}")

    next = filter <> "&page=" <> page
    last = contract(ctx, "SentMessagesPage", get(ctx, "a1", next))
    assert [%{"legacy_message_id" => "item-1"}] = last["messages"]
    refute Map.has_key?(last, "next_page")
    assert get(ctx, "a2", next).status == 400
    assert get(ctx, "a1", "/v2/agent/messages?page=" <> page).status == 400
  end

  @tag flag: false
  test "the server leaves the agent HTTP routes and durable log disabled with the flag off",
       ctx do
    forward(ctx, "a1", 1, @first_session)
    refute Server.client_api_v2?(ctx.server)
    refute Enum.any?(Supervisor.which_children(ctx.server), &(elem(&1, 0) == :client_api))
    state = :sys.get_state(ctx.relay)
    assert {:error, :disabled} = AgentChangeLog.head(state.store, {"A", ctx.keys["a1"]})
    assert state.store.engine.route_b_index.sent_messages_by_agent == %{}
    assert state.store.engine.route_b_index.agent_entry_pages == %{}
    refute Map.has_key?(state.priority_views["A"], :reply_entries_by_item)

    opted_in = AttentionServer.RouteBIndex.enable_agent_feed(state.store.engine)
    index = opted_in.route_b_index

    tree =
      AttentionServer.RouteBIndex.message_page(index, {"A", ctx.keys["a1"]}, %{
        "session" => @first_session
      })

    assert :gb_trees.size(tree) == 1
  end

  test "reads one message with an ETag and hides it from other principals", ctx do
    id = forward(ctx, "a1", 1, @first_session)

    path = "/v2/agent/messages/" <> id
    response = get(ctx, "a1", path)
    assert response.status == 200

    assert %{"message" => %{"item_id" => ^id, "session" => @first_session}} =
             contract(ctx, "SentMessageResponse", response)

    assert {_, tag} = List.keyfind(response.headers, "etag", 0)
    cached = get(ctx, "a1", path, [{"if-none-match", tag}])
    assert cached.status == 304
    assert cached.body == ""

    assert get(ctx, "a2", path).status == 404
    assert get(ctx, "b1", path).status == 404
  end

  test "pages conversation entries in creation order and hides them from other keys", ctx do
    id = forward(ctx, "a1", 1, @first_session)

    for {n, text} <- [{2, "First"}, {3, "Second"}] do
      frame(ctx, "a2", %{
        "type" => "discussion",
        "op_id" => op(n),
        "sender_id" => "app:test",
        "message_id" => "item-1",
        "request_revision" => 1,
        "text" => text
      })
    end

    path = "/v2/agent/messages/" <> id <> "/entries"
    response = get(ctx, "a1", path <> "?limit=1")
    assert response.status == 200
    assert %{"entries" => [latest], "older" => true} = contract(ctx, "AgentEntriesPage", response)
    assert latest["text"] == "Second"

    earlier = get(ctx, "a1", path <> "?before=" <> latest["entry_id"])

    assert %{"entries" => [first], "older" => false} = contract(ctx, "AgentEntriesPage", earlier)
    assert first["text"] == "First"
    assert first["position"] < latest["position"]
    assert get(ctx, "a2", path).status == 404
  end

  test "bounded entries retain an agent reply's cached priority", ctx do
    id = forward(ctx, "a1", 1, @first_session)

    turn =
      frame(ctx, "a2", %{
        "type" => "discussion",
        "op_id" => op(2),
        "sender_id" => "app:test",
        "message_id" => "item-1",
        "request_revision" => 1,
        "text" => "Question"
      })

    %{"seq" => position} =
      frame(ctx, "a1", %{
        "type" => "conversation_reply",
        "op_id" => op(3),
        "sender_id" => "app:test",
        "message_id" => "item-1",
        "request_revision" => 1,
        "in_reply_to" => String.to_integer(turn["event_id"]),
        "text" => "Reply"
      })

    view = :sys.get_state(ctx.relay).priority_views["A"]
    assert %{^position => %{"conversation_seq" => ^position}} =
             view.reply_entries_by_item[{"app:test", "item-1"}]

    reply = %{
      "sender_id" => "app:test",
      "message_id" => "item-1",
      "conversation_seq" => position,
      "priority" => "now",
      "score" => 0.9
    }

    :sys.replace_state(ctx.relay, fn state ->
      update_in(state.priority_views["A"], fn view ->
        view
        |> Map.put(:replies, [reply])
        |> Map.put(:reply_entries_by_item, %{{"app:test", "item-1"} => %{position => reply}})
      end)
    end)

    path = "/v2/agent/messages/" <> id <> "/entries?limit=1"
    response = get(ctx, "a1", path)
    assert response.status == 200

    body = Elixir.JSON.decode!(response.body)
    assert ClientAPISchema.errors(ctx.defs, "AgentEntriesPage", body) == []

    assert %{
             "entries" => [
               %{"position" => ^position, "priority" => %{"level" => "now", "score" => 0.9}}
             ],
             "older" => true
           } = body
  end

  test "me reports the enrolled key's sender names", ctx do
    response = get(ctx, "a1", "/v2/me")
    assert response.status == 200
    assert contract(ctx, "Me", response)["sender_names"] == ["app:test"]
  end

  test "reads the selected historical revision and hides it from other principals", ctx do
    id = forward(ctx, "a1", 1, @first_session)

    frame(ctx, "a1", %{
      "type" => "conversation_revise",
      "op_id" => op(2),
      "sender_id" => "app:test",
      "message_id" => "item-1",
      "base_revision" => 1,
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => "item-1",
        "title" => "Revised",
        "created_at" => 1
      }
    })

    path = "/v2/agent/messages/" <> id <> "/revisions/"
    original = get(ctx, "a1", path <> "1")
    assert original.status == 200

    assert %{"revision" => %{"request_revision" => 1, "message" => %{"title" => "Message 1"}}} =
             contract(ctx, "AgentRevisionResponse", original)

    assert %{"revision" => %{"request_revision" => 2, "message" => %{"title" => "Revised"}}} =
             contract(ctx, "AgentRevisionResponse", get(ctx, "a1", path <> "2"))

    assert get(ctx, "a1", path <> "3").status == 404
    assert get(ctx, "a2", path <> "1").status == 404
  end

  describe "agent message writes" do
    @describetag :agent_message_writes

    test "send creates one server-issued item and only successes consume a key", ctx do
      path = "/v2/agent/messages"

      body = %{
        "sender" => "app:test",
        "session" => @first_session,
        "message" => %{"title" => "Sent over v2", "created_at" => 1}
      }

      invalid = put_in(body["message"]["message_id"], "caller-id")
      assert request(ctx, "a1", "POST", path, invalid, 1).status == 400

      sent = request(ctx, "a1", "POST", path, body, 1)
      assert sent.status == 200
      assert %{"message" => message} = contract(ctx, "AgentWriteResult", sent)
      assert message["item_id"]
      refute Map.has_key?(message, "legacy_message_id")

      replay = request(ctx, "a1", "POST", path, body, 1)
      assert replay.status == 200

      assert List.keyfind(replay.headers, "idempotent-replayed", 0) ==
               {"idempotent-replayed", "true"}

      assert replay.body == sent.body

      changed = put_in(body["message"]["title"], "Different intent")
      assert_error(request(ctx, "a1", "POST", path, changed, 1), 409, "idempotency_conflict")

      other_key = request(ctx, "a2", "POST", path, body, 1)
      assert other_key.status == 200

      refute contract(ctx, "AgentWriteResult", other_key)["message"]["item_id"] ==
               message["item_id"]
    end

    test "revise uses If-Match, advances revision, and replays before checking it", ctx do
      id = forward(ctx, "a1", 1, @first_session)
      path = "/v2/agent/messages/" <> id
      body = %{"message" => %{"title" => "Updated", "created_at" => 1}}

      assert_error(request(ctx, "a1", "PUT", path, body, 2), 412, "precondition_failed")

      assert_error(
        request(ctx, "a1", "PUT", path, body, 2, [{"if-match", ~s("2")}]),
        412,
        "precondition_failed"
      )

      headers = [{"if-match", ~s("1")}]
      revised = request(ctx, "a1", "PUT", path, body, 2, headers)
      assert revised.status == 200

      assert %{"message" => %{"revision" => 2, "message" => %{"title" => "Updated"}}} =
               contract(ctx, "AgentWriteResult", revised)

      replay = request(ctx, "a1", "PUT", path, body, 2, headers)
      assert replay.status == 200
      assert replay.body == revised.body
      assert_error(request(ctx, "a2", "PUT", path, body, 3, headers), 404, "not_found")
    end

    test "correlated revise keeps, carries, and clears current attachments", ctx do
      sent =
        frame(ctx, "a1", %{
          "type" => "forward",
          "op_id" => op(1),
          "session_handle" => @first_session,
          "message" => %{
            "sender_id" => "app:test",
            "message_id" => "inline",
            "title" => "Original",
            "created_at" => 1
          },
          "files" => [%{"name" => "inline.txt", "data_base64" => Base.encode64("fixture")}]
        })

      id = Resources.item_id(%{accepted_seq: sent["seq"]})
      path = "/v2/agent/messages/" <> id
      entry_id = person_reply(ctx, id, 2)
      message = %{"title" => "Updated", "created_at" => 1}
      body = %{"message" => message, "in_reply_to" => entry_id}

      revised = request(ctx, "a1", "PUT", path, body, 3, [{"if-match", "1"}])
      assert revised.status == 200

      assert %{
               "message" => %{"files" => [%{"file_id" => file}]},
               "entry" => %{"entry_id" => ^entry_id, "delivery" => %{"acted" => true}}
             } = contract(ctx, "AgentWriteResult", revised)

      assert get(ctx, "a1", "/v2/files/" <> file).body == "fixture"
      body = %{"message" => %{"title" => "Carried", "created_at" => 1}, "file_ids" => [file]}
      carried = request(ctx, "a1", "PUT", path, body, 4, [{"if-match", "2"}])
      assert carried.status == 200

      assert %{"message" => %{"files" => [%{"name" => "inline.txt"}]}} =
               contract(ctx, "AgentWriteResult", carried)

      body = %{"message" => %{"title" => "Cleared", "created_at" => 1}, "file_ids" => []}
      cleared = request(ctx, "a1", "PUT", path, body, 5, [{"if-match", "3"}])
      assert %{"message" => %{"files" => []}} = contract(ctx, "AgentWriteResult", cleared)
    end

    test "a correlated revise refuses an already acted turn without changing the revision", ctx do
      id = forward(ctx, "a1", 1, @first_session)
      entry_id = person_reply(ctx, id, 2)
      path = "/v2/agent/messages/" <> id

      assert request(
               ctx,
               "a1",
               "POST",
               path <> "/replies",
               %{"text" => "Acted", "in_reply_to" => entry_id},
               3
             ).status == 200

      body = %{"message" => %{"title" => "Refused", "created_at" => 1}, "in_reply_to" => entry_id}

      assert_error(
        request(ctx, "a1", "PUT", path, body, 4, [{"if-match", "1"}]),
        409,
        "already_closed"
      )

      assert %{"message" => %{"revision" => 1, "message" => %{"title" => "Message 1"}}} =
               contract(ctx, "SentMessageResponse", get(ctx, "a1", path))
    end

    test "lifecycle stamps the transition and already_closed applies to a repeat", ctx do
      id = forward(ctx, "a1", 1, nil)
      path = "/v2/agent/messages/" <> id <> "/lifecycle"

      assert_error(
        request(ctx, "a1", "POST", path, %{"action" => "retracted"}, 2),
        400,
        "invalid_field"
      )

      response = request(ctx, "a1", "POST", path, %{"action" => "cleared"}, 2)
      assert response.status == 200

      assert %{"message" => %{"terminal" => %{"state" => "cleared", "at_ms" => at_ms}}} =
               contract(ctx, "AgentWriteResult", response)

      assert abs(System.system_time(:millisecond) - at_ms) < 60_000

      assert_error(
        request(ctx, "a1", "POST", path, %{"action" => "expired"}, 3),
        409,
        "already_closed"
      )
    end

    test "lifecycle refuses a retracted item with the closed transition error", ctx do
      id = forward(ctx, "a1", 1, nil)
      item = "/v2/agent/messages/" <> id
      assert request(ctx, "a1", "POST", item <> "/retract", %{}, 2).status == 200

      assert_error(
        request(ctx, "a1", "POST", item <> "/lifecycle", %{"action" => "expired"}, 3),
        409,
        "already_closed"
      )

      assert_error(request(ctx, "a1", "POST", item <> "/retract", %{}, 4), 409, "already_retracted")
    end

    test "correlated retract marks the person's turn acted without recording handoff", ctx do
      id = forward(ctx, "a1", 1, @first_session)
      entry_id = person_reply(ctx, id, 2)
      path = "/v2/agent/messages/" <> id <> "/retract"
      body = %{"in_reply_to" => entry_id}
      assert_error(request(ctx, "a2", "POST", path, body, 3), 404, "not_found")

      response = request(ctx, "a1", "POST", path, body, 3)
      assert response.status == 200

      assert %{
               "message" => %{"terminal" => %{"state" => "retracted"}},
               "entry" => %{"entry_id" => ^entry_id, "delivery" => %{"acted" => true} = delivery}
             } = contract(ctx, "AgentWriteResult", response)

      refute Map.has_key?(delivery, "outcome")
      assert_error(request(ctx, "a1", "POST", path, %{}, 4), 409, "already_retracted")
    end

    test "plain and repeated correlated replies are accepted on an answered conversation", ctx do
      id = forward(ctx, "a1", 1, @first_session)
      received = person_reply(ctx, id, 2)
      item = "/v2/agent/messages/" <> id

      answer =
        request(ctx, "a2", "POST", "/v2/items/" <> id <> "/answer", %{"request_revision" => 1}, 3)

      assert answer.status == 200

      for {n, body} <- [
            {4, %{"text" => "Plain"}},
            {5, %{"text" => "Correlated", "in_reply_to" => received}},
            {6, %{"text" => "Follow-up", "in_reply_to" => received}}
          ] do
        response = request(ctx, "a1", "POST", item <> "/replies", body, n)
        assert response.status == 200

        assert %{"entry" => %{"kind" => "reply", "author" => "origin"}} =
                 contract(ctx, "AgentWriteResult", response)
      end

      sessionless = forward(ctx, "a1", 7, nil)

      assert_error(
        request(
          ctx,
          "a1",
          "POST",
          "/v2/agent/messages/" <> sessionless <> "/replies",
          %{"text" => "No chat"},
          8
        ),
        400,
        "unsupported"
      )
    end
  end

  describe "agent results, receipts and session end" do
    @describetag :agent_receipts

    test "the message receipt and result require a session and an answer in that order", ctx do
      for {n, session, status, token} <- [
            {1, nil, 400, "unsupported"},
            {2, @first_session, 409, "not_answered"}
          ] do
        id = forward(ctx, "a1", n, session)
        path = "/v2/agent/messages/" <> id

        assert_error(
          request(ctx, "a1", "POST", path <> "/receipt", %{"outcome" => "handed"}, n + 2),
          status,
          token
        )

        assert_error(
          request(ctx, "a1", "POST", path <> "/result", %{"status" => "received"}, n + 4),
          status,
          token
        )
      end
    end

    test "answer receipt ensures delivery and a result marks the answer acted", ctx do
      id = forward(ctx, "a1", 1, @first_session)
      item = "/v2/agent/messages/" <> id

      answer =
        request(
          ctx,
          "a2",
          "POST",
          "/v2/items/" <> id <> "/answer",
          %{"request_revision" => 1, "comment" => "Person answer"},
          2
        )

      assert answer.status == 200

      receipt =
        request(
          ctx,
          "a1",
          "POST",
          item <> "/receipt",
          %{"outcome" => "handed", "agent" => "fixture"},
          3
        )

      assert receipt.status == 200

      assert %{
               "status" => "recorded",
               "stage_outcome" => "handed",
               "message" => %{
                 "terminal" => %{"delivery" => %{"delivered" => true, "agent" => "fixture"}}
               }
             } = contract(ctx, "AgentReceiptResult", receipt)

      result = request(ctx, "a1", "POST", item <> "/result", %{"status" => "applied"}, 4)
      assert result.status == 200

      assert %{
               "message" => %{
                 "result" => %{"status" => "applied"},
                 "terminal" => %{"delivery" => %{"acted" => true}}
               },
               "entry" => %{"kind" => "result"}
             } = contract(ctx, "AgentWriteResult", result)

      assert_error(
        request(ctx, "a1", "POST", item <> "/result", %{"status" => "received"}, 5),
        409,
        "already_closed"
      )
    end

    test "chat receipt applies only to the scoped person turn", ctx do
      id = forward(ctx, "a1", 1, @first_session)
      entry = person_reply(ctx, id, 2)
      path = "/v2/agent/messages/" <> id <> "/entries/" <> entry <> "/receipt"
      body = %{"outcome" => "not_handed", "reason" => "Fixture declined"}
      assert_error(request(ctx, "a2", "POST", path, body, 3), 404, "not_found")

      first = request(ctx, "a1", "POST", path, body, 3)
      assert first.status == 200

      assert %{"entry" => %{"delivery" => %{"delivered" => true, "outcome" => "not_handed"}}} =
               contract(ctx, "AgentReceiptResult", first)

      rescued = request(ctx, "a1", "POST", path, %{"outcome" => "handed", "rerouted" => true}, 4)

      assert %{"status" => "recorded", "stage_outcome" => "handed"} =
               contract(ctx, "AgentReceiptResult", rescued)

      already = request(ctx, "a1", "POST", path, body, 5)

      assert %{"status" => "superseded", "stage_outcome" => "handed"} =
               contract(ctx, "AgentReceiptResult", already)
    end

    test "ending a session is scoped and leaves the conversation open", ctx do
      id = forward(ctx, "a1", 1, @first_session)
      path = "/v2/agent/sessions/" <> @first_session <> "/end"
      assert_error(request(ctx, "a2", "POST", path, %{}, 2), 404, "not_found")
      ended = request(ctx, "a1", "POST", path, %{}, 2)
      assert ended.status == 200
      item = "/v2/agent/messages/" <> id

      assert %{"message" => %{"availability" => %{"state" => "gone"}}} =
               contract(ctx, "SentMessageResponse", get(ctx, "a1", item))

      assert request(ctx, "a1", "POST", item <> "/replies", %{"text" => "After end"}, 3).status ==
               200
    end

    test "ending a v2 session records gone for its open items", ctx do
      body = %{
        "sender" => "app:test",
        "session" => @first_session,
        "message" => %{"title" => "Open", "created_at" => 1}
      }

      sent = request(ctx, "a1", "POST", "/v2/agent/messages", body, 1)
      id = contract(ctx, "AgentSendResult", sent)["message"]["item_id"]
      other = forward(ctx, "a1", 2, @other_session)
      path = "/v2/agent/sessions/" <> @first_session <> "/end"

      assert request(ctx, "a1", "POST", path, %{}, 3).status == 200
      assert request(ctx, "a1", "POST", path, %{}, 4).status == 200

      entries = fn item ->
        route = "/v2/agent/messages/" <> item <> "/entries"
        contract(ctx, "AgentEntriesPage", get(ctx, "a1", route))["entries"]
      end

      assert [%{"kind" => "availability", "availability" => %{"state" => "gone"}}] = entries.(id)
      refute Enum.any?(entries.(other), &(&1["availability"]["state"] == "gone"))
    end
  end

  describe "agent files" do
    @describetag :agent_files

    test "upload handles are scoped, attach to file-only replies, and give distinct download IDs",
         ctx do
      id = forward(ctx, "a1", 1, @first_session)
      characters = "file contents"
      upload = upload(ctx, "a1", 2, characters)
      assert upload.status == 201
      handle = contract(ctx, "FileCreated", upload)["file_id"]
      path = "/v2/files/" <> handle
      assert_error(get(ctx, "a1", path), 404, "not_found")

      :sys.replace_state(ctx.relay, fn state ->
        update_in(state.store.engine.sender_grants, &Map.delete(&1, {"A", ctx.keys["a2"]}))
      end)

      assert_error(request(ctx, "a2", "DELETE", path, %{}, 3), 404, "not_found")

      sent =
        request(
          ctx,
          "a1",
          "POST",
          "/v2/agent/messages/" <> id <> "/replies",
          %{"file_ids" => [handle]},
          4
        )

      assert sent.status == 200

      assert %{"entry" => %{"files" => [%{"file_id" => download}]}} =
               contract(ctx, "AgentWriteResult", sent)

      refute handle == download
      file = get(ctx, "a1", "/v2/files/" <> download)
      assert file.status == 200
      assert file.body == characters
      assert_error(request(ctx, "a1", "DELETE", path, %{}, 5), 409, "already_closed")
    end

    test "shared file idempotency is person scoped across device keys", ctx do
      first = upload(ctx, "a1", 1, "first")
      assert first.status == 201
      assert_error(upload(ctx, "a2", 1, "different"), 409, "idempotency_conflict")
      assert upload(ctx, "b1", 1, "different account").status == 201

      first_id = contract(ctx, "FileCreated", first)["file_id"]
      second = upload(ctx, "a2", 2, "second")
      second_id = contract(ctx, "FileCreated", second)["file_id"]

      assert request(ctx, "a1", "DELETE", "/v2/files/" <> first_id, %{}, 3).status == 204

      assert_error(
        request(ctx, "a2", "DELETE", "/v2/files/" <> second_id, %{}, 3),
        409,
        "idempotency_conflict"
      )

      assert request(ctx, "a2", "DELETE", "/v2/files/" <> second_id, %{}, 4).status == 204
    end

    test "a same-account device can discard a handle staged before ownership tracking", ctx do
      test_pid = self()

      :sys.replace_state(ctx.relay, fn state ->
        alias AttentionServer.ClientAPI.Uploads

        {:ok, dir} = Uploads.dir(state.store)
        path = Path.join(dir, "pre-owner-fixture")
        File.write!(path, "fixture")

        attrs = %{name: "legacy.txt", content_type: "text/plain", size: 7, sha256: String.duplicate("a", 64)}
        {:ok, handle} = Uploads.stage(state.store, "A", attrs, path)
        send(test_pid, {:pre_owner_handle, handle})
        state
      end)

      assert_receive {:pre_owner_handle, handle}
      path = "/v2/files/" <> handle
      assert_error(request(ctx, "b1", "DELETE", path, %{}, 1), 404, "not_found")
      assert request(ctx, "a2", "DELETE", path, %{}, 1).status == 204
    end

    test "file-use preserves transfer failure, duplicate and conflict outcomes", ctx do
      item_id = forward(ctx, "a1", 1, @first_session)
      item = "/v2/agent/messages/" <> item_id
      handle = contract(ctx, "FileCreated", upload(ctx, "a2", 2, "person attachment"))["file_id"]

      reply =
        request(
          ctx,
          "a2",
          "POST",
          "/v2/items/" <> item_id <> "/replies",
          %{"text" => "File for the agent", "file_ids" => [handle]},
          3
        )

      assert %{"entry" => %{"entry_id" => entry, "files" => [%{"file_id" => file}]}} =
               contract(ctx, "WriteResult", reply)

      turn = item <> "/entries/" <> entry
      path = turn <> "/file-use"
      failed = %{"file_id" => file, "transfer" => "not_transferred", "reason" => "Fixture"}
      assert_error(request(ctx, "a1", "POST", path, failed, 4), 404, "not_found")

      assert request(ctx, "a1", "POST", turn <> "/receipt", %{"outcome" => "handed"}, 5).status ==
               200

      response = request(ctx, "a1", "POST", path, failed, 4)
      assert response.status == 200

      assert %{"file_id" => ^file, "transfer" => "not_transferred"} =
               decode_response(response)

      assert %{"duplicate" => true} =
               decode_response(request(ctx, "a1", "POST", path, failed, 6))

      usable = request(ctx, "a1", "POST", path, %{"file_id" => file, "use" => "used"}, 7)
      assert usable.status == 200

      assert %{
               "use" => "used",
               "entry" => %{
                 "delivery" => %{
                   "file_uses" => [
                     %{"file_id" => ^file, "transfer" => "not_transferred", "use" => "used"}
                   ]
                 }
               }
             } = decode_response(usable)

      conflict = %{"file_id" => file, "transfer" => "transferred", "use" => "used"}
      assert_error(request(ctx, "a1", "POST", path, conflict, 8), 409, "already_closed")
    end

    test "the uploader can delete an unattached upload, including idempotent retry", ctx do
      assert %{"file_id" => handle} =
               contract(ctx, "FileCreated", upload(ctx, "a1", 1, "discard"))

      path = "/v2/files/" <> handle
      first = request(ctx, "a1", "DELETE", path, %{}, 2)
      assert first.status == 204
      assert first.body == ""
      replay = request(ctx, "a1", "DELETE", path, %{}, 2)
      assert replay.status == 204

      assert List.keyfind(replay.headers, "idempotent-replayed", 0) ==
               {"idempotent-replayed", "true"}

      assert_error(request(ctx, "a1", "DELETE", path, %{}, 3), 404, "not_found")
    end
  end

  defp upload(ctx, name, n, data) do
    headers = [
      {"content-type", "text/plain"},
      {"content-length", Integer.to_string(byte_size(data))},
      {"content-disposition", "attachment; filename*=UTF-8''fixture.txt"},
      {"idempotency-key", op(n)}
    ]

    conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, name)
    assert {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", "/v2/files", headers, data)
    {conn, response} = ClientAPIFeedClient.collect(conn, ref)
    Mint.HTTP.close(conn)
    response
  end

  @tag :agent_advisory
  test "acknowledges the retained advisory of the scoped sent message", ctx do
    alias AttentionServer.{BounceDecision, BouncePolicy, Store}

    :sys.replace_state(ctx.relay, fn state ->
      at = state.clock.()
      levels = [%{"name" => "Urgent", "lower_bound" => 0, "bounce_after_days" => 1}]
      {:ok, normalized} = BouncePolicy.normalize_levels(levels)

      {:ok, configured, _} =
        BounceDecision.configure_levels(state.store.engine, "A", normalized, at)

      {:ok, store} =
        Store.commit(state.store, configured, %{
          person: "A",
          actor: "fixture",
          action: "configure_fixture"
        })

      %{state | store: store}
    end)

    id =
      frame(ctx, "a1", %{
        "type" => "forward",
        "op_id" => op(1),
        "session_handle" => @first_session,
        "route" => "claude-code",
        "cli_version" => "fixture",
        "unsupported_reason" => nil,
        "advisory_v1" => true,
        "message" => %{
          "sender_id" => "app:test",
          "message_id" => "advisory-item",
          "title" => "Needs attention",
          "message_type" => "fyi",
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
          "created_at" => 1
        }
      })
      |> then(&Resources.item_id(%{accepted_seq: &1["seq"]}))

    key = {"A", "app:test", "advisory-item"}

    state =
      :sys.replace_state(ctx.relay, fn state ->
        engine = state.store.engine
        at = engine.messages[key].accepted_at

        selected =
          BounceDecision.record_scores(
            engine,
            "A",
            [
              %{"sender_id" => "app:test", "message_id" => "advisory-item", "score" => 0.9}
            ],
            at
          )

        next = BounceDecision.evaluate_due(selected, at + 86_400_001)

        {:ok, store} =
          Store.commit(state.store, next, %{
            person: "A",
            actor: "fixture",
            action: "advisory_fixture"
          })

        %{state | store: store}
      end)

    feed = state.store.engine.advisory_feeds[{"A", ctx.keys["a1"], @first_session}]
    assert %{"advisory_id" => advisory_id} = feed.entries[1]
    path = "/v2/agent/messages/" <> id <> "/advisories/ack"
    body = %{"advisory_id" => advisory_id}
    assert_error(request(ctx, "a2", "POST", path, body, 2), 404, "not_found")
    first = request(ctx, "a1", "POST", path, body, 2)
    assert first.status == 200

    assert %{"delivered" => true, "already_delivered" => false} =
             contract(ctx, "AgentAdvisoryAckResult", first)

    assert %{"already_delivered" => true} =
             contract(ctx, "AgentAdvisoryAckResult", request(ctx, "a1", "POST", path, body, 3))
  end

  defp person_reply(ctx, id, n) do
    path = "/v2/items/" <> id <> "/replies"
    response = request(ctx, "a2", "POST", path, %{"text" => "Person turn"}, n)
    assert response.status == 200
    contract(ctx, "WriteResult", response)["entry"]["entry_id"]
  end

  defp assert_error(response, status, token) do
    assert response.status == status
    assert {:ok, %{"error" => ^token}} = JSON.decode_line(response.body <> "\n")
  end

  defp request(ctx, name, method, path, body, n, extra_headers \\ []) do
    headers = [{"content-type", "application/json"}, {"idempotency-key", op(n)} | extra_headers]
    conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, name)
    assert {:ok, conn, ref} = Mint.HTTP.request(conn, method, path, headers, JSON.encode!(body))
    {conn, response} = ClientAPIFeedClient.collect(conn, ref)
    Mint.HTTP.close(conn)
    response
  end

  defp forward(ctx, name, n, session) do
    %{"seq" => seq} =
      frame(ctx, name, %{
        "type" => "forward",
        "op_id" => op(n),
        "session_handle" => session,
        "message" => %{
          "sender_id" => "app:test",
          "message_id" => "item-#{n}",
          "title" => "Message #{n}",
          "created_at" => n
        }
      })

    Resources.item_id(%{accepted_seq: seq})
  end

  defp frame(ctx, name, payload) do
    line = JSON.encode!(Map.put(payload, "v", 1)) <> "\n"
    assert {:ok, response} = Relay.request_line(ctx.relay, ctx.keys[name], line)
    assert {:ok, %{"type" => "ok"} = decoded} = JSON.decode_line(response)
    decoded
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp get(ctx, name, path, headers \\ []) do
    conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, name)
    assert {:ok, conn, ref} = Mint.HTTP.request(conn, "GET", path, headers, nil)
    {conn, response} = ClientAPIFeedClient.collect(conn, ref)
    Mint.HTTP.close(conn)
    response
  end

  defp decode_response(response) do
    assert {:ok, body} = JSON.decode_line(response.body <> "\n")
    body
  end

  defp contract(ctx, schema, response) do
    assert {:ok, body} = JSON.decode_line(response.body <> "\n")
    assert ClientAPISchema.errors(ctx.defs, schema, body) == []
    body
  end
end
