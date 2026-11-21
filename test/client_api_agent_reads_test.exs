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
      assert List.keyfind(replay.headers, "idempotent-replayed", 0) == {"idempotent-replayed", "true"}
      assert replay.body == sent.body

      changed = put_in(body["message"]["title"], "Different intent")
      assert_error(request(ctx, "a1", "POST", path, changed, 1), 409, "idempotency_conflict")

      other_key = request(ctx, "a2", "POST", path, body, 1)
      assert other_key.status == 200
      refute contract(ctx, "AgentWriteResult", other_key)["message"]["item_id"] == message["item_id"]
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

    test "lifecycle stamps the transition and already_closed applies to a repeat", ctx do
      id = forward(ctx, "a1", 1, nil)
      path = "/v2/agent/messages/" <> id <> "/lifecycle"
      assert_error(request(ctx, "a1", "POST", path, %{"action" => "retracted"}, 2), 400, "invalid_field")

      response = request(ctx, "a1", "POST", path, %{"action" => "cleared"}, 2)
      assert response.status == 200
      assert %{"message" => %{"terminal" => %{"state" => "cleared", "at_ms" => at_ms}}} =
               contract(ctx, "AgentWriteResult", response)
      assert abs(System.system_time(:millisecond) - at_ms) < 60_000
      assert_error(request(ctx, "a1", "POST", path, %{"action" => "expired"}, 3), 409, "already_closed")
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
      answer = request(ctx, "a2", "POST", "/v2/items/" <> id <> "/answer", %{"request_revision" => 1}, 3)
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
        request(ctx, "a1", "POST", "/v2/agent/messages/" <> sessionless <> "/replies", %{"text" => "No chat"}, 8),
        400,
        "unsupported"
      )
    end
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

  defp contract(ctx, schema, response) do
    assert {:ok, body} = JSON.decode_line(response.body <> "\n")
    assert ClientAPISchema.errors(ctx.defs, schema, body) == []
    body
  end
end
