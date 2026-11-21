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

    assert %{
             "entries" => [
               %{"position" => ^position, "priority" => %{"level" => "now", "score" => 0.9}}
             ],
             "older" => true
           } = contract(ctx, "AgentEntriesPage", get(ctx, "a1", path))
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
