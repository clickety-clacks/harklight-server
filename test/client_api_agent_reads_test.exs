defmodule AttentionServer.ClientAPIAgentReadsTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{ClientAPIFeedClient, ClientAPISchema, JSON, Relay, Server, TLSFixtures}
  alias AttentionServer.ClientAPI.Resources

  @first_session "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @other_session "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    defs = ClientAPISchema.contract_defs(Path.expand("../docs/client-api-v2.md", __DIR__))
    %{certs: certs, defs: defs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "agent-reads-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    flags = Path.join(dir, "flags.json")
    File.write!(flags, ~s({"client_api_v2":true}))

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

    %{relay: relay, keys: keys, port: Server.port(server)}
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

  defp op(n), do: "00000000-0000-4000-8000-00000000000#{n}"

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
