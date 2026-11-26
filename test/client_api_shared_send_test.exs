defmodule AttentionServer.ClientAPISharedSendTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{ClientAPIFeedClient, JSON, Relay, Server, TLSFixtures}

  @moduletag :tmp_dir
  @key "10000000-0000-4000-8000-000000000001"
  @another_key "10000000-0000-4000-8000-000000000002"

  setup %{tmp_dir: dir} do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)

    server_opts = [
      store_path: Path.join(dir, "relay.sqlite3"),
      certfile: Path.join(certs, "server.pem"),
      keyfile: Path.join(certs, "server.key"),
      cacertfile: Path.join(certs, "ca.pem"),
      port: 0,
      enrollment_enabled: false,
      ip: {127, 0, 0, 1}
    ]

    %{certs: certs, server_opts: server_opts}
  end

  test "shared send binds one organization, commits once and replays the original result", ctx do
    server = start_supervised!({Server, [shared_messages: true] ++ ctx.server_opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    body = body([first, second, first])

    assert {400, %{"error" => "unsupported"}, _} =
             send_request(ctx, server, @key, body, false)

    assert :ok = Relay.replace_hosted_organization(relay, "org-a", [sender, first, second])
    assert :ok = Relay.replace_hosted_organization(relay, "org-b", [sender, first, second])

    headers = [{"harklight-features", "shared_messages"}]
    assert {200, %{"features" => %{"shared_messages" => true}}, _} =
             get_request(ctx, server, "a2", "/v2/me", [])

    assert {200, %{"items" => [], "rev" => baseline, "continuation" => continuation}, _} =
             get_request(ctx, server, "a2", "/v2/inbox", headers)

    assert {409, %{"error" => "organization_required", "organization_ids" => ids}, _} =
             send_request(ctx, server, @key, body, true)

    assert [organization, _] = ids
    selected = put_in(body, ["recipients", "organization_id"], organization)

    assert {200, committed, _} = send_request(ctx, server, @key, selected, true)
    assert %{
             "rev" => 1,
             "message" => %{"item_id" => id, "shared" => %{"organization_id" => ^organization}},
             "recipients" => [
               %{"person_id" => ^first, "status" => "reached"},
               %{"person_id" => ^second, "status" => "reached"}
             ]
           } = committed

    item_id = :sys.get_state(relay).store.engine.shared_recipients[{id, first}].item_id
    path = "/v2/items/" <> item_id
    headers = [{"harklight-features", "shared_messages"}]

    assert {200, %{"item" => %{"item_id" => ^item_id, "shared" => %{"shared_id" => ^id}}}, _} =
             get_request(ctx, server, "a2", path, headers)

    assert {404, _, _} = get_request(ctx, server, "a2", path, [])
    assert {404, _, _} = get_request(ctx, server, "b1", path, headers)

    assert {200, %{"revision" => %{"request_revision" => 1}}, _} =
             get_request(ctx, server, "a2", path <> "/revisions/1", headers)

    assert {200, %{"entries" => [], "older" => false}, _} =
             get_request(ctx, server, "a2", path <> "/entries", headers)

    assert {200, %{"message" => %{"item_id" => ^id}}, _} =
             get_request(ctx, server, "a1", "/v2/agent/messages/" <> id, headers)

    feed_path = "/v2/changes?after=#{baseline}"
    feed_headers = [{"harklight-continuation", continuation} | headers]

    assert {200, %{"changes" => changes}, _} =
             get_request(ctx, server, "a2", feed_path, feed_headers)

    assert Enum.any?(changes, &match?(%{"kind" => "item", "data" => %{"item_id" => ^item_id}}, &1))
    assert {410, %{"error" => "cursor_expired"}, _} =
             get_request(ctx, server, "b1", feed_path, feed_headers)

    assert {200, %{"items" => [%{"item_id" => ^item_id}]}, _} =
             get_request(ctx, server, "a2", "/v2/inbox", headers)

    assert {200, %{"items" => []}, _} = get_request(ctx, server, "a2", "/v2/inbox", [])

    assert :ok = Relay.delete_hosted_organization(relay, "org-a")
    assert :ok = Relay.delete_hosted_organization(relay, "org-b")
    assert {200, ^committed, headers} = send_request(ctx, server, @key, selected, true)
    assert {"idempotent-replayed", "true"} in headers
    assert {409, %{"error" => "idempotency_conflict"}, _} =
             send_request(ctx, server, @key, body, true)

    engine = :sys.get_state(relay).store.engine
    assert Map.keys(engine.shared_messages) == [id]
    assert Enum.sort(Map.keys(engine.shared_recipients)) == Enum.sort([{id, first}, {id, second}])
  end

  test "cross-organization refusal has no outcomes and creates no shared rows", ctx do
    server = start_supervised!({Server, [shared_messages: true] ++ ctx.server_opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    assert :ok = Relay.replace_hosted_organization(relay, "org-a", [sender, first])
    assert :ok = Relay.replace_hosted_organization(relay, "org-b", [sender, second])

    assert {403, %{"error" => "recipient_refused"} = error, _} =
             send_request(ctx, server, @another_key, body([first, second]), true)
    refute Map.has_key?(error, "recipients")
    assert :sys.get_state(relay).store.engine.shared_messages == %{}
    assert :sys.get_state(relay).store.engine.shared_recipients == %{}
  end

  test "the default-off setting refuses the path while preserving legacy sends", ctx do
    server = start_supervised!({Server, ctx.server_opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, first, second])

    body = body([first, second])
    assert {400, %{"error" => "unsupported"}, _} = send_request(ctx, server, @key, body, true)
    assert {200, %{"message" => message}, _} =
             send_request(ctx, server, @key, Map.delete(body, "recipients"), false)
    refute Map.has_key?(message, "shared")
  end

  defp people(ctx, relay) do
    assert {:ok, sender} = Relay.hosted_account_create(relay, "account-sender")
    assert {:ok, first} = Relay.hosted_account_create(relay, "account-first")
    assert {:ok, second} = Relay.hosted_account_create(relay, "account-second")
    for {person, name} <- [{sender, "a1"}, {first, "a2"}, {second, "b1"}] do
      assert {:ok, key} =
               Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(ctx.certs, name))

      if person == sender, do: assert(:ok = Relay.setup_sender(relay, key, "app:test"))
    end

    {sender, first, second}
  end

  defp body(recipients) do
    %{
      "sender" => "app:test",
      "message" => %{"title" => "Launch", "created_at" => 1},
      "recipients" => %{"kind" => "people", "people" => recipients}
    }
  end

  defp get_request(ctx, server, device, path, headers) do
    conn = ClientAPIFeedClient.connect(Server.port(server), ctx.certs, device)
    assert {:ok, conn, ref} = Mint.HTTP.request(conn, "GET", path, headers, nil)
    {conn, response} = ClientAPIFeedClient.collect(conn, ref)
    Mint.HTTP.close(conn)
    assert {:ok, decoded} = JSON.decode_line(response.body <> "\n")
    {response.status, decoded, response.headers}
  end

  defp send_request(ctx, server, key, body, shared?) do
    headers = [{"content-type", "application/json"}, {"idempotency-key", key}]
    headers = if shared?, do: [{"harklight-features", "shared_messages"} | headers], else: headers
    conn = ClientAPIFeedClient.connect(Server.port(server), ctx.certs, "a1")
    path = "/v2/agent/messages"
    assert {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", path, headers, JSON.encode!(body))
    {conn, response} = ClientAPIFeedClient.collect(conn, ref)
    Mint.HTTP.close(conn)
    assert {:ok, decoded} = JSON.decode_line(response.body <> "\n")
    {response.status, decoded, response.headers}
  end
end
