defmodule AttentionServer.ClientAPIListenerStopTest do
  # Guarantees the v2 device listener actually gives (rulings on C7 slice 3):
  # - when only the listener stops while the Relay keeps running, it drains
  #   connections it already accepted; a request completed during the drain
  #   commits exactly once and replays by its Idempotency-Key afterwards;
  # - a discarded upload handle never ends up attached or fetchable.
  use ExUnit.Case, async: false

  alias AttentionServer.{ClientAPIFeedClient, JSON, Relay, Server, TLSFixtures}
  alias AttentionServer.ClientAPI.Resources

  @answer_key "00000000-0000-4000-8000-000000000002"

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "listener-stop-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    server =
      start_supervised!(
        {Server,
         store_path: Path.join(dir, "relay.sqlite3"),
         certfile: Path.join(certs, "server.pem"),
         keyfile: Path.join(certs, "server.key"),
         cacertfile: Path.join(certs, "ca.pem"),
         port: 0,
         enrollment_enabled: false}
      )

    relay = Server.relay(server)
    assert :ok = Relay.setup_person(relay, "A")

    [sender, _reader] =
      for name <- ["a1", "a2"] do
        assert {:ok, key} =
                 Relay.setup_device_certificate(
                   relay,
                   "A",
                   TLSFixtures.certificate_der!(certs, name)
                 )

        key
      end

    assert :ok = Relay.setup_sender(relay, sender, "app:test")
    %{server: server, relay: relay, sender: sender, item: forward!(relay, sender)}
  end

  test "a request completed while the listener drains commits once and replays by key", ctx do
    {:ok, socket} = raw_connect(ctx.certs, Server.port(ctx.server), "a2")
    assert :ok = Supervisor.terminate_child(ctx.server, :client_api)

    body = JSON.encode!(%{"request_revision" => 1})

    :ok =
      :ssl.send(socket, [
        "POST /v2/items/#{ctx.item}/answer HTTP/1.1\r\nHost: relay.test\r\n",
        "Content-Type: application/json\r\n",
        "Idempotency-Key: #{@answer_key}\r\n",
        "Content-Length: #{byte_size(body)}\r\n\r\n",
        body
      ])

    assert {:ok, drained} = :ssl.recv(socket, 0, 5_000)
    assert drained =~ "HTTP/1.1 200"
    :ssl.close(socket)

    %{terminal: %{state: "answered", seq: seq}} = message(ctx)

    # The same request after the listener is back replays the committed answer.
    assert {:ok, _} = Supervisor.restart_child(ctx.server, :client_api)
    conn = ClientAPIFeedClient.connect(Server.port(ctx.server), ctx.certs, "a2")

    headers = [{"content-type", "application/json"}, {"idempotency-key", @answer_key}]

    assert {:ok, conn, ref} =
             Mint.HTTP.request(conn, "POST", "/v2/items/#{ctx.item}/answer", headers, body)

    {conn, replay} = ClientAPIFeedClient.collect(conn, ref)
    Mint.HTTP.close(conn)
    assert replay.status == 200
    assert {"idempotent-replayed", "true"} in replay.headers
    assert %{terminal: %{state: "answered", seq: ^seq}} = message(ctx)
  end

  test "a discarded upload handle is never attached or fetchable", ctx do
    port = Server.port(ctx.server)

    headers = [
      {"content-type", "text/plain"},
      {"content-length", "5"},
      {"content-disposition", "attachment; filename*=UTF-8''notes.txt"},
      {"idempotency-key", "00000000-0000-4000-8000-000000000003"}
    ]

    uploaded = request(port, ctx.certs, "POST", "/v2/files", headers, "notes")
    assert uploaded.status in [200, 201]
    assert {:ok, %{"file_id" => handle}} = JSON.decode_line(uploaded.body <> "\n")

    discard = [{"idempotency-key", "00000000-0000-4000-8000-000000000004"}]
    assert request(port, ctx.certs, "DELETE", "/v2/files/#{handle}", discard, nil).status == 204

    reply =
      request(
        port,
        ctx.certs,
        "POST",
        "/v2/items/#{ctx.item}/replies",
        [
          {"content-type", "application/json"},
          {"idempotency-key", "00000000-0000-4000-8000-000000000005"}
        ],
        JSON.encode!(%{"text" => "With a file", "file_ids" => [handle]})
      )

    assert reply.status in 400..499
    assert request(port, ctx.certs, "GET", "/v2/files/#{handle}", [], nil).status == 404

    entries = request(port, ctx.certs, "GET", "/v2/items/#{ctx.item}/entries", [], nil)
    assert entries.status == 200
    refute entries.body =~ handle
  end

  defp message(ctx),
    do: :sys.get_state(ctx.relay).store.engine.messages[{"A", "app:test", "stop"}]

  defp forward!(relay, sender) do
    line =
      JSON.encode!(%{
        "v" => 1,
        "type" => "forward",
        "op_id" => "00000000-0000-4000-8000-000000000001",
        "session_handle" => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "message" => %{
          "sender_id" => "app:test",
          "message_id" => "stop",
          "title" => "Stop",
          "created_at" => 1
        }
      }) <> "\n"

    assert {:ok, reply} = Relay.request_line(relay, sender, line)
    assert {:ok, %{"type" => "ok", "seq" => seq}} = JSON.decode_line(reply)
    Resources.item_id(%{accepted_seq: seq})
  end

  defp request(port, certs, method, path, headers, body) do
    conn = ClientAPIFeedClient.connect(port, certs, "a2")
    assert {:ok, conn, ref} = Mint.HTTP.request(conn, method, path, headers, body)
    {conn, response} = ClientAPIFeedClient.collect(conn, ref)
    Mint.HTTP.close(conn)
    response
  end

  defp raw_connect(certs, port, name) do
    :ssl.connect({127, 0, 0, 1}, port,
      versions: [:"tlsv1.3"],
      verify: :verify_peer,
      cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
      certfile: String.to_charlist(Path.join(certs, "#{name}.pem")),
      keyfile: String.to_charlist(Path.join(certs, "#{name}.key")),
      server_name_indication: ~c"relay.test",
      session_tickets: :disabled,
      active: false,
      mode: :binary,
      packet: :raw
    )
  end
end
