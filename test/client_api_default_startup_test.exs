defmodule AttentionServer.ClientAPIDefaultStartupTest do
  # Server 0.2.0 serves client API v2 only: a supervised Server starts /v2,
  # both change logs and the v2 dashboard origin, whatever the feature-flags
  # file says about `client_api_v2`. A Relay started directly has the change
  # log on as well; it has no option or file setting that turns it off.
  use ExUnit.Case, async: false

  alias AttentionServer.{
    ClientAPIFeedClient,
    DashboardBrowserSession,
    DashboardHTTPS,
    DeviceIdentity,
    JSON,
    PreChangeLogStore,
    Relay,
    Server,
    TLSFixtures
  }

  alias AttentionServer.ClientAPI.Resources

  @session "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "default-startup-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir, certs: certs}
  end

  describe "with no client_api_v2 setting" do
    setup ctx do
      store_path = Path.join(ctx.dir, "relay.sqlite3")
      owner_der = TLSFixtures.certificate_der!(ctx.certs, "a1")

      # The person exists before the first v2 start, as on a store upgraded
      # from a Server before 0.2.0, which committed with the change log off.
      person = "11111111-1111-4111-8111-111111111111"
      {:ok, owner} = DeviceIdentity.from_certificate(owner_der)

      PreChangeLogStore.write(
        store_path,
        &PreChangeLogStore.operator_bootstrap(&1, person, owner)
      )

      skill = Path.join(ctx.dir, "SKILL.md")
      File.write!(skill, "# Harklight agent")
      relay_name = String.to_atom("default_startup_#{System.unique_integer([:positive])}")
      dashboard_port = available_port()

      server =
        start_server(
          store_path: store_path,
          certfile: Path.join(ctx.certs, "server.pem"),
          keyfile: Path.join(ctx.certs, "server.key"),
          cacertfile: Path.join(ctx.certs, "ca.pem"),
          relay_name: relay_name,
          port: 0,
          enroll_port: 0,
          owner_port: 0,
          join_name: "relay.test",
          dashboard: [
            port: dashboard_port,
            ip: {127, 0, 0, 1},
            owner_person: person,
            certfile: Path.join(ctx.certs, "server.pem"),
            keyfile: Path.join(ctx.certs, "server.key"),
            skill_path: skill,
            skill_path_saas: skill
          ]
        )

      relay = Server.relay(server)
      {:ok, owner_key} = DeviceIdentity.from_certificate(owner_der)

      # a2 is a sender device (its agent feed); b1 reads as the person.
      [agent_key, reader_key] =
        for name <- ["a2", "b1"] do
          assert {:ok, key} =
                   Relay.setup_device_certificate(
                     relay,
                     person,
                     TLSFixtures.certificate_der!(ctx.certs, name)
                   )

          key
        end

      assert :ok = Relay.setup_sender(relay, agent_key, "app:test")

      Map.merge(ctx, %{
        server: server,
        relay: relay,
        relay_name: relay_name,
        person: person,
        keys: %{"a1" => owner_key, "a2" => agent_key, "b1" => reader_key},
        port: Server.port(server),
        dashboard_port: dashboard_port
      })
    end

    test "the device port serves /v2 only, beside every other listener", ctx do
      assert child?(ctx.server, :client_api)
      assert child?(ctx.server, :client_api_feed_shutdown)
      refute child?(ctx.server, :web_socket)

      assert Server.owner_port(ctx.server) > 0
      assert Server.enroll_port(ctx.server) > 0

      store = :sys.get_state(ctx.relay).store
      assert store.change_log
      assert store.mode == :legacy

      response = get(ctx, "b1", "/v2/inbox")
      assert response.status == 200
      assert [_] = header(response, "request-id")

      assert {:ok, socket} = raw_connect(ctx.certs, ctx.port, "b1")

      :ok =
        :ssl.send(socket, [
          "GET /relay HTTP/1.1\r\nHost: relay.test\r\nUpgrade: websocket\r\n",
          "Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n",
          "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
          "Sec-WebSocket-Protocol: attention-relay/1\r\n\r\n"
        ])

      reply = recv_until(socket, ~s("error":"not_found"), "")
      assert reply =~ "HTTP/1.1 404"
      :ssl.close(socket)
    end

    test "the person change log follows a committed change from a real cursor", ctx do
      %{"rev" => head} = batch(ctx, "b1", "/v2/changes?after=0")
      item = forward(ctx, 1)

      assert %{"rev" => next, "changes" => changes} =
               batch(ctx, "b1", "/v2/changes?after=#{head}")

      assert next > head
      assert Enum.any?(changes, &(get_in(&1, ["data", "item_id"]) == item))
    end

    test "the agent change log follows a committed person reply from a real cursor", ctx do
      item = forward(ctx, 1)
      %{"rev" => head} = batch(ctx, "a2", "/v2/agent/changes?after=0")
      entry = person_reply(ctx, item, 2)

      assert %{"rev" => next, "changes" => changes} =
               batch(ctx, "a2", "/v2/agent/changes?after=#{head}")

      assert next > head
      assert Enum.any?(changes, &(get_in(&1, ["data", "entry_id"]) == entry))
    end

    test "the dashboard origin serves same-origin /v2 over HTTP/2", ctx do
      conn = ClientAPIFeedClient.connect(ctx.dashboard_port, ctx.certs, nil, :http2)
      {conn, signed_out} = request(conn, "GET", "/v2/me", [])
      assert signed_out.status == 401
      assert %{"error" => _} = decode(signed_out)

      sessions = {:global, {DashboardHTTPS, ctx.relay_name}}

      {:ok, credential} =
        Relay.owner_browser_sign_in_create(ctx.relay, ctx.person, ctx.keys["a1"])

      {:ok, cookie, _} = DashboardBrowserSession.redeem(sessions, credential["sign_in_token"])

      {conn, me} = request(conn, "GET", "/v2/me", [{"cookie", "__Host-harklight=#{cookie}"}])
      assert me.status == 200
      assert Mint.HTTP.protocol(conn) == :http2
      Mint.HTTP.close(conn)
    end
  end

  describe "a feature-flags file" do
    test "with client_api_v2 false still starts /v2 only, with no /relay", ctx do
      flags = Path.join(ctx.dir, "flags.json")
      File.write!(flags, ~s({"client_api_v2":false,"priority_level_editing":true}))
      ctx = start_flagged_server(ctx, flags)

      assert child?(ctx.server, :client_api)
      assert child?(ctx.server, :client_api_feed_shutdown)
      refute child?(ctx.server, :web_socket)
      assert :sys.get_state(ctx.relay).store.change_log

      me = get(ctx, "b1", "/v2/me")
      assert me.status == 200
      assert decode(me)["features"]["priority_level_editing"] == true

      assert {:ok, socket} = raw_connect(ctx.certs, ctx.port, "b1")

      :ok =
        :ssl.send(socket, [
          "GET /relay HTTP/1.1\r\nHost: relay.test\r\nUpgrade: websocket\r\n",
          "Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n",
          "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
          "Sec-WebSocket-Protocol: attention-relay/1\r\n\r\n"
        ])

      reply = recv_until(socket, ~s("error":"not_found"), "")
      assert reply =~ "HTTP/1.1 404"
      assert reply =~ ~r/request-id: \S+/i
      :ssl.close(socket)
    end

    test "that is malformed, unreadable or missing gives default features and v2 still serves",
         ctx do
      malformed = Path.join(ctx.dir, "malformed.json")
      File.write!(malformed, "{not json")
      unreadable = Path.join(ctx.dir, "unreadable.json")
      File.mkdir_p!(unreadable)
      missing = Path.join(ctx.dir, "missing.json")

      for {flags, n} <- Enum.with_index([malformed, unreadable, missing]) do
        ctx = start_flagged_server(ctx, flags, "flags-#{n}")
        assert child?(ctx.server, :client_api)

        me = get(ctx, "b1", "/v2/me")
        assert me.status == 200

        assert decode(me)["features"] == %{
                 "server_matched_push_routing" => false,
                 "priority_level_editing" => false
               }

        assert get(ctx, "b1", "/v2/inbox").status == 200
        Supervisor.stop(ctx.server)
      end
    end
  end

  test "the protocol 0.1 device adapters and the flag query are gone" do
    refute Code.ensure_loaded?(AttentionServer.WebSocketRelay)
    refute Code.ensure_loaded?(AttentionServer.TLSRelay)
    assert Code.ensure_loaded?(Server)
    refute function_exported?(Server, :client_api_v2?, 1)
  end

  describe "a Relay started directly" do
    test "with no option and no flags file has the change log on", ctx do
      path = Path.join(ctx.dir, "direct.sqlite3")
      relay = start_relay(store_path: path)
      assert :sys.get_state(relay).store.change_log
      GenServer.stop(relay)
      assert "client_change_log_state" in tables(path)
      assert "agent_change_log_state" in tables(path)
    end

    test "keeps the change log on whatever a flags file or option says", ctx do
      flags = Path.join(ctx.dir, "flags.json")
      File.write!(flags, ~s({"client_api_v2":false}))

      from_file =
        start_relay(store_path: Path.join(ctx.dir, "file.sqlite3"), feature_flags_path: flags)

      assert :sys.get_state(from_file).store.change_log

      from_option =
        start_relay(store_path: Path.join(ctx.dir, "option.sqlite3"), client_api_v2: false)

      assert :sys.get_state(from_option).store.change_log
    end

    test "with no store path does not start, because the change log needs a durable store" do
      assert {:error, {:storage_unavailable, :not_durable}} =
               GenServer.start(Relay, store_path: nil)

      assert {:error, {:storage_unavailable, :not_durable}} = GenServer.start(Relay, [])
    end
  end

  # -- helpers ----------------------------------------------------------------

  defp start_server(opts) do
    {:ok, server} = Server.start_link(opts)
    Process.unlink(server)
    on_exit(fn -> if Process.alive?(server), do: Supervisor.stop(server) end)
    server
  end

  defp start_flagged_server(ctx, flags, name \\ "flagged") do
    server =
      start_server(
        store_path: Path.join(ctx.dir, name <> ".sqlite3"),
        feature_flags_path: flags,
        certfile: Path.join(ctx.certs, "server.pem"),
        keyfile: Path.join(ctx.certs, "server.key"),
        cacertfile: Path.join(ctx.certs, "ca.pem"),
        port: 0,
        enrollment_enabled: false
      )

    relay = Server.relay(server)
    assert :ok = Relay.setup_person(relay, "A")

    assert {:ok, _key} =
             Relay.setup_device_certificate(
               relay,
               "A",
               TLSFixtures.certificate_der!(ctx.certs, "b1")
             )

    Map.merge(ctx, %{server: server, relay: relay, port: Server.port(server)})
  end

  defp start_relay(opts) do
    {:ok, relay} = Relay.start_link(opts)
    Process.unlink(relay)
    on_exit(fn -> if Process.alive?(relay), do: GenServer.stop(relay) end)
    relay
  end

  defp tables(path) do
    {:ok, db} = Exqlite.Sqlite3.open(path)

    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, "SELECT name FROM sqlite_master WHERE type = 'table'")

    {:ok, rows} = Exqlite.Sqlite3.fetch_all(db, stmt)
    :ok = Exqlite.Sqlite3.release(db, stmt)
    :ok = Exqlite.Sqlite3.close(db)
    List.flatten(rows)
  end

  defp child?(server, id),
    do:
      Enum.any?(Supervisor.which_children(server), &match?({^id, pid, _, _} when is_pid(pid), &1))

  defp forward(ctx, n) do
    line =
      JSON.encode!(%{
        "v" => 1,
        "type" => "forward",
        "op_id" => op(n),
        "session_handle" => @session,
        "message" => %{
          "sender_id" => "app:test",
          "message_id" => "item-#{n}",
          "title" => "Message #{n}",
          "created_at" => n
        }
      }) <> "\n"

    assert {:ok, response} = Relay.request_line(ctx.relay, ctx.keys["a2"], line)
    assert {:ok, %{"type" => "ok", "seq" => seq}} = JSON.decode_line(response)
    Resources.item_id(%{accepted_seq: seq})
  end

  defp person_reply(ctx, item, n) do
    conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, "b1")

    headers = [{"content-type", "application/json"}, {"idempotency-key", op(n)}]
    body = JSON.encode!(%{"text" => "Person turn"})

    assert {:ok, conn, ref} =
             Mint.HTTP.request(conn, "POST", "/v2/items/#{item}/replies", headers, body)

    {conn, response} = ClientAPIFeedClient.collect(conn, ref)
    Mint.HTTP.close(conn)
    assert response.status == 200
    decode(response)["entry"]["entry_id"]
  end

  defp batch(ctx, name, path) do
    response = get(ctx, name, path, [{"accept", "application/json"}])
    assert response.status == 200
    decode(response)
  end

  defp get(ctx, name, path, headers \\ []) do
    conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, name)
    {conn, response} = request(conn, "GET", path, headers)
    Mint.HTTP.close(conn)
    response
  end

  defp request(conn, method, path, headers) do
    assert {:ok, conn, ref} = Mint.HTTP.request(conn, method, path, headers, nil)
    ClientAPIFeedClient.collect(conn, ref)
  end

  defp decode(response) do
    assert {:ok, body} = JSON.decode_line(response.body <> "\n")
    body
  end

  defp header(response, name),
    do: for({key, value} <- Map.get(response, :headers, []), key == name, do: value)

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp raw_connect(certs, port, name) do
    :ssl.connect({127, 0, 0, 1}, port,
      versions: [:"tlsv1.3"],
      verify: :verify_peer,
      cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
      certfile: String.to_charlist(Path.join(certs, "#{name}.pem")),
      keyfile: String.to_charlist(Path.join(certs, "#{name}.key")),
      server_name_indication: ~c"relay.test",
      active: false,
      mode: :binary,
      packet: :raw
    )
  end

  defp recv_until(socket, pattern, buffer) do
    if String.contains?(buffer, pattern) do
      buffer
    else
      assert {:ok, data} = :ssl.recv(socket, 0, 5_000)
      recv_until(socket, pattern, buffer <> data)
    end
  end

  defp available_port do
    {:ok, socket} = :gen_tcp.listen(0, ip: {127, 0, 0, 1})
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end
end
