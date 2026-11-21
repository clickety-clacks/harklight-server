defmodule AttentionServer.ClientAPIListenerTest do
  # Client API v2 listener, router and authenticators (spec §2 and §6), behind
  # the client_api_v2 flag.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AttentionServer.{
    ClientAPI,
    DashboardBrowserSession,
    DashboardHTTPS,
    DeviceIdentity,
    GuidedJoin,
    JSON,
    Relay,
    Server,
    TLSFixtures
  }

  defmodule TestRoutes do
    @behaviour AttentionServer.ClientAPI.Routes

    alias AttentionServer.ClientAPI.{Context, Errors}

    @impl true
    def routes do
      [
        {"GET", ["whoami"], :whoami},
        {"GET", ["me"], :whoami},
        {"POST", ["echo", :id], :echo},
        {"GET", ["boom"], :boom},
        {"GET", ["held"], :held},
        {"GET", ["held-stream"], :held, stream: true}
      ]
    end

    def whoami(conn, %Context{principal: principal, request_id: request_id}) do
      Errors.send_json(conn, 200, %{
        "person" => principal.person,
        "kind" => Atom.to_string(principal.kind),
        "request_id" => request_id
      })
    end

    def echo(conn, %Context{params: %{id: id}}), do: Errors.send_json(conn, 200, %{"id" => id})

    def boom(_conn, _ctx), do: raise("handler defect")

    # Holds its slot until the test releases it.
    def held(conn, _ctx) do
      send(:persistent_term.get({__MODULE__, :test}), {:held, self()})

      receive do
        :release -> Errors.send_json(conn, 200, %{})
      after
        5_000 -> Errors.send_json(conn, 200, %{})
      end
    end
  end

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir =
      Path.join(System.tmp_dir!(), "harklight-client-api-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    :persistent_term.put({TestRoutes, :test}, self())
    on_exit(fn -> :persistent_term.erase({TestRoutes, :test}) end)
    %{dir: dir, certs: certs}
  end

  defp device_relay(%{dir: dir, certs: certs}) do
    relay =
      start_supervised!(
        {Relay, store_path: Path.join(dir, "relay.sqlite3"), enrollment_fixture_mode: true},
        restart: :temporary
      )

    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")

    keys =
      Map.new(~w(a1 b1), fn name ->
        person = if name == "b1", do: "B", else: "A"

        assert {:ok, key} =
                 Relay.setup_device_certificate(
                   relay,
                   person,
                   TLSFixtures.certificate_der!(certs, name)
                 )

        {name, key}
      end)

    {relay, keys}
  end

  defp device_listener(relay, certs, extra \\ []) do
    listener =
      start_supervised!(
        {ClientAPI.Listener,
         [
           relay: relay,
           certfile: Path.join(certs, "server.pem"),
           keyfile: Path.join(certs, "server.key"),
           cacertfile: Path.join(certs, "ca.pem"),
           routes: [TestRoutes]
         ] ++ extra},
        id: make_ref()
      )

    ClientAPI.Listener.port(listener)
  end

  describe "device listener" do
    setup ctx do
      {relay, keys} = device_relay(ctx)
      %{relay: relay, keys: keys, port: device_listener(relay, ctx.certs)}
    end

    test "serves /v2 over HTTP/2 through ALPN and over HTTP/1.1 to an enrolled device", ctx do
      for protocol <- [:http2, :http1] do
        assert {:ok, conn} = device_connect(ctx, "a1", protocol)
        assert Mint.HTTP.protocol(conn) == protocol
        assert {:ok, conn, response} = request(conn, "GET", "/v2/whoami")
        assert response.status == 200
        body = json(response)
        assert body["person"] == "A"
        assert body["kind"] == "device"
        assert [request_id] = header(response, "request-id")
        assert body["request_id"] == request_id
        Mint.HTTP.close(conn)
      end

      assert {:ok, conn} = device_connect(ctx, "b1", :http2)
      assert {:ok, _conn, response} = request(conn, "GET", "/v2/whoami")
      assert json(response)["person"] == "B"
    end

    test "unknown and non-/v2 paths answer the not_found envelope with Request-Id", ctx do
      assert {:ok, conn} = device_connect(ctx, "a1", :http2)

      paths = [{"GET", "/v2/nothing"}, {"POST", "/v2/whoami"}, {"GET", "/relay"}]

      conn =
        Enum.reduce(paths, conn, fn {method, path}, conn ->
          assert {:ok, conn, response} = request(conn, method, path)
          assert response.status == 404
          assert [request_id] = header(response, "request-id")

          assert json(response) == %{
                   "error" => "not_found",
                   "retryable" => false,
                   "request_id" => request_id
                 }

          conn
        end)

      assert {:ok, _conn, response} = request(conn, "POST", "/v2/echo/a%2Fb")
      assert json(response) == %{"id" => "a/b"}
    end

    test "every response carries a distinct Request-Id", ctx do
      assert {:ok, conn} = device_connect(ctx, "a1", :http2)
      assert {:ok, conn, first} = request(conn, "GET", "/v2/whoami")
      assert {:ok, _conn, second} = request(conn, "GET", "/v2/nothing")
      assert [a] = header(first, "request-id")
      assert [b] = header(second, "request-id")
      assert a != b
      assert byte_size(a) <= 128
    end

    test "an unenrolled device certificate fails at the TLS handshake", ctx do
      TLSFixtures.make_self_signed_client!(ctx.certs, "never-enrolled")
      assert tls_refused?(ctx, "never-enrolled")
    end

    test "a CA-issued certificate with no enrollment gets 401 and no data", ctx do
      assert {:ok, conn} = device_connect(ctx, "unknown", :http1)
      assert {:ok, _conn, response} = request(conn, "GET", "/v2/whoami")
      assert response.status == 401
      assert %{"error" => "unauthorized", "retryable" => false} = json(response)
      assert header(response, "connection") == ["close"]
    end

    test "revoke ends requests: open connections are refused and new ones fail TLS", ctx do
      for protocol <- [:http2, :http1] do
        {relay, keys} = {ctx.relay, ctx.keys}
        assert :ok = Relay.setup_person(relay, "R#{protocol}")
        name = "revoked-#{protocol}"
        TLSFixtures.make_self_signed_client!(ctx.certs, name)

        assert {:ok, key} =
                 Relay.setup_device_certificate(
                   relay,
                   "R#{protocol}",
                   TLSFixtures.certificate_der!(ctx.certs, name)
                 )

        assert {:ok, conn} = device_connect(ctx, name, protocol)
        assert {:ok, conn, response} = request(conn, "GET", "/v2/whoami")
        assert response.status == 200

        assert :ok = Relay.revoke_device(relay, key)

        assert {:ok, _conn, response} = request(conn, "GET", "/v2/whoami")
        assert response.status == 401
        assert %{"error" => "unauthorized", "retryable" => false} = json(response)
        if protocol == :http1, do: assert(header(response, "connection") == ["close"])

        assert tls_refused?(ctx, name)
        # Another device of another person is unaffected.
        assert {:ok, other} = device_connect(ctx, "a1", protocol)
        assert {:ok, _other, %{status: 200}} = request(other, "GET", "/v2/whoami")
        assert keys["a1"] != key
      end
    end

    test "a paused account gets 403 account_paused on every request except GET /v2/me", ctx do
      assert {:ok, person} = Relay.hosted_account_create(ctx.relay, "acct-paused")
      TLSFixtures.make_self_signed_client!(ctx.certs, "paused-device")

      assert {:ok, _key} =
               Relay.setup_device_certificate(
                 ctx.relay,
                 person,
                 TLSFixtures.certificate_der!(ctx.certs, "paused-device")
               )

      assert {:ok, conn} = device_connect(ctx, "paused-device", :http2)
      assert {:ok, conn, %{status: 200}} = request(conn, "GET", "/v2/whoami")
      assert {:ok, _} = Relay.hosted_delivery_state(ctx.relay, person, "STOPPED")

      refused = [
        {"GET", "/v2/whoami"},
        {"POST", "/v2/echo/x"},
        {"GET", "/v2/nothing"},
        {"POST", "/v2/me"},
        {"GET", "/v2/me/more"}
      ]

      conn =
        Enum.reduce(refused, conn, fn {method, path}, conn ->
          assert {:ok, conn, response} = request(conn, method, path)
          assert response.status == 403
          assert [request_id] = header(response, "request-id")

          assert json(response) == %{
                   "error" => "account_paused",
                   "retryable" => false,
                   "request_id" => request_id
                 }

          conn
        end)

      assert {:ok, conn, response} = request(conn, "GET", "/v2/me")
      assert response.status == 200
      assert json(response)["person"] == person

      # Another account is unaffected.
      assert {:ok, other} = device_connect(ctx, "a1", :http2)
      assert {:ok, _other, %{status: 200}} = request(other, "GET", "/v2/whoami")

      assert {:ok, _} = Relay.hosted_delivery_state(ctx.relay, person, "RESTORED")
      assert {:ok, _conn, %{status: 200}} = request(conn, "GET", "/v2/whoami")
    end

    test "a handler defect answers 503 retry_later with Retry-After", ctx do
      assert {:ok, conn} = device_connect(ctx, "a1", :http2)

      log =
        capture_log(fn ->
          assert {:ok, _conn, response} = request(conn, "GET", "/v2/boom")
          send(self(), {:response, response})
        end)

      assert_received {:response, response}
      assert response.status == 503
      assert header(response, "retry-after") == ["1"]
      assert %{"error" => "retry_later", "retryable" => true} = json(response)
      assert log =~ "operation=client_api"
      refute log =~ "handler defect"
    end
  end

  test "under load the router answers 503 retry_later; streams are not counted", ctx do
    {relay, _keys} = device_relay(ctx)
    port = device_listener(relay, ctx.certs, max_in_flight: 1)
    ctx = Map.put(ctx, :port, port)

    assert {:ok, holder} = device_connect(ctx, "a1", :http2)
    assert {:ok, holder, held_ref} = Mint.HTTP.request(holder, "GET", "/v2/held", [], nil)
    assert_receive {:held, held}, 5_000

    assert {:ok, conn} = device_connect(ctx, "a1", :http2)
    assert {:ok, conn, response} = request(conn, "GET", "/v2/whoami")
    assert response.status == 503
    assert header(response, "retry-after") == ["1"]
    assert %{"error" => "retry_later", "retryable" => true} = json(response)

    # A stream route does not take a slot.
    assert {:ok, conn, stream_ref} = Mint.HTTP.request(conn, "GET", "/v2/held-stream", [], nil)
    assert_receive {:held, stream}, 5_000
    send(stream, :release)
    assert {:ok, conn, %{status: 200}} = collect(conn, stream_ref, %{})

    send(held, :release)
    assert {:ok, _holder, %{status: 200}} = collect(holder, held_ref, %{})
    assert {:ok, _conn, %{status: 200}} = request(conn, "GET", "/v2/whoami")
  end

  describe "web origin" do
    setup ctx do
      web_origin(ctx, client_api_v2: true)
    end

    test "browser and device resolve the same person for the same account", ctx do
      device_port = device_listener(ctx.relay, ctx.certs)
      assert {:ok, device} = device_connect(Map.put(ctx, :port, device_port), "a2", :http2)
      assert {:ok, _device, device_response} = request(device, "GET", "/v2/whoami")
      assert device_response.status == 200

      assert {:ok, browser} = browser_connect(ctx, :http2)
      assert Mint.HTTP.protocol(browser) == :http2

      assert {:ok, _browser, browser_response} =
               request(browser, "GET", "/v2/whoami", [{"cookie", cookie(ctx.cookie)}])

      assert browser_response.status == 200
      assert [_] = header(browser_response, "request-id")
      assert json(browser_response)["kind"] == "browser"
      assert json(device_response)["kind"] == "device"
      assert json(browser_response)["person"] == json(device_response)["person"]
      assert json(browser_response)["person"] == ctx.person
    end

    test "a signed-out browser gets 401 JSON on every /v2 path, never a redirect", ctx do
      assert {:ok, conn} = browser_connect(ctx, :http2)

      requests = [
        {"GET", "/v2/whoami", []},
        {"GET", "/v2/nothing", []},
        {"POST", "/v2/echo/x", [{"origin", ctx.origin}]},
        {"GET", "/v2/whoami", [{"cookie", cookie("not-a-session")}]},
        {"PUT", "/v2/settings/priority-levels", [{"cookie", cookie("not-a-session")}]}
      ]

      Enum.reduce(requests, conn, fn {method, path, headers}, conn ->
        assert {:ok, conn, response} = request(conn, method, path, headers)
        assert response.status == 401
        assert header(response, "location") == []
        assert [request_id] = header(response, "request-id")
        assert [content_type] = header(response, "content-type")
        assert content_type =~ "application/json"

        assert json(response) == %{
                 "error" => "unauthorized",
                 "retryable" => false,
                 "request_id" => request_id
               }

        conn
      end)
    end

    test "browser writes need the exact web origin", ctx do
      assert {:ok, conn} = browser_connect(ctx, :http2)
      cookie = {"cookie", cookie(ctx.cookie)}

      refused = [[], [{"origin", "https://evil.test"}], [{"origin", ctx.origin <> "/"}]]

      conn =
        Enum.reduce(refused, conn, fn headers, conn ->
          assert {:ok, conn, response} = request(conn, "POST", "/v2/echo/x", [cookie | headers])
          assert response.status == 403
          assert %{"error" => "forbidden", "retryable" => false} = json(response)
          conn
        end)

      assert {:ok, conn, response} =
               request(conn, "POST", "/v2/echo/x", [cookie, {"origin", ctx.origin}])

      assert response.status == 200
      # Reads carry no origin requirement.
      assert {:ok, _conn, %{status: 200}} = request(conn, "GET", "/v2/whoami", [cookie])
    end

    test "signing out ends the browser's /v2 access", ctx do
      assert {:ok, conn} = browser_connect(ctx, :http2)
      cookie = {"cookie", cookie(ctx.cookie)}
      assert {:ok, conn, %{status: 200}} = request(conn, "GET", "/v2/whoami", [cookie])
      DashboardBrowserSession.end_session(ctx.sessions, ctx.cookie)
      assert {:ok, _conn, response} = request(conn, "GET", "/v2/whoami", [cookie])
      assert response.status == 401
      assert %{"error" => "unauthorized"} = json(response)
    end

    test "the dashboard is still served beside /v2", ctx do
      assert {:ok, conn} = browser_connect(ctx, :http1)
      assert {:ok, _conn, response} = request(conn, "GET", "/sign-in")
      assert response.status == 200
      assert header(response, "request-id") == []
    end
  end

  describe "flag off" do
    test "the web origin answers 404 on /v2 and stays HTTP/1.1 only", ctx do
      ctx = Map.merge(ctx, web_origin(ctx, []))
      assert {:error, _reason} = browser_connect(ctx, :http2)
      assert {:ok, conn} = browser_connect(ctx, :http1)

      conn =
        Enum.reduce([[], [{"cookie", cookie(ctx.cookie)}]], conn, fn headers, conn ->
          assert {:ok, conn, response} = request(conn, "GET", "/v2/whoami", headers)
          assert response.status == 404
          assert header(response, "request-id") == []
          conn
        end)

      assert {:ok, _conn, %{status: 200}} = request(conn, "GET", "/sign-in")
    end

    test "the server starts today's /relay WebSocket and no /v2 listener", ctx do
      flags = Path.join(ctx.dir, "flags.json")
      File.write!(flags, ~s({"client_api_v2":false}))

      server =
        start_supervised!({Server, Keyword.put(server_opts(ctx), :feature_flags_path, flags)})

      refute Server.client_api_v2?(server)
      assert child?(server, :web_socket)
      refute child?(server, :client_api)

      relay = Server.relay(server)
      assert :ok = Relay.setup_person(relay, "A")

      assert {:ok, _} =
               Relay.setup_device_certificate(
                 relay,
                 "A",
                 TLSFixtures.certificate_der!(ctx.certs, "a1")
               )

      assert {:ok, socket} = raw_connect(ctx.certs, Server.port(server), "a1")

      :ok =
        :ssl.send(socket, [
          "GET /relay HTTP/1.1\r\nHost: relay.test\r\nUpgrade: websocket\r\n",
          "Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n",
          "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
          "Sec-WebSocket-Protocol: attention-relay/1\r\n\r\n"
        ])

      assert {:ok, reply} = :ssl.recv(socket, 0, 5_000)
      assert reply =~ "101 Switching Protocols"
      :ssl.close(socket)
    end
  end

  test "flag on: the device port serves /v2 only, with no /relay WebSocket", ctx do
    flags = Path.join(ctx.dir, "flags.json")
    File.write!(flags, ~s({"client_api_v2":true}))

    server =
      start_supervised!({Server, Keyword.put(server_opts(ctx), :feature_flags_path, flags)})

    assert Server.client_api_v2?(server)
    assert child?(server, :client_api)
    refute child?(server, :web_socket)

    relay = Server.relay(server)
    assert :ok = Relay.setup_person(relay, "A")

    assert {:ok, _} =
             Relay.setup_device_certificate(
               relay,
               "A",
               TLSFixtures.certificate_der!(ctx.certs, "a1")
             )

    ctx = Map.put(ctx, :port, Server.port(server))
    assert {:ok, conn} = device_connect(ctx, "a1", :http2)
    assert {:ok, _conn, response} = request(conn, "GET", "/v2/inbox")
    # No component has mounted routes yet.
    assert response.status == 404
    assert [_] = header(response, "request-id")

    assert {:ok, socket} = raw_connect(ctx.certs, ctx.port, "a1")

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

  # -- web origin fixture ---------------------------------------------------

  defp web_origin(%{dir: dir, certs: certs}, extra) do
    skill_path = Path.join(dir, "skills/harklight-agent/SKILL.md")
    File.mkdir_p!(Path.dirname(skill_path))
    File.write!(skill_path, "---\nname: harklight-agent\nversion: \"0.1.0\"\n---\n\n# Agent")
    port = available_port()
    origin = "https://relay.test:#{port}"

    {:ok, join} =
      GuidedJoin.configuration("relay.test", 18444, 18443, File.read!(Path.join(certs, "ca.pem")))

    relay =
      start_supervised!(
        {Relay,
         store_path: Path.join(dir, "web.sqlite3"),
         join_configuration: join,
         dashboard_origin: origin,
         enrollment_fixture_mode: true},
        restart: :temporary,
        id: :web_relay
      )

    owner_cert = TLSFixtures.certificate_der!(certs, "a1")
    {:ok, person} = Relay.operator_bootstrap(relay, owner_cert)
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner_cert)

    assert {:ok, _} =
             Relay.setup_device_certificate(
               relay,
               person,
               TLSFixtures.certificate_der!(certs, "a2")
             )

    start_supervised!(
      {DashboardHTTPS,
       [
         relay: relay,
         owner_person: person,
         port: port,
         ip: {127, 0, 0, 1},
         certfile: Path.join(certs, "server.pem"),
         keyfile: Path.join(certs, "server.key"),
         skill_path: skill_path,
         client_api_routes: [TestRoutes]
       ] ++ extra},
      id: :web_origin
    )

    sessions = {:global, {DashboardHTTPS, relay}}
    {:ok, credential} = Relay.owner_browser_sign_in_create(relay, person, owner_key)
    {:ok, cookie, _} = DashboardBrowserSession.redeem(sessions, credential["sign_in_token"])

    %{
      relay: relay,
      person: person,
      web_port: port,
      origin: origin,
      cookie: cookie,
      sessions: sessions
    }
  end

  defp server_opts(%{dir: dir, certs: certs}) do
    [
      store_path: Path.join(dir, "server.sqlite3"),
      certfile: Path.join(certs, "server.pem"),
      keyfile: Path.join(certs, "server.key"),
      cacertfile: Path.join(certs, "ca.pem"),
      enrollment_enabled: false
    ]
  end

  defp child?(server, id),
    do:
      Enum.any?(Supervisor.which_children(server), &match?({^id, pid, _, _} when is_pid(pid), &1))

  # -- HTTP client ------------------------------------------------------------

  defp device_connect(%{port: port, certs: certs}, name, protocol) do
    Mint.HTTP.connect(:https, "127.0.0.1", port,
      hostname: "relay.test",
      protocols: [protocol],
      mode: :passive,
      transport_opts: [
        versions: [:"tlsv1.3"],
        cacertfile: Path.join(certs, "ca.pem"),
        certfile: Path.join(certs, "#{name}.pem"),
        keyfile: Path.join(certs, "#{name}.key"),
        server_name_indication: ~c"relay.test",
        timeout: 5_000
      ]
    )
  end

  defp browser_connect(%{web_port: port, certs: certs}, protocol) do
    Mint.HTTP.connect(:https, "127.0.0.1", port,
      hostname: "relay.test",
      protocols: [protocol],
      mode: :passive,
      transport_opts: [
        versions: [:"tlsv1.3"],
        cacertfile: Path.join(certs, "ca.pem"),
        server_name_indication: ~c"relay.test",
        timeout: 5_000
      ]
    )
  end

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

  # A TLS 1.3 client may finish its side of the handshake before the server
  # rejects the certificate, so refusal shows on connect or on the first read.
  defp tls_refused?(ctx, name) do
    capture_log(fn ->
      result =
        case device_connect(ctx, name, :http2) do
          {:error, _} ->
            true

          {:ok, conn} ->
            match?({:error, _, _}, request(conn, "GET", "/v2/whoami"))
        end

      send(self(), {:tls_refused, result})
    end)

    assert_received {:tls_refused, result}
    result
  end

  defp request(conn, method, path, headers \\ []) do
    case Mint.HTTP.request(conn, method, path, headers, if(method in ~w(POST PUT), do: "{}")) do
      {:ok, conn, ref} -> collect(conn, ref, %{headers: [], body: ""})
      {:error, conn, reason} -> {:error, conn, reason}
    end
  end

  defp collect(conn, ref, acc) do
    case Mint.HTTP.recv(conn, 0, 5_000) do
      {:ok, conn, responses} ->
        acc =
          Enum.reduce(responses, acc, fn
            {:status, ^ref, status}, acc -> Map.put(acc, :status, status)
            {:headers, ^ref, headers}, acc -> Map.update(acc, :headers, headers, &(&1 ++ headers))
            {:data, ^ref, data}, acc -> Map.update(acc, :body, data, &(&1 <> data))
            {:done, ^ref}, acc -> Map.put(acc, :done, true)
            _other, acc -> acc
          end)

        if acc[:done], do: {:ok, conn, acc}, else: collect(conn, ref, acc)

      {:error, conn, reason, _responses} ->
        {:error, conn, reason}
    end
  end

  defp header(response, name),
    do: for({key, value} <- Map.get(response, :headers, []), key == name, do: value)

  defp json(response) do
    assert {:ok, body} = JSON.decode_line(response.body <> "\n")
    body
  end

  defp cookie(value), do: "__Host-harklight=#{value}"

  defp available_port do
    {:ok, socket} = :gen_tcp.listen(0, ip: {127, 0, 0, 1})
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end
end
