defmodule AttentionServer.ClientAPIListenerTest do
  # Client API v2 listener, router and authenticators (spec §2 and §6).
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AttentionServer.{
    APNS,
    ClientAPI,
    ClientAPIFeedClient,
    DashboardBrowserSession,
    DashboardHTTPS,
    DeviceIdentity,
    GuidedJoin,
    JSON,
    Relay,
    Server,
    TLSFixtures
  }

  alias AttentionServer.ClientAPI.Idempotency

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
        {"GET", ["bad", :thing], :bad_field},
        {"GET", ["held"], :held},
        {"GET", ["held-stream"], :held, stream: true},
        {"GET", ["closing"], :closing, stream: true}
      ]
    end

    # Waits for the change log's closing notice for this principal, as a feed does.
    def closing(conn, %Context{principal: principal}) do
      :ok = AttentionServer.ChangeLog.subscribe_closing(principal)
      send(:persistent_term.get({__MODULE__, :test}), {:closing_subscribed, self()})

      receive do
        {:client_closing, _scope, reason} ->
          Errors.send_json(conn, 200, %{"reason" => Atom.to_string(reason)})
      after
        3_000 -> Errors.send_json(conn, 200, %{"reason" => "none"})
      end
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

    # A handler naming the offending member by its route capture (an atom).
    def bad_field(conn, %Context{params: params}) do
      [field] = Map.keys(params)
      Errors.send_error(conn, "invalid_field", field: field)
    end

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

    test "an error naming its field by atom answers 400 with that field", ctx do
      assert {:ok, conn} = device_connect(ctx, "a1", :http2)
      assert {:ok, _conn, response} = request(conn, "GET", "/v2/bad/x")
      assert response.status == 400

      assert %{"error" => "invalid_field", "field" => "thing", "retryable" => false} =
               json(response)
    end

    test "a handler defect answers 500 with a non-retryable token", ctx do
      assert {:ok, conn} = device_connect(ctx, "a1", :http2)

      log =
        capture_log(fn ->
          assert {:ok, _conn, response} = request(conn, "GET", "/v2/boom")
          send(self(), {:response, response})
        end)

      assert_received {:response, response}
      assert response.status == 500
      assert header(response, "retry-after") == []
      assert [request_id] = header(response, "request-id")

      assert %{"error" => "internal_error", "retryable" => false, "request_id" => ^request_id} =
               json(response)

      assert log =~ "event=harklight_projection_failure request_id=#{request_id}"
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

    # The limit is counted before authentication and routing, so a saturated
    # server sheds every request before it reaches the Relay: with the Relay
    # suspended, authenticating would block, so only an early limit answers.
    :ok = :sys.suspend(relay)

    conn =
      try do
        Enum.reduce(["/v2/whoami", "/v2/nothing"], conn, fn path, conn ->
          assert {:ok, conn, response} = request(conn, "GET", path)
          assert response.status == 503
          assert %{"error" => "retry_later"} = json(response)
          conn
        end)
      after
        :ok = :sys.resume(relay)
      end

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
      web_origin(ctx, [])
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

    test "signing out sends the closing notice to that browser's open requests", ctx do
      assert {:ok, conn} = browser_connect(ctx, :http2)
      headers = [{"cookie", cookie(ctx.cookie)}]
      assert {:ok, conn, ref} = Mint.HTTP.request(conn, "GET", "/v2/closing", headers, nil)
      assert_receive {:closing_subscribed, _pid}, 5_000

      DashboardBrowserSession.end_session(ctx.sessions, ctx.cookie)

      assert {:ok, _conn, response} = collect(conn, ref, %{headers: [], body: ""})
      assert response.status == 200
      assert json(response) == %{"reason" => "revoked"}
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

  describe "owner-registered agents on the device port" do
    test "a registered agent reads the full view with empty grants", ctx do
      {relay, person, owner, agent, port} = start_granted_agent(ctx, "granted")
      assert :ok = Relay.setup_device(relay, person, "fixture-origin")

      for {sender, id, n} <- [{"app:first", "first", 1}, {"app:second", "second", 2}] do
        forward_granted_fixture(relay, sender, id, n)
      end

      assert {:ok, conn} = device_connect(Map.put(ctx, :port, port), "granted", :http2)
      assert {:ok, conn, %{status: 200} = inbox} = request(conn, "GET", "/v2/inbox")
      assert %{"items" => items, "counts" => %{"unresolved" => 2}} = json(inbox)
      assert Enum.sort(Enum.map(items, & &1["sender"])) == ["app:first", "app:second"]

      conn =
        Enum.reduce(items, conn, fn item, conn ->
          Enum.reduce(["", "/entries", "/revisions/1"], conn, fn suffix, conn ->
            assert {:ok, conn, %{status: 200}} =
                     request(conn, "GET", "/v2/items/" <> item["item_id"] <> suffix)

            conn
          end)
        end)

      assert {:ok, conn, %{status: 200} = batch} = request(conn, "GET", "/v2/changes")
      changes = json(batch)["changes"]

      assert Enum.sort(for(%{"kind" => "item", "data" => item} <- changes, do: item["sender"])) ==
               ["app:first", "app:second"]

      visible = hd(items)["item_id"]

      conn =
        Enum.reduce(
          [
            {"POST", "/v2/items/" <> visible <> "/replies"},
            {"POST", "/v2/items/" <> visible <> "/seen"},
            {"PUT", "/v2/devices/me/focus"},
            {"DELETE", "/v2/files/anything"},
            {"POST", "/v2/agent/messages"},
            {"GET", "/v2/settings"},
            {"GET", "/v2/files/anything"}
          ],
          conn,
          fn {method, path}, conn ->
            assert {:ok, conn, %{status: 403} = denied} = request(conn, method, path)
            assert json(denied)["error"] == "forbidden"
            conn
          end
        )

      {conn, feed} = ClientAPIFeedClient.open_feed(conn, "?after=#{json(batch)["rev"]}")
      {conn, feed} = ClientAPIFeedClient.await_open(conn, feed)
      assert feed.status == 200

      assert :ok = Relay.owner_revoke_agent(relay, person, owner, agent)
      {conn, feed, closing} = ClientAPIFeedClient.until_closing(conn, feed)
      assert closing == %{event: "closing", data: %{"reason" => "revoked"}}
      {conn, %{done: true}} = ClientAPIFeedClient.drain(conn, feed)
      assert {:ok, _, %{status: 401}} = request(conn, "GET", "/v2/inbox")
    end

    test "search keeps literal matching, exact filters, and newest-first paging", ctx do
      {relay, person, owner, agent, port} = start_granted_agent(ctx, "granted-search")
      assert :ok = Relay.setup_device(relay, person, "fixture-origin")
      number = %JSON.DecimalToken{raw: "0.12345678901234567890123456789"}
      needle = number.raw

      forward_granted_fixture(relay, "app:alpha", "first", 1, [%{"text" => needle}])
      forward_granted_fixture(relay, "app:beta", "numeric", 2, [%{needle => number}])
      forward_granted_fixture(relay, "app:beta", "last", 3, [%{"text" => needle}])

      assert {:ok, conn} = device_connect(Map.put(ctx, :port, port), "granted-search", :http2)
      path = "/v2/search?q=" <> needle
      assert {:ok, conn, %{status: 200} = first} = request(conn, "GET", path <> "&limit=1")

      assert %{
               "rev" => rev,
               "items" => [%{"sender" => "app:beta", "message" => %{"message_id" => "last"}}],
               "next_page" => page
             } = json(first)

      assert is_integer(rev) and rev > 0
      assert byte_size(page) <= 4_096
      forward_granted_fixture(relay, "app:beta", "new", 4, [%{"text" => needle}])

      assert :ok =
               Relay.owner_replace_agent_grants(relay, person, owner, agent, 1, ["app:beta"], [
                 "app:beta"
               ])

      assert {:ok, conn, %{status: 200} = second} =
               request(conn, "GET", path <> "&limit=4&page=" <> page)

      assert %{"items" => [%{"message" => %{"message_id" => "first"}}]} = json(second)
      refute Map.has_key?(json(second), "next_page")

      assert {:ok, conn, %{status: 200} = exact} =
               request(
                 conn,
                 "GET",
                 path <> "&sender=app:alpha&message_id=first&state=open&limit=500"
               )

      assert %{"items" => [%{"message" => %{"message_id" => "first"}}]} = json(exact)

      assert {:ok, conn, %{status: 404}} =
               request(conn, "GET", path <> "&sender=app:beta&message_id=numeric")

      assert {:ok, conn, %{status: 400} = invalid} =
               request(conn, "GET", "/v2/search?q=different&page=" <> page)

      assert %{"error" => "invalid_field", "field" => "page"} = json(invalid)

      oversized_page = "s" <> String.duplicate("a", 4_096)

      assert {:ok, _conn, %{status: 400} = oversized} =
               request(conn, "GET", path <> "&page=" <> oversized_page)

      assert %{"error" => "invalid_field", "field" => "page"} = json(oversized)
    end

    test "only senders matching both answer grants can be answered", ctx do
      {relay, person, owner, agent, port} = start_granted_agent(ctx, "granted-answer")
      assert :ok = Relay.setup_device(relay, person, "fixture-origin")
      forward_granted_fixture(relay, "app:team/first", "same", 1)
      forward_granted_fixture(relay, "app:team/second", "same", 2)

      assert {:ok, conn} = device_connect(Map.put(ctx, :port, port), "granted-answer", :http2)
      assert {:ok, conn, %{status: 200} = inbox} = request(conn, "GET", "/v2/inbox")
      items = Map.new(json(inbox)["items"], &{&1["sender"], &1["item_id"]})
      path = "/v2/items/" <> items["app:team/first"] <> "/answer"
      other_path = "/v2/items/" <> items["app:team/second"] <> "/answer"
      headers = [{"idempotency-key", "00000000-0000-4000-8000-000000000004"}]
      body = JSON.encode!(%{"request_revision" => 1})

      assert {:ok, conn, %{status: 403}} = request(conn, "POST", path, headers, body)

      assert :ok =
               Relay.owner_replace_agent_grants(relay, person, owner, agent, 1, [], [
                 "app:team/first"
               ])

      assert {:ok, conn, %{status: 403}} = request(conn, "POST", path, headers, body)

      assert :ok =
               Relay.owner_replace_agent_grants(relay, person, owner, agent, 2, ["app:team/"], [
                 "app:team/first"
               ])

      assert {:ok, conn, %{status: 403}} = request(conn, "POST", other_path, headers, body)
      stale = JSON.encode!(%{"request_revision" => 2})
      assert {:ok, conn, %{status: 409} = conflict} = request(conn, "POST", path, headers, stale)
      assert %{"error" => "stale_request"} = json(conflict)

      conn =
        Enum.reduce(
          [{"deliberate", true}, {"intent_ref", "obsolete"}, {"acceptance_seq", 1}],
          conn,
          fn {field, value}, conn ->
            obsolete_body = JSON.encode!(%{"request_revision" => 1, field => value})

            assert {:ok, conn, %{status: 400} = rejected} =
                     request(conn, "POST", path, headers, obsolete_body)

            assert %{"error" => "invalid_field", "field" => ^field} = json(rejected)
            conn
          end
        )

      assert {:ok, conn, %{status: 200} = answer} = request(conn, "POST", path, headers, body)
      assert %{"item" => %{"terminal" => %{"state" => "answered"}}} = json(answer)
      assert {:ok, conn, %{status: 200} = replay} = request(conn, "POST", path, headers, body)
      assert header(replay, "idempotent-replayed") == ["true"]

      assert {:ok, device} = device_connect(Map.put(ctx, :port, port), "a2", :http2)

      device_body = JSON.encode!(%{"request_revision" => 1, "comment" => "different answer"})

      assert {:ok, device, %{status: 409} = device_conflict} =
               request(device, "POST", other_path, headers, device_body)

      assert %{"error" => "idempotency_conflict"} = json(device_conflict)
      Mint.HTTP.close(device)

      assert :ok =
               Relay.owner_replace_agent_grants(relay, person, owner, agent, 3, ["app:team/"], [
                 "app:team/"
               ])

      comment = JSON.encode!(%{"request_revision" => 1, "comment" => "settled"})

      assert {:ok, conn, %{status: 409} = conflict} =
               request(conn, "POST", other_path, headers, comment)

      assert %{"error" => "idempotency_conflict"} = json(conflict)

      other_headers = [{"idempotency-key", "00000000-0000-4000-8000-000000000005"}]

      assert {:ok, conn, %{status: 200} = commented} =
               request(conn, "POST", other_path, other_headers, comment)

      assert %{
               "item" => %{
                 "item_id" => second_id,
                 "terminal" => %{
                   "state" => "answered",
                   "answer" => %{"kind" => "reply", "text" => "settled"}
                 }
               }
             } = json(commented)

      assert second_id == items["app:team/second"]

      assert :ok = Relay.owner_replace_agent_grants(relay, person, owner, agent, 4, [], [])
      assert {:ok, _conn, %{status: 403}} = request(conn, "POST", path, headers, body)
    end
  end

  defp start_granted_agent(ctx, name) do
    server = start_supervised!({Server, server_opts(ctx)})

    relay = Server.relay(server)
    owner_der = TLSFixtures.certificate_der!(ctx.certs, "a1")
    assert {:ok, person} = Relay.operator_bootstrap(relay, owner_der)
    assert {:ok, owner} = DeviceIdentity.from_certificate(owner_der)

    TLSFixtures.make_self_signed_client!(ctx.certs, name)

    assert {:ok, agent} =
             DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(ctx.certs, name))

    assert :ok =
             Relay.owner_register_agent(
               relay,
               person,
               owner,
               "00000000-0000-4000-8000-000000000001",
               agent
             )

    assert {:ok, _reader} =
             Relay.setup_device_certificate(
               relay,
               person,
               TLSFixtures.certificate_der!(ctx.certs, "a2")
             )

    {relay, person, owner, agent, Server.port(server)}
  end

  defp forward_granted_fixture(relay, sender, id, n, nested \\ []) do
    assert :ok = Relay.setup_sender(relay, "fixture-origin", sender)

    frame = %{
      "v" => 1,
      "type" => "forward",
      "op_id" => "00000000-0000-4000-8000-00000000000#{n}",
      "message" => %{
        "sender_id" => sender,
        "message_id" => id,
        "created_at" => n,
        "title" => id,
        "unknown" => %{"nested" => nested}
      }
    }

    assert {:ok, line} = Relay.request_line(relay, "fixture-origin", JSON.encode!(frame) <> "\n")
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(line)
  end

  describe "push matching on the enrolled device listener" do
    setup ctx do
      server = start_supervised!({Server, server_opts(ctx)})

      relay = Server.relay(server)
      assert :ok = Relay.enable_push(relay)
      assert :ok = Relay.setup_person(relay, "A")
      assert :ok = Relay.setup_person(relay, "B")

      for {person, name} <- [{"A", "a1"}, {"A", "a2"}, {"B", "b1"}] do
        assert {:ok, _} =
                 Relay.setup_device_certificate(
                   relay,
                   person,
                   TLSFixtures.certificate_der!(ctx.certs, name)
                 )
      end

      %{relay: relay, port: Server.port(server)}
    end

    test "me and settings expose exactly the application's push matching values", ctx do
      %{relay: relay} = ctx
      assert {:ok, conn} = device_connect(ctx, "a1", :http2)
      assert {:ok, conn, %{status: 200} = me} = request(conn, "GET", "/v2/me")
      assert {:ok, conn, %{status: 200} = put} = register_push(conn, "ab", 1, push_key(1))
      assert {:ok, conn, %{status: 200} = settings} = request(conn, "GET", "/v2/settings")

      assert {:ok, origin} =
               DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(ctx.certs, "a2"))

      assert :ok = Relay.setup_sender(relay, origin, "app:push-match")

      frame = %{
        "v" => 1,
        "type" => "forward",
        "op_id" => push_key(2),
        "message" => %{
          "sender_id" => "app:push-match",
          "message_id" => "push-match",
          "title" => "A push to match",
          "created_at" => 1
        }
      }

      assert {:ok, line} = Relay.request_line(relay, origin, JSON.encode!(frame) <> "\n")
      assert {:ok, %{"type" => "ok"}} = JSON.decode_line(line)
      now = System.system_time(:millisecond) + 1_000
      assert {:ok, claim} = Relay.claim_push(relay, now)

      assert %{"server" => server, "locator" => locator} =
               APNS.request_payload(claim)["attention"]

      assert %{"person_id" => person, "device_id" => device, "binding_id" => binding} = locator

      assert %{
               "server" => %{"id" => ^server},
               "principal" => %{"person_id" => ^person, "device_id" => ^device}
             } = json(me)

      assert json(put)["settings"]["push"]["binding_id"] == binding
      assert json(settings)["settings"]["push"]["binding_id"] == binding

      for {cert, expected_person} <- [{"a2", person}, {"b1", "B"}] do
        assert {:ok, other} = device_connect(ctx, cert, :http2)
        assert {:ok, other, %{status: 200} = other_me} = request(other, "GET", "/v2/me")

        assert %{"person_id" => ^expected_person, "device_id" => other_device} =
                 json(other_me)["principal"]

        refute other_device == device

        assert {:ok, other, %{status: 200} = other_settings} =
                 request(other, "GET", "/v2/settings")

        refute Map.has_key?(json(other_settings)["settings"]["push"], "binding_id")
        Mint.HTTP.close(other)
      end

      assert :ok =
               Relay.finish_push(
                 relay,
                 claim,
                 %{state: "endpoint_invalid", invalidated_at_ms: now},
                 now
               )

      assert {:ok, conn, invalid} = request(conn, "GET", "/v2/settings")

      assert %{"state" => "endpoint_invalid"} = invalid_push = json(invalid)["settings"]["push"]
      refute Map.has_key?(invalid_push, "binding_id")

      assert {:ok, _conn, %{status: 200} = deleted} =
               request(conn, "DELETE", "/v2/devices/me/push")

      refute Map.has_key?(json(deleted)["settings"]["push"], "binding_id")
    end

    test "push receipts replay their original bytes after a rebind and stay device-scoped", ctx do
      key = push_key(3)
      assert {:ok, conn} = device_connect(ctx, "a1", :http2)
      assert {:ok, conn, %{status: 200} = first} = register_push(conn, "ab", 1, key)
      assert {:ok, conn, %{status: 200} = second} = register_push(conn, "cd", 2, push_key(4))
      assert {:ok, conn, %{status: 200} = replay} = register_push(conn, "ab", 1, key)
      assert replay.body == first.body
      assert header(replay, "idempotent-replayed") == ["true"]
      assert %{"binding_id" => first_binding} = json(first)["settings"]["push"]
      assert %{"binding_id" => second_binding} = json(second)["settings"]["push"]
      refute first_binding == second_binding

      assert {:ok, conn, %{status: 200} = current} = request(conn, "GET", "/v2/settings")
      assert json(current)["settings"]["push"]["binding_id"] == second_binding
      assert {:ok, other} = device_connect(ctx, "a2", :http2)
      assert {:ok, other, %{status: 409} = conflict} = register_push(other, "ab", 1, key)
      assert json(conflict)["error"] == "idempotency_conflict"
      Mint.HTTP.close(other)

      assert {:ok, conn, %{status: 200} = deleted} =
               request(conn, "DELETE", "/v2/devices/me/push", [{"idempotency-key", push_key(5)}])

      refute Map.has_key?(json(deleted)["settings"]["push"], "binding_id")
      assert {:ok, conn, %{status: 200} = _third} = register_push(conn, "ef", 4, push_key(6))

      assert {:ok, _conn, %{status: 200} = replayed_delete} =
               request(conn, "DELETE", "/v2/devices/me/push", [{"idempotency-key", push_key(5)}])

      assert replayed_delete.body == deleted.body
      assert header(replayed_delete, "idempotent-replayed") == ["true"]
    end

    test "legacy compact push receipts cannot replay a replacement binding", ctx do
      path = ["devices", "me", "push"]

      old_put = %{
        "token" => String.duplicate("ab", 32),
        "environment" => "sandbox",
        "revision" => 1
      }

      put_key = push_key(7)
      delete_key = push_key(8)
      assert {:ok, conn} = device_connect(ctx, "a1", :http2)
      assert {:ok, conn, %{status: 200}} = register_push(conn, "ab", 1, push_key(1))
      assert {:ok, conn, %{status: 200} = rebound} = register_push(conn, "cd", 2, push_key(2))

      :sys.replace_state(ctx.relay, fn state ->
        for {method, key, body} <- [{"PUT", put_key, old_put}, {"DELETE", delete_key, %{}}] do
          digest = Idempotency.digest(method, path, body)
          assert :ok = Idempotency.save(state.store, "A", key, digest, 200, %{"settings" => true})
        end

        state
      end)

      assert {:ok, conn, %{status: 409} = put} = register_push(conn, "ab", 1, put_key)

      assert {:ok, conn, %{status: 409} = delete} =
               request(conn, "DELETE", "/v2/devices/me/push", [{"idempotency-key", delete_key}])

      for refused <- [put, delete] do
        assert json(refused)["error"] == "idempotency_conflict"
        assert header(refused, "idempotent-replayed") == []
      end

      assert {:ok, _conn, current} = request(conn, "GET", "/v2/settings")
      assert current.body == rebound.body
    end
  end

  defp push_key(n), do: "00000000-0000-4000-8000-00000000000#{n}"

  defp register_push(conn, octet, revision, key) do
    body =
      JSON.encode!(%{
        "token" => String.duplicate(octet, 32),
        "environment" => "sandbox",
        "revision" => revision
      })

    request(
      conn,
      "PUT",
      "/v2/devices/me/push",
      [{"idempotency-key", key}, {"content-type", "application/json"}],
      body
    )
  end

  test "browser me identifies the person without a device or binding ID", ctx do
    ctx =
      Map.merge(
        ctx,
        web_origin(ctx, client_api_routes: [ClientAPI.Reads])
      )

    assert {:ok, conn} = browser_connect(ctx, :http1)
    headers = [{"cookie", cookie(ctx.cookie)}]
    assert {:ok, conn, %{status: 200} = me} = request(conn, "GET", "/v2/me", headers)
    assert %{"server" => %{"id" => server}, "principal" => principal} = json(me)
    assert is_binary(server) and server != ""
    assert principal == %{"person_id" => ctx.person}
    assert {:ok, _conn, %{status: 200} = settings} = request(conn, "GET", "/v2/settings", headers)
    refute Map.has_key?(json(settings)["settings"]["push"], "binding_id")
  end

  test "the web origin serves /v2 over HTTP/2 and HTTP/1.1 with no flag option", ctx do
    ctx = Map.merge(ctx, web_origin(ctx, []))
    headers = [{"cookie", cookie(ctx.cookie)}]

    for protocol <- [:http2, :http1] do
      assert {:ok, conn} = browser_connect(ctx, protocol)
      assert Mint.HTTP.protocol(conn) == protocol
      assert {:ok, conn, response} = request(conn, "GET", "/v2/whoami", headers)
      assert response.status == 200
      assert json(response)["person"] == ctx.person
      assert [_] = header(response, "request-id")
      assert {:ok, _conn, %{status: 200}} = request(conn, "GET", "/sign-in")
    end
  end

  test "the device port serves /v2 only, with no /relay WebSocket", ctx do
    server = start_supervised!({Server, server_opts(ctx)})

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
    # The reads component serves /v2/inbox on the device port.
    assert response.status == 200
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

  describe "Seen on Progress revisions" do
    @describetag :progress_seen

    setup ctx do
      ctx =
        Map.merge(
          ctx,
          web_origin(ctx, client_api_routes: [ClientAPI.Reads, ClientAPI.Writes, ClientAPI.Feed])
        )

      listener =
        start_supervised!(
          {ClientAPI.Listener,
           relay: ctx.relay,
           certfile: Path.join(ctx.certs, "server.pem"),
           keyfile: Path.join(ctx.certs, "server.key"),
           cacertfile: Path.join(ctx.certs, "ca.pem")}
        )

      {:ok, origin} =
        DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(ctx.certs, "a2"))

      assert :ok = Relay.setup_sender(ctx.relay, origin, "app:test")

      assert {:ok, _} =
               Relay.setup_device_certificate(
                 ctx.relay,
                 ctx.person,
                 TLSFixtures.certificate_der!(ctx.certs, "b1")
               )

      Map.put(ctx, :port, ClientAPI.Listener.port(listener))
    end

    for surface <- [:device, :browser, :agent] do
      @tag surface: surface
      test "#{surface} reads and streams keep Seen for progress-only revisions", ctx do
        message = progress_message()
        id = send_progress(ctx, message)
        surface = ctx.surface

        {path, _changes, kind} = progress_paths(surface, id)
        assert %{status: 200} = before = progress_request(ctx, surface, "GET", path)
        assert json(before)[kind]["seen"] == false

        updates = [
          {Map.put(message, "progress", %{"completed" => 1}), false},
          {Map.put(message, "progress", %{"completed" => 2}), true},
          {Map.put(message, "progress", %{"completed" => 2}), true},
          {Map.merge(message, %{"created_at" => 2, "progress" => %{"completed" => 3}}), true},
          {Map.merge(message, %{
             "created_at" => 3,
             "title" => "Updated",
             "progress" => %{"completed" => 4}
           }), false},
          {Map.merge(message, %{
             "created_at" => 4,
             "title" => "Updated",
             "progress" => %{"completed" => 5}
           }), false}
        ]

        for {{update, seen}, revision} <- Enum.with_index(updates, 1) do
          if revision == 2 do
            seen_by = if surface == :browser, do: :browser, else: :device
            assert %{status: 200} = opened = mark_progress_seen(ctx, seen_by, id)
            assert json(opened)["item"]["seen"]
          end

          assert_progress_change(ctx, surface, id, revision + 1, seen, fn ->
            assert %{status: 200} = revised = revise_progress(ctx, id, revision, update)
            assert json(revised)["message"]["revision"] == revision + 1
          end)
        end
      end
    end

    # Opt in with paths to checksum-verified releases or a local Agent build.
    for binary <-
          String.split(System.get_env("HARKLIGHT_PROGRESS_AGENT_BINARIES", ""), ":", trim: true),
        surface <- [:device, :browser, :agent] do
      @tag agent_binary: binary, surface: surface
      test "#{surface} reads and streams keep Seen with #{Path.basename(Path.dirname(binary))}",
           %{agent_binary: binary, surface: surface} = ctx do
        run = progress_agent(ctx, binary)
        file = Path.join(ctx.dir, "fixture.txt")
        File.write!(file, "Progress attachment")

        assert %{"item_id" => id, "session_handle" => session} =
                 run.(
                   ~w(send --session new --sender-id app:test --title Working --body Status
                          --display-name Builder --progress 10 --progress-stage Initial --file) ++
                     [file]
                 )

        assert %{status: 200} = opened = mark_progress_seen(ctx, :device, id)
        assert json(opened)["item"]["seen"]
        before = stored_progress(ctx)

        assert_progress_change(ctx, surface, id, 2, true, fn ->
          assert %{"op_id" => op, "response" => %{"message" => %{"revision" => 2}}} =
                   run.(
                     ~w(progress --session) ++
                       [session, "--request", id] ++
                       ~w(--percent 25 --stage Testing --note Passed)
                   )

          wire = progress_agent_wire(ctx, op)

          assert %{"if_match" => "1", "method" => "put", "path" => ["agent", "messages", ^id]} =
                   wire

          assert %{"message" => message} = body = json(%{body: wire["body"]})
          assert map_size(body) == 1
          assert message["created_at"] > before.message["created_at"]
          assert %{"percent" => 25, "stage" => "Testing"} = message["progress"]

          assert Map.drop(message, ~w(created_at progress)) ==
                   Map.drop(before.message, ~w(created_at progress sender_id message_id))

          after_update = stored_progress(ctx)
          assert after_update.message == Map.merge(before.message, message)
          assert before.files_by_revision[1] == after_update.files_by_revision[2]
          assert [%{}] = after_update.files_by_revision[2]
        end)
      end
    end

    test "a correlated progress revision clears Seen and acts on the reply", ctx do
      message = progress_message()
      id = send_progress(ctx, message)
      headers = [{"idempotency-key", progress_op()}, {"content-type", "application/json"}]

      assert %{status: 200} =
               reply =
               progress_request(
                 ctx,
                 :device,
                 "POST",
                 "/v2/items/#{id}/replies",
                 headers,
                 ~s({"text":"Please continue"})
               )

      entry = json(reply)["entry"]["entry_id"]
      assert %{status: 200} = opened = mark_progress_seen(ctx, :device, id)
      assert json(opened)["item"]["seen"]

      assert %{status: 200} =
               revised =
               revise_progress(ctx, id, 1, Map.put(message, "progress", 1), %{
                 "in_reply_to" => entry
               })

      assert %{
               "message" => %{"revision" => 2, "seen" => false},
               "entry" => %{"entry_id" => ^entry, "delivery" => %{"acted" => true}}
             } = json(revised)
    end

    test "carrying an attachment keeps Seen but replacing it with identical bytes clears Seen",
         ctx do
      message = progress_message()
      file = upload_progress_file(ctx)
      replacement = upload_progress_file(ctx)
      id = send_progress(ctx, message, %{"file_ids" => [file]})

      assert %{status: 200} = opened = mark_progress_seen(ctx, :device, id)
      assert json(opened)["item"]["seen"]

      assert %{status: 200} =
               carried = revise_progress(ctx, id, 1, Map.put(message, "progress", 1))

      assert %{"message" => %{"revision" => 2, "seen" => true}} = json(carried)

      assert %{status: 200} =
               revised =
               revise_progress(ctx, id, 2, Map.put(message, "progress", 2), %{
                 "file_ids" => [replacement]
               })

      assert %{"message" => %{"revision" => 3, "seen" => false}} = json(revised)
    end

    test "adding or removing progress, editing other content or files, and answering keep their existing effects",
         ctx do
      progress = progress_message()
      fyi = Map.delete(progress, "progress")

      file = upload_progress_file(ctx)

      updated = Map.merge(progress, %{"created_at" => 2, "progress" => 1})

      request = %{
        "message_type" => "request",
        "answer_ui" => %{
          "schema" => "attention-client.actions/1",
          "actions" => [%{"id" => "reply", "label" => "Reply", "kind" => "text"}]
        }
      }

      for {initial, revised, extra} <- [
            {fyi, updated, %{}},
            {progress, Map.delete(updated, "progress"), %{}},
            {progress, Map.put(updated, "body", "New text"), %{}},
            {progress, Map.put(updated, "display_name", "New sender name"), %{}},
            {progress, Map.put(updated, "stake", "New stakes"), %{}},
            {progress, Map.merge(updated, request), %{}},
            {progress, updated, %{"file_ids" => [file]}},
            {fyi, Map.put(fyi, "created_at", 2), %{}},
            {progress, Map.put(progress, "created_at", 2), %{}},
            {Map.put(progress, "progress", 1),
             Map.put(updated, "progress", %JSON.DecimalToken{raw: "1.0"}), %{}}
          ] do
        id = send_progress(ctx, initial)
        path = "/v2/items/#{id}"
        assert %{status: 200} = seen = mark_progress_seen(ctx, :device, id)
        assert json(seen)["item"]["seen"]
        assert %{status: 200} = revise_progress(ctx, id, 1, revised, extra)
        assert %{status: 200} = read = progress_request(ctx, :device, "GET", path)
        assert json(read)["item"]["seen"] == false
      end

      id = send_progress(ctx, progress)
      headers = [{"idempotency-key", progress_op()}, {"content-type", "application/json"}]

      assert %{status: 200} =
               progress_request(
                 ctx,
                 :device,
                 "POST",
                 "/v2/items/#{id}/answer",
                 headers,
                 ~s({"request_revision":1,"choice":"seen"})
               )

      assert %{status: 409} =
               refused = revise_progress(ctx, id, 1, Map.put(progress, "progress", 1))

      assert json(refused)["error"] == "already_closed"
    end
  end

  defp progress_paths(:agent, id),
    do: {"/v2/agent/messages/#{id}", "/v2/agent/changes", "message"}

  defp progress_paths(_, id), do: {"/v2/items/#{id}", "/v2/changes", "item"}

  defp assert_progress_change(ctx, surface, id, revision, seen, update) do
    {path, changes, kind} = progress_paths(surface, id)
    assert %{status: 200} = initial = progress_request(ctx, surface, "GET", changes)
    cursor = json(initial)["rev"]
    after_cursor = changes <> "?after=#{cursor}"

    live = open_progress_feed(ctx, surface, after_cursor)
    update.()

    assert %{status: 200} = batch = progress_request(ctx, surface, "GET", after_cursor)
    assert %{"changes" => rows, "more" => false} = json(batch)

    assert Enum.any?(rows, fn row ->
             match?(
               %{
                 "kind" => "entry",
                 "data" => %{
                   "kind" => "revision",
                   "item_id" => ^id,
                   "request_revision" => ^revision
                 }
               },
               row
             )
           end)

    assert %{"rev" => rev, "data" => projected} =
             Enum.find(rows, &(&1["kind"] == kind and &1["data"]["item_id"] == id))

    for {conn, feed} <- [live, open_progress_feed(ctx, surface, after_cursor)] do
      {conn, event} = progress_change_at(conn, feed, rev)
      assert %{event: ^kind, id: ^rev, data: ^projected} = event
      Mint.HTTP.close(conn)
    end

    assert %{status: 200} = item = progress_request(ctx, surface, "GET", path)
    assert [projected["seen"], json(item)[kind]["seen"]] == [seen, seen]

    if kind == "item" do
      assert %{status: 200} = inbox = progress_request(ctx, surface, "GET", "/v2/inbox")
      assert %{"items" => [%{"item_id" => ^id, "seen" => ^seen}]} = json(inbox)
    end
  end

  defp progress_agent(ctx, binary) do
    identity = Path.join(ctx.dir, "agent-identity")
    File.mkdir!(identity)
    File.chmod!(identity, 0o700)
    File.cp!(Path.join(ctx.certs, "a2.pem"), Path.join(identity, "identity.pem"))
    File.cp!(Path.join(ctx.certs, "a2.key"), Path.join(identity, "identity.key"))
    File.chmod!(Path.join(identity, "identity.key"), 0o600)

    ca = Path.join(ctx.certs, "ca.pem")
    ca_hash = Base.encode16(:crypto.hash(:sha256, File.read!(ca)), case: :lower)

    File.write!(
      Path.join(identity, "server.json"),
      JSON.encode!(%{
        "address" => "127.0.0.1",
        "name" => "relay.test",
        "port" => ctx.port,
        "ca_sha256" => ca_hash
      })
    )

    File.write!(Path.join(identity, "enrollment.json"), ~s({"device_id":"fixture-agent"}))

    args = [
      "--identity-dir",
      identity,
      "--ca-file",
      ca,
      "--device-port",
      "#{ctx.port}",
      "--state-dir",
      Path.join(ctx.dir, "agent-state")
    ]

    fn command ->
      assert {output, 0} =
               System.cmd(binary, args ++ command,
                 env: [{"NO_PROXY", "*"}, {"no_proxy", "*"}],
                 stderr_to_stdout: true
               )

      json(%{body: output})
    end
  end

  defp progress_agent_wire(ctx, op) do
    alias Exqlite.Sqlite3
    [path] = Path.wildcard(Path.join(ctx.dir, "agent-state/*/origin-feed.sqlite3"))
    assert {:ok, db} = Sqlite3.open(path)
    on_exit(fn -> Sqlite3.close(db) end)

    assert {:ok, statement} =
             Sqlite3.prepare(db, "SELECT payload FROM operations WHERE op_id = ?")

    assert :ok = Sqlite3.bind(statement, [op])
    assert {:row, [payload]} = Sqlite3.step(db, statement)
    :ok = Sqlite3.release(db, statement)
    json(%{body: payload})["prepared_request"]
  end

  defp stored_progress(ctx) do
    [record] = Map.values(:sys.get_state(ctx.relay).store.engine.messages)
    record
  end

  defp progress_message do
    %{
      "title" => "Working",
      "created_at" => 1,
      "message_type" => "fyi",
      "progress" => nil,
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
      }
    }
  end

  defp upload_progress_file(ctx) do
    file_headers = [
      {"idempotency-key", progress_op()},
      {"content-type", "text/plain"},
      {"content-length", "4"},
      {"content-disposition", "attachment; filename*=UTF-8''fixture.txt"}
    ]

    assert %{status: 201} =
             upload =
             progress_request(ctx, :agent, "POST", "/v2/files", file_headers, "file")

    json(upload)["file_id"]
  end

  defp send_progress(ctx, message, extra \\ %{}) do
    body = %{
      "sender" => "app:test",
      "session" => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      "message" => message
    }

    assert %{status: 200} =
             sent =
             progress_write(ctx, "POST", "/v2/agent/messages", Map.merge(body, extra), [])

    json(sent)["message"]["item_id"]
  end

  defp revise_progress(ctx, id, revision, message, extra \\ %{}) do
    progress_write(ctx, "PUT", "/v2/agent/messages/#{id}", Map.put(extra, "message", message), [
      {"if-match", Integer.to_string(revision)}
    ])
  end

  defp progress_write(ctx, method, path, body, headers) do
    headers = [{"idempotency-key", progress_op()}, {"content-type", "application/json"} | headers]
    progress_request(ctx, :agent, method, path, headers, JSON.encode!(body))
  end

  defp mark_progress_seen(ctx, surface, id) do
    headers = [
      {"content-type", "application/json"},
      {"idempotency-key", progress_op()},
      {"origin", ctx.origin}
    ]

    progress_request(ctx, surface, "POST", "/v2/items/#{id}/seen", headers)
  end

  defp progress_request(ctx, surface, method, path, headers \\ [], body \\ nil) do
    {conn, auth} = progress_connection(ctx, surface)
    assert {:ok, conn, response} = request(conn, method, path, auth ++ headers, body)
    Mint.HTTP.close(conn)
    response
  end

  defp progress_connection(ctx, :browser),
    do: {ClientAPIFeedClient.connect(ctx.web_port, ctx.certs), [{"cookie", cookie(ctx.cookie)}]}

  defp progress_connection(ctx, :device),
    do: {ClientAPIFeedClient.connect(ctx.port, ctx.certs, "b1"), []}

  defp progress_connection(ctx, :agent),
    do: {ClientAPIFeedClient.connect(ctx.port, ctx.certs, "a2"), []}

  defp open_progress_feed(ctx, surface, path) do
    {conn, headers} = progress_connection(ctx, surface)

    assert {:ok, conn, ref} =
             Mint.HTTP.request(
               conn,
               "GET",
               path,
               [{"accept", "text/event-stream"} | headers],
               nil
             )

    {conn, feed} =
      ClientAPIFeedClient.await_open(conn, %{
        ref: ref,
        buffer: "",
        status: nil,
        headers: [],
        done: false
      })

    assert feed.status == 200
    {conn, feed}
  end

  defp progress_change_at(conn, feed, rev) do
    case ClientAPIFeedClient.events(conn, feed, 1) do
      {conn, _feed, [%{id: id} = event]} when id >= rev -> {conn, event}
      {conn, feed, [_]} -> progress_change_at(conn, feed, rev)
    end
  end

  defp progress_op, do: AttentionServer.ConversationFiles.new_upload_id()

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
         client_api_routes: Keyword.get(extra, :client_api_routes, [TestRoutes])
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

  defp request(conn, method, path, headers \\ [], body \\ nil) do
    case Mint.HTTP.request(
           conn,
           method,
           path,
           headers,
           body || if(method in ~w(POST PUT), do: "{}")
         ) do
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
