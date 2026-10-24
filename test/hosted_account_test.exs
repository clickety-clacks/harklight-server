defmodule AttentionServer.HostedAccountTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    DashboardReadHTTP,
    DeviceIdentity,
    Engine,
    GuidedJoin,
    JSON,
    Relay,
    Server,
    Store,
    TLSFixtures
  }

  alias Exqlite.Sqlite3

  @origin "https://app.example.test"
  @test_account "acct-test"

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup do
    dir = Path.join(System.tmp_dir!(), "harklight-hosted-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  defp start_relay(certs, dir, opts) do
    {:ok, join} =
      GuidedJoin.configuration("relay.test", 18444, 18443, File.read!(Path.join(certs, "ca.pem")))

    {:ok, relay} =
      Relay.start_link(
        [store_path: Path.join(dir, "relay.sqlite3"), join_configuration: join] ++ opts
      )

    relay
  end

  describe "hosted accounts in the relay" do
    test "an account maps to one person whose server-held owner key reaches only that person",
         %{certs: certs, dir: dir} do
      relay = start_relay(certs, dir, dashboard_origin: @origin)

      {:ok, self_hosted} =
        Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))

      {:ok, self_hosted_key} =
        DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "a1"))

      assert {:ok, alice} = Relay.hosted_account_create(relay, "acct-alice")
      assert {:ok, ^alice} = Relay.hosted_account_create(relay, "acct-alice")
      assert {:ok, bob} = Relay.hosted_account_create(relay, "acct-bob")
      assert bob != alice
      assert AttentionServer.Message.uuid_v4?(alice)
      assert alice != bob
      assert {:error, :invalid_account_ref} = Relay.hosted_account_create(relay, "")
      assert {:error, :invalid_account_ref} = Relay.hosted_account_create(relay, "has space")

      # A hosted sign-in names a hosted person only; a self-hosted person is out of reach.
      assert {:error, :not_hosted} = Relay.hosted_sign_in_create(relay, self_hosted, "Mallory")
      assert {:error, :invalid_label} = Relay.hosted_sign_in_create(relay, alice, "")
      assert {:error, :invalid_label} = Relay.hosted_sign_in_create(relay, alice, nil)

      assert {:ok, url} = Relay.hosted_sign_in_create(relay, alice, "Alice")
      assert String.starts_with?(url, @origin <> "/sign-in#hbs1_")
      token = url |> String.split("#", parts: 2) |> List.last()

      assert {:ok, principal} = Relay.consume_browser_sign_in(relay, token)
      assert principal.person_id == alice
      assert principal.display_name == "Alice"
      assert principal.scope == :person_owner
      assert {:error, _} = Relay.consume_browser_sign_in(relay, token)

      owner_key = principal.owner_key
      assert owner_key != self_hosted_key

      # The key reaches Alice's own view and nobody else's.
      assert {:ok, %{person_name: ^alice, devices: [], waiting: []}} =
               Relay.dashboard_inventory(relay, alice, owner_key, nil)

      assert {:error, :forbidden} = Relay.dashboard_inventory(relay, self_hosted, owner_key, nil)
      assert {:error, :forbidden} = Relay.dashboard_inventory(relay, bob, owner_key, nil)
      assert {:error, _} = Relay.owner_invite_create(relay, self_hosted, owner_key)

      # The self-hosted owner's own view and links are unchanged.
      assert {:ok, credential} =
               Relay.owner_browser_sign_in_create(relay, self_hosted, self_hosted_key)

      assert {:ok, own} = Relay.consume_browser_sign_in(relay, credential["sign_in_token"])
      refute Map.has_key?(own, :display_name)

      # A later label replaces the earlier one.
      assert {:ok, url} = Relay.hosted_sign_in_create(relay, alice, "Alice B.")
      token = url |> String.split("#", parts: 2) |> List.last()
      assert {:ok, %{display_name: "Alice B."}} = Relay.consume_browser_sign_in(relay, token)

      # A hosted invitation carries the label for the join page; a self-hosted one does not.
      assert {:ok, invitation} = Relay.owner_invite_create(relay, alice, owner_key)
      assert {:ok, resolved} = Relay.invitation_resolve(relay, invitation["invite_token"])
      assert resolved["account_label"] == "Alice B."
      assert resolved["person_id"] == alice

      assert {:ok, invitation} = Relay.owner_invite_create(relay, self_hosted, self_hosted_key)
      assert {:ok, resolved} = Relay.invitation_resolve(relay, invitation["invite_token"])
      refute Map.has_key?(resolved, "account_label")
    end

    test "the sign-in link needs a public origin", %{certs: certs, dir: dir} do
      relay = start_relay(certs, dir, [])
      assert {:ok, alice} = Relay.hosted_account_create(relay, "acct-alice")
      assert {:error, :no_public_origin} = Relay.hosted_sign_in_create(relay, alice, "Alice")
    end

    test "hosted accounts survive a restart", %{certs: certs, dir: dir} do
      relay = start_relay(certs, dir, dashboard_origin: @origin)
      assert {:ok, alice} = Relay.hosted_account_create(relay, "acct-alice")
      assert {:ok, _} = Relay.hosted_sign_in_create(relay, alice, "Alice")
      GenServer.stop(relay)

      relay = start_relay(certs, dir, dashboard_origin: @origin)
      assert {:ok, ^alice} = Relay.hosted_account_create(relay, "acct-alice")
      assert {:ok, url} = Relay.hosted_sign_in_create(relay, alice, "Alice")
      token = url |> String.split("#", parts: 2) |> List.last()

      assert {:ok, %{person_id: ^alice, display_name: "Alice"}} =
               Relay.consume_browser_sign_in(relay, token)
    end
  end

  describe "test account reset" do
    test "acts only on the designated account and reaches real product states",
         %{certs: certs, dir: dir} do
      relay =
        start_relay(certs, dir, dashboard_origin: @origin, hosted_test_account: @test_account)

      assert {:error, :unknown_state} = Relay.hosted_test_reset(relay, "everything")
      assert {:error, :not_created} = Relay.hosted_test_reset(relay, "no-devices")

      assert {:ok, alice} = Relay.hosted_account_create(relay, "acct-alice")
      assert {:ok, _} = Relay.hosted_sign_in_create(relay, alice, "Alice")
      alice_device = String.duplicate("a", 64)
      :ok = Relay.setup_device(relay, alice, alice_device)

      assert {:ok, tester} = Relay.hosted_account_create(relay, @test_account)
      assert tester != alice

      # The test account is always shown as "Test account", whatever label arrives.
      assert {:ok, url} = Relay.hosted_sign_in_create(relay, tester, "Whatever")
      token = url |> String.split("#", parts: 2) |> List.last()
      assert {:ok, principal} = Relay.consume_browser_sign_in(relay, token)
      assert principal.display_name == "Test account"
      owner_key = principal.owner_key

      assert {:ok, %{state: "device-waiting", person_id: ^tester, code: code}} =
               Relay.hosted_test_reset(relay, "device-waiting")

      assert {:ok, %{devices: [], waiting: [waiting]}} =
               Relay.dashboard_inventory(relay, tester, owner_key, nil)

      assert waiting.label == "Test device"
      assert waiting.code == code
      assert waiting.person_name == tester

      assert {:ok, %{state: "device-enrolled", person_id: ^tester}} =
               Relay.hosted_test_reset(relay, "device-enrolled")

      assert {:ok, %{devices: [device], waiting: []}} =
               Relay.dashboard_inventory(relay, tester, owner_key, nil)

      assert device.label == "Test device"
      refute device.revoked
      assert device.connection == :unknown

      assert {:ok, %{state: "no-devices", person_id: ^tester}} =
               Relay.hosted_test_reset(relay, "no-devices")

      assert {:ok, %{devices: [], waiting: []}} =
               Relay.dashboard_inventory(relay, tester, owner_key, nil)

      # The reset never touched anyone else, and the test person's sign-in still works.
      assert {:ok, %{devices: [%{device_id: ^alice_device}]}} =
               Relay.dashboard_inventory(
                 relay,
                 alice,
                 Engine.hosted_owner_key(relay_engine(relay), alice),
                 nil
               )

      assert {:ok, url} = Relay.hosted_sign_in_create(relay, tester, "Test account")
      token = url |> String.split("#", parts: 2) |> List.last()
      assert {:ok, %{person_id: ^tester}} = Relay.consume_browser_sign_in(relay, token)
    end

    test "refuses when no test account is designated", %{certs: certs, dir: dir} do
      relay = start_relay(certs, dir, dashboard_origin: @origin)
      assert {:ok, _} = Relay.hosted_account_create(relay, @test_account)
      assert {:error, :not_designated} = Relay.hosted_test_reset(relay, "no-devices")
    end

    test "the designation is durable on the account record", %{certs: certs, dir: dir} do
      relay =
        start_relay(certs, dir, dashboard_origin: @origin, hosted_test_account: @test_account)

      assert {:ok, tester} = Relay.hosted_account_create(relay, @test_account)
      assert {:ok, _} = Relay.hosted_test_reset(relay, "device-waiting")
      GenServer.stop(relay)

      {:ok, store} = Store.open(Path.join(dir, "relay.sqlite3"))

      assert %{person: ^tester, test: true, label: "Test account"} =
               store.engine.hosted_accounts[@test_account]

      assert :ok = Store.close(store)
    end
  end

  defp relay_engine(relay), do: :sys.get_state(relay).store.engine

  describe "store format" do
    test "earlier state gains an empty hosted account table", %{dir: dir} do
      path = Path.join(dir, "relay.sqlite3")
      {:ok, initial} = Store.open(path)
      assert :ok = Store.close(initial)

      {:ok, engine} = Engine.setup_person(initial.engine, "A")
      old_engine = Map.delete(engine, :hosted_accounts)

      payload =
        :erlang.term_to_binary({:attention_relay_state, 21, old_engine}, [{:compressed, 6}])

      assert {:ok, db} = Sqlite3.open(path)
      assert {:ok, stmt} = Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ?")
      assert :ok = Sqlite3.bind(stmt, [{:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}])
      assert :done = Sqlite3.step(db, stmt)
      assert :ok = Sqlite3.release(db, stmt)
      assert :ok = Sqlite3.close(db)

      assert {:ok, migrated} = Store.open(path)
      assert migrated.engine.hosted_accounts == %{}
      assert migrated.engine.persons == %{"A" => 0}
      assert :ok = Store.close(migrated)
    end
  end

  describe "hosted listener" do
    setup %{certs: certs, dir: dir} do
      {:ok, server} =
        Server.start_link(
          store_path: Path.join(dir, "state.sqlite3"),
          certfile: Path.join(certs, "server.pem"),
          keyfile: Path.join(certs, "server.key"),
          cacertfile: Path.join(certs, "ca.pem"),
          deployment_mode: :saas,
          upload_cap: 1_000_000,
          public_base_url: @origin <> "/",
          hosted_port: 0,
          hosted_test_account: @test_account,
          relay_name: :"hosted_http_#{System.unique_integer([:positive])}"
        )

      Process.unlink(server)
      on_exit(fn -> if Process.alive?(server), do: Supervisor.stop(server) end)
      %{server: server, port: Server.hosted_port(server)}
    end

    test "creates accounts, issues sign-in links and resets the test account", %{
      port: port,
      server: server
    } do
      assert {200, %{"person_id" => alice}} =
               post(port, "/accounts", %{"account_ref" => "acct-alice"})

      assert {200, %{"person_id" => ^alice}} =
               post(port, "/accounts", %{"account_ref" => "acct-alice"})

      assert {400, %{"error" => "invalid_account_ref"}} = post(port, "/accounts", %{})
      assert {400, %{"error" => "invalid_json"}} = raw_post(port, "/accounts", "not json")

      assert {200, %{"sign_in_url" => url}} =
               post(port, "/sign-in-links", %{"person_id" => alice, "label" => "Alice"})

      assert String.starts_with?(url, @origin <> "/sign-in#hbs1_")
      token = url |> String.split("#", parts: 2) |> List.last()
      relay = Server.relay(server)

      assert {:ok, %{person_id: ^alice, display_name: "Alice"}} =
               Relay.consume_browser_sign_in(relay, token)

      assert {404, %{"error" => "unknown_person"}} =
               post(port, "/sign-in-links", %{"person_id" => "nobody", "label" => "X"})

      assert {400, %{"error" => "invalid_label"}} =
               post(port, "/sign-in-links", %{"person_id" => alice, "label" => ""})

      assert {400, %{"error" => "unknown_state"}} = post(port, "/test-reset", %{"state" => "all"})

      assert {404, %{"error" => "not_created"}} =
               post(port, "/test-reset", %{"state" => "no-devices"})

      assert {200, %{"person_id" => tester}} =
               post(port, "/accounts", %{"account_ref" => @test_account})

      assert {200, %{"state" => "no-devices", "person_id" => ^tester}} =
               post(port, "/test-reset", %{"state" => "no-devices"})

      assert {405, _} =
               request(
                 port,
                 "GET /accounts HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
               )

      assert {404, _} = post(port, "/other", %{})
    end

    test "the public base address, not the listener's own, is the dashboard origin", %{
      server: server
    } do
      assert Relay.dashboard_origin(Server.relay(server)) == @origin
    end
  end

  describe "server configuration" do
    test "rejects a hosted listener outside saas mode and a bad public address", %{
      certs: certs,
      dir: dir
    } do
      base = [
        store_path: Path.join(dir, "state.sqlite3"),
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem")
      ]

      assert_raise ArgumentError, ~r/:saas/, fn ->
        Server.init(base ++ [hosted_port: 0, deployment_mode: :self_hosted])
      end

      assert_raise ArgumentError, ~r/public_base_url/, fn ->
        Server.init(base ++ [public_base_url: "http://app.example.test"])
      end

      assert_raise ArgumentError, ~r/public_base_url/, fn ->
        Server.init(base ++ [public_base_url: "https://app.example.test/account/"])
      end

      assert_raise ArgumentError, ~r/public_base_url/, fn ->
        Server.init(base ++ [public_base_url: "https://user@app.example.test"])
      end
    end

    test "hosted addresses are configuration for saas mode only", %{certs: certs, dir: dir} do
      base = [
        store_path: Path.join(dir, "state.sqlite3"),
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem")
      ]

      assert_raise ArgumentError, ~r/:saas/, fn ->
        Server.init(base ++ [hosted_sign_in_url: "/sign-in"])
      end

      saas = base ++ [deployment_mode: :saas, public_base_url: @origin]

      assert_raise ArgumentError, ~r/hosted address/, fn ->
        Server.init(saas ++ [hosted_sign_out_url: "http://app.example.test/sign-out"])
      end

      assert_raise ArgumentError, ~r/hosted address/, fn ->
        Server.init(saas ++ [hosted_account_url: "account"])
      end

      assert_raise ArgumentError, ~r/hosted address/, fn ->
        Server.init(base ++ [deployment_mode: :saas, hosted_account_url: "/account"])
      end
    end
  end

  defmodule Backend do
    @alice "00000000-0000-4000-8000-00000000a11c"

    def resolve_session("hosted-session"), do: {:ok, %{person_id: @alice, display_name: "Alice"}}
    def resolve_session("plain-session"), do: {:ok, %{person_id: @alice}}
    def resolve_session(_), do: {:error, :invalid_session}

    def end_session(cookie) do
      send(self(), {:session_ended, cookie})
      :ok
    end

    def list_devices(%{person_id: @alice} = principal) do
      name = Map.get(principal, :display_name, @alice)

      {:ok,
       %{
         person_name: name,
         devices: [%{label: "Phone", person_name: name, revoked: false}],
         waiting: []
       }}
    end

    def create_invitation({%{person_id: @alice}, _request_id}), do: {:error, :forbidden}

    def resolve_invitation(_token) do
      {:ok,
       %{
         "person_id" => @alice,
         "account_label" => "Alice",
         "server_name" => "relay.test",
         "expires_at" => 1,
         "join_url" => "harklight://join#" <> String.duplicate("f", 64)
       }}
    end
  end

  describe "hosted dashboard wording and origin" do
    setup %{dir: dir} do
      skill = Path.join(dir, "SKILL.md")
      File.write!(skill, "# Operate Harklight\n")

      %{
        hosted:
          DashboardReadHTTP.init(
            backend: Backend,
            skill_path: skill,
            skill_path_send_listen: skill,
            hosted: true,
            expected_origin: @origin
          ),
        addressed:
          DashboardReadHTTP.init(
            backend: Backend,
            skill_path: skill,
            skill_path_send_listen: skill,
            hosted: true,
            expected_origin: @origin,
            hosted_addresses: %{
              sign_in: @origin <> "/sign-in",
              sign_out: @origin <> "/signed-out",
              account: @origin <> "/account"
            }
          ),
        plain:
          DashboardReadHTTP.init(
            backend: Backend,
            skill_path: skill,
            skill_path_send_listen: skill
          )
      }
    end

    test "a hosted browser without a session is sent to the hosted sign-in address", %{
      addressed: addressed,
      hosted: hosted
    } do
      for session <- [nil, "stale-session"] do
        conn = "/devices" |> secure_get(session) |> DashboardReadHTTP.call(addressed)
        assert conn.status == 303
        assert Plug.Conn.get_resp_header(conn, "location") == [@origin <> "/sign-in"]
        assert %{"__Host-harklight" => %{max_age: 0}} = conn.resp_cookies
      end

      # Without configured addresses the hosted dashboard answers as before.
      conn = "/devices" |> secure_get() |> DashboardReadHTTP.call(hosted)
      assert conn.status == 401
    end

    test "signing out ends the session and sends the browser to the sign-out address", %{
      addressed: addressed,
      hosted: hosted
    } do
      conn = sign_out_post(addressed, @origin, "hosted-session")
      assert conn.status == 303
      assert Plug.Conn.get_resp_header(conn, "location") == [@origin <> "/signed-out"]
      assert %{"__Host-harklight" => %{max_age: 0}} = conn.resp_cookies
      assert_received {:session_ended, "hosted-session"}

      refused = sign_out_post(addressed, "https://www.example.com", "hosted-session")
      assert refused.status == 403
      refute_received {:session_ended, _}

      # Only the sign-in address configured: sign-out lands there too.
      sign_in_only =
        DashboardReadHTTP.init(
          backend: Backend,
          skill_path: addressed.skill_path,
          skill_path_send_listen: addressed.skill_path_send_listen,
          hosted: true,
          expected_origin: @origin,
          hosted_addresses: %{sign_in: @origin <> "/sign-in"}
        )

      conn = sign_out_post(sign_in_only, @origin, "hosted-session")
      assert Plug.Conn.get_resp_header(conn, "location") == [@origin <> "/sign-in"]

      # Hosted without addresses still signs out, back to its own devices page.
      conn = sign_out_post(hosted, @origin, "hosted-session")
      assert conn.status == 303
      assert Plug.Conn.get_resp_header(conn, "location") == ["/devices"]
    end

    test "hosted pages carry the Account link and Sign out; self-hosted output does not", %{
      addressed: addressed,
      hosted: hosted,
      plain: plain
    } do
      page = "/devices" |> secure_get("hosted-session") |> DashboardReadHTTP.call(addressed)
      assert page.status == 200
      assert page.resp_body =~ ~s(<a href="#{@origin}/account">Account</a>)
      assert page.resp_body =~ ~s(<form action="/sign-out" method="post">)
      assert page.resp_body =~ "Sign out"

      # The account link is the hosted side's; without an address none is rendered.
      page = "/devices" |> secure_get("hosted-session") |> DashboardReadHTTP.call(hosted)
      refute page.resp_body =~ "Account</a>"
      assert page.resp_body =~ "Sign out"

      # Self-hosted: no redirect, no sign-out route, no hosted chrome.
      assert ("/devices" |> secure_get() |> DashboardReadHTTP.call(plain)).status == 401
      own = "/devices" |> secure_get("plain-session") |> DashboardReadHTTP.call(plain)
      assert own.status == 200
      refute own.resp_body =~ "Sign out"
      refute own.resp_body =~ "Account</a>"
      refute own.resp_body =~ "/sign-out"
      assert sign_out_post(plain, "https://www.example.com", "plain-session").status == 404

      # Hosted addresses given to a self-hosted listener change nothing.
      ignored =
        DashboardReadHTTP.init(
          backend: Backend,
          skill_path: addressed.skill_path,
          skill_path_send_listen: addressed.skill_path_send_listen,
          hosted_addresses: %{sign_in: @origin <> "/sign-in", account: @origin <> "/account"}
        )

      same = "/devices" |> secure_get("plain-session") |> DashboardReadHTTP.call(ignored)
      assert without_request_id(same.resp_body) == without_request_id(own.resp_body)
      assert ("/devices" |> secure_get() |> DashboardReadHTTP.call(ignored)).status == 401
    end

    test "pages name the person by label and never send a hosted user to an owner", %{
      addressed: addressed,
      hosted: hosted,
      plain: plain
    } do
      devices = "/devices" |> secure_get("hosted-session") |> DashboardReadHTTP.call(hosted)
      assert devices.status == 200
      assert devices.resp_body =~ "Alice"
      refute devices.resp_body =~ "00000000-0000-4000-8000-00000000a11c"

      [_, request_id] = Regex.run(~r/name="request_id" value="([^"]+)"/, devices.resp_body)

      failure =
        form_post(hosted, "/devices/invitation/new", request_id, @origin, "hosted-session")

      assert failure.status == 403
      assert failure.resp_body =~ "Your access changed."
      assert failure.resp_body =~ "Alice"
      refute failure.resp_body =~ "owner"

      join = "/join" |> secure_get() |> DashboardReadHTTP.call(hosted)
      assert join.resp_body =~ "Open in Harklight"
      refute join.resp_body =~ "owner"
      refute join.resp_body =~ "Ask the"

      script = "/join.js" |> secure_get() |> DashboardReadHTTP.call(hosted)
      assert script.resp_body =~ "account_label"
      refute script.resp_body =~ "owner"
      refute script.resp_body =~ "Ask the"

      sign_in = "/sign-in.js" |> secure_get() |> DashboardReadHTTP.call(hosted)
      refute sign_in.resp_body =~ "owner"
      refute sign_in.resp_body =~ "Ask the"

      # A failed one-use link shows the way back only where a sign-in address exists.
      again = ~s(<a id="sign-in-again" href="#{@origin}/sign-in" hidden>Sign in again</a>)
      assert ("/sign-in" |> secure_get() |> DashboardReadHTTP.call(addressed)).resp_body =~ again

      refute ("/sign-in" |> secure_get() |> DashboardReadHTTP.call(hosted)).resp_body =~
               "sign-in-again"

      refute ("/sign-in" |> secure_get() |> DashboardReadHTTP.call(plain)).resp_body =~
               "sign-in-again"
    end

    test "the configured public address is the origin browsers must post from", %{
      hosted: hosted,
      plain: plain
    } do
      accepted =
        form_post(hosted, "/devices/invitation/new", request_id(), @origin, "hosted-session")

      assert accepted.resp_body =~ "Your access changed."

      refused =
        form_post(
          hosted,
          "/devices/invitation/new",
          request_id(),
          "https://www.example.com",
          "hosted-session"
        )

      assert refused.status == 403
      assert refused.resp_body =~ "could not be verified"

      resolve =
        %{
          Plug.Test.conn(:post, "/join/resolve", String.duplicate("f", 64))
          | scheme: :https,
            port: 443
        }
        |> Plug.Conn.put_req_header("origin", @origin)
        |> Plug.Conn.put_req_header("content-type", "text/plain")
        |> DashboardReadHTTP.call(hosted)

      assert resolve.status == 200

      assert JSON.decode_line(resolve.resp_body <> "\n") |> elem(1) |> Map.get("account_label") ==
               "Alice"

      # Self-hosted keeps the listener's own host as the origin and its owner wording.
      own =
        form_post(
          plain,
          "/devices/invitation/new",
          request_id(),
          "https://www.example.com",
          "plain-session"
        )

      assert own.resp_body =~ "Your owner access changed. Sign in again with a new owner link."

      foreign =
        form_post(plain, "/devices/invitation/new", request_id(), @origin, "plain-session")

      assert foreign.resp_body =~ "could not be verified"

      join = "/join" |> secure_get() |> DashboardReadHTTP.call(plain)
      assert join.resp_body =~ "The account owner must separately approve your exact device."
    end
  end

  defp request_id, do: "11111111-1111-4111-8111-111111111111"

  defp form_post(opts, path, request_id, origin, session) do
    body = URI.encode_query(%{"confirm" => "new_invitation", "request_id" => request_id})

    %{Plug.Test.conn(:post, path, body) | scheme: :https, port: 443}
    |> Plug.Conn.put_req_header("cookie", "__Host-harklight=#{session}")
    |> Plug.Conn.put_req_header("origin", origin)
    |> Plug.Conn.put_req_header("content-type", "application/x-www-form-urlencoded")
    |> DashboardReadHTTP.call(opts)
  end

  # Each render carries a fresh request identity; compare everything else.
  defp without_request_id(html),
    do: Regex.replace(~r/name="request_id" value="[^"]+"/, html, ~s(name="request_id"))

  defp sign_out_post(opts, origin, session) do
    %{Plug.Test.conn(:post, "/sign-out", "confirm=sign_out") | scheme: :https, port: 443}
    |> Plug.Conn.put_req_header("cookie", "__Host-harklight=#{session}")
    |> Plug.Conn.put_req_header("origin", origin)
    |> Plug.Conn.put_req_header("content-type", "application/x-www-form-urlencoded")
    |> DashboardReadHTTP.call(opts)
  end

  defp secure_get(path, session \\ nil) do
    conn = %{Plug.Test.conn(:get, path) | scheme: :https}

    if session do
      Plug.Conn.put_req_header(conn, "cookie", "__Host-harklight=#{session}")
    else
      conn
    end
  end

  defp post(port, path, body), do: raw_post(port, path, JSON.encode!(body))

  defp raw_post(port, path, body) do
    request(port, [
      "POST #{path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n",
      "Content-Length: #{byte_size(body)}\r\nConnection: close\r\n\r\n",
      body
    ])
  end

  defp request(port, request) do
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])
    :ok = :gen_tcp.send(socket, request)
    response = recv_all(socket, "")
    :gen_tcp.close(socket)
    [head, body] = String.split(response, "\r\n\r\n", parts: 2)
    [_, status] = Regex.run(~r/\AHTTP\/1\.1 (\d{3})/, head)

    decoded =
      case JSON.decode_line(body <> "\n") do
        {:ok, map} -> map
        _ -> body
      end

    {String.to_integer(status), decoded}
  end

  defp recv_all(socket, acc) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, chunk} -> recv_all(socket, acc <> chunk)
      {:error, :closed} -> acc
    end
  end
end
