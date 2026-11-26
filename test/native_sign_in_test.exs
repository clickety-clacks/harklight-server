defmodule AttentionServer.NativeSignInTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{BrowserAuth, DashboardBackend, DashboardBrowserSession, DashboardReadHTTP, DeviceProof, Engine, HostedRequests, JSON, Relay, Store}
  alias AttentionServer.ClientAPI.Router
  alias AttentionServer.Store.AuthSchema
  alias Exqlite.Sqlite3

  @moduletag :tmp_dir

  test "native redemption binds verifier and proof and resumes the same device after restart",
       %{tmp_dir: dir} do
    path = Path.join(dir, "relay.sqlite3")
    opts = [store_path: path, deployment_mode: :saas]
    {:ok, relay} = Relay.start_link(opts)
    {:ok, person} = Relay.hosted_account_create(relay, "native-fixture")
    :ok = GenServer.stop(relay)

    {:ok, db} = Sqlite3.open(path)
    assert {:ok, %{minimum_generation: 1}} = AuthSchema.initialize(db)
    :ok = Sqlite3.close(db)

    {:ok, relay} = Relay.start_link(opts)
    router = Router.init(relay: relay, auth: {:browser, nil, "https://example.test"})
    discovery = Plug.Test.conn(:get, "/v2/auth/methods") |> Router.call(router)
    assert discovery.status == 200
    assert json(discovery) == %{"methods" => ~w(google apple email_code)}

    verifier = BrowserAuth.new_secret()
    bearer = BrowserAuth.new_secret()

    started =
      post("/v2/auth/transactions", %{"verifier_hash" => DeviceProof.digest(verifier)})
      |> Router.call(router)

    assert started.status == 200
    assert %{"transaction" => transaction} = json(started)
    assert Plug.Conn.get_resp_header(started, "request-id") != []

    authorized =
      post("/session-grants", %{
        "person_id" => person,
        "purpose" => "app",
        "transaction" => transaction,
        "method" => "google"
      })
      |> HostedRequests.call(HostedRequests.init(relay: relay))

    assert authorized.status == 200
    assert json(authorized) == %{"transaction" => transaction}

    session_hash = DeviceProof.digest(bearer)
    {key, signature} = proof(dir, DeviceProof.sign_in_bytes(transaction, session_hash))

    request = %{
      "transaction" => transaction,
      "verifier" => verifier,
      "public_key" => key,
      "label" => "Test phone",
      "session_hash" => session_hash,
      "signature" => signature
    }

    assert post("/v2/auth/sign-in", %{request | "verifier" => BrowserAuth.new_secret()})
           |> Router.call(router)
           |> Map.fetch!(:status) == 401

    wrong_session = %{request | "session_hash" => DeviceProof.digest(BrowserAuth.new_secret())}
    refused = post("/v2/auth/sign-in", wrong_session) |> Router.call(router)
    assert refused.status == 401
    assert %{"error" => "unauthorized"} = json(refused)

    signed_in = post("/v2/auth/sign-in", request) |> Router.call(router)
    assert signed_in.status == 200
    assert %{"device_id" => device, "duplicate" => false, "person_id" => ^person, "method" => "google"} =
             json(signed_in)

    :ok = GenServer.stop(relay)
    {:ok, restarted} = Relay.start_link(opts)
    router = Router.init(relay: restarted, auth: {:browser, nil, "https://example.test"})
    replay = post("/v2/auth/sign-in", request) |> Router.call(router)
    assert replay.status == 200
    assert %{"device_id" => ^device, "duplicate" => true} = json(replay)

    request =
      Plug.Test.conn(:get, "/v2/me")
      |> Plug.Conn.put_req_header("authorization", "Bearer " <> bearer)

    assert Router.call(request, router).status == 200

    :sys.replace_state(restarted, fn state ->
      :ok = Sqlite3.execute(state.store.db, "ALTER TABLE harklight_device_sessions RENAME TO temporarily_hidden")
      state
    end)

    unavailable = Router.call(request, router)
    assert unavailable.status == 503
    assert %{"error" => "retry_later", "retryable" => true} = json(unavailable)

    :sys.replace_state(restarted, fn state ->
      :ok = Sqlite3.execute(state.store.db, "ALTER TABLE temporarily_hidden RENAME TO harklight_device_sessions")
      state
    end)

    assert Router.call(request, router).status == 200

    assert request
           |> Plug.Conn.put_req_header("cookie", "__Host-harklight=ambiguous")
           |> Router.call(router)
           |> Map.fetch!(:status) == 401

    devices =
      Plug.Test.conn(:get, "/v2/devices")
      |> Plug.Conn.put_req_header("authorization", "Bearer " <> bearer)
      |> Router.call(router)

    assert %{"devices" => [%{"device_id" => ^device, "current" => true, "signed_in_at" => time}]} =
             json(devices)
    assert is_integer(time) and time > 0

    sign_out =
      Plug.Test.conn(:post, "/v2/auth/sign-out")
      |> Plug.Conn.put_req_header("authorization", "Bearer " <> bearer)
      |> Router.call(router)

    assert sign_out.status == 204
    assert Router.call(request, router).status == 401
  end

  test "pending replacement preserves its recipient and activation permanently ends certificate access",
       %{tmp_dir: dir} do
    path = Path.join(dir, "overlap.sqlite3")
    origin = "https://phone.example.test"
    opts = [store_path: path, deployment_mode: :self_hosted, dashboard_origin: origin]
    person = "operator-person"
    device = String.duplicate("a", 64)

    {:ok, initial} = Relay.start_link(opts)
    assert :ok = Relay.setup_person(initial, person)
    assert :ok = Relay.setup_device(initial, person, device)
    :ok = GenServer.stop(initial)

    {:ok, store} = Store.open_relay(path)
    assert {:ok, %{minimum_generation: 1}} = AuthSchema.initialize(store.db, store.engine, [device])
    :ok = Store.close(store)

    {:ok, relay} = Relay.start_link(opts)
    legacy = %AttentionServer.ClientAPI.Principal{person: person, key: device, kind: :device}
    assert {:ok, ^person} = Relay.certificate_device_person(relay, device)
    assert {:ok, %{"features" => %{"auth_migration" => true}}} =
             AttentionServer.ClientAPI.Reads.me(relay, legacy)

    assert {:ok, %{"origin" => ^origin, "token" => first}} = Relay.migration_grant(relay, legacy)
    assert {:ok, %{"token" => replacement}} = Relay.migration_grant(relay, legacy)
    assert {:ok, %{"token" => unused}} = Relay.migration_grant(relay, legacy)

    bearer = BrowserAuth.new_secret()
    initial_hash = DeviceProof.digest(bearer)
    initial_request = migration_request(dir, first, initial_hash)
    assert {:ok, %{"device_id" => ^device, "duplicate" => false, "method" => nil}} =
             Relay.redeem_native_transaction(relay, initial_request)
    assert {:ok, %{pending: true}} = Relay.native_session(relay, bearer)
    assert {:ok, ^person} = Relay.certificate_device_person(relay, device)

    new_bearer = BrowserAuth.new_secret()
    new_hash = DeviceProof.digest(new_bearer)
    replacement_request = migration_request(dir, replacement, new_hash)
    unused_request = migration_request(dir, unused, DeviceProof.digest(BrowserAuth.new_secret()))
    assert {:ok, %{"device_id" => ^device, "duplicate" => false}} =
             Relay.redeem_native_transaction(relay, replacement_request)
    assert {:error, :unauthorized} = Relay.native_session(relay, bearer)
    assert {:ok, %{"device_id" => ^device, "duplicate" => true}} =
             Relay.redeem_native_transaction(relay, initial_request)

    router = Router.init(relay: relay, auth: {:browser, nil, origin})
    authenticated = fn conn -> Plug.Conn.put_req_header(conn, "authorization", "Bearer " <> new_bearer) end
    assert post("/v2/auth/sign-out", %{}) |> authenticated.() |> Router.call(router) |> Map.fetch!(:status) == 403

    signature = sign(dir, DeviceProof.activation_bytes(new_hash), replacement)
    complete = post("/v2/auth/migration-complete", %{"signature" => signature}) |> authenticated.()
    assert Router.call(complete, router).status == 204
    assert Router.call(complete, router).status == 204
    assert {:ok, %{pending: false}} = Relay.native_session(relay, new_bearer)
    assert {:error, :unauthorized} = Relay.certificate_device_person(relay, device)
    assert {:error, :already_migrated} = Relay.redeem_native_transaction(relay, unused_request)
  end

  test "late RSA proof replaces the certificate of an off-roster recipient with sender grants",
       %{tmp_dir: dir} do
    origin = "https://local.example.test"
    path = Path.join(dir, "local.sqlite3")
    opts = [store_path: path, deployment_mode: :self_hosted, dashboard_origin: origin]
    person = "00000000-0000-4000-8000-000000000001"
    owner = String.duplicate("a", 64)
    other_sender = String.duplicate("f", 64)
    private = Path.join(dir, "legacy.pem")
    public = Path.join(dir, "legacy.der")
    assert {_, 0} = System.cmd("openssl", ["genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048", "-out", private], stderr_to_stdout: true)
    assert {_, 0} = System.cmd("openssl", ["pkey", "-in", private, "-pubout", "-outform", "DER", "-out", public])
    der = File.read!(public)
    device = DeviceProof.digest(der)

    {:ok, store} = Store.open_relay(path)
    store = Store.track_relay_writes(store)
    {:ok, engine} = Engine.operator_bootstrap(store.engine, person, owner)
    {:ok, engine} = Engine.setup_device(engine, person, device)
    {:ok, engine} = Engine.setup_device(engine, person, other_sender)
    {:ok, engine} = Engine.setup_sender(engine, device, "app:fixture")
    {:ok, engine} = Engine.setup_sender(engine, other_sender, "app:fixture")
    {:ok, store} = Store.commit(store, engine, %{person: person, actor: owner, action: "fixture"})
    assert {:ok, %{minimum_generation: 1}} = AuthSchema.initialize(store.db, store.engine, [])
    :ok = Store.close(store)

    {:ok, relay} = Relay.start_link(opts)
    {browser, dashboard, router} = local_routes(relay, person, origin)
    assert {:ok, ^person} = Relay.certificate_device_person(relay, device)

    assert {:ok, %{"code" => browser_code}} = Relay.owner_sign_in_code(relay, person, owner)
    browser_request =
      local_code_post(%{"code" => browser_code})
      |> Plug.Conn.put_req_header("origin", origin)

    signed_in = DashboardReadHTTP.call(browser_request, dashboard)
    assert signed_in.status == 200
    assert json(signed_in) == %{"method" => "owner_code"}
    assert signed_in.resp_cookies["__Host-harklight"].http_only
    assert DashboardReadHTTP.call(browser_request, dashboard).status == 401

    {request, bearer} = late_request(relay, dashboard, dir, person, owner, device, der)
    result = post("/v2/auth/sign-in", request) |> Router.call(router)
    assert result.status == 200
    assert %{"old_device" => "attached", "device_id" => ^device, "duplicate" => false} = json(result)
    retry = post("/v2/auth/sign-in", request) |> Router.call(router)
    assert %{"old_device" => "attached", "device_id" => ^device, "duplicate" => true} = json(retry)
    assert {:error, :unauthorized} = Relay.certificate_device_person(relay, device)

    :ok = GenServer.stop(browser)
    :ok = GenServer.stop(relay)
    {:ok, relay} = Relay.start_link(opts)
    {_browser, dashboard, router} = local_routes(relay, person, origin)
    assert {:error, :unauthorized} = Relay.certificate_device_person(relay, device)
    assert {:ok, ^person} = Relay.certificate_device_person(relay, other_sender)

    device_request =
      Plug.Test.conn(:get, "/v2/me")
      |> Plug.Conn.put_req_header("authorization", "Bearer " <> bearer)

    assert Router.call(device_request, router).status == 200
    cookie = signed_in.resp_cookies["__Host-harklight"].value

    remove =
      Plug.Test.conn(:post, "/devices/remove/" <> device, "confirm=remove")
      |> Map.put(:scheme, :https)
      |> Map.put(:port, 443)
      |> Plug.Conn.put_req_header("content-type", "application/x-www-form-urlencoded")
      |> Plug.Conn.put_req_header("origin", origin)
      |> Plug.Conn.put_req_header("cookie", "__Host-harklight=" <> cookie)
      |> DashboardReadHTTP.call(dashboard)

    assert remove.status == 303
    assert Router.call(device_request, router).status == 401
    assert {:error, :unauthorized} = Relay.certificate_device_person(relay, device)

    {removed, _bearer} = late_request(relay, dashboard, dir, person, owner, device, der)
    response = post("/v2/auth/sign-in", removed) |> Router.call(router)
    assert %{"old_device" => "removed", "device_id" => replacement} = json(response)
    refute replacement == device

    {unknown, _bearer} = late_request(relay, dashboard, dir, person, owner, device, der)
    unknown = put_in(unknown["old_proof"]["signature"], BrowserAuth.new_secret())
    response = post("/v2/auth/sign-in", unknown) |> Router.call(router)
    assert %{"old_device" => "unknown", "device_id" => fresh} = json(response)
    refute fresh in [device, replacement]
  end

  defp local_routes(relay, person, origin) do
    {:ok, browser} = DashboardBrowserSession.start_link(relay: relay)
    backend = {DashboardBackend, %DashboardBackend{relay: relay, sessions: browser, configured_owner_person: person}}

    dashboard =
      DashboardReadHTTP.init(backend: backend, skill_path: __ENV__.file, expected_origin: origin)

    {browser, dashboard, Router.init(relay: relay, auth: {:browser, backend, origin})}
  end

  defp late_request(relay, dashboard, dir, person, owner, device, der) do
    verifier = BrowserAuth.new_secret()
    bearer = BrowserAuth.new_secret()
    hash = DeviceProof.digest(bearer)
    assert {:ok, transaction} = Relay.create_native_transaction(relay, DeviceProof.digest(verifier))
    assert {:ok, %{"code" => code}} = Relay.owner_sign_in_code(relay, person, owner)
    direct = local_code_post(%{"code" => code, "transaction" => transaction})
    assert DashboardReadHTTP.call(direct, dashboard).status == 200
    assert DashboardReadHTTP.call(direct, dashboard).status == 401

    bytes = DeviceProof.sign_in_bytes(transaction, hash)
    {key, signature} = proof(dir, bytes)
    old = %{"device_id" => device, "public_key" => Base.url_encode64(der, padding: false),
            "signature" => sign(dir, bytes, "legacy")}
    {%{"transaction" => transaction, "verifier" => verifier, "public_key" => key,
       "session_hash" => hash, "signature" => signature, "label" => "Test phone",
       "old_proof" => old}, bearer}
  end

  defp local_code_post(body) do
    post("/sign-in/code", body)
    |> Map.put(:scheme, :https)
    |> Map.put(:port, 443)
    |> Plug.Conn.put_req_header("content-type", "application/json")
  end

  defp migration_request(dir, token, hash) do
    {key, signature} = proof(dir, DeviceProof.sign_in_bytes(token, hash), token)

    %{
      "token" => token,
      "public_key" => key,
      "label" => "Test phone",
      "session_hash" => hash,
      "signature" => signature
    }
  end

  defp proof(dir, bytes, id \\ "native") do
    private = Path.join(dir, id <> ".pem")
    public = Path.join(dir, id <> ".public.der")
    assert {_, 0} = System.cmd("openssl", ["genpkey", "-algorithm", "EC", "-pkeyopt", "ec_paramgen_curve:P-256", "-out", private])
    assert {_, 0} = System.cmd("openssl", ["pkey", "-in", private, "-pubout", "-outform", "DER", "-out", public])
    {Base.url_encode64(File.read!(public), padding: false), sign(dir, bytes, id)}
  end

  defp sign(dir, bytes, id) do
    private = Path.join(dir, id <> ".pem")
    input = Path.join(dir, id <> ".input")
    signature = Path.join(dir, id <> ".signature.der")
    File.write!(input, bytes)
    assert {_, 0} = System.cmd("openssl", ["dgst", "-sha256", "-sign", private, "-out", signature, input])
    Base.url_encode64(File.read!(signature), padding: false)
  end

  defp post(path, body), do: Plug.Test.conn(:post, path, JSON.encode!(body))

  defp json(conn) do
    {:ok, value} = JSON.decode_line(conn.resp_body <> "\n")
    value
  end
end
