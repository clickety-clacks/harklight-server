defmodule AttentionServer.NativeSignInTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{BrowserAuth, DeviceProof, HostedRequests, JSON, Relay, Store}
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
