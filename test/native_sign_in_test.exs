defmodule AttentionServer.NativeSignInTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{BrowserAuth, DeviceProof, HostedRequests, JSON, Relay}
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
  end

  defp proof(dir, bytes) do
    private = Path.join(dir, "private.pem")
    public = Path.join(dir, "public.der")
    input = Path.join(dir, "proof-input")
    signature = Path.join(dir, "proof.der")
    File.write!(input, bytes)
    assert {_, 0} = System.cmd("openssl", ["genpkey", "-algorithm", "EC", "-pkeyopt", "ec_paramgen_curve:P-256", "-out", private])
    assert {_, 0} = System.cmd("openssl", ["pkey", "-in", private, "-pubout", "-outform", "DER", "-out", public])
    assert {_, 0} = System.cmd("openssl", ["dgst", "-sha256", "-sign", private, "-out", signature, input])
    {Base.url_encode64(File.read!(public), padding: false),
     Base.url_encode64(File.read!(signature), padding: false)}
  end

  defp post(path, body), do: Plug.Test.conn(:post, path, JSON.encode!(body))

  defp json(conn) do
    {:ok, value} = JSON.decode_line(conn.resp_body <> "\n")
    value
  end
end
