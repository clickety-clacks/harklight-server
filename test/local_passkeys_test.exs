defmodule AttentionServer.LocalPasskeysTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{DashboardBackend, DashboardBrowserSession, DashboardReadHTTP, Engine, JSON, Relay, Store}

  @moduletag :tmp_dir
  @person "00000000-0000-4000-8000-000000000001"
  @owner String.duplicate("a", 64)
  @origin "https://local.example.test"
  @host "local.example.test"

  test "a signed-in person registers a discoverable passkey and signs in on this origin",
       %{tmp_dir: directory} do
    path = Path.join(directory, "relay.sqlite3")
    {:ok, store} = Store.open_relay(path)
    store = Store.track_relay_writes(store)
    {:ok, engine} = Engine.operator_bootstrap(store.engine, @person, @owner)
    {:ok, store} = Store.commit(store, engine, %{person: @person, actor: @owner, action: "fixture"})
    :ok = Store.close(store)

    {:ok, relay} = Relay.start_link(store_path: path, sign_in_s1: true, dashboard_origin: @origin)
    {:ok, sessions} = DashboardBrowserSession.start_link(relay: relay)
    backend = %DashboardBackend{relay: relay, sessions: sessions, configured_owner_person: @person}
    dashboard = DashboardReadHTTP.init(backend: {DashboardBackend, backend}, skill_path: __ENV__.file, expected_origin: @origin)

    assert %{status: 401} = post(dashboard, "/devices/passkey/options", %{})
    assert {:ok, %{"code" => code}} = Relay.owner_sign_in_code(relay, @person, @owner)
    assert %{status: 200} = signed_in = post(dashboard, "/sign-in/code", %{"code" => code})
    cookie = signed_in.resp_cookies["__Host-harklight"].value

    assert %{status: 200} = registration = post(dashboard, "/devices/passkey/options", %{}, cookie)
    assert %{"reference" => reference, "publicKey" => %{"challenge" => challenge,
             "rp" => %{"id" => @host}}} = body(registration)

    {<<4, x::binary-size(32), y::binary-size(32)>>, private} =
      :crypto.generate_key(:ecdh, :secp256r1)

    id = :crypto.strong_rand_bytes(32)
    cose = CBOR.encode(%{1 => 2, 3 => -7, -1 => 1, -2 => bytes(x), -3 => bytes(y)})
    rp = :crypto.hash(:sha256, @host)
    auth = <<rp::binary, 0x45, 0::32, 0::128, byte_size(id)::16, id::binary, cose::binary>>
    attestation = CBOR.encode(%{"fmt" => "none", "authData" => bytes(auth), "attStmt" => %{}})
    register = %{"reference" => reference, "attestation" => encode(attestation),
                 "client_data" => encode(client_data("webauthn.create", challenge, @origin))}

    assert %{status: 401} = post(dashboard, "/devices/passkey/finish", register, "wrong")
    assert %{status: 200} = post(dashboard, "/devices/passkey/finish", register, cookie)
    assert %{status: 401} = post(dashboard, "/devices/passkey/finish", register, cookie)

    assert %{status: 200} = options = post(dashboard, "/sign-in/passkey/options", %{})
    assert %{"reference" => reference, "publicKey" => %{"challenge" => challenge}} = body(options)
    data = <<rp::binary, 0x05, 0::32>>
    request = %{"reference" => reference, "credential_id" => encode(id), "authenticator_data" => encode(data)}
    wrong = assertion(request, data, private, challenge, "https://other.example.test")
    assert %{status: 401} = post(dashboard, "/sign-in/passkey/finish", wrong)

    valid = assertion(request, data, private, challenge, @origin)
    assert %{status: 200} = redeemed = post(dashboard, "/sign-in/passkey/finish", valid)
    assert body(redeemed) == %{"method" => "passkey"}
    assert {:ok, %{person_id: @person}} =
             DashboardBrowserSession.resolve(sessions, redeemed.resp_cookies["__Host-harklight"].value)
    assert %{status: 401} = post(dashboard, "/sign-in/passkey/finish", valid)
    assert {:ok, ["owner_code"]} = Relay.native_sign_in_methods(relay)
    verifier = AttentionServer.BrowserAuth.new_secret()
    assert {:ok, transaction} = Relay.create_native_transaction(relay, AttentionServer.DeviceProof.digest(verifier))
    assert %{status: 401} = post(dashboard, "/sign-in/passkey/options", %{"transaction" => transaction})
    :ok = GenServer.stop(sessions)
    :ok = GenServer.stop(relay)

    {:ok, restarted} = Relay.start_link(store_path: path, native_passkeys: true, dashboard_origin: @origin)
    new_backend = %{backend | relay: restarted}
    new_dashboard = DashboardReadHTTP.init(backend: {DashboardBackend, new_backend}, skill_path: __ENV__.file, expected_origin: @origin)
    assert {:ok, ["owner_code", "passkey"]} = Relay.native_sign_in_methods(restarted)
    assert %{status: 200} = options = post(new_dashboard, "/sign-in/passkey/options", %{"transaction" => transaction})
    assert %{"reference" => reference, "publicKey" => %{"challenge" => challenge}} = body(options)
    native = assertion(%{request | "reference" => reference}, data, private, challenge, @origin)
    assert %{status: 200} = approved = post(new_dashboard, "/sign-in/passkey/finish", native)
    assert body(approved) == %{"transaction" => transaction}
    refute Map.has_key?(approved.resp_cookies, "__Host-harklight")
  end

  defp assertion(request, auth, private, challenge, origin) do
    client = client_data("webauthn.get", challenge, origin)
    signature = :crypto.sign(:ecdsa, :sha256, auth <> :crypto.hash(:sha256, client), [private, :secp256r1])
    Map.merge(request, %{"client_data" => encode(client), "signature" => encode(signature)})
  end

  defp client_data(type, challenge, origin),
    do: JSON.encode!(%{"type" => type, "challenge" => challenge, "origin" => origin, "crossOrigin" => false})

  defp bytes(value), do: %CBOR.Tag{tag: :bytes, value: value}
  defp encode(value), do: Base.url_encode64(value, padding: false)

  defp post(dashboard, path, payload, cookie \\ nil) do
    conn =
      Plug.Test.conn(:post, path, JSON.encode!(payload))
      |> Map.put(:scheme, :https)
      |> Map.put(:port, 443)
      |> Plug.Conn.put_req_header("origin", @origin)
      |> Plug.Conn.put_req_header("content-type", "application/json")

    conn = if cookie, do: Plug.Conn.put_req_header(conn, "cookie", "__Host-harklight=" <> cookie), else: conn
    DashboardReadHTTP.call(conn, dashboard)
  end

  defp body(conn) do
    {:ok, data} = JSON.decode_line(conn.resp_body <> "\n")
    data
  end
end
