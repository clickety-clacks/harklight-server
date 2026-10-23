defmodule AttentionServer.SelfSignedEnrollmentTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{OwnerTLS, Relay, TLSFixtures, WebSocketRelay}

  setup_all do
    certs = TLSFixtures.create!()
    TLSFixtures.make_self_signed_client!(certs, "local-device")
    TLSFixtures.make_self_signed_client!(certs, "local-owner")
    TLSFixtures.make_dated_self_signed_client!(certs, "expired-local", -172_800, -86_400)
    TLSFixtures.make_dated_self_signed_client!(certs, "future-local", 86_400, 172_800)
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir =
      Path.join(System.tmp_dir!(), "attention-selfsigned-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, relay} = Relay.start_link(store_path: Path.join(dir, "relay.sqlite3"))

    {:ok, web} =
      WebSocketRelay.start_link(
        relay: relay,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem")
      )

    {:ok, owner_tls} =
      OwnerTLS.start_link(
        relay: relay,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem")
      )

    on_exit(fn ->
      if Process.alive?(owner_tls), do: GenServer.stop(owner_tls)
      if Process.alive?(web), do: GenServer.stop(web)
      if Process.alive?(relay), do: GenServer.stop(relay)
    end)

    %{relay: relay, web: web, owner_tls: owner_tls}
  end

  test "only approved self-signed device leaf reaches /relay with TLS proof", %{
    certs: certs,
    relay: relay,
    web: web
  } do
    owner = TLSFixtures.certificate_der!(certs, "a1")
    device = TLSFixtures.certificate_der!(certs, "local-device")
    {:ok, owner_key} = AttentionServer.DeviceIdentity.from_certificate(owner)
    assert {:ok, person} = Relay.operator_bootstrap(relay, owner)

    assert_rejected(connect(web, certs, "local-device"))
    assert {:ok, pending} = Relay.pending_enroll(relay, device, "local laptop")

    assert {:ok, device_key} =
             relay_approve(relay, person, owner_key, pending.approval_code, [])

    assert_rejected(
      connect_with_key(WebSocketRelay.port(web), certs, "local-device", "local-owner")
    )

    assert {:ok, socket} = connect(web, certs, "local-device")

    assert :ok =
             :ssl.send(socket, [
               "GET /relay HTTP/1.1\r\n",
               "Host: relay.test\r\n",
               "Upgrade: websocket\r\n",
               "Connection: Upgrade\r\n",
               "Sec-WebSocket-Version: 13\r\n",
               "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
               "Sec-WebSocket-Protocol: attention-relay/1\r\n\r\n"
             ])

    assert {:ok, response} = :ssl.recv(socket, 0, 3_000)
    assert String.starts_with?(response, "HTTP/1.1 101")
    :ssl.close(socket)
    assert :ok = Relay.owner_revoke_device(relay, person, owner_key, device_key)
    assert_rejected(connect(web, certs, "local-device"))
  end

  test "self-signed owner leaf works only after operator bootstrap and is fenced on recovery", %{
    certs: certs,
    relay: relay,
    owner_tls: owner_tls
  } do
    port = OwnerTLS.port(owner_tls)
    assert_rejected(connect_port(port, certs, "local-owner"))

    assert {:ok, person} =
             Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "local-owner"))

    assert {:ok, socket} = connect_port(port, certs, "local-owner")

    assert :ok =
             :ssl.send(
               socket,
               AttentionServer.JSON.encode!(%{
                 "v" => 1,
                 "type" => "maintainer_use",
                 "person_id" => person
               }) <> "\n"
             )

    assert {:ok, response} = :ssl.recv(socket, 0, 3_000)
    assert {:ok, %{"error" => "not_supported"}} = AttentionServer.JSON.decode_line(response)
    :ssl.close(socket)

    assert :ok = Relay.operator_recover(relay, person, TLSFixtures.certificate_der!(certs, "a1"))
    assert_rejected(connect_port(port, certs, "local-owner"))
  end

  test "enrolled expired and future self-signed leaves still fail TLS validity", %{
    certs: certs,
    relay: relay,
    web: web
  } do
    owner = TLSFixtures.certificate_der!(certs, "a1")
    {:ok, owner_key} = AttentionServer.DeviceIdentity.from_certificate(owner)
    assert {:ok, person} = Relay.operator_bootstrap(relay, owner)

    for name <- ["expired-local", "future-local"] do
      certificate = TLSFixtures.certificate_der!(certs, name)
      assert {:ok, pending} = Relay.pending_enroll(relay, certificate, name)

      assert {:ok, _device} =
               relay_approve(relay, person, owner_key, pending.approval_code, [])

      assert_rejected(connect(web, certs, name))
    end
  end

  defp connect(web, certs, name), do: connect_port(WebSocketRelay.port(web), certs, name)

  defp assert_rejected({:error, _}), do: :ok

  defp assert_rejected({:ok, socket}) do
    assert {:error, _} = :ssl.recv(socket, 0, 3_000)
    :ssl.close(socket)
  end

  defp connect_port(port, certs, name) do
    connect_with_key(port, certs, name, name)
  end

  defp connect_with_key(port, certs, certificate_name, key_name) do
    :ssl.connect({127, 0, 0, 1}, port,
      versions: [:"tlsv1.3"],
      verify: :verify_peer,
      cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
      certfile: String.to_charlist(Path.join(certs, "#{certificate_name}.pem")),
      keyfile: String.to_charlist(Path.join(certs, "#{key_name}.key")),
      server_name_indication: ~c"relay.test",
      session_tickets: :disabled,
      active: false,
      mode: :binary,
      packet: :raw
    )
  end

  defp relay_approve(relay, person, owner, code, senders) do
    engine = :sys.get_state(relay).store.engine
    item = Map.get(engine.pending_enrollments, code) || Map.get(engine.pending_history, code)
    Relay.owner_pending_approve(relay, person, owner, code, item.key, item.expires_at, senders)
  end
end
