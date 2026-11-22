defmodule AttentionServer.SelfSignedEnrollmentTest do
  use ExUnit.Case, async: false

  @access_proof "GET /v2/inbox?limit=1 HTTP/1.1\r\nHost: relay.test\r\n\r\n"

  alias AttentionServer.{ClientAPI, OwnerTLS, Relay, TLSFixtures}

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

    {:ok, relay} =
      Relay.start_link(store_path: Path.join(dir, "relay.sqlite3"))

    {:ok, web} =
      ClientAPI.Listener.start_link(
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

    # These are linked to the test process, so they may already be shutting
    # down when on_exit runs; stopping one that is mid-exit is not a failure.
    on_exit(fn ->
      stop_quietly(owner_tls)
      stop_quietly(web)
      stop_quietly(relay)
    end)

    %{relay: relay, web: web, owner_tls: owner_tls}
  end

  test "only approved self-signed device leaf reaches /v2 with TLS proof", %{
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
      connect_with_key(ClientAPI.Listener.port(web), certs, "local-device", "local-owner")
    )

    # The enrollment access proof: one authenticated GET /v2/inbox?limit=1.
    assert {:ok, socket} = connect(web, certs, "local-device")
    assert :ok = :ssl.send(socket, @access_proof)
    assert {:ok, response} = :ssl.recv(socket, 0, 3_000)
    assert String.starts_with?(response, "HTTP/1.1 200")
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

  defp connect(web, certs, name), do: connect_port(ClientAPI.Listener.port(web), certs, name)

  defp assert_rejected({:error, _}), do: :ok

  # A TLS 1.3 client may finish its side of the handshake before the server
  # refuses the certificate. A refused peer then reads an alert or a close; an
  # admitted one would get an HTTP answer to the proof request.
  defp assert_rejected({:ok, socket}) do
    _ = :ssl.send(socket, @access_proof)
    assert {:error, reason} = :ssl.recv(socket, 0, 3_000)
    refute reason == :timeout
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

  defp stop_quietly(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  catch
    :exit, _reason -> :ok
  end
end
