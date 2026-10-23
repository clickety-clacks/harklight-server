defmodule AttentionServer.OwnerTLSTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DeviceIdentity, JSON, OwnerTLS, Relay, TLSFixtures}

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir =
      Path.join(System.tmp_dir!(), "attention-owner-tls-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")
    {:ok, relay} = Relay.start_link(store_path: path)

    {:ok, listener} =
      OwnerTLS.start_link(
        relay: relay,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem")
      )

    on_exit(fn ->
      if Process.alive?(listener), do: GenServer.stop(listener)
      if Process.alive?(relay), do: GenServer.stop(relay)
    end)

    %{relay: relay, listener: listener, path: path}
  end

  test "unprovisioned certificate and ordinary device never receive owner authority", %{
    certs: certs,
    relay: relay,
    listener: listener
  } do
    unauthenticated =
      :ssl.connect({127, 0, 0, 1}, OwnerTLS.port(listener),
        versions: [:"tlsv1.3"],
        verify: :verify_peer,
        cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
        server_name_indication: ~c"relay.test",
        active: false,
        mode: :binary,
        packet: :line
      )

    case unauthenticated do
      {:ok, no_cert_socket} ->
        assert {:error, _} = :ssl.recv(no_cert_socket, 0, 3_000)
        :ssl.close(no_cert_socket)

      {:error, _} ->
        :ok
    end

    {:ok, socket} = connect(listener, certs, "a1")
    assert {:error, :closed} = :ssl.recv(socket, 0, 3_000)
    :ssl.close(socket)

    owner = TLSFixtures.certificate_der!(certs, "a1")
    device = TLSFixtures.certificate_der!(certs, "a2")
    assert {:ok, person} = Relay.operator_bootstrap(relay, owner)
    assert {:ok, _request} = Relay.pending_enroll(relay, device, "ordinary device")
    {:ok, device_socket} = connect(listener, certs, "a2")
    assert {:error, :closed} = :ssl.recv(device_socket, 0, 3_000)
    :ssl.close(device_socket)

    assert %{"error" => "not_found"} =
             request(listener, certs, "a1", %{
               "v" => 1,
               "type" => "pending_lookup",
               "person_id" => "other-person",
               "approval_code" => "ABCDEFGHIJ"
             })

    assert is_binary(person)
  end

  test "proved owner approves one exact pending key and manages scoped rights", %{
    certs: certs,
    relay: relay,
    listener: listener,
    path: path
  } do
    owner = TLSFixtures.certificate_der!(certs, "a1")
    other_owner = TLSFixtures.certificate_der!(certs, "b1")
    device = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner)
    {:ok, device_key} = DeviceIdentity.from_certificate(device)
    assert {:ok, person} = Relay.operator_bootstrap(relay, owner)
    assert {:ok, other} = Relay.operator_bootstrap(relay, other_owner)
    assert {:ok, pending} = Relay.pending_enroll(relay, device, "A laptop")

    lookup = %{
      "v" => 1,
      "type" => "pending_lookup",
      "person_id" => person,
      "approval_code" => pending.approval_code
    }

    assert %{"error" => "not_found"} = request(listener, certs, "b1", lookup)

    assert %{
             "ok" => true,
             "label" => "A laptop",
             "device_id" => ^device_key,
             "expires_at" => deadline
           } =
             request(listener, certs, "a1", lookup)

    approve = %{
      "v" => 1,
      "type" => "pending_approve",
      "person_id" => person,
      "approval_code" => pending.approval_code,
      "expected_device_id" => device_key,
      "expected_expires_at" => deadline,
      "sender_grants" => ["app:test"]
    }

    assert %{"error" => "invalid_request"} =
             request(listener, certs, "a1", Map.drop(approve, ["expected_device_id"]))

    assert %{"error" => "invalid_request"} =
             request(listener, certs, "a1", Map.drop(approve, ["expected_expires_at"]))

    assert %{"error" => "not_found"} =
             request(
               listener,
               certs,
               "a1",
               %{approve | "expected_device_id" => String.duplicate("f", 64)}
             )

    assert %{"error" => "not_found"} =
             request(listener, certs, "a1", %{approve | "expected_expires_at" => deadline + 1})

    refute Map.has_key?(:sys.get_state(relay).store.engine.devices, device_key)

    assert %{"error" => "invalid_request"} =
             request(
               listener,
               certs,
               "a1",
               Map.put(approve, "device_id", String.duplicate("f", 64))
             )

    assert %{"ok" => true, "device_id" => ^device_key} =
             request(listener, certs, "a1", approve)

    assert %{"ok" => true, "device_id" => ^device_key} =
             request(listener, certs, "a1", approve)

    assert %{"error" => "not_found"} =
             request(listener, certs, "b1", %{approve | "person_id" => other})

    assert %{"error" => "not_found"} =
             request(listener, certs, "b1", %{
               "v" => 1,
               "type" => "sender_revoke",
               "person_id" => other,
               "device_id" => device_key,
               "sender_id" => "app:test"
             })

    revoke_sender = %{
      "v" => 1,
      "type" => "sender_revoke",
      "person_id" => person,
      "device_id" => device_key,
      "sender_id" => "app:test"
    }

    assert %{"ok" => true} = request(listener, certs, "a1", revoke_sender)
    grant_id = "00000000-0000-4000-8000-000000000088"
    target = %{"kind" => "device", "id" => device_key, "key_fingerprint" => device_key}

    grant = %{
      "v" => 1,
      "type" => "enrollment_read_grant",
      "person_id" => person,
      "connector_device_id" => device_key,
      "grant_id" => grant_id,
      "source_instance_id" => "source-A",
      "target" => target
    }

    assert %{"ok" => true} = request(listener, certs, "a1", grant)

    assert %{"error" => "not_found"} =
             request(listener, certs, "b1", %{grant | "person_id" => other})

    assert %{"ok" => true} =
             request(listener, certs, "a1", %{
               "v" => 1,
               "type" => "enrollment_read_revoke",
               "person_id" => person,
               "grant_id" => grant_id
             })

    assert %{"ok" => true} =
             request(listener, certs, "a1", %{
               "v" => 1,
               "type" => "device_revoke",
               "person_id" => person,
               "device_id" => device_key
             })

    assert {:error, :forbidden} = Relay.acknowledged_cursor(relay, device_key)
    assert {:error, :forbidden} = Relay.owner_revision(relay, other, owner_key)
    GenServer.stop(listener)
    GenServer.stop(relay)
    {:ok, restarted} = Relay.start_link(store_path: path)
    on_exit(fn -> if Process.alive?(restarted), do: GenServer.stop(restarted) end)
    engine = :sys.get_state(restarted).store.engine
    assert engine.devices[device_key].revoked
    assert engine.enrollment_read_grants[grant_id].revoked
  end

  test "recovery fences an already authenticated old owner socket before response", %{
    certs: certs,
    relay: relay,
    listener: listener
  } do
    owner = TLSFixtures.certificate_der!(certs, "a1")
    replacement = TLSFixtures.certificate_der!(certs, "unknown")
    device = TLSFixtures.certificate_der!(certs, "a2")
    assert {:ok, person} = Relay.operator_bootstrap(relay, owner)
    assert {:ok, pending} = Relay.pending_enroll(relay, device, "waiting")
    {:ok, old_socket} = connect(listener, certs, "a1")
    assert :ok = Relay.operator_recover(relay, person, replacement)

    line =
      JSON.encode!(%{
        "v" => 1,
        "type" => "pending_lookup",
        "person_id" => person,
        "approval_code" => pending.approval_code
      }) <> "\n"

    _ = :ssl.send(old_socket, line)
    assert {:error, :closed} = :ssl.recv(old_socket, 0, 3_000)
    :ssl.close(old_socket)

    {:ok, retired_socket} = connect(listener, certs, "a1")
    _ = :ssl.send(retired_socket, line)
    assert {:error, :closed} = :ssl.recv(retired_socket, 0, 3_000)
    :ssl.close(retired_socket)

    assert %{"ok" => true, "label" => "waiting"} =
             request(listener, certs, "unknown", %{
               "v" => 1,
               "type" => "pending_lookup",
               "person_id" => person,
               "approval_code" => pending.approval_code
             })
  end

  defp request(listener, certs, name, value) do
    {:ok, socket} = connect(listener, certs, name)
    assert :ok = :ssl.send(socket, JSON.encode!(value) <> "\n")
    assert {:ok, response} = :ssl.recv(socket, 0, 3_000)
    :ssl.close(socket)
    assert {:ok, reply} = JSON.decode_line(response)
    reply
  end

  defp connect(listener, certs, name) do
    :ssl.connect({127, 0, 0, 1}, OwnerTLS.port(listener),
      versions: [:"tlsv1.3"],
      verify: :verify_peer,
      cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
      certfile: String.to_charlist(Path.join(certs, "#{name}.pem")),
      keyfile: String.to_charlist(Path.join(certs, "#{name}.key")),
      server_name_indication: ~c"relay.test",
      session_tickets: :disabled,
      active: false,
      mode: :binary,
      packet: :line
    )
  end
end
