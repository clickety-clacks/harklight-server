defmodule AttentionServer.EnrollmentHTTPSTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DeviceIdentity, EnrollmentHTTPS, JSON, Relay, Store, TLSFixtures}

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "attention-https-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")
    {:ok, relay} = Relay.start_link(store_path: path)

    {:ok, listener} =
      EnrollmentHTTPS.start_link(
        relay: relay,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key")
      )

    on_exit(fn ->
      if Process.alive?(listener), do: GenServer.stop(listener)
      if Process.alive?(relay), do: GenServer.stop(relay)
    end)

    %{relay: relay, listener: listener, path: path}
  end

  test "real TLS pending POST is secret-scoped and approval survives restart", %{
    certs: certs,
    listener: listener,
    relay: relay,
    path: path
  } do
    owner = TLSFixtures.certificate_der!(certs, "a1")
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner)
    assert {:ok, person} = Relay.operator_bootstrap(relay, owner)
    pem = File.read!(Path.join(certs, "a2.pem"))

    assert {200,
            %{
              "approval_code" => code,
              "status_secret" => secret,
              "expires_at" => deadline,
              "status_expires_at" => status_deadline
            }} =
             post(listener, certs, "/enroll", %{
               "public_certificate" => pem,
               "label" => "laptop"
             })

    assert Regex.match?(~r/\A[A-Z2-7]{10}\z/, code)
    assert byte_size(secret) == 64
    assert deadline > System.system_time(:millisecond)
    assert status_deadline == deadline + 86_400_000

    assert {200, %{"state" => "pending"}} =
             post(listener, certs, "/enroll/status", %{"status_secret" => secret})

    assert {409, %{"error" => "key_pending"}} =
             post(listener, certs, "/enroll", %{
               "public_certificate" => pem,
               "label" => "laptop"
             })

    assert {404, %{"error" => "not_found"}} =
             post(listener, certs, "/enroll/status", %{})

    assert {404, %{"error" => "not_found"}} =
             post(listener, certs, "/enroll/status", %{"status_secret" => "bad"})

    assert {404, %{"error" => "not_found"}} =
             post(listener, certs, "/enroll/status", %{"status_secret" => code})

    assert {404, %{"error" => "not_found"}} =
             post(listener, certs, "/enroll/status", %{
               "status_secret" => String.duplicate("0", 64)
             })

    assert {:error, :forbidden} = Relay.acknowledged_cursor(relay, owner_key)
    assert {:ok, device} = relay_approve(relay, person, owner_key, code, [])

    assert {409, %{"error" => "key_in_use"}} =
             post(listener, certs, "/enroll", %{
               "public_certificate" => pem,
               "label" => "laptop"
             })

    assert {200, %{"state" => "approved", "person_id" => ^person, "device_id" => ^device}} =
             post(listener, certs, "/enroll/status", %{"status_secret" => secret})

    assert :ok = Relay.owner_revoke_device(relay, person, owner_key, device)

    assert {200, %{"state" => "revoked"}} =
             post(listener, certs, "/enroll/status", %{"status_secret" => secret})

    GenServer.stop(listener)
    GenServer.stop(relay)
    {:ok, restarted} = Relay.start_link(store_path: path)
    on_exit(fn -> if Process.alive?(restarted), do: GenServer.stop(restarted) end)
    assert {:error, :revoked} = relay_approve(restarted, person, owner_key, code, [])
    assert :sys.get_state(restarted).store.engine.devices[device].revoked
  end

  test "public status only commits a durable fence when a deadline first crosses", %{
    certs: certs,
    listener: listener,
    relay: relay
  } do
    initial_version = :sys.get_state(relay).store.version

    assert {404, %{"error" => "not_found"}} =
             post(listener, certs, "/enroll/status", %{
               "status_secret" => String.duplicate("0", 64)
             })

    assert :sys.get_state(relay).store.version == initial_version

    pem = File.read!(Path.join(certs, "a2.pem"))

    assert {200, %{"approval_code" => code, "status_secret" => secret}} =
             post(listener, certs, "/enroll", %{
               "public_certificate" => pem,
               "label" => "deadline fence"
             })

    version = :sys.get_state(relay).store.version

    assert {200, %{"state" => "pending"}} =
             post(listener, certs, "/enroll/status", %{"status_secret" => secret})

    assert :sys.get_state(relay).store.version == version

    state = :sys.get_state(relay)
    item = state.store.engine.pending_enrollments[code]
    expired_at = System.system_time(:millisecond) - 1

    engine = %{
      state.store.engine
      | pending_enrollments:
          Map.put(state.store.engine.pending_enrollments, code, %{
            item
            | expires_at: expired_at,
              status_expires_at: expired_at + 86_400_000
          })
    }

    assert {:ok, stored} =
             Store.commit(state.store, engine, %{
               person: "unassigned",
               actor: "test_fixture",
               action: "cross_pending_deadline"
             })

    :sys.replace_state(relay, fn current -> %{current | store: stored} end)
    version = stored.version

    assert {200, %{"state" => "expired"}} =
             post(listener, certs, "/enroll/status", %{"status_secret" => secret})

    assert :sys.get_state(relay).store.version == version + 1

    assert {200, %{"state" => "expired"}} =
             post(listener, certs, "/enroll/status", %{"status_secret" => secret})

    assert :sys.get_state(relay).store.version == version + 1
  end

  test "HTTP surface rejects wrong path, extra authority fields, and invalid certificates", %{
    certs: certs,
    listener: listener,
    relay: relay
  } do
    pem = File.read!(Path.join(certs, "a2.pem"))

    assert {400, _} =
             post(listener, certs, "/enroll", %{
               "public_certificate" => pem,
               "label" => "laptop",
               "person_id" => "chosen-by-client"
             })

    assert {400, _} =
             post(listener, certs, "/enroll", %{
               "public_certificate" => "not PEM",
               "label" => "laptop"
             })

    assert {400, _} = post(listener, certs, "/owner", %{})
    assert :sys.get_state(relay).store.engine.persons == %{}
    assert :sys.get_state(relay).store.engine.pending_enrollments == %{}
  end

  test "real TLS capacity refusal discloses no request handle and preserves retained status", %{
    certs: certs,
    listener: listener,
    relay: relay
  } do
    now_ms = System.system_time(:millisecond)
    {:ok, key} = DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "a2"))

    history =
      Map.new(0..1_023, fn index ->
        secret = <<index::256>>

        {Base.encode32(<<index::40>>, padding: false) <> "AA",
         %{
           state: :expired,
           secret_hash: :crypto.hash(:sha256, secret),
           key: key,
           expires_at: now_ms - 1,
           status_expires_at: now_ms - 1 + 86_400_000
         }}
      end)

    state = :sys.get_state(relay)

    assert {:ok, stored} =
             Store.commit(state.store, %{state.store.engine | pending_history: history}, %{
               person: "unassigned",
               actor: "test_fixture",
               action: "fill_pending"
             })

    :sys.replace_state(relay, fn current -> %{current | store: stored} end)
    pem = File.read!(Path.join(certs, "b1.pem"))

    assert {503, %{"error" => "capacity"}} =
             post(listener, certs, "/enroll", %{"public_certificate" => pem, "label" => "new"})

    assert {200, %{"state" => "expired"}} =
             post(listener, certs, "/enroll/status", %{
               "status_secret" => <<0::256>> |> Base.encode16(case: :lower)
             })

    assert map_size(:sys.get_state(relay).store.engine.pending_history) == 1_024
  end

  test "dropped first TLS reply leaves an opaque same-key retry without secret recovery", %{
    certs: certs,
    listener: listener,
    relay: relay
  } do
    pem = File.read!(Path.join(certs, "a2.pem"))
    request = %{"public_certificate" => pem, "label" => "lost first reply"}
    assert :ok = send_and_drop(listener, certs, "/enroll", request)
    assert :ok = await_pending(relay, System.monotonic_time(:millisecond) + 3_000)

    [{code, pending}] = Map.to_list(:sys.get_state(relay).store.engine.pending_enrollments)
    assert pending.label == "lost first reply"

    assert {409, %{"error" => "key_pending"}} =
             post(listener, certs, "/enroll", request)

    [{^code, same}] = Map.to_list(:sys.get_state(relay).store.engine.pending_enrollments)
    assert same == pending

    assert {404, %{"error" => "not_found"}} =
             post(listener, certs, "/enroll/status", %{
               "status_secret" => String.duplicate("0", 64)
             })
  end

  defp await_pending(relay, deadline) do
    if map_size(:sys.get_state(relay).store.engine.pending_enrollments) == 1 do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("TLS request did not commit before reply loss")
      else
        Process.sleep(10)
        await_pending(relay, deadline)
      end
    end
  end

  defp send_and_drop(listener, certs, path, request) do
    {:ok, socket} =
      :ssl.connect({127, 0, 0, 1}, EnrollmentHTTPS.port(listener),
        versions: [:"tlsv1.3"],
        verify: :verify_peer,
        cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
        server_name_indication: ~c"relay.test",
        session_tickets: :disabled,
        active: false,
        mode: :binary,
        packet: :raw
      )

    body = JSON.encode!(request)

    assert :ok =
             :ssl.send(socket, [
               "POST #{path} HTTP/1.1\r\n",
               "Host: relay.test\r\n",
               "Content-Type: application/json\r\n",
               "Content-Length: #{byte_size(body)}\r\n",
               "Connection: close\r\n\r\n",
               body
             ])

    :ssl.close(socket)
  end

  defp post(listener, certs, path, request) do
    {:ok, socket} =
      :ssl.connect({127, 0, 0, 1}, EnrollmentHTTPS.port(listener),
        versions: [:"tlsv1.3"],
        verify: :verify_peer,
        cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
        server_name_indication: ~c"relay.test",
        session_tickets: :disabled,
        active: false,
        mode: :binary,
        packet: :raw
      )

    body = JSON.encode!(request)

    assert :ok =
             :ssl.send(socket, [
               "POST #{path} HTTP/1.1\r\n",
               "Host: relay.test\r\n",
               "Content-Type: application/json\r\n",
               "Content-Length: #{byte_size(body)}\r\n",
               "Connection: close\r\n\r\n",
               body
             ])

    bytes = read_all(socket, <<>>)
    :ssl.close(socket)
    [header, response] = :binary.split(bytes, "\r\n\r\n")
    [first | _] = String.split(header, "\r\n")
    ["HTTP/1.1", status | _] = String.split(first, " ")
    assert {:ok, json} = JSON.decode_line(response <> "\n")
    {String.to_integer(status), json}
  end

  defp read_all(socket, bytes) do
    case :ssl.recv(socket, 0, 3_000) do
      {:ok, chunk} -> read_all(socket, bytes <> chunk)
      {:error, :closed} -> bytes
    end
  end

  defp relay_approve(relay, person, owner, code, senders) do
    engine = :sys.get_state(relay).store.engine
    item = Map.get(engine.pending_enrollments, code) || Map.get(engine.pending_history, code)
    Relay.owner_pending_approve(relay, person, owner, code, item.key, item.expires_at, senders)
  end
end
