defmodule AttentionServer.GuidedJoinHTTPSTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    BrowserAuth,
    DashboardBackend,
    DashboardBrowserSession,
    DashboardReadHTTP,
    DeviceIdentity,
    Engine,
    EnrollmentHTTPS,
    GuidedJoin,
    JSON,
    OwnerTLS,
    Relay,
    Store,
    TLSFixtures
  }

  alias AttentionServer.ClientAPI.Reads

  setup_all do
    certs = TLSFixtures.create!()
    TLSFixtures.make_self_signed_client!(certs, "self_device")
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} = certs_context do
    dir = Path.join(System.tmp_dir!(), "guided-https-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")

    {:ok, config} =
      GuidedJoin.configuration("relay.test", 18444, 18443, File.read!(Path.join(certs, "ca.pem")))

    relay =
      start_supervised!(
        {Relay,
         store_path: path,
         join_configuration: config,
         deployment_mode: Map.get(certs_context, :mode, :self_hosted),
         sign_in_s1: Map.has_key?(certs_context, :mode)}
      )
    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))
    {:ok, other} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "b1"))

    owner =
      start_supervised!(
        {OwnerTLS,
         relay: relay,
         certfile: Path.join(certs, "server.pem"),
         keyfile: Path.join(certs, "server.key"),
         cacertfile: Path.join(certs, "ca.pem")}
      )

    listener =
      start_supervised!(
        {EnrollmentHTTPS,
         relay: relay,
         cacertfile: Path.join(certs, "ca.pem"),
         certfile: Path.join(certs, "server.pem"),
         keyfile: Path.join(certs, "server.key")}
      )

    %{
      dir: dir,
      path: path,
      config: config,
      relay: relay,
      owner: owner,
      listener: listener,
      person: person,
      other: other
    }
  end

  @tag mode: :self_hosted
  test "S1 owner can approve an Agent over TLS and cannot approve a person device", c do
    assert %{"ok" => true, "invitation" => invitation} =
             owner(c, "a1", %{"type" => "invite_create"})

    {request, approval} = request_agent_enrollment(c, invitation, "a2")

    assert %{"error" => "not_found"} =
             owner(c, "a1", Map.put(approval, "sender_grants", []))

    assert {:error, :unauthorized} =
             Relay.certificate_device_person(c.relay, approval["expected_device_id"])

    assert %{"ok" => true} =
             owner(c, "a1", Map.put(approval, "sender_grants", ["app:fixture"]))

    assert_agent_enrollment(c, request, c.person, approval["expected_device_id"])
  end

  @tag mode: :saas
  test "S1 Hosted owner can approve Agent enrollment from the signed-in dashboard", c do
    {:ok, person} = Relay.hosted_account_create(c.relay, "synthetic-agent-fixture")
    transaction = BrowserAuth.new_secret()
    {:ok, grant} = Relay.hosted_browser_grant(c.relay, person, transaction, "email_code")
    {:ok, %{cookie: cookie}} = Relay.redeem_hosted_browser_grant(c.relay, grant, transaction)
    {:ok, sessions} = DashboardBrowserSession.start_link(relay: c.relay)
    origin = "https://relay.test"
    backend = %DashboardBackend{relay: c.relay, sessions: sessions, origin: origin}

    dashboard =
      DashboardReadHTTP.init(
        backend: {DashboardBackend, backend},
        hosted: true,
        expected_origin: origin,
        skill_path: __ENV__.file
      )

    {:ok, browser_owner} = DashboardBackend.resolve_session(backend, cookie)
    request_id = "00000000-0000-4000-8000-000000000001"
    {:ok, invitation} = DashboardBackend.create_invitation(backend, {browser_owner, request_id})
    {request, approval} = request_agent_enrollment(c, invitation, "a2")
    path = "/devices/waiting/" <> approval["approval_code"] <> "/approve"

    fields = %{
      "key" => approval["expected_device_id"],
      "deadline" => "",
      "senders" => ""
    }

    approve = fn fields ->
      %{Plug.Test.conn(:post, path, URI.encode_query(fields)) | scheme: :https, port: 443}
      |> Plug.Conn.put_req_header("origin", origin)
      |> Plug.Conn.put_req_header("content-type", "application/x-www-form-urlencoded")
      |> Plug.Conn.put_req_header("cookie", "__Host-harklight=" <> cookie)
      |> DashboardReadHTTP.call(dashboard)
    end

    assert approve.(fields).status == 409
    assert {:error, :unauthorized} =
             Relay.certificate_device_person(c.relay, approval["expected_device_id"])

    assert approve.(%{fields | "senders" => "app:fixture"}).status == 303
    assert_agent_enrollment(c, request, person, approval["expected_device_id"])
  end

  defp request_agent_enrollment(c, invitation, identity) do
    request = %{
      "invite_token" => invitation["invite_token"],
      "status_secret" => String.duplicate("b", 64),
      "public_certificate" => File.read!(Path.join(c.certs, identity <> ".pem"))
    }

    assert {200, %{"state" => "pending", "approval_code" => code, "device_id" => key,
                   "expires_at" => deadline}} = post(c, "/enroll", request)

    approval = %{
      "type" => "pending_approve",
      "approval_code" => code,
      "expected_device_id" => key,
      "expected_expires_at" => deadline
    }

    {request, approval}
  end

  defp assert_agent_enrollment(c, request, person, key) do
    assert {200, %{"state" => "approved", "person_id" => ^person, "device_id" => ^key}} =
             post(c, "/enroll/status", Map.take(request, ["status_secret"]))

    assert {200, %{"state" => "approved", "device_id" => ^key}} = post(c, "/enroll", request)
    assert {:ok, ^person} = Relay.certificate_device_person(c.relay, key)
  end

  test "owner CLI writes a usable pinned invitation; lost HTTP reply retries exactly and approval grants no sender",
       c do
    output = Path.join(c.dir, "invite.json")

    AttentionServer.CLI.Owner.run([
      "invite",
      "--connect",
      "127.0.0.1",
      "--name",
      "relay.test",
      "--port",
      Integer.to_string(OwnerTLS.port(c.owner)),
      "--ca",
      Path.join(c.certs, "ca.pem"),
      "--cert",
      Path.join(c.certs, "a1.pem"),
      "--key",
      Path.join(c.certs, "a1.key"),
      "--person",
      c.person,
      "--output",
      output
    ])

    {:ok, invitation} = JSON.decode_line(File.read!(output))
    assert Map.take(invitation, Map.keys(c.config)) == c.config
    assert invitation["person_id"] == c.person
    payload = URI.parse(invitation["join_url"]).fragment |> Base.url_decode64!(padding: false)
    assert {:ok, decoded} = JSON.decode_line(payload <> "\n")
    assert decoded == Map.delete(invitation, "join_url")
    secret = String.duplicate("a", 64)

    request = %{
      "invite_token" => invitation["invite_token"],
      "status_secret" => secret,
      "public_certificate" => File.read!(Path.join(c.certs, "a2.pem"))
    }

    post(c, "/enroll", request, :drop)
    wait_pending(c.relay, System.monotonic_time(:millisecond) + 3_000)
    [{code, original}] = Map.to_list(:sys.get_state(c.relay).store.engine.pending_enrollments)
    assert original.person == c.person
    assert {200, reply} = post(c, "/enroll", request)
    assert reply["approval_code"] == code
    assert reply["expires_at"] == nil
    assert original.expires_at == nil
    assert {200, ^reply} = post(c, "/enroll", Map.put(request, "label", "retry label"))

    assert {409, %{"error" => "used"}} =
             post(c, "/enroll", %{request | "status_secret" => String.duplicate("b", 64)})

    assert {400, %{"error" => "invalid_request"}} =
             post(c, "/enroll", Map.put(request, "person_id", c.other))

    assert {200, %{"state" => "pending"}} =
             post(c, "/enroll/status", %{"status_secret" => secret})

    assert %{"error" => "not_found"} =
             owner(c, "b1", %{
               "type" => "pending_lookup",
               "person_id" => c.other,
               "approval_code" => code
             })

    approval = %{
      "type" => "pending_approve",
      "approval_code" => code,
      "expected_device_id" => original.key,
      "expected_expires_at" => original.expires_at,
      "sender_grants" => ["app:unrequested"]
    }

    assert %{"error" => "not_found"} =
             owner(c, "a1", %{approval | "expected_expires_at" => 1})

    assert %{"ok" => true, "device_id" => device} =
             owner(c, "a1", %{approval | "sender_grants" => []})

    assert device == original.key

    assert {200, %{"state" => "approved", "person_id" => person, "device_id" => ^device}} =
             post(c, "/enroll/status", %{"status_secret" => secret})

    assert person == c.person
    assert :sys.get_state(c.relay).store.engine.sender_grants == %{}
    assert {:ok, _cursor} = Relay.acknowledged_cursor(c.relay, device)

    assert {200,
            %{
              "state" => "approved",
              "person_id" => ^person,
              "device_id" => ^device,
              "approval_code" => ^code,
              "request_generation" => 1,
              "expires_at" => nil
            }} =
             post(c, "/enroll/resume", %{
               "status_secret" => secret,
               "public_certificate" => File.read!(Path.join(c.certs, "a2.pem"))
             })

    assert {:ok, persisted} = Store.open(c.path)
    assert persisted.engine.pending_history[code].joined
    Store.close(persisted)

    {:ok, owner_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a1"))

    assert :ok = Relay.owner_revoke_device(c.relay, c.person, owner_key, device)

    assert {200, %{"state" => "revoked"}} =
             post(c, "/enroll/resume", %{
               "status_secret" => secret,
               "public_certificate" => File.read!(Path.join(c.certs, "a2.pem"))
             })
  end

  test "invitation refuses caller-chosen trust and honest declined state remains durable",
       c do
    assert %{"error" => "invalid_request"} =
             owner(c, "a1", %{
               "type" => "invite_create",
               "enrollment_url" => "https://other.test/enroll"
             })

    assert %{"ok" => true, "invitation" => invitation} =
             owner(c, "a1", %{"type" => "invite_create"})

    secret = String.duplicate("c", 64)

    request = %{
      "invite_token" => invitation["invite_token"],
      "status_secret" => secret,
      "public_certificate" => File.read!(Path.join(c.certs, "a2.pem")),
      "label" => "phone"
    }

    assert {200, %{"approval_code" => code, "expires_at" => deadline}} =
             post(c, "/enroll", request)

    {:ok, key} = DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a2"))

    decline = %{
      "type" => "pending_decline",
      "approval_code" => code,
      "expected_device_id" => key,
      "expected_expires_at" => deadline
    }

    assert %{"error" => "not_found"} = owner(c, "b1", Map.put(decline, "person_id", c.other))
    assert %{"ok" => true} = owner(c, "a1", decline)

    assert {200, %{"state" => "declined"}} =
             post(c, "/enroll/status", %{"status_secret" => secret})

    assert {200, %{"state" => "declined"}} = post(c, "/enroll", request)
    assert :sys.get_state(c.relay).store.engine.devices == %{}
    {:ok, stored} = Store.open(c.path)
    {:ok, raw_secret} = Base.decode16(secret, case: :lower)

    assert {:ok, %{"state" => "declined"}} =
             Engine.pending_status(stored.engine, :crypto.hash(:sha256, raw_secret))

    Store.close(stored)

    assert {404, %{"error" => "refused"}} =
             post(c, "/enroll", %{request | "invite_token" => "not-an-invite"})

    assert map_size(:sys.get_state(c.relay).store.engine.pending_history) == 1
  end

  test "exact requester cancellation survives restart and prevents a later approval", c do
    {:ok, owner_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a1"))

    {:ok, invitation} = Relay.owner_invite_create(c.relay, c.person, owner_key)
    secret = String.duplicate("d", 64)
    {:ok, key} = DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a2"))

    assert {200, %{"approval_code" => code, "expires_at" => deadline}} =
             post(c, "/enroll", %{
               "invite_token" => invitation["invite_token"],
               "status_secret" => secret,
               "public_certificate" => File.read!(Path.join(c.certs, "a2.pem"))
             })

    cancel = %{
      "status_secret" => secret,
      "approval_code" => code,
      "expected_device_id" => key,
      "expected_expires_at" => deadline
    }

    assert {404, %{"error" => "not_found"}} =
             post(c, "/enroll/cancel", %{cancel | "status_secret" => String.duplicate("e", 64)})

    assert {409, %{"error" => "stale_request", "current" => %{"approval_code" => ^code}}} =
             post(c, "/enroll/cancel", %{cancel | "expected_expires_at" => 1})

    assert {200, %{"state" => "cancelled"}} = post(c, "/enroll/cancel", cancel)
    assert {200, %{"state" => "cancelled"}} = post(c, "/enroll/cancel", cancel)

    assert {200, %{"state" => "cancelled"}} =
             post(c, "/enroll/status", %{"status_secret" => secret})

    assert {:error, :cancelled} =
             Relay.owner_pending_approve(c.relay, c.person, owner_key, code, key, deadline, [])

    {:ok, saved} = Store.open(c.path)
    assert saved.engine.devices == %{}
    Store.close(saved)
    stop_supervised(EnrollmentHTTPS)
    stop_supervised(OwnerTLS)
    stop_supervised(Relay)
    {:ok, restarted} = Relay.start_link(store_path: c.path, join_configuration: c.config)
    on_exit(fn -> if Process.alive?(restarted), do: GenServer.stop(restarted) end)

    assert {:ok, %{"state" => "cancelled"}} =
             Relay.pending_cancel(restarted, secret, code, key, deadline)

    assert {:error, :cancelled} =
             Relay.owner_pending_approve(restarted, c.person, owner_key, code, key, deadline, [])
  end

  test "serialized approval/cancel race has one durable truthful result", c do
    {:ok, owner_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a1"))

    cert = TLSFixtures.certificate_der!(c.certs, "a2")
    {:ok, key} = DeviceIdentity.from_certificate(cert)
    {:ok, invitation} = Relay.owner_invite_create(c.relay, c.person, owner_key)
    secret = String.duplicate("f", 64)
    {:ok, reply} = Relay.invited_enroll(c.relay, invitation["invite_token"], cert, "race", secret)
    code = reply["approval_code"]
    deadline = reply["expires_at"]
    cancel = Task.async(fn -> Relay.pending_cancel(c.relay, secret, code, key, deadline) end)

    approval =
      Task.async(fn ->
        Relay.owner_pending_approve(c.relay, c.person, owner_key, code, key, deadline, [])
      end)

    result = Task.await(cancel)
    outcome = Task.await(approval)

    assert {result, outcome} in [
             {{:ok, %{"state" => "cancelled"}}, {:error, :cancelled}},
             {{:ok, %{"state" => "approved", "person_id" => c.person, "device_id" => key}},
              {:ok, key}}
           ]

    {:ok, saved} = Store.open(c.path)

    assert {:ok, status} =
             Engine.pending_status(
               saved.engine,
               :crypto.hash(:sha256, Base.decode16!(secret, case: :lower))
             )

    assert status == elem(result, 1)
    assert Map.has_key?(saved.engine.devices, key) == (status["state"] == "approved")
    assert saved.engine.sender_grants == %{}
    Store.close(saved)
  end

  test "only proved current device removes itself; lost-reply retry preserves others and revokes access",
       c do
    cert = TLSFixtures.certificate_der!(c.certs, "self_device")
    other_cert = TLSFixtures.certificate_der!(c.certs, "unknown")
    {:ok, key} = Relay.setup_device_certificate(c.relay, c.person, cert)
    {:ok, other_key} = Relay.setup_device_certificate(c.relay, c.other, other_cert)
    assert {:ok, _} = Reads.me(c.relay, key)
    request = %{"expected_device_id" => key}
    assert {404, %{"error" => "not_found"}} = post(c, "/device/remove", request)
    assert {404, %{"error" => "not_found"}} = post(c, "/device/remove", request, :read, "unknown")

    assert {400, %{"error" => "invalid_request"}} =
             post(
               c,
               "/device/remove",
               Map.put(request, "person_id", c.person),
               :read,
               "self_device"
             )

    assert {404, %{"error" => "not_found"}} = post(c, "/device/remove", request, :read, "a1")
    post(c, "/device/remove", request, :drop, "self_device")
    # Reading a second response proves retry after the first reply was lost.
    assert {200, removed} = post(c, "/device/remove", request, :read, "self_device")
    assert removed["state"] == "removed"
    assert removed["device_id"] == key
    assert removed["person_id"] == c.person
    assert removed["enrollment_revision"] == 2
    assert {:error, :forbidden} = Relay.acknowledged_cursor(c.relay, key)

    # The removed device has no client API v2 principal any more.
    assert {:error, :unauthorized} = Reads.me(c.relay, key)

    assert {:ok, _} = Relay.acknowledged_cursor(c.relay, other_key)
    {:ok, saved} = Store.open(c.path)
    assert saved.engine.devices[key].revoked
    refute saved.engine.devices[other_key].revoked
    engine = saved.engine
    assert {:ok, ^engine, ^removed} = Engine.device_remove(engine, key)
    Store.close(saved)
    stop_supervised(EnrollmentHTTPS)
    stop_supervised(OwnerTLS)
    stop_supervised(Relay)
    restarted = start_supervised!({Relay, store_path: c.path, join_configuration: c.config})

    listener =
      start_supervised!(
        {EnrollmentHTTPS,
         relay: restarted,
         certfile: Path.join(c.certs, "server.pem"),
         keyfile: Path.join(c.certs, "server.key"),
         cacertfile: Path.join(c.certs, "ca.pem")}
      )

    assert {200, ^removed} =
             post(
               %{c | relay: restarted, listener: listener},
               "/device/remove",
               request,
               :read,
               "self_device"
             )

    assert {:error, :forbidden} = Relay.acknowledged_cursor(restarted, key)
  end

  test "after the upgrade an unused pre-v2 invitation is refused with a reissue request", c do
    owner_key = owner_key(c)
    {:ok, invitation} = Relay.owner_invite_create(c.relay, c.person, owner_key)
    token = invitation["invite_token"]
    hash = :crypto.hash(:sha256, Base.decode16!(token, case: :lower))
    assert invitation["v"] == 2
    assert invitation["device_url"] == "https://relay.test:18443"

    # Rewrite the stored invitation to the shape a 0.1 Server wrote, then
    # reopen the durable store as the upgraded Server does.
    stop_supervised!(EnrollmentHTTPS)
    stop_supervised!(OwnerTLS)
    stop_supervised!(Relay)
    {:ok, store} = Store.open(c.path)
    engine = store.engine
    config = engine.join_invitations[hash].configuration
    %URI{host: host, port: port} = URI.parse(config["device_url"])

    v1 =
      config
      |> Map.delete("device_url")
      |> Map.put("relay_url", "wss://#{host}:#{port}/relay")

    assert :ok = Store.close(store)
    :ok = AttentionServer.PreChangeLogStore.snapshot_fixture(c.path, 30, fn engine ->
      put_in(engine.join_invitations[hash].configuration, v1)
    end)
    :ok = AttentionServer.PreChangeLogStore.convert_fixture(c.path)

    relay = start_supervised!({Relay, store_path: c.path, join_configuration: c.config})

    listener =
      start_supervised!(
        {EnrollmentHTTPS,
         relay: relay,
         cacertfile: Path.join(c.certs, "ca.pem"),
         certfile: Path.join(c.certs, "server.pem"),
         keyfile: Path.join(c.certs, "server.key")}
      )

    c = %{c | relay: relay, listener: listener}
    assert {:error, :reissue_required} = Relay.invitation_resolve(relay, token)

    request = %{
      "invite_token" => token,
      "status_secret" => String.duplicate("a", 64),
      "public_certificate" => File.read!(Path.join(c.certs, "a2.pem"))
    }

    assert {410, %{"error" => "invitation_reissue_required", "message" => message}} =
             post(c, "/enroll", request)

    assert message =~ "older Harklight Server"
    assert message =~ "new invitation"
    refute message =~ "owner"
    assert :sys.get_state(relay).store.engine.pending_enrollments == %{}
  end

  defp owner_key(c) do
    {:ok, key} = DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a1"))
    key
  end

  defp owner(c, identity, request) do
    {:ok, socket} =
      :ssl.connect({127, 0, 0, 1}, OwnerTLS.port(c.owner),
        versions: [:"tlsv1.3"],
        verify: :verify_peer,
        active: false,
        mode: :binary,
        packet: :line,
        cacertfile: String.to_charlist(Path.join(c.certs, "ca.pem")),
        certfile: String.to_charlist(Path.join(c.certs, identity <> ".pem")),
        keyfile: String.to_charlist(Path.join(c.certs, identity <> ".key")),
        server_name_indication: ~c"relay.test",
        session_tickets: :disabled
      )

    frame = Map.merge(%{"v" => 1, "person_id" => c.person}, request)
    :ok = :ssl.send(socket, JSON.encode!(frame) <> "\n")
    {:ok, line} = :ssl.recv(socket, 0, 3_000)
    :ssl.close(socket)
    {:ok, reply} = JSON.decode_line(line)
    reply
  end

  defp post(c, path, request, mode \\ :read, identity \\ nil) do
    identity_opts =
      if identity,
        do: [
          certfile: String.to_charlist(Path.join(c.certs, identity <> ".pem")),
          keyfile: String.to_charlist(Path.join(c.certs, identity <> ".key"))
        ],
        else: []

    {:ok, socket} =
      :ssl.connect(
        {127, 0, 0, 1},
        EnrollmentHTTPS.port(c.listener),
        identity_opts ++
          [
            versions: [:"tlsv1.3"],
            verify: :verify_peer,
            active: false,
            mode: :binary,
            packet: :raw,
            cacertfile: String.to_charlist(Path.join(c.certs, "ca.pem")),
            server_name_indication: ~c"relay.test",
            session_tickets: :disabled
          ]
      )

    body = JSON.encode!(request)

    :ok =
      :ssl.send(socket, [
        "POST #{path} HTTP/1.1\r\nHost: relay.test\r\nContent-Type: application/json\r\n",
        "Content-Length: #{byte_size(body)}\r\nConnection: close\r\n\r\n",
        body
      ])

    if mode == :drop do
      :ssl.close(socket)
      :ok
    else
      bytes = receive_all(socket, <<>>)
      :ssl.close(socket)
      [header, response] = :binary.split(bytes, "\r\n\r\n")
      [first | _] = String.split(header, "\r\n")
      ["HTTP/1.1", status | _] = String.split(first, " ")
      {:ok, json} = JSON.decode_line(response <> "\n")
      {String.to_integer(status), json}
    end
  end

  defp receive_all(socket, bytes) do
    case :ssl.recv(socket, 0, 3_000) do
      {:ok, chunk} -> receive_all(socket, bytes <> chunk)
      {:error, :closed} -> bytes
    end
  end

  defp wait_pending(relay, deadline) do
    if map_size(:sys.get_state(relay).store.engine.pending_enrollments) == 0 do
      if System.monotonic_time(:millisecond) >= deadline,
        do: flunk("request did not commit before lost reply")

      Process.sleep(10)
      wait_pending(relay, deadline)
    else
      :ok
    end
  end
end
