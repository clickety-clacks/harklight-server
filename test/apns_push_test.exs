defmodule AttentionServer.APNSPushTest.Provider do
  @moduledoc false

  def init(opts), do: opts

  def call(conn, {test_pid, response_agent}) do
    {:ok, body, conn} = Plug.Conn.read_body(conn)
    send(test_pid, {:apns_request, conn.method, conn.request_path, conn.req_headers, body})
    {status, response_body} = Agent.get(response_agent, & &1)
    Plug.Conn.send_resp(conn, status, response_body)
  end
end

defmodule AttentionServer.APNSPushTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AttentionServer.{
    APNS,
    DeviceIdentity,
    Engine,
    JSON,
    OriginWire,
    Relay,
    Server,
    TLSFixtures
  }

  alias Exqlite.Sqlite3

  @provider_profile "harklight-sandbox"
  @production_profile "harklight-production"
  @device_token String.duplicate("ab", 32)

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "attention-apns-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    key_path = Path.join(dir, "AuthKey_TESTKEYID1.p8")
    production_key_path = Path.join(dir, "AuthKey_TESTKEYID2.p8")

    for path <- [key_path, production_key_path] do
      {_output, 0} =
        System.cmd("openssl", [
          "genpkey",
          "-algorithm",
          "EC",
          "-pkeyopt",
          "ec_paramgen_curve:P-256",
          "-out",
          path
        ])

      assert :ok = File.chmod(path, 0o600)
    end

    {:ok, response_agent} = Agent.start_link(fn -> {200, ""} end)
    on_exit(fn -> if Process.alive?(response_agent), do: Agent.stop(response_agent) end)

    port = available_port()

    {:ok, provider} =
      Bandit.start_link(
        plug: {AttentionServer.APNSPushTest.Provider, {self(), response_agent}},
        scheme: :https,
        ip: {127, 0, 0, 1},
        port: port,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        startup_log: false
      )

    Process.unlink(provider)
    on_exit(fn -> if Process.alive?(provider), do: Supervisor.stop(provider) end)

    %{
      certs: certs,
      dir: dir,
      key_path: key_path,
      production_key_path: production_key_path,
      provider_port: port,
      provider_response: response_agent
    }
  end

  test "a provider transport failure keeps retry identity and emits retrievable safe cause",
       ctx do
    %{certs: certs, key_path: key_path} = ctx
    claim_id = op(98)
    now = System.system_time(:millisecond)
    refused_port = available_port()

    config =
      APNS.prepare!(
        key_path: key_path,
        key_id: "TESTKEYID1",
        team_id: "TESTTEAMID",
        endpoint: %{
          address: {127, 0, 0, 1},
          hostname: "relay.test",
          port: refused_port,
          cacerts: [ca_der(certs)]
        }
      )

    claim = %{
      claim_id: claim_id,
      profile_id: @provider_profile,
      address: @device_token,
      deadline_at: now + 60_000,
      registered_at: now,
      presentation: :alert
    }

    log =
      capture_log(fn ->
        case APNS.send(config, claim, nil) do
          {%{state: "retry", reason: nil}, _token_cache} -> :ok
          _ -> flunk("expected provider retry outcome")
        end
      end)

    assert log =~ "event=harklight_failure"
    assert log =~ "operation=apns_dispatch"
    assert log =~ "phase=push_request"
    assert log =~ "cause=connection_refused"
    assert log =~ "outcome=retry"
    assert log =~ "operation_id=#{claim_id}"
    refute log =~ @device_token
    refute log =~ "AuthKey_TESTKEYID1"
  end

  test "alert payload fitting respects APNs byte limits for long alert titles", ctx do
    %{certs: certs, key_path: key_path, provider_port: provider_port} = ctx
    now = System.system_time(:millisecond)

    config =
      APNS.prepare!(
        key_path: key_path,
        key_id: "TESTKEYID1",
        team_id: "TESTTEAMID",
        endpoint: %{
          address: {127, 0, 0, 1},
          hostname: "relay.test",
          port: provider_port,
          cacerts: [ca_der(certs)]
        }
      )

    locator = %{
      "v" => 1,
      "binding_id" => op(94),
      "person_id" => "person-a",
      "device_id" => String.duplicate("a", 64),
      "sender_id" => "app:test",
      "message_id" => "bounded-preview",
      "acceptance_seq" => 7
    }

    body_preview = String.duplicate("名", 160)

    claim = %{
      claim_id: op(97),
      profile_id: @provider_profile,
      address: @device_token,
      deadline_at: now + 60_000,
      registered_at: now,
      presentation: :alert,
      alert: %{
        "title" => String.duplicate("名", 5_000),
        "body" => body_preview,
        "body_truncated" => false
      },
      locator: locator
    }

    {outcome, _cached_token} = APNS.send(config, claim, nil)
    assert outcome.state == "provider_accepted"
    assert_receive {:apns_request, "POST", _path, _headers, body}, 12_000
    assert byte_size(body) <= 4_096

    assert {:ok, %{"aps" => %{"alert" => %{"body" => ^body_preview, "title" => title}}} = payload} =
             JSON.decode_line(body <> "\n")

    assert String.ends_with?(title, "…")
    assert payload["attention"]["locator"] == locator

    legacy_claim =
      claim
      |> Map.delete(:locator)
      |> Map.put(:claim_id, op(93))
      |> Map.put(:alert, %{"title" => "Legacy sender", "body" => "Legacy preview"})

    {legacy_outcome, _cached_token} = APNS.send(config, legacy_claim, nil)
    assert legacy_outcome.state == "provider_accepted"
    assert_receive {:apns_request, "POST", _path, _headers, legacy_body}, 12_000
    assert byte_size(legacy_body) <= 4_096
    assert {:ok, legacy_payload} = JSON.decode_line(legacy_body <> "\n")

    assert legacy_payload == %{
             "aps" => %{
               "alert" => %{"title" => "Legacy sender", "body" => "Legacy preview"},
               "content-available" => 1
             },
             "attention" => %{"v" => 1, "type" => "sync"}
           }
  end

  test "exact grouped names stay intact in APNs and oversized names are not truncated", ctx do
    %{certs: certs, key_path: key_path, provider_port: provider_port} = ctx
    now = System.system_time(:millisecond)

    config =
      APNS.prepare!(
        key_path: key_path,
        key_id: "TESTKEYID1",
        team_id: "TESTTEAMID",
        endpoint: %{
          address: {127, 0, 0, 1},
          hostname: "relay.test",
          port: provider_port,
          cacerts: [ca_der(certs)]
        }
      )

    exact_names = String.duplicate("名", 90) <> "A, " <> String.duplicate("界", 90) <> "B"

    claim = %{
      claim_id: op(92),
      profile_id: @provider_profile,
      address: @device_token,
      deadline_at: now + 60_000,
      registered_at: now,
      presentation: :alert,
      alert: %{"title" => "2 new requests", "body" => exact_names},
      preserve_alert_body: true
    }

    {outcome, _cached_token} = APNS.send(config, claim, nil)
    assert outcome.state == "provider_accepted"
    assert_receive {:apns_request, "POST", _path, _headers, body}, 12_000
    assert byte_size(body) <= 4_096
    assert {:ok, %{"aps" => %{"alert" => %{"body" => ^exact_names}}}} =
             JSON.decode_line(body <> "\n")

    oversized_names = String.duplicate("untruncated-name-", 260)
    oversized_claim = %{claim | claim_id: op(91), alert: %{"title" => "2 new requests", "body" => oversized_names}}

    log =
      capture_log(fn ->
        {oversized_outcome, _cached_token} = APNS.send(config, oversized_claim, nil)
        send(self(), {:oversized_outcome, oversized_outcome.state})
      end)

    assert_receive {:oversized_outcome, "blocked_configuration"}
    refute_receive {:apns_request, "POST", _, _, _}, 100
    refute log =~ "untruncated-name-"
  end

  test "an oversized locator is omitted whole and reported without changing push status", ctx do
    %{certs: certs, key_path: key_path, provider_port: provider_port} = ctx

    config =
      APNS.prepare!(
        key_path: key_path,
        key_id: "TESTKEYID1",
        team_id: "TESTTEAMID",
        endpoint: %{
          address: {127, 0, 0, 1},
          hostname: "relay.test",
          port: provider_port,
          cacerts: [ca_der(certs)]
        }
      )

    person = "person-a"
    device = String.duplicate("b", 64)
    origin = String.duplicate("a", 64)
    message_id = String.duplicate("m", 5_000)

    {:ok, engine} = Engine.setup_person(Engine.new(), person)
    {:ok, engine} = Engine.setup_device(engine, person, device)
    {:ok, engine} = Engine.setup_device(engine, person, origin)
    {:ok, engine} = Engine.setup_sender(engine, origin, "app:test")

    register = %{
      "v" => 1,
      "type" => "push_register",
      "op_id" => op(96),
      "revision" => 1,
      "profile_id" => @provider_profile,
      "address_kind" => "apns_token",
      "address" => @device_token
    }

    {engine, {:ok, register_reply}} =
      Engine.request_line(engine, device, JSON.encode!(register) <> "\n")

    assert {:ok, %{"binding_id" => binding_id}} = JSON.decode_line(register_reply)

    forward = %{
      "v" => 1,
      "type" => "forward",
      "op_id" => op(95),
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => message_id,
        "created_at" => 1,
        "title" => "Long identity fixture"
      }
    }

    {engine, {:ok, forward_reply}} =
      Engine.request_line(engine, origin, JSON.encode!(forward) <> "\n")

    assert {:ok, %{"seq" => 1}} = JSON.decode_line(forward_reply)

    assert {:ok, engine, claim} =
             Engine.claim_push(engine, System.system_time(:millisecond) + 300_001)

    assert claim.binding_id == binding_id
    assert claim.locator["person_id"] == person
    assert claim.locator["device_id"] == device
    assert claim.locator["sender_id"] == "app:test"
    assert claim.locator["message_id"] == message_id
    assert claim.locator["acceptance_seq"] == 1

    test_process = self()

    log =
      capture_log(fn ->
        send(test_process, {:locator_send, APNS.send(config, claim, nil)})
      end)

    assert_receive {:locator_send, {outcome, _cached_token}}, 0

    assert outcome.state == "provider_accepted"
    refute Map.has_key?(outcome, :tap_locator)
    assert log =~ "event=harklight_apns_tap_locator outcome=omitted_payload_limit"
    refute log =~ message_id
    assert_receive {:apns_request, "POST", _path, _headers, body}, 12_000
    assert byte_size(body) <= 4_096
    assert {:ok, payload} = JSON.decode_line(body <> "\n")
    assert payload["aps"]["alert"]["body"] == "Long identity fixture"
    assert payload["aps"]["badge"] == 1
    assert payload["attention"] == %{"v" => 1, "type" => "sync"}
    refute Map.has_key?(payload["attention"], "locator")

    assert {:ok, engine} =
             Engine.finish_push(engine, claim, outcome, System.system_time(:millisecond) + 1)

    {_engine, {:ok, status_reply}} =
      Engine.request_line(
        engine,
        device,
        JSON.encode!(%{"v" => 1, "type" => "push_status"}) <> "\n"
      )

    assert {:ok, status} = JSON.decode_line(status_reply)
    assert status["push"]["state"] == "ready"
    refute Map.has_key?(status["push"]["last_outcome"], "tap_locator")
  end

  test "authenticated token registration survives restart and alerts with sender identity and locator",
       ctx do
    %{certs: certs, dir: dir, key_path: key_path, provider_port: provider_port} = ctx
    store_path = Path.join(dir, "relay.sqlite3")

    endpoint = %{
      address: {127, 0, 0, 1},
      hostname: "relay.test",
      port: provider_port,
      cacerts: [ca_der(certs)]
    }

    apns = [key_path: key_path, key_id: "TESTKEYID1", team_id: "TESTTEAMID", endpoint: endpoint]
    assert %{profiles: %{@provider_profile => %{private_key: _}}} = APNS.prepare!(apns)
    assert APNS.profile_id?(@provider_profile)
    assert APNS.profile_id?(@production_profile)
    refute APNS.profile_id?("harklight-unknown")

    {:ok, first_server} =
      Server.start_link(server_options(certs, store_path) ++ [apns: apns])

    Process.unlink(first_server)
    on_exit(fn -> if Process.alive?(first_server), do: Supervisor.stop(first_server) end)
    assert :ok = Supervisor.terminate_child(first_server, :push_dispatcher)

    relay = Server.relay(first_server)
    assert :ok = Relay.setup_person(relay, "person-a")

    assert {:ok, a1_key} =
             Relay.setup_device_certificate(
               relay,
               "person-a",
               TLSFixtures.certificate_der!(certs, "a1")
             )

    assert {:ok, a2_key} =
             Relay.setup_device_certificate(
               relay,
               "person-a",
               TLSFixtures.certificate_der!(certs, "a2")
             )

    assert :ok = Relay.setup_sender(relay, a1_key, "app:test")

    assert {:ok, unknown_key} =
             DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))

    assert {:error, :forbidden} =
             Relay.request_line(
               relay,
               unknown_key,
               JSON.encode!(%{
                 "v" => 1,
                 "type" => "push_register",
                 "op_id" => op(90),
                 "revision" => 1,
                 "profile_id" => @provider_profile,
                 "address_kind" => "apns_token",
                 "address" => @device_token
               }) <> "\n"
             )

    {:ok, sender_socket} = connect(first_server, certs, "a1")
    assert upgrade(sender_socket) =~ "101 Switching Protocols"
    assert {[], %{"seq" => 0}} = resync(sender_socket)

    {:ok, app_socket} = connect(first_server, certs, "a2")
    assert upgrade(app_socket) =~ "101 Switching Protocols"
    assert {[], %{"seq" => 0}} = resync(app_socket)

    assert :ok =
             send_request(app_socket, %{
               "type" => "push_register",
               "ref" => "register",
               "op_id" => op(1),
               "revision" => 1,
               "profile_id" => @provider_profile,
               "address_kind" => "apns_token",
               "address" => String.upcase(@device_token)
             })

    register_reply = reply(app_socket)

    assert %{
             "type" => "ok",
             "ref" => "register",
             "revision" => 1,
             "binding_id" => binding_id,
             "profile_id" => @provider_profile
           } = register_reply

    assert AttentionServer.Message.uuid_v4?(binding_id)
    refute Map.has_key?(register_reply, "address")

    assert :ok = send_request(app_socket, %{"type" => "push_status", "ref" => "status"})
    status_reply = reply(app_socket)

    assert %{
             "type" => "ok",
             "push" => %{
               "state" => "ready",
               "revision" => 1,
               "binding_id" => ^binding_id,
               "profile_id" => @provider_profile
             }
           } = status_reply

    refute Map.has_key?(status_reply["push"], "address")

    assert :ok =
             send_request(sender_socket, %{
               "type" => "push_register",
               "op_id" => op(2),
               "revision" => 1,
               "profile_id" => @provider_profile,
               "address_kind" => "apns_token",
               "address" => @device_token
             })

    assert %{"type" => "error", "reason" => "push_address_in_use"} = reply(sender_socket)

    accept_test_delivery(
      relay,
      a1_key,
      sender_socket,
      3,
      "apns-delivery",
      %{
        "body" => "  bounded   preview\nfrom sender  ",
        "display_name" => "  Agent\nλ / name  "
      }
    )

    refute_receive {:apns_request, _, _, _}, 100

    :ok = :ssl.close(sender_socket)
    :ok = :ssl.close(app_socket)
    assert :ok = Supervisor.stop(first_server)
    seed_legacy_push_format16_past_window(store_path)

    {:ok, server} = Server.start_link(server_options(certs, store_path) ++ [apns: apns])
    Process.unlink(server)
    on_exit(fn -> if Process.alive?(server), do: Supervisor.stop(server) end)

    assert_receive {:apns_request, method, path, headers, body}, 12_000
    assert method == "POST"
    assert path == "/3/device/#{@device_token}"
    headers = Map.new(headers)
    assert headers["apns-topic"] == "co.clicketyclacks.Harklight"
    assert headers["apns-push-type"] == "alert"
    assert headers["apns-priority"] == "10"
    assert headers["apns-collapse-id"] == "attention-sync"
    assert String.starts_with?(headers["authorization"], "bearer ")
    assert {:ok, payload} = JSON.decode_line(body <> "\n")

    assert payload == %{
             "aps" => %{
               "alert" => %{
                 "title" => "  Agent\nλ / name  ",
                 "body" => "bounded preview from sender"
               },
               "badge" => 1,
               "content-available" => 1
             },
             "attention" => %{
               "v" => 1,
               "type" => "sync",
               "locator" => %{
                 "v" => 1,
                 "binding_id" => binding_id,
                 "person_id" => "person-a",
                 "device_id" => a2_key,
                 "sender_id" => "app:test",
                 "message_id" => "apns-delivery",
                 "acceptance_seq" => 1
               }
             }
           }

    refute body =~ "private title"
    refute body =~ @device_token

    jwt = String.replace_prefix(headers["authorization"], "bearer ", "")
    [header64, claims64, signature64] = String.split(jwt, ".")
    assert {:ok, header_json} = Base.url_decode64(header64, padding: false)
    assert {:ok, claims_json} = Base.url_decode64(claims64, padding: false)
    assert {:ok, signature} = Base.url_decode64(signature64, padding: false)

    assert {:ok, %{"alg" => "ES256", "kid" => "TESTKEYID1"}} =
             JSON.decode_line(header_json <> "\n")

    assert {:ok, %{"iss" => "TESTTEAMID", "iat" => iat}} = JSON.decode_line(claims_json <> "\n")
    assert is_integer(iat)
    assert byte_size(signature) == 64
    assert_jwt_signature(key_path, dir, header64 <> "." <> claims64, signature)

    relay = Server.relay(server)
    assert {:ok, app_socket} = connect(server, certs, "a2")
    assert upgrade(app_socket) =~ "101 Switching Protocols"
    assert {[%{"type" => "delivery", "seq" => 1}], %{"seq" => 1}} = resync(app_socket)

    assert_push_state(app_socket, "ready")

    assert :ok =
             send_request(app_socket, %{
               "type" => "push_register",
               "ref" => "register-retry",
               "op_id" => op(1),
               "revision" => 1,
               "profile_id" => @provider_profile,
               "address_kind" => "apns_token",
               "address" => String.upcase(@device_token)
             })

    assert %{
             "type" => "ok",
             "ref" => "register-retry",
             "revision" => 1,
             "binding_id" => ^binding_id,
             "profile_id" => @provider_profile,
             "duplicate" => true
           } = reply(app_socket)

    assert :ok =
             send_request(app_socket, %{
               "type" => "push_register",
               "op_id" => op(1),
               "revision" => 1,
               "profile_id" => @provider_profile,
               "address_kind" => "apns_token",
               "address" => String.duplicate("cd", 32)
             })

    assert %{"type" => "error", "reason" => "operation_conflict"} = reply(app_socket)

    assert :ok = Supervisor.terminate_child(server, :push_dispatcher)

    assert :ok =
             send_request(app_socket, %{
               "type" => "answer",
               "op_id" => op(4),
               "sender_id" => "app:test",
               "message_id" => "apns-delivery",
               "answer" => "received"
             })

    assert %{"type" => "ok", "seq" => 2} = reply(app_socket)
    assert %{"type" => "lifecycle", "seq" => 2} = reply(app_socket)

    assert {:ok, nil} = Relay.claim_push(relay, System.system_time(:millisecond) + 1_000)
    assert {:ok, claim} = Relay.claim_push(relay, System.system_time(:millisecond) + 300_001)
    assert claim.high == 2
    assert claim.address == @device_token
    assert claim.presentation == :background
    assert claim.badge == 0

    {outcome, _token} = APNS.send(APNS.prepare!(apns), claim, nil)
    assert outcome.state == "provider_accepted"
    assert_receive {:apns_request, "POST", _path, quiet_headers, quiet_body}, 12_000
    quiet_headers = Map.new(quiet_headers)
    assert quiet_headers["apns-push-type"] == "alert"
    assert quiet_headers["apns-priority"] == "10"
    assert quiet_headers["apns-collapse-id"] == "attention-terminal-sync"
    assert {:ok, quiet_payload} = JSON.decode_line(quiet_body <> "\n")

    assert quiet_payload == %{
             "aps" => %{"badge" => 0, "content-available" => 1},
             "attention" => %{"v" => 1, "type" => "sync"}
           }

    assert :ok =
             Relay.finish_push(
               relay,
               claim,
               outcome,
               System.system_time(:millisecond) + 300_002
             )

    assert :ok =
             send_request(app_socket, %{
               "type" => "push_remove",
               "op_id" => op(5),
               "revision" => 2
             })

    assert %{"type" => "ok", "revision" => 2} = reply(app_socket)
    assert :ok = send_request(app_socket, %{"type" => "push_status"})

    assert %{"type" => "ok", "push" => %{"state" => "canceled", "revision" => 2}} =
             reply(app_socket)

    :ssl.close(app_socket)
  end

  test "each registered profile persists and routes its push to the matching APNs environment",
       ctx do
    %{
      certs: certs,
      dir: dir,
      key_path: key_path,
      production_key_path: production_key_path,
      provider_port: sandbox_port
    } = ctx

    production_port = available_port()
    parent = self()

    production_receiver =
      spawn(fn ->
        receive do
          request -> send(parent, {:production_apns_request, request})
        end
      end)

    on_exit(fn ->
      if Process.alive?(production_receiver), do: Process.exit(production_receiver, :kill)
    end)

    {:ok, production_provider} =
      Bandit.start_link(
        plug:
          {AttentionServer.APNSPushTest.Provider, {production_receiver, ctx.provider_response}},
        scheme: :https,
        ip: {127, 0, 0, 1},
        port: production_port,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        startup_log: false
      )

    Process.unlink(production_provider)

    on_exit(fn ->
      if Process.alive?(production_provider), do: Supervisor.stop(production_provider)
    end)

    endpoint = fn port ->
      %{address: {127, 0, 0, 1}, hostname: "relay.test", port: port, cacerts: [ca_der(certs)]}
    end

    apns = [
      team_id: "TESTTEAMID",
      profiles: %{
        @provider_profile => [key_path: key_path, key_id: "TESTKEYID1"],
        @production_profile => [key_path: production_key_path, key_id: "TESTKEYID2"]
      },
      endpoint: %{
        @provider_profile => endpoint.(sandbox_port),
        @production_profile => endpoint.(production_port)
      }
    ]

    store_path = Path.join(dir, "profile-routing.sqlite3")
    {:ok, server} = Server.start_link(server_options(certs, store_path) ++ [apns: apns])
    Process.unlink(server)
    on_exit(fn -> if Process.alive?(server), do: Supervisor.stop(server) end)
    assert :ok = Supervisor.terminate_child(server, :push_dispatcher)

    relay = Server.relay(server)
    {sender_key, sandbox_key} = setup_test_devices(relay, certs)

    assert {:ok, production_key} =
             Relay.setup_device_certificate(
               relay,
               "person-a",
               TLSFixtures.certificate_der!(certs, "unknown")
             )

    sandbox_register =
      request_frame(relay, sandbox_key, %{
        "type" => "push_register",
        "op_id" => op(81),
        "revision" => 1,
        "profile_id" => @provider_profile,
        "address_kind" => "apns_token",
        "address" => @device_token
      })

    production_register =
      request_frame(relay, production_key, %{
        "type" => "push_register",
        "op_id" => op(82),
        "revision" => 1,
        "profile_id" => @production_profile,
        "address_kind" => "apns_token",
        "address" => String.duplicate("cd", 32)
      })

    assert %{
             "type" => "ok",
             "revision" => 1,
             "profile_id" => @provider_profile,
             "binding_id" => sandbox_binding
           } = sandbox_register

    assert %{
             "type" => "ok",
             "revision" => 1,
             "profile_id" => @production_profile,
             "binding_id" => production_binding
           } = production_register

    assert %{"push" => %{"profile_id" => @provider_profile, "binding_id" => ^sandbox_binding}} =
             request_frame(relay, sandbox_key, %{"type" => "push_status"})

    assert %{
             "push" => %{"profile_id" => @production_profile, "binding_id" => ^production_binding}
           } =
             request_frame(relay, production_key, %{"type" => "push_status"})

    assert %{"type" => "error", "reason" => "invalid_field", "field" => "profile_id"} =
             request_frame(relay, production_key, %{
               "type" => "push_register",
               "op_id" => op(83),
               "revision" => 2,
               "profile_id" => "harklight-unknown",
               "address_kind" => "apns_token",
               "address" => String.duplicate("ef", 32)
             })

    assert %{"type" => "ok", "seq" => 1} =
             request_frame(relay, sender_key, %{
               "type" => "forward",
               "op_id" => op(84),
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "per-device-apns-environment",
                 "created_at" => 1,
                 "title" => "Environment routing"
               }
             })

    config = APNS.prepare!(apns)
    send_at = System.system_time(:millisecond) + 300_001

    for _binding <- [sandbox_binding, production_binding] do
      assert {:ok, claim} = Relay.claim_push(relay, send_at)
      {outcome, _token} = APNS.send(config, claim, nil)
      assert outcome.state == "provider_accepted"
      assert :ok = Relay.finish_push(relay, claim, outcome, send_at + 1)
    end

    assert {:ok, nil} = Relay.claim_push(relay, send_at + 2)

    assert_receive {:apns_request, "POST", "/3/device/#{@device_token}", sandbox_headers, _body},
                   12_000

    assert_receive {:production_apns_request,
                    {:apns_request, "POST", "/3/device/" <> production_token, production_headers,
                     _body}},
                   12_000

    assert production_token == String.duplicate("cd", 32)
    assert jwt_key_id(sandbox_headers) == "TESTKEYID1"
    assert jwt_key_id(production_headers) == "TESTKEYID2"

    assert :ok = Supervisor.stop(server)
    {:ok, restarted} = Server.start_link(server_options(certs, store_path) ++ [apns: apns])
    Process.unlink(restarted)
    on_exit(fn -> if Process.alive?(restarted), do: Supervisor.stop(restarted) end)

    restarted_relay = Server.relay(restarted)

    assert %{"push" => %{"profile_id" => @provider_profile, "binding_id" => ^sandbox_binding}} =
             request_frame(restarted_relay, sandbox_key, %{"type" => "push_status"})

    assert %{
             "push" => %{"profile_id" => @production_profile, "binding_id" => ^production_binding}
           } =
             request_frame(restarted_relay, production_key, %{"type" => "push_status"})
  end

  test "a production binding never falls back to the sandbox key", ctx do
    config = APNS.prepare!(apns_options(ctx))
    now = System.system_time(:millisecond)

    claim = %{
      claim_id: op(85),
      profile_id: @production_profile,
      address: String.duplicate("cd", 32),
      deadline_at: now + 60_000,
      registered_at: now,
      presentation: :alert
    }

    capture_log(fn ->
      assert {%{state: "blocked_configuration", http_status: nil, request_id: nil}, nil} =
               APNS.send(config, claim, nil)
    end)

    refute_receive {:apns_request, _, _, _, _}, 100
  end

  test "provider error text cannot echo the token through durable status", ctx do
    %{certs: certs, dir: dir, provider_response: response_agent} = ctx
    Agent.update(response_agent, fn _ -> {400, JSON.encode!(%{"reason" => @device_token})} end)

    store_path = Path.join(dir, "reason-redaction.sqlite3")
    server = start_apns_server(ctx, store_path)
    assert :ok = Supervisor.terminate_child(server, :push_dispatcher)
    relay = Server.relay(server)
    {sender_key, _app_key} = setup_test_devices(relay, certs)
    sender_socket = open_test_socket(server, certs, "a1")
    app_socket = open_test_socket(server, certs, "a2")

    assert %{"type" => "ok", "revision" => 1} = register_push(app_socket, 21, 1)
    accept_test_delivery(relay, sender_key, sender_socket, 40, "reason-redaction")
    assert %{"type" => "delivery", "seq" => 1} = reply(app_socket)
    send_push_after_window(ctx, relay)

    assert_receive {:apns_request, "POST", _path, _headers, _body}, 12_000
    assert_push_state(app_socket, "blocked_configuration")

    status = read_push_status(app_socket)
    refute Map.has_key?(status["push"]["last_outcome"], "reason")
    refute JSON.encode!(status) =~ @device_token

    :ok = :ssl.close(sender_socket)
    :ok = :ssl.close(app_socket)
    assert :ok = Supervisor.stop(server)

    seed_legacy_provider_reason(store_path, @device_token)

    server = start_apns_server(ctx, store_path)
    app_socket = open_test_socket(server, certs, "a2")
    status = read_push_status(app_socket)

    assert status["push"]["state"] == "blocked_configuration"
    refute Map.has_key?(status["push"]["last_outcome"], "reason")
    refute Map.has_key?(status["push"], "address")
    refute JSON.encode!(status) =~ @device_token

    :ok = :ssl.close(app_socket)
    assert :ok = Supervisor.stop(server)
  end

  test "coalesced and retried hints alert only for newly unresolved attention", ctx do
    store_path = Path.join(ctx.dir, "presentation.sqlite3")
    server = start_apns_server(ctx, store_path)
    assert :ok = Supervisor.terminate_child(server, :push_dispatcher)
    relay = Server.relay(server)
    {sender, recipient} = setup_test_devices(relay, ctx.certs)

    forward = fn relay, id, operation ->
      request_frame(relay, sender, %{
        "type" => "forward",
        "op_id" => op(operation),
        "message" => %{
          "sender_id" => "app:test",
          "message_id" => id,
          "created_at" => 1,
          "title" => "private title"
        }
      })
    end

    answer = fn relay, id, operation ->
      request_frame(relay, recipient, %{
        "type" => "answer",
        "op_id" => op(operation),
        "sender_id" => "app:test",
        "message_id" => id,
        "answer" => "done"
      })
    end

    accepted = %{
      state: "provider_accepted",
      http_status: 200,
      request_id: nil,
      reason: nil,
      retry_after_ms: 0,
      invalidated_at_ms: nil
    }

    retry = %{accepted | state: "retry", http_status: nil}

    assert %{"type" => "ok", "revision" => 1} =
             request_frame(relay, recipient, %{
               "type" => "push_register",
               "op_id" => op(60),
               "revision" => 1,
               "profile_id" => @provider_profile,
               "address_kind" => "apns_token",
               "address" => @device_token
             })

    assert %{"seq" => 1} = forward.(relay, "first", 61)
    assert %{"seq" => 2} = answer.(relay, "first", 62)
    now = System.system_time(:millisecond) + 300_001

    assert {:ok, quiet} = Relay.claim_push(relay, now)
    assert quiet.high == 2
    assert quiet.presentation == :background
    assert :ok = Relay.finish_push(relay, quiet, retry, now + 1)

    assert :ok = Supervisor.stop(server)
    server = start_apns_server(ctx, store_path)
    assert :ok = Supervisor.terminate_child(server, :push_dispatcher)
    relay = Server.relay(server)

    assert %{"seq" => 3} = forward.(relay, "second", 63)
    assert {:ok, mixed} = Relay.claim_push(relay, now + 300_001)
    assert mixed.high == 3
    assert mixed.presentation == :alert
    assert :ok = Relay.finish_push(relay, mixed, retry, now + 300_002)

    assert %{"seq" => 4} = answer.(relay, "second", 64)
    assert {:ok, answered} = Relay.claim_push(relay, now + 600_003)
    assert answered.high == 4
    assert answered.presentation == :background
    assert :ok = Relay.finish_push(relay, answered, accepted, now + 600_004)
  end

  test "terminal sync does not re-alert an older unresolved delivery", ctx do
    server = start_apns_server(ctx, Path.join(ctx.dir, "older-unresolved.sqlite3"))
    assert :ok = Supervisor.terminate_child(server, :push_dispatcher)
    relay = Server.relay(server)
    {sender, recipient} = setup_test_devices(relay, ctx.certs)

    assert %{"type" => "ok"} =
             request_frame(relay, recipient, %{
               "type" => "push_register",
               "op_id" => op(70),
               "revision" => 1,
               "profile_id" => @provider_profile,
               "address_kind" => "apns_token",
               "address" => @device_token
             })

    message = fn id, operation ->
      request_frame(relay, sender, %{
        "type" => "forward",
        "op_id" => op(operation),
        "message" => %{
          "sender_id" => "app:test",
          "message_id" => id,
          "created_at" => 1,
          "title" => "private title"
        }
      })
    end

    assert %{"seq" => 1} = message.("older", 71)
    now = System.system_time(:millisecond) + 300_001
    assert {:ok, first} = Relay.claim_push(relay, now)
    assert first.presentation == :alert

    assert :ok =
             Relay.finish_push(
               relay,
               first,
               %{state: "provider_accepted", http_status: 200},
               now + 1
             )

    assert %{"seq" => 2} = message.("already-answered", 72)

    assert %{"seq" => 3} =
             request_frame(relay, recipient, %{
               "type" => "answer",
               "op_id" => op(73),
               "sender_id" => "app:test",
               "message_id" => "already-answered",
               "answer" => "done"
             })

    assert {:ok, terminal} = Relay.claim_push(relay, now + 300_001)
    assert terminal.high == 3
    assert terminal.presentation == :background
    assert terminal.badge == 1
  end

  test "BadDeviceToken clears the durable binding, stops retrying, and releases its address",
       ctx do
    %{certs: certs, dir: dir, provider_response: response_agent} = ctx

    Agent.update(response_agent, fn _ ->
      {400, JSON.encode!(%{"reason" => "BadDeviceToken"})}
    end)

    store_path = Path.join(dir, "invalid-token.sqlite3")
    server = start_apns_server(ctx, store_path)
    assert :ok = Supervisor.terminate_child(server, :push_dispatcher)
    relay = Server.relay(server)
    {sender_key, _app_key} = setup_test_devices(relay, certs)
    sender_socket = open_test_socket(server, certs, "a1")
    app_socket = open_test_socket(server, certs, "a2")

    assert %{"type" => "ok", "revision" => 1} = register_push(app_socket, 31, 1)
    accept_test_delivery(relay, sender_key, sender_socket, 40, "bad-device-token")
    assert %{"type" => "delivery", "seq" => 1} = reply(app_socket)
    send_push_after_window(ctx, relay)

    assert_receive {:apns_request, "POST", _path, _headers, _body}, 12_000
    assert_push_state(app_socket, "endpoint_invalid")

    status = read_push_status(app_socket)
    assert get_in(status, ["push", "last_outcome", "reason"]) == "BadDeviceToken"
    refute Map.has_key?(status["push"], "address")
    refute JSON.encode!(status) =~ @device_token
    assert {:ok, nil} = Relay.claim_push(relay, System.system_time(:millisecond) + 86_400_000)
    refute_receive {:apns_request, _, _, _, _}, 1_100

    :ok = :ssl.close(sender_socket)
    :ok = :ssl.close(app_socket)
    assert :ok = Supervisor.stop(server)

    server = start_apns_server(ctx, store_path)
    assert :ok = Supervisor.terminate_child(server, :push_dispatcher)
    relay = Server.relay(server)
    app_socket = open_test_socket(server, certs, "a2")
    status = read_push_status(app_socket)

    assert status["push"]["state"] == "endpoint_invalid"
    assert get_in(status, ["push", "last_outcome", "reason"]) == "BadDeviceToken"
    refute Map.has_key?(status["push"], "address")
    assert {:ok, nil} = Relay.claim_push(relay, System.system_time(:millisecond) + 86_400_000)

    sender_socket = open_test_socket(server, certs, "a1")
    assert %{"type" => "ok", "revision" => 1} = register_push(sender_socket, 32, 1)
    refute_receive {:apns_request, _, _, _, _}, 1_100

    :ok = :ssl.close(sender_socket)
    :ok = :ssl.close(app_socket)
    assert :ok = Supervisor.stop(server)
  end

  test "an unconfigured relay refuses app token registration", %{certs: certs, dir: dir} do
    {:ok, server} = Server.start_link(server_options(certs, Path.join(dir, "disabled.sqlite3")))
    Process.unlink(server)
    on_exit(fn -> if Process.alive?(server), do: Supervisor.stop(server) end)

    relay = Server.relay(server)
    assert :ok = Relay.setup_person(relay, "person-a")

    assert {:ok, _key} =
             Relay.setup_device_certificate(
               relay,
               "person-a",
               TLSFixtures.certificate_der!(certs, "a1")
             )

    assert {:ok, key} = DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "a1"))

    frame = %{
      "v" => 1,
      "type" => "push_register",
      "op_id" => op(11),
      "revision" => 1,
      "profile_id" => @provider_profile,
      "address_kind" => "apns_token",
      "address" => @device_token
    }

    assert {:ok, response} = Relay.request_line(relay, key, JSON.encode!(frame) <> "\n")
    assert {:ok, %{"type" => "error", "reason" => "forbidden"}} = JSON.decode_line(response)
  end

  defp assert_jwt_signature(key_path, dir, signing_input, signature) do
    public_path = Path.join(dir, "AuthKey_TESTKEYID1.pub")
    input_path = Path.join(dir, "provider-token-input")
    signature_path = Path.join(dir, "provider-token-signature.der")

    {output, 0} = System.cmd("openssl", ["pkey", "-in", key_path, "-pubout", "-out", public_path])
    assert output == ""

    <<r::unsigned-big-integer-size(256), s::unsigned-big-integer-size(256)>> = signature
    assert :ok = File.write(input_path, signing_input)
    der_signature = :public_key.der_encode(:"ECDSA-Sig-Value", {:"ECDSA-Sig-Value", r, s})
    assert :ok = File.write(signature_path, der_signature)

    {verification, 0} =
      System.cmd("openssl", [
        "dgst",
        "-sha256",
        "-verify",
        public_path,
        "-signature",
        signature_path,
        input_path
      ])

    assert String.trim(verification) == "Verified OK"
  end

  defp server_options(certs, store_path) do
    [
      store_path: store_path,
      certfile: Path.join(certs, "server.pem"),
      keyfile: Path.join(certs, "server.key"),
      cacertfile: Path.join(certs, "ca.pem"),
      port: 0,
      ip: {127, 0, 0, 1},
      enroll_port: 0,
      enroll_ip: {127, 0, 0, 1},
      owner_port: 0,
      owner_ip: {127, 0, 0, 1},
      agent_port: 0,
      agent_ip: {127, 0, 0, 1}
    ]
  end

  defp apns_options(ctx) do
    endpoint = %{
      address: {127, 0, 0, 1},
      hostname: "relay.test",
      port: ctx.provider_port,
      cacerts: [ca_der(ctx.certs)]
    }

    [
      key_path: ctx.key_path,
      key_id: "TESTKEYID1",
      team_id: "TESTTEAMID",
      endpoint: endpoint
    ]
  end

  defp jwt_key_id(headers) do
    authorization = Map.new(headers) |> Map.fetch!("authorization")
    "bearer " <> jwt = authorization
    [header64, _claims64, _signature64] = String.split(jwt, ".")
    {:ok, header_json} = Base.url_decode64(header64, padding: false)
    {:ok, %{"kid" => key_id}} = JSON.decode_line(header_json <> "\n")
    key_id
  end

  defp start_apns_server(ctx, store_path) do
    {:ok, server} =
      Server.start_link(server_options(ctx.certs, store_path) ++ [apns: apns_options(ctx)])

    Process.unlink(server)
    on_exit(fn -> if Process.alive?(server), do: Supervisor.stop(server) end)
    server
  end

  defp setup_test_devices(relay, certs) do
    assert :ok = Relay.setup_person(relay, "person-a")

    assert {:ok, sender_key} =
             Relay.setup_device_certificate(
               relay,
               "person-a",
               TLSFixtures.certificate_der!(certs, "a1")
             )

    assert {:ok, app_key} =
             Relay.setup_device_certificate(
               relay,
               "person-a",
               TLSFixtures.certificate_der!(certs, "a2")
             )

    assert :ok = Relay.setup_sender(relay, sender_key, "app:test")
    {sender_key, app_key}
  end

  defp open_test_socket(server, certs, name) do
    {:ok, socket} = connect(server, certs, name)
    assert upgrade(socket) =~ "101 Switching Protocols"
    _ = resync(socket)
    socket
  end

  defp register_push(socket, operation, revision) do
    assert :ok =
             send_request(socket, %{
               "type" => "push_register",
               "op_id" => op(operation),
               "revision" => revision,
               "profile_id" => @provider_profile,
               "address_kind" => "apns_token",
               "address" => @device_token
             })

    reply(socket)
  end

  defp request_frame(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, reply} = JSON.decode_line(line)
    reply
  end

  defp accept_test_delivery(
         relay,
         sender_key,
         socket,
         operation,
         message_id,
         extra_fields \\ %{}
       ) do
    message =
      %{
        "sender_id" => "app:test",
        "message_id" => message_id,
        "created_at" => 1,
        "title" => "private title"
      }
      |> Map.merge(extra_fields)

    assert %{"type" => "ok", "seq" => 1} =
             OriginWire.command(relay, sender_key, %{
               "v" => 1,
               "type" => "send",
               "op_id" => op(operation),
               "message" => message
             })

    assert %{"type" => "delivery", "seq" => 1} = reply(socket)
  end

  defp read_push_status(socket) do
    assert :ok = send_request(socket, %{"type" => "push_status"})
    reply(socket)
  end

  # Drives one push by hand at the end of the batching window, the way the
  # dispatcher would, without waiting five minutes of wall-clock time.
  defp send_push_after_window(ctx, relay) do
    send_at = System.system_time(:millisecond) + 300_001
    assert {:ok, claim} = Relay.claim_push(relay, send_at)
    {outcome, _token} = APNS.send(APNS.prepare!(apns_options(ctx)), claim, nil)
    assert :ok = Relay.finish_push(relay, claim, outcome, send_at + 1)
    outcome
  end

  defp seed_legacy_provider_reason(path, reason) do
    assert {:ok, db} = Sqlite3.open(path)
    assert {:ok, select} = Sqlite3.prepare(db, "SELECT payload FROM relay_state WHERE id = 1")
    assert {:row, [payload]} = Sqlite3.step(db, select)
    assert :ok = Sqlite3.release(db, select)
    assert :ok = Sqlite3.close(db)

    {:attention_relay_state, _format, engine} = :erlang.binary_to_term(payload, [:safe])

    {key, binding} =
      Enum.find(engine.push_bindings, fn {_key, binding} -> binding.address == @device_token end)

    old_outcome = Map.put(binding.last_outcome, "reason", reason)
    old_binding = %{binding | last_outcome: old_outcome}
    old_engine = %{engine | push_bindings: Map.put(engine.push_bindings, key, old_binding)}

    legacy_payload =
      :erlang.term_to_binary({:attention_relay_state, 10, old_engine}, [{:compressed, 6}])

    assert {:ok, db} = Sqlite3.open(path)

    assert {:ok, update} =
             Sqlite3.prepare(
               db,
               "UPDATE relay_state SET payload = ?, checksum = ? WHERE id = 1"
             )

    assert :ok =
             Sqlite3.bind(update, [
               {:blob, legacy_payload},
               {:blob, :crypto.hash(:sha256, legacy_payload)}
             ])

    assert :done = Sqlite3.step(db, update)
    assert :ok = Sqlite3.release(db, update)
    assert :ok = Sqlite3.close(db)
  end

  # Rewrites the persisted engine as a format-16 payload (no accepted_high) and
  # moves the binding's send time into the past, as if the server had been down
  # for longer than the batching window.
  defp seed_legacy_push_format16_past_window(path) do
    assert {:ok, db} = Sqlite3.open(path)
    assert {:ok, select} = Sqlite3.prepare(db, "SELECT payload FROM relay_state WHERE id = 1")
    assert {:row, [payload]} = Sqlite3.step(db, select)
    assert :ok = Sqlite3.release(db, select)

    {:attention_relay_state, _format, engine} = :erlang.binary_to_term(payload, [:safe])
    past = System.system_time(:millisecond) - 1

    bindings =
      Map.new(engine.push_bindings, fn {key, binding} ->
        next_at = if is_integer(binding.next_at), do: past, else: nil
        {key, binding |> Map.delete(:accepted_high) |> Map.put(:next_at, next_at)}
      end)

    legacy = %{engine | push_bindings: bindings}

    legacy_payload =
      :erlang.term_to_binary({:attention_relay_state, 16, legacy}, [{:compressed, 6}])

    assert {:ok, update} =
             Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ? WHERE id = 1")

    assert :ok =
             Sqlite3.bind(update, [
               {:blob, legacy_payload},
               {:blob, :crypto.hash(:sha256, legacy_payload)}
             ])

    assert :done = Sqlite3.step(db, update)
    assert :ok = Sqlite3.release(db, update)
    assert :ok = Sqlite3.close(db)
  end

  defp connect(server, certs, name) do
    :ssl.connect({127, 0, 0, 1}, Server.port(server),
      versions: [:"tlsv1.3"],
      verify: :verify_peer,
      cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
      certfile: String.to_charlist(Path.join(certs, "#{name}.pem")),
      keyfile: String.to_charlist(Path.join(certs, "#{name}.key")),
      server_name_indication: ~c"relay.test",
      session_tickets: :disabled,
      active: false,
      mode: :binary,
      packet: :raw
    )
  end

  defp upgrade(socket) do
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

    read_http(socket, <<>>)
  end

  defp read_http(_socket, buffer) when byte_size(buffer) > 16_384, do: flunk("HTTP limit")

  defp read_http(socket, buffer) do
    if String.ends_with?(buffer, "\r\n\r\n") do
      buffer
    else
      assert {:ok, byte} = :ssl.recv(socket, 1, 2_000)
      read_http(socket, buffer <> byte)
    end
  end

  defp resync(socket) do
    assert :ok = send_request(socket, %{"type" => "resync", "since" => 0})
    assert %{"type" => "ok"} = reply(socket)
    drain_resync(socket, [])
  end

  defp drain_resync(socket, events) do
    case reply(socket) do
      %{"type" => "resynced", "seq" => high} -> {Enum.reverse(events), %{"seq" => high}}
      event -> drain_resync(socket, [event | events])
    end
  end

  defp send_request(socket, payload) do
    payload = JSON.encode!(Map.put_new(payload, "v", 1)) <> "\n"
    assert :ok = :ssl.send(socket, websocket_frame(payload))
  end

  defp websocket_frame(payload) do
    mask = :crypto.strong_rand_bytes(4)
    size = byte_size(payload)

    header =
      if size < 126,
        do: <<1::1, 0::3, 1::4, 1::1, size::7>>,
        else: <<1::1, 0::3, 1::4, 1::1, 126::7, size::16>>

    repeated = :binary.copy(mask, div(size + 3, 4))
    [header, mask, :crypto.exor(payload, binary_part(repeated, 0, size))]
  end

  defp reply(socket) do
    assert {:ok, <<1::1, 0::3, 1::4, 0::1, length::7>>} = :ssl.recv(socket, 2, 2_000)

    length =
      case length do
        126 ->
          assert {:ok, <<size::16>>} = :ssl.recv(socket, 2, 2_000)
          size

        value ->
          value
      end

    assert {:ok, payload} = :ssl.recv(socket, length, 2_000)
    assert {:ok, value} = JSON.decode_line(payload)
    value
  end

  defp available_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_address, port}} = :inet.sockname(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp ca_der(certs) do
    [{:Certificate, der, :not_encrypted}] =
      certs
      |> Path.join("ca.pem")
      |> File.read!()
      |> :public_key.pem_decode()

    der
  end

  defp assert_push_state(socket, expected, attempts \\ 50)
  defp assert_push_state(_socket, _expected, 0), do: flunk("push state did not settle")

  defp assert_push_state(socket, expected, attempts) do
    assert :ok = send_request(socket, %{"type" => "push_status"})
    reply = reply(socket)

    if get_in(reply, ["push", "state"]) == expected do
      :ok
    else
      Process.sleep(20)
      assert_push_state(socket, expected, attempts - 1)
    end
  end

  defp op(number), do: "00000000-0000-4000-8000-" <> String.pad_leading("#{number}", 12, "0")
end
