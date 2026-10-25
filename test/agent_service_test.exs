defmodule AttentionServer.AgentServiceTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    AgentHTTPS,
    AgentTLS,
    DeviceIdentity,
    HeadlessAgent,
    HeadlessTLS,
    JSON,
    OwnerTLS,
    Relay,
    TLSFixtures
  }

  @agent_id "00000000-0000-4000-8000-0000000000a1"
  @op1 "00000000-0000-4000-8000-0000000000b1"
  @op2 "00000000-0000-4000-8000-0000000000b2"
  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  setup_all do
    certs = TLSFixtures.create!()
    TLSFixtures.make_self_signed_client!(certs, "b2")
    TLSFixtures.make_self_signed_client!(certs, "other-agent")
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "attention-agent-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "state.sqlite3")
    {:ok, relay} = Relay.start_link(store_path: path)
    {:ok, owner} = listener(OwnerTLS, relay, certs)
    {:ok, agent} = listener(AgentTLS, relay, certs)
    {:ok, agent_https} = listener(AgentHTTPS, relay, certs)

    on_exit(fn ->
      for pid <- [agent_https, agent, owner, relay], Process.alive?(pid), do: GenServer.stop(pid)
    end)

    %{relay: relay, owner: owner, agent: agent, agent_https: agent_https, path: path}
  end

  test "an agent answer refreshes the waiting view and notifies supporting sessions", context do
    %{certs: certs, relay: relay, owner: owner, agent: agent} = context
    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))

    {:ok, device_key} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    assert :ok = Relay.setup_sender(relay, device_key, "app:test")

    assert %{"type" => "ok", "seq" => seq} =
             forward(relay, device_key, "app:test", "waiting", "request")

    {:ok, agent_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))

    owner_endpoint = endpoint(certs, OwnerTLS.port(owner))
    agent_endpoint = endpoint(certs, AgentTLS.port(agent))
    agent_cert = Path.join(certs, "unknown.pem")
    agent_private = Path.join(certs, "unknown.key")

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => agent_key
             })

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_grants_replace",
               "key_fingerprint" => agent_key,
               "expected_revision" => 1,
               "read_grants" => ["app:test"],
               "respond_grants" => ["app:test"]
             })

    assert {:ok, supporting} = Relay.open_websocket_session(relay, device_key, nil, nil)

    assert %{"type" => "resynced"} =
             session_request(relay, supporting, %{"type" => "resync", "since" => 0})
             |> List.last()

    before = hd(session_request(relay, supporting, %{"type" => "view_get", "ref" => "before"}))
    assert Enum.map(before["entries"], & &1["message_id"]) == ["waiting"]

    assert %{"type" => "answer_result", "ok" => true} =
             agent_request(
               agent_endpoint,
               agent_cert,
               agent_private,
               answer("app:test", "waiting", seq, unique_op(), "answered")
             )

    after_answer =
      hd(session_request(relay, supporting, %{"type" => "view_get", "ref" => "after"}))

    assert after_answer["entries"] == []
    refute after_answer["view_rev"] == before["view_rev"]

    assert [%{"type" => "view_changed", "view_rev" => notice_revision}] =
             session_drain(relay, supporting)
             |> Enum.filter(&(&1["type"] == "view_changed"))

    assert notice_revision == after_answer["view_rev"]
  end

  test "owner scopes exact content and response, cursor revision, tenant, and durable null winner",
       context do
    %{certs: certs, relay: relay, owner: owner, agent: agent, path: path} = context
    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))
    {:ok, other} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "b1"))

    {:ok, device_key} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, b_device} =
      Relay.setup_device_certificate(relay, other, TLSFixtures.certificate_der!(certs, "b2"))

    {:ok, agent_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))

    for sender <- ~w(app:test app:private app:sub/x),
        do: assert(:ok = Relay.setup_sender(relay, device_key, sender))

    assert :ok = Relay.setup_sender(relay, b_device, "app:test")

    assert %{"type" => "ok", "seq" => 1} =
             forward(relay, device_key, "app:test", "one", "printer jam")

    assert %{"type" => "ok", "seq" => 2} =
             forward(relay, device_key, "app:private", "hidden", "printer jam secret")

    assert %{"type" => "ok", "seq" => 3} =
             forward(relay, device_key, "app:sub/x", "three", "other")

    assert %{"type" => "ok", "seq" => 1} =
             forward(relay, b_device, "app:test", "one", "B printer jam")

    agent_endpoint = endpoint(certs, AgentTLS.port(agent))
    owner_endpoint = endpoint(certs, OwnerTLS.port(owner))
    cert = Path.join(certs, "unknown.pem")
    private = Path.join(certs, "unknown.key")

    assert {:error, _} =
             HeadlessTLS.agent_request(agent_endpoint, cert, private, %{
               "v" => 1,
               "type" => "search"
             })

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => agent_key
             })

    assert %{
             "ok" => true,
             "agents" => [%{"read_grants" => [], "respond_grants" => [], "grant_revision" => 1}]
           } =
             owner_request(owner_endpoint, certs, "a1", person, %{"type" => "agent_list"})

    assert {:ok, %{"items" => [], "next_cursor" => nil, "initial_high_water" => 0}} =
             HeadlessTLS.agent_request(agent_endpoint, cert, private, %{
               "v" => 1,
               "type" => "search"
             })

    assert %{"error" => "not_found"} =
             agent_request(agent_endpoint, cert, private, %{
               "v" => 1,
               "type" => "get",
               "sender_id" => "app:test",
               "message_id" => "one"
             })

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_grants_replace",
               "key_fingerprint" => agent_key,
               "expected_revision" => 1,
               "read_grants" => ["app:test", "app:sub/"],
               "respond_grants" => []
             })

    assert %{"error" => "not_found"} =
             owner_request(owner_endpoint, certs, "b1", other, %{
               "type" => "agent_grants_replace",
               "key_fingerprint" => agent_key,
               "expected_revision" => 2,
               "read_grants" => ["app:private"],
               "respond_grants" => ["app:private"]
             })

    first =
      agent_request(agent_endpoint, cert, private, %{"v" => 1, "type" => "search", "limit" => 1})

    assert %{
             "items" => [%{"sender_id" => "app:sub/x", "state" => "open"}],
             "next_cursor" => cursor
           } = first

    assert is_binary(cursor)

    second =
      agent_request(agent_endpoint, cert, private, %{
        "v" => 1,
        "type" => "search",
        "limit" => 1,
        "cursor" => cursor
      })

    assert %{
             "items" => [
               %{
                 "sender_id" => "app:test",
                 "message" => %{"unknown" => %{"text" => "printer jam"}}
               }
             ],
             "next_cursor" => nil
           } = second

    assert %{"items" => [%{"message_id" => "one"}]} =
             agent_request(agent_endpoint, cert, private, %{
               "v" => 1,
               "type" => "search",
               "contains" => "printer jam"
             })

    assert %{"error" => "not_found"} =
             agent_request(agent_endpoint, cert, private, %{
               "v" => 1,
               "type" => "get",
               "sender_id" => "app:private",
               "message_id" => "hidden"
             })

    assert %{"error" => "not_found"} =
             agent_request(agent_endpoint, cert, private, %{
               "v" => 1,
               "type" => "search",
               "sender_id" => "app:private",
               "message_id" => "hidden"
             })

    assert %{"error" => "not_found"} =
             agent_request(agent_endpoint, cert, private, %{
               "v" => 1,
               "type" => "search",
               "sender_id" => "app:test",
               "message_id" => "absent"
             })

    read_only = answer("app:test", "one", 1, @op1, nil)
    assert %{"error" => "forbidden"} = agent_request(agent_endpoint, cert, private, read_only)

    assert {:ok, staged} =
             :ssl.connect({127, 0, 0, 1}, AgentTLS.port(agent),
               versions: [:"tlsv1.3"],
               verify: :verify_peer,
               cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
               certfile: String.to_charlist(cert),
               keyfile: String.to_charlist(private),
               server_name_indication: ~c"relay.test",
               session_tickets: :disabled,
               active: false,
               mode: :binary,
               packet: :line
             )

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_grants_replace",
               "key_fingerprint" => agent_key,
               "expected_revision" => 2,
               "read_grants" => ["app:test"],
               "respond_grants" => ["app:test"]
             })

    assert :ok =
             :ssl.send(
               staged,
               JSON.encode!(%{
                 "v" => 1,
                 "type" => "get",
                 "sender_id" => "app:sub/x",
                 "message_id" => "three"
               }) <> "\n"
             )

    assert {:ok, staged_reply} = :ssl.recv(staged, 0, 3_000)
    assert {:ok, %{"error" => "not_found"}} = JSON.decode_line(staged_reply)
    :ssl.close(staged)

    {:ok, owner_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "a1"))

    assert %{grant_revision: 3, grant_history: history} =
             :sys.get_state(relay).store.engine.agents[agent_key]

    assert Enum.map(history, &{&1.revision, &1.action, &1.actor}) == [
             {1, "register", owner_key},
             {2, "replace", owner_key},
             {3, "replace", owner_key}
           ]

    assert %{"error" => "not_found"} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_grants_replace",
               "key_fingerprint" => agent_key,
               "expected_revision" => 2,
               "read_grants" => ["app:private"],
               "respond_grants" => ["app:private"]
             })

    assert :sys.get_state(relay).store.engine.agents[agent_key].grant_revision == 3

    assert %{"error" => "stale_query"} =
             agent_request(agent_endpoint, cert, private, %{
               "v" => 1,
               "type" => "search",
               "limit" => 1,
               "cursor" => cursor
             })

    assert %{"error" => "invalid_field"} =
             agent_request(agent_endpoint, cert, private, %{read_only | "deliberate" => false})

    assert %{"error" => "stale_target"} =
             agent_request(agent_endpoint, cert, private, %{read_only | "acceptance_seq" => 999})

    assert %{"type" => "answer_result", "ok" => true, "seq" => 4} =
             agent_request(agent_endpoint, cert, private, read_only)

    assert %{"type" => "answer_result", "duplicate" => true, "seq" => 4} =
             agent_request(agent_endpoint, cert, private, read_only)

    assert %{"error" => "closed"} =
             agent_request(
               agent_endpoint,
               cert,
               private,
               answer("app:test", "one", 1, @op2, "late")
             )

    assert %{
             "item" => %{
               "state" => "answered",
               "answer" => nil,
               "terminal_seq" => 4,
               "source_status" => "unreported"
             }
           } =
             agent_request(agent_endpoint, cert, private, %{
               "v" => 1,
               "type" => "get",
               "sender_id" => "app:test",
               "message_id" => "one"
             })

    assert {:ok, [origin | _], 4} = Relay.events(relay, device_key, 0)
    refute Map.has_key?(origin, "message")

    GenServer.stop(agent)
    GenServer.stop(owner)
    GenServer.stop(relay)
    {:ok, restarted} = Relay.start_link(store_path: path)
    {:ok, agent_restarted} = listener(AgentTLS, restarted, certs)

    on_exit(fn ->
      if Process.alive?(agent_restarted), do: GenServer.stop(agent_restarted)
      if Process.alive?(restarted), do: GenServer.stop(restarted)
    end)

    restarted_endpoint = endpoint(certs, AgentTLS.port(agent_restarted))

    assert %{grant_revision: 3, grant_history: persisted_history} =
             :sys.get_state(restarted).store.engine.agents[agent_key]

    assert persisted_history == history

    assert %{"type" => "answer_result", "duplicate" => true, "seq" => 4} =
             agent_request(restarted_endpoint, cert, private, read_only)

    assert %{"item" => %{"answer" => nil}} =
             agent_request(restarted_endpoint, cert, private, %{
               "v" => 1,
               "type" => "get",
               "sender_id" => "app:test",
               "message_id" => "one"
             })
  end

  test "agent admission is distinct from devices and a revoked staged TLS peer cannot disclose content",
       context do
    %{certs: certs, relay: relay, owner: owner, agent: agent} = context
    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))

    {:ok, device_key} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, agent_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))

    assert :ok = Relay.setup_sender(relay, device_key, "app:test")
    assert %{"type" => "ok"} = forward(relay, device_key, "app:test", "secret", "private text")
    agent_endpoint = endpoint(certs, AgentTLS.port(agent))
    owner_endpoint = endpoint(certs, OwnerTLS.port(owner))

    assert %{"error" => "not_found"} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => device_key
             })

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => agent_key
             })

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_grants_replace",
               "key_fingerprint" => agent_key,
               "expected_revision" => 1,
               "read_grants" => ["app:test"],
               "respond_grants" => ["app:test"]
             })

    assert {:error, :forbidden} = Relay.acknowledged_cursor(relay, agent_key)

    {:ok, expired_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "expired"))

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_register",
               "agent_id" => "00000000-0000-4000-8000-0000000000a2",
               "key_fingerprint" => expired_key
             })

    assert {:error, _} =
             HeadlessTLS.agent_request(
               agent_endpoint,
               Path.join(certs, "expired.pem"),
               Path.join(certs, "expired.key"),
               %{"v" => 1, "type" => "search"}
             )

    assert {:error, _} =
             HeadlessTLS.agent_request(
               agent_endpoint,
               Path.join(certs, "unknown.pem"),
               Path.join(certs, "a2.key"),
               %{"v" => 1, "type" => "search"}
             )

    assert {:error, _} =
             HeadlessTLS.agent_request(
               agent_endpoint,
               Path.join(certs, "a2.pem"),
               Path.join(certs, "a2.key"),
               %{"v" => 1, "type" => "get", "sender_id" => "app:test", "message_id" => "secret"}
             )

    assert {:ok, socket} =
             :ssl.connect({127, 0, 0, 1}, AgentTLS.port(agent),
               versions: [:"tlsv1.3"],
               verify: :verify_peer,
               cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
               certfile: String.to_charlist(Path.join(certs, "unknown.pem")),
               keyfile: String.to_charlist(Path.join(certs, "unknown.key")),
               server_name_indication: ~c"relay.test",
               session_tickets: :disabled,
               active: false,
               mode: :binary,
               packet: :line
             )

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_revoke",
               "key_fingerprint" => agent_key
             })

    query =
      JSON.encode!(%{
        "v" => 1,
        "type" => "get",
        "sender_id" => "app:test",
        "message_id" => "secret"
      }) <> "\n"

    assert :ok = :ssl.send(socket, query)
    assert {:error, _} = :ssl.recv(socket, 0, 3_000)
    :ssl.close(socket)

    assert {:error, _} =
             HeadlessTLS.agent_request(
               agent_endpoint,
               Path.join(certs, "unknown.pem"),
               Path.join(certs, "unknown.key"),
               %{"v" => 1, "type" => "search"}
             )
  end

  test "headless self-signed service key is usable only after exact owner registration",
       context do
    %{certs: certs, relay: relay, owner: owner, agent_https: agent_https, path: path} = context
    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))
    directory = Path.join(Path.dirname(path), "service-key")
    assert {:ok, fingerprint} = HeadlessAgent.create_identity(directory)
    assert byte_size(fingerprint) == 64
    assert {:error, :existing_identity} = HeadlessAgent.create_identity(directory)
    assert {:ok, stat} = File.stat(Path.join(directory, "identity.key"))
    assert Bitwise.band(stat.mode, 0o777) == 0o600
    agent_endpoint = endpoint(certs, AgentHTTPS.port(agent_https))

    assert {:error, _} = HeadlessAgent.request(directory, agent_endpoint, %{"type" => "search"})

    assert %{"ok" => true} =
             owner_request(endpoint(certs, OwnerTLS.port(owner)), certs, "a1", person, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => fingerprint
             })

    assert {:ok, %{"type" => "ok", "items" => []}} =
             HeadlessAgent.request(directory, agent_endpoint, %{"type" => "search"})

    assert {:error, {:application, "forbidden", %{"type" => "error"}}} =
             HeadlessAgent.request(directory, agent_endpoint, %{
               "type" => "respond",
               "sender_id" => "app:test",
               "message_id" => "unknown",
               "acceptance_seq" => 1,
               "op_id" => @op1,
               "deliberate" => true,
               "intent_ref" => "explicit-user-instruction",
               "answer" => nil
             })
  end

  test "a concurrent device close and authenticated agent answer commit one terminal winner",
       context do
    %{certs: certs, relay: relay, owner: owner, agent: agent} = context
    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))

    {:ok, device_key} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, agent_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))

    assert :ok = Relay.setup_sender(relay, device_key, "app:test")

    assert %{"type" => "ok", "seq" => 1} =
             forward(relay, device_key, "app:test", "race", "race content")

    owner_endpoint = endpoint(certs, OwnerTLS.port(owner))

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => agent_key
             })

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_grants_replace",
               "key_fingerprint" => agent_key,
               "expected_revision" => 1,
               "read_grants" => ["app:test"],
               "respond_grants" => ["app:test"]
             })

    agent_endpoint = endpoint(certs, AgentTLS.port(agent))
    cert = Path.join(certs, "unknown.pem")
    key = Path.join(certs, "unknown.key")
    parent = self()

    answer_task =
      Task.async(fn ->
        send(parent, :answer_ready)

        receive do
          :go ->
            agent_request(
              agent_endpoint,
              cert,
              key,
              answer("app:test", "race", 1, @op1, %{"choice" => "yes"})
            )
        end
      end)

    close_task =
      Task.async(fn ->
        send(parent, :close_ready)

        receive do
          :go ->
            frame = %{
              "v" => 1,
              "type" => "lifecycle",
              "op_id" => @op2,
              "sender_id" => "app:test",
              "message_id" => "race",
              "state" => "cleared",
              "at" => 1
            }

            {:ok, line} = Relay.request_line(relay, device_key, JSON.encode!(frame) <> "\n")
            {:ok, reply} = JSON.decode_line(line)
            reply
        end
      end)

    assert_receive :answer_ready
    assert_receive :close_ready
    send(answer_task.pid, :go)
    send(close_task.pid, :go)
    answer_result = Task.await(answer_task, 10_000)
    close_result = Task.await(close_task, 10_000)
    winners = Enum.count([answer_result, close_result], &(&1["seq"] == 2))
    assert winners == 1

    assert Enum.count(
             [answer_result, close_result],
             &(&1["error"] == "closed" or &1["reason"] == "closed")
           ) == 1

    assert %{"item" => %{"terminal_seq" => 2, "state" => state}} =
             agent_request(agent_endpoint, cert, key, %{
               "v" => 1,
               "type" => "get",
               "sender_id" => "app:test",
               "message_id" => "race"
             })

    assert state in ["answered", "cleared"]
    assert :sys.get_state(relay).store.engine.persons[person] == 2
  end

  test "headless owner CLI requires explicit exact-key registration and scoped replacement",
       context do
    %{
      certs: certs,
      relay: relay,
      owner: owner,
      agent: agent,
      agent_https: agent_https,
      path: path
    } = context

    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))
    directory = Path.join(Path.dirname(path), "cli-agent")
    assert {:ok, fingerprint} = HeadlessAgent.create_identity(directory)
    previous = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(previous) end)

    common = [
      "--connect",
      "127.0.0.1",
      "--name",
      "relay.test",
      "--port",
      Integer.to_string(OwnerTLS.port(owner)),
      "--ca",
      Path.join(certs, "ca.pem"),
      "--cert",
      Path.join(certs, "a1.pem"),
      "--key",
      Path.join(certs, "a1.key"),
      "--person",
      person
    ]

    register =
      ["agent-register" | common] ++
        ["--agent-id", @agent_id, "--agent-cert", Path.join(directory, "identity.pem")]

    send(self(), {:mix_shell_input, :prompt, "CANCEL"})
    assert_raise Mix.Error, fn -> Mix.Tasks.Attention.Owner.run(register) end
    assert {:error, :forbidden} = Relay.agent_identity(relay, fingerprint)
    send(self(), {:mix_shell_input, :prompt, "ENROLL"})
    assert :ok = Mix.Tasks.Attention.Owner.run(register)
    assert {:ok, ^person, @agent_id, 1} = Relay.agent_identity(relay, fingerprint)

    replace =
      ["agent-grants-replace" | common] ++
        [
          "--agent-key",
          fingerprint,
          "--revision",
          "1",
          "--read",
          "app:ops/",
          "--respond",
          "app:ops/questions"
        ]

    send(self(), {:mix_shell_input, :prompt, "REPLACE"})
    assert :ok = Mix.Tasks.Attention.Owner.run(replace)

    assert %{grant_revision: 2, read_grants: read, respond_grants: respond} =
             :sys.get_state(relay).store.engine.agents[fingerprint]

    assert MapSet.member?(read, "app:ops/")
    assert MapSet.member?(respond, "app:ops/questions")

    {:ok, device_key} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    assert :ok = Relay.setup_sender(relay, device_key, "app:ops/questions")
    hostile_id = "cli" <> <<27>> <> "[2K\nforged prompt"

    assert %{"type" => "ok", "seq" => 1} =
             forward(relay, device_key, "app:ops/questions", hostile_id, "private")

    answer_file = Path.join(Path.dirname(path), "answer.json")
    File.write!(answer_file, "null\n")

    answer_args = [
      "respond",
      "--directory",
      directory,
      "--connect",
      "127.0.0.1",
      "--name",
      "relay.test",
      "--port",
      Integer.to_string(AgentHTTPS.port(agent_https)),
      "--ca",
      Path.join(certs, "ca.pem"),
      "--sender",
      "app:ops/questions",
      "--message",
      hostile_id,
      "--acceptance",
      "1",
      "--op",
      @op1,
      "--intent-ref",
      "owner-approved-human-instruction",
      "--answer-file",
      answer_file
    ]

    send(self(), {:mix_shell_input, :prompt, "CANCEL"})

    assert_raise Mix.Error, "answer cancelled", fn ->
      Mix.Tasks.Attention.Agent.run(answer_args)
    end

    assert_receive {:mix_shell, :info, ["Answer target " <> display]}
    assert String.contains?(display, "cli\\x1B[2K\\x0Aforged prompt")
    assert :binary.match(display, <<27>>) == :nomatch
    assert :binary.match(display, "\n") == :nomatch

    assert :sys.get_state(relay).store.engine.messages[{person, "app:ops/questions", hostile_id}].terminal ==
             nil

    send(self(), {:mix_shell_input, :prompt, "ANSWER"})
    assert :ok = Mix.Tasks.Attention.Agent.run(answer_args)

    assert %{"item" => %{"state" => "answered", "answer" => nil}} =
             agent_request(
               endpoint(certs, AgentTLS.port(agent)),
               Path.join(directory, "identity.pem"),
               Path.join(directory, "identity.key"),
               %{
                 "v" => 1,
                 "type" => "get",
                 "sender_id" => "app:ops/questions",
                 "message_id" => hostile_id
               }
             )
  end

  test "literal content search visits JSON string values but not nested decimal or integer tokens",
       context do
    %{certs: certs, relay: relay, owner: owner, agent: agent} = context
    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))

    {:ok, device_key} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, agent_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))

    assert :ok = Relay.setup_sender(relay, device_key, "app:test")
    decimal = %JSON.DecimalToken{raw: "0.123456789012345678901234567890"}
    large = 9_007_199_254_740_993

    for {id, nested} <- [
          {"numbers", [%{"amount" => decimal, "integer" => large}]},
          {"string", [%{"description" => decimal.raw}]}
        ] do
      assert %{"type" => "ok"} =
               device_request(relay, device_key, %{
                 "v" => 1,
                 "type" => "forward",
                 "op_id" => unique_op(),
                 "message" => %{
                   "sender_id" => "app:test",
                   "message_id" => id,
                   "created_at" => 1,
                   "title" => "item",
                   "unknown" => %{"nested" => nested}
                 }
               })
    end

    owner_endpoint = endpoint(certs, OwnerTLS.port(owner))

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => agent_key
             })

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_grants_replace",
               "key_fingerprint" => agent_key,
               "expected_revision" => 1,
               "read_grants" => ["app:test"],
               "respond_grants" => []
             })

    agent_endpoint = endpoint(certs, AgentTLS.port(agent))
    cert = Path.join(certs, "unknown.pem")
    private = Path.join(certs, "unknown.key")

    assert %{"items" => [%{"message_id" => "string"}]} =
             agent_request(agent_endpoint, cert, private, %{
               "v" => 1,
               "type" => "search",
               "contains" => decimal.raw
             })

    assert %{"items" => []} =
             agent_request(agent_endpoint, cert, private, %{
               "v" => 1,
               "type" => "search",
               "contains" => Integer.to_string(large)
             })
  end

  test "the same agent UUID belongs independently to two people across restart", context do
    %{certs: certs, relay: relay, owner: owner, agent: agent, path: path} = context
    {:ok, person_a} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))
    {:ok, person_b} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "b1"))

    {:ok, device_a} =
      Relay.setup_device_certificate(relay, person_a, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, device_b} =
      Relay.setup_device_certificate(relay, person_b, TLSFixtures.certificate_der!(certs, "b2"))

    {:ok, key_a} = DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))

    {:ok, key_b} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "other-agent"))

    assert :ok = Relay.setup_sender(relay, device_a, "app:test")
    assert :ok = Relay.setup_sender(relay, device_b, "app:test")
    assert %{"type" => "ok"} = forward(relay, device_a, "app:test", "shared", "A only")
    assert %{"type" => "ok"} = forward(relay, device_b, "app:test", "shared", "B only")

    owner_endpoint = endpoint(certs, OwnerTLS.port(owner))

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person_a, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => key_a
             })

    # A registered key remains globally exclusive even when B uses the same UUID.
    assert %{"error" => "not_found"} =
             owner_request(owner_endpoint, certs, "b1", person_b, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => key_a
             })

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "b1", person_b, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => key_b
             })

    {:ok, unused_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "expired"))

    assert %{"error" => "not_found"} =
             owner_request(owner_endpoint, certs, "a1", person_a, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => unused_key
             })

    for {owner_name, person, key} <- [
          {"a1", person_a, key_a},
          {"b1", person_b, key_b}
        ] do
      assert %{"ok" => true, "agents" => [%{"agent_id" => @agent_id, "key_fingerprint" => ^key}]} =
               owner_request(owner_endpoint, certs, owner_name, person, %{"type" => "agent_list"})
    end

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person_a, %{
               "type" => "agent_grants_replace",
               "key_fingerprint" => key_a,
               "expected_revision" => 1,
               "read_grants" => ["app:test"],
               "respond_grants" => []
             })

    agent_endpoint = endpoint(certs, AgentTLS.port(agent))
    cert_a = Path.join(certs, "unknown.pem")
    private_a = Path.join(certs, "unknown.key")
    cert_b = Path.join(certs, "other-agent.pem")
    private_b = Path.join(certs, "other-agent.key")

    assert %{"items" => [%{"message" => %{"unknown" => %{"text" => "A only"}}}]} =
             agent_request(agent_endpoint, cert_a, private_a, %{"v" => 1, "type" => "search"})

    assert %{"items" => [], "initial_high_water" => 0} =
             agent_request(agent_endpoint, cert_b, private_b, %{"v" => 1, "type" => "search"})

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "b1", person_b, %{
               "type" => "agent_grants_replace",
               "key_fingerprint" => key_b,
               "expected_revision" => 1,
               "read_grants" => ["app:test"],
               "respond_grants" => []
             })

    assert %{"items" => [%{"message" => %{"unknown" => %{"text" => "B only"}}}]} =
             agent_request(agent_endpoint, cert_b, private_b, %{"v" => 1, "type" => "search"})

    assert %{"item" => %{"message" => %{"unknown" => %{"text" => "A only"}}}} =
             agent_request(agent_endpoint, cert_a, private_a, %{
               "v" => 1,
               "type" => "get",
               "sender_id" => "app:test",
               "message_id" => "shared"
             })

    GenServer.stop(agent)
    GenServer.stop(owner)
    GenServer.stop(relay)
    {:ok, restarted} = Relay.start_link(store_path: path)
    {:ok, owner_restarted} = listener(OwnerTLS, restarted, certs)
    {:ok, agent_restarted} = listener(AgentTLS, restarted, certs)

    on_exit(fn ->
      for pid <- [agent_restarted, owner_restarted, restarted],
          Process.alive?(pid),
          do: GenServer.stop(pid)
    end)

    assert {:ok, ^person_a, @agent_id, 1} = Relay.agent_identity(restarted, key_a)
    assert {:ok, ^person_b, @agent_id, 1} = Relay.agent_identity(restarted, key_b)

    assert %{"items" => [%{"message" => %{"unknown" => %{"text" => "A only"}}}]} =
             agent_request(endpoint(certs, AgentTLS.port(agent_restarted)), cert_a, private_a, %{
               "v" => 1,
               "type" => "search"
             })

    assert %{"items" => [%{"message" => %{"unknown" => %{"text" => "B only"}}}]} =
             agent_request(endpoint(certs, AgentTLS.port(agent_restarted)), cert_b, private_b, %{
               "v" => 1,
               "type" => "search"
             })

    assert %{"agents" => [%{"key_fingerprint" => ^key_b}]} =
             owner_request(
               endpoint(certs, OwnerTLS.port(owner_restarted)),
               certs,
               "b1",
               person_b,
               %{
                 "type" => "agent_list"
               }
             )
  end

  test "encrypted page cursor survives restart while lifecycle and exact JSON value advance",
       context do
    %{certs: certs, relay: relay, owner: owner, agent: agent, path: path} = context
    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))

    {:ok, device_key} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, agent_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))

    assert :ok = Relay.setup_sender(relay, device_key, "app:test")

    large = 9_007_199_254_740_993
    decimal = %JSON.DecimalToken{raw: "0.123456789012345678901234567890"}

    original = %{
      "sender_id" => "app:test",
      "message_id" => "first",
      "created_at" => 1,
      "title" => "precise",
      "unknown" => %{"items" => [%{"amount" => decimal, "integer" => large}]}
    }

    assert %{"type" => "ok", "seq" => 1} =
             device_request(relay, device_key, %{
               "v" => 1,
               "type" => "forward",
               "op_id" => unique_op(),
               "message" => original
             })

    assert %{"type" => "ok", "seq" => 2} =
             forward(relay, device_key, "app:test", "second", "later")

    owner_endpoint = endpoint(certs, OwnerTLS.port(owner))

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => agent_key
             })

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_grants_replace",
               "key_fingerprint" => agent_key,
               "expected_revision" => 1,
               "read_grants" => ["app:test"],
               "respond_grants" => []
             })

    endpoint_before = endpoint(certs, AgentTLS.port(agent))
    cert = Path.join(certs, "unknown.pem")
    key = Path.join(certs, "unknown.key")

    assert %{
             "initial_high_water" => 2,
             "items" => [%{"message_id" => "second"}],
             "next_cursor" => cursor
           } =
             agent_request(endpoint_before, cert, key, %{
               "v" => 1,
               "type" => "search",
               "limit" => 1
             })

    assert is_binary(cursor) and not String.contains?(cursor, "app:test")

    assert %{"type" => "ok", "seq" => 3} =
             device_request(relay, device_key, %{
               "v" => 1,
               "type" => "answer",
               "sender_id" => "app:test",
               "message_id" => "first",
               "op_id" => unique_op(),
               "answer" => %{"exact" => decimal, "large" => large}
             })

    GenServer.stop(agent)
    GenServer.stop(owner)
    GenServer.stop(relay)
    {:ok, restarted} = Relay.start_link(store_path: path)
    {:ok, agent_restarted} = listener(AgentTLS, restarted, certs)

    on_exit(fn ->
      if Process.alive?(agent_restarted), do: GenServer.stop(agent_restarted)
      if Process.alive?(restarted), do: GenServer.stop(restarted)
    end)

    endpoint_after = endpoint(certs, AgentTLS.port(agent_restarted))

    assert %{
             "initial_high_water" => 2,
             "items" => [
               %{
                 "message_id" => "first",
                 "state" => "answered",
                 "answer" => answer,
                 "message" => recovered,
                 "observation_high_water" => 3
               }
             ],
             "next_cursor" => nil
           } =
             agent_request(endpoint_after, cert, key, %{
               "v" => 1,
               "type" => "search",
               "limit" => 1,
               "cursor" => cursor
             })

    assert answer == %{"exact" => decimal, "large" => large}
    assert recovered == original
  end

  test "agent pages and detail preserve the immutable message beside current retracted state",
       context do
    %{certs: certs, relay: relay, owner: owner, agent: agent} = context
    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))

    {:ok, device_key} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, agent_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))

    assert :ok = Relay.setup_sender(relay, device_key, "app:test")

    assert %{"type" => "ok", "seq" => 1} =
             device_request(relay, device_key, %{
               "v" => 1,
               "type" => "forward",
               "op_id" => unique_op(),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "retracted",
                 "created_at" => 1,
                 "title" => "title",
                 "unknown" => %{"text" => "original"}
               }
             })

    assert %{"type" => "ok", "seq" => 2} =
             forward(relay, device_key, "app:test", "later", "later")

    owner_endpoint = endpoint(certs, OwnerTLS.port(owner))

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => agent_key
             })

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_grants_replace",
               "key_fingerprint" => agent_key,
               "expected_revision" => 1,
               "read_grants" => ["app:test"],
               "respond_grants" => []
             })

    agent_endpoint = endpoint(certs, AgentTLS.port(agent))
    cert = Path.join(certs, "unknown.pem")
    private = Path.join(certs, "unknown.key")

    assert %{
             "initial_high_water" => 2,
             "items" => [%{"message_id" => "later"}],
             "next_cursor" => cursor
           } =
             agent_request(agent_endpoint, cert, private, %{
               "v" => 1,
               "type" => "search",
               "limit" => 1
             })

    assert is_binary(cursor)

    assert %{"type" => "ok", "state" => "retracted", "retracted_at" => retracted_at} =
             device_request(relay, device_key, %{
               "v" => 1,
               "type" => "conversation_withdraw",
               "op_id" => unique_op(),
               "sender_id" => "app:test",
               "message_id" => "retracted"
             })

    assert %{
             "initial_high_water" => 2,
             "observation_high_water" => 3,
             "items" => [page_item],
             "next_cursor" => nil
           } =
             agent_request(agent_endpoint, cert, private, %{
               "v" => 1,
               "type" => "search",
               "limit" => 1,
               "cursor" => cursor
             })

    assert page_item["state"] == "retracted"
    assert page_item["retracted_at"] == retracted_at

    assert page_item["message"] == %{
             "sender_id" => "app:test",
             "message_id" => "retracted",
             "created_at" => 1,
             "title" => "title",
             "unknown" => %{"text" => "original"}
           }

    assert %{"item" => detail} =
             agent_request(agent_endpoint, cert, private, %{
               "v" => 1,
               "type" => "get",
               "sender_id" => "app:test",
               "message_id" => "retracted"
             })

    assert detail["message"] == page_item["message"]
    assert detail["state"] == "retracted"
    assert detail["retracted_at"] == retracted_at

    GenServer.stop(relay)
  end

  test "stopping only the agent listener fences an accepted socket before a pending answer",
       context do
    %{certs: certs, relay: relay, owner: owner, agent: agent} = context
    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))

    {:ok, device_key} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, agent_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))

    assert :ok = Relay.setup_sender(relay, device_key, "app:test")

    assert %{"type" => "ok", "seq" => 1} =
             forward(relay, device_key, "app:test", "stop", "private")

    owner_endpoint = endpoint(certs, OwnerTLS.port(owner))

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => agent_key
             })

    assert %{"ok" => true} =
             owner_request(owner_endpoint, certs, "a1", person, %{
               "type" => "agent_grants_replace",
               "key_fingerprint" => agent_key,
               "expected_revision" => 1,
               "read_grants" => ["app:test"],
               "respond_grants" => ["app:test"]
             })

    {:ok, socket} =
      :ssl.connect({127, 0, 0, 1}, AgentTLS.port(agent),
        versions: [:"tlsv1.3"],
        verify: :verify_peer,
        cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
        certfile: String.to_charlist(Path.join(certs, "unknown.pem")),
        keyfile: String.to_charlist(Path.join(certs, "unknown.key")),
        server_name_indication: ~c"relay.test",
        session_tickets: :disabled,
        active: false,
        mode: :binary,
        packet: :line
      )

    assert :ok = GenServer.stop(agent)
    _ = :ssl.send(socket, JSON.encode!(answer("app:test", "stop", 1, @op1, nil)) <> "\n")
    assert {:error, _} = :ssl.recv(socket, 0, 3_000)
    :ssl.close(socket)
    assert :sys.get_state(relay).store.engine.persons[person] == 1

    assert :sys.get_state(relay).store.engine.messages[{person, "app:test", "stop"}].terminal ==
             nil
  end

  defp listener(module, relay, certs) do
    module.start_link(
      relay: relay,
      certfile: Path.join(certs, "server.pem"),
      keyfile: Path.join(certs, "server.key"),
      cacertfile: Path.join(certs, "ca.pem")
    )
  end

  defp endpoint(certs, port),
    do: %{
      address: {127, 0, 0, 1},
      name: "relay.test",
      ca_file: Path.join(certs, "ca.pem"),
      port: port
    }

  defp owner_request(endpoint, certs, identity, person, fields) do
    {:ok, reply} =
      HeadlessTLS.owner_request(
        endpoint,
        Path.join(certs, "#{identity}.pem"),
        Path.join(certs, "#{identity}.key"),
        Map.merge(fields, %{"v" => 1, "person_id" => person})
      )

    reply
  end

  defp agent_request(endpoint, cert, key, request) do
    {:ok, reply} = HeadlessTLS.agent_request(endpoint, cert, key, request)
    reply
  end

  defp session_request(relay, session, frame) do
    assert {:ok, lines} =
             Relay.session_request_line(
               relay,
               session,
               JSON.encode!(Map.put_new(frame, "v", 1)) <> "\n"
             )

    Enum.map(lines, fn line ->
      assert {:ok, reply} = JSON.decode_line(line)
      reply
    end)
  end

  defp session_drain(relay, session) do
    assert {:ok, lines} = Relay.session_drain(relay, session)

    Enum.map(lines, fn line ->
      assert {:ok, reply} = JSON.decode_line(line)
      reply
    end)
  end

  defp forward(relay, key, sender, id, text) do
    request = %{
      "v" => 1,
      "type" => "forward",
      "op_id" => unique_op(),
      "message" => %{
        "sender_id" => sender,
        "message_id" => id,
        "created_at" => 1,
        "title" => "title",
        "unknown" => %{"text" => text}
      }
    }

    {:ok, line} = Relay.request_line(relay, key, JSON.encode!(request) <> "\n")
    {:ok, reply} = JSON.decode_line(line)
    reply
  end

  defp device_request(relay, key, frame) do
    {:ok, line} = Relay.request_line(relay, key, JSON.encode!(frame) <> "\n")
    {:ok, reply} = JSON.decode_line(line)
    reply
  end

  defp answer(sender, id, seq, op, value),
    do: %{
      "v" => 1,
      "type" => "answer",
      "sender_id" => sender,
      "message_id" => id,
      "acceptance_seq" => seq,
      "op_id" => op,
      "deliberate" => true,
      "intent_ref" => "human-instruction-1",
      "answer" => value
    }

  defp unique_op do
    suffix =
      System.unique_integer([:positive])
      |> Integer.to_string(16)
      |> String.downcase()
      |> String.pad_leading(12, "0")

    "00000000-0000-4000-8000-" <> suffix
  end
end
