defmodule AttentionServer.AgentHTTPSTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DeviceIdentity, HeadlessTLS, JSON, Relay, Server, TLSFixtures}
  alias Exqlite.Sqlite3

  @agent_id "00000000-0000-4000-8000-0000000000a1"
  @op "00000000-0000-4000-8000-0000000000b1"

  setup_all do
    certs = TLSFixtures.create!()
    TLSFixtures.make_self_signed_client!(certs, "b2")
    TLSFixtures.make_self_signed_client!(certs, "other-agent")
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    directory =
      Path.join(System.tmp_dir!(), "attention-agent-https-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)
    path = Path.join(directory, "state.sqlite3")
    opts = server_options(certs, path)
    {:ok, server} = Server.start_link(opts)
    Process.unlink(server)
    on_exit(fn -> if Process.alive?(server), do: Supervisor.stop(server) end)
    %{server: server, relay: Server.relay(server), path: path, opts: opts}
  end

  test "supervised agent HTTPS separates mTLS, HTTP errors and application denials", context do
    %{certs: certs, server: server, relay: relay} = context
    person = bootstrap(relay, certs, "a1")

    {:ok, device_key} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, agent_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))

    endpoint = endpoint(certs, Server.agent_port(server))

    assert {:error, _} = agent_https(endpoint, certs, "unknown", %{"v" => 1, "type" => "search"})
    register(server, certs, "a1", person, agent_key)

    assert {:ok, 200, %{"type" => "ok", "items" => [], "ref" => "query-1"}} =
             agent_https(endpoint, certs, "unknown", %{
               "v" => 1,
               "type" => "search",
               "ref" => "query-1"
             })

    assert {:ok, 200,
            %{
              "type" => "error",
              "reason" => "invalid_field",
              "field" => "person_id",
              "ref" => "x"
            }} =
             agent_https(endpoint, certs, "unknown", %{
               "v" => 1,
               "type" => "search",
               "person_id" => person,
               "ref" => "x"
             })

    assert {:ok, 200, %{"type" => "error", "reason" => "invalid_field", "field" => "ref"}} =
             agent_https(endpoint, certs, "unknown", %{"v" => 1, "type" => "search", "ref" => 7})

    assert {:ok, 200, %{"type" => "error", "reason" => "invalid_field", "field" => "ref"}} =
             agent_https(endpoint, certs, "unknown", %{"v" => 1, "type" => "search", "ref" => nil})

    assert {:ok, 200, %{"type" => "error", "reason" => "invalid_field", "field" => "text"}} =
             agent_https(endpoint, certs, "unknown", %{
               "v" => 1,
               "type" => "search",
               "text" => nil
             })

    assert {:ok, 200, %{"type" => "ok", "items" => []}} =
             agent_https(endpoint, certs, "unknown", %{
               "v" => 1,
               "type" => "search",
               "opaque_extension" => "ignored"
             })

    assert {405, %{"type" => "error", "reason" => "method_not_allowed"}} =
             raw_http(
               endpoint,
               certs,
               "unknown",
               "GET /agent/v1 HTTP/1.1\r\nHost: relay.test\r\n\r\n"
             )

    assert {404, %{"type" => "error", "reason" => "not_found"}} =
             raw_http(endpoint, certs, "unknown", http("POST", "/wrong", "{}"))

    assert {400, %{"type" => "error", "reason" => "invalid_json"}} =
             raw_http(endpoint, certs, "unknown", http("POST", "/agent/v1", "{bad"))

    assert {400, %{"type" => "error", "reason" => "invalid_json"}} =
             raw_http(endpoint, certs, "unknown", http("POST", "/agent/v1", "[]"))

    assert {400, %{"type" => "error", "reason" => "bad_frame"}} =
             raw_http(endpoint, certs, "unknown", [
               "POST /agent/v1 HTTP/1.1\r\nHost: relay.test\r\n",
               "Content-Type: application/json\r\n",
               "Content-Length: invalid\r\nConnection: close\r\n\r\n"
             ])

    assert {:ok, 200, %{"type" => "ok", "items" => []}} =
             agent_https(endpoint, certs, "unknown", %{"v" => 1, "type" => "search"})

    assert {:error, _} = agent_https(endpoint, certs, "a2", %{"v" => 1, "type" => "search"})
    assert {:error, :forbidden} = Relay.agent_identity(relay, device_key)

    assert {:error, _} =
             HeadlessTLS.agent_https_request(
               endpoint,
               Path.join(certs, "unknown.pem"),
               Path.join(certs, "a2.key"),
               %{"v" => 1, "type" => "search"}
             )
  end

  test "large client request reaches Agent HTTPS without a local size refusal", context do
    %{certs: certs, server: server, relay: relay} = context
    person = bootstrap(relay, certs, "a1")

    {:ok, agent_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))

    register(server, certs, "a1", person, agent_key)
    endpoint = endpoint(certs, Server.agent_port(server))

    request = %{
      "v" => 1,
      "type" => "search",
      "text" => String.duplicate("x", 2 * 1024 * 1024)
    }

    assert {:ok, status, %{"type" => type}} =
             agent_https(endpoint, certs, "unknown", request)

    assert status in 200..499
    assert type in ["ok", "error"]
  end

  test "large Agent HTTPS answers stream to a connected device and read back intact", context do
    %{certs: certs, server: server, relay: relay, path: path} = context
    person = bootstrap(relay, certs, "a1")

    {:ok, device_key} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, agent_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))

    assert :ok = Relay.setup_sender(relay, device_key, "app:test")
    register(server, certs, "a1", person, agent_key)
    grant(server, certs, "a1", person, agent_key, ["app:test"], ["app:test"])

    assert %{"type" => "ok", "seq" => 1} =
             forward(relay, device_key, message("large-answer", "Large answer"))

    payload_before = persisted_payload_size(path)
    answer = Base.encode64(:crypto.strong_rand_bytes(2_097_152))
    expected_hash = Base.encode16(:crypto.hash(:sha256, answer), case: :lower)
    answer_op = unique_op()

    assert {:ok, 200, %{"type" => "ok", "relay_outcome" => "answered"}} =
             agent_https(endpoint(certs, Server.agent_port(server)), certs, "unknown", %{
               "v" => 1,
               "type" => "respond",
               "sender_id" => "app:test",
               "message_id" => "large-answer",
               "acceptance_seq" => 1,
               "op_id" => answer_op,
               "deliberate" => true,
               "intent_ref" => "large-answer-regression",
               "answer" => answer
             })

    device = websocket_connect(server, certs, "a1")

    try do
      assert {[
                %{"type" => "delivery", "message_id" => "large-answer"},
                %{"type" => "lifecycle", "answer" => delivered_answer}
              ], %{"seq" => 2}} = websocket_resync(device)

      assert delivered_answer == answer
      assert Base.encode16(:crypto.hash(:sha256, delivered_answer), case: :lower) == expected_hash

      assert {:ok, 200, %{"type" => "ok", "item" => %{"answer" => read_answer}}} =
               agent_https(endpoint(certs, Server.agent_port(server)), certs, "unknown", %{
                 "v" => 1,
                 "type" => "get",
                 "sender_id" => "app:test",
                 "message_id" => "large-answer"
               })

      assert read_answer == answer
      assert Base.encode16(:crypto.hash(:sha256, read_answer), case: :lower) == expected_hash
      assert persisted_payload_size(path) - payload_before < 65_536
    after
      :ssl.close(device)
    end
  end

  test "WebSocket frames above four MiB reach a connected device and later Agent HTTPS read", context do
    %{certs: certs, server: server, relay: relay, path: path} = context
    person = bootstrap(relay, certs, "a1")

    {:ok, sender_key} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, agent_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))

    assert :ok = Relay.setup_sender(relay, sender_key, "app:test")
    register(server, certs, "a1", person, agent_key)
    grant(server, certs, "a1", person, agent_key, ["app:test"], [])

    receiver = websocket_connect(server, certs, "a1")
    sender = websocket_connect(server, certs, "a2")

    try do
      assert {[], %{"seq" => 0}} = websocket_resync(receiver)
      assert {[], %{"seq" => 0}} = websocket_resync(sender)

      body = Base.encode64(:crypto.strong_rand_bytes(3_145_729))
      assert byte_size(body) > 4 * 1024 * 1024
      body_hash = Base.encode16(:crypto.hash(:sha256, body), case: :lower)
      message = Map.put(message("large-frame", "Large WebSocket frame"), "body", body)
      payload_before = persisted_payload_size(path)

      assert :ok =
               websocket_request(sender, %{
                 "type" => "forward",
                 "op_id" => unique_op(),
                 "message" => message
               })

      assert %{"type" => "ok", "seq" => 1} = websocket_reply(sender)
      assert %{"type" => "delivery", "message" => delivered} = websocket_reply(receiver)
      assert delivered == message
      assert Base.encode16(:crypto.hash(:sha256, delivered["body"]), case: :lower) == body_hash

      assert {:ok, 200, %{"type" => "ok", "item" => %{"message" => read_message}}} =
               agent_https(endpoint(certs, Server.agent_port(server)), certs, "unknown", %{
                 "v" => 1,
                 "type" => "get",
                 "sender_id" => "app:test",
                 "message_id" => "large-frame"
               })

      assert read_message == message
      assert Base.encode16(:crypto.hash(:sha256, read_message["body"]), case: :lower) == body_hash
      assert persisted_payload_size(path) - payload_before < 65_536
    after
      :ssl.close(receiver)
      :ssl.close(sender)
    end
  end

  @tag skip: is_nil(System.get_env("HARKLIGHT_SCRATCH_FS_ROOT"))
  test "full scratch storage refuses one large answer and serves a small retry", %{certs: certs} do
    root = System.fetch_env!("HARKLIGHT_SCRATCH_FS_ROOT")
    {filesystem, 0} = System.cmd("findmnt", ["-n", "-o", "FSTYPE", "--target", root])
    assert String.trim(filesystem) == "tmpfs"

    dir = Path.join(root, "agent-https-full-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "state.sqlite3")
    opts = server_options(certs, path)
    {:ok, server} = Server.start_link(opts)
    Process.unlink(server)
    on_exit(fn -> if Process.alive?(server), do: Supervisor.stop(server) end)
    relay = Server.relay(server)
    person = bootstrap(relay, certs, "a1")

    {:ok, device_key} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, agent_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))

    assert :ok = Relay.setup_sender(relay, device_key, "app:test")
    register(server, certs, "a1", person, agent_key)
    grant(server, certs, "a1", person, agent_key, ["app:test"], ["app:test"])
    assert %{"type" => "ok", "seq" => 1} = forward(relay, device_key, message("scratch", "question"))

    operation_id = unique_op()
    large_answer = Base.encode64(:crypto.strong_rand_bytes(2_097_152))

    request = %{
      "v" => 1,
      "type" => "respond",
      "sender_id" => "app:test",
      "message_id" => "scratch",
      "acceptance_seq" => 1,
      "op_id" => operation_id,
      "deliberate" => true,
      "intent_ref" => "scratch-storage-regression",
      "answer" => large_answer
    }

    filler = Path.join(root, "harklight-fill-#{System.unique_integer([:positive])}")
    on_exit(fn -> if File.exists?(filler), do: File.rm(filler) end)
    filled_size = fill_scratch_filesystem(filler, 0)
    leave_scratch_space(filler, filled_size - 2 * 1024 * 1024)

    data_dir = path <> ".uploads"
    assert {:ok, 503, %{"type" => "error", "reason" => "storage_unavailable"}} =
             agent_https(endpoint(certs, Server.agent_port(server)), certs, "unknown", request)

    assert Process.alive?(relay)
    assert :sys.get_state(relay).store.engine.messages[{person, "app:test", "scratch"}].terminal ==
             nil

    assert File.ls!(data_dir) == []
    File.rm!(filler)

    small_answer = "space is available again"

    assert {:ok, 200, %{"type" => "ok", "relay_outcome" => "answered"}} =
             agent_https(
               endpoint(certs, Server.agent_port(server)),
               certs,
               "unknown",
               Map.put(request, "answer", small_answer)
             )

    assert {:ok, 200, %{"type" => "ok", "item" => %{"answer" => ^small_answer}}} =
             agent_https(endpoint(certs, Server.agent_port(server)), certs, "unknown", %{
               "v" => 1,
               "type" => "get",
               "sender_id" => "app:test",
               "message_id" => "scratch"
             })

    assert Process.alive?(relay)
  end

  test "HTTPS storage failures use 503 while application errors keep their status" do
    storage_error = %{"v" => 1, "type" => "error", "reason" => "storage_unavailable"}
    invalid_error = %{"v" => 1, "type" => "error", "reason" => "invalid_field"}

    for wire <- [AttentionServer.AgentWire, AttentionServer.OriginWire] do
      storage_response = IO.iodata_to_binary(wire.http_response(200, storage_error))
      invalid_response = IO.iodata_to_binary(wire.http_response(200, invalid_error))

      assert String.starts_with?(storage_response, "HTTP/1.1 503 Service Unavailable\r\n")
      assert String.starts_with?(invalid_response, "HTTP/1.1 200 OK\r\n")
    end
  end

  test "non-object JSON roots return 400 without restarting Relay or mutating SQLite", context do
    %{certs: certs, server: server, relay: relay} = context
    person = bootstrap(relay, certs, "a1")

    {:ok, agent_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))

    register(server, certs, "a1", person, agent_key)
    endpoint = endpoint(certs, Server.agent_port(server))
    version = :sys.get_state(relay).store.version

    {_, listener, _, _} =
      Enum.find(Supervisor.which_children(server), fn {id, _, _, _} -> id == :agent_https end)

    for body <- ["1.25", "1.25e7", "[]", "1", "\"value\"", "null", "false"] do
      assert {400, %{"type" => "error", "reason" => "invalid_json"}} =
               raw_http(endpoint, certs, "unknown", http("POST", "/agent/v1", body))

      assert Process.alive?(relay)
      assert Server.relay(server) == relay
      assert :sys.get_state(relay).store.version == version

      assert {_, ^listener, _, _} =
               Enum.find(Supervisor.which_children(server), fn {id, _, _, _} ->
                 id == :agent_https
               end)
    end

    assert {:ok, 200, %{"type" => "ok", "items" => []}} =
             agent_https(endpoint, certs, "unknown", %{"v" => 1, "type" => "search"})

    assert :sys.get_state(relay).store.version == version
  end

  test "HTTPS content, exact null, lost-response retry and restart state are truthful", context do
    %{certs: certs, server: server, relay: relay, path: path, opts: opts} = context
    person = bootstrap(relay, certs, "a1")

    {:ok, device_key} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, agent_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))

    assert :ok = Relay.setup_sender(relay, device_key, "app:test")
    register(server, certs, "a1", person, agent_key)
    grant(server, certs, "a1", person, agent_key, ["app:test"], ["app:test"])

    exact = %{
      "sender_id" => "app:test",
      "message_id" => "one",
      "created_at" => 1,
      "title" => "question",
      "unknown" => %{
        "nested" => [
          %{
            "amount" => %JSON.DecimalToken{raw: "1.234567890123456789"},
            "large" => 9_007_199_254_740_993
          }
        ]
      }
    }

    assert %{"type" => "ok", "seq" => 1} = forward(relay, device_key, exact)
    endpoint_before = endpoint(certs, Server.agent_port(server))

    assert {:ok, 200,
            %{
              "type" => "ok",
              "items" => [%{"message" => ^exact, "source_status" => "unreported"}]
            }} =
             agent_https(endpoint_before, certs, "unknown", %{
               "v" => 1,
               "type" => "search",
               "text" => "question"
             })

    assert {:ok, 200, %{"type" => "ok", "items" => []}} =
             agent_https(endpoint_before, certs, "unknown", %{
               "v" => 1,
               "type" => "search",
               "text" => "1.234567890123456789"
             })

    assert {:ok, 200, %{"type" => "ok", "item" => %{"message" => ^exact, "state" => "open"}}} =
             agent_https(endpoint_before, certs, "unknown", %{
               "v" => 1,
               "type" => "get",
               "sender_id" => "app:test",
               "message_id" => "one"
             })

    response = %{
      "v" => 1,
      "type" => "respond",
      "sender_id" => "app:test",
      "message_id" => "one",
      "acceptance_seq" => 1,
      "op_id" => @op,
      "deliberate" => true,
      "intent_ref" => "human-instruction-1",
      "answer" => nil,
      "ref" => "answer-1"
    }

    assert {:ok, 200, %{"type" => "error", "reason" => "invalid_field"}} =
             agent_https(endpoint_before, certs, "unknown", Map.delete(response, "answer"))

    assert :sys.get_state(relay).store.engine.messages[{person, "app:test", "one"}].terminal ==
             nil

    {:ok, socket} = agent_socket(endpoint_before, certs, "unknown")
    assert :ok = :ssl.send(socket, http("POST", "/agent/v1", JSON.encode!(response)))
    assert :ok = await_terminal(relay, person, "one", 100)
    # Deliberately never consume the first HTTP reply.
    :ssl.close(socket)

    assert {:ok, 200,
            %{
              "type" => "ok",
              "terminal_seq" => 2,
              "operation_id" => @op,
              "relay_outcome" => "answered",
              "ref" => "answer-1"
            }} = agent_https(endpoint_before, certs, "unknown", response)

    assert {:ok, 200,
            %{
              "type" => "ok",
              "item" => %{"state" => "answered", "has_answer" => true, "answer" => nil}
            }} =
             agent_https(endpoint_before, certs, "unknown", %{
               "v" => 1,
               "type" => "get",
               "sender_id" => "app:test",
               "message_id" => "one"
             })

    Supervisor.stop(server)
    assert File.exists?(path)
    {:ok, restarted} = Server.start_link(opts)
    Process.unlink(restarted)
    on_exit(fn -> if Process.alive?(restarted), do: Supervisor.stop(restarted) end)
    endpoint_after = endpoint(certs, Server.agent_port(restarted))

    assert {:ok, 200,
            %{
              "type" => "ok",
              "terminal_seq" => 2,
              "operation_id" => @op,
              "relay_outcome" => "answered"
            }} = agent_https(endpoint_after, certs, "unknown", response)

    assert {:ok, 200, %{"type" => "error", "reason" => "closed", "terminal" => _}} =
             agent_https(
               endpoint_after,
               certs,
               "unknown",
               Map.put(response, "op_id", "00000000-0000-4000-8000-0000000000b2")
             )

    assert :sys.get_state(Server.relay(restarted)).store.engine.persons[person] == 2

    {:ok, staged} = agent_socket(endpoint_after, certs, "unknown")
    revoke(restarted, certs, "a1", person, agent_key)

    assert :ok =
             :ssl.send(
               staged,
               http(
                 "POST",
                 "/agent/v1",
                 JSON.encode!(%{
                   "v" => 1,
                   "type" => "get",
                   "sender_id" => "app:test",
                   "message_id" => "one"
                 })
               )
             )

    assert {:error, _} = :ssl.recv(staged, 0, 3_000)
    :ssl.close(staged)

    assert {:error, _} =
             agent_https(endpoint_after, certs, "unknown", %{"v" => 1, "type" => "search"})
  end

  test "HTTPS accepts a MiB body while tenant-colliding IDs remain isolated", context do
    %{certs: certs, server: server, relay: relay} = context
    person_a = bootstrap(relay, certs, "a1")
    person_b = bootstrap(relay, certs, "b1")

    {:ok, device_a} =
      Relay.setup_device_certificate(relay, person_a, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, device_b} =
      Relay.setup_device_certificate(relay, person_b, TLSFixtures.certificate_der!(certs, "b2"))

    {:ok, key_a} = DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))

    {:ok, key_b} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "other-agent"))

    assert :ok = Relay.setup_sender(relay, device_a, "app:test")
    assert :ok = Relay.setup_sender(relay, device_b, "app:test")
    assert %{"type" => "ok"} = forward(relay, device_a, message("shared", "A secret"))
    assert %{"type" => "ok"} = forward(relay, device_b, message("shared", "B secret"))
    register(server, certs, "a1", person_a, key_a)
    register(server, certs, "b1", person_b, key_b)
    grant(server, certs, "a1", person_a, key_a, ["app:test"], ["app:test"])
    grant(server, certs, "b1", person_b, key_b, ["app:test"], [])
    endpoint = endpoint(certs, Server.agent_port(server))

    assert {:ok, 200,
            %{
              "type" => "ok",
              "items" => [%{"message" => %{"unknown" => %{"text" => "A secret"}}}]
            }} =
             agent_https(endpoint, certs, "unknown", %{
               "v" => 1,
               "type" => "search",
               "opaque_extension" => :binary.copy("x", 1_100_000)
             })

    assert {:ok, 200,
            %{
              "type" => "ok",
              "items" => [%{"message" => %{"unknown" => %{"text" => "B secret"}}}]
            }} =
             agent_https(endpoint, certs, "other-agent", %{"v" => 1, "type" => "search"})

    assert {:ok, 200, %{"type" => "error", "reason" => "forbidden"}} =
             agent_https(endpoint, certs, "other-agent", %{
               "v" => 1,
               "type" => "respond",
               "sender_id" => "app:test",
               "message_id" => "shared",
               "acceptance_seq" => 1,
               "op_id" => @op,
               "deliberate" => true,
               "intent_ref" => "no-respond-grant",
               "answer" => nil
             })

    exact_answer = %{
      "large" => 9_007_199_254_740_993,
      "fraction" => %JSON.DecimalToken{raw: "0.123456789012345678901234567890"}
    }

    assert {:ok, 200,
            %{
              "type" => "ok",
              "terminal_seq" => 2,
              "operation_id" => @op,
              "relay_outcome" => "answered"
            }} =
             agent_https(endpoint, certs, "unknown", %{
               "v" => 1,
               "type" => "respond",
               "sender_id" => "app:test",
               "message_id" => "shared",
               "acceptance_seq" => 1,
               "op_id" => @op,
               "deliberate" => true,
               "intent_ref" => "exact-json-instruction",
               "answer" => exact_answer
             })

    assert {:ok, 200, %{"type" => "ok", "item" => %{"answer" => ^exact_answer}}} =
             agent_https(endpoint, certs, "unknown", %{
               "v" => 1,
               "type" => "get",
               "sender_id" => "app:test",
               "message_id" => "shared"
             })

    assert {:ok, 200, %{"type" => "ok", "item" => %{"state" => "open"}}} =
             agent_https(endpoint, certs, "other-agent", %{
               "v" => 1,
               "type" => "get",
               "sender_id" => "app:test",
               "message_id" => "shared"
             })
  end

  test "stopping only the HTTPS listener fences its accepted socket before an answer", context do
    %{certs: certs, server: server, relay: relay} = context
    person = bootstrap(relay, certs, "a1")

    {:ok, device} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, key} = DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "unknown"))
    assert :ok = Relay.setup_sender(relay, device, "app:test")
    assert %{"type" => "ok", "seq" => 1} = forward(relay, device, message("stop", "private"))
    register(server, certs, "a1", person, key)
    grant(server, certs, "a1", person, key, ["app:test"], ["app:test"])
    {:ok, socket} = agent_socket(endpoint(certs, Server.agent_port(server)), certs, "unknown")

    {_, listener, _, _} =
      Enum.find(Supervisor.which_children(server), fn {id, _, _, _} -> id == :agent_https end)

    assert :ok = GenServer.stop(listener)

    request = %{
      "v" => 1,
      "type" => "respond",
      "sender_id" => "app:test",
      "message_id" => "stop",
      "acceptance_seq" => 1,
      "op_id" => @op,
      "deliberate" => true,
      "intent_ref" => "explicit-instruction",
      "answer" => nil
    }

    _ = :ssl.send(socket, http("POST", "/agent/v1", JSON.encode!(request)))
    assert {:error, _} = :ssl.recv(socket, 0, 3_000)
    :ssl.close(socket)

    assert :sys.get_state(relay).store.engine.messages[{person, "app:test", "stop"}].terminal ==
             nil
  end

  defp server_options(certs, path) do
    [
      store_path: path,
      certfile: Path.join(certs, "server.pem"),
      keyfile: Path.join(certs, "server.key"),
      cacertfile: Path.join(certs, "ca.pem"),
      relay_name: :"agent_https_#{System.unique_integer([:positive])}"
    ]
  end

  defp bootstrap(relay, certs, owner) do
    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, owner))
    person
  end

  defp register(server, certs, owner, person, key) do
    assert %{"ok" => true} =
             owner_request(server, certs, owner, person, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => key
             })
  end

  defp grant(server, certs, owner, person, key, read, respond) do
    assert %{"ok" => true} =
             owner_request(server, certs, owner, person, %{
               "type" => "agent_grants_replace",
               "key_fingerprint" => key,
               "expected_revision" => 1,
               "read_grants" => read,
               "respond_grants" => respond
             })
  end

  defp revoke(server, certs, owner, person, key) do
    assert %{"ok" => true} =
             owner_request(server, certs, owner, person, %{
               "type" => "agent_revoke",
               "key_fingerprint" => key
             })
  end

  defp owner_request(server, certs, owner, person, fields) do
    {:ok, reply} =
      HeadlessTLS.owner_request(
        endpoint(certs, Server.owner_port(server)),
        Path.join(certs, "#{owner}.pem"),
        Path.join(certs, "#{owner}.key"),
        Map.merge(fields, %{"v" => 1, "person_id" => person})
      )

    reply
  end

  defp endpoint(certs, port),
    do: %{
      address: {127, 0, 0, 1},
      name: "relay.test",
      ca_file: Path.join(certs, "ca.pem"),
      port: port
    }

  defp agent_https(endpoint, certs, name, request),
    do:
      HeadlessTLS.agent_https_request(
        endpoint,
        Path.join(certs, "#{name}.pem"),
        Path.join(certs, "#{name}.key"),
        request
      )

  defp agent_socket(endpoint, certs, name),
    do:
      HeadlessTLS.connect(
        endpoint,
        {Path.join(certs, "#{name}.pem"), Path.join(certs, "#{name}.key")}
      )

  defp raw_http(endpoint, certs, name, bytes) do
    {:ok, socket} = agent_socket(endpoint, certs, name)

    try do
      assert :ok = :ssl.send(socket, bytes)
      {:ok, response} = read_all(socket, <<>>)
      [header, body] = :binary.split(response, "\r\n\r\n")
      [status_line | _] = String.split(header, "\r\n")
      ["HTTP/1.1", status | _] = String.split(status_line, " ")
      {:ok, value} = JSON.decode_line(body <> "\n")
      {String.to_integer(status), value}
    after
      :ssl.close(socket)
    end
  end

  defp read_all(_socket, bytes) when byte_size(bytes) > 6 * 1024 * 1024,
    do: {:error, :response_too_large}

  defp read_all(socket, bytes) do
    case :ssl.recv(socket, 0, 5_000) do
      {:ok, chunk} -> read_all(socket, bytes <> chunk)
      {:error, :closed} -> {:ok, bytes}
      error -> error
    end
  end

  defp websocket_connect(server, certs, name) do
    {:ok, socket} =
      HeadlessTLS.connect(
        endpoint(certs, Server.port(server)),
        {Path.join(certs, "#{name}.pem"), Path.join(certs, "#{name}.key")}
      )

    request =
      "GET /relay HTTP/1.1\r\n" <>
        "Host: relay.test\r\n" <>
        "Upgrade: websocket\r\n" <>
        "Connection: Upgrade\r\n" <>
        "Sec-WebSocket-Version: 13\r\n" <>
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" <>
        "Sec-WebSocket-Protocol: attention-relay/1\r\n\r\n"

    assert :ok = :ssl.send(socket, request)
    assert websocket_handshake(socket, <<>>) =~ "101 Switching Protocols"
    socket
  end

  defp websocket_handshake(_socket, bytes) when byte_size(bytes) > 16_384,
    do: flunk("WebSocket handshake exceeded the HTTP header limit")

  defp websocket_handshake(socket, bytes) do
    if String.ends_with?(bytes, "\r\n\r\n") do
      bytes
    else
      assert {:ok, chunk} = :ssl.recv(socket, 1, 5_000)
      websocket_handshake(socket, bytes <> chunk)
    end
  end

  defp websocket_request(socket, request) do
    payload = JSON.encode!(Map.put_new(request, "v", 1)) <> "\n"
    :ssl.send(socket, masked_websocket_frame(payload))
  end

  defp masked_websocket_frame(payload) do
    mask = :crypto.strong_rand_bytes(4)
    length = byte_size(payload)

    header =
      cond do
        length < 126 -> <<1::1, 0::3, 1::4, 1::1, length::7>>
        length < 65_536 -> <<1::1, 0::3, 1::4, 1::1, 126::7, length::16>>
        true -> <<1::1, 0::3, 1::4, 1::1, 127::7, length::64>>
      end

    repeated_mask = :binary.copy(mask, div(length + 3, 4))
    masked = :crypto.exor(payload, binary_part(repeated_mask, 0, length))
    [header, mask, masked]
  end

  defp websocket_resync(socket) do
    assert :ok = websocket_request(socket, %{"type" => "resync", "since" => 0})
    assert %{"type" => "ok"} = websocket_reply(socket)
    websocket_resync_events(socket, [])
  end

  defp websocket_resync_events(socket, events) do
    case websocket_reply(socket) do
      %{"type" => "resynced"} = barrier -> {Enum.reverse(events), barrier}
      event -> websocket_resync_events(socket, [event | events])
    end
  end

  defp websocket_reply(socket) do
    socket
    |> websocket_message(<<>>)
    |> JSON.decode_line()
    |> case do
      {:ok, value} -> value
      error -> flunk("invalid WebSocket JSON message: #{inspect(error)}")
    end
  end

  defp websocket_message(socket, accumulated) do
    assert {:ok, <<fin::1, 0::3, opcode::4, 0::1, length::7>>} = :ssl.recv(socket, 2, 10_000)
    assert opcode in [0, 1]

    length =
      case length do
        126 ->
          assert {:ok, <<size::16>>} = :ssl.recv(socket, 2, 10_000)
          size

        127 ->
          assert {:ok, <<size::64>>} = :ssl.recv(socket, 8, 10_000)
          size

        size ->
          size
      end

    assert {:ok, payload} = :ssl.recv(socket, length, 10_000)
    next = accumulated <> payload
    if fin == 1, do: next, else: websocket_message(socket, next)
  end

  defp persisted_payload_size(path) do
    assert {:ok, db} = Sqlite3.open(path)
    assert {:ok, stmt} = Sqlite3.prepare(db, "SELECT length(payload) FROM relay_state WHERE id = 1")
    assert {:row, [size]} = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)
    size
  end

  defp fill_scratch_filesystem(path, written) when written < 96 * 1024 * 1024 do
    case File.write(path, :binary.copy(<<0>>, 256 * 1024), [:append, :binary]) do
      :ok -> fill_scratch_filesystem(path, written + 256 * 1024)
      {:error, :enospc} -> File.stat!(path).size
      error -> flunk("scratch filesystem fill failed: #{inspect(error)}")
    end
  end

  defp fill_scratch_filesystem(_path, _written),
    do: flunk("scratch filesystem did not fill at the expected size")

  defp leave_scratch_space(path, offset) when offset >= 0 do
    assert {:ok, file} = File.open(path, [:read, :write, :binary])
    assert {:ok, ^offset} = :file.position(file, offset)
    assert :ok = :file.truncate(file)
    assert :ok = File.close(file)
  end

  defp http(method, path, body) do
    [
      "#{method} #{path} HTTP/1.1\r\n",
      "Host: relay.test\r\n",
      "Content-Type: application/json\r\n",
      "Content-Length: #{byte_size(body)}\r\n",
      "Connection: close\r\n\r\n",
      body
    ]
  end

  defp message(id, text),
    do: %{
      "sender_id" => "app:test",
      "message_id" => id,
      "created_at" => 1,
      "title" => "question",
      "unknown" => %{"text" => text}
    }

  defp forward(relay, key, message) do
    frame = %{"v" => 1, "type" => "forward", "op_id" => unique_op(), "message" => message}
    {:ok, line} = Relay.request_line(relay, key, JSON.encode!(frame) <> "\n")
    {:ok, reply} = JSON.decode_line(line)
    reply
  end

  defp unique_op do
    suffix =
      System.unique_integer([:positive])
      |> Integer.to_string(16)
      |> String.downcase()
      |> String.pad_leading(12, "0")

    "00000000-0000-4000-8000-" <> suffix
  end

  defp await_terminal(_relay, _person, _id, 0), do: {:error, :timeout}

  defp await_terminal(relay, person, id, remaining) do
    case :sys.get_state(relay).store.engine.messages[{person, "app:test", id}].terminal do
      nil ->
        Process.sleep(10)
        await_terminal(relay, person, id, remaining - 1)

      _ ->
        :ok
    end
  end
end
