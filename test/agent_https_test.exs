defmodule AttentionServer.AgentHTTPSTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DeviceIdentity, HeadlessTLS, JSON, Relay, Server, TLSFixtures}

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
               "Content-Length: 2097153\r\nConnection: close\r\n\r\n"
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
