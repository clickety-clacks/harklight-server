defmodule AttentionServer.AgentServiceTest do
  # Registered agents read and answer over client API v2 on the device port;
  # owners register, scope and revoke them over owner TLS. The protocol 0.1
  # agent listeners these cases first exercised are retired.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AttentionServer.{
    DeviceIdentity,
    HeadlessAgent,
    HeadlessTLS,
    JSON,
    Relay,
    Server,
    TLSFixtures
  }

  @agent_id "00000000-0000-4000-8000-0000000000a1"
  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  setup_all do
    certs = TLSFixtures.create!()
    TLSFixtures.make_self_signed_client!(certs, "b2")
    TLSFixtures.make_self_signed_client!(certs, "agent")
    TLSFixtures.make_self_signed_client!(certs, "other-agent")
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "attention-agent-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "state.sqlite3")
    server = start_server(path, certs)
    %{certs: certs, path: path, server: server, relay: Server.relay(server)}
  end

  test "an agent answer on /v2 settles the person's waiting item for its devices", ctx do
    {person, device_key} = person_with_device(ctx)
    assert %{"type" => "ok"} = forward(ctx.relay, device_key, "app:test", "waiting", "request")
    agent_key = register_agent(ctx, person, "agent", ["app:test"], ["app:test"])
    assert {:ok, ^person, @agent_id, 1} = Relay.agent_identity(ctx.relay, agent_key)

    assert {:ok, 200, %{"rev" => rev, "counts" => %{"unresolved" => 1}}} =
             get(ctx, "a2", "/v2/inbox")

    id = item_id(ctx, "agent", "app:test", "waiting")

    assert {:ok, 200, %{"item" => %{"terminal" => %{"state" => "answered"}}}} =
             answer(ctx, "agent", id, %{"request_revision" => 1, "comment" => "done"}, key(1))

    assert {:ok, 200, %{"counts" => %{"unresolved" => 0}}} = get(ctx, "a2", "/v2/inbox")
    assert {:ok, 200, %{"changes" => changes}} = get(ctx, "a2", "/v2/changes?after=#{rev}")

    assert [%{"item_id" => ^id, "terminal" => terminal}] =
             for(%{"kind" => "item", "data" => item} <- changes, do: item)

    assert %{"state" => "answered", "answer" => %{"kind" => "reply", "text" => "done"}} =
             terminal
  end

  test "registered agents read only their own person while answers stay scoped and durable",
       ctx do
    {person, device_key} = person_with_device(ctx)
    {:ok, other} = Relay.operator_bootstrap(ctx.relay, der(ctx, "b1"))
    {:ok, b_device} = Relay.setup_device_certificate(ctx.relay, other, der(ctx, "b2"))

    for sender <- ~w(app:test app:private app:sub/x),
        do: assert(:ok = Relay.setup_sender(ctx.relay, device_key, sender))

    assert :ok = Relay.setup_sender(ctx.relay, b_device, "app:test")
    assert %{"seq" => 1} = forward(ctx.relay, device_key, "app:test", "one", "printer jam")
    assert %{"seq" => 2} = forward(ctx.relay, device_key, "app:private", "hidden", "secret")
    assert %{"seq" => 3} = forward(ctx.relay, device_key, "app:sub/x", "three", "other")
    assert %{"seq" => 1} = forward(ctx.relay, b_device, "app:test", "one", "B printer jam")

    {:ok, agent_key} = DeviceIdentity.from_certificate(der(ctx, "agent"))
    {:ok, owner_key} = DeviceIdentity.from_certificate(der(ctx, "a1"))
    assert {:error, _} = quietly(fn -> get(ctx, "agent", "/v2/search") end)

    assert %{"ok" => true} =
             owner_request(ctx, "a1", person, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => agent_key
             })

    assert %{
             "ok" => true,
             "agents" => [%{"read_grants" => [], "respond_grants" => [], "grant_revision" => 1}]
           } = owner_request(ctx, "a1", person, %{"type" => "agent_list"})

    # Empty grants read every sender of the agent's own person, never another's.
    assert {:ok, 200, %{"items" => items}} = get(ctx, "agent", "/v2/search")

    assert Enum.map(items, &{&1["sender"], &1["message"]["message_id"]}) == [
             {"app:sub/x", "three"},
             {"app:private", "hidden"},
             {"app:test", "one"}
           ]

    one = item_id(ctx, "agent", "app:test", "one")

    assert {:ok, 200, %{"item" => %{"message" => %{"unknown" => %{"text" => "printer jam"}}}}} =
             get(ctx, "agent", "/v2/items/" <> one)

    assert %{"error" => "not_found"} =
             owner_request(ctx, "b1", other, %{
               "type" => "agent_grants_replace",
               "key_fingerprint" => agent_key,
               "expected_revision" => 1,
               "read_grants" => ["app:private"],
               "respond_grants" => ["app:private"]
             })

    replace_grants(ctx, person, agent_key, 1, ["app:test", "app:sub/"], [])
    body = %{"request_revision" => 1}

    assert {:ok, 403, %{"error" => "forbidden"}} = answer(ctx, "agent", one, body, key(1))

    replace_grants(ctx, person, agent_key, 2, ["app:test"], ["app:test"])

    assert %{"error" => "not_found"} =
             owner_request(ctx, "a1", person, %{
               "type" => "agent_grants_replace",
               "key_fingerprint" => agent_key,
               "expected_revision" => 2,
               "read_grants" => ["app:private"],
               "respond_grants" => ["app:private"]
             })

    assert %{grant_revision: 3, grant_history: history} =
             :sys.get_state(ctx.relay).store.engine.agents[agent_key]

    assert Enum.map(history, &{&1.revision, &1.action, &1.actor}) == [
             {1, "register", owner_key},
             {2, "replace", owner_key},
             {3, "replace", owner_key}
           ]

    assert {:ok, 200, %{"item" => %{"terminal" => answered}}, false} =
             answer_with_replay(ctx, "agent", one, body, key(2))

    assert %{"state" => "answered", "answer" => %{"kind" => "no_comment"}} = answered
    assert {:ok, 200, _, true} = answer_with_replay(ctx, "agent", one, body, key(2))

    assert {:ok, 409, %{"error" => "already_answered"}} =
             answer(ctx, "agent", one, %{"request_revision" => 1, "comment" => "late"}, key(3))

    server = restart(ctx)
    restarted = %{ctx | server: server, relay: Server.relay(server)}

    assert %{grant_revision: 3, grant_history: ^history} =
             :sys.get_state(restarted.relay).store.engine.agents[agent_key]

    assert {:ok, 200, %{"item" => %{"terminal" => ^answered}}, true} =
             answer_with_replay(restarted, "agent", one, body, key(2))

    assert {:ok, 200, %{"item" => %{"terminal" => ^answered}}} =
             get(restarted, "agent", "/v2/items/" <> one)
  end

  test "agent admission refuses device keys, expired certificates and revoked keys", ctx do
    {person, device_key} = person_with_device(ctx)
    assert %{"type" => "ok"} = forward(ctx.relay, device_key, "app:test", "secret", "private")

    assert %{"error" => "not_found"} =
             owner_request(ctx, "a1", person, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => device_key
             })

    agent_key = register_agent(ctx, person, "agent", ["app:test"], ["app:test"])
    assert {:error, :forbidden} = Relay.acknowledged_cursor(ctx.relay, agent_key)
    {:ok, expired_key} = DeviceIdentity.from_certificate(der(ctx, "expired"))

    assert %{"ok" => true} =
             owner_request(ctx, "a1", person, %{
               "type" => "agent_register",
               "agent_id" => "00000000-0000-4000-8000-0000000000a2",
               "key_fingerprint" => expired_key
             })

    assert {:error, _} = quietly(fn -> get(ctx, "expired", "/v2/search") end)
    id = item_id(ctx, "agent", "app:test", "secret")
    assert {:ok, conn} = connect(ctx, "agent")
    assert {:ok, conn, %{status: 200}} = request(conn, "GET", "/v2/items/" <> id)

    assert %{"ok" => true} =
             owner_request(ctx, "a1", person, %{
               "type" => "agent_revoke",
               "key_fingerprint" => agent_key
             })

    # The already-open connection gets no content and cannot answer: the next
    # request is refused and the server may then close the connection.
    assert {:ok, conn, %{status: 401}} = request(conn, "GET", "/v2/items/" <> id)

    late =
      request(
        conn,
        "POST",
        "/v2/items/" <> id <> "/answer",
        [{"idempotency-key", key(1)}],
        JSON.encode!(%{"request_revision" => 1})
      )

    assert match?({:ok, _, %{status: 401}}, late) or match?({:error, _, _}, late)

    assert {:ok, 200, %{"item" => %{"terminal" => nil}}} = get(ctx, "a2", "/v2/items/" <> id)
    assert {:error, _} = quietly(fn -> get(ctx, "agent", "/v2/search") end)
  end

  test "headless self-signed service key is usable only after exact owner registration",
       ctx do
    {person, device_key} = person_with_device(ctx)
    assert %{"type" => "ok"} = forward(ctx.relay, device_key, "app:test", "question", "text")
    directory = Path.join(Path.dirname(ctx.path), "service-key")
    assert {:ok, fingerprint} = HeadlessAgent.create_identity(directory)
    assert byte_size(fingerprint) == 64
    assert {:error, :existing_identity} = HeadlessAgent.create_identity(directory)
    assert {:ok, stat} = File.stat(Path.join(directory, "identity.key"))
    assert Bitwise.band(stat.mode, 0o777) == 0o600
    identity = {Path.join(directory, "identity.pem"), Path.join(directory, "identity.key")}

    assert {:error, _} =
             quietly(fn -> HeadlessTLS.v2_get(endpoint(ctx), identity, "/v2/search") end)

    assert %{"ok" => true} =
             owner_request(ctx, "a1", person, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => fingerprint
             })

    assert {:ok, 200, %{"items" => [%{"item_id" => id}]}} =
             HeadlessTLS.v2_get(endpoint(ctx), identity, "/v2/search")

    assert {:ok, 403, _headers, %{"error" => "forbidden"}} =
             HeadlessTLS.v2_request(
               endpoint(ctx),
               identity,
               "POST",
               "/v2/items/" <> id <> "/answer",
               [{"content-type", "application/json"}, {"idempotency-key", key(1)}],
               JSON.encode!(%{"request_revision" => 1})
             )
  end

  test "a concurrent device close and agent answer commit one terminal winner", ctx do
    {person, device_key} = person_with_device(ctx)
    assert %{"seq" => 1} = forward(ctx.relay, device_key, "app:test", "race", "race content")
    register_agent(ctx, person, "agent", ["app:test"], ["app:test"])
    id = item_id(ctx, "agent", "app:test", "race")
    parent = self()

    answer_task =
      Task.async(fn ->
        send(parent, :answer_ready)

        receive do
          :go -> answer(ctx, "agent", id, %{"request_revision" => 1}, key(1))
        end
      end)

    close_task =
      Task.async(fn ->
        send(parent, :close_ready)

        receive do
          :go ->
            device_request(ctx.relay, device_key, %{
              "v" => 1,
              "type" => "lifecycle",
              "op_id" => unique_op(),
              "sender_id" => "app:test",
              "message_id" => "race",
              "state" => "cleared",
              "at" => 1
            })
        end
      end)

    assert_receive :answer_ready
    assert_receive :close_ready
    send(answer_task.pid, :go)
    send(close_task.pid, :go)
    answer_result = Task.await(answer_task, 10_000)
    close_result = Task.await(close_task, 10_000)

    answered? = match?({:ok, 200, _}, answer_result)
    closed? = match?(%{"type" => "ok", "seq" => 2}, close_result)
    assert answered? != closed?

    if answered?,
      do: assert(%{"reason" => "closed"} = close_result),
      else: assert({:ok, 409, %{"error" => "already_" <> _}} = answer_result)

    assert {:ok, 200, %{"item" => %{"terminal" => %{"state" => state}}}} =
             get(ctx, "agent", "/v2/items/" <> id)

    assert state in ["answered", "cleared"]
    assert :sys.get_state(ctx.relay).store.engine.persons[person] == 2
  end

  test "headless owner CLI requires explicit exact-key registration and scoped replacement",
       ctx do
    %{certs: certs, relay: relay, path: path} = ctx
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
      Integer.to_string(Server.owner_port(ctx.server)),
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
  end

  test "literal search visits JSON string values but not nested decimal or integer tokens",
       ctx do
    {person, device_key} = person_with_device(ctx)
    decimal = %JSON.DecimalToken{raw: "0.123456789012345678901234567890"}
    large = 9_007_199_254_740_993

    for {id, nested} <- [
          {"numbers", [%{"amount" => decimal, "integer" => large}]},
          {"string", [%{"description" => decimal.raw}]}
        ] do
      assert %{"type" => "ok"} = forward_message(ctx.relay, device_key, message(id, nested))
    end

    register_agent(ctx, person, "agent", ["app:test"], [])

    assert {:ok, 200, %{"items" => [%{"message" => %{"message_id" => "string"}}]}} =
             get(ctx, "agent", "/v2/search?q=" <> decimal.raw)

    assert {:ok, 200, %{"items" => []}} =
             get(ctx, "agent", "/v2/search?q=" <> Integer.to_string(large))
  end

  test "the same agent UUID belongs independently to two people across restart", ctx do
    {person_a, device_a} = person_with_device(ctx)
    {:ok, person_b} = Relay.operator_bootstrap(ctx.relay, der(ctx, "b1"))
    {:ok, device_b} = Relay.setup_device_certificate(ctx.relay, person_b, der(ctx, "b2"))
    {:ok, key_a} = DeviceIdentity.from_certificate(der(ctx, "agent"))
    {:ok, key_b} = DeviceIdentity.from_certificate(der(ctx, "other-agent"))
    assert :ok = Relay.setup_sender(ctx.relay, device_b, "app:test")
    assert %{"type" => "ok"} = forward(ctx.relay, device_a, "app:test", "shared", "A only")
    assert %{"type" => "ok"} = forward(ctx.relay, device_b, "app:test", "shared", "B only")

    assert %{"ok" => true} =
             owner_request(ctx, "a1", person_a, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => key_a
             })

    # A registered key remains globally exclusive even when B uses the same UUID.
    assert %{"error" => "not_found"} =
             owner_request(ctx, "b1", person_b, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => key_a
             })

    assert %{"ok" => true} =
             owner_request(ctx, "b1", person_b, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => key_b
             })

    {:ok, unused_key} = DeviceIdentity.from_certificate(der(ctx, "expired"))

    assert %{"error" => "not_found"} =
             owner_request(ctx, "a1", person_a, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => unused_key
             })

    for {owner_name, person, key} <- [{"a1", person_a, key_a}, {"b1", person_b, key_b}] do
      assert %{"ok" => true, "agents" => [%{"agent_id" => @agent_id, "key_fingerprint" => ^key}]} =
               owner_request(ctx, owner_name, person, %{"type" => "agent_list"})
    end

    replace_grants(ctx, person_a, key_a, 1, ["app:test"], [])
    assert only_text(ctx, "agent") == "A only"
    assert only_text(ctx, "other-agent") == "B only"

    server = restart(ctx)
    restarted = %{ctx | server: server, relay: Server.relay(server)}
    assert {:ok, ^person_a, @agent_id, 1} = Relay.agent_identity(restarted.relay, key_a)
    assert {:ok, ^person_b, @agent_id, 1} = Relay.agent_identity(restarted.relay, key_b)
    assert only_text(restarted, "agent") == "A only"
    assert only_text(restarted, "other-agent") == "B only"

    assert %{"agents" => [%{"key_fingerprint" => ^key_b}]} =
             owner_request(restarted, "b1", person_b, %{"type" => "agent_list"})
  end

  test "a search page continues across restart while its item's state advances and the message stays exact",
       ctx do
    {person, device_key} = person_with_device(ctx)
    decimal = %JSON.DecimalToken{raw: "0.123456789012345678901234567890"}
    original = message("first", [%{"amount" => decimal, "integer" => 9_007_199_254_740_993}])
    assert %{"seq" => 1} = forward_message(ctx.relay, device_key, original)
    assert %{"seq" => 2} = forward(ctx.relay, device_key, "app:test", "second", "later")
    register_agent(ctx, person, "agent", ["app:test"], [])

    assert {:ok, 200, %{"items" => [%{"message" => %{"message_id" => "second"}}]} = first} =
             get(ctx, "agent", "/v2/search?limit=1")

    page = first["next_page"]
    assert is_binary(page) and not String.contains?(page, "app:test")
    id = item_id(ctx, "agent", "app:test", "first")

    assert {:ok, 200, _} =
             answer(ctx, "a2", id, %{"request_revision" => 1, "comment" => "settled"}, key(1))

    server = restart(ctx)
    restarted = %{ctx | server: server, relay: Server.relay(server)}

    assert {:ok, 200, %{"items" => [item]} = next} =
             get(restarted, "agent", "/v2/search?limit=1&page=" <> page)

    refute Map.has_key?(next, "next_page")
    assert item["item_id"] == id
    assert %{"state" => "answered", "answer" => %{"text" => "settled"}} = item["terminal"]
    assert item["message"] == original
  end

  test "agent pages and detail preserve the immutable message beside current retracted state",
       ctx do
    {person, device_key} = person_with_device(ctx)

    original = %{
      "sender_id" => "app:test",
      "message_id" => "retracted",
      "created_at" => 1,
      "title" => "title",
      "unknown" => %{"text" => "original"}
    }

    assert %{"type" => "ok", "seq" => 1} =
             device_request(ctx.relay, device_key, %{
               "v" => 1,
               "type" => "forward",
               "op_id" => unique_op(),
               "session_handle" => @handle,
               "message" => original
             })

    assert %{"seq" => 2} = forward(ctx.relay, device_key, "app:test", "later", "later")
    register_agent(ctx, person, "agent", ["app:test"], [])

    assert {:ok, 200, %{"items" => [%{"message" => %{"message_id" => "later"}}]} = first} =
             get(ctx, "agent", "/v2/search?limit=1")

    assert %{"type" => "ok", "state" => "retracted"} =
             device_request(ctx.relay, device_key, %{
               "v" => 1,
               "type" => "conversation_withdraw",
               "op_id" => unique_op(),
               "sender_id" => "app:test",
               "message_id" => "retracted"
             })

    assert {:ok, 200, %{"items" => [page_item]}} =
             get(ctx, "agent", "/v2/search?limit=1&page=" <> first["next_page"])

    assert %{"state" => "retracted"} = page_item["terminal"]
    assert page_item["message"] == original

    assert {:ok, 200, %{"item" => detail}} =
             get(ctx, "agent", "/v2/items/" <> page_item["item_id"])

    assert detail["message"] == original
    assert detail["terminal"] == page_item["terminal"]
  end

  test "non-object answer bodies are refused without restarting Relay or changing the item",
       ctx do
    {person, device_key} = person_with_device(ctx)
    assert %{"type" => "ok"} = forward(ctx.relay, device_key, "app:test", "kept", "open")
    register_agent(ctx, person, "agent", ["app:test"], ["app:test"])
    id = item_id(ctx, "agent", "app:test", "kept")
    relay = ctx.relay
    version = :sys.get_state(relay).store.version

    for {body, n} <- Enum.with_index(~w(1.25 1.25e7 [] 1 "value" null false), 1) do
      assert {:ok, 400, %{"error" => "invalid_field"}} =
               raw_answer(ctx, "agent", id, body, key(n))
    end

    assert Server.relay(ctx.server) == relay
    assert Process.alive?(relay)
    assert :sys.get_state(relay).store.version == version
    assert {:ok, 200, %{"item" => %{"terminal" => nil}}} = get(ctx, "agent", "/v2/items/" <> id)
  end

  test "an answer whose reply is never read commits once and replays on retry", ctx do
    {person, device_key} = person_with_device(ctx)
    assert %{"type" => "ok"} = forward(ctx.relay, device_key, "app:test", "lost", "question")
    register_agent(ctx, person, "agent", ["app:test"], ["app:test"])
    id = item_id(ctx, "agent", "app:test", "lost")
    fields = %{"request_revision" => 1, "comment" => "sent once"}
    body = JSON.encode!(fields)

    {:ok, socket} =
      HeadlessTLS.connect(endpoint(ctx), {cert(ctx, "agent"), private(ctx, "agent")})

    :ok =
      :ssl.send(socket, [
        "POST /v2/items/#{id}/answer HTTP/1.1\r\nHost: relay.test\r\n",
        "Content-Type: application/json\r\nIdempotency-Key: #{key(1)}\r\n",
        "Content-Length: #{byte_size(body)}\r\n\r\n",
        body
      ])

    :ssl.close(socket)
    terminal = await_terminal(ctx, id, 50)
    assert %{"state" => "answered", "answer" => %{"text" => "sent once"}} = terminal

    assert {:ok, 200, %{"item" => %{"terminal" => ^terminal}}, true} =
             answer_with_replay(ctx, "agent", id, fields, key(1))

    assert :sys.get_state(ctx.relay).store.engine.persons[person] == 2
  end

  # -- Fixtures -----------------------------------------------------------------

  defp start_server(path, certs) do
    {:ok, server} =
      Server.start_link(
        store_path: path,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem"),
        port: 0,
        owner_port: 0,
        enrollment_enabled: false,
        relay_name: :"agent_service_#{System.unique_integer([:positive])}"
      )

    Process.unlink(server)
    on_exit(fn -> if Process.alive?(server), do: Supervisor.stop(server) end)
    server
  end

  defp restart(ctx) do
    Supervisor.stop(ctx.server)
    start_server(ctx.path, ctx.certs)
  end

  defp person_with_device(ctx) do
    {:ok, person} = Relay.operator_bootstrap(ctx.relay, der(ctx, "a1"))
    {:ok, device_key} = Relay.setup_device_certificate(ctx.relay, person, der(ctx, "a2"))
    assert :ok = Relay.setup_sender(ctx.relay, device_key, "app:test")
    {person, device_key}
  end

  defp register_agent(ctx, person, name, read, respond) do
    {:ok, agent_key} = DeviceIdentity.from_certificate(der(ctx, name))

    assert %{"ok" => true} =
             owner_request(ctx, "a1", person, %{
               "type" => "agent_register",
               "agent_id" => @agent_id,
               "key_fingerprint" => agent_key
             })

    if read != [] or respond != [], do: replace_grants(ctx, person, agent_key, 1, read, respond)
    agent_key
  end

  defp replace_grants(ctx, person, agent_key, revision, read, respond) do
    assert %{"ok" => true} =
             owner_request(ctx, "a1", person, %{
               "type" => "agent_grants_replace",
               "key_fingerprint" => agent_key,
               "expected_revision" => revision,
               "read_grants" => read,
               "respond_grants" => respond
             })
  end

  defp owner_request(ctx, identity, person, fields) do
    {:ok, reply} =
      HeadlessTLS.owner_request(
        endpoint(ctx, Server.owner_port(ctx.server)),
        cert(ctx, identity),
        private(ctx, identity),
        Map.merge(fields, %{"v" => 1, "person_id" => person})
      )

    reply
  end

  defp forward(relay, key, sender, id, text) do
    forward_message(relay, key, %{
      "sender_id" => sender,
      "message_id" => id,
      "created_at" => 1,
      "title" => "title",
      "unknown" => %{"text" => text}
    })
  end

  defp forward_message(relay, key, message),
    do:
      device_request(relay, key, %{
        "v" => 1,
        "type" => "forward",
        "op_id" => unique_op(),
        "message" => message
      })

  defp message(id, nested),
    do: %{
      "sender_id" => "app:test",
      "message_id" => id,
      "created_at" => 1,
      "title" => "item",
      "unknown" => %{"nested" => nested}
    }

  defp device_request(relay, key, frame) do
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

  defp key(n), do: "00000000-0000-4000-8000-0000000001" <> String.pad_leading("#{n}", 2, "0")

  # -- Client API v2 over the device port --------------------------------------

  defp get(ctx, name, path), do: HeadlessTLS.v2_get(endpoint(ctx), identity(ctx, name), path)

  defp answer(ctx, name, id, body, idempotency_key) do
    with {:ok, status, value, _replayed} <-
           answer_with_replay(ctx, name, id, body, idempotency_key),
         do: {:ok, status, value}
  end

  defp answer_with_replay(ctx, name, id, body, idempotency_key) do
    with {:ok, status, headers, value} <-
           raw_answer_request(ctx, name, id, JSON.encode!(body), idempotency_key),
         do: {:ok, status, value, {"idempotent-replayed", "true"} in headers}
  end

  defp raw_answer(ctx, name, id, body, idempotency_key) do
    with {:ok, status, _headers, value} <-
           raw_answer_request(ctx, name, id, body, idempotency_key),
         do: {:ok, status, value}
  end

  defp raw_answer_request(ctx, name, id, body, idempotency_key) do
    HeadlessTLS.v2_request(
      endpoint(ctx),
      identity(ctx, name),
      "POST",
      "/v2/items/" <> id <> "/answer",
      [{"content-type", "application/json"}, {"idempotency-key", idempotency_key}],
      body
    )
  end

  defp item_id(ctx, name, sender, message_id) do
    assert {:ok, 200, %{"items" => [%{"item_id" => id}]}} =
             get(ctx, name, "/v2/search?sender=#{sender}&message_id=#{message_id}")

    id
  end

  defp only_text(ctx, name) do
    assert {:ok, 200, %{"items" => [%{"message" => %{"unknown" => %{"text" => text}}}]}} =
             get(ctx, name, "/v2/search")

    text
  end

  defp await_terminal(_ctx, _id, 0), do: flunk("the unread answer was not committed")

  defp await_terminal(ctx, id, attempts) do
    case get(ctx, "a2", "/v2/items/" <> id) do
      {:ok, 200, %{"item" => %{"terminal" => %{} = terminal}}} ->
        terminal

      _ ->
        Process.sleep(20)
        await_terminal(ctx, id, attempts - 1)
    end
  end

  defp connect(ctx, name) do
    {certificate, key} = identity(ctx, name)

    Mint.HTTP.connect(:https, "127.0.0.1", Server.port(ctx.server),
      hostname: "relay.test",
      protocols: [:http1],
      mode: :passive,
      transport_opts: [
        versions: [:"tlsv1.3"],
        cacertfile: Path.join(ctx.certs, "ca.pem"),
        certfile: certificate,
        keyfile: key,
        server_name_indication: ~c"relay.test",
        timeout: 5_000
      ]
    )
  end

  defp request(conn, method, path, headers \\ [], body \\ nil) do
    case Mint.HTTP.request(conn, method, path, headers, body) do
      {:ok, conn, ref} -> collect(conn, ref, %{})
      {:error, conn, reason} -> {:error, conn, reason}
    end
  end

  defp collect(conn, ref, acc) do
    case Mint.HTTP.recv(conn, 0, 5_000) do
      {:ok, conn, responses} ->
        acc =
          Enum.reduce(responses, acc, fn
            {:status, ^ref, status}, acc -> Map.put(acc, :status, status)
            {:done, ^ref}, acc -> Map.put(acc, :done, true)
            _other, acc -> acc
          end)

        if acc[:done], do: {:ok, conn, acc}, else: collect(conn, ref, acc)

      {:error, conn, reason, _responses} ->
        {:error, conn, reason}
    end
  end

  # A refused TLS 1.3 client certificate can surface as a logged alert.
  defp quietly(fun) do
    capture_log(fn -> send(self(), {:quiet, fun.()}) end)
    assert_received {:quiet, result}
    result
  end

  defp endpoint(ctx, port \\ nil),
    do: %{
      address: {127, 0, 0, 1},
      name: "relay.test",
      ca_file: Path.join(ctx.certs, "ca.pem"),
      port: port || Server.port(ctx.server)
    }

  defp identity(ctx, name), do: {cert(ctx, name), private(ctx, name)}
  defp cert(ctx, name), do: Path.join(ctx.certs, "#{name}.pem")
  defp private(ctx, name), do: Path.join(ctx.certs, "#{name}.key")
  defp der(ctx, name), do: TLSFixtures.certificate_der!(ctx.certs, name)
end
