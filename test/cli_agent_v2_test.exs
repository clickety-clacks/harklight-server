defmodule AttentionServer.CLIAgentV2Test do
  # The packaged granted-agent CLI (`harklight agent`, `mix attention.agent`)
  # over client API v2 on the device port: GET /v2/search, GET /v2/items/{id}
  # with its entries, and POST /v2/items/{id}/answer with the exact body and
  # Idempotency-Key journaled before it is sent.
  use ExUnit.Case, async: false

  alias AttentionServer.{CLI, DeviceIdentity, HeadlessAgent, JSON, Relay, Server, TLSFixtures}

  @agent_id "00000000-0000-4000-8000-0000000000a1"

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "cli-agent-v2-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    server =
      start_supervised!(
        {Server,
         store_path: Path.join(dir, "relay.sqlite3"),
         certfile: Path.join(certs, "server.pem"),
         keyfile: Path.join(certs, "server.key"),
         cacertfile: Path.join(certs, "ca.pem"),
         port: 0,
         enrollment_enabled: false}
      )

    relay = Server.relay(server)
    owner_der = TLSFixtures.certificate_der!(certs, "a1")
    {:ok, person} = Relay.operator_bootstrap(relay, owner_der)
    {:ok, owner} = DeviceIdentity.from_certificate(owner_der)

    agent_dir = Path.join(dir, "agent")
    assert {:ok, _fingerprint} = HeadlessAgent.create_identity(agent_dir)

    [{:Certificate, agent_der, _}] =
      :public_key.pem_decode(File.read!(Path.join(agent_dir, "identity.pem")))

    {:ok, agent} = DeviceIdentity.from_certificate(agent_der)
    assert :ok = Relay.owner_register_agent(relay, person, owner, @agent_id, agent)

    assert :ok =
             Relay.owner_replace_agent_grants(relay, person, owner, agent, 1, ["app:test"], [
               "app:test"
             ])

    assert :ok = Relay.setup_device(relay, person, "fixture-origin")
    assert :ok = Relay.setup_sender(relay, "fixture-origin", "app:test")

    base = [
      "--directory",
      agent_dir,
      "--connect",
      "127.0.0.1",
      "--name",
      "relay.test",
      "--port",
      Integer.to_string(Server.port(server)),
      "--ca",
      Path.join(certs, "ca.pem")
    ]

    %{
      relay: relay,
      person: person,
      owner: owner,
      agent: agent,
      agent_dir: agent_dir,
      dir: dir,
      base: base
    }
  end

  test "search maps its filters to GET /v2/search and pages with next_page", ctx do
    for n <- 1..3, do: forward!(ctx.relay, "m#{n}", "Report #{n}", n)
    forward!(ctx.relay, "other", "Unrelated title", 4)

    assert %{"items" => first, "next_page" => page} =
             run!(ctx, ["search", "--sender", "app:test", "--q", "Report", "--limit", "2"])

    assert length(first) == 2

    assert %{"items" => rest} =
             run!(ctx, [
               "search",
               "--sender",
               "app:test",
               "--q",
               "Report",
               "--limit",
               "2",
               "--page",
               page
             ])

    titles = Enum.map(first ++ rest, &get_in(&1, ["message", "title"]))
    assert Enum.sort(titles) == ["Report 1", "Report 2", "Report 3"]

    assert %{"items" => [%{"message" => %{"message_id" => "m2"}}]} =
             run!(ctx, ["search", "--sender", "app:test", "--message", "m2", "--state", "open"])
  end

  test "get reads one item by item ID or by its exact sender and message ID", ctx do
    forward!(ctx.relay, "m1", "Report", 1)

    %{"items" => [%{"item_id" => item}]} =
      run!(ctx, ["search", "--message", "m1", "--sender", "app:test"])

    assert %{"item" => %{"item_id" => ^item}} = run!(ctx, ["get", "--item-id", item])

    assert %{"item" => %{"item" => %{"item_id" => ^item}}, "entries" => %{"entries" => entries}} =
             run!(ctx, ["get", "--sender", "app:test", "--message", "m1", "--entries"])

    assert is_list(entries)

    assert_raise Mix.Error, ~r/not_found/, fn ->
      run!(ctx, ["get", "--sender", "app:test", "--message", "none"])
    end
  end

  test "answer journals the exact body and key before sending, then replays it", ctx do
    forward!(ctx.relay, "m1", "Choose", 1)
    answer = answer_file!(ctx, %{})

    args = [
      "answer",
      "--sender",
      "app:test",
      "--message",
      "m1",
      "--request-revision",
      "1",
      "--answer-file",
      answer
    ]

    # Cancelling at the prompt: the journal already holds the exact request,
    # and nothing reached the server.
    assert_raise Mix.Error, ~r/answer cancelled/, fn -> run!(ctx, args, "no") end
    # Closed input cancels too.
    assert_raise Mix.Error, ~r/answer cancelled/, fn -> run!(ctx, args, :eof) end
    [journal] = journals(ctx)

    assert %{"item_id" => item, "body" => %{"request_revision" => 1}, "idempotency_key" => key} =
             journal

    assert %{"item" => %{"terminal" => nil}} = run!(ctx, ["get", "--item-id", item])

    assert %{
             "rev" => _,
             "item" => %{"terminal" => %{"state" => "answered"}},
             "replayed" => false,
             "idempotency_key" => ^key
           } = run!(ctx, args, "ANSWER")

    assert %{"replayed" => true, "idempotency_key" => ^key} = run!(ctx, args, "ANSWER")
    assert [^journal] = journals(ctx)

    changed = answer_file!(ctx, %{"comment" => "different"}, "changed.json")
    changed_args = List.replace_at(args, -1, changed)
    assert_raise Mix.Error, ~r/operation_conflict/, fn -> run!(ctx, changed_args, "ANSWER") end
  end

  test "an answer the grants do not cover is refused and its journal kept", ctx do
    assert :ok =
             Relay.owner_replace_agent_grants(
               ctx.relay,
               ctx.person,
               ctx.owner,
               ctx.agent,
               2,
               ["app:test"],
               []
             )

    forward!(ctx.relay, "m1", "Choose", 1)

    args = [
      "answer",
      "--sender",
      "app:test",
      "--message",
      "m1",
      "--request-revision",
      "1",
      "--answer-file",
      answer_file!(ctx, %{})
    ]

    # The server's refusal is reported with its request ID and not retried.
    error = assert_raise Mix.Error, fn -> run!(ctx, args, "ANSWER") end
    assert error.message =~ "agent request refused: forbidden request_id="
    assert [_journal] = journals(ctx)
  end

  test "the confirmation line cannot carry terminal control bytes from a message ID", ctx do
    hostile = "cli" <> <<27>> <> "[2K\nforged prompt"
    forward!(ctx.relay, hostile, "Choose", 1)

    args = [
      "answer",
      "--sender",
      "app:test",
      "--message",
      hostile,
      "--request-revision",
      "1",
      "--answer-file",
      answer_file!(ctx, %{"comment" => "line\n" <> <<27>> <> "[2K"})
    ]

    assert_raise Mix.Error, ~r/answer cancelled/, fn -> run!(ctx, args, "no") end
    assert_received {:mix_shell, :info, ["Answer item " <> display]}
    assert :binary.match(display, <<27>>) == :nomatch
    assert :binary.match(display, "\n") == :nomatch
  end

  test "0.1-only flags are refused with a clear message", ctx do
    for flag <- ~w(--acceptance --op --intent-ref --text --contains --cursor) do
      assert_raise Mix.Error, ~r/#{flag} was removed/, fn -> run!(ctx, ["search", flag, "x"]) end
    end
  end

  test "keygen and the Mix task keep their entry points", ctx do
    directory = Path.join(ctx.dir, "second-agent")

    CLI.with_io(Mix.Shell.Process, Mix.Error, fn ->
      CLI.Agent.run(["keygen", "--directory", directory])
    end)

    assert_received {:mix_shell, :info, [created]}
    assert created =~ "public-key fingerprint"
    assert File.exists?(Path.join(directory, "identity.key"))

    forward!(ctx.relay, "m1", "Report", 1)

    previous = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(previous) end)
    Mix.Tasks.Attention.Agent.run(["search" | ctx.base] ++ ["--sender", "app:test"])

    assert_received {:mix_shell, :info, [line]}
    assert {:ok, %{"items" => [_]}} = JSON.decode_line(line <> "\n")
  end

  # -- helpers ----------------------------------------------------------------

  defp run!(ctx, [command | args], input \\ nil) do
    if input, do: send(self(), {:mix_shell_input, :prompt, input})

    CLI.with_io(Mix.Shell.Process, Mix.Error, fn ->
      CLI.Agent.run([command | ctx.base] ++ args)
    end)

    last_json()
  end

  defp last_json(last \\ nil) do
    receive do
      {:mix_shell, :info, [line]} ->
        case JSON.decode_line(line <> "\n") do
          {:ok, value} when is_map(value) -> last_json(value)
          _ -> last_json(last)
        end

      {:mix_shell, :prompt, _} ->
        last_json(last)
    after
      0 -> last
    end
  end

  defp journals(ctx) do
    dir = Path.join(ctx.agent_dir, "operations")

    for name <- File.ls!(dir), String.starts_with?(name, "v2-") do
      {:ok, value} =
        JSON.decode_line(String.trim_trailing(File.read!(Path.join(dir, name))) <> "\n")

      value
    end
  end

  defp answer_file!(ctx, value, name \\ "answer.json") do
    path = Path.join(ctx.dir, name)
    File.write!(path, JSON.encode!(value))
    path
  end

  defp forward!(relay, id, title, n) do
    frame = %{
      "v" => 1,
      "type" => "forward",
      "op_id" => "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}",
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => id,
        "created_at" => n,
        "title" => title,
        "answer_ui" => %{
          "schema" => "attention-client.actions/1",
          "actions" => [%{"id" => "yes", "label" => "Yes", "kind" => "value", "value" => true}]
        }
      }
    }

    assert {:ok, line} = Relay.request_line(relay, "fixture-origin", JSON.encode!(frame) <> "\n")
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(line)
  end
end
