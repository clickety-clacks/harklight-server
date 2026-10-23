defmodule AttentionServer.ConversationAgentCLIFixtureTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    DeviceIdentity,
    EnrolledLeafVerify,
    HeadlessTLS,
    JSON,
    OriginWire,
    Relay,
    Server,
    Store,
    TLSFixtures
  }

  alias Exqlite.Sqlite3

  @moduletag timeout: 120_000
  @agent_bin System.get_env("HARKLIGHT_AGENT_BIN")
  @moduletag skip:
               if(@agent_bin in [nil, ""],
                 do: "set HARKLIGHT_AGENT_BIN to run the Rust CLI fixture",
                 else: false
               )

  @sender_id "app:rust-cli-fixture"

  test "the Rust listener reports NoComment as a distinct final terminal for choice and reply-only requests" do
    root =
      Path.join(
        System.tmp_dir!(),
        "harklight-agent-cli-no-comment-#{System.unique_integer([:positive])}"
      )

    fixture = start_file_fixture!(root, :none)
    agent_bin = System.fetch_env!("HARKLIGHT_AGENT_BIN")

    choice_request =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "claude-code",
          "--title",
          "No comment choice request",
          "--choice",
          "Approve",
          "--choice",
          "Decline"
        ],
        fixture.env
      )

    choice_response =
      submit_no_comment!(fixture.relay, fixture.reader, choice_request["request"], 1)

    assert choice_response["terminal_kind"] == "no_comment"

    choice_terminal = cli_next!(agent_bin, choice_request["session"], fixture.env)
    assert choice_terminal["final_terminal"] == "NoComment"
    assert choice_terminal["entry"]["kind"] == "terminal"
    assert choice_terminal["entry"]["terminal_state"] == "answered"
    assert choice_terminal["entry"]["terminal_kind"] == "no_comment"
    assert choice_terminal["entry"]["has_answer"] == false
    refute Map.has_key?(choice_terminal["entry"], "answer")

    reply_request =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "claude-code",
          "--title",
          "No comment reply-only request",
          "--body",
          "Please write a response"
        ],
        fixture.env
      )

    reply_response =
      submit_no_comment!(fixture.relay, fixture.reader, reply_request["request"], 1)

    assert reply_response["terminal_kind"] == "no_comment"

    reply_terminal = cli_next!(agent_bin, reply_request["session"], fixture.env)
    assert reply_terminal["final_terminal"] == "NoComment"
    assert reply_terminal["entry"]["kind"] == "terminal"
    assert reply_terminal["entry"]["terminal_state"] == "answered"
    assert reply_terminal["entry"]["terminal_kind"] == "no_comment"
    assert reply_terminal["entry"]["has_answer"] == false
    refute Map.has_key?(reply_terminal["entry"], "answer")

    {:ok, %{"entries" => entries}} = Relay.origin_feed(fixture.relay, fixture.origin, 0)

    no_comment_terminals =
      Enum.filter(entries, &(&1["kind"] == "terminal" and &1["terminal_kind"] == "no_comment"))

    assert Enum.count(no_comment_terminals) == 2
    assert Enum.all?(no_comment_terminals, &(not Map.has_key?(&1, "answer")))
  end

  test "retract sends the journal-bound v1 withdrawal and reuses its operation on retry" do
    root =
      Path.join(
        System.tmp_dir!(),
        "harklight-agent-cli-retract-#{System.unique_integer([:positive])}"
      )

    fixture = start_file_fixture!(root, :none)
    agent_bin = System.fetch_env!("HARKLIGHT_AGENT_BIN")

    request =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "claude-code",
          "--title",
          "Retraction fixture"
        ],
        fixture.env
      )

    start_origin_command_trace!()
    on_exit(&stop_origin_command_trace/0)

    {unauthorized_output, unauthorized_status} =
      System.cmd(
        agent_bin,
        [
          "retract",
          "--session",
          request["session"],
          "--request",
          request["request"],
          "--sender-id",
          @sender_id
        ],
        env: fixture.env,
        stderr_to_stdout: true
      )

    assert unauthorized_status == 2
    assert String.contains?(unauthorized_output, "--sender-id")

    retracted =
      cli_json!(
        agent_bin,
        [
          "retract",
          "--session",
          request["session"],
          "--request",
          request["request"]
        ],
        fixture.env
      )

    op_id = retracted["op_id"]
    assert retracted["kind"] == "committed"
    assert retracted["operation"] == "withdraw"

    assert Regex.match?(
             ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/,
             op_id
           )

    assert await_origin_command_call!("withdraw", 10_000) == %{
             "v" => 1,
             "type" => "withdraw",
             "op_id" => op_id,
             "sender_id" => @sender_id,
             "message_id" => request["request"]
           }

    retry = cli_json!(agent_bin, ["retry", "--op-id", op_id], fixture.env)
    assert retry["kind"] == "committed"
    assert retry["op_id"] == op_id
    assert drain_origin_command_calls("withdraw") == 0

    {:ok, %{"entries" => entries}} = Relay.origin_feed(fixture.relay, fixture.origin, 0)

    matching =
      Enum.filter(
        entries,
        &(&1["kind"] == "terminal" and &1["message_id"] == request["request"])
      )

    assert length(matching) == 1
    assert hd(matching)["terminal_state"] == "retracted"
  end

  test "a retracted-answer refusal keeps its terminal snapshot and exact retry key" do
    root =
      Path.join(
        System.tmp_dir!(),
        "harklight-agent-cli-retracted-answer-#{System.unique_integer([:positive])}"
      )

    fixture = start_file_fixture!(root, :none)
    agent_bin = System.fetch_env!("HARKLIGHT_AGENT_BIN")

    request =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "claude-code",
          "--title",
          "Refused answer fixture"
        ],
        fixture.env
      )

    turn =
      submit_discussion!(fixture.relay, fixture.reader, request["request"], 902, "Answer this")

    event = cli_next!(agent_bin, request["session"], fixture.env)
    assert event["event_id"] == turn["event_id"]

    ack =
      cli_json!(
        agent_bin,
        ["ack", "--session", request["session"], "--event", event["event_id"]],
        fixture.env
      )

    assert ack["status"] == "delivered"

    terminal = %{"state" => "retracted", "retracted_at" => 1_792_368_000_123}
    refusal = %{"v" => 1, "type" => "error", "reason" => "retracted", "terminal" => terminal}
    {port, proxy} = start_refusal_proxy!(fixture, refusal)
    proxy_env = put_env(fixture.env, "HARKLIGHT_ORIGIN_PORT", Integer.to_string(port))
    op_id = op(903)

    {output, status} =
      System.cmd(
        agent_bin,
        [
          "reply",
          "--session",
          request["session"],
          "--request",
          request["request"],
          "--in-reply-to",
          event["event_id"],
          "--body",
          "An answer that must not be reported as sent",
          "--op-id",
          op_id
        ],
        env: proxy_env,
        stderr_to_stdout: true
      )

    assert status == 2
    result = decode_first_line!(output)
    assert result["kind"] == "refused"
    assert result["operation"] == "reply"
    assert result["op_id"] == op_id
    assert result["reason"] == "retracted"
    assert result["response"]["terminal"] == terminal
    refute Map.has_key?(result, "sent")

    observed = Task.await(proxy, 15_000)
    assert observed.path == "/origin/v1"
    assert observed.request["type"] == "reply"
    assert observed.request["op_id"] == op_id

    {retry_output, retry_status} =
      System.cmd(agent_bin, ["retry", "--op-id", op_id], env: fixture.env, stderr_to_stdout: true)

    assert retry_status == 2
    retried = decode_first_line!(retry_output)
    assert retried["kind"] == "refused"
    assert retried["op_id"] == op_id
    assert retried["reason"] == "retracted"
    assert retried["response"]["terminal"] == terminal

    {:ok, %{"entries" => entries}} = Relay.origin_feed(fixture.relay, fixture.origin, 0)
    refute Enum.any?(entries, &(&1["op_id"] == op_id))
  end

  test "controller support is limited to an other-route follow listener" do
    root =
      Path.join(
        System.tmp_dir!(),
        "harklight-agent-cli-controller-#{System.unique_integer([:positive])}"
      )

    fixture = start_file_fixture!(root, :none)
    agent_bin = System.fetch_env!("HARKLIGHT_AGENT_BIN")

    start_origin_command_trace!()
    on_exit(&stop_origin_command_trace/0)

    {invalid_send_output, invalid_send_status} =
      System.cmd(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "claude-code",
          "--controller",
          "--title",
          "Invalid controller route"
        ],
        env: fixture.env,
        stderr_to_stdout: true
      )

    assert invalid_send_status == 2

    assert String.contains?(
             invalid_send_output,
             "--controller requires --session new --route other"
           )

    assert drain_origin_command_calls("send") == 0

    controller_request =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "other",
          "--controller",
          "--title",
          "Controller route"
        ],
        fixture.env
      )

    send_frame = await_origin_command_call!("send", 5_000)
    assert send_frame["route"] == "other"
    assert is_binary(send_frame["cli_version"])
    assert send_frame["unsupported_reason"] == nil

    controller_status =
      cli_json!(agent_bin, ["status", "--session", controller_request["session"]], fixture.env)

    assert controller_status["listening_support"]["route"] == "other"
    assert controller_status["listening_support"]["unsupported_reason"] == nil

    {invalid_next_output, invalid_next_status} =
      System.cmd(
        agent_bin,
        [
          "listen",
          "--session",
          controller_request["session"],
          "--next",
          "--controller"
        ],
        env: fixture.env,
        stderr_to_stdout: true
      )

    assert invalid_next_status == 2
    assert String.contains?(invalid_next_output, "--controller requires listen --follow")
    assert drain_origin_command_calls("support") == 0

    controller_listener =
      launch_listener(agent_bin, controller_request["session"], "follow", fixture.env, true)

    supported_mint_arm = await_origin_command_call!("support", 5_000)
    assert supported_mint_arm["route"] == "other"
    assert supported_mint_arm["cli_version"] == send_frame["cli_version"]
    assert supported_mint_arm["unsupported_reason"] == nil
    wait_availability!(fixture.relay, fixture.origin, controller_request["session"], "listening")
    terminate_listener!(controller_listener)

    wait_availability!(
      fixture.relay,
      fixture.origin,
      controller_request["session"],
      "not_listening"
    )

    claude_request =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "claude-code",
          "--title",
          "Claude route"
        ],
        fixture.env
      )

    _claude_send_frame = await_origin_command_call!("send", 5_000)

    claude_status_before =
      cli_json!(agent_bin, ["status", "--session", claude_request["session"]], fixture.env)

    {wrong_route_output, wrong_route_status} =
      System.cmd(
        agent_bin,
        [
          "listen",
          "--session",
          claude_request["session"],
          "--follow",
          "--controller"
        ],
        env: fixture.env,
        stderr_to_stdout: true
      )

    assert wrong_route_status == 2

    assert String.contains?(
             wrong_route_output,
             "--controller requires a session declared with route other"
           )

    assert drain_origin_command_calls("support") == 0

    claude_status_after =
      cli_json!(agent_bin, ["status", "--session", claude_request["session"]], fixture.env)

    assert claude_status_after["listening_support"] == claude_status_before["listening_support"]

    unsupported_request =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "other",
          "--title",
          "Other route without controller"
        ],
        fixture.env
      )

    _unsupported_send_frame = await_origin_command_call!("send", 5_000)
    assert unsupported_request["session"] != controller_request["session"]

    turn =
      submit_discussion!(
        fixture.relay,
        fixture.reader,
        unsupported_request["request"],
        1,
        "Pending until the controller arms"
      )

    {unsupported_output, unsupported_status} =
      System.cmd(
        agent_bin,
        ["listen", "--session", unsupported_request["session"], "--follow"],
        env: fixture.env,
        stderr_to_stdout: true
      )

    assert unsupported_status != 0
    unsupported = decode_first_line!(unsupported_output)
    assert unsupported["kind"] == "unsupported"
    assert unsupported["reason"] == "no controller declared for the other route"

    unsupported_arm = await_origin_command_call!("support", 5_000)
    assert unsupported_arm["route"] == "other"
    assert unsupported_arm["unsupported_reason"] == "no controller declared for the other route"

    unsupported_status =
      wait_availability!(
        fixture.relay,
        fixture.origin,
        unsupported_request["session"],
        "unsupported"
      )

    assert unsupported_status["listening_support"]["unsupported_reason"] ==
             "no controller declared for the other route"

    {:ok, %{"entries" => pending_entries}} =
      Relay.origin_feed(fixture.relay, fixture.origin, 0)

    assert Enum.any?(pending_entries, &(&1["event_id"] == turn["event_id"]))

    listener =
      launch_listener(agent_bin, unsupported_request["session"], "follow", fixture.env, true)

    supported_arm = await_origin_command_call!("support", 5_000)
    assert supported_arm["route"] == "other"
    assert supported_arm["cli_version"] == send_frame["cli_version"]
    assert supported_arm["unsupported_reason"] == nil
    wait_availability!(fixture.relay, fixture.origin, unsupported_request["session"], "listening")

    {lock_output, lock_status} =
      System.cmd(
        agent_bin,
        [
          "listen",
          "--session",
          unsupported_request["session"],
          "--follow",
          "--controller"
        ],
        env: fixture.env,
        stderr_to_stdout: true
      )

    assert lock_status == 6
    assert String.contains?(lock_output, "listener already active")
    assert drain_origin_command_calls("support") == 0

    event = receive_event!(listener)
    assert event["event_id"] == turn["event_id"]
    assert event["request"]["message_id"] == unsupported_request["request"]
    terminate_listener!(listener)
  end

  test "same-handle listen replays pending work until an action commits" do
    root =
      Path.join(
        System.tmp_dir!(),
        "harklight-agent-cli-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root)
    certs = TLSFixtures.create!(server_san: "DNS:relay.test")

    on_exit(fn ->
      File.rm_rf!(root)
      File.rm_rf!(certs)
    end)

    {:ok, server} =
      Server.start_link(
        store_path: Path.join(root, "state.sqlite3"),
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem"),
        port: 0,
        origin_port: 0,
        relay_name: String.to_atom("agent_cli_fixture_#{System.unique_integer([:positive])}")
      )

    Process.unlink(server)
    on_exit(fn -> if Process.alive?(server), do: Supervisor.stop(server) end)
    relay = Server.relay(server)
    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))

    {:ok, origin} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, reader} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "b1"))

    :ok = Relay.setup_sender(relay, origin, @sender_id)

    identity_dir = Path.join(root, "identity")
    state_dir = Path.join(root, "agent-state")
    write_identity!(identity_dir, certs, person, origin, server)

    legacy_journal_dir =
      Path.join(state_dir, :crypto.hash(:sha256, origin) |> Base.encode16(case: :lower))

    File.mkdir_p!(legacy_journal_dir)
    File.chmod!(legacy_journal_dir, 0o700)
    legacy_journal_path = Path.join(legacy_journal_dir, "origin-feed.json")
    legacy_journal = %{"schema" => 1, "cursor" => 0, "entries" => %{}, "operations" => %{}}
    legacy_journal_bytes = JSON.encode!(legacy_journal) <> "\n"
    File.write!(legacy_journal_path, legacy_journal_bytes)

    env =
      cli_env(
        identity_dir,
        state_dir,
        Path.join(certs, "ca.pem"),
        Server.origin_port(server)
      )

    agent_bin = System.fetch_env!("HARKLIGHT_AGENT_BIN")

    send_result =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "claude-code",
          "--title",
          "Rust CLI fixture request",
          "--body",
          "Disposable conversation fixture"
        ],
        env
      )

    session = send_result["session"]
    request_id = send_result["request"]
    assert is_binary(session)
    assert is_binary(request_id)
    assert File.exists?(Path.join(legacy_journal_dir, "origin-feed.sqlite3"))
    assert File.read!(legacy_journal_path) == legacy_journal_bytes

    {unsupported_output, unsupported_status} =
      System.cmd(
        agent_bin,
        ["listen", "--next", "--session", session, "--session-context", "bare"],
        env: env,
        stderr_to_stdout: true
      )

    assert unsupported_status != 0
    unsupported = decode_first_line!(unsupported_output)
    assert unsupported["kind"] == "unsupported"
    assert unsupported["session_handle"] == session
    assert is_binary(unsupported["reason"])

    unsupported_status = cli_json!(agent_bin, ["status", "--session", session], env)
    assert unsupported_status["availability"] == "unsupported"
    assert is_binary(unsupported_status["unsupported_reason"])

    listener = launch_listener(agent_bin, session, "follow", env)
    wait_availability!(relay, origin, session, "listening")

    {lock_output, lock_status} =
      System.cmd(
        agent_bin,
        [
          "listen",
          "--session",
          session,
          "--session-context",
          "interactive",
          "--claude-code-version",
          "2.1.234",
          "--background-tasks-enabled",
          "--next"
        ],
        env: env,
        stderr_to_stdout: true
      )

    assert lock_status == 6
    assert String.contains?(lock_output, "listener already active")

    first_turn = submit_discussion!(relay, reader, request_id, 1, "first person turn")
    duplicate_turn = submit_discussion!(relay, reader, request_id, 1, "first person turn")
    assert duplicate_turn["event_id"] == first_turn["event_id"]
    first_event = receive_event!(listener)
    assert first_event["event_id"] == first_turn["event_id"]
    assert first_event["request"]["message_id"] == request_id
    assert first_event["request_revision"] == 1
    assert first_event["entry"]["text"] == "first person turn"
    refute_receive {^listener, {:exit_status, _status}}, 100

    terminate_listener!(listener)
    wait_availability!(relay, origin, session, "not_listening")

    replay_listener = launch_listener(agent_bin, session, "next", env)
    replayed = receive_event!(replay_listener)
    assert {:ok, 0} = await_exit(replay_listener, 10_000)
    assert replayed["event_id"] == first_event["event_id"]
    assert replayed["request"]["message_id"] == request_id
    assert replayed["request_revision"] == first_event["request_revision"]
    assert replayed["delivered"] == false
    assert replayed["acted"] == false

    ack =
      cli_json!(
        agent_bin,
        ["ack", "--session", session, "--event", first_event["event_id"]],
        env
      )

    assert ack["kind"] == "acknowledged"
    assert ack["status"] == "delivered"
    assert ack["event_id"] == first_event["event_id"]

    already_delivered =
      cli_json!(
        agent_bin,
        ["ack", "--session", session, "--event", first_event["event_id"]],
        env
      )

    assert already_delivered["status"] == "already_delivered"

    {:ok, %{"entries" => entries}} = Relay.origin_feed(relay, origin, 0)
    first_feed_entry = Enum.find(entries, &(&1["event_id"] == first_event["event_id"]))
    assert first_feed_entry["delivered"] == true

    delivered_replay_listener = launch_listener(agent_bin, session, "next", env)
    delivered_replay = receive_event!(delivered_replay_listener)
    assert {:ok, 0} = await_exit(delivered_replay_listener, 10_000)
    assert delivered_replay["event_id"] == first_event["event_id"]
    assert delivered_replay["request"]["message_id"] == request_id
    assert delivered_replay["request_revision"] == 1
    assert delivered_replay["delivered"] == true
    assert delivered_replay["acted"] == false

    reply_op_id = op(101)

    reply =
      cli_json!(
        agent_bin,
        [
          "reply",
          "--session",
          session,
          "--request",
          request_id,
          "--in-reply-to",
          first_event["event_id"],
          "--body",
          "Handled once after recovery",
          "--op-id",
          reply_op_id
        ],
        env
      )

    assert reply["kind"] == "committed"
    assert reply["operation"] == "reply"

    reply_retry = cli_json!(agent_bin, ["retry", "--op-id", reply_op_id], env)

    assert reply_retry["kind"] == "committed"
    assert reply_retry["op_id"] == reply_op_id
    {:ok, %{"entries" => after_reply_entries}} = Relay.origin_feed(relay, origin, 0)
    assert Enum.count(after_reply_entries, &(&1["op_id"] == reply_op_id)) == 1

    backlog =
      Enum.map(400..527, fn operation_number ->
        submit_discussion!(
          relay,
          reader,
          request_id,
          operation_number,
          "backlog turn #{operation_number}"
        )
      end)

    next_listener = launch_listener(agent_bin, session, "next", env)
    wait_availability!(relay, origin, session, "listening")
    first_backlog_event = receive_event!(next_listener)
    assert {:ok, 0} = await_exit(next_listener, 10_000)
    assert first_backlog_event["event_id"] == hd(backlog)["event_id"]
    refute first_backlog_event["event_id"] == first_event["event_id"]
    assert first_backlog_event["request"]["message_id"] == request_id
    assert first_backlog_event["request_revision"] == 1
    assert first_backlog_event["entry"]["text"] == "backlog turn 400"

    second_ack =
      cli_json!(
        agent_bin,
        ["ack", "--session", session, "--event", first_backlog_event["event_id"]],
        env
      )

    assert second_ack["status"] == "delivered"

    backlog_reply =
      cli_json!(
        agent_bin,
        [
          "reply",
          "--session",
          session,
          "--request",
          request_id,
          "--in-reply-to",
          first_backlog_event["event_id"],
          "--body",
          "Handled from a long request history",
          "--op-id",
          op(601)
        ],
        env
      )

    assert backlog_reply["kind"] == "committed"
    assert backlog_reply["operation"] == "reply"

    remaining_listener = launch_listener(agent_bin, session, "next", env)
    remaining_event = receive_event!(remaining_listener)
    assert {:ok, 0} = await_exit(remaining_listener, 10_000)
    assert remaining_event["event_id"] == Enum.at(backlog, 1)["event_id"]
    assert remaining_event["entry"]["text"] == "backlog turn 401"

    gone = cli_json!(agent_bin, ["session", "end", "--session", session, "--gone"], env)
    assert gone["availability"] == "gone"
    final_status = cli_json!(agent_bin, ["status", "--session", session], env)
    assert final_status["availability"] == "gone"
  end

  test "CLI recovers a lost answered-result response without claiming an unknown action applied" do
    root =
      Path.join(
        System.tmp_dir!(),
        "harklight-agent-cli-result-#{System.unique_integer([:positive])}"
      )

    fixture = start_file_fixture!(root, :none)
    agent_bin = System.fetch_env!("HARKLIGHT_AGENT_BIN")

    request =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "claude-code",
          "--title",
          "Result recovery"
        ],
        fixture.env
      )

    submit_answer!(fixture.relay, fixture.reader, request["request"], 41, "approve")
    terminal = cli_next!(agent_bin, request["session"], fixture.env)
    assert terminal["entry"]["kind"] == "terminal"
    assert terminal["entry"]["terminal_state"] == "answered"
    assert terminal["request"]["message_id"] == request["request"]
    assert terminal["request_revision"] == request["revision"]

    ack =
      cli_json!(
        agent_bin,
        ["ack", "--session", request["session"], "--event", terminal["event_id"]],
        fixture.env
      )

    assert ack["status"] == "delivered"

    {:ok, %{"entries" => after_ack}} = Relay.origin_feed(fixture.relay, fixture.origin, 0)
    acknowledged = Enum.find(after_ack, &(&1["event_id"] == terminal["event_id"]))
    assert acknowledged["delivered"] == true
    assert acknowledged["acted"] == false

    replay = cli_next!(agent_bin, request["session"], fixture.env)
    assert replay["event_id"] == terminal["event_id"]
    assert replay["request"] == terminal["request"]
    assert replay["request_revision"] == terminal["request_revision"]
    assert replay["delivered"] == true
    assert replay["acted"] == false

    received_op = op(902)
    {proxy_port, proxy} = start_drop_response_proxy!(fixture)
    proxy_env = put_env(fixture.env, "HARKLIGHT_ORIGIN_PORT", Integer.to_string(proxy_port))

    {unknown_output, unknown_status} =
      System.cmd(
        agent_bin,
        [
          "result",
          "--session",
          request["session"],
          "--request",
          request["request"],
          "--in-reply-to",
          terminal["event_id"],
          "--status",
          "received",
          "--op-id",
          received_op
        ],
        env: proxy_env,
        stderr_to_stdout: true
      )

    assert unknown_status == 7
    unknown = decode_first_line!(unknown_output)
    assert unknown["kind"] == "outcome_unknown"
    assert unknown["operation"] == "result"
    assert unknown["op_id"] == received_op

    dropped = Task.await(proxy, 15_000)
    assert dropped.request["type"] == "result"
    assert dropped.request["status"] == "received"
    assert dropped.request["op_id"] == received_op
    assert dropped.response["type"] == "ok"
    refute dropped.response["duplicate"] == true

    recovered = cli_json!(agent_bin, ["retry", "--op-id", received_op], fixture.env)
    assert recovered["kind"] == "committed"
    assert recovered["operation"] == "retry"
    assert recovered["op_id"] == received_op
    assert recovered["response"]["duplicate"] == true

    {:ok, %{"entries" => after_received}} = Relay.origin_feed(fixture.relay, fixture.origin, 0)

    received_fact =
      Enum.find(
        after_received,
        &(&1["kind"] == "result" and &1["in_reply_to"] == terminal["event_id"])
      )

    assert received_fact["status"] == "received"
    assert Enum.count(after_received, &(&1["op_id"] == received_op)) == 1
    acted_terminal = Enum.find(after_received, &(&1["event_id"] == terminal["event_id"]))
    assert acted_terminal["acted"] == true

    idle = cli_next!(agent_bin, request["session"], fixture.env)
    assert idle == %{"kind" => "idle", "open_requests" => 0}
  end

  @tag :long_idle
  @tag timeout: 3_700_000
  test "listener fetches once and holds its SSE stream through an idle hour" do
    root =
      Path.join(
        System.tmp_dir!(),
        "harklight-agent-cli-idle-hour-#{System.unique_integer([:positive])}"
      )

    fixture = start_file_fixture!(root, :none)
    agent_bin = System.fetch_env!("HARKLIGHT_AGENT_BIN")

    request =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "claude-code",
          "--title",
          "Idle hour"
        ],
        fixture.env
      )

    start_origin_command_trace!()
    on_exit(&stop_origin_command_trace/0)
    listener = launch_listener(agent_bin, request["session"], "follow", fixture.env)
    wait_availability!(fixture.relay, fixture.origin, request["session"], "listening")
    await_origin_feed_call!(10_000)

    Process.sleep(3_600_000)

    assert drain_origin_feed_calls() == 0

    assert Relay.origin_status(fixture.relay, fixture.origin, request["session"])
           |> elem(1)
           |> Map.fetch!("availability") == "listening"

    terminate_listener!(listener)
  end

  test "a journal write failure before commit leaves the server entry replayable" do
    root =
      Path.join(
        System.tmp_dir!(),
        "harklight-agent-cli-before-journal-#{System.unique_integer([:positive])}"
      )

    fixture = start_file_fixture!(root, :none)
    agent_bin = System.fetch_env!("HARKLIGHT_AGENT_BIN")

    request =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "claude-code",
          "--title",
          "Crash before journal"
        ],
        fixture.env
      )

    start_origin_command_trace!()
    on_exit(&stop_origin_command_trace/0)
    listener = launch_listener(agent_bin, request["session"], "follow", fixture.env)
    wait_availability!(fixture.relay, fixture.origin, request["session"], "listening")
    await_origin_feed_call!(10_000)

    database = agent_journal_database!(fixture, request["session"])
    Process.sleep(100)
    assert {:ok, db} = Sqlite3.open(database)
    exec_sql!(db, "BEGIN IMMEDIATE")

    turn =
      try do
        turn =
          submit_discussion!(
            fixture.relay,
            fixture.reader,
            request["request"],
            42,
            "Crash while recording"
          )

        await_origin_feed_call!(10_000)
        assert {:ok, status} = await_exit(listener, 10_000)
        assert status != 0
        wait_availability!(fixture.relay, fixture.origin, request["session"], "not_listening")

        assert {:ok, %{"entries" => entries}} =
                 Relay.origin_feed(fixture.relay, fixture.origin, 0)

        server_turn = Enum.find(entries, &(&1["event_id"] == turn["event_id"]))
        assert server_turn["delivered"] == false
        turn
      after
        exec_sql!(db, "ROLLBACK")
        assert :ok = Sqlite3.close(db)
      end

    recovered = cli_next!(agent_bin, request["session"], fixture.env)
    assert recovered["event_id"] == turn["event_id"]
    assert recovered["request"]["message_id"] == request["request"]
    assert recovered["request_revision"] == 1
    assert recovered["delivered"] == false
  end

  test "a stdout failure after journal commit leaves the turn available for replay" do
    root =
      Path.join(
        System.tmp_dir!(),
        "harklight-agent-cli-after-journal-#{System.unique_integer([:positive])}"
      )

    fixture = start_file_fixture!(root, :none)
    agent_bin = System.fetch_env!("HARKLIGHT_AGENT_BIN")

    request =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "claude-code",
          "--title",
          "Crash after journal"
        ],
        fixture.env
      )

    turn =
      submit_discussion!(
        fixture.relay,
        fixture.reader,
        request["request"],
        43,
        "Output pipe closes"
      )

    start_origin_command_trace!()
    on_exit(&stop_origin_command_trace/0)

    shell =
      ~S(exec "$1" listen --session "$2" --session-context interactive --claude-code-version 2.1.234 --background-tasks-enabled --next | /usr/bin/head -n 0)

    {_output, status} =
      System.cmd(
        "/bin/bash",
        ["-o", "pipefail", "-c", shell, "fixture", agent_bin, request["session"]],
        env: fixture.env,
        stderr_to_stdout: true
      )

    assert status != 0
    await_origin_feed_call!(10_000)

    {:ok, %{"entries" => entries}} = Relay.origin_feed(fixture.relay, fixture.origin, 0)
    server_turn = Enum.find(entries, &(&1["event_id"] == turn["event_id"]))
    assert server_turn["delivered"] == false

    replay = cli_next!(agent_bin, request["session"], fixture.env)
    assert replay["event_id"] == turn["event_id"]
    assert replay["request"]["message_id"] == request["request"]
    assert replay["request_revision"] == 1
    assert replay["delivered"] == false
  end

  test "CLI retries an unknown send with the same operation after the server commits and drops the response" do
    root =
      Path.join(
        System.tmp_dir!(),
        "harklight-agent-cli-unknown-send-#{System.unique_integer([:positive])}"
      )

    fixture = start_file_fixture!(root, :none)
    agent_bin = System.fetch_env!("HARKLIGHT_AGENT_BIN")
    {proxy_port, proxy} = start_drop_response_proxy!(fixture)
    proxy_env = put_env(fixture.env, "HARKLIGHT_ORIGIN_PORT", Integer.to_string(proxy_port))

    {output, status} =
      System.cmd(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "claude-code",
          "--title",
          "Unknown response recovery"
        ],
        env: proxy_env,
        stderr_to_stdout: true
      )

    assert status == 7
    unknown = decode_first_line!(output)
    assert unknown["kind"] == "outcome_unknown"
    assert unknown["operation"] == "send"
    assert is_binary(unknown["op_id"])

    dropped = Task.await(proxy, 15_000)
    assert dropped.path == "/origin/v1"
    assert dropped.request["type"] == "send"
    assert dropped.request["op_id"] == unknown["op_id"]
    assert dropped.response["type"] == "ok"
    refute dropped.response["duplicate"] == true

    recovered = cli_json!(agent_bin, ["retry", "--op-id", unknown["op_id"]], fixture.env)
    assert recovered["kind"] == "committed"
    assert recovered["op_id"] == unknown["op_id"]
    assert recovered["response"]["duplicate"] == true
    {:ok, %{"entries" => entries}} = Relay.origin_feed(fixture.relay, fixture.origin, 0)
    assert Enum.count(entries, &(&1["op_id"] == unknown["op_id"])) == 1
  end

  test "CLI converges on feed order after duplicate and reordered high-water notices" do
    root =
      Path.join(
        System.tmp_dir!(),
        "harklight-agent-cli-notice-order-#{System.unique_integer([:positive])}"
      )

    fixture = start_file_fixture!(root, :none)
    agent_bin = System.fetch_env!("HARKLIGHT_AGENT_BIN")

    request =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "claude-code",
          "--title",
          "Notice order"
        ],
        fixture.env
      )

    start_ssl_send_trace!()
    on_exit(&stop_ssl_send_trace/0)
    listener = launch_listener(agent_bin, request["session"], "follow", fixture.env)
    socket = await_change_notice_socket!(10_000)
    stop_ssl_send_trace()
    wait_availability!(fixture.relay, fixture.origin, request["session"], "listening")

    relay_state = :sys.get_state(fixture.relay)

    stream =
      Map.fetch!(relay_state.origin_streams, {fixture.person, fixture.origin, request["session"]})

    assert true = :erlang.suspend_process(stream.worker)

    on_exit(fn ->
      if Process.alive?(stream.worker) do
        try do
          :erlang.resume_process(stream.worker)
        rescue
          ArgumentError -> :ok
        end
      end
    end)

    first =
      submit_discussion!(fixture.relay, fixture.reader, request["request"], 44, "feed turn one")

    second =
      submit_discussion!(fixture.relay, fixture.reader, request["request"], 45, "feed turn two")

    high_water = String.to_integer(second["event_id"])
    notices = [high_water, high_water - 1, high_water]

    Enum.each(notices, fn cursor ->
      :ok =
        :ssl.send(socket, [
          "event: change\n",
          "data: ",
          JSON.encode!(%{"cursor" => cursor}),
          "\n\n"
        ])
    end)

    first_event = receive_event!(listener)
    second_event = receive_event!(listener)

    assert [first_event["event_id"], second_event["event_id"]] == [
             first["event_id"],
             second["event_id"]
           ]

    assert [first_event["entry"]["text"], second_event["entry"]["text"]] == [
             "feed turn one",
             "feed turn two"
           ]

    assert true = :erlang.resume_process(stream.worker)
    refute_receive {^listener, {:data, {:eol, _line}}}, 300
    terminate_listener!(listener)
  end

  test "startup fetch includes a turn committed after its stream opens" do
    root =
      Path.join(
        System.tmp_dir!(),
        "harklight-agent-cli-startup-race-#{System.unique_integer([:positive])}"
      )

    fixture = start_file_fixture!(root, :none)
    agent_bin = System.fetch_env!("HARKLIGHT_AGENT_BIN")

    request =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "claude-code",
          "--title",
          "Startup race"
        ],
        fixture.env
      )

    {proxy_port, _proxy} = start_feed_barrier_proxy!(fixture, self())
    proxy_env = put_env(fixture.env, "HARKLIGHT_ORIGIN_PORT", Integer.to_string(proxy_port))
    listener = launch_listener(agent_bin, request["session"], "next", proxy_env)

    assert_receive {:startup_stream_open, _stream_worker, session}, 10_000
    assert session == request["session"]
    assert_receive {:startup_feed_waiting, feed_worker, feed_ref, feed_request}, 10_000
    assert feed_request["type"] == "feed"

    turn =
      submit_discussion!(
        fixture.relay,
        fixture.reader,
        request["request"],
        46,
        "Committed between stream open and fetch"
      )

    send(feed_worker, {:release_startup_feed, feed_ref})

    event = receive_event!(listener)
    assert {:ok, 0} = await_exit(listener, 10_000)
    assert event["event_id"] == turn["event_id"]
    assert event["request"]["message_id"] == request["request"]
    assert event["request_revision"] == 1
    assert event["entry"]["text"] == "Committed between stream open and fetch"
    refute_receive {^listener, {:data, _line}}, 300
  end

  test "CLI migrates populated legacy cursor, pending acknowledgement, and committed operation" do
    root =
      Path.join(
        System.tmp_dir!(),
        "harklight-agent-cli-legacy-#{System.unique_integer([:positive])}"
      )

    fixture = start_file_fixture!(root, :none)
    agent_bin = System.fetch_env!("HARKLIGHT_AGENT_BIN")
    session = "a9d32ee1-89cb-4a4a-9e21-719d5a54a3e1"
    request_id = "ad1a6874-44a1-4a22-a44d-abaed342c84f"
    send_op_id = op(301)

    send_request = %{
      "v" => 1,
      "type" => "send",
      "op_id" => send_op_id,
      "session_handle" => session,
      "route" => "claude-code",
      "cli_version" => "0.1.0",
      "unsupported_reason" => nil,
      "message" => %{
        "sender_id" => @sender_id,
        "message_id" => request_id,
        "created_at" => 1,
        "title" => "Populated legacy journal"
      }
    }

    send_response = origin_post!(fixture, send_request)
    assert send_response["type"] == "ok"

    discussion =
      submit_discussion!(
        fixture.relay,
        fixture.reader,
        request_id,
        302,
        "This turn has a pending acknowledgement in the old journal"
      )

    assert {:ok, %{"entries" => raw_entries, "cursor" => cursor}} =
             Relay.origin_feed(fixture.relay, fixture.origin, 0)

    legacy_entries =
      Map.new(raw_entries, fn raw ->
        sequence = String.to_integer(raw["event_id"])
        ack_pending = raw["event_id"] == discussion["event_id"]
        {Integer.to_string(sequence), legacy_entry(raw, ack_pending)}
      end)

    operation = %{
      "op_id" => send_op_id,
      "session_handle" => session,
      "kind" => "send",
      "request" => send_request,
      "state" => "committed",
      "created_at_ms" => 1,
      "updated_at_ms" => 2,
      "response" => send_response,
      "uploads" => []
    }

    legacy_journal = %{
      "schema" => 1,
      "cursor" => cursor,
      "entries" => legacy_entries,
      "operations" => %{send_op_id => operation}
    }

    legacy_directory =
      Path.join(
        Path.join(root, "agent-state"),
        :crypto.hash(:sha256, fixture.origin) |> Base.encode16(case: :lower)
      )

    File.mkdir_p!(legacy_directory)
    File.chmod!(legacy_directory, 0o700)
    legacy_path = Path.join(legacy_directory, "origin-feed.json")
    legacy_bytes = JSON.encode!(legacy_journal) <> "\n"
    File.write!(legacy_path, legacy_bytes)

    retried = cli_json!(agent_bin, ["retry", "--op-id", send_op_id], fixture.env)
    assert retried["kind"] == "committed"
    assert retried["operation"] == "send"
    assert retried["response"] == send_response

    event = cli_next!(agent_bin, session, fixture.env)
    assert event["event_id"] == discussion["event_id"]
    assert event["delivered"] == true
    assert File.read!(legacy_path) == legacy_bytes

    {:ok, %{"entries" => server_entries}} =
      Relay.origin_feed(fixture.relay, fixture.origin, cursor)

    assert server_entries == []
    {:ok, %{"entries" => all_entries}} = Relay.origin_feed(fixture.relay, fixture.origin, 0)
    recovered = Enum.find(all_entries, &(&1["event_id"] == discussion["event_id"]))
    assert recovered["delivered"] == true
  end

  test "CLI applies published inline and upload tiers, including files above the JSON body bound" do
    root =
      Path.join(
        System.tmp_dir!(),
        "harklight-agent-cli-files-#{System.unique_integer([:positive])}"
      )

    fixture = start_file_fixture!(root, :none)
    agent_bin = System.fetch_env!("HARKLIGHT_AGENT_BIN")

    inline_bytes = :binary.copy(<<17>>, 262_144)
    inline_path = write_file!(root, "inline.bin", inline_bytes)

    request =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "claude-code",
          "--title",
          "Inline boundary",
          "--file",
          inline_path
        ],
        fixture.env
      )

    inline_entry = origin_entry!(fixture.relay, fixture.origin, request["request"])
    [inline_file] = inline_entry["files"]
    assert inline_file["tier"] == "inline"
    assert inline_file["size"] == 262_144
    assert Base.decode64!(inline_file["data_base64"]) == inline_bytes

    first_mixed_bytes = :binary.copy(<<29, 0, 255>>, 70_000)
    second_mixed_bytes = :binary.copy(<<254, 7>>, 50_000)
    assert byte_size(first_mixed_bytes) + byte_size(second_mixed_bytes) > 262_144
    first_mixed_path = write_file!(root, "mixed-inline.bin", first_mixed_bytes)
    second_mixed_path = write_file!(root, "mixed-upload.bin", second_mixed_bytes)

    mixed_request =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          request["session"],
          "--sender-id",
          @sender_id,
          "--title",
          "Mixed tier",
          "--file",
          first_mixed_path,
          "--file",
          second_mixed_path
        ],
        fixture.env
      )

    mixed_entry = origin_entry!(fixture.relay, fixture.origin, mixed_request["request"])
    [mixed_inline, mixed_upload] = mixed_entry["files"]
    assert mixed_inline["name"] == "mixed-inline.bin"
    assert mixed_inline["tier"] == "inline"
    assert mixed_upload["name"] == "mixed-upload.bin"
    assert mixed_upload["tier"] == "upload"
    assert mixed_upload["size"] == byte_size(second_mixed_bytes)
    assert mixed_upload["sha256"] == sha256(second_mixed_bytes)
    assert fetch_uploaded_bytes!(fixture, mixed_upload["upload_id"]) == second_mixed_bytes

    large_bytes = :binary.copy(<<0, 255, 12, 90>>, div(2 * 1024 * 1024 + 4, 4)) <> <<1>>
    assert byte_size(large_bytes) > 2 * 1024 * 1024
    large_path = write_file!(root, "larger-than-json.bin", large_bytes)

    large_request =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          request["session"],
          "--sender-id",
          @sender_id,
          "--title",
          "Raw upload",
          "--file",
          large_path
        ],
        fixture.env
      )

    large_entry = origin_entry!(fixture.relay, fixture.origin, large_request["request"])
    [large_upload] = large_entry["files"]
    assert large_upload["tier"] == "upload"
    assert large_upload["size"] == byte_size(large_bytes)
    assert large_upload["sha256"] == sha256(large_bytes)
    assert fetch_uploaded_bytes!(fixture, large_upload["upload_id"]) == large_bytes

    upload_directory = fixture.store_path <> ".uploads"
    backup_directory = upload_directory <> ".saved"
    assert :ok = File.rename(upload_directory, backup_directory)
    File.write!(upload_directory, "storage failure fixture")

    storage_bytes = :binary.copy(<<7, 11, 19>>, 100_000)
    storage_path = write_file!(root, "storage-failure.bin", storage_bytes)
    {:ok, %{"cursor" => cursor_before}} = Relay.origin_feed(fixture.relay, fixture.origin, 0)

    {storage_output, storage_status} =
      System.cmd(
        agent_bin,
        [
          "send",
          "--session",
          request["session"],
          "--sender-id",
          @sender_id,
          "--title",
          "Storage refusal must submit nothing",
          "--file",
          storage_path
        ],
        env: fixture.env,
        stderr_to_stdout: true
      )

    assert storage_status != 0
    storage_refusal = decode_first_line!(storage_output)
    assert storage_refusal["kind"] == "upload_refused"
    assert storage_refusal["file"] == "storage-failure.bin"
    assert storage_refusal["reason"] == "storage_unavailable"
    assert storage_refusal["sent"] == false
    assert is_nil(storage_refusal["cap_bytes_per_file"])
    refute storage_output =~ "upload_cap_exceeded"

    {:ok, %{"cursor" => cursor_after, "entries" => after_storage_refusal}} =
      Relay.origin_feed(fixture.relay, fixture.origin, cursor_before)

    assert cursor_after == cursor_before
    assert after_storage_refusal == []
  end

  test "agent upload inactivity is per group and active groups are fenced" do
    root =
      Path.join(
        System.tmp_dir!(),
        "harklight-agent-cli-upload-groups-#{System.unique_integer([:positive])}"
      )

    fixture = start_file_fixture!(root, :none)
    first_bytes = <<1, 3, 5, 7>>
    first_op = op(904)

    first =
      upload_person_file!(fixture, first_op, "agent-request-a", "first.bin", first_bytes, "a2")

    first_path = Path.join(fixture.store_path <> ".uploads", first["upload_id"])
    first_activity = upload_activity!(fixture.store_path, first["upload_id"])

    other_op = op(905)

    other =
      upload_person_file!(
        fixture,
        other_op,
        "agent-request-b",
        "other-group.bin",
        <<2, 4, 6>>,
        "a2"
      )

    other_path = Path.join(fixture.store_path <> ".uploads", other["upload_id"])

    incomplete_attrs = %{
      "op_id" => other_op,
      "sender_id" => @sender_id,
      "message_id" => "agent-request-b",
      "revision" => 1,
      "name" => "incomplete.bin"
    }

    assert {:ok, incomplete_lease} =
             Relay.upload_begin(
               fixture.relay,
               fixture.origin,
               incomplete_attrs,
               byte_size(first_bytes)
             )

    active_activity = upload_activity!(fixture.store_path, other["upload_id"])

    # A different active group must not extend the first group's retention.
    # Use each group's persisted millisecond timestamp as its own clock base;
    # wall-clock timestamps from separate operations need not be strictly ordered.
    sweep_now = max(first_activity, active_activity) + 60 * 60_000
    assert {:ok, 1} = Relay.sweep_uploads(fixture.relay, sweep_now)
    refute File.exists?(first_path)
    assert File.exists?(other_path)

    # Ending an incomplete upload stores no ready file. The remaining upload
    # expires at one hour from its group's latest persisted activity.
    assert :ok = Relay.upload_end(fixture.relay, incomplete_lease)
    assert {:ok, store} = Store.open(fixture.store_path)
    assert Map.keys(store.engine.uploads) == [other["upload_id"]]
    assert :ok = Store.close(store)

    final_activity = upload_activity!(fixture.store_path, other["upload_id"])
    assert {:ok, 0} = Relay.sweep_uploads(fixture.relay, final_activity + 60 * 60_000 - 1)
    assert File.exists?(other_path)

    assert {:ok, 1} = Relay.sweep_uploads(fixture.relay, final_activity + 60 * 60_000)
    refute File.exists?(other_path)

    assert {:error, :not_found} =
             Relay.fetch_file(fixture.relay, fixture.origin, other["upload_id"])
  end

  test "CLI verifies received upload bytes before transfer ACK and reports use separately" do
    root =
      Path.join(
        System.tmp_dir!(),
        "harklight-agent-cli-receive-#{System.unique_integer([:positive])}"
      )

    fixture = start_file_fixture!(root, :none)
    agent_bin = System.fetch_env!("HARKLIGHT_AGENT_BIN")

    request =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "claude-code",
          "--title",
          "File transfer request"
        ],
        fixture.env
      )

    good_bytes = :binary.copy(<<0, 255, 13>>, 700_001)
    good_op = op(201)
    good_name = "exact.bin"

    good_upload =
      upload_person_file!(fixture, good_op, request["request"], good_name, good_bytes)

    good_turn =
      submit_discussion_with_files!(
        fixture.relay,
        fixture.reader,
        request["request"],
        good_op,
        "Use the uploaded bytes",
        [upload_ref(good_upload, good_name, good_bytes)]
      )

    good_event = cli_next!(agent_bin, request["session"], fixture.env)
    assert good_event["event_id"] == good_turn["event_id"]
    [good_file] = good_event["entry"]["files"]
    assert good_file["transfer"] == "transferred"
    assert good_file["use"] == "unknown"
    assert File.read!(good_file["path"]) == good_bytes

    ack =
      cli_json!(
        agent_bin,
        ["ack", "--session", request["session"], "--event", good_event["event_id"]],
        fixture.env
      )

    assert ack["file_transfers"][good_name]["status"] == "transferred"

    use =
      cli_json!(
        agent_bin,
        [
          "file-use",
          "--session",
          request["session"],
          "--event",
          good_event["event_id"],
          "--file",
          good_name,
          "--used"
        ],
        fixture.env
      )

    assert use["kind"] == "committed"
    assert use["operation"] == "file_use"

    handled_good_turn =
      cli_json!(
        agent_bin,
        [
          "reply",
          "--session",
          request["session"],
          "--request",
          request["request"],
          "--in-reply-to",
          good_event["event_id"],
          "--body",
          "I inspected the uploaded bytes"
        ],
        fixture.env
      )

    assert handled_good_turn["kind"] == "committed"

    corrupt_bytes = :binary.copy(<<4, 5, 6>>, 90_000)
    corrupt_op = op(202)
    corrupt_name = "corrupt.bin"

    corrupt_upload =
      upload_person_file!(fixture, corrupt_op, request["request"], corrupt_name, corrupt_bytes)

    upload_path = Path.join(fixture.store_path <> ".uploads", corrupt_upload["upload_id"])
    assert File.exists?(upload_path)
    File.write!(upload_path, :binary.copy(<<99>>, byte_size(corrupt_bytes)))

    corrupt_turn =
      submit_discussion_with_files!(
        fixture.relay,
        fixture.reader,
        request["request"],
        corrupt_op,
        "The stored object no longer matches its committed hash",
        [upload_ref(corrupt_upload, corrupt_name, corrupt_bytes)]
      )

    corrupt_event = cli_next!(agent_bin, request["session"], fixture.env)
    assert corrupt_event["event_id"] == corrupt_turn["event_id"]
    [corrupt_file] = corrupt_event["entry"]["files"]
    assert corrupt_file["transfer"] == "not_transferred"
    assert is_binary(corrupt_file["reason"])

    corrupt_ack =
      cli_json!(
        agent_bin,
        ["ack", "--session", request["session"], "--event", corrupt_event["event_id"]],
        fixture.env
      )

    assert corrupt_ack["file_transfers"][corrupt_name]["status"] == "not_transferred"

    corrupt_reply =
      cli_json!(
        agent_bin,
        [
          "reply",
          "--session",
          request["session"],
          "--request",
          request["request"],
          "--in-reply-to",
          corrupt_event["event_id"],
          "--body",
          "I could not use the untransferred file"
        ],
        fixture.env
      )

    assert corrupt_reply["kind"] == "committed"

    {:ok, %{"entries" => entries}} = Relay.origin_feed(fixture.relay, fixture.origin, 0)
    good_feed_entry = Enum.find(entries, &(&1["event_id"] == good_event["event_id"]))
    corrupt_feed_entry = Enum.find(entries, &(&1["event_id"] == corrupt_event["event_id"]))
    assert good_feed_entry["file_use"][good_name]["status"] == "used"
    assert corrupt_feed_entry["file_transfers"][corrupt_name]["status"] == "not_transferred"
    refute Map.has_key?(corrupt_feed_entry["file_use"] || %{}, corrupt_name)

    storage_bytes = :binary.copy(<<6, 9, 12>>, 20_000)
    storage_op = op(203)
    storage_name = "unwritable.bin"

    storage_upload =
      upload_person_file!(fixture, storage_op, request["request"], storage_name, storage_bytes)

    storage_turn =
      submit_discussion_with_files!(
        fixture.relay,
        fixture.reader,
        request["request"],
        storage_op,
        "The local destination cannot be created",
        [upload_ref(storage_upload, storage_name, storage_bytes)]
      )

    device_scope = :crypto.hash(:sha256, fixture.origin) |> Base.encode16(case: :lower)

    event_directory =
      Path.join([
        fixture.root,
        "agent-state",
        device_scope,
        "files",
        request["session"],
        storage_turn["event_id"]
      ])

    File.mkdir_p!(Path.dirname(event_directory))
    File.write!(event_directory, "occupies the destination directory path")

    storage_event = cli_next!(agent_bin, request["session"], fixture.env)
    assert storage_event["event_id"] == storage_turn["event_id"]
    [storage_file] = storage_event["entry"]["files"]
    assert storage_file["transfer"] == "not_transferred"
    assert storage_file["reason"] =~ "could not inspect existing local file"

    storage_ack =
      cli_json!(
        agent_bin,
        ["ack", "--session", request["session"], "--event", storage_event["event_id"]],
        fixture.env
      )

    assert storage_ack["file_transfers"][storage_name]["status"] == "not_transferred"

    {:ok, %{"entries" => after_storage}} = Relay.origin_feed(fixture.relay, fixture.origin, 0)
    stored_storage_turn = Enum.find(after_storage, &(&1["event_id"] == storage_event["event_id"]))
    assert stored_storage_turn["file_transfers"][storage_name]["status"] == "not_transferred"
  end

  test "CLI refuses a file above the published finite cap without submitting the entry" do
    root =
      Path.join(
        System.tmp_dir!(),
        "harklight-agent-cli-cap-#{System.unique_integer([:positive])}"
      )

    fixture = start_file_fixture!(root, 1_024)
    agent_bin = System.fetch_env!("HARKLIGHT_AGENT_BIN")
    path = write_file!(root, "over-cap.bin", :binary.copy(<<1>>, 262_145))

    {output, status} =
      System.cmd(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "claude-code",
          "--title",
          "This entry must not be sent",
          "--body",
          "The server published a finite test cap",
          "--file",
          path
        ],
        env: fixture.env,
        stderr_to_stdout: true
      )

    assert status == 2
    refused = decode_first_line!(output)
    assert refused["kind"] == "file_preparation_refused"
    assert refused["sent"] == false
    assert refused["upload_cap_bytes_per_file"] == 1_024
    assert {:ok, %{"entries" => []}} = Relay.origin_feed(fixture.relay, fixture.origin, 0)
  end

  test "CLI cleanup preserves committed uploads and retries its original scope" do
    root =
      Path.join(
        System.tmp_dir!(),
        "harklight-agent-cli-cleanup-#{System.unique_integer([:positive])}"
      )

    fixture = start_file_fixture!(root, :none)
    agent_bin = System.fetch_env!("HARKLIGHT_AGENT_BIN")

    request =
      cli_json!(
        agent_bin,
        [
          "send",
          "--session",
          "new",
          "--sender-id",
          @sender_id,
          "--route",
          "claude-code",
          "--title",
          "Cleanup scope"
        ],
        fixture.env
      )

    message_id = request["request"]
    committed_bytes = :binary.copy(<<17, 23, 29>>, 1_000)
    committed_op = op(801)
    committed_name = "committed.bin"

    committed_upload =
      upload_person_file!(
        fixture,
        committed_op,
        message_id,
        committed_name,
        committed_bytes
      )

    _turn =
      submit_discussion_with_files!(
        fixture.relay,
        fixture.reader,
        message_id,
        committed_op,
        "This upload is now referenced",
        [upload_ref(committed_upload, committed_name, committed_bytes)]
      )

    uncommitted_a =
      upload_person_file!(
        fixture,
        op(802),
        message_id,
        "abandoned-a.bin",
        :binary.copy(<<31>>, 700)
      )

    uncommitted_b =
      upload_person_file!(
        fixture,
        op(803),
        message_id,
        "abandoned-b.bin",
        :binary.copy(<<37>>, 900)
      )

    upload_directory = fixture.store_path <> ".uploads"
    committed_path = Path.join(upload_directory, committed_upload["upload_id"])
    abandoned_a_path = Path.join(upload_directory, uncommitted_a["upload_id"])
    abandoned_b_path = Path.join(upload_directory, uncommitted_b["upload_id"])
    assert File.exists?(committed_path)
    assert File.exists?(abandoned_a_path)
    assert File.exists?(abandoned_b_path)

    active_bytes = :binary.copy(<<41>>, 1_200)

    active_attrs = %{
      "op_id" => op(805),
      "sender_id" => @sender_id,
      "message_id" => message_id,
      "revision" => 1,
      "name" => "active.bin"
    }

    assert {:ok, active_lease} =
             Relay.upload_begin(
               fixture.relay,
               fixture.reader,
               active_attrs,
               byte_size(active_bytes)
             )

    active_path = write_file!(root, "active-source.bin", active_bytes)

    client_identity = Path.join(root, "person-identity")
    client_state = Path.join(root, "person-state")

    write_identity!(
      client_identity,
      fixture.certs,
      fixture.person,
      fixture.reader,
      fixture.server,
      "b1"
    )

    client_env =
      cli_env(
        client_identity,
        client_state,
        fixture.endpoint.ca_file,
        fixture.endpoint.port
      )

    cleanup_op = op(804)

    cleanup =
      cli_json!(
        agent_bin,
        [
          "discard-uploads",
          "--sender-id",
          @sender_id,
          "--message-id",
          message_id,
          "--revision",
          "1",
          "--op-id",
          cleanup_op
        ],
        client_env
      )

    assert cleanup["kind"] == "committed"
    assert cleanup["operation"] == "discard_uploads"
    assert cleanup["op_id"] == cleanup_op
    assert cleanup["state"] == "committed"
    assert cleanup["discarded"] == 2
    assert File.exists?(committed_path)
    refute File.exists?(abandoned_a_path)
    refute File.exists?(abandoned_b_path)

    retry_result = cli_json!(agent_bin, ["retry", "--op-id", cleanup_op], client_env)
    assert retry_result["kind"] == "committed"
    assert retry_result["operation"] == "discard_uploads"
    assert retry_result["op_id"] == cleanup_op
    assert retry_result["state"] == "committed"
    assert retry_result["discarded"] == cleanup["discarded"]
    assert File.exists?(committed_path)

    assert {:error, :upload_discarded} =
             Relay.upload_file(
               fixture.relay,
               fixture.reader,
               active_attrs,
               active_path,
               byte_size(active_bytes),
               sha256(active_bytes),
               active_lease
             )

    assert :ok = Relay.upload_end(fixture.relay, active_lease)

    pending =
      upload_person_file!(
        fixture,
        op(806),
        message_id,
        "lost-response.bin",
        :binary.copy(<<47>>, 300)
      )

    pending_path = Path.join(upload_directory, pending["upload_id"])
    assert File.exists?(pending_path)

    lost_op = op(807)
    {proxy_port, proxy} = start_drop_response_proxy!(%{fixture | reader: fixture.reader})
    proxy_env = put_env(client_env, "HARKLIGHT_ORIGIN_PORT", Integer.to_string(proxy_port))

    {unknown_output, unknown_status} =
      System.cmd(
        agent_bin,
        [
          "discard-uploads",
          "--sender-id",
          @sender_id,
          "--message-id",
          message_id,
          "--revision",
          "1",
          "--op-id",
          lost_op
        ],
        env: proxy_env,
        stderr_to_stdout: true
      )

    assert unknown_status == 7
    unknown = decode_first_line!(unknown_output)
    assert unknown["kind"] == "outcome_unknown"
    assert unknown["op_id"] == lost_op
    dropped = Task.await(proxy, 15_000)
    assert dropped.request["type"] == "discard_uploads"
    assert dropped.request["op_id"] == lost_op
    assert dropped.response["state"] == "committed"
    assert dropped.response["discarded"] == 1
    refute File.exists?(pending_path)

    recovered = cli_json!(agent_bin, ["retry", "--op-id", lost_op], client_env)
    assert recovered["kind"] == "committed"
    assert recovered["op_id"] == lost_op
    assert recovered["state"] == "committed"
    assert recovered["discarded"] == 0

    zero_retry =
      cli_json!(
        agent_bin,
        [
          "discard-uploads",
          "--sender-id",
          @sender_id,
          "--message-id",
          message_id,
          "--revision",
          "1",
          "--op-id",
          op(808)
        ],
        client_env
      )

    assert zero_retry["state"] == "committed"
    assert zero_retry["discarded"] == 0
    assert File.exists?(committed_path)
  end

  defp write_identity!(identity_dir, certs, person, device_id, server, certificate \\ "a2") do
    File.mkdir_p!(identity_dir)
    File.chmod!(identity_dir, 0o700)
    File.cp!(Path.join(certs, "#{certificate}.pem"), Path.join(identity_dir, "identity.pem"))
    key_path = Path.join(identity_dir, "identity.key")
    File.cp!(Path.join(certs, "#{certificate}.key"), key_path)
    File.chmod!(key_path, 0o600)

    ca = File.read!(Path.join(certs, "ca.pem"))

    write_json!(Path.join(identity_dir, "server.json"), %{
      "address" => "127.0.0.1",
      "name" => "relay.test",
      "port" => Server.port(server),
      "origin_port" => Server.origin_port(server),
      "ca_sha256" => :crypto.hash(:sha256, ca) |> Base.encode16(case: :lower)
    })

    write_json!(Path.join(identity_dir, "enrollment.json"), %{
      "person_id" => person,
      "device_id" => device_id
    })
  end

  defp write_json!(path, value), do: File.write!(path, JSON.encode!(value) <> "\n")

  defp legacy_entry(raw, ack_pending) do
    %{
      "feed_seq" => String.to_integer(raw["event_id"]),
      "event_id" => raw["event_id"],
      "session_handle" => raw["session_handle"],
      "kind" => raw["kind"],
      "author" => raw["author"],
      "actionable" => raw["author"] != "origin" and raw["kind"] in ["discussion", "terminal"],
      "sender_id" => raw["sender_id"],
      "request_id" => raw["message_id"],
      "request_revision" => raw["request_revision"],
      "raw" => raw,
      "journaled_at_ms" => 1,
      "emitted_at_ms" => nil,
      "ack_pending_at_ms" => if(ack_pending, do: 2, else: nil),
      "delivered_at_ms" => nil,
      "acted_at_ms" => nil,
      "file_transfers" => %{}
    }
  end

  defp cli_env(identity_dir, state_dir, ca_file, origin_port) do
    [
      {"HARKLIGHT_IDENTITY_DIR", identity_dir},
      {"HARKLIGHT_CA_FILE", ca_file},
      {"HARKLIGHT_ORIGIN_PORT", Integer.to_string(origin_port)},
      {"HARKLIGHT_AGENT_STATE_DIR", state_dir}
    ]
  end

  defp start_origin_command_trace! do
    Code.ensure_loaded!(OriginWire)
    :erlang.trace_pattern({OriginWire, :command, 3}, true, [])
    :erlang.trace(:all, true, [:call, {:tracer, self()}, :set_on_spawn])
  end

  defp stop_origin_command_trace do
    :erlang.trace(:all, false, [:call])
    :erlang.trace_pattern({OriginWire, :command, 3}, false, [])
    :ok
  end

  defp await_origin_feed_call!(timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_origin_feed_call(deadline)
  end

  defp await_origin_feed_call(deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:trace, pid, :call, {OriginWire, :command, [_relay, _key, request]}} when pid != self() ->
        if request["type"] == "feed" do
          :ok
        else
          await_origin_feed_call(deadline)
        end
    after
      remaining -> flunk("Rust CLI did not request the authoritative feed")
    end
  end

  defp drain_origin_feed_calls do
    receive do
      {:trace, pid, :call, {OriginWire, :command, [_relay, _key, request]}} when pid != self() ->
        drain_origin_feed_calls() + if(request["type"] == "feed", do: 1, else: 0)
    after
      0 -> 0
    end
  end

  defp await_origin_command_call!(type, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_origin_command_call(type, deadline)
  end

  defp await_origin_command_call(type, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:trace, pid, :call, {OriginWire, :command, [_relay, _key, request]}} when pid != self() ->
        if request["type"] == type do
          request
        else
          await_origin_command_call(type, deadline)
        end
    after
      remaining -> flunk("Rust CLI did not submit the expected #{type} operation")
    end
  end

  defp drain_origin_command_calls(type) do
    receive do
      {:trace, pid, :call, {OriginWire, :command, [_relay, _key, request]}} when pid != self() ->
        drain_origin_command_calls(type) + if(request["type"] == type, do: 1, else: 0)
    after
      0 -> 0
    end
  end

  defp start_ssl_send_trace! do
    Code.ensure_loaded!(:ssl)
    :erlang.trace_pattern({:ssl, :send, 2}, true, [])
    :erlang.trace(:all, true, [:call, {:tracer, self()}, :set_on_spawn])
  end

  defp stop_ssl_send_trace do
    :erlang.trace(:all, false, [:call])
    :erlang.trace_pattern({:ssl, :send, 2}, false, [])
    :ok
  end

  defp await_change_notice_socket!(timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_change_notice_socket(deadline)
  end

  defp await_change_notice_socket(deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:trace, pid, :call, {:ssl, :send, [socket, data]}} when pid != self() ->
        case safe_iodata(data) do
          <<"event: change\n", _::binary>> -> socket
          _ -> await_change_notice_socket(deadline)
        end
    after
      remaining -> flunk("did not observe the server's initial SSE change notice")
    end
  end

  defp safe_iodata(data) do
    IO.iodata_to_binary(data)
  rescue
    _ -> <<>>
  end

  defp exec_sql!(db, sql) do
    assert {:ok, statement} = Sqlite3.prepare(db, sql)
    assert :done = Sqlite3.step(db, statement)
    assert :ok = Sqlite3.release(db, statement)
  end

  defp agent_journal_database!(fixture, _session) do
    identity_path = Path.join([fixture.root, "identity", "enrollment.json"])
    {:ok, %{"device_id" => device_id}} = JSON.decode_line(File.read!(identity_path))
    device_scope = :crypto.hash(:sha256, device_id) |> Base.encode16(case: :lower)
    Path.join([fixture.root, "agent-state", device_scope, "origin-feed.sqlite3"])
  end

  defp start_drop_response_proxy!(fixture) do
    options = [
      ip: {127, 0, 0, 1},
      active: false,
      mode: :binary,
      packet: :raw,
      reuseaddr: true,
      versions: [:"tlsv1.3"],
      certfile: String.to_charlist(Path.join(fixture.certs, "server.pem")),
      keyfile: String.to_charlist(Path.join(fixture.certs, "server.key")),
      cacertfile: String.to_charlist(Path.join(fixture.certs, "ca.pem")),
      verify: :verify_peer,
      verify_fun: EnrolledLeafVerify.device(fixture.relay),
      fail_if_no_peer_cert: true,
      session_tickets: :disabled,
      early_data: :disabled
    ]

    assert {:ok, listener} = :ssl.listen(0, options)
    assert {:ok, {_ip, port}} = :ssl.sockname(listener)

    task =
      Task.async(fn ->
        assert {:ok, transport} = :ssl.transport_accept(listener, 10_000)
        assert {:ok, socket} = :ssl.handshake(transport, 10_000)

        try do
          assert {:ok, certificate} = :ssl.peercert(socket)
          assert {:ok, device_key} = DeviceIdentity.from_certificate(certificate)
          {path, request} = read_http_json_request!(socket)
          response = OriginWire.command(fixture.relay, device_key, request)
          %{path: path, request: request, response: response}
        after
          :ssl.close(socket)
        end
      end)

    on_exit(fn ->
      if Process.alive?(task.pid), do: Process.exit(task.pid, :kill)
      :ssl.close(listener)
    end)

    {port, task}
  end

  defp start_refusal_proxy!(fixture, response) do
    options = [
      ip: {127, 0, 0, 1},
      active: false,
      mode: :binary,
      packet: :raw,
      reuseaddr: true,
      versions: [:"tlsv1.3"],
      certfile: String.to_charlist(Path.join(fixture.certs, "server.pem")),
      keyfile: String.to_charlist(Path.join(fixture.certs, "server.key")),
      cacertfile: String.to_charlist(Path.join(fixture.certs, "ca.pem")),
      verify: :verify_peer,
      verify_fun: EnrolledLeafVerify.device(fixture.relay),
      fail_if_no_peer_cert: true,
      session_tickets: :disabled,
      early_data: :disabled
    ]

    assert {:ok, listener} = :ssl.listen(0, options)
    assert {:ok, {_ip, port}} = :ssl.sockname(listener)

    task =
      Task.async(fn ->
        assert {:ok, transport} = :ssl.transport_accept(listener, 10_000)
        assert {:ok, socket} = :ssl.handshake(transport, 10_000)

        try do
          assert {:ok, certificate} = :ssl.peercert(socket)
          assert {:ok, _device_key} = DeviceIdentity.from_certificate(certificate)
          {path, request} = read_http_json_request!(socket)
          body = JSON.encode!(response)

          :ok =
            :ssl.send(socket, [
              "HTTP/1.1 200 OK\r\n",
              "Content-Type: application/json\r\n",
              "Content-Length: #{byte_size(body)}\r\n",
              "Connection: close\r\n\r\n",
              body
            ])

          %{path: path, request: request}
        after
          :ssl.close(socket)
        end
      end)

    on_exit(fn ->
      if Process.alive?(task.pid), do: Process.exit(task.pid, :kill)
      :ssl.close(listener)
    end)

    {port, task}
  end

  defp start_feed_barrier_proxy!(fixture, owner) do
    options = [
      ip: {127, 0, 0, 1},
      active: false,
      mode: :binary,
      packet: :raw,
      reuseaddr: true,
      versions: [:"tlsv1.3"],
      certfile: String.to_charlist(Path.join(fixture.certs, "server.pem")),
      keyfile: String.to_charlist(Path.join(fixture.certs, "server.key")),
      cacertfile: String.to_charlist(Path.join(fixture.certs, "ca.pem")),
      verify: :verify_peer,
      verify_fun: EnrolledLeafVerify.device(fixture.relay),
      fail_if_no_peer_cert: true,
      session_tickets: :disabled,
      early_data: :disabled
    ]

    assert {:ok, listener} = :ssl.listen(0, options)
    assert {:ok, {_ip, port}} = :ssl.sockname(listener)
    assert {:ok, supervisor} = Task.Supervisor.start_link()

    acceptor =
      Task.async(fn -> feed_barrier_accept_loop(listener, supervisor, fixture, owner) end)

    on_exit(fn ->
      if Process.alive?(acceptor.pid), do: Process.exit(acceptor.pid, :kill)
      :ssl.close(listener)
      if Process.alive?(supervisor), do: Supervisor.stop(supervisor, :normal)
    end)

    {port, acceptor}
  end

  defp feed_barrier_accept_loop(listener, supervisor, fixture, owner) do
    case :ssl.transport_accept(listener, 10_000) do
      {:ok, transport} ->
        case :ssl.handshake(transport, 10_000) do
          {:ok, socket} ->
            {:ok, worker} =
              Task.Supervisor.start_child(supervisor, fn ->
                receive do
                  {:barrier_socket, accepted} -> feed_barrier_serve(accepted, fixture, owner)
                end
              end)

            case :ssl.controlling_process(socket, worker) do
              :ok -> send(worker, {:barrier_socket, socket})
              _ -> :ssl.close(socket)
            end

          _ ->
            :ssl.close(transport)
        end

        feed_barrier_accept_loop(listener, supervisor, fixture, owner)

      {:error, :timeout} ->
        feed_barrier_accept_loop(listener, supervisor, fixture, owner)

      {:error, :closed} ->
        :ok

      {:error, _} ->
        Process.sleep(50)
        feed_barrier_accept_loop(listener, supervisor, fixture, owner)
    end
  end

  defp feed_barrier_serve(socket, fixture, owner) do
    try do
      assert {:ok, certificate} = :ssl.peercert(socket)
      assert {:ok, device_key} = DeviceIdentity.from_certificate(certificate)

      case read_barrier_request!(socket) do
        {:post, "/origin/v1", request} ->
          if request["type"] == "feed" do
            feed_ref = make_ref()
            send(owner, {:startup_feed_waiting, self(), feed_ref, request})

            receive do
              {:release_startup_feed, ^feed_ref} -> :ok
            after
              15_000 -> flunk("startup feed barrier was not released")
            end
          end

          response = OriginWire.command(fixture.relay, device_key, request)
          :ok = :ssl.send(socket, OriginWire.http_response(200, response))

        {:stream, "/origin/v1/stream?session=" <> session_text} ->
          session = session_text
          :ok = Relay.origin_stream_attach(fixture.relay, device_key, session, self())

          try do
            assert {:ok, %{"cursor" => cursor}} =
                     Relay.origin_status(fixture.relay, device_key, session)

            :ok = :ssl.send(socket, barrier_stream_headers())
            :ok = :ssl.send(socket, barrier_change_notice(cursor))
            send(owner, {:startup_stream_open, self(), session})
            feed_barrier_stream_loop(socket, cursor)
          after
            :ok = Relay.origin_stream_detach(fixture.relay, device_key, session, self())
          end

        other ->
          flunk("unexpected request to startup barrier proxy: #{inspect(other)}")
      end
    after
      :ssl.close(socket)
    end
  end

  defp read_barrier_request!(socket, buffer \\ <<>>) do
    case :binary.match(buffer, "\r\n\r\n") do
      {header_length, 4} ->
        header = binary_part(buffer, 0, header_length)
        body_offset = header_length + 4
        body = binary_part(buffer, body_offset, byte_size(buffer) - body_offset)
        [method, path, _version] = header |> String.split("\r\n") |> hd() |> String.split(" ")

        case method do
          "GET" ->
            {:stream, path}

          "POST" ->
            content_length =
              header
              |> String.split("\r\n")
              |> Enum.drop(1)
              |> Enum.find_value(fn line ->
                case String.split(line, ":", parts: 2) do
                  [name, value] ->
                    if String.downcase(name) == "content-length" do
                      String.trim(value) |> String.to_integer()
                    end

                  _ ->
                    nil
                end
              end)

            if is_integer(content_length) and byte_size(body) >= content_length do
              payload = binary_part(body, 0, content_length)
              {:ok, request} = JSON.decode_line(payload <> "\n")
              {:post, path, request}
            else
              receive_barrier_body!(socket, buffer)
            end

          _ ->
            flunk("unexpected HTTP method #{inspect(method)}")
        end

      :nomatch ->
        receive_barrier_body!(socket, buffer)
    end
  end

  defp receive_barrier_body!(socket, buffer) do
    assert {:ok, next} = :ssl.recv(socket, 0, 10_000)
    read_barrier_request!(socket, buffer <> next)
  end

  defp barrier_stream_headers do
    [
      "HTTP/1.1 200 OK\r\n",
      "Content-Type: text/event-stream; charset=utf-8\r\n",
      "Cache-Control: no-store\r\n",
      "Connection: close\r\n\r\n"
    ]
  end

  defp barrier_change_notice(cursor),
    do: ["event: change\n", "data: ", JSON.encode!(%{"cursor" => cursor}), "\n\n"]

  defp feed_barrier_stream_loop(socket, cursor) do
    :ok = :ssl.setopts(socket, active: :once)

    receive do
      {:origin_state_changed, next} when is_integer(next) and next > cursor ->
        case :ssl.send(socket, barrier_change_notice(next)) do
          :ok -> feed_barrier_stream_loop(socket, next)
          _ -> :ok
        end

      {:ssl, ^socket, _bytes} ->
        :ok

      {:ssl_closed, ^socket} ->
        :ok

      {:ssl_error, ^socket, _reason} ->
        :ok
    after
      15_000 ->
        case :ssl.send(socket, ": keepalive\n\n") do
          :ok -> feed_barrier_stream_loop(socket, cursor)
          _ -> :ok
        end
    end
  end

  defp read_http_json_request!(socket, buffer \\ <<>>) do
    case :binary.match(buffer, "\r\n\r\n") do
      {header_length, 4} ->
        header = binary_part(buffer, 0, header_length)
        body_offset = header_length + 4
        body = binary_part(buffer, body_offset, byte_size(buffer) - body_offset)
        [method, path, _version] = header |> String.split("\r\n") |> hd() |> String.split(" ")

        content_length =
          header
          |> String.split("\r\n")
          |> Enum.drop(1)
          |> Enum.find_value(fn line ->
            case String.split(line, ":", parts: 2) do
              [name, value] ->
                if String.downcase(name) == "content-length" do
                  String.trim(value) |> String.to_integer()
                end

              _ ->
                nil
            end
          end)

        if is_integer(content_length) and byte_size(body) >= content_length do
          assert method == "POST"
          payload = binary_part(body, 0, content_length)
          {:ok, request} = JSON.decode_line(payload <> "\n")
          {path, request}
        else
          receive_http_body!(socket, buffer)
        end

      :nomatch ->
        receive_http_body!(socket, buffer)
    end
  end

  defp receive_http_body!(socket, buffer) do
    assert {:ok, next} = :ssl.recv(socket, 0, 10_000)
    read_http_json_request!(socket, buffer <> next)
  end

  defp put_env(env, name, value),
    do: [{name, value} | Enum.reject(env, fn {key, _} -> key == name end)]

  defp submit_answer!(relay, reader, request_id, operation_number, answer) do
    frame = %{
      "v" => 1,
      "type" => "answer",
      "op_id" => op(operation_number),
      "sender_id" => @sender_id,
      "message_id" => request_id,
      "request_revision" => 1,
      "answer" => answer
    }

    assert {:ok, line} = Relay.request_line(relay, reader, JSON.encode!(frame) <> "\n")
    assert {:ok, %{"type" => "ok"} = response} = JSON.decode_line(line)
    response
  end

  defp submit_no_comment!(relay, reader, request_id, request_revision) do
    frame = %{
      "type" => "answer_no_comment",
      "request_id" => request_id,
      "request_revision" => request_revision
    }

    assert {:ok, line} = Relay.request_line(relay, reader, JSON.encode!(frame) <> "\n")

    assert {:ok, %{"type" => "ok", "terminal_kind" => "no_comment"} = response} =
             JSON.decode_line(line)

    response
  end

  defp start_file_fixture!(root, upload_cap) do
    File.mkdir_p!(root)
    certs = TLSFixtures.create!(server_san: "DNS:relay.test")

    on_exit(fn ->
      File.rm_rf!(root)
      File.rm_rf!(certs)
    end)

    server_opts = [
      store_path: Path.join(root, "state.sqlite3"),
      certfile: Path.join(certs, "server.pem"),
      keyfile: Path.join(certs, "server.key"),
      cacertfile: Path.join(certs, "ca.pem"),
      port: 0,
      origin_port: 0,
      relay_name: String.to_atom("agent_cli_file_#{System.unique_integer([:positive])}")
    ]

    server_opts =
      if upload_cap == :none,
        do: server_opts,
        else: Keyword.put(server_opts, :upload_cap, upload_cap)

    {:ok, server} = Server.start_link(server_opts)
    Process.unlink(server)
    on_exit(fn -> if Process.alive?(server), do: Supervisor.stop(server) end)

    relay = Server.relay(server)
    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))

    {:ok, origin} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, reader} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "b1"))

    :ok = Relay.setup_sender(relay, origin, @sender_id)

    identity_dir = Path.join(root, "identity")
    state_dir = Path.join(root, "agent-state")
    write_identity!(identity_dir, certs, person, origin, server)

    %{
      root: root,
      certs: certs,
      server: server,
      relay: relay,
      person: person,
      origin: origin,
      reader: reader,
      store_path: Path.join(root, "state.sqlite3"),
      endpoint: %{
        address: {127, 0, 0, 1},
        name: "relay.test",
        ca_file: Path.join(certs, "ca.pem"),
        port: Server.origin_port(server)
      },
      env:
        cli_env(
          identity_dir,
          state_dir,
          Path.join(certs, "ca.pem"),
          Server.origin_port(server)
        )
    }
  end

  defp write_file!(root, name, bytes) do
    path = Path.join(root, name)
    File.write!(path, bytes)
    path
  end

  defp origin_entry!(relay, origin, message_id) do
    {:ok, %{"entries" => entries}} = Relay.origin_feed(relay, origin, 0)
    Enum.find(entries, &(&1["message_id"] == message_id)) || flunk("origin entry not found")
  end

  defp fetch_uploaded_bytes!(fixture, upload_id) do
    assert {:ok, socket} =
             HeadlessTLS.connect(
               fixture.endpoint,
               {Path.join(fixture.certs, "a2.pem"), Path.join(fixture.certs, "a2.key")}
             )

    try do
      :ok =
        :ssl.send(
          socket,
          "GET /origin/v1/files/#{upload_id} HTTP/1.1\r\nHost: relay.test\r\n\r\n"
        )

      assert {:ok, response} = read_all(socket, <<>>)
      [head, body] = :binary.split(response, "\r\n\r\n")
      ["HTTP/1.1", "200" | _] = head |> String.split("\r\n") |> hd() |> String.split(" ")
      body
    after
      :ssl.close(socket)
    end
  end

  defp origin_post!(fixture, request) do
    body = JSON.encode!(request)

    assert {:ok, socket} =
             HeadlessTLS.connect(
               fixture.endpoint,
               {Path.join(fixture.certs, "a2.pem"), Path.join(fixture.certs, "a2.key")}
             )

    try do
      :ok =
        :ssl.send(
          socket,
          [
            "POST /origin/v1 HTTP/1.1\r\n",
            "Host: relay.test\r\n",
            "Content-Type: application/json\r\n",
            "Content-Length: #{byte_size(body)}\r\n\r\n",
            body
          ]
        )

      assert {:ok, response} = read_all(socket, <<>>)
      [head, response_body] = :binary.split(response, "\r\n\r\n")
      ["HTTP/1.1", "200" | _] = head |> String.split("\r\n") |> hd() |> String.split(" ")
      {:ok, decoded} = JSON.decode_line(response_body <> "\n")
      decoded
    after
      :ssl.close(socket)
    end
  end

  defp upload_person_file!(fixture, op_id, message_id, name, bytes, device \\ "b1") do
    encoded_name = Base.url_encode64(name, padding: false)

    request = [
      "POST /origin/v1/upload HTTP/1.1\r\n",
      "Host: relay.test\r\n",
      "Content-Type: application/octet-stream\r\n",
      "Content-Length: #{byte_size(bytes)}\r\n",
      "X-Harklight-Op-Id: #{op_id}\r\n",
      "X-Harklight-Sender-Id: #{@sender_id}\r\n",
      "X-Harklight-Message-Id: #{message_id}\r\n",
      "X-Harklight-Request-Revision: 1\r\n",
      "X-Harklight-File-Name: #{encoded_name}\r\n\r\n",
      bytes
    ]

    assert {:ok, socket} =
             HeadlessTLS.connect(
               fixture.endpoint,
               {Path.join(fixture.certs, "#{device}.pem"),
                Path.join(fixture.certs, "#{device}.key")}
             )

    try do
      :ok = :ssl.send(socket, request)
      assert {:ok, response} = read_all(socket, <<>>)
      [head, body] = :binary.split(response, "\r\n\r\n")
      ["HTTP/1.1", "200" | _] = head |> String.split("\r\n") |> hd() |> String.split(" ")
      {:ok, upload} = JSON.decode_line(body <> "\n")
      assert upload["type"] == "ok"
      upload
    after
      :ssl.close(socket)
    end
  end

  defp upload_activity!(store_path, upload_id) do
    assert {:ok, store} = Store.open(store_path)
    activity = store.engine.uploads[upload_id].last_activity_ms
    assert is_integer(activity)
    assert :ok = Store.close(store)
    activity
  end

  defp upload_ref(upload, name, bytes) do
    %{
      "name" => name,
      "upload_id" => upload["upload_id"],
      "size" => byte_size(bytes),
      "sha256" => sha256(bytes)
    }
  end

  defp submit_discussion_with_files!(relay, reader, request_id, operation, text, files) do
    frame = %{
      "v" => 1,
      "type" => "discussion",
      "op_id" => operation,
      "sender_id" => @sender_id,
      "message_id" => request_id,
      "request_revision" => 1,
      "text" => text,
      "files" => files
    }

    assert {:ok, line} = Relay.request_line(relay, reader, JSON.encode!(frame) <> "\n")

    assert {:ok, %{"type" => "ok", "event_id" => _event_id} = response} =
             JSON.decode_line(line)

    response
  end

  defp cli_next!(agent_bin, session, env) do
    cli_json!(
      agent_bin,
      [
        "listen",
        "--session",
        session,
        "--session-context",
        "interactive",
        "--claude-code-version",
        "2.1.234",
        "--background-tasks-enabled",
        "--next"
      ],
      env
    )
  end

  defp sha256(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

  defp read_all(socket, buffer) do
    case :ssl.recv(socket, 0, 5_000) do
      {:ok, bytes} -> read_all(socket, buffer <> bytes)
      {:error, :closed} -> {:ok, buffer}
      error -> error
    end
  end

  defp cli_json!(agent_bin, args, env) do
    {output, 0} = System.cmd(agent_bin, args, env: env, stderr_to_stdout: true)
    decode_first_line!(output)
  end

  defp decode_first_line!(output) do
    line = output |> String.split("\n", trim: true) |> List.first()
    assert is_binary(line), "expected JSON output, got: #{inspect(output)}"
    {:ok, decoded} = JSON.decode_line(line <> "\n")
    decoded
  end

  defp launch_listener(agent_bin, session, mode, env, controller? \\ false) do
    args = [
      "listen",
      "--session",
      session,
      "--session-context",
      "interactive",
      "--claude-code-version",
      "2.1.234",
      "--background-tasks-enabled",
      "--#{mode}"
    ]

    args = if controller?, do: args ++ ["--controller"], else: args

    port_env =
      Enum.map(env, fn {name, value} ->
        {String.to_charlist(name), String.to_charlist(value)}
      end)

    port =
      Port.open({:spawn_executable, String.to_charlist(agent_bin)}, [
        :binary,
        :exit_status,
        :use_stdio,
        {:line, 1_048_576},
        {:args, Enum.map(args, &String.to_charlist/1)},
        {:env, port_env}
      ])

    on_exit(fn -> terminate_if_running(port) end)
    port
  end

  defp receive_event!(port) do
    receive do
      {^port, {:data, {:eol, line}}} ->
        {:ok, %{"kind" => "event"} = event} = JSON.decode_line(line <> "\n")
        event

      {^port, {:data, {:noeol, fragment}}} ->
        flunk("CLI emitted an unterminated event fragment: #{inspect(fragment)}")

      {^port, {:exit_status, status}} ->
        flunk("CLI listener exited before an event with status #{status}")
    after
      15_000 -> flunk("CLI did not emit the expected event")
    end
  end

  defp await_exit(port, timeout) do
    receive do
      {^port, {:exit_status, status}} ->
        {:ok, status}

      {^port, {:data, _data}} ->
        await_exit(port, timeout)
    after
      timeout -> {:error, :timeout}
    end
  end

  defp terminate_listener!(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} ->
        kill = System.find_executable("kill") || "/bin/kill"
        {_, 0} = System.cmd(kill, ["-KILL", Integer.to_string(pid)])
        assert {:ok, status} = await_exit(port, 10_000)
        assert status != 0

      nil ->
        flunk("CLI listener exited before the interruption boundary")
    end
  end

  defp terminate_if_running(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} ->
        kill = System.find_executable("kill") || "/bin/kill"
        System.cmd(kill, ["-KILL", Integer.to_string(pid)])

      nil ->
        :ok
    end
  end

  defp submit_discussion!(relay, reader, request_id, op_number, text) do
    frame = %{
      "v" => 1,
      "type" => "discussion",
      "op_id" => op(op_number),
      "sender_id" => @sender_id,
      "message_id" => request_id,
      "request_revision" => 1,
      "text" => text
    }

    assert {:ok, line} = Relay.request_line(relay, reader, JSON.encode!(frame) <> "\n")

    assert {:ok, %{"type" => "ok", "event_id" => _event_id} = response} =
             JSON.decode_line(line)

    response
  end

  defp wait_availability!(relay, origin, session, expected) do
    deadline = System.monotonic_time(:millisecond) + 10_000
    wait_availability(relay, origin, session, expected, deadline)
  end

  defp wait_availability(relay, origin, session, expected, deadline) do
    case Relay.origin_status(relay, origin, session) do
      {:ok, %{"availability" => ^expected} = status} ->
        status

      _ ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("origin availability did not become #{expected}")
        else
          Process.sleep(20)
          wait_availability(relay, origin, session, expected, deadline)
        end
    end
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"
end
