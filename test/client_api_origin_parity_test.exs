defmodule AttentionServer.ClientAPIOriginParityTest do
  # Origin-side behaviors the retired /origin/v1 listener tests asserted,
  # checked on client API v2 over the supervised device listener.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AttentionServer.{
    AgentChangeLog,
    ChangeLog,
    ClientAPIFeedClient,
    DeviceIdentity,
    JSON,
    Relay,
    Server,
    TLSFixtures
  }

  @agent_id "00000000-0000-4000-8000-0000000000a1"
  @origin "parity-origin"
  @session "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @ended_session "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

  setup_all do
    certs = TLSFixtures.create!()
    TLSFixtures.make_self_signed_client!(certs, "parity-owner")
    TLSFixtures.make_self_signed_client!(certs, "parity-agent")
    TLSFixtures.make_self_signed_client!(certs, @origin)
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "origin-parity-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    server_opts = [
      store_path: Path.join(dir, "relay.sqlite3"),
      certfile: Path.join(certs, "server.pem"),
      keyfile: Path.join(certs, "server.key"),
      cacertfile: Path.join(certs, "ca.pem"),
      port: 0,
      enrollment_enabled: false,
      ip: {127, 0, 0, 1}
    ]

    server = start_supervised!({Server, server_opts})

    relay = Server.relay(server)
    owner_der = TLSFixtures.certificate_der!(certs, "parity-owner")
    assert {:ok, person} = Relay.operator_bootstrap(relay, owner_der)
    assert {:ok, owner} = DeviceIdentity.from_certificate(owner_der)
    assert :ok = Relay.setup_person(relay, "B")

    keys =
      Map.new([{@origin, person}, {"a2", person}, {"b1", "B"}], fn {name, account} ->
        der = TLSFixtures.certificate_der!(certs, name)
        assert {:ok, key} = Relay.setup_device_certificate(relay, account, der)
        {name, key}
      end)

    assert :ok = Relay.setup_sender(relay, keys[@origin], "app:test")
    assert :ok = Relay.setup_sender(relay, keys["b1"], "app:test")

    %{
      server_opts: server_opts,
      relay: relay,
      person: person,
      owner: owner,
      keys: keys,
      port: Server.port(server),
      certs: certs
    }
  end

  test "a send naming a sender the key is not granted is forbidden and stores nothing", ctx do
    refused = send_message(ctx, @origin, 1, %{}, "app:ungranted")
    assert_error(refused, 403, "forbidden")
    assert messages(ctx) == []

    # A sender granted to another account's key does not authorize this one.
    assert :ok = Relay.setup_sender(ctx.relay, ctx.keys[@origin], "app:a-only")
    assert_error(send_message(ctx, "b1", 2, %{}, "app:a-only"), 403, "forbidden")

    sent = send_message(ctx, @origin, 3)
    assert sent.status == 200
    assert [{_person, "app:test", _id}] = messages(ctx)
  end

  test "an attached upload is readable within its account and not_found to another person",
       ctx do
    data = "upload-tier attachment bytes"
    upload = upload(ctx, @origin, 1, data)
    assert upload.status == 201
    handle = decode(upload)["file_id"]

    sent = send_message(ctx, @origin, 2, %{"file_ids" => [handle]})
    assert sent.status == 200, sent.body
    assert %{"message" => %{"files" => [%{"file_id" => download}]}} = decode(sent)
    path = "/v2/files/" <> download

    for name <- [@origin, "a2"] do
      file = get(ctx, name, path)
      assert file.status == 200
      assert file.body == data
    end

    assert_error(get(ctx, "b1", path), 404, "not_found")
    assert_error(get(ctx, "b1", "/v2/files/" <> handle), 404, "not_found")
  end

  test "the origin reads the exact person answer, choice and no comment, and an agent answer",
       ctx do
    actions = %{
      "schema" => "attention-client.actions/1",
      "actions" => [
        %{"id" => "approve", "label" => "Approve", "kind" => "value", "value" => "approved"}
      ]
    }

    text = "Exact person answer: \"quoted\" ✓"
    comment = item_id(send_message(ctx, @origin, 1))
    choice = item_id(send_message(ctx, @origin, 2, %{}, "app:test", %{"answer_ui" => actions}))
    no_comment = item_id(send_message(ctx, @origin, 3))
    by_agent = item_id(send_message(ctx, @origin, 4))

    for {id, body, n} <- [
          {comment, %{"comment" => text}, 5},
          {choice, %{"choice" => "approve"}, 6},
          {no_comment, %{}, 7}
        ] do
      answer =
        request(ctx, "a2", "POST", answer_path(id), Map.put(body, "request_revision", 1), n)

      assert answer.status == 200
    end

    register_agent(ctx)
    agent_body = %{"request_revision" => 1, "comment" => "Agent answer"}
    answered = request(ctx, "parity-agent", "POST", answer_path(by_agent), agent_body, 8)
    assert answered.status == 200

    assert %{
             "state" => "answered",
             "answer" => ^text,
             "outcome" => %{"kind" => "reply", "text" => ^text}
           } = person_terminal = origin_terminal(ctx, comment)

    assert %{
             "state" => "answered",
             "answer" => "approved",
             "outcome" => %{"kind" => "choice", "id" => "approve", "label" => "Approve"}
           } = origin_terminal(ctx, choice)

    assert %{"state" => "answered", "outcome" => %{"kind" => "no_comment"}} =
             no_comment_terminal = origin_terminal(ctx, no_comment)

    refute Map.has_key?(no_comment_terminal, "answer")

    # The agent's answer reaches the origin as its text. Only a person's answer
    # carries the origin's delivery facts.
    assert %{
             "state" => "answered",
             "answer" => "Agent answer",
             "outcome" => %{"kind" => "reply", "text" => "Agent answer"}
           } = agent_terminal = origin_terminal(ctx, by_agent)

    assert Map.has_key?(person_terminal, "delivery")
    refute Map.has_key?(agent_terminal, "delivery")
  end

  test "pending baselines serve non-person answers once without publishing delivery facts", ctx do
    register_agent(ctx)

    actions = %{
      "schema" => "attention-client.actions/1",
      "actions" => [
        %{"id" => "approve", "label" => "Approve", "kind" => "value", "value" => "approved"},
        %{"id" => "seen", "label" => "Seen", "kind" => "value",
          "value" => %{"acknowledgment" => "seen"}}
      ]
    }

    expected =
      for {n, answer, kind} <- [
            {10, %{"choice" => "approve"}, "choice"},
            {11, %{"comment" => "Agent words"}, "reply"},
            {12, %{}, "no_comment"},
            {13, %{"choice" => "seen"}, "seen"}
          ], into: %{} do
        id = item_id(send_message(ctx, @origin, n, %{}, "app:test", %{"answer_ui" => actions}))
        body = Map.put(answer, "request_revision", 1)
        assert request(ctx, "parity-agent", "POST", answer_path(id), body, n + 20).status == 200
        {id, kind}
      end

    default_at = System.system_time(:millisecond) + 2_000

    default = item_id(send_message(ctx, @origin, 14, %{}, "app:test", %{
      "answer_ui" => actions,
      "default" => %{"value" => "approved", "at" => default_at}
    }))

    default_terminal = await_default_answer(ctx, default, 250)
    assert %{"state" => "answered", "outcome" => %{"kind" => "default"}} = default_terminal

    outside = item_id(send_message(ctx, @origin, 15, %{"session" => @ended_session}))
    assert request(ctx, "parity-agent", "POST", answer_path(outside),
             %{"request_revision" => 1}, 35).status == 200

    retracted = item_id(send_message(ctx, @origin, 16))
    assert request(ctx, @origin, "POST", "/v2/agent/messages/#{retracted}/retract", %{}, 36).status == 200

    ids = [default | Map.keys(expected)]

    for {id, n} <- Enum.with_index(ids, 50) do
      assert request(ctx, "a2", "POST", "/v2/items/#{id}/seen", %{}, n).status == 200
    end

    before = Map.new(ids, &{&1, origin_terminal(ctx, &1)})
    person_answer = fn ctx, id ->
      get(ctx, "a2", "/v2/items/#{id}")
      |> decode()
      |> Map.fetch!("item")
      |> Map.take(["terminal", "seen"])
    end

    person_before = Map.new(ids, &{&1, person_answer.(ctx, &1)})
    path = "/v2/agent/delivery-baseline?session=#{@session}"

    assert %{"rev" => rev, "next_page" => token, "answers" => "all"} = decode(get(ctx, @origin, path))
    page_path = path <> "&page=" <> token
    assert %{"rev" => ^rev, "changes" => changes} = page = decode(get(ctx, @origin, page_path))

    assert MapSet.new(Enum.map(changes, &get_in(&1, ["data", "item_id"]))) == MapSet.new(ids)
    assert length(changes) == length(ids)
    for %{"kind" => "message", "data" => %{"item_id" => id, "terminal" => terminal}} <- changes do
      assert terminal == before[id]
      assert %{"state" => "answered", "at_ms" => at_ms} = terminal
      assert is_integer(at_ms) and at_ms > 0
      refute Map.has_key?(terminal, "delivery")
      if kind = expected[id], do: assert(terminal["outcome"]["kind"] == kind)
    end

    # The consumer identifies a replay by item ID, terminal state and at_ms.
    assert decode(get(ctx, @origin, page_path)) == page

    for _ <- 1..3 do
      assert %{"rev" => ^rev, "next_page" => fresh} = decode(get(ctx, @origin, path))
      assert %{"changes" => []} = decode(get(ctx, @origin, path <> "&page=" <> fresh))
    end

    assert :ok = stop_supervised(Server)
    restarted = start_supervised!({Server, ctx.server_opts})
    ctx = %{ctx | relay: Server.relay(restarted), port: Server.port(restarted)}
    assert decode(get(ctx, @origin, page_path)) == page

    for id <- ids do
      assert origin_terminal(ctx, id) == before[id]
      assert person_answer.(ctx, id) == person_before[id]
    end

    for {id, n} <- Enum.with_index(Map.keys(expected), 40) do
      path = "/v2/agent/messages/#{id}"
      assert_error(request(ctx, @origin, "POST", path <> "/receipt", %{"outcome" => "handed"}, n * 2),
        400, "unsupported")
      assert_error(request(ctx, @origin, "POST", path <> "/result", %{"status" => "received"}, n * 2 + 1),
        400, "unsupported")
    end

    outside_path = "/v2/agent/delivery-baseline?session=#{@ended_session}"
    assert %{"next_page" => outside_token} = decode(get(ctx, @origin, outside_path))
    assert %{"changes" => [%{"data" => %{"item_id" => ^outside}}]} =
             decode(get(ctx, @origin, outside_path <> "&page=" <> outside_token))
  end

  test "a sender's answered lifecycle close-out is excluded from pending baselines", ctx do
    item = item_id(send_message(ctx, @origin, 10))

    assert request(
             ctx,
             @origin,
             "POST",
             "/v2/agent/messages/#{item}/lifecycle",
             %{"action" => "answered"},
             11
           ).status == 200

    assert %{"state" => "answered"} = terminal = origin_terminal(ctx, item)
    refute Map.has_key?(terminal, "outcome")

    baseline = "/v2/agent/delivery-baseline?session=#{@session}"
    assert %{"next_page" => token} = decode(get(ctx, @origin, baseline))
    assert %{"changes" => []} = decode(get(ctx, @origin, baseline <> "&page=" <> token))
  end

  test "an unconfirmed stream leaves a granted-agent answer pending for a fresh baseline",
       ctx do
    register_agent(ctx)
    item = item_id(send_message(ctx, @origin, 10))
    {conn, feed} = open_agent_feed(ctx, agent_head(ctx), @session)

    assert request(
             ctx,
             "parity-agent",
             "POST",
             answer_path(item),
             %{"request_revision" => 1, "comment" => "Agent words"},
             11
           ).status == 200

    {conn, _feed, answer} = next_answer(conn, feed)
    assert %{event: "message", data: %{"item_id" => ^item}} = answer
    Mint.HTTP.close(conn)

    baseline = "/v2/agent/delivery-baseline?session=#{@session}"
    assert %{"next_page" => token} = decode(get(ctx, @origin, baseline))

    assert %{"changes" => [%{"kind" => "message", "data" => %{"item_id" => ^item}}]} =
             decode(get(ctx, @origin, baseline <> "&page=" <> token))
  end

  test "a JSON feed batch excludes granted-agent and default answers from a fresh baseline",
       ctx do
    register_agent(ctx)
    agent_answer = item_id(send_message(ctx, @origin, 10))

    assert request(ctx, "parity-agent", "POST", answer_path(agent_answer),
             %{"request_revision" => 1, "comment" => "Agent words"}, 11).status == 200

    default =
      item_id(send_message(ctx, @origin, 12, %{}, "app:test", %{
        "answer_ui" => %{
          "schema" => "attention-client.actions/1",
          "actions" => [
            %{"id" => "approve", "label" => "Approve", "kind" => "value", "value" => "approved"}
          ]
        },
        "default" => %{"value" => "approved", "at" => System.system_time(:millisecond) + 2_000}
      }))

    assert %{"state" => "answered"} = await_default_answer(ctx, default, 250)
    feed = "/v2/agent/changes?session=#{@session}&after=0"

    assert %{"rev" => rev, "changes" => changes} = decode(get(ctx, @origin, feed))

    received =
      for %{"kind" => "message", "data" => %{"item_id" => id, "terminal" => terminal}} <- changes,
          terminal["state"] == "answered" do
        refute Map.has_key?(terminal, "delivery")
        id
      end

    assert Enum.sort(received) == Enum.sort([agent_answer, default])
    baseline = "/v2/agent/delivery-baseline?session=#{@session}"
    assert %{"rev" => ^rev, "next_page" => token} = decode(get(ctx, @origin, baseline))
    assert %{"rev" => ^rev, "changes" => []} =
             decode(get(ctx, @origin, baseline <> "&page=" <> token))
  end

  test "simultaneous agent feeds of one key each receive a change and close independently",
       ctx do
    id = item_id(send_message(ctx, @origin, 1))
    head = agent_head(ctx)
    {first, first_feed} = open_agent_feed(ctx, head)
    {second, second_feed} = open_agent_feed(ctx, head)

    assert person_reply(ctx, id, 2).status == 200
    {first, _first_feed, change} = next_change(first, first_feed)
    {second, second_feed, same} = next_change(second, second_feed)
    assert change.id > head
    assert same.data["entry_id"] == change.data["entry_id"]
    assert same.data["text"] == change.data["text"]

    Mint.HTTP.close(first)
    assert person_reply(ctx, id, 3).status == 200
    {second, _second_feed, later} = next_change(second, second_feed, "Turn 3")
    assert later.id > change.id
    Mint.HTTP.close(second)
  end

  test "closed chats follow a scoped listener from a saved client position", ctx do
    id = item_id(send_message(ctx, @origin, 1))

    assert request(ctx, "a2", "POST", answer_path(id), %{"request_revision" => 1}, 2).status ==
             200

    cached = decode(get(ctx, "a2", "/v2/items/#{id}"))["item"]
    assert cached["terminal"]["state"] == "answered"
    assert cached["availability"]["state"] == "not_listening"
    cursor = person_head(ctx, ctx.person)

    {conn, _feed} = open_agent_feed(ctx, agent_head(ctx), @session)

    try do
      assert eventually(fn -> availability(ctx, "a2", id) == "listening" end)

      assert %{"changes" => [%{"kind" => "item", "data" => refreshed}], "rev" => position} =
               decode(get(ctx, "a2", "/v2/changes?after=#{cursor}"))

      assert refreshed["rev"] > cached["rev"]
      assert refreshed["availability"]["state"] == "listening"
      assert Map.drop(refreshed, ~w(rev availability)) == Map.drop(cached, ~w(rev availability))

      Mint.HTTP.close(conn)
      assert eventually(fn -> availability(ctx, "a2", id) == "not_listening" end)

      assert %{"changes" => [%{"kind" => "item", "data" => detached}]} =
               decode(get(ctx, "a2", "/v2/changes?after=#{position}"))

      assert detached["rev"] > refreshed["rev"]
      assert detached["availability"]["state"] == "not_listening"
    after
      Mint.HTTP.close(conn)
    end
  end

  test "an inbox read between availability pages uses the committed Item revision", ctx do
    id = item_id(send_message(ctx, @origin, 1))
    cached = decode(get(ctx, "a2", "/v2/items/#{id}"))["item"]
    key = :sys.get_state(ctx.relay).store.engine.messages |> Map.keys() |> hd()

    # Hold this page pending as if an earlier page had yielded to HTTP.
    :sys.replace_state(ctx.relay, &Map.put(&1, :agent_availability_jobs, MapSet.new([key])))
    {conn, _feed} = open_agent_feed(ctx, agent_head(ctx))
    scope = {ctx.person, ctx.keys[@origin], nil}

    assert eventually(fn ->
             get_in(:sys.get_state(ctx.relay), [:agent_feed_activity, :open, scope]) == 1
           end)

    inbox = decode(get(ctx, "a2", "/v2/inbox"))
    assert [^cached] = inbox["items"]
    Mint.HTTP.close(conn)

    assert eventually(fn ->
             is_nil(get_in(:sys.get_state(ctx.relay), [:agent_feed_activity, :open, scope]))
           end)

    send(ctx.relay, {:agent_feed_activity, :project, nil})
    assert %{"changes" => []} = decode(get(ctx, "a2", "/v2/changes?after=#{inbox["rev"]}"))
    assert cached == decode(get(ctx, "a2", "/v2/items/#{id}"))["item"]
  end

  test "only a new scoped SSE or accepted send reopens an ended session", ctx do
    closed = item_id(send_message(ctx, @origin, 1))
    open = item_id(send_message(ctx, @origin, 2))

    assert request(ctx, "a2", "POST", answer_path(closed), %{"request_revision" => 1}, 3).status ==
             200

    end_path = "/v2/agent/sessions/#{@session}/end"
    assert request(ctx, @origin, "POST", end_path, %{}, 4).status == 200
    assert availability(ctx, "a2", closed) == "gone"
    assert availability(ctx, "a2", open) == "gone"

    head = agent_head(ctx)
    broad_path = "/v2/agent/changes?after=#{head}"
    scoped_path = broad_path <> "&session=#{@session}"
    assert get(ctx, @origin, scoped_path).status == 200
    assert get(ctx, @origin, broad_path).status == 200
    {unscoped, _feed} = open_agent_feed(ctx, head)
    assert item_id(send_message(ctx, @origin, 1)) == closed
    assert :ok = Relay.setup_sender(ctx.relay, ctx.keys["a2"], "app:test")
    assert send_message(ctx, "a2", 5).status == 200
    assert availability(ctx, "a2", closed) == "gone"
    assert availability(ctx, "a2", open) == "gone"
    Mint.HTTP.close(unscoped)

    {scoped, _feed} = open_agent_feed(ctx, head, @session)
    assert eventually(fn -> availability(ctx, "a2", closed) == "listening" end)
    assert availability(ctx, "a2", open) == "listening"

    assert request(ctx, @origin, "POST", end_path, %{}, 6).status == 200
    assert availability(ctx, "a2", closed) == "gone"
    assert availability(ctx, "a2", open) == "gone"
    Mint.HTTP.close(scoped)

    assert send_message(ctx, @origin, 7).status == 200
    assert availability(ctx, "a2", closed) == "listening"
    assert :ok = stop_supervised(Server)
    server = start_supervised!({Server, ctx.server_opts})
    reopened = %{ctx | relay: Server.relay(server), port: Server.port(server)}
    assert availability(reopened, "a2", closed) == "not_listening"
    assert availability(reopened, "a2", open) == "not_listening"
  end

  test "overlapping coverage and lease renewal emit only effective changes", ctx do
    id = item_id(send_message(ctx, @origin, 1))
    head = agent_head(ctx)
    path = "/v2/agent/changes?after=#{head}&session=#{@session}"
    cursor = person_head(ctx, ctx.person)

    assert get(ctx, @origin, path).status == 200
    assert eventually(fn -> availability(ctx, "a2", id) == "listening" end)
    active = person_head(ctx, ctx.person)
    assert active == cursor + 1

    assert get(ctx, @origin, path).status == 200
    {conn, _feed} = open_agent_feed(ctx, head, @session)

    scope = {ctx.person, ctx.keys[@origin], @session}
    assert eventually(fn ->
             get_in(:sys.get_state(ctx.relay), [:agent_feed_activity, :open, scope]) == 1
           end)

    state = :sys.get_state(ctx.relay)
    {_, token} = state.agent_feed_activity.batches[scope]
    send(ctx.relay, {:agent_feed_activity, {:expire, token}, scope})

    assert availability(ctx, "a2", id) == "listening"
    assert person_head(ctx, ctx.person) == active
    Mint.HTTP.close(conn)
    assert eventually(fn -> availability(ctx, "a2", id) == "not_listening" end)
    assert person_head(ctx, ctx.person) == active + 1
  end

  defp eventually(check, attempts \\ 80)
  defp eventually(check, 0), do: check.()

  defp eventually(check, attempts) do
    check.() or (Process.sleep(25) == :ok and eventually(check, attempts - 1))
  end

  test "owner revocation closes feeds and refreshes only affected v2 items",
       ctx do
    first = item_id(send_message(ctx, @origin, 1))
    second = item_id(send_message(ctx, @origin, 2))
    ended = item_id(send_message(ctx, @origin, 3, %{"session" => @ended_session}))
    assert :ok = Relay.setup_sender(ctx.relay, ctx.keys["a2"], "app:test")
    other_origin = item_id(send_message(ctx, "a2", 4))
    other_account = item_id(send_message(ctx, "b1", 5))
    original_reply = person_reply(ctx, first, 7)
    assert original_reply.status == 200
    assert %{"entry" => %{"entry_id" => reply_id}} = decode(original_reply)

    assert request(ctx, @origin, "POST", "/v2/agent/sessions/#{@ended_session}/end", %{}, 6).status ==
             200

    assert availability(ctx, "a2", first) != "gone"
    assert availability(ctx, "a2", second) != "gone"
    assert availability(ctx, "a2", ended) == "gone"

    cursor = person_head(ctx, ctx.person)
    other_cursor = person_head(ctx, "B")
    head = agent_head(ctx)
    feeds = for _ <- 1..2, do: open_agent_feed(ctx, head)

    assert :ok = Relay.owner_revoke_device(ctx.relay, ctx.person, ctx.owner, ctx.keys[@origin])

    for {conn, feed} <- feeds do
      {conn, feed, closing} = ClientAPIFeedClient.until_closing(conn, feed)
      assert closing == %{event: "closing", data: %{"reason" => "revoked"}}
      assert {_conn, %{done: true}} = ClientAPIFeedClient.drain(conn, feed)
    end

    assert tls_refused?(ctx, @origin)
    assert_gone_items(ctx, cursor, first, second, ended, other_origin)
    assert gone_changes(ctx, "b1", other_cursor) == []
    assert availability(ctx, "b1", other_account) != "gone"

    assert :ok = Relay.owner_revoke_device(ctx.relay, ctx.person, ctx.owner, ctx.keys[@origin])
    assert :ok = stop_supervised(Server)
    server = start_supervised!({Server, ctx.server_opts})
    reopened = %{ctx | relay: Server.relay(server), port: Server.port(server)}

    assert tls_refused?(reopened, @origin)
    assert_gone_items(reopened, cursor, first, second, ended, other_origin)

    assert Enum.any?(
             item_entries(reopened, first),
             &match?(%{"kind" => "reply", "entry_id" => ^reply_id}, &1)
           )

    assert person_reply(reopened, first, 8).status == 200
  end

  defp assert_gone_items(ctx, cursor, first, second, ended, other_origin) do
    assert Enum.sort(gone_changes(ctx, "a2", cursor)) == Enum.sort([first, second])

    for id <- [first, second, ended] do
      assert availability(ctx, "a2", id) == "gone"
      refute Enum.any?(item_entries(ctx, id), &(&1["kind"] == "availability"))
    end

    assert availability(ctx, "a2", other_origin) != "gone"
  end

  defp item_entries(ctx, id) do
    response = get(ctx, "a2", "/v2/items/#{id}/entries")
    assert response.status == 200
    assert %{"entries" => entries} = decode(response)
    entries
  end

  defp gone_changes(ctx, reader, cursor) do
    response = get(ctx, reader, "/v2/changes?after=#{cursor}")
    assert response.status == 200
    assert %{"changes" => changes, "more" => false} = decode(response)

    for %{"kind" => "item", "data" => %{"availability" => %{"state" => "gone"}} = item} <- changes,
        do: item["item_id"]
  end

  defp availability(ctx, reader, id) do
    response = get(ctx, reader, "/v2/items/#{id}")
    assert response.status == 200
    assert %{"item" => %{"availability" => %{"state" => state}}} = decode(response)
    state
  end

  defp person_head(ctx, person) do
    assert {:ok, head} = ChangeLog.head(:sys.get_state(ctx.relay).store, person)
    head
  end

  defp await_default_answer(ctx, id, retries) do
    case origin_terminal(ctx, id) do
      nil when retries > 0 ->
        Process.sleep(20)
        await_default_answer(ctx, id, retries - 1)

      terminal ->
        terminal
    end
  end

  defp register_agent(ctx) do
    der = TLSFixtures.certificate_der!(ctx.certs, "parity-agent")
    assert {:ok, agent} = DeviceIdentity.from_certificate(der)
    assert :ok = Relay.owner_register_agent(ctx.relay, ctx.person, ctx.owner, @agent_id, agent)

    assert :ok =
             Relay.owner_replace_agent_grants(
               ctx.relay,
               ctx.person,
               ctx.owner,
               agent,
               1,
               ["app:test"],
               ["app:test"]
             )
  end

  defp messages(ctx), do: Map.keys(:sys.get_state(ctx.relay).store.engine.messages)

  defp send_message(ctx, name, n, extra \\ %{}, sender \\ "app:test", content \\ %{}) do
    message = Map.merge(%{"title" => "Message #{n}", "created_at" => n}, content)
    body = Map.merge(%{"sender" => sender, "session" => @session, "message" => message}, extra)
    request(ctx, name, "POST", "/v2/agent/messages", body, n)
  end

  defp item_id(response) do
    assert response.status == 200
    decode(response)["message"]["item_id"]
  end

  defp answer_path(id), do: "/v2/items/" <> id <> "/answer"

  defp origin_terminal(ctx, id) do
    response = get(ctx, @origin, "/v2/agent/messages/" <> id)
    assert response.status == 200
    decode(response)["message"]["terminal"]
  end

  defp person_reply(ctx, id, n),
    do: request(ctx, "a2", "POST", "/v2/items/" <> id <> "/replies", %{"text" => "Turn #{n}"}, n)

  defp agent_head(ctx) do
    store = :sys.get_state(ctx.relay).store
    assert {:ok, head} = AgentChangeLog.head(store, {ctx.person, ctx.keys[@origin]})
    head
  end

  defp open_agent_feed(ctx, head, session \\ nil) do
    conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, @origin)
    path = "/v2/agent/changes?after=#{head}" <> if(session, do: "&session=#{session}", else: "")

    assert {:ok, conn, ref} =
             Mint.HTTP.request(conn, "GET", path, [{"accept", "text/event-stream"}], nil)

    feed = %{ref: ref, buffer: "", status: nil, headers: [], done: false}
    {conn, feed} = ClientAPIFeedClient.await_open(conn, feed)
    assert feed.status == 200
    {conn, feed}
  end

  defp next_change(conn, feed, text \\ nil) do
    case ClientAPIFeedClient.events(conn, feed, 1) do
      {conn, feed, [%{id: _, data: %{"text" => message}} = event]}
      when is_nil(text) or text == message ->
        {conn, feed, event}

      {conn, feed, [_]} ->
        next_change(conn, feed, text)
    end
  end

  defp next_answer(conn, feed) do
    case ClientAPIFeedClient.events(conn, feed, 1) do
      {conn, feed,
       [%{event: "message", data: %{"terminal" => %{"state" => "answered"}}} = answer]} ->
        {conn, feed, answer}

      {conn, feed, [_]} ->
        next_answer(conn, feed)
    end
  end

  defp upload(ctx, name, n, data) do
    headers = [
      {"content-type", "text/plain"},
      {"content-length", Integer.to_string(byte_size(data))},
      {"content-disposition", "attachment; filename*=UTF-8''fixture.txt"},
      {"idempotency-key", op(n)}
    ]

    conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, name)
    assert {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", "/v2/files", headers, data)
    {conn, response} = ClientAPIFeedClient.collect(conn, ref)
    Mint.HTTP.close(conn)
    response
  end

  defp request(ctx, name, method, path, body, n) do
    headers = [{"content-type", "application/json"}, {"idempotency-key", op(n)}]
    conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, name)
    assert {:ok, conn, ref} = Mint.HTTP.request(conn, method, path, headers, JSON.encode!(body))
    {conn, response} = ClientAPIFeedClient.collect(conn, ref)
    Mint.HTTP.close(conn)
    response
  end

  defp get(ctx, name, path) do
    conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, name)
    assert {:ok, conn, ref} = Mint.HTTP.request(conn, "GET", path, [], nil)
    {conn, response} = ClientAPIFeedClient.collect(conn, ref)
    Mint.HTTP.close(conn)
    response
  end

  # A TLS 1.3 client may finish its side of the handshake before the server
  # rejects the certificate, so refusal shows on connect or on the first read.
  defp tls_refused?(ctx, name) do
    capture_log(fn ->
      result =
        case Mint.HTTP.connect(:https, "127.0.0.1", ctx.port,
               hostname: "relay.test",
               protocols: [:http2],
               mode: :passive,
               transport_opts: [
                 versions: [:"tlsv1.3"],
                 cacertfile: Path.join(ctx.certs, "ca.pem"),
                 certfile: Path.join(ctx.certs, "#{name}.pem"),
                 keyfile: Path.join(ctx.certs, "#{name}.key"),
                 server_name_indication: ~c"relay.test",
                 timeout: 5_000
               ]
             ) do
          {:error, _} ->
            true

          {:ok, conn} ->
            case Mint.HTTP.request(conn, "GET", "/v2/me", [], nil) do
              {:error, _, _} ->
                true

              {:ok, conn, _ref} ->
                match?({:error, _, _, _}, Mint.HTTP.recv(conn, 0, 5_000))
            end
        end

      send(self(), {:tls_refused, result})
    end)

    assert_received {:tls_refused, result}
    result
  end

  defp assert_error(response, status, token) do
    assert response.status == status
    assert %{"error" => ^token} = decode(response)
  end

  defp decode(response) do
    assert {:ok, body} = JSON.decode_line(response.body <> "\n")
    body
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"
end
