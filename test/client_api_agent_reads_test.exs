defmodule AttentionServer.ClientAPIAgentReadsTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    AgentChangeLog,
    ChangeLog,
    ClientAPIFeedClient,
    ClientAPISchema,
    JSON,
    Relay,
    Server,
    TLSFixtures
  }

  alias AttentionServer.ClientAPI.Resources

  @first_session "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @other_session "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    defs = ClientAPISchema.contract_defs(Path.expand("../docs/client-api-v2.md", __DIR__))
    %{certs: certs, defs: defs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "agent-reads-#{System.unique_integer([:positive])}")
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
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")

    keys =
      Map.new([{"a1", "A"}, {"a2", "A"}, {"b1", "B"}], fn {name, person} ->
        assert {:ok, key} =
                 Relay.setup_device_certificate(
                   relay,
                   person,
                   TLSFixtures.certificate_der!(certs, name)
                 )

        {name, key}
      end)

    assert :ok = Relay.setup_sender(relay, keys["a1"], "app:test")
    assert :ok = Relay.setup_sender(relay, keys["a2"], "app:test")
    assert :ok = Relay.setup_sender(relay, keys["b1"], "app:test")

    %{
      server: server,
      server_opts: server_opts,
      relay: relay,
      keys: keys,
      port: Server.port(server)
    }
  end

  describe "delivery baseline" do
    @baseline "/v2/agent/delivery-baseline"

    test "pages pending terminal Chat and answers, and retries the served page", ctx do
      item = forward(ctx, "a1", 1, @first_session)
      entry = person_reply(ctx, item, 2)
      outside = forward(ctx, "a1", 3, @other_session)
      person_reply(ctx, outside, 4)

      assert request(
               ctx,
               "a2",
               "POST",
               "/v2/items/#{item}/answer",
               %{"request_revision" => 1, "comment" => "Answer after Chat"},
               5
             ).status == 200

      entries = "/v2/items/#{item}/entries"

      assert %{"entries" => [%{"delivery" => %{"delivered" => false}}]} =
               decode_response(get(ctx, "a2", entries))

      path = @baseline <> "?session=#{@first_session}&limit=1"
      headers = [{"harklight-ack2", "true"}]
      prepared = get(ctx, "a1", path, headers)
      assert prepared.status == 200
      assert List.keyfind(prepared.headers, "harklight-ack2", 0) == {"harklight-ack2", "true"}
      assert %{"rev" => rev, "next_page" => page} = decode_response(prepared)

      assert %{"entries" => [%{"delivery" => %{"delivered" => false}}]} =
               decode_response(get(ctx, "a2", entries))

      {first_path, first} = next_delivery_page(ctx, path, page, headers)

      assert %{
               "rev" => ^rev,
               "changes" => [%{"kind" => "entry", "data" => %{"entry_id" => ^entry}}],
               "next_page" => next
             } = first

      assert %{"entries" => [%{"delivery" => %{"delivered" => true, "ack2_expected" => true}}]} =
               decode_response(get(ctx, "a2", entries))

      assert :ok = stop_supervised(Server)
      restarted = start_supervised!({Server, ctx.server_opts})

      ctx = %{
        ctx
        | server: restarted,
          relay: Server.relay(restarted),
          port: Server.port(restarted)
      }

      assert %{"changes" => [%{"kind" => "entry", "data" => %{"entry_id" => ^entry}}]} =
               decode_response(get(ctx, "a1", first_path, headers))

      assert %{
               "rev" => ^rev,
               "changes" => [
                 %{"kind" => "message", "data" => %{"item_id" => ^item, "terminal" => %{}}}
               ]
             } = last = elem(next_delivery_page(ctx, path, next, headers), 1)

      if last["next_page"] do
        assert {_path, %{"changes" => []} = tail} =
                 next_delivery_page(ctx, path, last["next_page"], headers)

        refute Map.has_key?(tail, "next_page")
      end

      assert %{"message" => %{"terminal" => %{"delivery" => %{"delivered" => true}}}} =
               decode_response(get(ctx, "a1", "/v2/agent/messages/#{item}"))

      [baseline_chat] = first["changes"]
      feed_path = "/v2/agent/changes?session=#{@first_session}"
      assert %{"rev" => caught_up, "changes" => changes} =
               decode_response(get(ctx, "a1", feed_path <> "&after=#{rev}"))

      assert replay = Enum.find(changes, &(get_in(&1, ["data", "entry_id"]) == entry))
      assert replay["rev"] > baseline_chat["rev"]
      assert get_in(replay, ["data", "delivery", "delivered"]) == true
      assert normalize_delivery_replay(replay) == normalize_delivery_replay(baseline_chat)

      receipt = "/v2/agent/messages/#{item}/entries/#{entry}/receipt"
      assert request(ctx, "a1", "POST", receipt, %{"outcome" => "handed"}, 6).status == 200

      assert %{"changes" => changes} =
               decode_response(get(ctx, "a1", feed_path <> "&after=#{caught_up}"))

      assert updated = Enum.find(changes, &(get_in(&1, ["data", "entry_id"]) == entry))
      assert get_in(updated, ["data", "delivery", "outcome"]) == "handed"
      refute normalize_delivery_replay(updated) == normalize_delivery_replay(replay)
      assert %{"changes" => []} = decode_response(get(ctx, "a1", first_path, headers))

      assert %{"next_page" => fresh} = decode_response(get(ctx, "a1", path, headers))

      assert {_path, %{"changes" => []} = finished} =
               next_delivery_page(ctx, path, fresh, headers)

      refute Map.has_key?(finished, "next_page")
    end

    test "the attempt excludes concurrent arrivals and a new scoped attempt recovers them after expiry",
         ctx do
      item = forward(ctx, "a1", 1, @first_session)
      first = person_reply(ctx, item, 2)
      path = @baseline <> "?session=#{@first_session}"
      assert %{"rev" => rev, "next_page" => page} = decode_response(get(ctx, "a1", path))

      late = person_reply(ctx, item, 3)

      assert {_path,
              %{
                "rev" => ^rev,
                "changes" => [
                  %{"kind" => "entry", "data" => %{"entry_id" => ^first}}
                ]
              }} = next_delivery_page(ctx, path, page, [])

      assert %{"delivery" => %{"delivered" => false}} =
               person_chat(ctx, "/v2/items/#{item}/entries", late)

      assert {:ok, _} =
               ClientAPIFeedClient.prune_changes(ctx.relay, System.system_time(:millisecond) + 1)

      assert get(ctx, "a1", "/v2/agent/changes?after=#{rev}&session=#{@first_session}").status ==
               410

      assert %{"next_page" => next} = decode_response(get(ctx, "a1", path))

      assert {_path,
              %{
                "changes" => [
                  %{"kind" => "entry", "data" => %{"entry_id" => ^late}}
                ]
              }} = next_delivery_page(ctx, path, next, [])

      assert %{"delivery" => %{"delivered" => true} = unknown} =
               person_chat(ctx, "/v2/items/#{item}/entries", late)

      refute Map.has_key?(unknown, "ack2_expected")
      refute Map.has_key?(unknown, "legacy_caption")
    end

    test "an attachment failure does not settle a saved baseline turn", ctx do
      item = forward(ctx, "a1", 1, @first_session)
      handle = contract(ctx, "FileCreated", upload(ctx, "a2", 2, "Attachment"))["file_id"]

      posted =
        request(
          ctx,
          "a2",
          "POST",
          "/v2/items/#{item}/replies",
          %{"text" => "Chat with attachment", "file_ids" => [handle]},
          3
        )

      assert %{"entry" => %{"entry_id" => id, "files" => [%{"file_id" => file}]}} =
               contract(ctx, "WriteResult", posted)

      path = @baseline <> "?session=#{@first_session}"
      assert %{"next_page" => token} = decode_response(get(ctx, "a1", path))

      assert {_, %{"changes" => [%{"data" => %{"entry_id" => ^id}}]}} =
               next_delivery_page(ctx, path, token, [])

      use = "/v2/agent/messages/#{item}/entries/#{id}/file-use"
      body = %{"file_id" => file, "transfer" => "not_transferred", "reason" => "Read interrupted"}
      assert request(ctx, "a1", "POST", use, body, 4).status == 200

      assert :ok = stop_supervised(Server)
      server = start_supervised!({Server, ctx.server_opts})
      ctx = %{ctx | server: server, relay: Server.relay(server), port: Server.port(server)}

      assert {_, %{"changes" => [%{"data" => %{"entry_id" => ^id}}]}} =
               next_delivery_page(ctx, path, token, [])
    end

    test "a false declaration needs positive receipt and later declarations do not replace it",
         ctx do
      item = forward(ctx, "a1", 1, @first_session)
      id = person_reply(ctx, item, 2)
      path = "/v2/agent/changes?after=0&session=#{@first_session}"
      read = "/v2/items/#{item}/entries"
      confirm = "/v2/agent/messages/#{item}/entries/#{id}/listener-received"

      response = get(ctx, "a1", path, [{"harklight-ack2", "false"}])
      assert response.status == 200
      assert List.keyfind(response.headers, "harklight-ack2", 0) == {"harklight-ack2", "false"}
      conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, "a1")
      headers = [{"accept", "text/event-stream"}, {"harklight-ack2", "true"}]
      assert {:ok, conn, ref} = Mint.HTTP.request(conn, "GET", path, headers, nil)
      feed = %{ref: ref, buffer: "", status: nil, headers: [], done: false}
      {conn, feed} = ClientAPIFeedClient.await_open(conn, feed)
      assert feed.status == 200
      assert List.keyfind(feed.headers, "harklight-ack2", 0) == {"harklight-ack2", "true"}
      Mint.HTTP.close(conn)

      assert %{"rev" => before, "delivery" => pending} = person_chat(ctx, read, id)
      assert pending == %{"delivered" => true, "acted" => false, "ack2_expected" => false}

      assert request(ctx, "a2", "PUT", confirm, %{}, 3).status == 404
      assert request(ctx, "a1", "PUT", confirm, %{}, 4).status == 204
      assert %{"rev" => confirmed, "delivery" => confirmed_delivery} = person_chat(ctx, read, id)
      assert confirmed > before
      assert confirmed_delivery == Map.put(pending, "listener_received", true)
      assert request(ctx, "a1", "PUT", confirm, %{}, 5).status == 204
      assert %{"rev" => ^confirmed} = person_chat(ctx, read, id)

      assert %{"features" => %{"handoff_captions" => true}} =
               decode_response(get(ctx, "a2", "/v2/me"))
    end

    test "person delivery shows a receipt transition without conflating action and handoff",
         ctx do
      item = forward(ctx, "a1", 1, @first_session)
      entry = person_reply(ctx, item, 2)
      receipt = "/v2/agent/messages/#{item}/entries/#{entry}/receipt"
      read = "/v2/items/#{item}/entries"

      assert request(
               ctx,
               "a1",
               "POST",
               receipt,
               %{"outcome" => "not_handed", "reason" => "Unavailable"},
               3
             ).status == 200

      assert %{"entries" => [%{"rev" => before, "delivery" => delivery}]} =
               decode_response(get(ctx, "a2", read))

      assert %{"outcome" => "not_handed", "reason" => "Unavailable", "acted" => true} = delivery
      refute Map.has_key?(delivery, "agent_acted")
      refute Map.has_key?(delivery, "legacy_caption")

      assert request(
               ctx,
               "a1",
               "POST",
               receipt,
               %{"outcome" => "handed", "agent" => "Fixture", "rerouted" => true},
               4
             ).status == 200

      assert %{"entries" => [%{"rev" => after_receipt, "delivery" => delivery}]} =
               decode_response(get(ctx, "a2", read))

      assert after_receipt > before
      assert %{"outcome" => "handed", "agent" => "Fixture", "rerouted" => true} = delivery
      refute Map.has_key?(delivery, "reason")
    end
  end

  describe "a deterministic projection fault" do
    @describetag :projection_fault

    setup ctx do
      on_exit(fn -> Application.delete_env(:attention_server, :client_api_projection_fault) end)
      %{item: forward(ctx, "a1", 1, @first_session)}
    end

    test "the person entries read reports a terminal error without restarting Relay", ctx do
      entry = person_reply(ctx, ctx.item, 2)
      fault_on_entry(entry)

      {reply, log} =
        ExUnit.CaptureLog.with_log(fn -> get(ctx, "a2", "/v2/items/" <> ctx.item <> "/entries") end)

      assert_internal_error(reply)
      {"request-id", request_id} = List.keyfind(reply.headers, "request-id", 0)
      assert log =~ "request_id=#{request_id} resource=entry:#{entry}"
      refute log =~ "deterministic projection fault"
      assert Server.relay(ctx.server) == ctx.relay
    end

    test "a failed person batch leaves its cursor before the entry", ctx do
      entry = person_reply(ctx, ctx.item, 2)
      fault_on_entry(entry)
      path = "/v2/changes?after=0"

      assert_internal_error(get(ctx, "a2", path))
      assert Server.relay(ctx.server) == ctx.relay
      Application.delete_env(:attention_server, :client_api_projection_fault)

      assert %{"changes" => changes} = Elixir.JSON.decode!(get(ctx, "a2", path).body)
      assert Enum.any?(changes, &(get_in(&1, ["data", "entry_id"]) == entry))
    end

    for {name, path} <- [{"a2", "/v2/changes?after=0"}, {"a1", "/v2/agent/changes?after=0"}] do
      test "a failure before resource rendering stops #{path} at the original cursor", ctx do
        name = unquote(name)
        path = unquote(path)
        entry = person_reply(ctx, ctx.item, 2)

        Application.put_env(:attention_server, :client_api_projection_fault, fn
          :feed -> raise "feed setup fault"
          _ -> :ok
        end)

        assert_internal_error(get(ctx, name, path))
        assert Server.relay(ctx.server) == ctx.relay
        Application.delete_env(:attention_server, :client_api_projection_fault)

        assert %{"changes" => changes} = Elixir.JSON.decode!(get(ctx, name, path).body)
        assert Enum.any?(changes, &(get_in(&1, ["data", "entry_id"]) == entry))
      end
    end

    test "the person SSE backlog closes without advancing the cursor", ctx do
      assert {:ok, before} = ChangeLog.head(:sys.get_state(ctx.relay).store, "A")
      entry = person_reply(ctx, ctx.item, 2)
      fault_on_entry(entry)

      conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, "a2")
      {conn, feed} = ClientAPIFeedClient.open_feed(conn, "?after=#{before}")
      {conn, feed} = ClientAPIFeedClient.await_open(conn, feed)
      assert feed.status == 200
      {conn, feed, [event]} = ClientAPIFeedClient.events(conn, feed, 1)
      assert event == %{event: "closing", data: %{"reason" => "internal_error"}}
      {conn, feed} = ClientAPIFeedClient.drain(conn, feed)
      assert feed.done
      Mint.HTTP.close(conn)
      assert Server.relay(ctx.server) == ctx.relay
    end

    test "a live SSE feed closes on a subsequent projection fault", ctx do
      assert {:ok, before} = ChangeLog.head(:sys.get_state(ctx.relay).store, "A")
      conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, "a2")
      {conn, feed} = ClientAPIFeedClient.open_feed(conn, "?after=#{before}")
      {conn, feed} = ClientAPIFeedClient.await_open(conn, feed)
      assert feed.status == 200
      fault_on_entry(:any)

      path = "/v2/items/" <> ctx.item <> "/replies"
      assert_internal_error(request(ctx, "a2", "POST", path, %{"text" => "Live reply"}, 2))

      {conn, feed, [event]} = ClientAPIFeedClient.events(conn, feed, 1)
      assert event == %{event: "closing", data: %{"reason" => "internal_error"}}
      {conn, feed} = ClientAPIFeedClient.drain(conn, feed)
      assert feed.done
      Mint.HTTP.close(conn)
      assert Server.relay(ctx.server) == ctx.relay
    end

    test "an agent batch fails before recording delivery and replays the entry", ctx do
      entry = person_reply(ctx, ctx.item, 2)
      fault_on_entry(entry)
      path = "/v2/agent/changes?after=0"

      assert_internal_error(get(ctx, "a1", path))
      assert Server.relay(ctx.server) == ctx.relay

      conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, "a1")
      headers = [{"accept", "text/event-stream"}]
      assert {:ok, conn, ref} = Mint.HTTP.request(conn, "GET", path, headers, nil)
      feed = %{ref: ref, buffer: "", status: nil, headers: [], done: false}
      {conn, feed} = ClientAPIFeedClient.await_open(conn, feed)
      assert feed.status == 200
      {conn, feed, [event]} = ClientAPIFeedClient.events(conn, feed, 1)
      assert event == %{event: "closing", data: %{"reason" => "internal_error"}}
      {conn, feed} = ClientAPIFeedClient.drain(conn, feed)
      assert feed.done
      Mint.HTTP.close(conn)
      Application.delete_env(:attention_server, :client_api_projection_fault)

      entries = get(ctx, "a1", "/v2/agent/messages/" <> ctx.item <> "/entries")
      assert %{"entries" => turns} = contract(ctx, "AgentEntriesPage", entries)
      assert %{"delivery" => %{"delivered" => false}} =
               Enum.find(turns, &(&1["entry_id"] == entry))

      assert %{"changes" => changes} = Elixir.JSON.decode!(get(ctx, "a1", path).body)
      assert Enum.any?(changes, &(get_in(&1, ["data", "entry_id"]) == entry))
    end

    for {stage, probe_key} <- [{"entry projection", :entry}, {"renderer setup", :feed}] do
      test "a committed reply survives a fault in #{stage} and keyed replay", ctx do
        probe_key = unquote(probe_key)

        Application.put_env(:attention_server, :client_api_projection_fault, fn
          {:entry, _id} when probe_key == :entry -> raise "deterministic projection fault"
          ^probe_key -> raise "deterministic projection fault"
          _ -> :ok
        end)

        path = "/v2/items/" <> ctx.item <> "/replies"
        body = %{"text" => "Committed before the response"}

        assert_internal_error(request(ctx, "a2", "POST", path, body, 2))
        assert Server.relay(ctx.server) == ctx.relay

        assert_internal_error(request(ctx, "a2", "POST", path, body, 2))
        Application.delete_env(:attention_server, :client_api_projection_fault)

        replay = request(ctx, "a2", "POST", path, body, 2)
        assert replay.status == 200
        assert {"idempotent-replayed", "true"} =
                 List.keyfind(replay.headers, "idempotent-replayed", 0)

        assert %{"entry" => %{"entry_id" => id}} = contract(ctx, "WriteResult", replay)

        read = get(ctx, "a2", "/v2/items/" <> ctx.item <> "/entries")
        assert [%{"entry_id" => ^id}] = contract(ctx, "EntriesPage", read)["entries"]
      end
    end

    test "a committed agent reply saves its receipt before rendering", ctx do
      fault_on_entry(:any)
      path = "/v2/agent/messages/" <> ctx.item <> "/replies"
      body = %{"text" => "Agent reply with a failing response"}

      assert_internal_error(request(ctx, "a1", "POST", path, body, 2))
      assert_internal_error(request(ctx, "a1", "POST", path, body, 2))
      assert Server.relay(ctx.server) == ctx.relay
      Application.delete_env(:attention_server, :client_api_projection_fault)

      replay = request(ctx, "a1", "POST", path, body, 2)
      assert replay.status == 200
      assert {"idempotent-replayed", "true"} = List.keyfind(replay.headers, "idempotent-replayed", 0)
      assert %{"entry" => %{"entry_id" => id}} = contract(ctx, "AgentWriteResult", replay)

      read = get(ctx, "a1", "/v2/agent/messages/" <> ctx.item <> "/entries")
      assert [%{"entry_id" => ^id}] = contract(ctx, "AgentEntriesPage", read)["entries"]
    end

    test "the shared file route distinguishes a projection error from storage", ctx do
      staged = upload(ctx, "a1", 2, "attachment bytes")
      assert %{"file_id" => handle} = contract(ctx, "FileCreated", staged)

      reply =
        request(
          ctx,
          "a1",
          "POST",
          "/v2/agent/messages/" <> ctx.item <> "/replies",
          %{"file_ids" => [handle]},
          3
        )

      assert %{"entry" => %{"files" => [%{"file_id" => file}]}} =
               contract(ctx, "AgentWriteResult", reply)

      Application.put_env(:attention_server, :client_api_projection_fault, fn
        :file -> raise "deterministic projection fault"
        _ -> :ok
      end)

      path = "/v2/files/" <> file
      assert_internal_error(get(ctx, "a2", path))
      assert Server.relay(ctx.server) == ctx.relay
      Application.delete_env(:attention_server, :client_api_projection_fault)
      assert get(ctx, "a2", path).body == "attachment bytes"
    end
  end

  defp fault_on_entry(target) do
    Application.put_env(:attention_server, :client_api_projection_fault, fn
      {:entry, id} when id == target or target == :any -> raise "deterministic projection fault"
      _ -> :ok
    end)
  end

  defp assert_internal_error(reply) do
    assert reply.status == 500
    assert {"request-id", request_id} = List.keyfind(reply.headers, "request-id", 0)
    refute List.keymember?(reply.headers, "retry-after", 0)

    assert %{"error" => "internal_error", "retryable" => false, "request_id" => ^request_id} =
             Elixir.JSON.decode!(reply.body)
  end

  test "lists only the enrolled agent's messages, newest first, and filters by session", ctx do
    forward(ctx, "a1", 1, @first_session)
    forward(ctx, "a1", 2, @other_session)
    forward(ctx, "a2", 3, @first_session)
    forward(ctx, "b1", 4, @first_session)

    response = get(ctx, "a1", "/v2/agent/messages")
    assert response.status == 200

    assert %{"rev" => rev, "messages" => [newer, older]} =
             contract(ctx, "SentMessagesPage", response)

    assert rev > 0
    assert [newer["session"], older["session"]] == [@other_session, @first_session]

    scoped = get(ctx, "a1", "/v2/agent/messages?session=" <> @first_session)
    assert contract(ctx, "SentMessagesPage", scoped)["messages"] == [older]

    legacy =
      get(ctx, "a1", "/v2/agent/messages?sender=app%3Atest&legacy_message_id=item-1")

    assert older["legacy_message_id"] == "item-1"
    assert contract(ctx, "SentMessagesPage", legacy)["messages"] == [older]

    combined = "/v2/agent/messages?sender=app%3Atest&legacy_message_id=item-1&session="

    assert contract(ctx, "SentMessagesPage", get(ctx, "a1", combined <> @first_session))[
             "messages"
           ] == [older]

    assert contract(ctx, "SentMessagesPage", get(ctx, "a1", combined <> @other_session))[
             "messages"
           ] == []

    assert get(ctx, "a1", "/v2/agent/messages?session=invalid").status == 400
  end

  test "message pages bind the token to the agent and the filters", ctx do
    for n <- 1..51, do: forward(ctx, "a1", n, @first_session)
    filter = "/v2/agent/messages?session=" <> @first_session

    assert %{"messages" => messages, "next_page" => page} =
             contract(ctx, "SentMessagesPage", get(ctx, "a1", filter))

    assert Enum.map(messages, & &1["legacy_message_id"]) ==
             Enum.map(51..2//-1, &"item-#{&1}")

    next = filter <> "&page=" <> page
    last = contract(ctx, "SentMessagesPage", get(ctx, "a1", next))
    assert [%{"legacy_message_id" => "item-1"}] = last["messages"]
    refute Map.has_key?(last, "next_page")
    assert get(ctx, "a2", next).status == 400
    assert get(ctx, "a1", "/v2/agent/messages?page=" <> page).status == 400
  end

  test "reads one message with an ETag and hides it from other principals", ctx do
    id = forward(ctx, "a1", 1, @first_session)

    path = "/v2/agent/messages/" <> id
    response = get(ctx, "a1", path)
    assert response.status == 200

    assert %{"message" => %{"item_id" => ^id, "session" => @first_session}} =
             contract(ctx, "SentMessageResponse", response)

    assert {_, tag} = List.keyfind(response.headers, "etag", 0)
    cached = get(ctx, "a1", path, [{"if-none-match", tag}])
    assert cached.status == 304
    assert cached.body == ""

    assert get(ctx, "a2", path).status == 404
    assert get(ctx, "b1", path).status == 404
  end

  test "pages conversation entries in creation order and hides them from other keys", ctx do
    id = forward(ctx, "a1", 1, @first_session)

    for {n, text} <- [{2, "First"}, {3, "Second"}] do
      frame(ctx, "a2", %{
        "type" => "discussion",
        "op_id" => op(n),
        "sender_id" => "app:test",
        "message_id" => "item-1",
        "request_revision" => 1,
        "text" => text
      })
    end

    path = "/v2/agent/messages/" <> id <> "/entries"
    response = get(ctx, "a1", path <> "?limit=1")
    assert response.status == 200
    assert %{"entries" => [latest], "older" => true} = contract(ctx, "AgentEntriesPage", response)
    assert latest["text"] == "Second"

    earlier = get(ctx, "a1", path <> "?before=" <> latest["entry_id"])

    assert %{"entries" => [first], "older" => false} = contract(ctx, "AgentEntriesPage", earlier)
    assert first["text"] == "First"
    assert first["position"] < latest["position"]
    assert get(ctx, "a2", path).status == 404
  end

  test "bounded entries render an agent reply's committed priority", ctx do
    id = forward(ctx, "a1", 1, @first_session)

    turn =
      frame(ctx, "a2", %{
        "type" => "discussion",
        "op_id" => op(2),
        "sender_id" => "app:test",
        "message_id" => "item-1",
        "request_revision" => 1,
        "text" => "Question"
      })

    %{"seq" => position} =
      frame(ctx, "a1", %{
        "type" => "conversation_reply",
        "op_id" => op(3),
        "sender_id" => "app:test",
        "message_id" => "item-1",
        "request_revision" => 1,
        "in_reply_to" => String.to_integer(turn["event_id"]),
        "text" => "Reply"
      })

    view = :sys.get_state(ctx.relay).priority_views["A"]
    assert %{^position => %{"conversation_seq" => ^position}} =
             view.reply_entries_by_item[{"app:test", "item-1"}]

    reply = %{
      "sender_id" => "app:test",
      "message_id" => "item-1",
      "conversation_seq" => position,
      "score" => 0.9
    }

    :sys.replace_state(ctx.relay, fn state ->
      {engine, positions} =
        AttentionServer.Engine.retain_reply_scores(state.store.engine, "A", [reply])

      assert {:ok, store} =
               AttentionServer.Store.commit(
                 state.store,
                 engine,
                 %{person: "A", actor: "server", action: "reply_group_scores"},
                 positions
               )

      %{state | store: store}
    end)

    path = "/v2/agent/messages/" <> id <> "/entries?limit=1"
    response = get(ctx, "a1", path)
    assert response.status == 200

    body = Elixir.JSON.decode!(response.body)
    assert ClientAPISchema.errors(ctx.defs, "AgentEntriesPage", body) == []

    assert %{
             "entries" => [
               %{"position" => ^position, "priority" => %{"level" => "normal", "score" => 0.9}}
             ],
             "older" => true
           } = body
  end

  @tag :agent_focus
  test "focus requires see permission and lists only eligible account devices", ctx do
    id = forward(ctx, "a1", 1, @first_session)
    report = %{"screen" => "message", "item_id" => id, "ttl_s" => 120}
    assert request(ctx, "a2", "PUT", "/v2/devices/me/focus", report, 2).status == 200

    assert request(
             ctx,
             "b1",
             "PUT",
             "/v2/devices/me/focus",
             %{"screen" => "inbox", "ttl_s" => 120},
             3
           ).status == 200

    route = "/v2/agent/focus"
    assert_error(get(ctx, "a1", route), 403, "forbidden")

    :sys.replace_state(ctx.relay, fn state ->
      put_in(state.store.engine.devices[ctx.keys["a1"]].see_focus, true)
    end)

    key = ctx.keys["a2"]
    snapshot = contract(ctx, "AgentFocus", get(ctx, "a1", route))

    assert %{"clients" => [%{"device_id" => ^key, "screen" => "message", "item_id" => ^id}]} =
             snapshot

    state = :sys.get_state(ctx.relay)
    assert {:ok, head} = AgentChangeLog.head(state.store, {"A", ctx.keys["a1"]})
    conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, "a1")
    feed_path = "/v2/agent/changes?after=#{head}&session=#{@other_session}"

    assert {:ok, conn, ref} =
             Mint.HTTP.request(conn, "GET", feed_path, [{"accept", "text/event-stream"}], nil)

    feed = %{ref: ref, buffer: "", status: nil, headers: [], done: false}
    {conn, feed} = ClientAPIFeedClient.await_open(conn, feed)
    {conn, _feed, [event]} = ClientAPIFeedClient.events(conn, feed, 1)
    Mint.HTTP.close(conn)

    assert %{event: "focus", data: %{"focus" => ^snapshot}} = event
    refute Map.has_key?(event, :id)
  end

  @tag :projection_fault
  test "a Focus fault returns 500 on GET or closes the agent SSE feed", ctx do
    id = forward(ctx, "a1", 1, @first_session)
    report = %{"screen" => "message", "item_id" => id, "ttl_s" => 120}
    assert request(ctx, "a2", "PUT", "/v2/devices/me/focus", report, 2).status == 200

    :sys.replace_state(ctx.relay, fn state ->
      put_in(state.store.engine.devices[ctx.keys["a1"]].see_focus, true)
    end)

    on_exit(fn -> Application.delete_env(:attention_server, :client_api_projection_fault) end)

    Application.put_env(:attention_server, :client_api_projection_fault, fn
      :focus_snapshot -> raise "Focus snapshot fault"
      _ -> :ok
    end)

    assert_internal_error(get(ctx, "a1", "/v2/agent/focus"))

    Application.put_env(:attention_server, :client_api_projection_fault, fn
      {:feed_event, "focus"} -> raise "Focus frame fault"
      _ -> :ok
    end)

    store = :sys.get_state(ctx.relay).store
    assert {:ok, head} = AgentChangeLog.head(store, {"A", ctx.keys["a1"]})
    conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, "a1")
    feed_path = "/v2/agent/changes?after=#{head}&session=#{@other_session}"

    assert {:ok, conn, ref} =
             Mint.HTTP.request(conn, "GET", feed_path, [{"accept", "text/event-stream"}], nil)

    feed = %{ref: ref, buffer: "", status: nil, headers: [], done: false}
    {conn, feed} = ClientAPIFeedClient.await_open(conn, feed)
    assert feed.status == 200
    {conn, feed, [event]} = ClientAPIFeedClient.events(conn, feed, 1)
    assert event == %{event: "closing", data: %{"reason" => "internal_error"}}
    {conn, feed} = ClientAPIFeedClient.drain(conn, feed)
    assert feed.done
    Mint.HTTP.close(conn)

    Application.delete_env(:attention_server, :client_api_projection_fault)
    assert get(ctx, "a1", "/v2/agent/focus").status == 200
    assert Server.relay(ctx.server) == ctx.relay
  end

  test "me reports the enrolled key's sender names", ctx do
    response = get(ctx, "a1", "/v2/me")
    assert response.status == 200
    assert {:ok, body} = JSON.decode_line(response.body <> "\n")
    errors = ClientAPISchema.errors(ctx.defs, "Me", body)
    assert errors -- ["Me: undeclared principal", "Me: undeclared server"] == []
    assert body["sender_names"] == ["app:test"]
  end

  test "reads the selected historical revision and hides it from other principals", ctx do
    id = forward(ctx, "a1", 1, @first_session)

    frame(ctx, "a1", %{
      "type" => "conversation_revise",
      "op_id" => op(2),
      "sender_id" => "app:test",
      "message_id" => "item-1",
      "base_revision" => 1,
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => "item-1",
        "title" => "Revised",
        "created_at" => 1
      }
    })

    path = "/v2/agent/messages/" <> id <> "/revisions/"
    original = get(ctx, "a1", path <> "1")
    assert original.status == 200

    assert %{"revision" => %{"request_revision" => 1, "message" => %{"title" => "Message 1"}}} =
             contract(ctx, "AgentRevisionResponse", original)

    assert %{"revision" => %{"request_revision" => 2, "message" => %{"title" => "Revised"}}} =
             contract(ctx, "AgentRevisionResponse", get(ctx, "a1", path <> "2"))

    assert get(ctx, "a1", path <> "3").status == 404
    assert get(ctx, "a2", path <> "1").status == 404
  end

  describe "agent message writes" do
    @describetag :agent_message_writes

    test "send creates one server-issued item and only successes consume a key", ctx do
      path = "/v2/agent/messages"

      body = %{
        "sender" => "app:test",
        "session" => @first_session,
        "message" => %{"title" => "Sent over v2", "created_at" => 1}
      }

      invalid = put_in(body["message"]["message_id"], "caller-id")
      assert request(ctx, "a1", "POST", path, invalid, 1).status == 400

      sent = request(ctx, "a1", "POST", path, body, 1)
      assert sent.status == 200
      assert %{"message" => message} = contract(ctx, "AgentWriteResult", sent)
      assert message["item_id"]
      refute Map.has_key?(message, "legacy_message_id")

      replay = request(ctx, "a1", "POST", path, body, 1)
      assert replay.status == 200

      assert List.keyfind(replay.headers, "idempotent-replayed", 0) ==
               {"idempotent-replayed", "true"}

      assert replay.body == sent.body

      changed = put_in(body["message"]["title"], "Different intent")
      assert_error(request(ctx, "a1", "POST", path, changed, 1), 409, "idempotency_conflict")

      other_key = request(ctx, "a2", "POST", path, body, 1)
      assert other_key.status == 200

      refute contract(ctx, "AgentWriteResult", other_key)["message"]["item_id"] ==
               message["item_id"]
    end

    test "revise uses If-Match, advances revision, and replays before checking it", ctx do
      id = forward(ctx, "a1", 1, @first_session)
      path = "/v2/agent/messages/" <> id
      body = %{"message" => %{"title" => "Updated", "created_at" => 1}}

      assert_error(request(ctx, "a1", "PUT", path, body, 2), 412, "precondition_failed")

      assert_error(
        request(ctx, "a1", "PUT", path, body, 2, [{"if-match", ~s("2")}]),
        412,
        "precondition_failed"
      )

      headers = [{"if-match", ~s("1")}]
      revised = request(ctx, "a1", "PUT", path, body, 2, headers)
      assert revised.status == 200

      assert %{"message" => %{"revision" => 2, "message" => %{"title" => "Updated"}}} =
               contract(ctx, "AgentWriteResult", revised)

      replay = request(ctx, "a1", "PUT", path, body, 2, headers)
      assert replay.status == 200
      assert replay.body == revised.body
      assert_error(request(ctx, "a2", "PUT", path, body, 3, headers), 404, "not_found")
    end

    @tag :revision_attachment_render
    test "correlated revise keeps, carries, and clears current attachments", ctx do
      sent =
        frame(ctx, "a1", %{
          "type" => "forward",
          "op_id" => op(1),
          "session_handle" => @first_session,
          "message" => %{
            "sender_id" => "app:test",
            "message_id" => "inline",
            "title" => "Original",
            "created_at" => 1
          },
          "files" => [%{"name" => "inline.txt", "data_base64" => Base.encode64("fixture")}]
        })

      id = Resources.item_id(%{accepted_seq: sent["seq"]})
      path = "/v2/agent/messages/" <> id
      entry_id = person_reply(ctx, id, 2)
      message = %{"title" => "Updated", "created_at" => 1}
      body = %{"message" => message, "in_reply_to" => entry_id}

      revised = request(ctx, "a1", "PUT", path, body, 3, [{"if-match", "1"}])
      assert revised.status == 200

      assert %{
               "message" => %{"files" => [%{"file_id" => file}]},
               "entry" => %{"entry_id" => ^entry_id, "delivery" => %{"acted" => true}}
             } = contract(ctx, "AgentWriteResult", revised)

      assert get(ctx, "a1", "/v2/files/" <> file).body == "fixture"
      follow_up = person_reply(ctx, id, 4)
      entries = get(ctx, "a2", "/v2/items/" <> id <> "/entries")
      assert entries.status == 200

      person_entries = contract(ctx, "EntriesPage", entries)["entries"]

      assert Enum.any?(person_entries, &match?(%{"entry_id" => ^follow_up}, &1))

      assert Enum.any?(
               person_entries,
               &match?(%{"kind" => "revision", "files" => [%{"file_id" => ^file}]}, &1)
             )

      body = %{"message" => %{"title" => "Carried", "created_at" => 1}, "file_ids" => [file]}
      carried = request(ctx, "a1", "PUT", path, body, 5, [{"if-match", "2"}])
      assert carried.status == 200

      assert %{"message" => %{"files" => [%{"name" => "inline.txt"}]}} =
               contract(ctx, "AgentWriteResult", carried)

      body = %{"message" => %{"title" => "Cleared", "created_at" => 1}, "file_ids" => []}
      cleared = request(ctx, "a1", "PUT", path, body, 6, [{"if-match", "3"}])
      assert %{"message" => %{"files" => []}} = contract(ctx, "AgentWriteResult", cleared)
    end

    test "a correlated revise refuses an already acted turn without changing the revision", ctx do
      id = forward(ctx, "a1", 1, @first_session)
      entry_id = person_reply(ctx, id, 2)
      path = "/v2/agent/messages/" <> id

      assert request(
               ctx,
               "a1",
               "POST",
               path <> "/replies",
               %{"text" => "Acted", "in_reply_to" => entry_id},
               3
             ).status == 200

      body = %{"message" => %{"title" => "Refused", "created_at" => 1}, "in_reply_to" => entry_id}

      assert_error(
        request(ctx, "a1", "PUT", path, body, 4, [{"if-match", "1"}]),
        409,
        "already_closed"
      )

      assert %{"message" => %{"revision" => 1, "message" => %{"title" => "Message 1"}}} =
               contract(ctx, "SentMessageResponse", get(ctx, "a1", path))
    end

    test "large write responses stay out of durable receipts and off the relay", ctx do
      alias AttentionServer.ClientAPI.Idempotency
      alias AttentionServer.LargeData
      alias Exqlite.Sqlite3

      title = String.duplicate("large-title-", div(LargeData.inline_limit(), 12) + 1)

      %{"seq" => seq} =
        frame(ctx, "a1", %{
          "type" => "forward",
          "op_id" => op(1),
          "session_handle" => @first_session,
          "message" => %{
            "sender_id" => "app:test",
            "message_id" => "large-title",
            "title" => title,
            "created_at" => 1
          }
        })

      state = :sys.get_state(ctx.relay)
      record = state.store.engine.messages[{"A", "app:test", "large-title"}]
      assert %LargeData.Ref{} = record.message["title"]
      path = "/v2/agent/messages/" <> Resources.item_id(%{accepted_seq: seq}) <> "/lifecycle"
      body = %{"action" => "expired"}
      trace = {LargeData, :read!, 1}
      relay = ctx.relay
      :erlang.trace_pattern(trace, true, [])
      :erlang.trace(relay, true, [:call])

      try do
        first = request(ctx, "a1", "POST", path, body, 2)
        assert first.status == 200

        assert %{"message" => %{"message" => %{"title" => ^title}}} =
                 contract(ctx, "AgentMessageResult", first)

        replay = request(ctx, "a1", "POST", path, body, 2)
        assert replay.status == 200
        assert replay.body == first.body
        assert {"idempotent-replayed", "true"} in replay.headers

        delivered = :erlang.trace_delivered(relay)
        assert_receive {:trace_delivered, ^relay, ^delivered}, 1_000
        refute_received {:trace, ^relay, :call, {LargeData, :read!, [_]}}

        assert {:ok, stmt} =
                 Sqlite3.prepare(
                   state.store.db,
                   "SELECT length(body) FROM client_idempotency WHERE person_id = ? AND idempotency_key = ?"
                 )

        try do
          assert :ok =
                   Sqlite3.bind(stmt, [Idempotency.agent_scope("A", ctx.keys["a1"]), op(2)])

          assert {:row, [bytes]} = Sqlite3.step(state.store.db, stmt)
          assert bytes < 1_024
        after
          Sqlite3.release(state.store.db, stmt)
        end
      after
        :erlang.trace(relay, false, [:call])
        :erlang.trace_pattern(trace, false, [])
      end
    end

    test "a full success receipt from before compact storage still replays", ctx do
      alias AttentionServer.ClientAPI.Idempotency

      id = forward(ctx, "a1", 1, nil)
      path = "/v2/agent/messages/" <> id <> "/lifecycle"
      body = %{"action" => "expired"}
      first = request(ctx, "a1", "POST", path, body, 2)
      assert first.status == 200

      state = :sys.get_state(ctx.relay)
      scope = Idempotency.agent_scope("A", ctx.keys["a1"])

      digest =
        Idempotency.digest("POST", ["v2", "agent", "messages", id, "lifecycle"], %{
          "body" => body,
          "if_match" => []
        })

      assert :ok = Idempotency.save(state.store, scope, op(3), digest, 200, decode_response(first))

      replay = request(ctx, "a1", "POST", path, body, 3)
      assert replay.status == 200
      assert replay.body == first.body
      assert {"idempotent-replayed", "true"} in replay.headers
    end

    test "lifecycle stamps the transition and already_closed applies to a repeat", ctx do
      id = forward(ctx, "a1", 1, nil)
      path = "/v2/agent/messages/" <> id <> "/lifecycle"

      assert_error(
        request(ctx, "a1", "POST", path, %{"action" => "retracted"}, 2),
        400,
        "invalid_field"
      )

      response = request(ctx, "a1", "POST", path, %{"action" => "cleared"}, 2)
      assert response.status == 200

      assert %{"message" => %{"terminal" => %{"state" => "cleared", "at_ms" => at_ms}}} =
               contract(ctx, "AgentWriteResult", response)

      assert abs(System.system_time(:millisecond) - at_ms) < 60_000

      assert_error(
        request(ctx, "a1", "POST", path, %{"action" => "expired"}, 3),
        409,
        "already_closed"
      )
    end

    test "lifecycle refuses a retracted item with the closed transition error", ctx do
      id = forward(ctx, "a1", 1, @first_session)
      item = "/v2/agent/messages/" <> id
      assert request(ctx, "a1", "POST", item <> "/retract", %{}, 2).status == 200

      assert_error(
        request(ctx, "a1", "POST", item <> "/lifecycle", %{"action" => "expired"}, 3),
        409,
        "already_closed"
      )

      assert_error(request(ctx, "a1", "POST", item <> "/retract", %{}, 4), 409, "already_retracted")
    end

    test "can retract a message without a session", ctx do
      id = forward(ctx, "a1", 1, nil)
      item = "/v2/agent/messages/" <> id
      response = request(ctx, "a1", "POST", item <> "/retract", %{}, 2)
      assert response.status == 200

      assert %{"message" => %{"terminal" => %{"state" => "retracted"}}} =
               contract(ctx, "AgentRetractResult", response)
    end

    test "correlated retract marks the person's turn acted without recording handoff", ctx do
      id = forward(ctx, "a1", 1, @first_session)
      entry_id = person_reply(ctx, id, 2)
      path = "/v2/agent/messages/" <> id <> "/retract"
      body = %{"in_reply_to" => entry_id}
      assert_error(request(ctx, "a2", "POST", path, body, 3), 404, "not_found")

      response = request(ctx, "a1", "POST", path, body, 3)
      assert response.status == 200

      assert %{
               "message" => %{"terminal" => %{"state" => "retracted"}},
               "entry" => %{"entry_id" => ^entry_id, "delivery" => %{"acted" => true} = delivery}
             } = contract(ctx, "AgentWriteResult", response)

      refute Map.has_key?(delivery, "outcome")
      assert_error(request(ctx, "a1", "POST", path, %{}, 4), 409, "already_retracted")
    end

    test "plain and repeated correlated replies are accepted on an answered conversation", ctx do
      id = forward(ctx, "a1", 1, @first_session)
      received = person_reply(ctx, id, 2)
      item = "/v2/agent/messages/" <> id

      answer =
        request(ctx, "a2", "POST", "/v2/items/" <> id <> "/answer", %{"request_revision" => 1}, 3)

      assert answer.status == 200

      for {n, body} <- [
            {4, %{"text" => "Plain"}},
            {5, %{"text" => "Correlated", "in_reply_to" => received}},
            {6, %{"text" => "Follow-up", "in_reply_to" => received}}
          ] do
        response = request(ctx, "a1", "POST", item <> "/replies", body, n)
        assert response.status == 200

        assert %{"entry" => %{"kind" => "reply", "author" => "origin"}} =
                 contract(ctx, "AgentWriteResult", response)
      end

      sessionless = forward(ctx, "a1", 7, nil)

      assert_error(
        request(
          ctx,
          "a1",
          "POST",
          "/v2/agent/messages/" <> sessionless <> "/replies",
          %{"text" => "No chat"},
          8
        ),
        400,
        "unsupported"
      )
    end
  end

  describe "rendering an agent revision with a v2 attachment" do
    @describetag :revision_attachment_render

    setup ctx do
      sent =
        request(
          ctx,
          "a1",
          "POST",
          "/v2/agent/messages",
          %{
            "sender" => "app:test",
            "session" => @first_session,
            "message" => %{"title" => "Original", "created_at" => 1}
          },
          1
        )

      assert %{"message" => %{"item_id" => id}} = contract(ctx, "AgentWriteResult", sent)
      turn = person_reply(ctx, id, 2)
      staged = upload(ctx, "a1", 3, "revision attachment")
      assert %{"file_id" => handle} = contract(ctx, "FileCreated", staged)

      revised =
        request(
          ctx,
          "a1",
          "PUT",
          "/v2/agent/messages/" <> id,
          %{
            "message" => %{"title" => "Revised", "created_at" => 1},
            "file_ids" => [handle],
            "in_reply_to" => turn
          },
          4,
          [{"if-match", "1"}]
        )

      assert revised.status == 200

      assert %{"message" => %{"files" => [file]}, "entry" => %{"entry_id" => ^turn}} =
               contract(ctx, "AgentWriteResult", revised)

      %{revised_item: id, revision_file: file}
    end

    test "a person reply succeeds after the revision", ctx do
      assert is_binary(person_reply(ctx, ctx.revised_item, 5))
    end

    test "the person entries read renders the revision attachment", ctx do
      id = ctx.revised_item
      response = get(ctx, "a2", "/v2/items/" <> id <> "/entries")
      assert response.status == 200
      entries = contract(ctx, "EntriesPage", response)["entries"]
      assert_revision_attachment(entries, ctx)

      assert get(ctx, "a2", "/v2/files/" <> ctx.revision_file["file_id"]).body ==
               "revision attachment"
    end

    test "the person JSON batch includes the revision attachment", ctx do
      conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, "a2")
      {conn, status, batch} = ClientAPIFeedClient.batch(conn, "?after=0")
      Mint.HTTP.close(conn)
      assert status == 200

      entries = for %{"kind" => "entry", "data" => entry} <- batch["changes"], do: entry
      assert_revision_attachment(entries, ctx)
    end

    test "the person SSE backlog includes the revision attachment", ctx do
      # A later, independent item provides a cursor the stream can reach
      # even if the revision entry was dropped.
      forward(ctx, "a2", 5, @other_session)
      store = :sys.get_state(ctx.relay).store
      assert {:ok, latest} = ChangeLog.head(store, "A")

      conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, "a2")
      {conn, feed} = ClientAPIFeedClient.open_feed(conn, "?after=0")
      {conn, feed} = ClientAPIFeedClient.await_open(conn, feed)
      assert feed.status == 200
      {conn, _feed, changes} = ClientAPIFeedClient.changes_through(conn, feed, latest)
      Mint.HTTP.close(conn)

      entries = for %{event: "entry", data: entry} <- changes, do: entry
      assert_revision_attachment(entries, ctx)
    end

    test "the agent entries read and batch render the same revision attachment", ctx do
      path = "/v2/agent/messages/" <> ctx.revised_item <> "/entries"
      response = get(ctx, "a1", path)
      assert response.status == 200
      assert_revision_attachment(contract(ctx, "AgentEntriesPage", response)["entries"], ctx)

      batch = get(ctx, "a1", "/v2/agent/changes?after=0", [{"accept", "application/json"}])
      assert batch.status == 200
      changes = Elixir.JSON.decode!(batch.body)["changes"]
      entries = for %{"kind" => "entry", "data" => entry} <- changes, do: entry

      assert_revision_attachment(entries, ctx)
    end
  end

  defp assert_revision_attachment(entries, %{revised_item: id, revision_file: file}) do
    assert Enum.any?(
             entries,
             &match?(
               %{"kind" => "revision", "item_id" => ^id, "request_revision" => 2, "files" => [^file]},
               &1
             )
           )
  end

  describe "agent results, receipts and session end" do
    @describetag :agent_receipts

    test "the message receipt and result require a session and an answer in that order", ctx do
      for {n, session, status, token} <- [
            {1, nil, 400, "unsupported"},
            {2, @first_session, 409, "not_answered"}
          ] do
        id = forward(ctx, "a1", n, session)
        path = "/v2/agent/messages/" <> id

        assert_error(
          request(ctx, "a1", "POST", path <> "/receipt", %{"outcome" => "handed"}, n + 2),
          status,
          token
        )

        assert_error(
          request(ctx, "a1", "POST", path <> "/result", %{"status" => "received"}, n + 4),
          status,
          token
        )
      end
    end

    test "after a registered agent's answer the sender's receipt and result are unsupported",
         ctx do
      der = fn name -> TLSFixtures.certificate_der!(ctx.certs, name) end

      for name <- ~w(ga-owner ga-sender ga-agent),
          do: TLSFixtures.make_self_signed_client!(ctx.certs, name)

      {:ok, person} = Relay.operator_bootstrap(ctx.relay, der.("ga-owner"))
      {:ok, owner} = AttentionServer.DeviceIdentity.from_certificate(der.("ga-owner"))
      {:ok, sender} = Relay.setup_device_certificate(ctx.relay, person, der.("ga-sender"))
      assert :ok = Relay.setup_sender(ctx.relay, sender, "app:test")
      {:ok, agent} = AttentionServer.DeviceIdentity.from_certificate(der.("ga-agent"))

      assert :ok =
               Relay.owner_register_agent(
                 ctx.relay,
                 person,
                 owner,
                 "00000000-0000-4000-8000-0000000000a1",
                 agent
               )

      assert :ok =
               Relay.owner_replace_agent_grants(
                 ctx.relay,
                 person,
                 owner,
                 agent,
                 1,
                 ["app:test"],
                 [
                   "app:test"
                 ]
               )

      line =
        JSON.encode!(%{
          "v" => 1,
          "type" => "forward",
          "op_id" => op(21),
          "session_handle" => @first_session,
          "message" => %{
            "sender_id" => "app:test",
            "message_id" => "granted-answer",
            "title" => "Granted",
            "created_at" => 21
          }
        }) <> "\n"

      assert {:ok, reply} = Relay.request_line(ctx.relay, sender, line)
      assert {:ok, %{"type" => "ok", "seq" => seq}} = JSON.decode_line(reply)
      id = Resources.item_id(%{accepted_seq: seq})

      answer =
        request(
          ctx,
          "ga-agent",
          "POST",
          "/v2/items/" <> id <> "/answer",
          %{"request_revision" => 1},
          22
        )

      assert answer.status == 200
      path = "/v2/agent/messages/" <> id

      # The item is answered, so not_answered does not apply; a receipt and a
      # result follow only a person's answer (contract addendum 8).
      assert_error(
        request(ctx, "ga-sender", "POST", path <> "/receipt", %{"outcome" => "handed"}, 23),
        400,
        "unsupported"
      )

      assert_error(
        request(ctx, "ga-sender", "POST", path <> "/result", %{"status" => "received"}, 24),
        400,
        "unsupported"
      )
    end

    test "answer receipt ensures delivery and a result marks the answer acted", ctx do
      id = forward(ctx, "a1", 1, @first_session)
      item = "/v2/agent/messages/" <> id

      answer =
        request(
          ctx,
          "a2",
          "POST",
          "/v2/items/" <> id <> "/answer",
          %{"request_revision" => 1, "comment" => "Person answer"},
          2
        )

      assert answer.status == 200

      receipt =
        request(
          ctx,
          "a1",
          "POST",
          item <> "/receipt",
          %{"outcome" => "handed", "agent" => "fixture"},
          3
        )

      assert receipt.status == 200

      assert %{
               "status" => "recorded",
               "stage_outcome" => "handed",
               "message" => %{
                 "terminal" => %{"delivery" => %{"delivered" => true, "agent" => "fixture"}}
               }
             } = contract(ctx, "AgentReceiptResult", receipt)

      result = request(ctx, "a1", "POST", item <> "/result", %{"status" => "applied"}, 4)
      assert result.status == 200

      assert %{
               "message" => %{
                 "result" => %{"status" => "applied"},
                 "terminal" => %{"delivery" => %{"acted" => true}}
               },
               "entry" => %{"kind" => "result"}
             } = contract(ctx, "AgentWriteResult", result)

      assert_error(
        request(ctx, "a1", "POST", item <> "/result", %{"status" => "received"}, 5),
        409,
        "already_closed"
      )
    end

    test "chat receipt applies only to the scoped person turn", ctx do
      id = forward(ctx, "a1", 1, @first_session)
      entry = person_reply(ctx, id, 2)
      path = "/v2/agent/messages/" <> id <> "/entries/" <> entry <> "/receipt"
      body = %{"outcome" => "not_handed", "reason" => "Fixture declined"}
      assert_error(request(ctx, "a2", "POST", path, body, 3), 404, "not_found")

      first = request(ctx, "a1", "POST", path, body, 3)
      assert first.status == 200

      assert %{"entry" => %{"delivery" => %{"delivered" => true, "outcome" => "not_handed"}}} =
               contract(ctx, "AgentReceiptResult", first)

      rescued = request(ctx, "a1", "POST", path, %{"outcome" => "handed", "rerouted" => true}, 4)

      assert %{"status" => "recorded", "stage_outcome" => "handed"} =
               contract(ctx, "AgentReceiptResult", rescued)

      already = request(ctx, "a1", "POST", path, body, 5)

      assert %{"status" => "superseded", "stage_outcome" => "handed"} =
               contract(ctx, "AgentReceiptResult", already)
    end

    test "ending a session is scoped and leaves the conversation open", ctx do
      id = forward(ctx, "a1", 1, @first_session)
      path = "/v2/agent/sessions/" <> @first_session <> "/end"
      assert_error(request(ctx, "a2", "POST", path, %{}, 2), 404, "not_found")
      ended = request(ctx, "a1", "POST", path, %{}, 2)
      assert ended.status == 200
      item = "/v2/agent/messages/" <> id

      assert %{"message" => %{"availability" => %{"state" => "gone"}}} =
               contract(ctx, "SentMessageResponse", get(ctx, "a1", item))

      assert request(ctx, "a1", "POST", item <> "/replies", %{"text" => "After end"}, 3).status ==
               200
    end

    test "ending a v2 session projects gone without adding history", ctx do
      body = %{
        "sender" => "app:test",
        "session" => @first_session,
        "message" => %{"title" => "Open", "created_at" => 1}
      }

      sent = request(ctx, "a1", "POST", "/v2/agent/messages", body, 1)
      id = contract(ctx, "AgentSendResult", sent)["message"]["item_id"]
      other = forward(ctx, "a1", 2, @other_session)
      path = "/v2/agent/sessions/" <> @first_session <> "/end"

      assert request(ctx, "a1", "POST", path, %{}, 3).status == 200
      assert request(ctx, "a1", "POST", path, %{}, 4).status == 200

      entries = fn item ->
        route = "/v2/agent/messages/" <> item <> "/entries"
        contract(ctx, "AgentEntriesPage", get(ctx, "a1", route))["entries"]
      end

      assert %{"message" => %{"availability" => %{"state" => "gone"}}} =
               contract(ctx, "SentMessageResponse", get(ctx, "a1", "/v2/agent/messages/#{id}"))

      assert entries.(id) == []
      assert entries.(other) == []
    end
  end

  describe "agent files" do
    @describetag :agent_files

    test "upload handles are scoped, attach to file-only replies, and give distinct download IDs",
         ctx do
      id = forward(ctx, "a1", 1, @first_session)
      characters = "file contents"
      upload = upload(ctx, "a1", 2, characters)
      assert upload.status == 201
      handle = contract(ctx, "FileCreated", upload)["file_id"]
      path = "/v2/files/" <> handle
      assert_error(get(ctx, "a1", path), 404, "not_found")

      :sys.replace_state(ctx.relay, fn state ->
        update_in(state.store.engine.sender_grants, &Map.delete(&1, {"A", ctx.keys["a2"]}))
      end)

      assert_error(request(ctx, "a2", "DELETE", path, %{}, 3), 404, "not_found")

      sent =
        request(
          ctx,
          "a1",
          "POST",
          "/v2/agent/messages/" <> id <> "/replies",
          %{"file_ids" => [handle]},
          4
        )

      assert sent.status == 200

      assert %{"entry" => %{"files" => [%{"file_id" => download}]}} =
               contract(ctx, "AgentWriteResult", sent)

      refute handle == download
      file = get(ctx, "a1", "/v2/files/" <> download)
      assert file.status == 200
      assert file.body == characters
      assert_error(request(ctx, "a1", "DELETE", path, %{}, 5), 409, "already_closed")
    end

    test "shared file idempotency is person scoped across device keys", ctx do
      first = upload(ctx, "a1", 1, "first")
      assert first.status == 201
      assert_error(upload(ctx, "a2", 1, "different"), 409, "idempotency_conflict")
      assert upload(ctx, "b1", 1, "different account").status == 201

      first_id = contract(ctx, "FileCreated", first)["file_id"]
      second = upload(ctx, "a2", 2, "second")
      second_id = contract(ctx, "FileCreated", second)["file_id"]

      assert request(ctx, "a1", "DELETE", "/v2/files/" <> first_id, %{}, 3).status == 204

      assert_error(
        request(ctx, "a2", "DELETE", "/v2/files/" <> second_id, %{}, 3),
        409,
        "idempotency_conflict"
      )

      assert request(ctx, "a2", "DELETE", "/v2/files/" <> second_id, %{}, 4).status == 204
    end

    test "a same-account device can discard a handle staged before ownership tracking", ctx do
      test_pid = self()

      :sys.replace_state(ctx.relay, fn state ->
        alias AttentionServer.ClientAPI.Uploads

        {:ok, dir} = Uploads.dir(state.store)
        path = Path.join(dir, "pre-owner-fixture")
        File.write!(path, "fixture")
        attrs = %{
          name: "legacy.txt",
          content_type: "text/plain",
          size: 7,
          sha256: :crypto.hash(:sha256, "fixture") |> Base.encode16(case: :lower)
        }

        {:ok, handle} = Uploads.stage(state.store, "A", attrs, path)
        send(test_pid, {:pre_owner_handle, handle})
        state
      end)

      assert_receive {:pre_owner_handle, handle}
      path = "/v2/files/" <> handle
      assert_error(request(ctx, "b1", "DELETE", path, %{}, 1), 404, "not_found")
      assert request(ctx, "a2", "DELETE", path, %{}, 1).status == 204
    end

    test "file-use preserves transfer failure, duplicate and conflict outcomes", ctx do
      item_id = forward(ctx, "a1", 1, @first_session)
      item = "/v2/agent/messages/" <> item_id
      handle = contract(ctx, "FileCreated", upload(ctx, "a2", 2, "person attachment"))["file_id"]

      reply =
        request(
          ctx,
          "a2",
          "POST",
          "/v2/items/" <> item_id <> "/replies",
          %{"text" => "File for the agent", "file_ids" => [handle]},
          3
        )

      assert %{"entry" => %{"entry_id" => entry, "files" => [%{"file_id" => file}]}} =
               contract(ctx, "WriteResult", reply)

      turn = item <> "/entries/" <> entry
      path = turn <> "/file-use"
      failed = %{"file_id" => file, "transfer" => "not_transferred", "reason" => "Fixture"}
      assert_error(request(ctx, "a1", "POST", path, failed, 4), 404, "not_found")

      assert request(ctx, "a1", "POST", turn <> "/receipt", %{"outcome" => "handed"}, 5).status ==
               200

      response = request(ctx, "a1", "POST", path, failed, 4)
      assert response.status == 200

      assert %{"file_id" => ^file, "transfer" => "not_transferred"} =
               decode_response(response)

      assert %{"duplicate" => true} =
               decode_response(request(ctx, "a1", "POST", path, failed, 6))

      usable = request(ctx, "a1", "POST", path, %{"file_id" => file, "use" => "used"}, 7)
      assert usable.status == 200

      assert %{
               "use" => "used",
               "entry" => %{
                 "delivery" => %{
                   "file_uses" => [
                     %{"file_id" => ^file, "transfer" => "not_transferred", "use" => "used"}
                   ]
                 }
               }
             } = decode_response(usable)

      conflict = %{"file_id" => file, "transfer" => "transferred", "use" => "used"}
      assert_error(request(ctx, "a1", "POST", path, conflict, 8), 409, "already_closed")
    end

    test "the uploader can delete an unattached upload, including idempotent retry", ctx do
      assert %{"file_id" => handle} =
               contract(ctx, "FileCreated", upload(ctx, "a1", 1, "discard"))

      path = "/v2/files/" <> handle
      first = request(ctx, "a1", "DELETE", path, %{}, 2)
      assert first.status == 204
      assert first.body == ""
      replay = request(ctx, "a1", "DELETE", path, %{}, 2)
      assert replay.status == 204

      assert List.keyfind(replay.headers, "idempotent-replayed", 0) ==
               {"idempotent-replayed", "true"}

      assert_error(request(ctx, "a1", "DELETE", path, %{}, 3), 404, "not_found")
    end
  end

  # Two staged files with the same bytes under one name attach as one upload
  # twice. Today's refusal names no field, and no message is sent.
  test "a send naming two staged files under one name is refused", ctx do
    handles =
      for n <- [1, 2] do
        assert %{"file_id" => handle} =
                 contract(ctx, "FileCreated", upload(ctx, "a1", n, "same bytes"))

        handle
      end

    refute Enum.at(handles, 0) == Enum.at(handles, 1)

    body = %{
      "sender" => "app:test",
      "session" => @first_session,
      "message" => %{"title" => "Twins", "created_at" => 1},
      "file_ids" => handles
    }

    refused = request(ctx, "a1", "POST", "/v2/agent/messages", body, 3)
    assert refused.status == 400
    assert {:ok, %{"error" => "invalid_field"} = error} = JSON.decode_line(refused.body <> "\n")
    refute Map.has_key?(error, "field")
    assert :sys.get_state(ctx.relay).store.engine.messages == %{}
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

  @tag :agent_advisory
  test "acknowledges the retained advisory of the scoped sent message", ctx do
    alias AttentionServer.{BounceDecision, BouncePolicy, Store}

    :sys.replace_state(ctx.relay, fn state ->
      at = state.clock.()
      levels = [%{"name" => "Urgent", "lower_bound" => 0, "bounce_after_days" => 1}]
      {:ok, normalized} = BouncePolicy.normalize_levels(levels)

      {:ok, configured, _} =
        BounceDecision.configure_levels(state.store.engine, "A", normalized, at)

      {:ok, store} =
        Store.commit(state.store, configured, %{
          person: "A",
          actor: "fixture",
          action: "configure_fixture"
        })

      %{state | store: store}
    end)

    id =
      frame(ctx, "a1", %{
        "type" => "forward",
        "op_id" => op(1),
        "session_handle" => @first_session,
        "route" => "claude-code",
        "cli_version" => "fixture",
        "unsupported_reason" => nil,
        "advisory_v1" => true,
        "message" => %{
          "sender_id" => "app:test",
          "message_id" => "advisory-item",
          "title" => "Needs attention",
          "message_type" => "fyi",
          "answer_ui" => %{
            "schema" => "attention-client.actions/1",
            "actions" => [
              %{
                "id" => "seen",
                "label" => "Seen",
                "kind" => "value",
                "value" => %{"acknowledgment" => "seen"}
              }
            ]
          },
          "created_at" => 1
        }
      })
      |> then(&Resources.item_id(%{accepted_seq: &1["seq"]}))

    key = {"A", "app:test", "advisory-item"}

    state =
      :sys.replace_state(ctx.relay, fn state ->
        engine = state.store.engine
        at = engine.messages[key].accepted_at

        selected =
          BounceDecision.record_scores(
            engine,
            "A",
            [
              %{"sender_id" => "app:test", "message_id" => "advisory-item", "score" => 0.9}
            ],
            at
          )

        next = BounceDecision.evaluate_due(selected, at + 86_400_001)

        {:ok, store} =
          Store.commit(state.store, next, %{
            person: "A",
            actor: "fixture",
            action: "advisory_fixture"
          })

        %{state | store: store}
      end)

    feed = state.store.engine.advisory_feeds[{"A", ctx.keys["a1"], @first_session}]
    assert %{"advisory_id" => advisory_id} = feed.entries[1]
    path = "/v2/agent/messages/" <> id <> "/advisories/ack"
    body = %{"advisory_id" => advisory_id}
    assert_error(request(ctx, "a2", "POST", path, body, 2), 404, "not_found")
    first = request(ctx, "a1", "POST", path, body, 2)
    assert first.status == 200

    assert %{"delivered" => true, "already_delivered" => false} =
             contract(ctx, "AgentAdvisoryAckResult", first)

    assert %{"already_delivered" => true} =
             contract(ctx, "AgentAdvisoryAckResult", request(ctx, "a1", "POST", path, body, 3))
  end

  defp normalize_delivery_replay(change) do
    change
    |> Map.delete("rev")
    |> update_in(["data"], &Map.delete(&1, "rev"))
    |> update_in(["data", "delivery"], &Map.delete(&1, "delivered"))
  end

  defp person_chat(ctx, path, id) do
    %{"entries" => entries} = decode_response(get(ctx, "a2", path))
    Enum.find(entries, &(&1["entry_id"] == id))
  end

  defp next_delivery_page(ctx, path, page, headers) do
    page_path = path <> "&page=" <> URI.encode_www_form(page)
    response = get(ctx, "a1", page_path, headers)
    assert response.status == 200

    case decode_response(response) do
      %{"changes" => [], "next_page" => next} -> next_delivery_page(ctx, path, next, headers)
      body -> {page_path, body}
    end
  end

  defp person_reply(ctx, id, n) do
    path = "/v2/items/" <> id <> "/replies"
    response = request(ctx, "a2", "POST", path, %{"text" => "Person turn"}, n)
    assert response.status == 200
    contract(ctx, "WriteResult", response)["entry"]["entry_id"]
  end

  defp assert_error(response, status, token) do
    assert response.status == status
    assert {:ok, %{"error" => ^token}} = JSON.decode_line(response.body <> "\n")
  end

  defp request(ctx, name, method, path, body, n, extra_headers \\ []) do
    headers = [{"content-type", "application/json"}, {"idempotency-key", op(n)} | extra_headers]
    conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, name)
    assert {:ok, conn, ref} = Mint.HTTP.request(conn, method, path, headers, JSON.encode!(body))
    {conn, response} = ClientAPIFeedClient.collect(conn, ref)
    Mint.HTTP.close(conn)
    response
  end

  defp forward(ctx, name, n, session) do
    %{"seq" => seq} =
      frame(ctx, name, %{
        "type" => "forward",
        "op_id" => op(n),
        "session_handle" => session,
        "message" => %{
          "sender_id" => "app:test",
          "message_id" => "item-#{n}",
          "title" => "Message #{n}",
          "created_at" => n
        }
      })

    Resources.item_id(%{accepted_seq: seq})
  end

  defp frame(ctx, name, payload) do
    line = JSON.encode!(Map.put(payload, "v", 1)) <> "\n"
    assert {:ok, response} = Relay.request_line(ctx.relay, ctx.keys[name], line)
    assert {:ok, %{"type" => "ok"} = decoded} = JSON.decode_line(response)
    decoded
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp get(ctx, name, path, headers \\ []) do
    conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, name)
    assert {:ok, conn, ref} = Mint.HTTP.request(conn, "GET", path, headers, nil)
    {conn, response} = ClientAPIFeedClient.collect(conn, ref)
    Mint.HTTP.close(conn)
    response
  end

  defp decode_response(response) do
    assert {:ok, body} = JSON.decode_line(response.body <> "\n")
    body
  end

  defp contract(ctx, schema, response) do
    assert {:ok, body} = JSON.decode_line(response.body <> "\n")
    assert ClientAPISchema.errors(ctx.defs, schema, body) == []
    body
  end
end
