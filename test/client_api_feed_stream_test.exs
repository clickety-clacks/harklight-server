defmodule AttentionServer.ClientAPIFeedStreamTest.SubscriptionProbe do
  # A feed renderer that reports, from inside the Relay step, which processes
  # are subscribed to person A's change log at that moment.
  def render(context, principal, row) do
    case :persistent_term.get({__MODULE__, :test}, nil) do
      pid when is_pid(pid) ->
        send(pid, {:subscribed_at_read, Registry.lookup(AttentionServer.ChangeLog.Registry, "A")})

      _ ->
        :ok
    end

    AttentionServer.ClientAPI.FeedRenderer.render(context, principal, row)
  end
end

defmodule AttentionServer.ClientAPIFeedStreamTest do
  # GET /v2/changes as Server-Sent Events (docs/client-api-v2.md §6), over
  # HTTP/2 with mutual TLS, behind client_api_v2.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  import AttentionServer.ClientAPIFeedClient, except: [connect: 2, connect: 3, connect: 4]

  alias AttentionServer.{
    ChangeLog,
    ClientAPI,
    ClientAPIFeedClient,
    DeviceIdentity,
    JSON,
    Relay,
    TLSFixtures
  }

  alias AttentionServer.ClientAPI.{Principal, Reads, Writes}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @other_handle "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "harklight-sse-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    flags = Path.join(dir, "flags.json")
    File.write!(flags, ~s({"client_api_v2":true}))

    relay =
      start_supervised!(
        {Relay,
         store_path: Path.join(dir, "state.sqlite3"),
         feature_flags_path: flags,
         agent_fixture_mode: true},
        restart: :temporary
      )

    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert {:ok, reader} =
             Relay.setup_device_certificate(relay, "A", TLSFixtures.certificate_der!(certs, "a1"))

    listener =
      start_supervised!(
        {ClientAPI.Listener,
         [
           relay: relay,
           certfile: Path.join(certs, "server.pem"),
           keyfile: Path.join(certs, "server.key"),
           cacertfile: Path.join(certs, "ca.pem")
         ]},
        id: make_ref()
      )

    %{relay: relay, reader: reader, port: ClientAPI.Listener.port(listener), certs: certs}
  end

  ## Helpers

  defp op(n), do: "00000000-0000-4000-8000-" <> String.pad_leading("#{n}", 12, "0")

  defp request(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp forward(relay, id, n, title \\ nil) do
    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(n),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => id,
                 "created_at" => n,
                 "title" => title || id
               }
             })
  end

  defp discuss(%{relay: relay, reader: reader}, id, n) do
    assert %{"type" => "ok"} =
             request(relay, reader, %{
               "type" => "discussion",
               "op_id" => op(n),
               "sender_id" => "app:test",
               "message_id" => id,
               "request_revision" => 1,
               "text" => "Turn #{n}"
             })
  end

  defp connect(ctx), do: ClientAPIFeedClient.connect(ctx.port, ctx.certs, "a1")

  defp head(ctx), do: elem(ChangeLog.head(:sys.get_state(ctx.relay).store, "A"), 1)

  defp eventually(fun, tries \\ 50) do
    cond do
      fun.() ->
        true

      tries == 0 ->
        false

      true ->
        Process.sleep(20)
        eventually(fun, tries - 1)
    end
  end

  # The feeds of the reader device: the processes subscribed to its closing.
  defp feed_pids(ctx),
    do:
      for(
        {pid, _} <- Registry.lookup(ChangeLog.Registry, {:closing, {:device, ctx.reader}}),
        do: pid
      )

  # Change-log subscribers of person A (each feed's watcher).
  defp watchers, do: for({pid, _} <- Registry.lookup(ChangeLog.Registry, "A"), do: pid)

  defp principal(ctx), do: %Principal{person: "A", kind: :device, key: ctx.reader}

  # Applies events with the version rule, as a client does.
  defp apply_events(model, events) do
    Enum.reduce(events, model, fn
      %{event: "item", id: rev, data: data}, model ->
        if newer?(model.items[data["item_id"]], rev),
          do: put_in(model, [:items, data["item_id"]], data),
          else: model

      %{event: "entry", id: rev, data: data}, model ->
        if newer?(model.entries[data["entry_id"]], rev),
          do: put_in(model, [:entries, data["entry_id"]], data),
          else: model

      %{event: "counts", data: data}, model ->
        %{model | counts: data}

      _, model ->
        model
    end)
  end

  defp newer?(nil, _rev), do: true
  defp newer?(%{"rev" => held}, rev), do: rev > held

  defp snapshot(ctx) do
    assert {:ok, inbox} = Reads.inbox(ctx.relay, principal(ctx), %{"limit" => "200"})

    entries =
      for item <- inbox["items"],
          {:ok, %{"entries" => entries}} =
            Reads.entries(ctx.relay, principal(ctx), item["item_id"], %{"limit" => "200"}),
          entry <- entries,
          into: %{},
          do: {entry["entry_id"], entry}

    %{
      rev: inbox["rev"],
      items: Map.new(inbox["items"], &{&1["item_id"], &1}),
      entries: entries,
      counts: inbox["counts"]
    }
  end

  ## Backlog, then live

  test "a feed sends the backlog, then live changes, while reads and writes run beside it",
       ctx do
    forward(ctx.relay, "m1", 1)
    conn = connect(ctx)
    {conn, 200, backlog} = batch(conn, "?after=0")

    {conn, feed} = open_feed(conn, "?after=0")
    {conn, feed, first} = events(conn, feed, length(backlog["changes"]))
    assert feed.status == 200
    assert {"content-type", "text/event-stream; charset=utf-8"} in feed.headers
    assert {"cache-control", "no-cache"} in feed.headers
    assert Enum.map(first, & &1.id) == Enum.map(backlog["changes"], & &1["rev"])
    assert Enum.map(first, & &1.event) == Enum.map(backlog["changes"], & &1["kind"])
    assert Enum.map(first, & &1.data) == Enum.map(backlog["changes"], & &1["data"])

    # Parallel reads on another connection and writes, during the open feed.
    reader = connect(ctx)

    reads =
      for _ <- 1..4 do
        Task.async(fn ->
          {_conn, 200, body} = batch(connect(ctx), "?after=0")
          body
        end)
      end

    writes = Task.async(fn -> for n <- 2..6, do: forward(ctx.relay, "m#{n}", n) end)
    Task.await(writes)
    for body <- Task.await_many(reads), do: assert(body["rev"] >= backlog["rev"])

    {_reader, 200, live} = batch(reader, "?after=#{backlog["rev"]}")
    assert live["changes"] != []
    {_conn, _feed, later} = changes_through(conn, feed, live["rev"])

    # The feed saw each change as it was committed, so it also carries rows
    # the compacted log has since replaced. It holds every row the log keeps,
    # in rev order, at the same state.
    ids = Enum.map(later, & &1.id)
    assert ids == Enum.sort(ids) and ids == Enum.uniq(ids)
    by_id = Map.new(later, &{&1.id, &1})

    for change <- live["changes"] do
      assert %{event: kind, data: data} = by_id[change["rev"]]

      assert {kind, data} == {change["kind"], change["data"]}
    end
  end

  test "a cursor past retention is 410 before the stream opens", ctx do
    forward(ctx.relay, "m1", 1)

    assert {:ok, _} =
             ClientAPIFeedClient.prune_changes(ctx.relay, System.system_time(:millisecond) + 1)

    conn = connect(ctx)
    {conn, feed} = open_feed(conn, "?after=0")
    {_conn, response} = collect(conn, feed.ref, %{headers: [], body: ""})
    assert response.status == 410
    assert {:ok, %{"error" => "cursor_expired"}} = JSON.decode_line(response.body <> "\n")
    assert eventually(fn -> feed_pids(ctx) == [] and watchers() == [] end)
  end

  ## Resume

  test "Last-Event-ID resumes with no gap under concurrent writes", ctx do
    forward(ctx.relay, "m0", 0)
    conn = connect(ctx)
    {conn, feed} = open_feed(conn, "?after=0")
    {conn, feed} = await_open(conn, feed)

    # Writes keep landing while the client reads, drops and resumes.
    writer =
      Task.async(fn ->
        for n <- 1..40 do
          if rem(n, 3) == 0, do: discuss(ctx, "m0", 100 + n), else: forward(ctx.relay, "m#{n}", n)
        end
      end)

    {conn, _feed, seen} = events(conn, feed, 5)
    seen = Enum.reject(seen, &(&1[:event] == "heartbeat"))
    last_id = List.last(seen).id
    Mint.HTTP.close(conn)

    conn = connect(ctx)
    # after=0 is ignored: Last-Event-ID takes precedence.
    {conn, feed} = open_feed(conn, "?after=0", [{"last-event-id", Integer.to_string(last_id)}])
    Task.await(writer, 30_000)
    final = head(ctx)
    {_conn, _feed, resumed} = changes_through(conn, feed, final)

    ids = Enum.map(seen ++ resumed, & &1.id)
    assert ids == Enum.sort(ids) and ids == Enum.uniq(ids)
    assert hd(resumed).id > last_id

    # What the client applied from both feeds is exactly the snapshot.
    replayed = apply_events(%{items: %{}, entries: %{}, counts: nil}, seen ++ resumed)
    expected = snapshot(ctx)
    assert expected.rev == final
    assert replayed.items == expected.items
    assert replayed.entries == expected.entries
    assert replayed.counts == expected.counts
  end

  # Rank keys are stored per change (#228, #231), so a moved rank is an item
  # row and a replay reaches the snapshot's keys, also under concurrent
  # writes (above).
  test "replaying the feed reaches the snapshot's rank keys", ctx do
    for n <- 1..40, do: forward(ctx.relay, "m#{n}", n)
    conn = connect(ctx)
    {conn, feed} = open_feed(conn, "?after=0")
    {_conn, _feed, events} = changes_through(conn, feed, head(ctx))
    replayed = apply_events(%{items: %{}, entries: %{}, counts: nil}, events)
    assert replayed.items == snapshot(ctx).items
  end

  test "the mounted agent feed honors Last-Event-ID and validates session", ctx do
    conn = connect(ctx)

    {conn, result} =
      agent_batch(conn, "?after=invalid&session=#{@handle}", [{"last-event-id", "0"}])

    assert result.status == 200

    assert {:ok, %{"changes" => [], "rev" => 0, "more" => false}} =
             JSON.decode_line(result.body <> "\n")

    {conn, invalid} = agent_batch(conn, "?session=invalid", [])
    assert invalid.status == 400

    assert {:ok, %{"error" => "invalid_field", "field" => "session"}} =
             JSON.decode_line(invalid.body <> "\n")

    {_conn, expired} = agent_batch(conn, "?after=1", [])
    assert expired.status == 410
  end

  test "agent replay reaches the message snapshot after chat, a raw answer and a sessionless close",
       ctx do
    assert :ok = Relay.setup_sender(ctx.relay, ctx.reader, "app:agent")

    for {id, n, session} <- [{"chat", 1, @handle}, {"sessionless", 2, nil}] do
      frame = %{
        "type" => "forward",
        "op_id" => op(n),
        "message" => %{
          "sender_id" => "app:agent",
          "message_id" => id,
          "created_at" => n,
          "title" => id
        }
      }

      frame = if session, do: Map.put(frame, "session_handle", session), else: frame
      assert %{"type" => "ok"} = request(ctx.relay, ctx.reader, frame)
    end

    assert %{"type" => "ok"} =
             request(ctx.relay, "origin", %{
               "type" => "discussion",
               "op_id" => op(3),
               "sender_id" => "app:agent",
               "message_id" => "chat",
               "request_revision" => 1,
               "text" => "Question before answer"
             })

    assert %{"type" => "ok"} =
             request(ctx.relay, "origin", %{
               "type" => "answer",
               "op_id" => op(4),
               "sender_id" => "app:agent",
               "message_id" => "chat",
               "request_revision" => 1,
               "answer" => nil
             })

    assert %{"type" => "ok"} =
             request(ctx.relay, ctx.reader, %{
               "type" => "lifecycle",
               "op_id" => op(5),
               "sender_id" => "app:agent",
               "message_id" => "sessionless",
               "state" => "cleared",
               "at" => 6
             })

    {conn, first} = agent_batch(connect(ctx), "?after=0", [])
    assert first.status == 200

    assert {:ok, %{"changes" => changes, "rev" => first_rev}} =
             JSON.decode_line(first.body <> "\n")

    assert [
             %{"kind" => "entry", "data" => %{"kind" => "reply"}, "rev" => chat_rev},
             %{
               "kind" => "message",
               "data" => %{"terminal" => %{"state" => "answered", "answer" => nil}},
               "rev" => answer_rev
             },
             %{
               "kind" => "message",
               "data" => %{"terminal" => %{"state" => "cleared"}},
               "rev" => close_rev
             }
           ] = changes

    assert chat_rev < answer_rev and answer_rev < close_rev

    assert eventually(fn ->
             store = :sys.get_state(ctx.relay).store

             case AttentionServer.AgentChangeLog.head(store, {"A", ctx.reader}) do
               {:ok, head} -> head > first_rev
               _ -> false
             end
           end)

    {conn, receipts} = agent_batch(conn, "?after=#{first_rev}", [])
    assert receipts.status == 200
    assert {:ok, %{"changes" => receipt_changes}} = JSON.decode_line(receipts.body <> "\n")

    replayed =
      for %{"kind" => "message", "data" => message} <- changes ++ receipt_changes,
          into: %{},
          do: {message["item_id"], message}

    {:ok, conn, ref} = Mint.HTTP.request(conn, "GET", "/v2/agent/messages", [], nil)
    {_conn, response} = collect(conn, ref)
    assert response.status == 200
    assert {:ok, %{"messages" => messages}} = JSON.decode_line(response.body <> "\n")
    assert replayed == Map.new(messages, &{&1["item_id"], &1})
  end

  test "filtered agent SSE resumes across concurrent writes and converges with the snapshot", ctx do
    assert :ok = Relay.setup_sender(ctx.relay, ctx.reader, "app:agent")
    {conn, feed} = open_agent_feed(connect(ctx), "?session=#{@handle}")
    {conn, feed} = await_open(conn, feed)
    assert feed.status == 200

    writer =
      Task.async(fn ->
        for n <- 1..40 do
          session = if rem(n, 2) == 0, do: @handle, else: @other_handle

          assert %{"type" => "ok"} =
                   request(ctx.relay, ctx.reader, %{
                     "type" => "forward",
                     "op_id" => op(n),
                     "session_handle" => session,
                     "message" => %{
                       "sender_id" => "app:agent",
                       "message_id" => "concurrent-#{n}",
                       "created_at" => n,
                       "title" => "Concurrent #{n}"
                     }
                   })
        end
      end)

    {conn, _feed, seen} = agent_messages(conn, feed, 3)
    last_id = List.last(seen).id
    Mint.HTTP.close(conn)

    {conn, feed} =
      open_agent_feed(connect(ctx), "?after=0&session=#{@handle}", [
        {"last-event-id", Integer.to_string(last_id)}
      ])

    {conn, feed} = await_open(conn, feed)
    assert feed.status == 200
    {conn, _feed, resumed} = agent_messages(conn, feed, 17)
    Task.await(writer, 10_000)
    all = seen ++ resumed
    assert Enum.all?(all, &(&1.data["session"] == @handle))
    ids = Enum.map(all, & &1.id)
    assert ids == Enum.sort(Enum.uniq(ids))
    assert Enum.any?(Enum.chunk_every(ids, 2, 1, :discard), fn [a, b] -> b > a + 1 end)
    replayed = Map.new(all, &{&1.data["item_id"], &1.data})

    {:ok, conn, ref} =
      Mint.HTTP.request(conn, "GET", "/v2/agent/messages?session=#{@handle}", [], nil)

    {_conn, response} = collect(conn, ref)
    assert response.status == 200
    assert {:ok, %{"messages" => messages}} = JSON.decode_line(response.body <> "\n")
    assert replayed == Map.new(messages, &{&1["item_id"], &1})
  end

  defp agent_messages(conn, feed, count, acc \\ [])

  defp agent_messages(conn, feed, 0, acc), do: {conn, feed, Enum.reverse(acc)}

  defp agent_messages(conn, feed, count, acc) do
    case events(conn, feed, 1) do
      {conn, feed, [%{event: "message"} = event]} ->
        agent_messages(conn, feed, count - 1, [event | acc])

      {conn, feed, [%{event: kind}]} when kind in ["heartbeat", "entry"] ->
        agent_messages(conn, feed, count, acc)
    end
  end

  test "a live agent feed starts and stops focus when its see grant changes", ctx do
    Application.put_env(:attention_server, :client_api_feed_heartbeat_ms, 80)
    on_exit(fn -> Application.delete_env(:attention_server, :client_api_feed_heartbeat_ms) end)
    owner_cert = TLSFixtures.certificate_der!(ctx.certs, "a2")
    {:ok, owner} = DeviceIdentity.from_certificate(owner_cert)
    assert {:ok, person} = Relay.operator_bootstrap(ctx.relay, owner_cert)

    assert {:ok, agent} =
             Relay.setup_device_certificate(
               ctx.relay,
               person,
               TLSFixtures.certificate_der!(ctx.certs, "b1")
             )

    principal = %Principal{kind: :device, key: agent, person: person}
    conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, "b1")

    {conn, feed} = open_agent_feed(conn)
    {conn, feed} = await_open(conn, feed)
    assert feed.status == 200

    {conn, feed, [beat]} = events(conn, feed, 1)
    assert beat == %{event: "heartbeat", data: %{}}

    assert :ok =
             Relay.save_dashboard_focus_permission(ctx.relay, person, owner, agent, :see, true)

    {conn, feed, focus} = next_focus(conn, feed, 10)
    assert focus == %{event: "focus", data: %{"focus" => %{"clients" => []}}}

    assert {:ok, 200, _, false} =
             Writes.focus(ctx.relay, principal, %{"screen" => "inbox", "ttl_s" => 120})

    {conn, feed, focus} = next_focus(conn, feed, 10)

    assert %{
             event: "focus",
             data: %{"focus" => %{"clients" => [%{"device_id" => ^agent, "screen" => "inbox"}]}}
           } = focus

    refute Map.has_key?(focus, :id)

    assert :ok =
             Relay.save_dashboard_focus_permission(ctx.relay, person, owner, agent, :see, false)

    assert {:ok, 200, _, false} =
             Writes.focus(ctx.relay, principal, %{"screen" => "settings", "ttl_s" => 120})

    {_conn, feed, beats} = events(conn, feed, 2)
    assert beats == List.duplicate(%{event: "heartbeat", data: %{}}, 2)
    refute feed.done
  end

  test "an agent storage failure closes the SSE feed and leaves Relay alive", ctx do
    {conn, feed} = open_agent_feed(connect(ctx))
    {conn, feed} = await_open(conn, feed)
    assert feed.status == 200
    break_store(ctx)

    assert :ok = ChangeLog.notify(%{{:agent, "A", ctx.reader} => 1})
    {conn, feed, closing} = until_closing(conn, feed)

    assert closing == %{event: "closing", data: %{"reason" => "storage_unavailable"}}
    assert {_conn, %{done: true}} = drain(conn, feed)
    assert Process.alive?(ctx.relay)
  end

  defp open_agent_feed(conn, query \\ "", headers \\ []) do
    {:ok, conn, ref} =
      Mint.HTTP.request(conn, "GET", "/v2/agent/changes" <> query, [{"accept", "text/event-stream"} | headers], nil)

    {conn, %{ref: ref, buffer: "", status: nil, headers: [], done: false}}
  end

  defp next_focus(_conn, _feed, 0), do: flunk("focus event missing")

  defp next_focus(conn, feed, attempts) do
    case events(conn, feed, 1) do
      {conn, feed, [%{event: "heartbeat"}]} -> next_focus(conn, feed, attempts - 1)
      {conn, feed, [%{event: "focus"} = event]} -> {conn, feed, event}
    end
  end

  defp agent_batch(conn, query, headers) do
    {:ok, conn, ref} =
      Mint.HTTP.request(
        conn,
        "GET",
        "/v2/agent/changes" <> query,
        [{"accept", "application/json"} | headers],
        nil
      )

    collect(conn, ref)
  end

  ## Liveness

  test "an idle feed sends id-less heartbeats and stays open", ctx do
    Application.put_env(:attention_server, :client_api_feed_heartbeat_ms, 100)
    on_exit(fn -> Application.delete_env(:attention_server, :client_api_feed_heartbeat_ms) end)

    conn = connect(ctx)
    {conn, feed} = open_feed(conn)
    started = System.monotonic_time(:millisecond)
    {conn, feed, beats} = events(conn, feed, 3)
    elapsed = System.monotonic_time(:millisecond) - started

    assert beats == List.duplicate(%{event: "heartbeat", data: %{}}, 3)
    assert elapsed >= 250 and elapsed < 2_000

    # A change still arrives on the same stream.
    forward(ctx.relay, "m1", 1)
    {_conn, _feed, changes} = changes_through(conn, feed, head(ctx))
    assert Enum.all?(changes, &is_integer(&1.id))
  end

  test "the default heartbeat is inside the contract's 25 s" do
    assert Application.get_env(:attention_server, :client_api_feed_heartbeat_ms) == nil
    source = File.read!(Path.expand("../lib/attention_server/client_api/feed_stream.ex", __DIR__))
    [_, ms] = Regex.run(~r/@default_heartbeat_ms (\d[\d_]*)/, source)
    assert String.to_integer(String.replace(ms, "_", "")) <= 25_000
  end

  test "the feed's Request-Id is logged at open and at close", ctx do
    Application.put_env(:attention_server, :client_api_feed_heartbeat_ms, 50)
    on_exit(fn -> Application.delete_env(:attention_server, :client_api_feed_heartbeat_ms) end)

    log =
      capture_log(fn ->
        conn = connect(ctx)
        {conn, feed} = open_feed(conn)
        {conn, feed} = await_open(conn, feed)
        [request_id] = for {"request-id", id} <- feed.headers, do: id
        send(self(), {:request_id, request_id})
        [pid] = feed_pids(ctx)
        ref = Process.monitor(pid)
        Mint.HTTP.close(conn)
        assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
        Process.sleep(100)
      end)

    assert_received {:request_id, request_id}
    assert log =~ "event=harklight_client_feed request_id=#{request_id} phase=open"

    assert log =~
             "event=harklight_client_feed request_id=#{request_id} phase=close reason=client_closed"
  end

  ## A slow client

  test "a client that stops reading holds no backlog and never blocks the relay", ctx do
    conn = connect(ctx)
    {conn, feed} = open_feed(conn)
    {_conn, _feed} = await_open(conn, feed)
    [pid] = feed_pids(ctx)

    # The client reads nothing more. Far more than its windows and the socket
    # buffers can hold arrives, so the feed's send blocks (review N1).
    title = String.duplicate("x", 200_000)

    for n <- 1..60 do
      {micros, _} = :timer.tc(fn -> forward(ctx.relay, "m#{n}", n, title) end)
      # Each write commits promptly: the relay never waits on the feed.
      assert micros < 2_000_000
    end

    # Small commits while the send is blocked: each wakes the watcher.
    for n <- 61..260, do: forward(ctx.relay, "m#{n}", n)

    {micros, {:ok, _}} = :timer.tc(fn -> ChangeLog.head(:sys.get_state(ctx.relay).store, "A") end)
    assert micros < 1_000_000

    # The feed is blocked in the HTTP server's send ...
    {:current_stacktrace, stack} = Process.info(pid, :current_stacktrace)
    assert Enum.any?(stack, &match?({Plug.Conn, :chunk, _, _}, &1))

    # ... and holds at most one wake-up and one page.
    {:messages, messages} = Process.info(pid, :messages)
    assert Enum.count(messages, &match?({:change_log, _, _}, &1)) <= 1
    {:memory, memory} = Process.info(pid, :memory)
    assert memory < 64_000_000
  end

  ## Ends that are not the server's shutdown (review of #207)

  # The feed's own process, once it has subscribed.
  defp feed_pid(ctx) do
    assert eventually(fn -> feed_pids(ctx) != [] end)
    [pid] = feed_pids(ctx)
    pid
  end

  # Replaces the Relay's store with one whose database is unusable, so its
  # next change-log read raises inside the Relay step.
  defp break_store(ctx) do
    original = :sys.get_state(ctx.relay)
    :sys.replace_state(ctx.relay, fn state -> put_in(state.store.db, :unusable) end)

    on_exit(fn ->
      if Process.alive?(ctx.relay), do: :sys.replace_state(ctx.relay, fn _ -> original end)
    end)
  end

  test "a change-log read that fails or raises answers storage_unavailable and the Relay lives",
       ctx do
    break_store(ctx)

    assert AttentionServer.ClientAPI.FeedStep.read(ctx.relay, principal(ctx), 0, 10) ==
             {:error, :storage_unavailable}

    # A raise inside the Relay step: the engine itself is unusable.
    :sys.replace_state(ctx.relay, fn state -> put_in(state.store.engine, :unusable) end)

    assert AttentionServer.ClientAPI.FeedStep.read(ctx.relay, principal(ctx), 0, 10) ==
             {:error, :storage_unavailable}

    assert Process.alive?(ctx.relay)
  end

  test "a storage failure mid-stream ends the feed with closing: storage_unavailable", ctx do
    log =
      capture_log(fn ->
        conn = connect(ctx)
        {conn, feed} = open_feed(conn)
        {conn, feed} = await_open(conn, feed)
        pid = feed_pid(ctx)

        break_store(ctx)
        send(pid, {:change_log, "A", 1_000_000})

        {conn, feed, closing} = until_closing(conn, feed)
        assert closing == %{event: "closing", data: %{"reason" => "storage_unavailable"}}
        assert {_conn, %{done: true}} = drain(conn, feed)
        assert Process.alive?(ctx.relay)
        Process.sleep(50)
      end)

    assert log =~ "phase=close reason=storage_unavailable"
  end

  test "a position that falls out of retention mid-stream gets closing: cursor_expired", ctx do
    forward(ctx.relay, "m1", 1)
    conn = connect(ctx)
    {conn, feed} = open_feed(conn, "?after=0")
    {conn, feed, _backlog} = changes_through(conn, feed, head(ctx))
    pid = feed_pid(ctx)

    # While the feed is held, later changes commit and retention passes them.
    :erlang.suspend_process(pid)
    forward(ctx.relay, "m2", 2)
    forward(ctx.relay, "m3", 3)

    assert {:ok, _} =
             ClientAPIFeedClient.prune_changes(ctx.relay, System.system_time(:millisecond) + 1)

    :erlang.resume_process(pid)

    {conn, feed, closing} = until_closing(conn, feed)
    assert closing == %{event: "closing", data: %{"reason" => "cursor_expired"}}
    assert {_conn, %{done: true}} = drain(conn, feed)
  end

  test "a principal that stops authenticating mid-stream gets closing: revoked", ctx do
    conn = connect(ctx)
    {conn, feed} = open_feed(conn)
    {conn, feed} = await_open(conn, feed)
    pid = feed_pid(ctx)

    # The device leaves the engine with no closing signal, as a read racing
    # its revocation sees it.
    :sys.replace_state(ctx.relay, fn state ->
      update_in(state.store.engine.devices, &Map.delete(&1, ctx.reader))
    end)

    send(pid, {:change_log, "A", 1_000_000})
    {conn, feed, closing} = until_closing(conn, feed)
    assert closing == %{event: "closing", data: %{"reason" => "revoked"}}
    assert {_conn, %{done: true}} = drain(conn, feed)
  end

  test "an HTTP/1.1 connection reused after a feed keeps no subscription or timer", ctx do
    Application.put_env(:attention_server, :client_api_feed_heartbeat_ms, 50)
    on_exit(fn -> Application.delete_env(:attention_server, :client_api_feed_heartbeat_ms) end)

    conn = ClientAPIFeedClient.connect(ctx.port, ctx.certs, "a1", :http1)
    {conn, feed} = open_feed(conn)
    {conn, feed} = await_open(conn, feed)
    pid = feed_pid(ctx)

    # End the feed from the server side, without revoking the device: the
    # store fails once, then recovers.
    original = :sys.get_state(ctx.relay)
    :sys.replace_state(ctx.relay, fn state -> put_in(state.store.db, :unusable) end)
    send(pid, {:change_log, "A", 1_000_000})
    {conn, _feed} = drain(conn, feed)
    :sys.replace_state(ctx.relay, fn _ -> original end)

    # The same connection serves another request.
    {_conn, 200, _batch} = batch(conn, "?after=0")
    Process.sleep(200)

    if Process.alive?(pid) do
      assert Registry.keys(ChangeLog.Registry, pid) == []
      {:messages, messages} = Process.info(pid, :messages)
      refute Enum.any?(messages, &match?({:feed_heartbeat, _}, &1))
      refute Enum.any?(messages, &match?({:client_closing, _, _}, &1))
    end
  end

  ## No lost wake-up (review H4)

  test "the feed is subscribed before its first read", ctx do
    forward(ctx.relay, "m1", 1)
    probe = AttentionServer.ClientAPIFeedStreamTest.SubscriptionProbe
    :persistent_term.put({probe, :test}, self())
    Application.put_env(:attention_server, :client_api_feed_renderer, probe)

    on_exit(fn ->
      Application.delete_env(:attention_server, :client_api_feed_renderer)
      :persistent_term.erase({probe, :test})
    end)

    conn = connect(ctx)
    {conn, feed} = open_feed(conn, "?after=0")
    {_conn, _feed} = await_open(conn, feed)

    # At the first read, the feed's process is already subscribed, so a
    # commit between that read and the feed's wait still wakes it.
    assert_receive {:subscribed_at_read, subscribers}, 5_000
    assert [{pid, _}] = subscribers
    assert pid != GenServer.whereis(ctx.relay)
  end
end
