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

  alias AttentionServer.{ChangeLog, ClientAPI, ClientAPIFeedClient, JSON, Relay, TLSFixtures}
  alias AttentionServer.ClientAPI.{Principal, Reads}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

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
           cacertfile: Path.join(certs, "ca.pem"),
           routes: [ClientAPI.Reads, ClientAPI.Feed]
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
