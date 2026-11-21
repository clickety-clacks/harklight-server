defmodule AttentionServer.ClientAPIFeedStreamTest do
  # GET /v2/changes as Server-Sent Events (docs/client-api-v2.md §6), over
  # HTTP/2 with mutual TLS, behind client_api_v2.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AttentionServer.{ChangeLog, ClientAPI, JSON, Relay, TLSFixtures}
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

  defp connect(ctx) do
    assert {:ok, conn} =
             Mint.HTTP.connect(:https, "127.0.0.1", ctx.port,
               hostname: "relay.test",
               protocols: [:http2],
               mode: :passive,
               transport_opts: [
                 versions: [:"tlsv1.3"],
                 cacertfile: Path.join(ctx.certs, "ca.pem"),
                 certfile: Path.join(ctx.certs, "a1.pem"),
                 keyfile: Path.join(ctx.certs, "a1.key"),
                 server_name_indication: ~c"relay.test",
                 timeout: 5_000
               ]
             )

    conn
  end

  defp batch(conn, query) do
    {:ok, conn, ref} =
      Mint.HTTP.request(
        conn,
        "GET",
        "/v2/changes" <> query,
        [{"accept", "application/json"}],
        nil
      )

    {conn, response} = collect(conn, ref, %{headers: [], body: ""})
    assert {:ok, body} = JSON.decode_line(response.body <> "\n")
    {conn, response.status, body}
  end

  defp collect(conn, ref, acc) do
    {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, 5_000)

    acc =
      Enum.reduce(responses, acc, fn
        {:status, ^ref, status}, acc -> Map.put(acc, :status, status)
        {:headers, ^ref, headers}, acc -> Map.update!(acc, :headers, &(&1 ++ headers))
        {:data, ^ref, data}, acc -> Map.update!(acc, :body, &(&1 <> data))
        {:done, ^ref}, acc -> Map.put(acc, :done, true)
        _, acc -> acc
      end)

    if acc[:done], do: {conn, acc}, else: collect(conn, ref, acc)
  end

  defp open_feed(conn, query \\ "", headers \\ []) do
    {:ok, conn, ref} =
      Mint.HTTP.request(
        conn,
        "GET",
        "/v2/changes" <> query,
        [{"accept", "text/event-stream"} | headers],
        nil
      )

    {conn, %{ref: ref, buffer: "", status: nil, headers: [], done: false}}
  end

  defp recv(conn, feed) do
    {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, 5_000)

    feed =
      Enum.reduce(responses, feed, fn
        {:status, ref, status}, f when ref == feed.ref -> %{f | status: status}
        {:headers, ref, headers}, f when ref == feed.ref -> %{f | headers: f.headers ++ headers}
        {:data, ref, data}, f when ref == feed.ref -> %{f | buffer: f.buffer <> data}
        {:done, ref}, f when ref == feed.ref -> %{f | done: true}
        _, f -> f
      end)

    {conn, feed}
  end

  # Reads until `count` events (heartbeats included) have arrived.
  defp events(conn, feed, count, acc \\ [])

  defp events(conn, feed, count, acc) when length(acc) >= count,
    do: {conn, feed, Enum.reverse(acc)}

  defp events(conn, feed, count, acc) do
    case String.split(feed.buffer, "\n\n", parts: 2) do
      [block, rest] ->
        events(conn, %{feed | buffer: rest}, count, [parse(block) | acc])

      [_incomplete] ->
        if feed.done, do: flunk("feed ended after #{length(acc)} events")
        {conn, feed} = recv(conn, feed)
        events(conn, feed, count, acc)
    end
  end

  # Reads change events until one has `id` >= rev, skipping heartbeats.
  defp changes_through(conn, feed, rev, acc \\ []) do
    {conn, feed, [event]} = events(conn, feed, 1)

    cond do
      event[:event] == "heartbeat" -> changes_through(conn, feed, rev, acc)
      event.id >= rev -> {conn, feed, Enum.reverse([event | acc])}
      true -> changes_through(conn, feed, rev, [event | acc])
    end
  end

  defp await_open(conn, %{status: nil} = feed) do
    {conn, feed} = recv(conn, feed)
    await_open(conn, feed)
  end

  defp await_open(conn, feed), do: {conn, feed}

  defp parse(block) do
    block
    |> String.split("\n")
    |> Enum.reduce(%{}, fn line, event ->
      case String.split(line, ": ", parts: 2) do
        ["id", id] -> Map.put(event, :id, String.to_integer(id))
        ["event", kind] -> Map.put(event, :event, kind)
        ["data", data] -> Map.put(event, :data, elem(JSON.decode_line(data <> "\n"), 1))
        _ -> event
      end
    end)
  end

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

  defp feed_pids, do: for({pid, _} <- Registry.lookup(ChangeLog.Registry, "A"), do: pid)

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

      assert {kind, Map.delete(data, "rank")} ==
               {change["kind"], Map.delete(change["data"], "rank")}
    end
  end

  test "a cursor past retention is 410 before the stream opens", ctx do
    forward(ctx.relay, "m1", 1)

    assert {:ok, _} =
             ChangeLog.prune(
               :sys.get_state(ctx.relay).store,
               System.system_time(:millisecond) + 1
             )

    conn = connect(ctx)
    {conn, feed} = open_feed(conn, "?after=0")
    {_conn, response} = collect(conn, feed.ref, %{headers: [], body: ""})
    assert response.status == 410
    assert {:ok, %{"error" => "cursor_expired"}} = JSON.decode_line(response.body <> "\n")
    assert eventually(fn -> feed_pids() == [] end)
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
    assert without_rank(replayed.items) == without_rank(expected.items)
    assert replayed.entries == expected.entries
    assert replayed.counts == expected.counts
  end

  # Rank keys move without an item row until the Relay records rank changes
  # in the change log (docs/change-log-interface.md, "Not yet on main"). The
  # skipped test below holds the full check until then.
  defp without_rank(items),
    do: Map.new(items, fn {id, item} -> {id, Map.delete(item, "rank")} end)

  @tag skip: "rank keys move without an item row until append_ranks runs in the Relay"
  test "replaying the feed reaches the snapshot's rank keys", ctx do
    for n <- 1..12, do: forward(ctx.relay, "m#{n}", n)
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
        [pid] = feed_pids()
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
    [pid] = feed_pids()

    # The client reads nothing more. Far more than its HTTP/2 window arrives.
    title = String.duplicate("x", 2_000)

    for n <- 1..150 do
      {micros, _} = :timer.tc(fn -> forward(ctx.relay, "m#{n}", n, title) end)
      # Each write commits promptly: the relay never waits on the feed.
      assert micros < 2_000_000
    end

    {micros, {:ok, _}} = :timer.tc(fn -> ChangeLog.head(:sys.get_state(ctx.relay).store, "A") end)
    assert micros < 1_000_000

    # The blocked feed holds at most one page and one wake-up.
    {:message_queue_len, queued} = Process.info(pid, :message_queue_len)
    {:memory, memory} = Process.info(pid, :memory)
    assert queued <= 5
    assert memory < 8_000_000
  end
end
