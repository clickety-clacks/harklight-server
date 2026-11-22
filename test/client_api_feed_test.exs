defmodule AttentionServer.ClientAPIFeedTest do
  # GET /v2/changes (docs/client-api-v2.md §6).
  use ExUnit.Case, async: false

  alias AttentionServer.{ChangeLog, ClientAPIFeedClient, ClientAPI, JSON, Relay, TLSFixtures}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "harklight-feed-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    relay =
      start_supervised!(
        {Relay, store_path: Path.join(dir, "relay.sqlite3"), enrollment_fixture_mode: true},
        restart: :temporary
      )

    assert :ok = Relay.setup_person(relay, "A")

    keys =
      Map.new(~w(a1 a2), fn name ->
        assert {:ok, key} =
                 Relay.setup_device_certificate(
                   relay,
                   "A",
                   TLSFixtures.certificate_der!(certs, name)
                 )

        {name, key}
      end)

    assert :ok = Relay.setup_sender(relay, keys["a1"], "app:test")

    listener =
      start_supervised!(
        {ClientAPI.Listener,
         [
           relay: relay,
           certfile: Path.join(certs, "server.pem"),
           keyfile: Path.join(certs, "server.key"),
           cacertfile: Path.join(certs, "ca.pem"),
           routes: [ClientAPI.Feed]
         ]},
        id: make_ref()
      )

    %{relay: relay, keys: keys, port: ClientAPI.Listener.port(listener), certs: certs}
  end

  # -- helpers ---------------------------------------------------------------

  defp op(n), do: "00000000-0000-4000-8000-" <> String.pad_leading("#{n}", 12, "0")

  defp forward(ctx, id, n) do
    frame = %{
      "v" => 1,
      "type" => "forward",
      "op_id" => op(n),
      "session_handle" => @handle,
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => id,
        "created_at" => n,
        "title" => "Question #{id}"
      }
    }

    assert {:ok, line} =
             Relay.request_line(ctx.relay, ctx.keys["a1"], JSON.encode!(frame) <> "\n")

    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(line)
  end

  defp connect(ctx, name \\ "a2") do
    assert {:ok, conn} =
             Mint.HTTP.connect(:https, "127.0.0.1", ctx.port,
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
             )

    conn
  end

  defp get(conn, path, headers \\ []) do
    {:ok, conn, ref} = Mint.HTTP.request(conn, "GET", path, headers, nil)
    collect(conn, ref, %{headers: [], body: ""})
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

  defp batch(conn, query) do
    {conn, response} = get(conn, "/v2/changes" <> query, [{"accept", "application/json"}])
    assert {:ok, body} = JSON.decode_line(response.body <> "\n")
    {conn, response.status, body}
  end

  # Opens an SSE feed; returns the connection, the stream ref and its state.
  defp open_feed(conn, query \\ "", headers \\ []) do
    {:ok, conn, ref} =
      Mint.HTTP.request(
        conn,
        "GET",
        "/v2/changes" <> query,
        [{"accept", "text/event-stream"} | headers],
        nil
      )

    {conn, %{ref: ref, buffer: "", status: nil, done: false}}
  end

  # Reads until `count` SSE events (heartbeats included) have arrived.
  defp events(conn, feed, count, acc \\ [])

  defp events(conn, feed, count, acc) when length(acc) >= count,
    do: {conn, feed, Enum.reverse(acc)}

  defp events(conn, feed, count, acc) do
    case String.split(feed.buffer, "\n\n", parts: 2) do
      [block, rest] ->
        events(conn, %{feed | buffer: rest}, count, [parse(block) | acc])

      [_incomplete] ->
        if feed.done, do: flunk("feed ended after #{length(acc)} events")
        {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, 5_000)

        feed =
          Enum.reduce(responses, feed, fn
            {:status, ref, status}, f when ref == feed.ref -> %{f | status: status}
            {:data, ref, data}, f when ref == feed.ref -> %{f | buffer: f.buffer <> data}
            {:done, ref}, f when ref == feed.ref -> %{f | done: true}
            _, f -> f
          end)

        events(conn, feed, count, acc)
    end
  end

  defp until_closing(conn, feed) do
    {conn, feed, [event]} = events(conn, feed, 1)
    if event[:event] == "closing", do: {conn, feed, event}, else: until_closing(conn, feed)
  end

  defp drain(conn, %{done: true} = feed), do: {conn, feed}

  defp drain(conn, feed) do
    {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, 5_000)

    feed =
      Enum.reduce(responses, feed, fn
        {:data, ref, data}, f when ref == feed.ref -> %{f | buffer: f.buffer <> data}
        {:done, ref}, f when ref == feed.ref -> %{f | done: true}
        _, f -> f
      end)

    drain(conn, feed)
  end

  # Waits for the feed's 200, so it is subscribed before the test acts.
  defp await_open(conn, %{status: nil} = feed) do
    {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, 5_000)

    feed =
      Enum.reduce(responses, feed, fn
        {:status, ref, status}, f when ref == feed.ref -> %{f | status: status}
        {:data, ref, data}, f when ref == feed.ref -> %{f | buffer: f.buffer <> data}
        {:done, ref}, f when ref == feed.ref -> %{f | done: true}
        _, f -> f
      end)

    await_open(conn, feed)
  end

  defp await_open(conn, %{status: 200} = feed), do: {conn, feed}

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

  # -- JSON batch --------------------------------------------------------------

  test "a JSON batch pages through the log gap-free with limit, more and rev", ctx do
    for {id, n} <- [{"m1", 1}, {"m2", 2}, {"m3", 3}], do: forward(ctx, id, n)
    conn = connect(ctx)

    {conn, 200, all} = batch(conn, "?after=0")
    assert all["more"] == false
    assert all["rev"] == head(ctx)
    revs = Enum.map(all["changes"], & &1["rev"])
    assert revs == Enum.sort(revs)
    assert Enum.all?(all["changes"], &match?(%{"rev" => _, "kind" => _, "data" => %{}}, &1))

    {conn, 200, first} = batch(conn, "?after=0&limit=2")
    assert length(first["changes"]) == 2
    assert first["more"] == true
    assert first["rev"] == List.last(first["changes"])["rev"]
    {conn, 200, rest} = batch(conn, "?after=#{first["rev"]}")
    assert first["changes"] ++ rest["changes"] == all["changes"]

    {conn, 200, empty} = batch(conn, "?after=#{all["rev"]}")
    assert empty == %{"changes" => [], "rev" => all["rev"], "more" => false}

    {conn, 400, %{"error" => "invalid_field", "field" => "after"}} = batch(conn, "?after=-1")
    {_conn, 400, %{"error" => "invalid_field", "field" => "limit"}} = batch(conn, "?limit=0")
  end

  test "a cursor past retention is 410 cursor_expired in both forms", ctx do
    forward(ctx, "m1", 1)

    assert {:ok, _} =
             ClientAPIFeedClient.prune_changes(ctx.relay, System.system_time(:millisecond) + 1)

    conn = connect(ctx)

    {conn, 410, %{"error" => "cursor_expired", "retryable" => false}} = batch(conn, "?after=0")
    {conn, feed} = open_feed(conn, "?after=0")
    {_conn, response} = collect(conn, feed.ref, %{headers: [], body: ""})
    assert response.status == 410
    assert {:ok, %{"error" => "cursor_expired"}} = JSON.decode_line(response.body <> "\n")
  end

  # -- Server-Sent Events --------------------------------------------------------

  test "an open feed delivers backlog then live changes while reads run beside it", ctx do
    forward(ctx, "m1", 1)
    conn = connect(ctx)
    {conn, 200, backlog} = batch(conn, "?after=0")
    {conn, feed} = open_feed(conn, "?after=0")
    {conn, feed, first} = events(conn, feed, length(backlog["changes"]))
    assert feed.status == 200
    assert Enum.map(first, & &1.id) == Enum.map(backlog["changes"], & &1["rev"])
    assert Enum.all?(first, &(is_binary(&1.event) and is_map(&1.data)))

    # Reads on another connection, and writes, run beside the open feed.
    reader = connect(ctx)
    reads = Task.async(fn -> batch(reader, "?after=0") end)
    {_reader, 200, %{"rev" => rev}} = Task.await(reads)
    assert rev == backlog["rev"]
    forward(ctx, "m2", 2)
    {_reader, 200, live} = batch(connect(ctx), "?after=#{backlog["rev"]}")
    assert live["changes"] != []
    {_conn, _feed, later} = events(conn, feed, length(live["changes"]))
    assert Enum.map(later, & &1.id) == Enum.map(live["changes"], & &1["rev"])
  end

  test "Last-Event-ID resumes with no gap and takes precedence over after", ctx do
    forward(ctx, "m1", 1)
    conn = connect(ctx)
    # Compaction keeps only each resource's latest row, so count rows, not revs.
    {conn, 200, backlog} = batch(conn, "?after=0")
    {conn, feed} = open_feed(conn, "?after=0")
    {conn, _feed, seen} = events(conn, feed, length(backlog["changes"]))
    last_id = List.last(seen).id
    Mint.HTTP.close(conn)

    forward(ctx, "m2", 2)
    forward(ctx, "m3", 3)

    conn = connect(ctx)
    {conn, 200, expected} = batch(conn, "?after=#{last_id}")
    assert expected["rev"] == head(ctx)
    {conn, feed} = open_feed(conn, "?after=0", [{"last-event-id", Integer.to_string(last_id)}])
    {_conn, _feed, resumed} = events(conn, feed, length(expected["changes"]))
    assert Enum.map(resumed, & &1.id) == Enum.map(expected["changes"], & &1["rev"])
  end

  test "an idle feed sends id-less heartbeat events and stays open", ctx do
    Application.put_env(:attention_server, :client_api_feed_heartbeat_ms, 100)
    on_exit(fn -> Application.delete_env(:attention_server, :client_api_feed_heartbeat_ms) end)

    conn = connect(ctx)
    {conn, feed} = open_feed(conn)
    {conn, feed, beats} = events(conn, feed, 3)
    assert Enum.all?(beats, &(&1 == %{event: "heartbeat", data: %{}}))

    # A change still arrives on the same stream after the heartbeats.
    forward(ctx, "m1", 1)
    {_conn, _feed, more} = events(conn, feed, 12)
    assert Enum.any?(more, &Map.has_key?(&1, :id))
  end

  test "revoking the device ends its open feed with closing: revoked", ctx do
    conn = connect(ctx)
    {conn, feed} = open_feed(conn)
    {conn, feed} = await_open(conn, feed)

    assert :ok = Relay.revoke_device(ctx.relay, ctx.keys["a2"])

    # Changes the revocation itself made (its push binding) may come first; the
    # stream then ends with an id-less closing event.
    {conn, feed, closing} = until_closing(conn, feed)
    assert closing == %{event: "closing", data: %{"reason" => "revoked"}}
    assert {_conn, %{done: true, buffer: ""}} = drain(conn, feed)
  end

  test "a stopped relay ends the feed with closing: server_shutdown", ctx do
    conn = connect(ctx)
    {conn, feed} = open_feed(conn)
    {conn, feed} = await_open(conn, feed)

    GenServer.stop(ctx.relay)
    {conn, feed, closing} = until_closing(conn, feed)
    assert closing == %{event: "closing", data: %{"reason" => "server_shutdown"}}
    assert {_conn, %{done: true}} = drain(conn, feed)
  end
end
