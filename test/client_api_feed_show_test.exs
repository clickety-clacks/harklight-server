defmodule AttentionServer.ClientAPIFeedShowTest do
  # A device's held shows on its change feed (contract rulings round 11 r4
  # §3b): an ephemeral `event: show` with no `id:` line, from the Relay's
  # memory-only focus state, never in a batch and never replayed.
  use ExUnit.Case, async: false

  import AttentionServer.ClientAPIFeedClient

  alias AttentionServer.{ChangeLog, ClientAPI, JSON, Relay, TLSFixtures}
  alias AttentionServer.ClientAPI.Writes

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @agent String.duplicate("e", 64)

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "harklight-show-#{System.unique_integer([:positive])}")
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
    assert :ok = Relay.setup_device(relay, "A", @agent)
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert {:ok, reader} =
             Relay.setup_device_certificate(relay, "A", TLSFixtures.certificate_der!(certs, "a1"))

    forward(relay, "one", 1)

    # The agent may push views, as an owner's switches allow.
    :sys.replace_state(relay, fn state ->
      engine = state.store.engine
      device = %{engine.devices[@agent] | see_focus: true, push_view: true}
      put_in(state.store.engine, %{engine | devices: Map.put(engine.devices, @agent, device)})
    end)

    # A held show needs an accepted focus report from the device.
    assert {:ok, 200, _, false} =
             Writes.focus(relay, reader, %{"screen" => "inbox", "ttl_s" => 120})

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

  defp forward(relay, id, n) do
    frame = %{
      "v" => 1,
      "type" => "forward",
      "op_id" => "00000000-0000-4000-8000-" <> String.pad_leading("#{n}", 12, "0"),
      "session_handle" => @handle,
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => id,
        "created_at" => n,
        "title" => id
      }
    }

    assert {:ok, line} = Relay.request_line(relay, "origin", JSON.encode!(frame) <> "\n")
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(line)
  end

  defp push_view(relay, target) do
    Relay.view_show(relay, @agent, %{
      "sender_id" => "app:test",
      "message_id" => "one",
      "target" => target
    })
  end

  defp head(relay), do: elem(ChangeLog.head(:sys.get_state(relay).store, "A"), 1)

  # Reads events until a `show`, skipping heartbeats; returns the events seen.
  defp until_show(conn, feed, acc \\ []) do
    {conn, feed, [event]} = events(conn, feed, 1)

    if event[:event] == "show",
      do: {conn, feed, event, Enum.reverse(acc)},
      else: until_show(conn, feed, [event | acc])
  end

  defp heartbeat_fast do
    Application.put_env(:attention_server, :client_api_feed_heartbeat_ms, 50)
    on_exit(fn -> Application.delete_env(:attention_server, :client_api_feed_heartbeat_ms) end)
  end

  test "a held show reaches the device's open feed as an id-less show event", ctx do
    conn = connect(ctx.port, ctx.certs, "a1")
    {conn, feed} = open_feed(conn, "?after=#{head(ctx.relay)}")
    {conn, feed} = await_open(conn, feed)

    sent_at = System.system_time(:millisecond)
    assert {:ok, %{"delivery" => "held"}} = push_view(ctx.relay, "chat")
    {_conn, _feed, show, _before} = until_show(conn, feed)

    # No id: line, so Last-Event-ID never moves.
    refute Map.has_key?(show, :id)

    assert %{
             "show_id" => "s" <> _,
             "device_id" => device_id,
             "item_id" => "i" <> _,
             "target" => "chat",
             "held_ms" => held_ms
           } = show.data

    assert device_id == ctx.reader
    assert is_integer(held_ms) and held_ms >= 0
    assert held_ms <= System.system_time(:millisecond) - sent_at + 50

    # It is the same show the device's next focus write returns.
    assert {:ok, 200, %{"show" => %{"show_id" => show_id}}, false} =
             Writes.focus(ctx.relay, ctx.reader, %{"screen" => "inbox", "ttl_s" => 120})

    assert show_id == show.data["show_id"]
  end

  test "a show is never in a batch and never replayed on resume", ctx do
    heartbeat_fast()
    start = head(ctx.relay)
    conn = connect(ctx.port, ctx.certs, "a1")
    {conn, feed} = open_feed(conn, "?after=#{start}")
    {conn, feed} = await_open(conn, feed)

    assert {:ok, _} = push_view(ctx.relay, "message")
    {conn, _feed, _show, _} = until_show(conn, feed)
    Mint.HTTP.close(conn)

    # Nothing about the show is in the log.
    assert head(ctx.relay) == start
    conn = connect(ctx.port, ctx.certs, "a1")
    {conn, 200, batch} = batch(conn, "?after=0")
    refute Enum.any?(batch["changes"], &(&1["kind"] == "show"))

    # Resuming from the same position replays no show, only heartbeats.
    {conn, feed} = open_feed(conn, "", [{"last-event-id", Integer.to_string(start)}])
    {_conn, _feed, later} = events(conn, feed, 4)
    assert Enum.all?(later, &(&1 == %{event: "heartbeat", data: %{}}))
  end

  test "a device's feed subscribes to its own shows only, and releases them", ctx do
    conn = connect(ctx.port, ctx.certs, "a1")
    {conn, feed} = open_feed(conn)
    {conn, _feed} = await_open(conn, feed)

    assert [{pid, _}] = Registry.lookup(ChangeLog.Registry, {:client_api_show, ctx.reader})
    assert Registry.lookup(ChangeLog.Registry, {:client_api_show, @agent}) == []

    ref = Process.monitor(pid)
    Mint.HTTP.close(conn)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000

    # The registry drops a dead process's keys asynchronously.
    assert Enum.any?(1..100, fn _ ->
             Registry.lookup(ChangeLog.Registry, {:client_api_show, ctx.reader}) == [] or
               (Process.sleep(20) && false)
           end)
  end
end
