defmodule AttentionServer.ClientAPIFeedClosingTest do
  # The change feed's server-initiated ends (docs/client-api-v2.md §6): an
  # id-less `closing` event with `revoked`, `account_paused` or
  # `server_shutdown`, for device and browser principals.
  use ExUnit.Case, async: false

  import AttentionServer.ClientAPIFeedClient

  alias AttentionServer.{
    ChangeLog,
    ClientAPI,
    DashboardBrowserSession,
    DashboardHTTPS,
    DeviceIdentity,
    GuidedJoin,
    JSON,
    Relay,
    Server,
    TLSFixtures
  }

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "harklight-closing-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    flags = Path.join(dir, "flags.json")
    File.write!(flags, ~s({"client_api_v2":true}))
    %{dir: dir, certs: certs, flags: flags}
  end

  defp heartbeat_fast do
    Application.put_env(:attention_server, :client_api_feed_heartbeat_ms, 100)
    on_exit(fn -> Application.delete_env(:attention_server, :client_api_feed_heartbeat_ms) end)
  end

  ## Devices

  describe "a device feed" do
    setup ctx do
      relay =
        start_supervised!(
          {Relay,
           store_path: Path.join(ctx.dir, "state.sqlite3"),
           feature_flags_path: ctx.flags,
           enrollment_fixture_mode: true,
           agent_fixture_mode: true},
          restart: :temporary
        )

      assert :ok = Relay.setup_person(relay, "A")

      keys =
        Map.new(~w(a1 a2), fn name ->
          der = TLSFixtures.certificate_der!(ctx.certs, name)
          assert {:ok, key} = Relay.setup_device_certificate(relay, "A", der)
          {name, key}
        end)

      listener =
        start_supervised!(
          {ClientAPI.Listener,
           [
             relay: relay,
             certfile: Path.join(ctx.certs, "server.pem"),
             keyfile: Path.join(ctx.certs, "server.key"),
             cacertfile: Path.join(ctx.certs, "ca.pem"),
             routes: [ClientAPI.Feed]
           ]},
          id: make_ref()
        )

      %{relay: relay, keys: keys, port: ClientAPI.Listener.port(listener)}
    end

    test "revoking the device ends its feed with closing: revoked, and not the other device's",
         ctx do
      heartbeat_fast()
      {conn, feed} = open(ctx, "a1")
      {other, other_feed} = open(ctx, "a2")

      assert :ok = Relay.revoke_device(ctx.relay, ctx.keys["a1"])

      # Changes the revocation itself made may come first.
      {conn, feed, closing} = until_closing(conn, feed)
      assert closing == %{event: "closing", data: %{"reason" => "revoked"}}
      assert {_conn, %{done: true, buffer: ""}} = drain(conn, feed)

      # The person's other device keeps its feed.
      {_other, _feed, beats} = events(other, other_feed, 3)
      refute Enum.any?(beats, &(&1[:event] == "closing"))
    end

    test "pausing the account ends its feeds with closing: account_paused", ctx do
      assert {:ok, person} = Relay.hosted_account_create(ctx.relay, "acct-paused")
      TLSFixtures.make_self_signed_client!(ctx.certs, "paused-device")
      der = TLSFixtures.certificate_der!(ctx.certs, "paused-device")
      assert {:ok, _key} = Relay.setup_device_certificate(ctx.relay, person, der)

      {conn, feed} = open(ctx, "paused-device")
      assert {:ok, _} = Relay.hosted_delivery_state(ctx.relay, person, "STOPPED")

      {conn, feed, closing} = until_closing(conn, feed)
      assert closing == %{event: "closing", data: %{"reason" => "account_paused"}}
      assert {_conn, %{done: true}} = drain(conn, feed)

      # A new feed is refused while paused.
      conn = connect(ctx.port, ctx.certs, "paused-device")
      {_conn, status, body} = batch(conn, "")
      assert {status, body["error"]} == {403, "account_paused"}
    end

    test "a stopped relay ends the feed with closing: server_shutdown", ctx do
      {conn, feed} = open(ctx, "a1")
      GenServer.stop(ctx.relay)

      {conn, feed, closing} = until_closing(conn, feed)
      assert closing == %{event: "closing", data: %{"reason" => "server_shutdown"}}
      assert {_conn, %{done: true}} = drain(conn, feed)
    end

    test "a closed feed releases its subscriptions", ctx do
      {conn, feed} = open(ctx, "a1")
      scope = {:closing, {:device, ctx.keys["a1"]}}
      assert [{feed_pid, _}] = Registry.lookup(ChangeLog.Registry, scope)
      assert [{watcher, _}] = Registry.lookup(ChangeLog.Registry, "A")

      assert :ok = Relay.revoke_device(ctx.relay, ctx.keys["a1"])
      {conn, feed, _closing} = until_closing(conn, feed)
      drain(conn, feed)

      # This feed's own registrations, so no other process can disturb the
      # check (review N2).
      released = fn ->
        Registry.keys(ChangeLog.Registry, feed_pid) == [] and
          Registry.keys(ChangeLog.Registry, watcher) == []
      end

      assert eventually(released),
             "still registered: " <>
               inspect(
                 feed: {Process.alive?(feed_pid), Registry.keys(ChangeLog.Registry, feed_pid)},
                 watcher: {Process.alive?(watcher), Registry.keys(ChangeLog.Registry, watcher)}
               )
    end
  end

  ## The server stopping (review H1)

  test "a graceful server stop ends open feeds with closing: server_shutdown", ctx do
    server =
      start_supervised!(
        {Server,
         [
           store_path: Path.join(ctx.dir, "server.sqlite3"),
           certfile: Path.join(ctx.certs, "server.pem"),
           keyfile: Path.join(ctx.certs, "server.key"),
           cacertfile: Path.join(ctx.certs, "ca.pem"),
           enrollment_enabled: false,
           feature_flags_path: ctx.flags
         ]},
        restart: :temporary
      )

    relay = Server.relay(server)
    assert :ok = Relay.setup_person(relay, "A")
    der = TLSFixtures.certificate_der!(ctx.certs, "a1")
    assert {:ok, _key} = Relay.setup_device_certificate(relay, "A", der)

    {conn, feed} = open(%{port: Server.port(server), certs: ctx.certs}, "a1")

    # The listeners stop before the Relay; the feed must hear first.
    stopping = Task.async(fn -> Supervisor.stop(server) end)
    {_conn, _feed, closing} = until_closing(conn, feed)
    assert closing == %{event: "closing", data: %{"reason" => "server_shutdown"}}
    assert :ok = Task.await(stopping, 10_000)
  end

  ## Browsers

  describe "a browser feed" do
    setup ctx do
      web_origin(ctx)
    end

    test "serves the batch and the stream to a signed-in browser", ctx do
      forward(ctx, "m1", 1)
      conn = connect(ctx.web_port, ctx.certs)
      {conn, 200, batch} = batch(conn, "?after=0", [cookie(ctx)])
      assert [%{"kind" => "item"} | _] = for(c <- batch["changes"], c["kind"] == "item", do: c)

      {conn, feed} = open_feed(conn, "?after=0", [cookie(ctx)])
      {_conn, _feed, events} = changes_through(conn, feed, batch["rev"])
      assert Enum.map(events, & &1.id) == Enum.map(batch["changes"], & &1["rev"])
    end

    test "signing out ends the browser's feed with closing: revoked", ctx do
      conn = connect(ctx.web_port, ctx.certs)
      {conn, feed} = open_feed(conn, "", [cookie(ctx)])
      {conn, feed} = await_open(conn, feed)
      assert feed.status == 200

      DashboardBrowserSession.end_session(ctx.sessions, ctx.cookie)

      {conn, feed, closing} = until_closing(conn, feed)
      assert closing == %{event: "closing", data: %{"reason" => "revoked"}}
      assert {conn, %{done: true}} = drain(conn, feed)

      # Its next feed is refused.
      {_conn, status, body} = batch(conn, "", [cookie(ctx)])
      assert {status, body["error"]} == {401, "unauthorized"}
    end
  end

  ## Helpers

  defp open(ctx, name) do
    conn = connect(ctx.port, ctx.certs, name)
    {conn, feed} = open_feed(conn)
    {conn, feed} = await_open(conn, feed)
    assert feed.status == 200
    {conn, feed}
  end

  defp cookie(ctx), do: {"cookie", "__Host-harklight=#{ctx.cookie}"}

  defp forward(ctx, id, n) do
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

    assert {:ok, line} = Relay.request_line(ctx.relay, "origin", JSON.encode!(frame) <> "\n")
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(line)
  end

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

  # The web origin with the feed mounted, and a signed-in browser session.
  defp web_origin(%{dir: dir, certs: certs, flags: flags}) do
    skill_path = Path.join(dir, "skills/harklight-agent/SKILL.md")
    File.mkdir_p!(Path.dirname(skill_path))
    File.write!(skill_path, "---\nname: harklight-agent\nversion: \"0.1.0\"\n---\n\n# Agent")
    port = available_port()
    origin = "https://relay.test:#{port}"

    {:ok, join} =
      GuidedJoin.configuration("relay.test", 18444, 18443, File.read!(Path.join(certs, "ca.pem")))

    relay =
      start_supervised!(
        {Relay,
         store_path: Path.join(dir, "web.sqlite3"),
         feature_flags_path: flags,
         join_configuration: join,
         dashboard_origin: origin,
         enrollment_fixture_mode: true},
        restart: :temporary
      )

    owner_cert = TLSFixtures.certificate_der!(certs, "a1")
    {:ok, person} = Relay.operator_bootstrap(relay, owner_cert)
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner_cert)
    assert :ok = Relay.setup_device(relay, person, "origin")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    start_supervised!(
      {DashboardHTTPS,
       [
         relay: relay,
         owner_person: person,
         port: port,
         ip: {127, 0, 0, 1},
         certfile: Path.join(certs, "server.pem"),
         keyfile: Path.join(certs, "server.key"),
         skill_path: skill_path,
         client_api_routes: [ClientAPI.Feed]
       ]}
    )

    sessions = {:global, {DashboardHTTPS, relay}}
    {:ok, credential} = Relay.owner_browser_sign_in_create(relay, person, owner_key)
    {:ok, cookie, _} = DashboardBrowserSession.redeem(sessions, credential["sign_in_token"])

    %{relay: relay, web_port: port, cookie: cookie, sessions: sessions}
  end

  defp available_port do
    {:ok, socket} = :gen_tcp.listen(0, ip: {127, 0, 0, 1})
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end
end
