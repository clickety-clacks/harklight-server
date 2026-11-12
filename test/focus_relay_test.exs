defmodule AttentionServer.FocusRelayTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DeviceIdentity, JSON, Relay, TLSFixtures}

  test "background and reconnect reports keep the last foreground rank and default target" do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)

    dir =
      Path.join(System.tmp_dir!(), "harklight-focus-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    clock = :atomics.new(1, [])
    :atomics.put(clock, 1, 1_000)
    clock_fn = fn -> :atomics.get(clock, 1) end

    {:ok, relay} = Relay.start_link(store_path: Path.join(dir, "relay.sqlite3"), clock: clock_fn)
    on_exit(fn -> if Process.alive?(relay), do: GenServer.stop(relay) end)

    owner_certificate = TLSFixtures.certificate_der!(certs, "a1")
    {:ok, person} = Relay.operator_bootstrap(relay, owner_certificate)
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner_certificate)

    tablet = String.duplicate("c", 64)
    agent = String.duplicate("e", 64)
    phone_certificate = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, phone} = Relay.setup_device_certificate(relay, person, phone_certificate)

    :ok = Relay.setup_device(relay, person, tablet)
    :ok = Relay.setup_device(relay, person, agent)
    :ok = Relay.setup_sender(relay, phone, "app:test")

    :ok = Relay.save_dashboard_focus_permission(relay, person, owner_key, agent, :see, true)
    :ok = Relay.save_dashboard_focus_permission(relay, person, owner_key, agent, :push, true)

    assert {:ok, message_line} =
             Relay.request_line(
               relay,
               phone,
               JSON.encode!(%{
                 "v" => 1,
                 "type" => "forward",
                 "op_id" => "00000000-0000-4000-8000-000000000951",
                 "message" => %{
                   "sender_id" => "app:test",
                   "message_id" => "focus-item",
                   "created_at" => 1,
                   "title" => "Focus item"
                 }
               }) <> "\n"
             )

    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(message_line <> "\n")

    {:ok, tablet_session} = Relay.open_session(relay, tablet)
    report(relay, tablet_session, "tablet-front", "inbox")

    :atomics.put(clock, 1, 1_100)
    :ok = Relay.close_session(relay, tablet_session)

    :atomics.put(clock, 1, 2_000)
    {:ok, phone_session} = Relay.open_session(relay, phone)

    report(relay, phone_session, "phone-chat", "chat", %{
      "sender_id" => "app:test",
      "message_id" => "focus-item",
      "visible_event_id" => "4182"
    })

    :atomics.put(clock, 1, 3_000)
    {:ok, tablet_reconnected} = Relay.open_session(relay, tablet)
    report(relay, tablet_reconnected, "tablet-background", "not_in_front")

    :atomics.put(clock, 1, 4_000)
    report(relay, phone_session, "phone-background", "not_in_front")

    assert {:ok, %{"clients" => [phone_focus, tablet_focus]}} = Relay.focus_get(relay, agent)
    assert phone_focus["client_id"] == phone
    assert phone_focus["screen"] == "not_in_front"
    assert phone_focus["reported_at"] == 2_000
    refute phone_focus["in_front"]

    assert tablet_focus["client_id"] == tablet
    assert tablet_focus["screen"] == "not_in_front"
    assert tablet_focus["reported_at"] == 1_000
    refute tablet_focus["in_front"]

    assert {:ok,
            %{
              "client_id" => ^phone,
              "device_name" => "Device " <> _,
              "delivery" => "held"
            }} =
             Relay.view_show(relay, agent, %{
               "sender_id" => "app:test",
               "message_id" => "focus-item",
               "target" => "message"
             })
  end

  test "quiet window starts and restarts at each disconnect, even after days connected" do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)

    dir =
      Path.join(System.tmp_dir!(), "harklight-focus-quiet-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    clock = :atomics.new(1, [])
    :atomics.put(clock, 1, 1_000)
    clock_fn = fn -> :atomics.get(clock, 1) end

    {:ok, relay} = Relay.start_link(store_path: Path.join(dir, "relay.sqlite3"), clock: clock_fn)
    on_exit(fn -> if Process.alive?(relay), do: GenServer.stop(relay) end)

    owner_certificate = TLSFixtures.certificate_der!(certs, "a1")
    {:ok, person} = Relay.operator_bootstrap(relay, owner_certificate)
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner_certificate)

    client_certificate = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, client} = Relay.setup_device_certificate(relay, person, client_certificate)
    agent = String.duplicate("d", 64)
    :ok = Relay.setup_device(relay, person, agent)
    :ok = Relay.setup_sender(relay, client, "app:test")
    :ok = Relay.save_dashboard_focus_permission(relay, person, owner_key, agent, :see, true)
    :ok = Relay.save_dashboard_focus_permission(relay, person, owner_key, agent, :push, true)

    assert {:ok, message_line} =
             Relay.request_line(
               relay,
               client,
               JSON.encode!(%{
                 "v" => 1,
                 "type" => "forward",
                 "op_id" => "00000000-0000-4000-8000-000000000961",
                 "message" => %{
                   "sender_id" => "app:test",
                   "message_id" => "quiet-window-item",
                   "created_at" => 1,
                   "title" => "Quiet window item"
                 }
               }) <> "\n"
             )

    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(message_line <> "\n")

    {:ok, session} = Relay.open_session(relay, client)
    report(relay, session, "quiet-first", "inbox")

    quiet_window = 24 * 60 * 60 * 1_000
    three_days = 3 * quiet_window
    twenty_three_hours = 23 * 60 * 60 * 1_000
    twenty_five_hours = 25 * 60 * 60 * 1_000

    :atomics.put(clock, 1, 1_000 + three_days)
    assert {:ok, %{"clients" => [connected]}} = Relay.focus_get(relay, agent)
    assert connected["client_id"] == client
    assert connected["connected"]
    assert connected["reported_at"] == 1_000

    :ok = Relay.close_session(relay, session)
    first_disconnect = :atomics.get(clock, 1)

    :atomics.put(clock, 1, first_disconnect + twenty_three_hours)
    assert {:ok, %{"clients" => [disconnected]}} = Relay.focus_get(relay, agent)
    assert disconnected["client_id"] == client
    refute disconnected["connected"]
    assert disconnected["reported_at"] == 1_000

    assert {:ok, %{"client_id" => ^client, "delivery" => "held"}} =
             Relay.view_show(relay, agent, %{
               "sender_id" => "app:test",
               "message_id" => "quiet-window-item",
               "target" => "message",
               "target_client" => client
             })

    :atomics.put(clock, 1, first_disconnect + twenty_five_hours)
    assert {:ok, %{"clients" => []}} = Relay.focus_get(relay, agent)

    state = :sys.get_state(relay)
    send(relay, {:focus_expire, client, state.focus_expiry_generations[client]})
    assert {:ok, %{"clients" => []}} = Relay.focus_get(relay, agent)
    refute Map.has_key?(:sys.get_state(relay).focus_clients, client)

    {:ok, reconnected} = Relay.open_session(relay, client)

    reconnected_report = %{
      "v" => 1,
      "type" => "focus_report",
      "ref" => "quiet-reconnect",
      "screen" => "inbox"
    }

    assert {:ok, lines} =
             Relay.session_request_line(
               relay,
               reconnected,
               JSON.encode!(reconnected_report) <> "\n"
             )

    assert [
             %{"type" => "ok", "ref" => "quiet-reconnect"},
             %{
               "type" => "view_show",
               "message_id" => "quiet-window-item",
               "held_ms" => held_ms
             }
           ] =
             Enum.map(lines, fn line ->
               {:ok, frame} = JSON.decode_line(line)
               frame
             end)

    assert held_ms == 2 * 60 * 60 * 1_000
    assert {:ok, %{"clients" => [reconnected_focus]}} = Relay.focus_get(relay, agent)
    assert reconnected_focus["connected"]
    assert reconnected_focus["reported_at"] == first_disconnect + twenty_five_hours

    :ok = Relay.close_session(relay, reconnected)
    second_disconnect = :atomics.get(clock, 1)
    :atomics.put(clock, 1, second_disconnect + twenty_three_hours)
    assert {:ok, %{"clients" => [_]}} = Relay.focus_get(relay, agent)
    :atomics.put(clock, 1, second_disconnect + twenty_five_hours)
    state = :sys.get_state(relay)
    send(relay, {:focus_expire, client, state.focus_expiry_generations[client]})
    assert {:ok, %{"clients" => []}} = Relay.focus_get(relay, agent)
  end

  test "self-removal and local revocation clear held focus and notify subscribers" do
    for action <- [:remove, :revoke] do
      {relay, person, client, agent} = focus_cleanup_case(Atom.to_string(action))

      assert {:ok, [%{"client_id" => ^client}]} = Relay.focus_stream_attach(relay, agent, self())

      assert {:ok, %{"client_id" => ^client, "delivery" => "held"}} =
               Relay.view_show(relay, agent, %{
                 "sender_id" => "app:test",
                 "message_id" => "cleanup-item",
                 "target" => "message",
                 "target_client" => client
               })

      before = :sys.get_state(relay)
      assert Map.has_key?(before.held_shows, client)
      assert Map.has_key?(before.focus_expiry_timers, client)
      assert MapSet.member?(before.focus_clients_by_person[person], client)
      assert MapSet.member?(before.focus_streams_by_person[person], self())

      case action do
        :remove -> assert {:ok, _reply} = Relay.device_remove(relay, client)
        :revoke -> assert :ok = Relay.revoke_device(relay, client)
      end

      assert_receive {:focus_change, %{"clients" => []}}, 1_000

      after_cleanup = :sys.get_state(relay)
      refute Map.has_key?(after_cleanup.focus_clients, client)
      refute Map.has_key?(after_cleanup.held_shows, client)
      refute Map.has_key?(after_cleanup.focus_expiry_timers, client)
      refute Map.has_key?(after_cleanup.focus_clients_by_person, person)
      assert MapSet.member?(after_cleanup.focus_streams_by_person[person], self())

      :ok = Relay.focus_stream_detach(relay, agent, self())
    end
  end

  defp focus_cleanup_case(suffix) do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)

    dir =
      Path.join(
        System.tmp_dir!(),
        "harklight-focus-cleanup-#{suffix}-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    {:ok, relay} = Relay.start_link(store_path: Path.join(dir, "relay.sqlite3"))
    on_exit(fn -> if Process.alive?(relay), do: GenServer.stop(relay) end)

    owner_certificate = TLSFixtures.certificate_der!(certs, "a1")
    {:ok, person} = Relay.operator_bootstrap(relay, owner_certificate)
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner_certificate)

    client_certificate = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, client} = Relay.setup_device_certificate(relay, person, client_certificate)
    agent = String.duplicate("f", 64)
    :ok = Relay.setup_device(relay, person, agent)
    :ok = Relay.setup_sender(relay, client, "app:test")
    :ok = Relay.save_dashboard_focus_permission(relay, person, owner_key, agent, :see, true)
    :ok = Relay.save_dashboard_focus_permission(relay, person, owner_key, agent, :push, true)

    assert {:ok, message_line} =
             Relay.request_line(
               relay,
               client,
               JSON.encode!(%{
                 "v" => 1,
                 "type" => "forward",
                 "op_id" => "00000000-0000-4000-8000-000000000971",
                 "message" => %{
                   "sender_id" => "app:test",
                   "message_id" => "cleanup-item",
                   "created_at" => 1,
                   "title" => "Cleanup item"
                 }
               }) <> "\n"
             )

    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(message_line <> "\n")

    {:ok, session} = Relay.open_session(relay, client)

    report(relay, session, "cleanup-#{suffix}", "message", %{
      "sender_id" => "app:test",
      "message_id" => "cleanup-item"
    })

    :ok = Relay.close_session(relay, session)

    {relay, person, client, agent}
  end

  defp report(relay, session, ref, screen, fields \\ %{}) do
    frame =
      Map.merge(
        %{"v" => 1, "type" => "focus_report", "ref" => ref, "screen" => screen},
        fields
      )

    assert {:ok, [line]} = Relay.session_request_line(relay, session, JSON.encode!(frame) <> "\n")
    assert {:ok, %{"type" => "ok", "ref" => ^ref}} = JSON.decode_line(line)
  end
end
