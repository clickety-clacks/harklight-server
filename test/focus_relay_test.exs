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
