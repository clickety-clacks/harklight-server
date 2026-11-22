defmodule AttentionServer.FocusRelayTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DeviceIdentity, JSON, Relay, TLSFixtures}
  alias AttentionServer.ClientAPI.{AgentShow, Context, Resources}

  test "background reports within a session keep the last foreground rank and default target" do
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

    :atomics.put(clock, 1, 2_000)
    {:ok, phone_session} = Relay.open_session(relay, phone)

    report(relay, phone_session, "phone-chat", "chat", %{
      "sender_id" => "app:test",
      "message_id" => "focus-item",
      "visible_event_id" => "4182"
    })

    :atomics.put(clock, 1, 3_000)
    report(relay, tablet_session, "tablet-background", "not_in_front")

    :atomics.put(clock, 1, 4_000)
    report(relay, phone_session, "phone-background", "not_in_front")

    assert {:ok, %{"clients" => [phone_focus, tablet_focus]}} = focus_get(relay, person, agent)
    assert phone_focus["client_id"] == phone
    assert phone_focus["screen"] == "not_in_front"
    assert phone_focus["reported_at"] == 2_000
    refute phone_focus["in_front"]

    assert tablet_focus["client_id"] == tablet
    assert tablet_focus["screen"] == "not_in_front"
    assert tablet_focus["reported_at"] == 1_000
    refute tablet_focus["in_front"]

    assert {200,
            %{
              "device_id" => ^phone,
              "device_name" => "Device " <> _,
              "state" => "held"
            }} = show(relay, person, agent, "focus-item", %{"target" => "message"})
  end

  test "focus lasts beyond a day, changes per device, and ends immediately with its session" do
    clock = :atomics.new(1, [])
    :atomics.put(clock, 1, 1_000)

    {relay, person, client, agent, session} =
      focus_cleanup_case("lifetime", clock: fn -> :atomics.get(clock, 1) end)

    other = String.duplicate("a", 64)
    :ok = Relay.setup_device(relay, person, other)
    {:ok, other_session} = Relay.open_session(relay, other)
    report(relay, other_session, "other-front", "settings")

    :atomics.put(clock, 1, 1_000 + 3 * 24 * 60 * 60 * 1_000)
    assert {:ok, %{"clients" => clients}} = focus_get(relay, person, agent)
    assert Enum.find(clients, &(&1["client_id"] == client))["screen"] == "message"
    assert Enum.all?(clients, & &1["connected"])
    assert Enum.all?(clients, &(&1["reported_at"] == 1_000))

    report(relay, session, "changed-focus", "inbox")
    assert {:ok, %{"clients" => clients}} = focus_get(relay, person, agent)
    assert Enum.find(clients, &(&1["client_id"] == client))["screen"] == "inbox"
    assert Enum.find(clients, &(&1["client_id"] == other))["screen"] == "settings"

    assert {:ok, _} = Relay.focus_stream_attach(relay, agent, self())
    :ok = Relay.close_session(relay, session)
    assert_receive {:focus_change, %{"clients" => [%{"client_id" => ^other}]}}, 1_000
    assert {:ok, %{"clients" => [%{"client_id" => ^other}]}} = focus_get(relay, person, agent)

    assert {404, %{"error" => "not_found"}} =
             show(relay, person, agent, "cleanup-item", %{
               "target" => "message",
               "device_id" => client
             })

    {:ok, replacement} = Relay.open_session(relay, client)
    assert {:ok, %{"clients" => [%{"client_id" => ^other}]}} = focus_get(relay, person, agent)

    report(relay, replacement, "new-session-focus", "chat", %{
      "sender_id" => "app:test",
      "message_id" => "cleanup-item"
    })

    assert {:ok, %{"clients" => clients}} = focus_get(relay, person, agent)
    assert Enum.find(clients, &(&1["client_id"] == client))["screen"] == "chat"

    # Replacing the session ends its focus; a late close of the fenced session
    # must not clear a fresh report from its replacement.
    {:ok, latest} = Relay.open_session(relay, client)
    assert {:ok, %{"clients" => [%{"client_id" => ^other}]}} = focus_get(relay, person, agent)
    report(relay, latest, "latest-session", "settings")
    :ok = Relay.close_session(relay, replacement)
    assert {:ok, %{"clients" => clients}} = focus_get(relay, person, agent)
    assert Enum.find(clients, &(&1["client_id"] == client))["screen"] == "settings"

    report(relay, latest, "signed-out", "signed_out")
    assert {:ok, %{"clients" => [%{"client_id" => ^other}]}} = focus_get(relay, person, agent)
    refute :sys.get_state(relay).sessions[latest].focus_opted_in
  end

  test "listener disconnect clears only that device focus" do
    {relay, person, client, agent, _session} = focus_cleanup_case("disconnect")
    :ok = Relay.register_tls_listener(relay, self())
    {:ok, connected} = Relay.open_tls_session(relay, client, nil, self())
    report(relay, connected, "listener-front", "inbox")
    assert {:ok, [%{"client_id" => ^client}]} = Relay.focus_stream_attach(relay, agent, self())

    :ok = Relay.unregister_tls_listener(relay, self())
    assert_receive {:focus_change, %{"clients" => []}}, 1_000
    assert {:ok, %{"clients" => []}} = focus_get(relay, person, agent)
    assert {:error, :unknown_session} = Relay.session_request_line(relay, connected, "{}\n")
  end

  test "self-removal and local revocation clear held focus and notify subscribers" do
    for action <- [:remove, :revoke] do
      {relay, person, client, agent, _session} = focus_cleanup_case(Atom.to_string(action))

      assert {:ok, [%{"client_id" => ^client}]} = Relay.focus_stream_attach(relay, agent, self())

      assert {200, %{"device_id" => ^client, "state" => "held"}} =
               show(relay, person, agent, "cleanup-item", %{
                 "target" => "message",
                 "device_id" => client
               })

      before = :sys.get_state(relay)
      assert Map.has_key?(before.held_shows, client)
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
      refute Map.has_key?(after_cleanup.focus_clients_by_person, person)
      assert MapSet.member?(after_cleanup.focus_streams_by_person[person], self())
    end
  end

  defp focus_cleanup_case(suffix, opts \\ []) do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)

    dir =
      Path.join(
        System.tmp_dir!(),
        "harklight-focus-cleanup-#{suffix}-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    {:ok, relay} =
      Relay.start_link(Keyword.put(opts, :store_path, Path.join(dir, "relay.sqlite3")))

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

    {relay, person, client, agent, session}
  end

  # The agent's focus list in today's identifiers, as GET /v2/agent/focus
  # reads it before projecting.
  defp focus_get(relay, person, agent) do
    with {:ok, clients} <- Relay.agent_focus(:sys.get_state(relay), person, agent),
         do: {:ok, %{"clients" => clients}}
  end

  # POST /v2/agent/show for one stored message, run in-process for the
  # trusted agent key. No feed writer is subscribed, so a show is held.
  defp show(relay, person, agent, message_id, fields) do
    {:ok, item} =
      Resources.item_id(:sys.get_state(relay).store.engine, {person, "app:test", message_id})

    key =
      "00000000-0000-4000-8000-" <>
        String.pad_leading("#{System.unique_integer([:positive])}", 12, "0")

    conn =
      Plug.Test.conn(:post, "/v2/agent/show", JSON.encode!(Map.put(fields, "item_id", item)))
      |> Plug.Conn.put_req_header("idempotency-key", key)

    ctx = %Context{
      principal: %{kind: :device, person: person, key: agent},
      relay: relay,
      request_id: "focus-relay-show"
    }

    conn = AgentShow.show(conn, ctx)
    assert {:ok, body} = JSON.decode_line(conn.resp_body <> "\n")
    {conn.status, body}
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
