defmodule AttentionServer.FocusRelayTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DeviceIdentity, JSON, Relay, TLSFixtures}
  alias AttentionServer.ClientAPI.{AgentShow, Context, Resources, Writes}

  # Focus is reported by PUT /v2/devices/me/focus (Writes.focus), whose ttl
  # replaces the 0.1 session; client_api_focus_test covers the ttl itself.
  test "background reports keep the last foreground rank and default target" do
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

    tablet = String.duplicate("c", 64)
    agent = String.duplicate("e", 64)
    phone_certificate = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, phone} = Relay.setup_device_certificate(relay, person, phone_certificate)

    :ok = Relay.setup_device(relay, person, tablet)
    :ok = Relay.setup_device(relay, person, agent)
    :ok = Relay.setup_sender(relay, phone, "app:test")

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

    report(relay, tablet, "inbox")

    :atomics.put(clock, 1, 2_000)
    report(relay, phone, "chat", item_id(relay, person, "focus-item"))

    :atomics.put(clock, 1, 3_000)
    report(relay, tablet, "not_in_front")

    :atomics.put(clock, 1, 4_000)
    report(relay, phone, "not_in_front")

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

  test "focus changes per device and a signed-out report ends only that device's focus" do
    clock = :atomics.new(1, [])
    :atomics.put(clock, 1, 1_000)

    {relay, person, client, agent, _owner_key} =
      focus_cleanup_case("lifetime", clock: fn -> :atomics.get(clock, 1) end)

    other = String.duplicate("a", 64)
    :ok = Relay.setup_device(relay, person, other)
    report(relay, other, "settings")

    assert {:ok, %{"clients" => clients}} = focus_get(relay, person, agent)
    assert Enum.find(clients, &(&1["client_id"] == client))["screen"] == "message"
    assert Enum.all?(clients, & &1["connected"])
    assert Enum.all?(clients, &(&1["reported_at"] == 1_000))

    :atomics.put(clock, 1, 2_000)
    report(relay, client, "inbox")
    assert {:ok, %{"clients" => clients}} = focus_get(relay, person, agent)
    assert Enum.find(clients, &(&1["client_id"] == client))["screen"] == "inbox"
    assert Enum.find(clients, &(&1["client_id"] == other))["screen"] == "settings"
    assert Enum.find(clients, &(&1["client_id"] == other))["reported_at"] == 1_000

    assert {:ok, _} = Relay.focus_stream_attach(relay, agent, self())
    report(relay, client, "signed_out")
    assert_receive {:focus_change, %{"clients" => [%{"client_id" => ^other}]}}, 1_000
    assert {:ok, %{"clients" => [%{"client_id" => ^other}]}} = focus_get(relay, person, agent)

    assert {404, %{"error" => "not_found"}} =
             show(relay, person, agent, "cleanup-item", %{
               "target" => "message",
               "device_id" => client
             })

    report(relay, client, "chat", item_id(relay, person, "cleanup-item"))
    assert {:ok, %{"clients" => clients}} = focus_get(relay, person, agent)
    assert Enum.find(clients, &(&1["client_id"] == client))["screen"] == "chat"
  end

  test "self-removal and owner and local revocation clear held focus and notify subscribers" do
    for action <- [:remove, :owner_revoke, :revoke] do
      {relay, person, client, agent, owner_key} = focus_cleanup_case(Atom.to_string(action))

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
        :owner_revoke -> assert :ok = Relay.owner_revoke_device(relay, person, owner_key, client)
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

    report(relay, client, "message", item_id(relay, person, "cleanup-item"))
    {relay, person, client, agent, owner_key}
  end

  # The agent's focus list in today's identifiers, as GET /v2/agent/focus
  # reads it before projecting.
  defp focus_get(relay, person, agent) do
    with {:ok, clients} <- Relay.agent_focus(:sys.get_state(relay), person, agent),
         do: {:ok, %{"clients" => clients}}
  end

  defp item_id(relay, person, message_id) do
    {:ok, item} =
      Resources.item_id(:sys.get_state(relay).store.engine, {person, "app:test", message_id})

    item
  end

  # POST /v2/agent/show for one stored message, run in-process for the
  # trusted agent key. No feed writer is subscribed, so a show is held.
  defp show(relay, person, agent, message_id, fields) do
    item = item_id(relay, person, message_id)

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

  # PUT /v2/devices/me/focus, in-process, with a ttl that outlasts the test.
  defp report(relay, device, screen, item \\ nil) do
    body = %{"screen" => screen, "ttl_s" => 120}
    body = if item, do: Map.put(body, "item_id", item), else: body
    assert {:ok, 200, %{"rev" => _}, false} = Writes.focus(relay, device, body)
  end
end
