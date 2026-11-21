defmodule AttentionServer.ClientAPIFocusTest do
  # PUT /v2/devices/me/focus and held shows (contract rulings round 11 r3,
  # focus spec r6 unchanged): today's focus report over a write, ended by its
  # ttl as today's disconnect ends it, with memory-only shows.
  use ExUnit.Case, async: false

  alias AttentionServer.{ChangeLog, JSON, Relay}
  alias AttentionServer.ClientAPI.{Focus, Reads, Writes}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @agent String.duplicate("e", 64)

  setup do
    dir = Path.join(System.tmp_dir!(), "harklight-focus-v2-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    flags = Path.join(dir, "flags.json")
    File.write!(flags, ~s({"client_api_v2":true}))
    clock = :atomics.new(1, [])
    :atomics.put(clock, 1, 1_000)
    %{path: Path.join(dir, "state.sqlite3"), flags: flags, clock: clock}
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp request(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp relay(%{path: path, flags: flags, clock: clock}, opts \\ []) do
    flags = Keyword.get(opts, :flags, flags)

    relay_opts = [
      store_path: path,
      agent_fixture_mode: true,
      clock: fn -> :atomics.get(clock, 1) end
    ]

    relay_opts =
      if flags, do: Keyword.put(relay_opts, :feature_flags_path, flags), else: relay_opts

    assert {:ok, relay} = Relay.start_link(relay_opts)
    on_exit(fn -> if Process.alive?(relay), do: GenServer.stop(relay) end)

    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_device(relay, "A", "tablet")
    assert :ok = Relay.setup_device(relay, "A", @agent)
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(1),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "one",
                 "created_at" => 1,
                 "title" => "One"
               }
             })

    # The agent may see focus and push views, as an owner's switches allow.
    :sys.replace_state(relay, fn state ->
      engine = state.store.engine
      device = %{engine.devices[@agent] | see_focus: true, push_view: true}
      devices = Map.put(engine.devices, @agent, device)
      %{state | store: %{state.store | engine: %{engine | devices: devices}}}
    end)

    relay
  end

  defp at(%{clock: clock}, ms), do: :atomics.put(clock, 1, ms)

  defp item_id(relay) do
    {:ok, %{"items" => [%{"item_id" => id}]}} = Reads.inbox(relay, "reader")
    id
  end

  defp clients(relay) do
    assert {:ok, %{"clients" => clients}} = Relay.focus_get(relay, @agent)
    clients
  end

  defp push_view(relay, target \\ "message") do
    Relay.view_show(relay, @agent, %{
      "sender_id" => "app:test",
      "message_id" => "one",
      "target" => target
    })
  end

  test "a focus write is today's focus report, in today's identifiers", ctx do
    relay = relay(ctx)
    item = item_id(relay)

    assert %{"event_id" => event, "seq" => seq} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(2),
               "sender_id" => "app:test",
               "message_id" => "one",
               "request_revision" => 1,
               "text" => "Hi"
             })

    entry = AttentionServer.ClientAPI.Resources.entry_id(seq)
    {:ok, position} = ChangeLog.head(:sys.get_state(relay).store, "A")

    report = %{"screen" => "chat", "item_id" => item, "visible_entry_id" => entry, "ttl_s" => 120}
    assert {:ok, 200, %{"rev" => ^position} = body, false} = Writes.focus(relay, "reader", report)
    refute Map.has_key?(body, "show")

    assert [client] = clients(relay)

    assert %{
             "client_id" => "reader",
             "screen" => "chat",
             "sender_id" => "app:test",
             "message_id" => "one",
             "visible_event_id" => ^event,
             "reported_at" => 1_000,
             "in_front" => true,
             "connected" => true
           } = client

    # Nothing about focus is written to the change log.
    assert ChangeLog.head(:sys.get_state(relay).store, "A") == {:ok, position}

    # A renewal with unchanged fields is liveness only; not_in_front is not activity.
    at(ctx, 2_000)
    assert {:ok, 200, _, false} = Writes.focus(relay, "reader", report)
    assert [%{"reported_at" => 1_000}] = clients(relay)

    at(ctx, 3_000)

    assert {:ok, 200, _, false} =
             Writes.focus(relay, "reader", %{"screen" => "not_in_front", "ttl_s" => 120})

    assert [%{"reported_at" => 1_000, "in_front" => false, "screen" => "not_in_front"}] =
             clients(relay)

    # The most recently active device is listed first and is the default target.
    at(ctx, 4_000)

    assert {:ok, 200, _, false} =
             Writes.focus(relay, "tablet", %{"screen" => "inbox", "ttl_s" => 120})

    assert [%{"client_id" => "tablet"}, %{"client_id" => "reader"}] = clients(relay)
  end

  test "the ttl lapsing ends focus as today's disconnect does; signed_out ends it at once", ctx do
    relay = relay(ctx)

    assert {:ok, 200, _, false} =
             Writes.focus(relay, "reader", %{"screen" => "inbox", "ttl_s" => 1})

    assert {:ok, 200, _, false} =
             Writes.focus(relay, "tablet", %{"screen" => "inbox", "ttl_s" => 120})

    assert {:ok, _initial} = Relay.focus_stream_attach(relay, @agent, self())

    assert_receive {:focus_change, %{"clients" => [%{"client_id" => "tablet"}]}}, 2_500
    assert [%{"client_id" => "tablet"}] = clients(relay)

    # A renewal before the lapse keeps the device: only the newest report's ttl counts.
    assert {:ok, 200, _, false} =
             Writes.focus(relay, "reader", %{"screen" => "inbox", "ttl_s" => 1})

    assert {:ok, 200, _, false} =
             Writes.focus(relay, "reader", %{"screen" => "inbox", "ttl_s" => 120})

    Process.sleep(1_300)
    assert Enum.any?(clients(relay), &(&1["client_id"] == "reader"))

    assert {:ok, 200, _, false} =
             Writes.focus(relay, "tablet", %{"screen" => "signed_out", "ttl_s" => 120})

    refute Enum.any?(clients(relay), &(&1["client_id"] == "tablet"))
  end

  test "a held show arrives once on the next focus write and as an ephemeral feed event", ctx do
    relay = relay(ctx)
    item = item_id(relay)

    assert {:ok, 200, _, false} =
             Writes.focus(relay, "reader", %{"screen" => "inbox", "ttl_s" => 120})

    :ok = Focus.subscribe_shows("reader")

    at(ctx, 5_000)
    assert {:ok, %{"client_id" => "reader", "delivery" => "held"}} = push_view(relay, "chat")

    assert_receive {:client_api_show, "reader", message}, 1_000

    # The feed computes held_ms when it writes the event (round 11 r4 §4).
    live = Focus.wire(message, 5_000)
    assert Focus.wire(message, 5_400)["held_ms"] == 400

    assert %{"device_id" => "reader", "item_id" => ^item, "target" => "chat", "held_ms" => 0} =
             live

    at(ctx, 5_750)

    assert {:ok, 200, %{"show" => show}, false} =
             Writes.focus(relay, "reader", %{"screen" => "inbox", "ttl_s" => 120})

    assert show["show_id"] == live["show_id"]
    assert show["held_ms"] == 750
    assert Map.delete(show, "held_ms") == Map.delete(live, "held_ms")

    # A feed that closes unsubscribes; later shows are not sent to it.
    :ok = Focus.unsubscribe_shows("reader")
    assert {:ok, _} = push_view(relay, "message")
    refute_receive {:client_api_show, "reader", _}, 200

    assert {:ok, 200, %{"show" => %{"target" => "message"}}, false} =
             Writes.focus(relay, "reader", %{"screen" => "inbox", "ttl_s" => 120})

    # Consumed: the next write carries none. A show never marks anything Seen.
    assert {:ok, 200, body, false} =
             Writes.focus(relay, "reader", %{"screen" => "inbox", "ttl_s" => 120})

    refute Map.has_key?(body, "show")
    assert {:ok, %{"item" => %{"seen" => false}}} = Reads.item(relay, "reader", item)

    # Only the latest show is held, and it survives the device leaving the list.
    assert {:ok, _} = push_view(relay, "message")
    assert {:ok, _} = push_view(relay, "chat")

    assert {:ok, 200, _, false} =
             Writes.focus(relay, "reader", %{"screen" => "signed_out", "ttl_s" => 120})

    assert {:ok, 200, %{"show" => %{"target" => "chat"}}, false} =
             Writes.focus(relay, "reader", %{"screen" => "inbox", "ttl_s" => 120})
  end

  test "bad reports keep their field names; browsers are forbidden", ctx do
    relay = relay(ctx)
    item = item_id(relay)

    for {report, field} <- [
          {%{"screen" => "elsewhere", "ttl_s" => 60}, "screen"},
          {%{"screen" => "message", "ttl_s" => 60}, "item_id"},
          {%{"screen" => "inbox", "item_id" => item, "ttl_s" => 60}, "item_id"},
          {%{"screen" => "message", "item_id" => "i999", "ttl_s" => 60}, "item_id"},
          {%{"screen" => "message", "item_id" => item, "visible_entry_id" => "e1", "ttl_s" => 60},
           "visible_entry_id"},
          {%{"screen" => "chat", "item_id" => item, "visible_entry_id" => "e999", "ttl_s" => 60},
           "visible_entry_id"},
          {%{"screen" => "inbox", "ttl_s" => 0}, "ttl_s"},
          {%{"screen" => "inbox", "ttl_s" => 3601}, "ttl_s"},
          {%{"screen" => "inbox"}, "ttl_s"},
          {%{"screen" => "inbox", "ttl_s" => 60, "extra" => 1}, "extra"}
        ] do
      assert {:error, "invalid_field", ^field, false} = Writes.focus(relay, "reader", report)
    end

    assert clients(relay) == []

    browser = %{
      kind: :browser,
      person: "A",
      key: String.duplicate("ab", 32),
      owner_revision: 4,
      session_id: "s1"
    }

    :sys.replace_state(relay, fn state ->
      engine = state.store.engine
      owners = Map.put(engine.owners, "A", %{key: browser.key, revision: 4})
      %{state | store: %{state.store | engine: %{engine | owners: owners}}}
    end)

    assert {:error, "forbidden", nil, false} =
             Writes.focus(relay, browser, %{"screen" => "inbox", "ttl_s" => 60})
  end

  test "with client_api_v2 off there is no focus write and no relay change", ctx do
    relay = relay(ctx, flags: nil)
    before = :sys.get_state(relay)

    assert {:error, "not_found", nil, false} =
             Writes.focus(relay, "reader", %{"screen" => "inbox", "ttl_s" => 60})

    after_ = :sys.get_state(relay)
    assert after_.focus_clients == before.focus_clients
    assert after_.held_shows == before.held_shows
  end
end
