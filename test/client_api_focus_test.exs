defmodule AttentionServer.ClientAPIFocusTest do
  # PUT /v2/devices/me/focus and held shows (contract rulings round 11 r3,
  # focus spec r6 unchanged): today's focus report over a write, ended by its
  # ttl as today's disconnect ends it, with memory-only shows.
  use ExUnit.Case, async: false

  alias AttentionServer.{ChangeLog, JSON, Relay}
  alias AttentionServer.ClientAPI.{AgentShow, Context, Focus, Reads, Writes}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @agent String.duplicate("e", 64)

  setup do
    dir = Path.join(System.tmp_dir!(), "harklight-focus-v2-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    clock = :atomics.new(1, [])
    :atomics.put(clock, 1, 1_000)
    %{path: Path.join(dir, "state.sqlite3"), clock: clock}
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp request(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp relay(%{path: path, clock: clock}) do
    relay_opts = [
      store_path: path,
      agent_fixture_mode: true,
      clock: fn -> :atomics.get(clock, 1) end
    ]

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

    relay
  end

  defp at(%{clock: clock}, ms), do: :atomics.put(clock, 1, ms)

  defp item_id(relay) do
    {:ok, %{"items" => [%{"item_id" => id}]}} = Reads.inbox(relay, "reader")
    id
  end

  # The agent's focus list, in today's identifiers, as GET /v2/agent/focus
  # reads it before projecting.
  defp clients(relay) do
    assert {:ok, clients} = Relay.agent_focus(:sys.get_state(relay), "A", @agent)
    clients
  end

  # POST /v2/agent/show for the item, run in-process for the trusted agent key.
  # The request waits for a subscribed feed writer, so run it in a task when
  # the test process is subscribed.
  defp show(relay, item, target, status \\ 200) do
    key =
      "00000000-0000-4000-8000-" <>
        String.pad_leading("#{System.unique_integer([:positive])}", 12, "0")

    conn =
      Plug.Test.conn(
        :post,
        "/v2/agent/show",
        JSON.encode!(%{"item_id" => item, "target" => target})
      )
      |> Plug.Conn.put_req_header("idempotency-key", key)

    ctx = %Context{
      principal: %{kind: :device, person: "A", key: @agent},
      relay: relay,
      request_id: "focus-show-test"
    }

    conn = AgentShow.show(conn, ctx)
    assert conn.status == status
    assert {:ok, body} = JSON.decode_line(conn.resp_body <> "\n")
    body
  end

  test "the FYI list is foreground focus without item references", ctx do
    relay = relay(ctx)
    assert {:ok, 200, _, false} =
             Writes.focus(relay, "reader", %{"screen" => "inbox", "ttl_s" => 120})

    at(ctx, 2_000)
    report = %{"screen" => "fyi", "ttl_s" => 120}
    assert {:ok, 200, _, false} = Writes.focus(relay, "reader", report)

    assert [%{"screen" => "fyi", "in_front" => true, "reported_at" => 2_000} = focus] =
             clients(relay)
    refute Map.has_key?(focus, "message_id")

    assert {:error, "invalid_field", "item_id", false} =
             Writes.focus(relay, "reader", Map.put(report, "item_id", item_id(relay)))

    assert {:error, "invalid_field", "visible_entry_id", false} =
             Writes.focus(relay, "reader", Map.put(report, "visible_entry_id", "e1"))
  end

  test "an accepted show carries its original item id after the item is removed", ctx do
    relay = relay(ctx)
    item = item_id(relay)
    report = %{"screen" => "inbox", "ttl_s" => 120}
    assert {:ok, 200, _, false} = Writes.focus(relay, "reader", report)
    assert %{"show_id" => id, "state" => "held"} = show(relay, item, "message")

    :sys.replace_state(relay, fn state ->
      update_in(state.store.engine.messages, &Map.delete(&1, {"A", "app:test", "one"}))
    end)

    assert {:error, :not_found} = Reads.item(relay, "reader", item)
    assert %{"show_id" => ^id, "item_id" => ^item} =
             Focus.pending_show(relay, %{kind: :device, key: "reader", person: "A"})

    assert {:ok, 200, %{"show" => envelope}, false} = Writes.focus(relay, "reader", report)
    assert envelope == %{
      "device_id" => "reader",
      "show_id" => id,
      "item_id" => item,
      "target" => "message",
      "held_ms" => 0
    }
    assert {:ok, 200, %{"rev" => _} = response, false} = Writes.focus(relay, "reader", report)
    refute Map.has_key?(response, "show")
    assert %{"error" => "not_found"} = show(relay, item, "message", 404)
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
    request = Task.async(fn -> show(relay, item, "chat") end)

    # The subscribed feed is offered the show; this one reports it held.
    assert_receive {:client_api_show, "reader", show_id, result_ref}, 1_000
    send(result_ref, {result_ref, :held})

    assert %{"show_id" => ^show_id, "device_id" => "reader", "state" => "held"} =
             Task.await(request)

    # The feed computes held_ms when it writes the event (round 11 r4 §4).
    live = Focus.pending_show(relay, "reader")
    at(ctx, 5_400)
    assert Focus.pending_show(relay, "reader")["held_ms"] == 400

    assert %{
             "show_id" => ^show_id,
             "device_id" => "reader",
             "item_id" => ^item,
             "target" => "chat",
             "held_ms" => 0
           } = live

    at(ctx, 5_750)

    assert {:ok, 200, %{"show" => show}, false} =
             Writes.focus(relay, "reader", %{"screen" => "inbox", "ttl_s" => 120})

    assert show["show_id"] == live["show_id"]
    assert show["held_ms"] == 750
    assert Map.delete(show, "held_ms") == Map.delete(live, "held_ms")
    assert Focus.pending_show(relay, "reader") == nil

    # A feed that closes unsubscribes; later shows are not sent to it.
    :ok = Focus.unsubscribe_shows("reader")
    assert %{"state" => "held"} = show(relay, item, "message")
    refute_receive {:client_api_show, "reader", _, _}, 200

    assert {:ok, 200, %{"show" => %{"target" => "message"}}, false} =
             Writes.focus(relay, "reader", %{"screen" => "inbox", "ttl_s" => 120})

    # Consumed: the next write carries none. A show never marks anything Seen.
    assert {:ok, 200, body, false} =
             Writes.focus(relay, "reader", %{"screen" => "inbox", "ttl_s" => 120})

    refute Map.has_key?(body, "show")
    assert {:ok, %{"item" => %{"seen" => false}}} = Reads.item(relay, "reader", item)

    # Only the latest show is held, and it survives the device leaving the list.
    assert %{"state" => "held"} = show(relay, item, "message")
    assert %{"state" => "held"} = show(relay, item, "chat")

    assert {:ok, 200, _, false} =
             Writes.focus(relay, "reader", %{"screen" => "signed_out", "ttl_s" => 120})

    assert {:ok, 200, %{"show" => %{"target" => "chat"}}, false} =
             Writes.focus(relay, "reader", %{"screen" => "inbox", "ttl_s" => 120})
  end

  test "bad reports keep their field names", ctx do
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
  end

  test "browser focus is session-scoped, expires, and can never receive a show", ctx do
    relay = relay(ctx)
    browser = %{
      kind: :browser,
      person: "A",
      key: String.duplicate("ab", 32),
      owner_revision: 4,
      session_id: "s1"
    }

    :sys.replace_state(relay, fn state ->
      put_in(state.store.engine.owners["A"], %{key: browser.key, revision: 4})
    end)

    second = %{browser | session_id: "s2"}
    report = %{"screen" => "inbox", "device_name" => "Web, Safari", "ttl_s" => 60}

    assert {:error, "invalid_field", "device_name", false} =
             Writes.focus(relay, browser, Map.put(report, "device_name", String.duplicate("x", 129)))

    assert {:error, "invalid_field", "device_name", false} =
             Writes.focus(relay, "reader", report)

    assert {:ok, 200, %{"rev" => _}, false} = Writes.focus(relay, browser, report)
    assert [%{"kind" => "browser", "client_id" => one}] = clients(relay)
    assert one != browser.session_id

    renamed = %{report | "device_name" => "Web, other tab"}
    assert {:ok, 200, _, false} = Writes.focus(relay, browser, renamed)
    assert [%{"client_id" => ^one, "device_name" => "Web, other tab"}] = clients(relay)

    assert {:ok, 200, _, false} = Writes.focus(relay, second, report)
    assert length(clients(relay)) == 2
    state = :sys.get_state(relay)
    assert {:error, :forbidden} = Relay.agent_focus(state, "B", @agent)
    assert %{"error" => "not_found"} = show(relay, item_id(relay), "message", 404)

    assert :ok = Relay.end_browser_session(relay, browser.session_id)
    assert [%{"kind" => "browser", "device_name" => "Web, Safari"}] = clients(relay)
    assert {:ok, 200, _, false} = Writes.focus(relay, second, %{"screen" => "signed_out", "ttl_s" => 60})
    assert clients(relay) == []

    assert {:ok, 200, _, false} = Writes.focus(relay, second, %{report | "ttl_s" => 1})
    assert [%{"kind" => "browser"}] = clients(relay)
    state = :sys.get_state(relay)
    {:lapse, timer_id, _} = state.focus_clients[{:browser, "s2"}].client_api_token
    assert :ok = Focus.lapse(relay, {:browser, "s2"}, {:lapse, timer_id})
    assert clients(relay) == []
  end
end
