defmodule AttentionServer.AgentChangeLogTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    AgentAPIInProcess,
    AgentChangeLog,
    ChangeLog,
    JSON,
    PreChangeLogStore,
    Relay
  }

  alias AttentionServer.AgentAPI.FeedActivity
  alias Exqlite.Sqlite3

  @session "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @other_session "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

  setup do
    dir = Path.join(System.tmp_dir!(), "agent-change-log-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{path: Path.join(dir, "state.sqlite3")}
  end

  test "an empty agent starts at zero, with its own sequence across sessions", ctx do
    relay = start(ctx)
    setup_people(relay)
    assert {:ok, 0} = stored(relay, &AgentChangeLog.head(&1, {"A", "other"}))
    assert {:ok, [], false, 0} = read(relay, "origin")

    forward(relay, "origin", "m1", 1, @session)
    forward(relay, "other", "m2", 2, @session)
    forward(relay, "origin", "m3", 3, @other_session)

    assert {:ok, rows, false, position} = read(relay, "origin")
    assert messages(rows) == [{"m1", 1}, {"m3", position}]
    assert {:ok, rows, false, 1} = read(relay, "other")
    assert messages(rows) == [{"m2", 1}]

    assert {:ok, rows, false, ^position} =
             stored(relay, &AgentChangeLog.read(&1, {"A", "origin"}, 0, 500, @session))

    assert messages(rows) == [{"m1", 1}]
    GenServer.stop(relay)
  end

  test "cut-over seeds retained messages and leaves agents with no messages at zero", ctx do
    # Written before 0.2.0, with the change logs off.
    test_pid = self()

    PreChangeLogStore.write(ctx.path, fn store ->
      store = store |> pre_log_people() |> pre_log_forward("origin", "retained", 1, @session)

      assert {%{"type" => "ok", "seq" => seq, "event_id" => event_id}, store} =
               PreChangeLogStore.request(store, "other", %{
                 "type" => "discussion",
                 "op_id" => op(2),
                 "sender_id" => "app:test",
                 "message_id" => "retained",
                 "request_revision" => 1,
                 "text" => "Pre-cut-over chat"
               })

      assert {:error, :disabled} = AgentChangeLog.read(store, {"A", "origin"}, 0, 500, nil)

      assert %{
               agent_feed?: false,
               entry_positions: positions,
               messages_by_origin_session: sessions,
               advisory_sequences: advisories
             } = store.engine.route_b_index

      assert positions == %{} and sessions == %{} and advisories == %{}
      send(test_pid, {:seeded, seq, event_id})
      store
    end)

    assert_received {:seeded, seq, event_id}

    relay = start(ctx)
    retained = key("retained")

    assert %{
             agent_feed?: true,
             entry_positions: %{{^retained, ^event_id} => ^seq},
             messages_by_origin_session: %{{"A", "origin"} => %{@session => [^retained]}}
           } = stored(relay, & &1.engine.route_b_index)

    assert {:error, :cursor_expired} = read(relay, "origin")
    assert {:ok, [], false, 1} = read(relay, "origin", 1)

    assert {:ok, 1} =
             stored(relay, &AgentChangeLog.rev(&1, {"A", "origin"}, {:message, key("retained")}))

    assert {:ok, [], false, 0} = read(relay, "other")

    forward(relay, "origin", "new", 2, @session)
    assert {:ok, rows, false, 2} = read(relay, "origin", 1)
    assert messages(rows) == [{"new", 2}]
    GenServer.stop(relay)
  end

  test "a failed agent append rolls back both the person's change log and the message", ctx do
    relay = start(ctx)
    setup_people(relay)

    assert {:ok, person_head} =
             stored(relay, fn store ->
               assert :ok =
                        Sqlite3.execute(
                          store.db,
                          "CREATE TRIGGER fail_agent_log BEFORE INSERT ON agent_changes " <>
                            "BEGIN SELECT RAISE(ABORT, 'agent log failed'); END"
                        )

               ChangeLog.head(store, "A")
             end)

    assert %{"type" => "error", "reason" => "storage_unavailable"} =
             send_message(relay, "origin", "failed", 1, @session)

    assert {:ok, [], false, 0} = read(relay, "origin")
    assert {:ok, ^person_head} = stored(relay, &ChangeLog.head(&1, "A"))
    refute stored(relay, &Map.has_key?(&1.engine.messages, key("failed")))
    GenServer.stop(relay)
  end

  test "a person reply writes an entry on both logs in the same commit", ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "origin", "chat", 1, @session)
    assert {:ok, agent_before} = stored(relay, &AgentChangeLog.head(&1, {"A", "origin"}))
    assert {:ok, person_before} = stored(relay, &ChangeLog.head(&1, "A"))

    assert %{"type" => "ok", "seq" => seq} =
             request(relay, "other", %{
               "type" => "discussion",
               "op_id" => op(2),
               "sender_id" => "app:test",
               "message_id" => "chat",
               "request_revision" => 1,
               "text" => "Could you clarify?"
             })

    assert {:ok, [%{kind: :entry, resource: {:entry, ^seq}}], false, _} =
             read(relay, "origin", agent_before)

    assert {:ok, [], false, 0} = read(relay, "other")
    id = ChangeLog.entry_resource(seq)
    assert {:ok, rows, _, _} = stored(relay, &ChangeLog.read(&1, "A", person_before, 500))
    assert Enum.any?(rows, &match?(%{kind: :entry, resource_id: ^id}, &1))
    GenServer.stop(relay)
  end

  test "replay orders a raw answer after chat and includes sessionless close-outs", ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "origin", "chat", 1, @session)
    forward(relay, "origin", "sessionless", 2, nil)

    assert %{"type" => "ok", "seq" => chat_seq} =
             request(relay, "other", %{
               "type" => "discussion",
               "op_id" => op(3),
               "sender_id" => "app:test",
               "message_id" => "chat",
               "request_revision" => 1,
               "text" => "A question first"
             })

    assert %{"type" => "ok"} =
             request(relay, "other", %{
               "type" => "answer",
               "op_id" => op(4),
               "sender_id" => "app:test",
               "message_id" => "chat",
               "request_revision" => 1,
               "answer" => nil
             })

    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "lifecycle",
               "op_id" => op(5),
               "sender_id" => "app:test",
               "message_id" => "sessionless",
               "state" => "cleared",
               "at" => 6
             })

    assert {:ok, replay, false, _} = read(relay, "origin")
    chat_key = key("chat")
    sessionless_key = key("sessionless")

    assert [
             %{resource: {:entry, ^chat_seq}, rev: chat_rev},
             %{resource: {:message, ^chat_key}, rev: answer_rev},
             %{resource: {:message, ^sessionless_key}, rev: close_rev, session: nil}
           ] = replay

    assert chat_rev < answer_rev and answer_rev < close_rev

    stored(relay, fn store ->
      assert %{terminal: %{state: "answered", answer_present: true, answer: nil}} =
               store.engine.messages[chat_key]

      assert %{terminal: %{state: "cleared", answer_present: false}} =
               store.engine.messages[sessionless_key]

      assert {:ok, ^answer_rev} = AgentChangeLog.rev(store, {"A", "origin"}, {:message, chat_key})

      assert {:ok, ^close_rev} =
               AgentChangeLog.rev(store, {"A", "origin"}, {:message, sessionless_key})
    end)

    GenServer.stop(relay)
  end

  test "delivery updates the original entry resource, including on replay", ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "origin", "chat", 1, @session)
    scope = {"A", "origin"}

    assert %{"type" => "ok", "seq" => chat_seq} =
             request(relay, "other", %{
               "type" => "discussion",
               "op_id" => op(2),
               "sender_id" => "app:test",
               "message_id" => "chat",
               "request_revision" => 1,
               "text" => "An entry to deliver"
             })

    resource = {:entry, chat_seq}
    assert {:ok, first_rev} = stored(relay, &AgentChangeLog.rev(&1, scope, resource))

    assert {:ok, _, false, _} = AgentAPIInProcess.changes(relay, "A", "other")
    assert {:ok, ^first_rev} = stored(relay, &AgentChangeLog.rev(&1, scope, resource))
    assert {:ok, _, false, _} = AgentAPIInProcess.changes(relay, "A", "origin")
    assert {:ok, updated_rev} = stored(relay, &AgentChangeLog.rev(&1, scope, resource))
    assert updated_rev > first_rev

    assert {:ok, rows, false, head} = read(relay, "origin")
    assert [%{resource: ^resource, rev: ^updated_rev}] = Enum.filter(rows, &(&1.kind == :entry))

    assert {:ok, _, false, _} = AgentAPIInProcess.changes(relay, "A", "origin")
    assert {:ok, [], false, ^head} = read(relay, "origin", head)
    GenServer.stop(relay)
  end

  test "an answer's delivery updates the message resource with no answer entry", ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "origin", "question", 1, @session)

    assert %{"type" => "ok"} =
             request(relay, "other", %{
               "type" => "answer",
               "op_id" => op(2),
               "sender_id" => "app:test",
               "message_id" => "question",
               "request_revision" => 1,
               "answer" => "Yes"
             })

    assert {:ok, _, false, answer_rev} = read(relay, "origin")

    assert {:ok, _, false, _} = AgentAPIInProcess.changes(relay, "A", "origin")

    resource = {:message, key("question")}

    assert {:ok, [%{resource: ^resource, rev: delivery_rev}], false, delivery_rev} =
             read(relay, "origin", answer_rev)

    assert delivery_rev > answer_rev
    GenServer.stop(relay)
  end

  test "ending a session revises only messages from the affected session", ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "origin", "affected", 1, @session)
    forward(relay, "origin", "unaffected", 2, @other_session)
    assert {:ok, _, false, previous} = read(relay, "origin")

    assert {:ok, 200, _, false} =
             AgentAPIInProcess.write(
               relay,
               "A",
               "origin",
               :session_end,
               %{session: @session},
               %{}
             )

    assert {:ok, rows, false, next} = read(relay, "origin", previous)
    assert [{"affected", rev}] = messages(rows)
    assert previous < rev and rev <= next
    GenServer.stop(relay)
  end

  test "open feeds and a recent batch cover sessions independently", ctx do
    relay = start(ctx)
    setup_people(relay)
    supported =
      Map.merge(forward_frame("covered", 1, @session), %{
        "route" => "claude-code",
        "cli_version" => "fixture",
        "unsupported_reason" => nil
      })

    assert %{"type" => "ok"} = request(relay, "origin", supported)
    forward(relay, "origin", "uncovered", 2, @other_session)

    scope = {"A", "origin", @session}
    other = {"A", "origin", @other_session}

    feed = %{
      relay: relay,
      principal: %{kind: :device, person: "A", key: "origin"},
      params: %{session: @session}
    }

    FeedActivity.observe(feed, :open)
    FeedActivity.observe(feed, :open)
    FeedActivity.observe(feed, :close)

    state = :sys.get_state(relay)
    assert FeedActivity.listening?(state, scope)
    refute FeedActivity.listening?(state, other)

    assert %{"state" => "listening"} =
             FeedActivity.item_availability(
               state.store.engine,
               key("covered")
             )

    assert %{"state" => "unsupported"} =
             FeedActivity.item_availability(
               state.store.engine,
               key("uncovered")
             )

    FeedActivity.observe(feed, :batch)
    FeedActivity.observe(feed, :close)
    state = :sys.get_state(relay)
    assert FeedActivity.listening?(state, scope)

    {_, token} = state.agent_feed_activity.batches[scope]
    send(relay, {:agent_feed_activity, {:expire, token}, scope})

    refute FeedActivity.listening?(:sys.get_state(relay), scope)

    FeedActivity.observe(feed, :open)
    assert FeedActivity.listening?(:sys.get_state(relay), scope)
    GenServer.stop(relay)

    relay = start(ctx)
    state = :sys.get_state(relay)
    refute FeedActivity.listening?(state, scope)

    assert %{"state" => "not_listening"} =
             FeedActivity.item_availability(
               state.store.engine,
               key("covered")
             )

    GenServer.stop(relay)
  end

  test "a disabled interval expires the previous cursor and rebaselines on reopen", ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "origin", "before", 1, @session)
    assert {:ok, _, false, previous} = read(relay, "origin")
    GenServer.stop(relay)

    # A Server before 0.2.0 run on this store again commits with the logs off.
    PreChangeLogStore.write(ctx.path, fn store ->
      store
      |> pre_log_forward("origin", "while-off", 2, @session)
      |> pre_log_forward("other", "first-while-off", 3, @other_session)
    end)

    relay = start(ctx)
    assert {:error, :cursor_expired} = read(relay, "origin", previous)
    assert {:ok, head} = stored(relay, &AgentChangeLog.head(&1, {"A", "origin"}))
    assert head > previous
    assert {:ok, [], false, ^head} = read(relay, "origin", head)
    assert {:ok, 1} = stored(relay, &AgentChangeLog.head(&1, {"A", "other"}))
    assert {:error, :cursor_expired} = read(relay, "other")
    GenServer.stop(relay)
  end

  test "pruning expires cursors without dropping snapshot resource revs", ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "origin", "old", 1, @session)
    assert {:ok, _, false, old_head} = read(relay, "origin")

    stored(relay, fn store ->
      assert :ok = Sqlite3.execute(store.db, "UPDATE agent_changes SET at_ms = 1")
    end)

    forward(relay, "origin", "new", 2, @other_session)

    stored(relay, fn store ->
      assert {:ok, _} = ChangeLog.prune(store, 2)
      assert {:ok, 1} = AgentChangeLog.rev(store, {"A", "origin"}, {:message, key("old")})
    end)

    assert {:error, :cursor_expired} = read(relay, "origin")
    assert {:ok, rows, false, head} = read(relay, "origin", old_head)
    assert messages(rows) == [{"new", head}]
    GenServer.stop(relay)
  end

  defp start(ctx) do
    :ok = PreChangeLogStore.convert_fixture(ctx.path)
    assert {:ok, relay} = Relay.start_link(store_path: ctx.path, agent_fixture_mode: true)
    relay
  end

  defp pre_log_people(store) do
    store = PreChangeLogStore.setup_person(store, "A")

    Enum.reduce(["origin", "other"], store, fn device, store ->
      store
      |> PreChangeLogStore.setup_device("A", device)
      |> PreChangeLogStore.setup_sender(device, "app:test")
    end)
  end

  defp pre_log_forward(store, device, id, n, session) do
    assert {%{"type" => "ok"}, store} =
             PreChangeLogStore.request(store, device, forward_frame(id, n, session))

    store
  end

  defp setup_people(relay) do
    assert :ok = Relay.setup_person(relay, "A")

    for device <- ["origin", "other"] do
      assert :ok = Relay.setup_device(relay, "A", device)
      assert :ok = Relay.setup_sender(relay, device, "app:test")
    end
  end

  defp forward(relay, device, id, n, session) do
    assert %{"type" => "ok"} = send_message(relay, device, id, n, session)
  end

  defp send_message(relay, device, id, n, session),
    do: request(relay, device, forward_frame(id, n, session))

  defp forward_frame(id, n, session) do
    frame = %{
      "type" => "forward",
      "op_id" => op(n),
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => id,
        "created_at" => n,
        "title" => "Question #{id}"
      }
    }

    if session, do: Map.put(frame, "session_handle", session), else: frame
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp request(relay, device, frame) do
    line = JSON.encode!(Map.put(frame, "v", 1)) <> "\n"
    assert {:ok, line} = Relay.request_line(relay, device, line)
    assert {:ok, reply} = JSON.decode_line(line)
    reply
  end

  defp key(id), do: {"A", "app:test", id}

  defp messages(rows),
    do: for(%{kind: :message, resource: {:message, {_, _, id}}, rev: rev} <- rows, do: {id, rev})

  defp read(relay, agent, after_rev \\ 0),
    do: stored(relay, &AgentChangeLog.read(&1, {"A", agent}, after_rev, 500, nil))

  defp stored(relay, fun) do
    :ok = :sys.suspend(relay)

    try do
      fun.(:sys.get_state(relay).store)
    after
      :ok = :sys.resume(relay)
    end
  end
end
