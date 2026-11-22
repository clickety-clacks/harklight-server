defmodule AttentionServer.DeliveryAcksTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    AgentAPIInProcess,
    Engine,
    JSON,
    PreChangeLogStore,
    Relay,
    RelayReaders,
    Store
  }

  alias Exqlite.Sqlite3

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(to_string(n), 12, "0")}"

  setup do
    dir = Path.join(System.tmp_dir!(), "delivery-acks-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "state.sqlite3")
    relay = start_supervised!({Relay, store_path: path})
    :ok = Relay.setup_person(relay, "person")
    for key <- ~w(origin phone tablet), do: :ok = Relay.setup_device(relay, "person", key)
    :ok = Relay.setup_person(relay, "other")
    :ok = Relay.setup_device(relay, "other", "stranger")
    :ok = Relay.setup_sender(relay, "origin", "app:test")
    send_request(relay)
    %{relay: relay, path: path}
  end

  defp request(relay, key, frame) do
    {:ok, line} =
      Relay.request_line(
        relay,
        key,
        JSON.encode!(
          if(frame["type"] == "answer_no_comment", do: frame, else: Map.put(frame, "v", 1))
        ) <> "\n"
      )

    {:ok, reply} = JSON.decode_line(line)
    reply
  end

  defp send_request(relay, origin \\ "origin") do
    assert %{"type" => "ok"} =
             request(relay, origin, %{
               "type" => "forward",
               "op_id" => op(1),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "question",
                 "display_name" => "Build helper",
                 "created_at" => 1,
                 "title" => "Check this"
               }
             })
  end

  defp discussion(relay, reader \\ "phone", n \\ 2) do
    assert %{"type" => "ok"} =
             reply =
             request(relay, reader, %{
               "type" => "discussion",
               "op_id" => op(n),
               "sender_id" => "app:test",
               "message_id" => "question",
               "request_revision" => 1,
               "text" => "Please check"
             })

    reply["event_id"]
  end

  # Client API v2 never records listening support. A store written by a 0.1
  # origin carries it, and its "ack2" member sets ack2_expected on ACK1.
  defp support(relay, capable, n \\ 3) do
    PreChangeLogStore.in_relay(
      relay,
      &PreChangeLogStore.origin_support(&1, "origin", %{
        "type" => "support",
        "op_id" => op(n),
        "session_handle" => @handle,
        "route" => "codex",
        "cli_version" => "fixture",
        "unsupported_reason" => nil,
        "ack2" => capable
      })
    )

    session = :sys.get_state(relay).store.engine.origin_sessions[{"person", "origin", @handle}]
    assert session.support["ack2"] == capable
  end

  defp item(relay, id \\ "question"),
    do: AgentAPIInProcess.item_id(relay, "person", "app:test", id)

  # The v2 entry id of the person's chat turn on "question".
  defp person_entry(relay) do
    assert {:ok, %{"entries" => entries}} =
             AgentAPIInProcess.entries(relay, "person", "origin", item(relay))

    [%{"entry_id" => entry}] = Enum.filter(entries, &(&1["author"] == "person"))
    entry
  end

  # `GET /v2/agent/changes` from `cursor`; ACK1 commits for what it returns.
  defp changes(relay, cursor, opts \\ [], key \\ "origin"),
    do: AgentAPIInProcess.changes(relay, "person", key, cursor, opts)

  defp person_changes(changes),
    do: Enum.filter(changes, &(&1["kind"] == "entry" and &1["data"]["author"] == "person"))

  # `POST /v2/agent/messages/{id}/entries/{entry}/receipt` (ACK2) with
  # Idempotency-Key op(n). The result is the response body, or the error token.
  defp ack2(relay, entry, outcome, n, extra \\ %{}, person \\ "person", key \\ "origin") do
    body = Map.put(extra, "outcome", outcome)
    target = %{item_id: item(relay), entry_id: entry}
    receipt(relay, person, key, :chat_receipt, target, body, n)
  end

  defp answer_receipt(relay, id, outcome, n) do
    target = %{item_id: item(relay, id)}
    receipt(relay, "person", "origin", :answer_receipt, target, %{"outcome" => outcome}, n)
  end

  defp receipt(relay, person, key, operation, target, body, n) do
    case AgentAPIInProcess.write(relay, person, key, operation, target, body, op(n)) do
      {:ok, 200, response, false} ->
        response

      {:stored, 200, encoded} ->
        {:ok, %{"outcome" => outcome}} = JSON.decode_line(encoded <> "\n")
        Map.put(outcome, "replayed", true)

      {:error, token, _field, false} ->
        %{"error" => token}
    end
  end

  defp facts(relay, key \\ "phone") do
    {:ok, events, _} = RelayReaders.conversation_events(relay, key, 0)
    events
  end

  defp delivery_facts(relay, key \\ "phone"),
    do: Enum.filter(facts(relay, key), &(&1["kind"] == "delivery_ack"))

  test "ACK1 is committed only for returned person entries and is independent of Seen", c do
    id = discussion(c.relay)
    assert [%{"delivery_acks" => true, "event_id" => ^id}] = facts(c.relay)
    assert delivery_facts(c.relay) == []

    # A batch that does not return the person entry records no ACK1.
    assert {:ok, [%{"kind" => "message"}], true, message_rev} = changes(c.relay, 0, limit: 1)
    assert {:ok, [], false, _} = changes(c.relay, message_rev + 1)
    assert {:error, :cursor_expired} = changes(c.relay, message_rev + 10)
    assert {:error, :unauthorized} = changes(c.relay, 0, [], "unknown")
    assert delivery_facts(c.relay) == []

    support(c.relay, true)
    assert {:ok, rows, false, _} = changes(c.relay, message_rev)
    assert [%{"data" => entry}] = person_changes(rows)
    assert entry["entry_id"] == person_entry(c.relay)
    assert entry["delivery"] == %{"delivered" => false, "acted" => false}
    assert [ack] = delivery_facts(c.relay)
    assert ack["event_id"] == id
    assert ack["stage"] == "ack1"
    assert ack["ack2_expected"] == true
    assert ack["op_id"] == op(2)
    assert ack["session_handle"] == @handle
    assert ack["request_revision"] == 1
    assert ack["acknowledger"] == %{"system" => "Build helper"}
    assert is_integer(ack["at_ms"])
    assert delivery_facts(c.relay, "tablet") == [ack]
    assert delivery_facts(c.relay, "origin") == [ack]
    assert facts(c.relay, "stranger") == []

    support(c.relay, false, 4)
    assert {:ok, _, false, _} = changes(c.relay, 0)
    assert delivery_facts(c.relay) == [ack]

    # A stored 0.1 delivered receipt is a separate fact from ACK1.
    PreChangeLogStore.in_relay(c.relay, &PreChangeLogStore.origin_ack(&1, "origin", 2))
    assert Enum.count(facts(c.relay), &(&1["kind"] == "receipt")) == 1
    assert delivery_facts(c.relay) == [ack]
  end

  test "ACK2 implies consecutive ACK1 then ACK2 and first handed outcome is permanent", c do
    discussion(c.relay)
    support(c.relay, true)
    entry = person_entry(c.relay)
    failed = %{"reason" => "No session is available", "agent" => "Reviewer"}

    assert %{"status" => "recorded", "stage_outcome" => "not_handed"} =
             ack2(c.relay, entry, "not_handed", 4, failed)

    assert [first, second] = delivery_facts(c.relay)
    assert first["stage"] == "ack1"
    assert first["ack2_expected"] == true
    assert second["stage"] == "ack2"
    assert second["conversation_seq"] == first["conversation_seq"] + 1
    assert second["acknowledger"]["agent"] == "Reviewer"
    refute Map.has_key?(second, "rerouted")

    assert %{"replayed" => true, "status" => "recorded"} =
             ack2(c.relay, entry, "not_handed", 4, failed)

    assert %{"status" => "already_recorded"} =
             ack2(c.relay, entry, "not_handed", 5, %{"reason" => "Later reason"})

    assert delivery_facts(c.relay) == [first, second]

    assert %{"status" => "recorded", "stage_outcome" => "handed"} =
             ack2(c.relay, entry, "handed", 6, %{
               "agent" => "Alternate reviewer",
               "rerouted" => true
             })

    assert [^first, ^second, handed] = delivery_facts(c.relay)
    assert handed["rerouted"] == true
    assert handed["acknowledger"]["agent"] == "Alternate reviewer"
    refute Map.has_key?(handed, "reason")

    assert %{"status" => "already_recorded"} =
             ack2(c.relay, entry, "handed", 7, %{"agent" => "Different", "rerouted" => false})

    assert %{"status" => "superseded", "stage_outcome" => "handed"} =
             ack2(c.relay, entry, "not_handed", 8, failed)

    assert delivery_facts(c.relay) == [first, second, handed]
    # Same operation retains its original result even after a newer outcome wins.
    assert %{"replayed" => true, "stage_outcome" => "not_handed"} =
             ack2(c.relay, entry, "not_handed", 4, failed)

    assert %{"error" => "idempotency_conflict"} = ack2(c.relay, entry, "handed", 4)
    assert Enum.all?(facts(c.relay), &(&1["kind"] != "receipt"))
  end

  test "malformed ACK2 and wrong origin cannot create a delivery fact", c do
    discussion(c.relay)
    entry = person_entry(c.relay)

    for {{outcome, extra}, n} <-
          Enum.with_index(
            [
              {"invalid", %{}},
              {"handed", %{"agent" => " "}},
              {"handed", %{"rerouted" => "true"}},
              {"handed", %{"reason" => "not permitted"}},
              {"not_handed", %{}},
              {"not_handed", %{"reason" => ""}},
              {"not_handed", %{"reason" => "No route", "rerouted" => false}}
            ],
            10
          ) do
      assert %{"error" => _} = ack2(c.relay, entry, outcome, n, extra)
    end

    for {bad_entry, n} <- Enum.with_index(["e999", "1", "e02", "e"], 20) do
      assert %{"error" => "not_found"} = ack2(c.relay, bad_entry, "handed", n)
    end

    assert %{"error" => "not_found"} = ack2(c.relay, entry, "handed", 30, %{}, "person", "phone")
    assert %{"error" => _} = ack2(c.relay, entry, "handed", 31, %{}, "other", "stranger")
    assert %{"error" => _} = ack2(c.relay, entry, "handed", 32, %{}, "person", "unknown")
    assert delivery_facts(c.relay) == []
  end

  test "answer and No comment ACKs retain exact terminal identity", c do
    for {kind, n} <- [{"answer", 10}, {"answer_no_comment", 20}] do
      id = "terminal-#{kind}"

      assert %{"type" => "ok"} =
               request(c.relay, "origin", %{
                 "type" => "forward",
                 "op_id" => op(n),
                 "session_handle" => @handle,
                 "message" => %{
                   "sender_id" => "app:test",
                   "message_id" => id,
                   "created_at" => 1,
                   "display_name" => "Build helper",
                   "title" => "Check"
                 }
               })

      frame = %{
        "type" => kind,
        "op_id" => op(n + 1),
        "sender_id" => "app:test",
        "message_id" => id
      }

      frame =
        if kind == "answer",
          do: Map.put(frame, "answer", nil),
          else: %{"type" => kind, "request_id" => id, "request_revision" => 1}

      assert %{"type" => "ok", "seq" => terminal_seq} = request(c.relay, "phone", frame)

      terminal =
        Enum.find(
          origin_feed_entries(c.relay),
          &(&1["message_id"] == id and &1["kind"] == "terminal")
        )

      assert terminal["author"] == "person"
      assert %{"status" => "recorded"} = answer_receipt(c.relay, id, "handed", n + 2)
      acks = Enum.filter(delivery_facts(c.relay), &(&1["message_id"] == id))
      assert length(acks) == 2

      for ack <- acks do
        assert ack["terminal_seq"] == terminal_seq
        assert ack["event_id"] == terminal["event_id"]
        assert ack["request_revision"] == terminal["request_revision"]
        refute Map.has_key?(ack, "op_id")
      end
    end
  end

  test "feed and ACK2 facts roll back with failed durable commit, then survive restart", c do
    discussion(c.relay)
    entry = person_entry(c.relay)
    {:ok, db} = Sqlite3.open(c.path)

    :ok =
      Sqlite3.execute(
        db,
        "CREATE TRIGGER block_ack BEFORE UPDATE ON relay_state BEGIN SELECT RAISE(ABORT, 'fixture'); END"
      )

    assert {:error, :delivery_unavailable} = changes(c.relay, 0)
    assert %{"error" => "storage_unavailable"} = ack2(c.relay, entry, "handed", 4)

    assert delivery_facts(c.relay) == []
    :ok = Sqlite3.execute(db, "DROP TRIGGER block_ack")
    :ok = Sqlite3.close(db)
    assert %{"status" => "recorded"} = ack2(c.relay, entry, "handed", 4)
    expected = facts(c.relay)
    assert [%{"stage" => "ack1"}, %{"stage" => "ack2"}] = delivery_facts(c.relay)
    stop_supervised!(Relay)
    restarted = start_supervised!({Relay, store_path: c.path})
    assert facts(restarted) == expected
    assert %{"replayed" => true, "status" => "recorded"} = ack2(restarted, entry, "handed", 4)
    assert {:ok, _, false, _} = changes(restarted, 0)
    assert facts(restarted) == expected
  end

  test "pre-upgrade discussion has no marker synthesized on replay", c do
    discussion(c.relay)
    stop_supervised!(Relay)
    {:ok, store} = Store.open(c.path)

    events =
      Map.new(store.engine.conversation_events, fn {key, event} ->
        {key, %{event | payload: Map.delete(event.payload, "delivery_acks")}}
      end)

    {:ok, store} =
      Store.commit(store, %{store.engine | conversation_events: events}, %{
        person: "person",
        actor: "phone",
        action: "fixture"
      })

    :ok = Store.close(store)
    restarted = start_supervised!({Relay, store_path: c.path})
    assert [old] = facts(restarted)
    refute Map.has_key?(old, "delivery_acks")
    discussion(restarted, "phone", 5)
    assert [^old, %{"delivery_acks" => true}] = facts(restarted)
  end

  test "ACK1 and ACK2 report every record they write", c do
    discussion(c.relay)
    {:ok, store} = Store.open(c.path)
    engine = store.engine
    target = discussion_target(engine)

    {acked, :ok, keys} =
      Engine.with_touches(engine, fn current ->
        {:ok, current} = Engine.agent_feed_delivered(current, "person", "origin", [target])
        {current, :ok}
      end)

    assert keys == AttentionServer.TouchOracle.diff(engine, acked)
    assert Enum.any?(keys, &match?({:origin_feed, {"person", "origin"}, {:delivery_ack, _}}, &1))

    {handed, :ok, keys} =
      Engine.with_touches(acked, fn current ->
        {:ok, current, _reply} =
          Engine.origin_ack2(current, "origin", 2, %{"op_id" => op(9), "outcome" => "handed"})

        {current, :ok}
      end)

    assert keys == AttentionServer.TouchOracle.diff(acked, handed)
    assert Enum.any?(keys, &match?({:origin_feed, {"person", "origin"}, {:delivery_ack, _}}, &1))
    Store.close(store)
  end

  test "device label precedes sender display and server/agent entries never receive ACK1", c do
    discussion(c.relay)
    {:ok, store} = Store.open(c.path)
    engine = store.engine
    engine = put_in(engine.devices["origin"].label, "Automation service")
    target = discussion_target(engine)
    {:ok, next} = Engine.agent_feed_delivered(engine, "person", "origin", [target])
    {:ok, events, _} = Engine.conversation_events(next, "phone", 0)
    ack = Enum.find(events, &(&1["kind"] == "delivery_ack"))
    assert ack["acknowledger"] == %{"system" => "Automation service"}

    # Delivery stages use person-only eligibility, in the agent feed's turn and
    # in the stored origin feed entry ACK2 names.
    {:entry, position} = target

    for author <- ["server", "agent", "origin"] do
      turn = put_in(engine.conversation_events[{"person", position}].payload["author"], author)
      assert {:ok, ^turn} = Engine.agent_feed_delivered(turn, "person", "origin", [target])

      changed = put_in(engine.origin_feeds[{"person", "origin"}].entries[2]["author"], author)

      assert {:error, _} =
               Engine.origin_ack2(changed, "origin", 2, %{
                 "op_id" => op(9),
                 "outcome" => "handed"
               })
    end

    Store.close(store)
  end

  test "missing human names omit system without suppressing delivery; committed name is last fallback",
       c do
    discussion(c.relay)
    {:ok, store} = Store.open(c.path)
    key = {"person", "app:test", "question"}
    record = store.engine.messages[key]
    record = %{record | message: Map.delete(record.message, "display_name")}
    engine = %{store.engine | messages: Map.put(store.engine.messages, key, record)}
    target = discussion_target(engine)
    {:ok, next} = Engine.agent_feed_delivered(engine, "person", "origin", [target])
    {:ok, [_, ack], _} = Engine.conversation_events(next, "phone", 0)
    assert ack["acknowledger"] == %{}

    {:ok, next, _} =
      Engine.origin_ack2(next, "origin", 2, %{
        "op_id" => op(9),
        "outcome" => "handed",
        "agent" => "Reviewer"
      })

    {:ok, [_, _, handed], _} = Engine.conversation_events(next, "phone", 0)
    assert handed["acknowledger"] == %{"agent" => "Reviewer"}

    record =
      Map.put(record, "sender_display", %{
        "v" => 1,
        "sender_id" => "app:test",
        "origin" => "origin",
        "name" => "Committed name"
      })

    engine = %{engine | messages: Map.put(engine.messages, key, record)}
    {:ok, next} = Engine.agent_feed_delivered(engine, "person", "origin", [target])
    {:ok, [_, ack], _} = Engine.conversation_events(next, "phone", 0)
    assert ack["acknowledger"] == %{"system" => "Committed name"}
    Store.close(store)
  end

  defp discussion_target(engine) do
    [position] =
      for {{"person", position}, %{payload: %{"kind" => "discussion", "author" => "person"}}} <-
            engine.conversation_events,
          do: position

    {:entry, position}
  end

  # The origin's stored conversation feed, in feed order.
  defp origin_feed_entries(relay) do
    :sys.get_state(relay).store.engine.origin_feeds[{"person", "origin"}].entries
    |> Enum.sort()
    |> Enum.map(fn {_seq, entry} -> entry end)
  end
end
