defmodule AttentionServer.OriginReceiptTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{Engine, JSON, Relay, Store}
  alias Exqlite.Sqlite3

  setup do
    dir = Path.join(System.tmp_dir!(), "attention-receipt-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")

    {:ok, relay} =
      Relay.start_link(store_path: path, source_fixture_mode: true, agent_fixture_mode: true)

    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "A", "a1")
    assert :ok = Relay.setup_device(relay, "A", "a2")
    assert :ok = Relay.setup_device(relay, "B", "b1")
    assert :ok = Relay.setup_sender(relay, "a1", "app:test")
    %{relay: relay, path: path}
  end

  defp op(n),
    do:
      "00000000-0000-4000-8000-" <>
        (n |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(12, "0"))

  defp message(id) do
    %{
      "sender_id" => "app:test",
      "message_id" => id,
      "created_at" => 1,
      "title" => "PRIVATE TITLE",
      "unknown" => %{"canary" => "PRIVATE CONTENT"}
    }
  end

  defp request(relay, key, frame) do
    raw = JSON.encode!(Map.put_new(frame, "v", 1)) <> "\n"
    assert {:ok, line} = Relay.request_line(relay, key, raw)
    assert {:ok, reply} = JSON.decode_line(line)
    reply
  end

  defp target(id, seq),
    do: %{"sender_id" => "app:test", "message_id" => id, "terminal_seq" => seq}

  defp receipt(relay, key, id, seq),
    do:
      request(
        relay,
        key,
        Map.merge(%{"type" => "answer_receipt", "receipt_version" => 1}, target(id, seq))
      )

  test "source and agent fixture authority is disabled in a normal Relay process" do
    {:ok, normal} = Relay.start_link()
    assert :ok = Relay.setup_person(normal, "A")
    assert :ok = Relay.setup_device(normal, "A", "a1")

    assert {:error, :not_supported} =
             Relay.grant_source_report(normal, "A", "a1", "source-A", ["app:test"], "fixture")

    assert {:error, :not_supported} =
             Relay.setup_agent_fixture(normal, "A", op(91), String.duplicate("d", 64), [
               "app:test"
             ])

    assert {:error, :not_supported} =
             Relay.fixture_agent_answer(normal, String.duplicate("d", 64), %{})

    bind = %{
      "type" => "source_bind",
      "op_id" => op(92),
      "sender_id" => "app:test",
      "message_id" => "m"
    }

    assert %{"reason" => "not_supported"} = request(normal, "a1", bind)
    raw = JSON.encode!(Map.put(bind, "v", 1)) <> "\n"
    assert {:error, :forbidden} = Relay.request_line(normal, "unknown", raw)
    GenServer.stop(normal)
  end

  test "origin-only receipt survives ACK/restart and keeps null distinct from absent", %{
    relay: relay,
    path: path
  } do
    assert %{"seq" => 1} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(1),
               "message" => message("null")
             })

    assert %{"status" => "pending"} = receipt(relay, "a1", "null", 2)
    assert %{"reason" => "not_found"} = receipt(relay, "a2", "null", 2)
    assert %{"reason" => "not_found"} = receipt(relay, "b1", "null", 2)
    assert %{"reason" => "not_found"} = receipt(relay, "a1", "absent", 2)

    assert %{"seq" => 2} =
             request(relay, "a2", %{
               "type" => "answer",
               "op_id" => op(2),
               "sender_id" => "app:test",
               "message_id" => "null",
               "answer" => nil
             })

    assert %{"receipt" => proof} = receipt(relay, "a1", "null", 2)
    assert proof["receipt_version"] == 1
    assert proof["relay_instance_id"] == :sys.get_state(relay).store.engine.relay_instance_id
    assert proof["person_id"] == "A"
    assert proof["acceptance_seq"] == 1
    assert proof["terminal_seq"] == 2
    assert proof["terminal_kind"] == "answer"
    assert proof["has_answer"] == true
    assert Map.has_key?(proof, "answer") and is_nil(proof["answer"])
    assert proof["actor"] == %{"kind" => "device", "id" => "a2", "key_fingerprint" => "a2"}
    assert proof["winning_op_id"] == op(2)
    refute String.contains?(JSON.encode!(proof), "PRIVATE")
    assert %{"reason" => "stale_target"} = receipt(relay, "a1", "null", 3)

    assert %{"reason" => "unsupported_version"} =
             request(relay, "a1", %{
               "type" => "answer_receipt",
               "receipt_version" => 2,
               "sender_id" => "app:test",
               "message_id" => "null",
               "terminal_seq" => 2
             })

    assert %{"reason" => "invalid_field"} =
             request(relay, "a1", %{
               "type" => "answer_receipt",
               "receipt_version" => 1,
               "sender_id" => "app:test",
               "message_id" => "null",
               "terminal_seq" => 2,
               "person_id" => "B"
             })

    assert {:ok, session} = Relay.open_session(relay, "a1")
    raw = JSON.encode!(%{"v" => 1, "type" => "resync", "since" => 0}) <> "\n"

    assert {:ok, [_ok, _delivery, ordinary, _barrier]} =
             Relay.session_request_line(relay, session, raw)

    assert {:ok, lifecycle} = JSON.decode_line(ordinary)
    refute Map.has_key?(lifecycle, "actor")
    refute Map.has_key?(lifecycle, "terminal_kind")
    ack = JSON.encode!(%{"v" => 1, "type" => "ack", "seq" => 2}) <> "\n"
    assert {:ok, [_]} = Relay.session_request_line(relay, session, ack)
    GenServer.stop(relay)

    {:ok, restarted} =
      Relay.start_link(store_path: path, source_fixture_mode: true, agent_fixture_mode: true)

    assert %{"receipt" => ^proof} = receipt(restarted, "a1", "null", 2)
    assert {:ok, 2} = Relay.acknowledged_cursor(restarted, "a1")
    assert :ok = Relay.revoke_device(restarted, "a2")
    assert %{"receipt" => ^proof} = receipt(restarted, "a1", "null", 2)
    assert :ok = Relay.revoke_device(restarted, "a1")

    assert {:error, :forbidden} =
             Relay.request_line(
               restarted,
               "a1",
               JSON.encode!(%{"v" => 1, "type" => "answer_receipt"}) <> "\n"
             )

    GenServer.stop(restarted)
  end

  test "value-less answered lifecycle has no answer or answer-kind proof", %{relay: relay} do
    assert %{"seq" => 1} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(10),
               "message" => message("life")
             })

    assert %{"seq" => 2} =
             request(relay, "a1", %{
               "type" => "lifecycle",
               "op_id" => op(11),
               "sender_id" => "app:test",
               "message_id" => "life",
               "state" => "answered",
               "at" => 1234,
               "by" => "answer"
             })

    assert %{"receipt" => proof} = receipt(relay, "a1", "life", 2)
    assert proof["terminal_kind"] == "lifecycle"
    assert proof["has_answer"] == false
    refute Map.has_key?(proof, "answer")
    assert proof["actor"]["id"] == "a1"
  end

  test "same sender and message IDs remain distinct across persons", %{relay: relay} do
    assert :ok = Relay.setup_sender(relay, "b1", "app:test")

    assert %{"seq" => 1} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(14),
               "message" => message("collision")
             })

    assert %{"seq" => 1} =
             request(relay, "b1", %{
               "type" => "forward",
               "op_id" => op(14),
               "message" => message("collision")
             })

    assert %{"seq" => 2} =
             request(relay, "a2", %{
               "type" => "answer",
               "op_id" => op(15),
               "sender_id" => "app:test",
               "message_id" => "collision",
               "answer" => "A-only"
             })

    assert %{"seq" => 2} =
             request(relay, "b1", %{
               "type" => "lifecycle",
               "op_id" => op(15),
               "sender_id" => "app:test",
               "message_id" => "collision",
               "state" => "cleared",
               "at" => 42
             })

    assert %{"receipt" => a} = receipt(relay, "a1", "collision", 2)
    assert %{"receipt" => b} = receipt(relay, "b1", "collision", 2)
    assert a["person_id"] == "A" and b["person_id"] == "B"
    assert a["terminal_kind"] == "answer" and b["terminal_kind"] == "lifecycle"
    assert a["answer"] == "A-only" and not Map.has_key?(b, "answer")
    assert %{"reason" => "not_found"} = receipt(relay, "a2", "collision", 2)
  end

  test "bounded fixture agent winner projects its own historical kind without device wire access",
       %{relay: relay, path: path} do
    agent_key = String.duplicate("c", 64)
    agent_id = op(90)
    assert :ok = Relay.setup_agent_fixture(relay, "A", agent_id, agent_key, ["app:test"])

    assert %{"seq" => 1} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(12),
               "message" => message("agent")
             })

    frame = %{
      "type" => "answer",
      "op_id" => op(13),
      "sender_id" => "app:test",
      "message_id" => "agent",
      "acceptance_seq" => 1,
      "deliberate" => true,
      "intent_ref" => "fixture-intent",
      "answer" => %{"choice" => "yes"}
    }

    assert {:error, "invalid_field", "intent_ref"} =
             Relay.fixture_agent_answer(relay, agent_key, %{frame | "deliberate" => false})

    assert {:error, "stale_target", nil} =
             Relay.fixture_agent_answer(relay, agent_key, %{frame | "acceptance_seq" => 2})

    assert {:ok, %{"seq" => 2}} = Relay.fixture_agent_answer(relay, agent_key, frame)

    assert {:ok, %{"seq" => 2, "duplicate" => true}} =
             Relay.fixture_agent_answer(relay, agent_key, frame)

    assert {:error, "operation_conflict", nil} =
             Relay.fixture_agent_answer(relay, agent_key, %{frame | "intent_ref" => "different"})

    assert %{"receipt" => proof} = receipt(relay, "a1", "agent", 2)

    assert proof["actor"] == %{
             "kind" => "agent",
             "id" => agent_id,
             "key_fingerprint" => agent_key
           }

    assert proof["answer"] == %{"choice" => "yes"}
    assert %{"reason" => "not_found"} = receipt(relay, "a2", "agent", 2)

    assert {:error, :forbidden} =
             Relay.request_line(
               relay,
               agent_key,
               JSON.encode!(%{"v" => 1, "type" => "answer_receipt"}) <> "\n"
             )

    GenServer.stop(relay)

    {:ok, restarted} =
      Relay.start_link(store_path: path, source_fixture_mode: true, agent_fixture_mode: true)

    assert %{"receipt" => ^proof} = receipt(restarted, "a1", "agent", 2)
    GenServer.stop(restarted)
  end

  test "source reporting needs a separate delegated origin grant and immutable binding", %{
    relay: relay,
    path: path
  } do
    assert %{"seq" => 1} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(20),
               "message" => message("source")
             })

    assert {:ok, %{"source_status" => "unreported"}} =
             Engine.source_status(:sys.get_state(relay).store.engine, "A", "app:test", "source")

    bind = %{
      "type" => "source_bind",
      "op_id" => op(21),
      "sender_id" => "app:test",
      "message_id" => "source",
      "acceptance_seq" => 1,
      "source_instance_id" => "source-A",
      "request_id" => "req-1",
      "request_revision" => "rev-1"
    }

    assert %{"reason" => "forbidden"} = request(relay, "a1", bind)

    assert :ok =
             Relay.grant_source_report(
               relay,
               "A",
               "a1",
               "source-A",
               ["app:test"],
               "source-owner-delegation-1"
             )

    assert :ok =
             Relay.grant_source_report(
               relay,
               "A",
               "a2",
               "source-A",
               ["app:test"],
               "source-owner-delegation-2"
             )

    assert %{"reason" => "not_found"} = request(relay, "a2", bind)
    assert %{"reason" => "forbidden"} = request(relay, "a1", %{bind | "sender_id" => "app:other"})
    assert %{"reason" => "forbidden"} = request(relay, "b1", bind)

    assert %{"reason" => "forbidden"} =
             request(relay, "a1", %{bind | "source_instance_id" => "source-B"})

    assert %{"reason" => "stale_target"} = request(relay, "a1", %{bind | "acceptance_seq" => 2})
    assert %{"type" => "ok", "acceptance_seq" => 1} = request(relay, "a1", bind)
    assert %{"duplicate" => true} = request(relay, "a1", bind)

    assert %{"reason" => "operation_conflict"} =
             request(relay, "a1", %{bind | "request_revision" => "rev-2"})

    assert %{"reason" => "source_binding_conflict"} =
             request(relay, "a1", %{bind | "op_id" => op(22), "request_revision" => "rev-2"})

    assert %{"type" => "ok"} = request(relay, "a1", %{bind | "op_id" => op(23)})

    assert %{"seq" => 2} =
             request(relay, "a2", %{
               "type" => "answer",
               "op_id" => op(24),
               "sender_id" => "app:test",
               "message_id" => "source",
               "answer" => "yes"
             })

    result = %{
      "type" => "source_result",
      "op_id" => op(25),
      "sender_id" => "app:test",
      "message_id" => "source",
      "terminal_seq" => 2,
      "source_instance_id" => "source-A",
      "request_id" => "req-1",
      "request_revision" => "rev-1",
      "result_revision" => 1,
      "status" => "received",
      "result_ref" => "inbox-1"
    }

    assert %{"reason" => "invalid_field", "field" => "reason"} =
             request(relay, "a1", Map.put(result, "reason", "PRIVATE CONTENT"))

    assert %{"reason" => "stale_target"} = request(relay, "a1", %{result | "terminal_seq" => 3})

    assert %{"reason" => "source_binding_conflict"} =
             request(relay, "a1", %{result | "request_revision" => "rev-2"})

    assert %{"reason" => "not_found"} = request(relay, "a2", result)
    assert %{"status" => "received", "result_revision" => 1} = request(relay, "a1", result)
    assert %{"duplicate" => true} = request(relay, "a1", result)

    applied = %{
      result
      | "op_id" => op(26),
        "result_revision" => 2,
        "status" => "applied",
        "result_ref" => "ruling-1"
    }

    assert %{"status" => "applied", "result_revision" => 2} = request(relay, "a1", applied)

    assert {:ok,
            %{
              "source_status" => "source_reported",
              "status" => "applied",
              "result_ref" => "ruling-1"
            }} =
             Engine.source_status(:sys.get_state(relay).store.engine, "A", "app:test", "source")

    assert %{"reason" => "stale_result"} = request(relay, "a1", %{applied | "op_id" => op(27)})

    assert %{"reason" => "receipt_conflict"} =
             request(relay, "a1", %{
               applied
               | "op_id" => op(28),
                 "result_revision" => 3,
                 "status" => "refused"
             })

    assert %{"status" => "applied", "result_revision" => 2} =
             request(relay, "a1", %{applied | "op_id" => op(29), "result_revision" => 3})

    GenServer.stop(relay)

    {:ok, restarted} =
      Relay.start_link(store_path: path, source_fixture_mode: true, agent_fixture_mode: true)

    assert %{"duplicate" => true} = request(restarted, "a1", bind)

    assert %{"duplicate" => true, "source_status" => "source_reported", "status" => "applied"} =
             request(restarted, "a1", applied)

    assert %{"reason" => "operation_conflict"} =
             request(restarted, "a1", %{applied | "result_ref" => "changed"})

    assert :ok = Relay.revoke_source_report(restarted, "A", "a1", "source-A")

    assert %{"reason" => "forbidden"} =
             request(restarted, "a1", %{applied | "op_id" => op(30), "result_revision" => 4})

    assert %{"reason" => "forbidden"} = request(restarted, "a1", applied)
    assert %{"receipt" => _} = receipt(restarted, "a1", "source", 2)

    record = :sys.get_state(restarted).store.engine.messages[{"A", "app:test", "source"}]
    assert record.source_binding["request_revision"] == "rev-1"
    assert record.source_result.status == "applied"
    assert record.source_result.revision == 2

    assert {:ok, %{"source_status" => "source_reported", "status" => "applied"}} =
             Engine.source_status(
               :sys.get_state(restarted).store.engine,
               "A",
               "app:test",
               "source"
             )

    assert %{"reason" => "forbidden"} = request(restarted, "a1", %{result | "op_id" => op(31)})
    assert %{"receipt" => _} = receipt(restarted, "a1", "source", 2)
    GenServer.stop(restarted)
  end

  test "one source request tuple cannot bind two messages, even after restart", %{
    relay: relay,
    path: path
  } do
    assert :ok = Relay.setup_sender(relay, "a2", "app:test")
    assert :ok = Relay.setup_sender(relay, "b1", "app:test")

    for {origin, id, seq, operation} <- [
          {"a1", "first", 1, 60},
          {"a2", "second", 2, 61},
          {"b1", "other-person", 1, 62}
        ] do
      assert %{"seq" => ^seq} =
               request(relay, origin, %{
                 "type" => "forward",
                 "op_id" => op(operation),
                 "message" => message(id)
               })

      person = if origin == "b1", do: "B", else: "A"

      assert :ok =
               Relay.grant_source_report(
                 relay,
                 person,
                 origin,
                 "source-A",
                 ["app:test"],
                 "source-owner-delegation"
               )
    end

    first = %{
      "type" => "source_bind",
      "op_id" => op(63),
      "sender_id" => "app:test",
      "message_id" => "first",
      "acceptance_seq" => 1,
      "source_instance_id" => "source-A",
      "request_id" => "req-one",
      "request_revision" => "rev-one"
    }

    assert %{"type" => "ok"} = request(relay, "a1", first)
    second = %{first | "op_id" => op(64), "message_id" => "second", "acceptance_seq" => 2}

    other_person = %{
      first
      | "op_id" => op(65),
        "message_id" => "other-person",
        "acceptance_seq" => 1
    }

    before = :sys.get_state(relay).store.engine
    assert %{"reason" => "source_binding_conflict"} = request(relay, "a2", second)
    assert %{"reason" => "source_binding_conflict"} = request(relay, "b1", other_person)
    assert :sys.get_state(relay).store.engine == before
    refute Map.has_key?(before.receipts, {"A", "a2", op(64)})
    refute Map.has_key?(before.receipts, {"B", "b1", op(65)})

    GenServer.stop(relay)

    {:ok, restarted} =
      Relay.start_link(store_path: path, source_fixture_mode: true, agent_fixture_mode: true)

    assert %{"reason" => "source_binding_conflict"} = request(restarted, "a2", second)
    assert %{"reason" => "source_binding_conflict"} = request(restarted, "b1", other_person)
    assert :sys.get_state(restarted).store.engine == before

    distinct_second = %{second | "request_id" => "req-two"}
    distinct_other = %{other_person | "request_id" => "req-three"}
    assert %{"type" => "ok"} = request(restarted, "a2", distinct_second)
    assert %{"type" => "ok"} = request(restarted, "b1", distinct_other)

    engine = :sys.get_state(restarted).store.engine
    assert engine.messages[{"A", "app:test", "first"}].source_binding["request_id"] == "req-one"
    assert engine.messages[{"A", "app:test", "second"}].source_binding["request_id"] == "req-two"
    GenServer.stop(restarted)

    # Even a correctly checksummed but inconsistent snapshot must not reload.
    duplicate =
      Map.put(
        first |> Map.take(~w(source_instance_id request_id request_revision)),
        "acceptance_seq",
        2
      )

    second_key = {"A", "app:test", "second"}

    corrupted = %{
      engine
      | messages:
          Map.update!(engine.messages, second_key, &Map.put(&1, :source_binding, duplicate))
    }

    payload = :erlang.term_to_binary({:attention_relay_state, 2, corrupted}, [{:compressed, 6}])
    assert {:ok, db} = Sqlite3.open(path)

    assert {:ok, stmt} =
             Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ? WHERE id = 1")

    assert :ok = Sqlite3.bind(stmt, [{:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}])
    assert :done = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)

    assert {:error, {:storage_unavailable, {:error, :invalid_state}}} =
             Store.open(path)
  end

  test "format-one SQLite snapshot reopens with new receipt/report fields", %{
    relay: relay,
    path: path
  } do
    assert %{"seq" => 1} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(40),
               "message" => message("legacy")
             })

    engine = :sys.get_state(relay).store.engine
    GenServer.stop(relay)
    legacy = engine |> Map.delete(:agents) |> Map.delete(:source_report_grants)
    payload = :erlang.term_to_binary({:attention_relay_state, 1, legacy}, [{:compressed, 6}])
    assert {:ok, db} = Sqlite3.open(path)

    assert {:ok, stmt} =
             Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ? WHERE id = 1")

    assert :ok = Sqlite3.bind(stmt, [{:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}])
    assert :done = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)

    {:ok, restarted} =
      Relay.start_link(store_path: path, source_fixture_mode: true, agent_fixture_mode: true)

    assert %{"status" => "pending"} = receipt(restarted, "a1", "legacy", 2)

    assert :ok =
             Relay.grant_source_report(
               restarted,
               "A",
               "a1",
               "source-A",
               ["app:test"],
               "owner-fixture"
             )

    GenServer.stop(restarted)

    {:ok, again} =
      Relay.start_link(store_path: path, source_fixture_mode: true, agent_fixture_mode: true)

    assert :sys.get_state(again).store.engine.source_report_grants[{"A", "a1", "source-A"}].revoked ==
             false

    GenServer.stop(again)
  end
end
