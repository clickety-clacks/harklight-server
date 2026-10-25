defmodule AttentionServer.SourceReportTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{Engine, JSON, OriginWire, Relay, Store}
  alias Exqlite.Sqlite3

  setup do
    dir =
      Path.join(
        System.tmp_dir!(),
        "attention-source-report-#{System.unique_integer([:positive])}"
      )

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

  defp send_origin(relay, key, operation_id, value) do
    OriginWire.command(relay, key, %{
      "v" => 1,
      "type" => "send",
      "op_id" => operation_id,
      "message" => value
    })
  end

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

  test "source reporting needs a separate delegated origin grant and immutable binding", %{
    relay: relay,
    path: path
  } do
    assert %{"seq" => 1, "type" => "ok"} = send_origin(relay, "a1", op(20), message("source"))

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
      assert %{"seq" => ^seq, "type" => "ok"} =
               send_origin(relay, origin, op(operation), message(id))

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

  test "format-one SQLite snapshot preserves stored messages and source report fields", %{
    relay: relay,
    path: path
  } do
    assert %{"seq" => 1, "type" => "ok"} = send_origin(relay, "a1", op(40), message("legacy"))

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

    assert {:ok, [%{"message_id" => "legacy"}], 1} = Relay.events(restarted, "a1", 0)

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
