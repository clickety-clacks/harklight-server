defmodule AttentionServer.DurableReplayTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{JSON, Relay}
  alias AttentionServer.JSON.DecimalToken
  alias Exqlite.Sqlite3

  setup do
    dir = Path.join(System.tmp_dir!(), "attention-elixir-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{path: Path.join(dir, "relay.sqlite3")}
  end

  defp op(n),
    do:
      "00000000-0000-4000-8000-" <>
        (n |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(12, "0"))

  defp message(id, title \\ "private") do
    %{
      "sender_id" => "app:test",
      "message_id" => id,
      "created_at" => 1,
      "title" => title,
      "unknown" => %{
        "large" => 9_007_199_254_740_993,
        "decimal" => %DecimalToken{raw: "0.12345678901234567890123"},
        "canary" => "MUST-NOT-ECHO-TO-ORIGIN"
      }
    }
  end

  defp raw(frame), do: JSON.encode!(Map.put_new(frame, "v", 1)) <> "\n"

  defp request(relay, key, frame) do
    assert {:ok, line} = Relay.request_line(relay, key, raw(frame))
    assert {:ok, decoded} = JSON.decode_line(line)
    decoded
  end

  defp session_request(relay, session, frame) do
    assert {:ok, lines} = Relay.session_request_line(relay, session, raw(frame))

    Enum.map(lines, fn line ->
      assert {:ok, decoded} = JSON.decode_line(line)
      decoded
    end)
  end

  defp setup_people(relay) do
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "A", "a1")
    assert :ok = Relay.setup_device(relay, "A", "a2")
    assert :ok = Relay.setup_device(relay, "B", "b1")
    assert :ok = Relay.setup_sender(relay, "a1", "app:test")
    assert :ok = Relay.setup_sender(relay, "b1", "app:test")
  end

  defp start(path) do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    relay
  end

  test "restart preserves exact first copy, terminal, cursor, grants and lost-reply receipts", %{
    path: path
  } do
    relay = start(path)
    setup_people(relay)
    instance_id = :sys.get_state(relay).store.engine.relay_instance_id
    original = message("same")

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "a1", %{"type" => "forward", "op_id" => op(1), "message" => original})

    assert %{"type" => "ok", "seq" => 2} =
             request(relay, "a2", %{
               "type" => "answer",
               "op_id" => op(2),
               "sender_id" => "app:test",
               "message_id" => "same",
               "answer" => nil
             })

    assert %{"revision" => 1} =
             request(relay, "a2", %{
               "type" => "subscribe_remote",
               "op_id" => op(3),
               "revision" => 1,
               "sender_ids" => ["app:test", "app:test"]
             })

    assert {:ok, origin_session} = Relay.open_session(relay, "a1")

    assert [
             %{"type" => "ok"},
             origin_delivery,
             origin_terminal,
             %{"type" => "resynced", "seq" => 2}
           ] =
             session_request(relay, origin_session, %{"type" => "resync", "since" => 0})

    assert Map.keys(origin_delivery) |> Enum.sort() == ~w(message_id origin sender_id seq type v)
    assert origin_terminal["answer"] == nil
    refute String.contains?(JSON.encode!(origin_delivery), "MUST-NOT-ECHO")

    assert [%{"type" => "ok", "seq" => 2}] =
             session_request(relay, origin_session, %{"type" => "ack", "seq" => 2})

    assert {:ok, [_, _], 2} = Relay.events(relay, "a2", 0)
    GenServer.stop(relay)

    restarted = start(path)
    assert :sys.get_state(restarted).store.engine.relay_instance_id == instance_id
    assert {:ok, 2} = Relay.acknowledged_cursor(restarted, "a1")
    assert {:ok, [_, _], 2} = Relay.events(restarted, "a1", 0)
    assert {:ok, [remote, _], 2} = Relay.events(restarted, "a2", 0)
    assert remote["message"] == original
    assert remote["as"] == "message"
    assert {:ok, [], 0} = Relay.events(restarted, "b1", 0)

    assert %{"type" => "ok", "seq" => 1, "duplicate" => true} =
             request(restarted, "a1", %{
               "type" => "forward",
               "op_id" => op(1),
               "message" => original
             })

    assert %{"type" => "ok", "seq" => 2, "duplicate" => true} =
             request(restarted, "a2", %{
               "type" => "answer",
               "op_id" => op(2),
               "sender_id" => "app:test",
               "message_id" => "same",
               "answer" => nil
             })

    assert %{"reason" => "operation_conflict"} =
             request(restarted, "a2", %{
               "type" => "answer",
               "op_id" => op(2),
               "sender_id" => "app:test",
               "message_id" => "same",
               "answer" => false
             })

    assert %{"revision" => 1, "duplicate" => true} =
             request(restarted, "a2", %{
               "type" => "subscribe_remote",
               "op_id" => op(3),
               "revision" => 1,
               "sender_ids" => ["app:test"]
             })

    assert %{"reason" => "stale_subscription"} =
             request(restarted, "a2", %{
               "type" => "subscribe_remote",
               "op_id" => op(5),
               "revision" => 1,
               "sender_ids" => []
             })

    assert %{"reason" => "closed"} =
             request(restarted, "a2", %{
               "type" => "answer",
               "op_id" => op(4),
               "sender_id" => "app:test",
               "message_id" => "same",
               "answer" => "late"
             })

    GenServer.stop(restarted)
  end

  test "resync high-water barrier and ACK require contiguous events actually sent on that session",
       %{path: path} do
    relay = start(path)
    setup_people(relay)

    for n <- 1..12 do
      assert %{"seq" => ^n} =
               request(relay, "a1", %{
                 "type" => "forward",
                 "op_id" => op(100 + n),
                 "message" => message("m#{n}")
               })
    end

    assert {:ok, session} = Relay.open_session(relay, "a2")
    assert {:error, :resync_required} = Relay.session_drain(relay, session)

    assert [%{"reason" => "invalid_cursor"}] =
             session_request(relay, session, %{"type" => "ack", "seq" => 1})

    assert [
             %{"type" => "ok"},
             %{"seq" => 11},
             %{"seq" => 12},
             %{"type" => "resynced", "seq" => 12}
           ] =
             session_request(relay, session, %{"type" => "resync", "since" => 10})

    assert [%{"reason" => "invalid_cursor"}] =
             session_request(relay, session, %{"type" => "ack", "seq" => 12})

    frames = session_request(relay, session, %{"type" => "resync", "since" => 0})
    assert length(frames) == 14
    assert Enum.map(Enum.slice(frames, 1, 12), & &1["seq"]) == Enum.to_list(1..12)
    assert List.last(frames) == %{"v" => 1, "type" => "resynced", "seq" => 12}

    assert [%{"type" => "ok", "seq" => 12}] =
             session_request(relay, session, %{"type" => "ack", "seq" => 12})

    assert %{"seq" => 13} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(200),
               "message" => message("later")
             })

    assert {:ok, [later_line]} = Relay.session_drain(relay, session)
    assert {:ok, %{"seq" => 13, "message_id" => "later"}} = JSON.decode_line(later_line)

    assert [%{"type" => "ok", "seq" => 13}] =
             session_request(relay, session, %{"type" => "ack", "seq" => 13})

    for invalid <- [-1, 14, %DecimalToken{raw: "1.0"}] do
      assert [%{"reason" => "invalid_cursor"}] =
               session_request(relay, session, %{"type" => "resync", "since" => invalid})
    end

    GenServer.stop(relay)
    restarted = start(path)
    assert {:ok, 13} = Relay.acknowledged_cursor(restarted, "a2")
    assert {:ok, new_session} = Relay.open_session(restarted, "a2")

    assert [%{"reason" => "invalid_cursor"}] =
             session_request(restarted, new_session, %{"type" => "ack", "seq" => 14})

    assert [%{"reason" => "invalid_cursor"}] =
             session_request(restarted, new_session, %{"type" => "ack", "seq" => 12})

    GenServer.stop(restarted)
  end

  test "tenant collisions and revoked devices remain isolated after restart", %{path: path} do
    relay = start(path)
    setup_people(relay)

    assert %{"seq" => 1} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(301),
               "message" => message("same", "A secret")
             })

    assert %{"seq" => 1} =
             request(relay, "b1", %{
               "type" => "forward",
               "op_id" => op(301),
               "message" => message("same", "B secret")
             })

    assert :ok = Relay.revoke_device(relay, "a2")
    GenServer.stop(relay)

    restarted = start(path)
    assert {:ok, [a_event], 1} = Relay.events(restarted, "a1", 0)
    assert {:ok, [b_event], 1} = Relay.events(restarted, "b1", 0)
    refute Map.has_key?(a_event, "message")
    refute Map.has_key?(b_event, "message")
    assert a_event["origin"] == "a1"
    assert b_event["origin"] == "b1"
    assert {:error, :forbidden} = Relay.events(restarted, "a2", 0)
    assert {:error, :forbidden} = Relay.open_session(restarted, "a2")

    assert %{"seq" => 2} =
             request(restarted, "b1", %{
               "type" => "answer",
               "op_id" => op(302),
               "sender_id" => "app:test",
               "message_id" => "same",
               "answer" => "B only"
             })

    assert {:ok, [_], 1} = Relay.events(restarted, "a1", 0)
    GenServer.stop(restarted)
  end

  test "stale second store process cannot overwrite a committed first copy", %{path: path} do
    first = start(path)
    setup_people(first)
    stale = start(path)

    assert %{"type" => "ok", "seq" => 1} =
             request(first, "a1", %{
               "type" => "forward",
               "op_id" => op(350),
               "message" => message("first")
             })

    assert %{"reason" => "storage_unavailable"} =
             request(stale, "a1", %{
               "type" => "forward",
               "op_id" => op(351),
               "message" => message("stale")
             })

    GenServer.stop(stale)
    GenServer.stop(first)
    restarted = start(path)
    assert {:ok, [event], 1} = Relay.events(restarted, "a2", 0)
    assert event["message_id"] == "first"
    assert event["message"]["title"] == "private"
    GenServer.stop(restarted)
  end

  test "a committed reply lost to a crashing fixture is recovered by exact retry", %{path: path} do
    relay = start(path)
    setup_people(relay)
    submitted = %{"type" => "forward", "op_id" => op(375), "message" => message("lost-reply")}

    # The fixture drops the accepted reply instead of applying it locally.
    {:ok, _dropped_line} = Relay.request_line(relay, "a1", raw(submitted))
    monitor = Process.monitor(relay)
    Process.unlink(relay)
    Process.exit(relay, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^relay, :killed}

    restarted = start(path)

    assert %{"type" => "ok", "seq" => 1, "duplicate" => true} =
             request(restarted, "a1", submitted)

    assert {:ok, [event], 1} = Relay.events(restarted, "a2", 0)
    assert event["message_id"] == "lost-reply"
    GenServer.stop(restarted)
  end

  test "failed audit insert rolls back state, event and receipt before reply", %{path: path} do
    relay = start(path)
    setup_people(relay)
    assert {:ok, db} = Sqlite3.open(path)

    assert :ok =
             Sqlite3.execute(
               db,
               "CREATE TRIGGER refuse_audit BEFORE INSERT ON relay_audit BEGIN SELECT RAISE(ABORT, 'blocked'); END"
             )

    assert %{"reason" => "storage_unavailable"} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(401),
               "message" => message("fail")
             })

    assert {:ok, [], 0} = Relay.events(relay, "a1", 0)
    GenServer.stop(relay)

    restarted = start(path)
    assert {:ok, [], 0} = Relay.events(restarted, "a1", 0)
    assert :ok = Sqlite3.execute(db, "DROP TRIGGER refuse_audit")

    assert %{"type" => "ok", "seq" => 1} =
             request(restarted, "a1", %{
               "type" => "forward",
               "op_id" => op(401),
               "message" => message("fail")
             })

    assert {:ok, [_], 1} = Relay.events(restarted, "a1", 0)

    assert {:ok, stmt} =
             Sqlite3.prepare(db, "SELECT person_id, actor_id, action FROM relay_audit")

    assert {:ok, rows} = Sqlite3.fetch_all(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert ["A", "a1", "forward"] in rows
    refute inspect(rows) =~ "MUST-NOT-ECHO"
    assert :ok = Sqlite3.close(db)
    GenServer.stop(restarted)
  end
end
