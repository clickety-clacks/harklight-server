defmodule AttentionServer.DeliveryAcksTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{Engine, HeadlessTLS, JSON, OriginWire, Relay, Server, Store, TLSFixtures}
  alias Exqlite.Sqlite3

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(to_string(n), 12, "0")}"

  setup_all do
    certs = TLSFixtures.create!()
    TLSFixtures.make_self_signed_client!(certs, "reader")
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

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

  defp support(relay, capable, n \\ 3) do
    OriginWire.command(relay, "origin", %{
      "v" => 1,
      "type" => "support",
      "op_id" => op(n),
      "session_handle" => @handle,
      "route" => "codex",
      "cli_version" => "fixture",
      "unsupported_reason" => nil,
      "ack2" => capable
    })
  end

  defp ack2(relay, id, outcome, n, extra \\ %{}) do
    OriginWire.command(
      relay,
      "origin",
      Map.merge(
        %{
          "v" => 1,
          "type" => "ack2",
          "op_id" => op(n),
          "event_id" => id,
          "outcome" => outcome
        },
        extra
      )
    )
  end

  defp facts(relay, key \\ "phone") do
    {:ok, events, _} = Relay.conversation_events(relay, key, 0)
    events
  end

  defp delivery_facts(relay, key \\ "phone"),
    do: Enum.filter(facts(relay, key), &(&1["kind"] == "delivery_ack"))

  test "ACK1 is committed only for returned person entries and is independent of Seen", c do
    id = discussion(c.relay)
    assert [%{"delivery_acks" => true, "event_id" => ^id}] = facts(c.relay)
    assert delivery_facts(c.relay) == []
    assert {:ok, %{"entries" => []}} = Relay.origin_feed(c.relay, "origin", 2)
    assert {:error, :invalid_cursor} = Relay.origin_feed(c.relay, "origin", 3)
    assert {:error, :forbidden} = Relay.origin_feed(c.relay, "unknown", 0)
    assert delivery_facts(c.relay) == []

    assert %{"listening_support" => %{"ack2" => true}} = support(c.relay, true)
    assert {:ok, %{"entries" => [entry]}} = Relay.origin_feed(c.relay, "origin", 1)
    assert entry["event_id"] == id
    assert entry["delivered"] == false
    assert entry["acted"] == false
    assert [ack] = delivery_facts(c.relay)
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

    assert %{"type" => "ok"} = support(c.relay, false, 4)
    assert {:ok, _} = Relay.origin_feed(c.relay, "origin", 0)
    assert delivery_facts(c.relay) == [ack]
    assert {:ok, %{"status" => "delivered"}} = Relay.origin_ack(c.relay, "origin", 2)
    assert Enum.count(facts(c.relay), &(&1["kind"] == "receipt")) == 1
    assert delivery_facts(c.relay) == [ack]
  end

  test "legacy support makes ACK1 final and future support does not rewrite its snapshot", c do
    id = discussion(c.relay)
    assert {:ok, _} = Relay.origin_feed(c.relay, "origin", 1)
    assert [%{"ack2_expected" => false} = ack] = delivery_facts(c.relay)
    assert %{"type" => "ok"} = support(c.relay, true)
    assert {:ok, _} = Relay.origin_feed(c.relay, "origin", 1)
    assert delivery_facts(c.relay) == [ack]
    assert %{"status" => "recorded", "stage_outcome" => "handed"} = ack2(c.relay, id, "handed", 4)
    assert [^ack, %{"rerouted" => false}] = delivery_facts(c.relay)
  end

  test "ACK2 implies consecutive ACK1 then ACK2 and first handed outcome is permanent", c do
    id = discussion(c.relay)
    assert %{"type" => "ok"} = support(c.relay, true)
    failed = %{"reason" => "No session is available", "agent" => "Reviewer"}

    assert %{"status" => "recorded", "stage_outcome" => "not_handed"} =
             ack2(c.relay, id, "not_handed", 4, failed)

    assert [first, second] = delivery_facts(c.relay)
    assert first["stage"] == "ack1"
    assert second["stage"] == "ack2"
    assert second["conversation_seq"] == first["conversation_seq"] + 1
    assert second["acknowledger"]["agent"] == "Reviewer"
    refute Map.has_key?(second, "rerouted")

    assert %{"duplicate" => true, "status" => "recorded"} =
             ack2(c.relay, id, "not_handed", 4, failed)

    assert %{"status" => "already_recorded"} =
             ack2(c.relay, id, "not_handed", 5, %{"reason" => "Later reason"})

    assert delivery_facts(c.relay) == [first, second]

    assert %{"status" => "recorded", "stage_outcome" => "handed"} =
             ack2(c.relay, id, "handed", 6, %{"agent" => "Alternate reviewer", "rerouted" => true})

    assert [^first, ^second, handed] = delivery_facts(c.relay)
    assert handed["rerouted"] == true
    assert handed["acknowledger"]["agent"] == "Alternate reviewer"
    refute Map.has_key?(handed, "reason")

    assert %{"status" => "already_recorded"} =
             ack2(c.relay, id, "handed", 7, %{"agent" => "Different", "rerouted" => false})

    assert %{"status" => "superseded", "stage_outcome" => "handed"} =
             ack2(c.relay, id, "not_handed", 8, failed)

    assert delivery_facts(c.relay) == [first, second, handed]
    # Same operation retains its original result even after a newer outcome wins.
    assert %{"duplicate" => true, "stage_outcome" => "not_handed"} =
             ack2(c.relay, id, "not_handed", 4, failed)

    assert %{"type" => "error", "reason" => "operation_conflict"} = ack2(c.relay, id, "handed", 4)
    assert Enum.all?(facts(c.relay), &(&1["kind"] != "receipt"))
  end

  test "malformed ACK2 and wrong origin cannot create a delivery fact", c do
    id = discussion(c.relay)

    for {outcome, extra} <- [
          {"invalid", %{}},
          {"handed", %{"agent" => " "}},
          {"handed", %{"rerouted" => "true"}},
          {"handed", %{"reason" => "not permitted"}},
          {"not_handed", %{}},
          {"not_handed", %{"reason" => ""}},
          {"not_handed", %{"reason" => "No route", "rerouted" => false}}
        ] do
      assert %{"type" => "error"} = ack2(c.relay, id, outcome, 4, extra)
    end

    for bad_id <- ["1", "999", "02", 2] do
      assert %{"type" => "error"} = ack2(c.relay, bad_id, "handed", 4)
    end

    for key <- ["phone", "stranger", "unknown"] do
      assert %{"type" => "error"} =
               OriginWire.command(c.relay, key, %{
                 "v" => 1,
                 "type" => "ack2",
                 "op_id" => op(4),
                 "event_id" => id,
                 "outcome" => "handed"
               })
    end

    assert %{"type" => "error"} = ack2(c.relay, id, "handed", 4, %{"op_id" => "not-a-uuid"})
    assert delivery_facts(c.relay) == []
    assert %{"type" => "error"} = support(c.relay, "true")
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
      assert {:ok, %{"entries" => entries}} = Relay.origin_feed(c.relay, "origin", 0)
      terminal = Enum.find(entries, &(&1["message_id"] == id and &1["kind"] == "terminal"))
      assert terminal["author"] == "person"
      assert %{"status" => "recorded"} = ack2(c.relay, terminal["event_id"], "handed", n + 2)
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
    id = discussion(c.relay)
    {:ok, db} = Sqlite3.open(c.path)

    :ok =
      Sqlite3.execute(
        db,
        "CREATE TRIGGER block_ack BEFORE UPDATE ON relay_state BEGIN SELECT RAISE(ABORT, 'fixture'); END"
      )

    assert {:error, :storage_unavailable} = Relay.origin_feed(c.relay, "origin", 1)

    assert %{"type" => "error", "reason" => "storage_unavailable"} =
             ack2(c.relay, id, "handed", 4)

    assert delivery_facts(c.relay) == []
    :ok = Sqlite3.execute(db, "DROP TRIGGER block_ack")
    :ok = Sqlite3.close(db)
    assert %{"status" => "recorded"} = ack2(c.relay, id, "handed", 4)
    expected = facts(c.relay)
    stop_supervised!(Relay)
    restarted = start_supervised!({Relay, store_path: c.path})
    assert facts(restarted) == expected
    assert %{"duplicate" => true, "status" => "recorded"} = ack2(restarted, id, "handed", 4)
    assert {:ok, _} = Relay.origin_feed(restarted, "origin", 0)
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

  test "device label precedes sender display and server/agent entries never receive ACK1", c do
    discussion(c.relay)
    {:ok, store} = Store.open(c.path)
    engine = store.engine
    engine = put_in(engine.devices["origin"].label, "Automation service")
    {:ok, next, _} = Engine.deliver_origin_feed(engine, "origin", 0)
    {:ok, events, _} = Engine.conversation_events(next, "phone", 0)
    ack = Enum.find(events, &(&1["kind"] == "delivery_ack"))
    assert ack["acknowledger"] == %{"system" => "Automation service"}

    # Preserve old origin feed eligibility for Seen; delivery stages use person-only eligibility.
    for author <- ["server", "agent", "origin"] do
      changed = put_in(engine.origin_feeds[{"person", "origin"}].entries[2]["author"], author)
      assert {:ok, ^changed, _} = Engine.deliver_origin_feed(changed, "origin", 0)

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
    {:ok, next, %{"entries" => entries}} = Engine.deliver_origin_feed(engine, "origin", 1)
    assert length(entries) == 1
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
    {:ok, next, _} = Engine.deliver_origin_feed(engine, "origin", 1)
    {:ok, [_, ack], _} = Engine.conversation_events(next, "phone", 0)
    assert ack["acknowledger"] == %{"system" => "Committed name"}
    Store.close(store)
  end

  test "authenticated HTTP feed and ACK2 produce live and catch-up facts for both clients", c do
    path = Path.join(Path.dirname(c.path), "transport.sqlite3")

    server =
      start_supervised!(
        {Server,
         store_path: path,
         certfile: Path.join(c.certs, "server.pem"),
         keyfile: Path.join(c.certs, "server.key"),
         cacertfile: Path.join(c.certs, "ca.pem"),
         origin_port: 0,
         relay_name: :"two_ack_#{System.unique_integer([:positive])}"}
      )

    relay = Server.relay(server)
    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(c.certs, "a1"))

    {:ok, origin} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(c.certs, "a2"))

    {:ok, phone} =
      Relay.setup_device_certificate(
        relay,
        person,
        TLSFixtures.certificate_der!(c.certs, "reader")
      )

    {:ok, tablet} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(c.certs, "b1"))

    :ok = Relay.setup_sender(relay, origin, "app:test")

    endpoint = %{
      address: {127, 0, 0, 1},
      name: "relay.test",
      ca_file: Path.join(c.certs, "ca.pem"),
      port: Server.origin_port(server)
    }

    http = fn frame ->
      https_request(endpoint, c.certs, Map.put(frame, "v", 1))
    end

    send_request(relay, origin)

    sessions =
      for device <- [phone, tablet] do
        {:ok, session} = Relay.open_session(relay, device)

        {:ok, _} =
          Relay.session_request_line(
            relay,
            session,
            JSON.encode!(%{"v" => 1, "type" => "resync", "since" => 0}) <> "\n"
          )

        {:ok, _} =
          Relay.session_request_line(
            relay,
            session,
            JSON.encode!(%{"v" => 1, "type" => "conversation_open", "since" => 0}) <> "\n"
          )

        session
      end

    id = discussion(relay, phone)

    assert {:ok, 200, %{"type" => "ok", "listening_support" => %{"ack2" => true}}} =
             http.(%{
               "type" => "support",
               "op_id" => op(3),
               "session_handle" => @handle,
               "route" => "codex",
               "cli_version" => "fixture",
               "unsupported_reason" => nil,
               "ack2" => true
             })

    {:ok, stream} =
      HeadlessTLS.connect(endpoint, {Path.join(c.certs, "a2.pem"), Path.join(c.certs, "a2.key")})

    on_exit(fn -> :ssl.close(stream) end)

    :ok =
      :ssl.send(
        stream,
        "GET /origin/v1/stream?session=#{@handle} HTTP/1.1\r\nHost: relay.test\r\n\r\n"
      )

    notice = read_notice(stream, <<>>, 16)
    assert notice =~ "HTTP/1.1 200 OK"
    assert notice =~ "data: {\"cursor\":2}"
    assert delivery_facts(relay, phone) == []

    assert {:ok, 200, %{"type" => "ok", "entries" => [entry]}} =
             http.(%{"type" => "feed", "since" => 1})

    assert entry["event_id"] == id
    assert entry["delivered"] == false

    assert {:ok, 200, %{"type" => "ok", "status" => "recorded", "stage_outcome" => "handed"}} =
             http.(%{
               "type" => "ack2",
               "op_id" => op(4),
               "event_id" => id,
               "outcome" => "handed",
               "agent" => "Reviewer"
             })

    expected = delivery_facts(relay, phone)

    assert [
             %{"stage" => "ack1", "ack2_expected" => true},
             %{"stage" => "ack2", "rerouted" => false}
           ] = expected

    for session <- sessions do
      {:ok, lines} = Relay.session_drain(relay, session)

      live =
        Enum.map(lines, fn line ->
          {:ok, frame} = JSON.decode_line(line)
          frame
        end)

      assert Enum.filter(live, &(&1["kind"] == "delivery_ack")) == expected
    end

    # Explicit catch-up uses the same authoritative journal and does not touch Seen.
    {:ok, catching_up} = Relay.open_session(relay, tablet)

    {:ok, _} =
      Relay.session_request_line(
        relay,
        catching_up,
        JSON.encode!(%{"v" => 1, "type" => "resync", "since" => 0}) <> "\n"
      )

    {:ok, lines} =
      Relay.session_request_line(
        relay,
        catching_up,
        JSON.encode!(%{"v" => 1, "type" => "conversation_open", "since" => 0}) <> "\n"
      )

    replay =
      Enum.map(lines, fn line ->
        {:ok, frame} = JSON.decode_line(line)
        frame
      end)

    assert Enum.filter(replay, &(&1["kind"] == "delivery_ack")) == expected

    assert {:ok, 200, %{"entries" => [%{"delivered" => false, "acted" => false}]}} =
             http.(%{"type" => "feed", "since" => 1})

    assert {:ok, 200, %{"status" => "delivered"}} = http.(%{"type" => "ack", "event_id" => id})
    assert Enum.count(facts(relay, phone), &(&1["kind"] == "receipt")) == 1
  end

  defp https_request(endpoint, certs, frame) do
    {:ok, socket} =
      HeadlessTLS.connect(endpoint, {Path.join(certs, "a2.pem"), Path.join(certs, "a2.key")})

    try do
      body = JSON.encode!(frame)

      :ok =
        :ssl.send(socket, [
          "POST /origin/v1 HTTP/1.1\r\nHost: relay.test\r\nContent-Type: application/json\r\nContent-Length: #{byte_size(body)}\r\nConnection: close\r\n\r\n",
          body
        ])

      response = read_http(socket, <<>>)
      [head, body] = :binary.split(response, "\r\n\r\n")
      ["HTTP/1.1", status | _] = String.split(head)
      {:ok, decoded} = JSON.decode_line(body <> "\n")
      {:ok, String.to_integer(status), decoded}
    after
      :ssl.close(socket)
    end
  end

  defp read_http(socket, bytes) do
    case :ssl.recv(socket, 0, 5_000) do
      {:ok, chunk} -> read_http(socket, bytes <> chunk)
      {:error, :closed} -> bytes
      error -> flunk("fixture HTTP response failed: #{inspect(error)}")
    end
  end

  defp read_notice(_socket, _bytes, 0), do: flunk("no complete SSE notice")

  defp read_notice(socket, bytes, remaining) do
    if String.contains?(bytes, "data: {\"cursor\":2}\n\n") do
      bytes
    else
      assert {:ok, chunk} = :ssl.recv(socket, 0, 5_000)
      read_notice(socket, bytes <> chunk, remaining - 1)
    end
  end
end
