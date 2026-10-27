defmodule AttentionServer.WebSocketRelayTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{JSON, OriginHTTPS, Relay, Server, TLSFixtures, WebSocketRelay}

  setup_all do
    dir = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(dir) end)
    %{certs: dir}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "attention-ws-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")
    {:ok, relay} = Relay.start_link(store_path: path, enrollment_fixture_mode: true)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")

    keys =
      Map.new(~w(a1 a2 b1), fn name ->
        person = if name == "b1", do: "B", else: "A"

        assert {:ok, key} =
                 Relay.setup_device_certificate(
                   relay,
                   person,
                   TLSFixtures.certificate_der!(certs, name)
                 )

        {name, key}
      end)

    assert :ok = Relay.setup_sender(relay, keys["a1"], "app:test")
    assert :ok = Relay.setup_sender(relay, keys["b1"], "app:test")

    {:ok, web} =
      WebSocketRelay.start_link(
        relay: relay,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem")
      )

    on_exit(fn ->
      if Process.alive?(web), do: GenServer.stop(web)
      if Process.alive?(relay), do: GenServer.stop(relay)
    end)

    %{relay: relay, web: web, path: path, keys: keys}
  end

  defp connect(web, certs, name) do
    :ssl.connect({127, 0, 0, 1}, WebSocketRelay.port(web),
      versions: [:"tlsv1.3"],
      verify: :verify_peer,
      cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
      certfile: String.to_charlist(Path.join(certs, "#{name}.pem")),
      keyfile: String.to_charlist(Path.join(certs, "#{name}.key")),
      server_name_indication: ~c"relay.test",
      session_tickets: :disabled,
      active: false,
      mode: :binary,
      packet: :raw
    )
  end

  defp upgrade(socket, protocol \\ "attention-relay/1", path \\ "/relay") do
    request = [
      "GET ",
      path,
      " HTTP/1.1\r\n",
      "Host: relay.test\r\n",
      "Upgrade: websocket\r\n",
      "Connection: Upgrade\r\n",
      "Sec-WebSocket-Version: 13\r\n",
      "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
      "Sec-WebSocket-Protocol: ",
      protocol,
      "\r\n\r\n"
    ]

    assert :ok = :ssl.send(socket, request)
    read_http(socket, <<>>)
  end

  defp read_http(_socket, buffer) when byte_size(buffer) > 16_384, do: flunk("HTTP limit")

  defp read_http(socket, buffer) do
    if String.ends_with?(buffer, "\r\n\r\n") do
      buffer
    else
      assert {:ok, byte} = :ssl.recv(socket, 1, 2_000)
      read_http(socket, buffer <> byte)
    end
  end

  defp frame(payload, opcode \\ 1, fin \\ true) do
    mask = :crypto.strong_rand_bytes(4)
    length = byte_size(payload)

    header =
      cond do
        length < 126 -> <<bool(fin)::1, 0::3, opcode::4, 1::1, length::7>>
        length < 65_536 -> <<bool(fin)::1, 0::3, opcode::4, 1::1, 126::7, length::16>>
        true -> <<bool(fin)::1, 0::3, opcode::4, 1::1, 127::7, length::64>>
      end

    repeated = :binary.copy(mask, div(length + 3, 4))
    masked = :crypto.exor(payload, binary_part(repeated, 0, length))
    [header, mask, masked]
  end

  defp bool(true), do: 1
  defp bool(false), do: 0

  defp send_request(socket, payload) when is_map(payload),
    do: :ssl.send(socket, frame(JSON.encode!(Map.put_new(payload, "v", 1)) <> "\n"))

  defp store_request(relay, key, payload) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put_new(payload, "v", 1)) <> "\n")

    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp recv_frame(socket, timeout \\ 2_000) do
    assert {:ok, <<1::1, 0::3, opcode::4, 0::1, length::7>>} =
             :ssl.recv(socket, 2, timeout)

    length =
      case length do
        126 ->
          assert {:ok, <<size::16>>} = :ssl.recv(socket, 2, timeout)
          size

        127 ->
          assert {:ok, <<size::64>>} = :ssl.recv(socket, 8, timeout)
          size

        size ->
          size
      end

    assert {:ok, payload} = :ssl.recv(socket, length, timeout)
    {opcode, payload}
  end

  defp reply(socket) do
    assert {1, line} = recv_frame(socket)
    assert {:ok, value} = JSON.decode_line(line)
    value
  end

  defp resync(socket) do
    assert :ok = send_request(socket, %{"type" => "resync", "since" => 0})
    assert %{"type" => "ok"} = reply(socket)
    drain_barrier(socket, [])
  end

  defp start_websocket(relay, certs) do
    {:ok, web} =
      WebSocketRelay.start_link(
        relay: relay,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem")
      )

    web
  end

  defp restart_runtime(ctx, certs) do
    if Process.alive?(ctx.web), do: GenServer.stop(ctx.web)
    if Process.alive?(ctx.relay), do: GenServer.stop(ctx.relay)

    {:ok, relay} = Relay.start_link(store_path: ctx.path, enrollment_fixture_mode: true)
    web = start_websocket(relay, certs)
    runtime = %{relay: relay, web: web, path: ctx.path}

    on_exit(fn ->
      if Process.alive?(runtime.web), do: GenServer.stop(runtime.web)
      if Process.alive?(runtime.relay), do: GenServer.stop(runtime.relay)
    end)

    runtime
  end

  defp drain_barrier(socket, events) do
    case reply(socket) do
      %{"type" => "resynced"} = barrier -> {Enum.reverse(events), barrier}
      event -> drain_barrier(socket, [event | events])
    end
  end

  defp op(n), do: "00000000-0000-4000-8000-" <> String.pad_leading("#{n}", 12, "0")

  defp message(id, title),
    do: %{"sender_id" => "app:test", "message_id" => id, "created_at" => 1, "title" => title}

  test "TLS WebSocket keeps stored history readable and accepts legacy agent frames", ctx do
    %{certs: certs, web: web, relay: relay, keys: keys} = ctx
    message = message("one", "stored before route removal")

    assert {:ok, raw} =
             Relay.request_line(
               relay,
               keys["a1"],
               JSON.encode!(%{
                 "v" => 1,
                 "type" => "forward",
                 "op_id" => op(1),
                 "message" => message
               }) <> "\n"
             )

    assert {:ok, %{"type" => "ok", "seq" => 1}} = JSON.decode_line(raw)

    assert {:ok, raw} =
             Relay.request_line(
               relay,
               keys["a2"],
               JSON.encode!(%{
                 "v" => 1,
                 "type" => "answer",
                 "op_id" => op(2),
                 "sender_id" => "app:test",
                 "message_id" => "one",
                 "answer" => "kept"
               }) <> "\n"
             )

    assert {:ok, %{"type" => "ok", "seq" => 2}} = JSON.decode_line(raw)

    assert {:ok, a1} = connect(web, certs, "a1")
    assert upgrade(a1) =~ "101 Switching Protocols"

    assert {[
              %{"type" => "delivery", "message_id" => "one", "seq" => 1},
              %{"type" => "lifecycle", "message_id" => "one", "answer" => "kept"}
            ], %{"seq" => 2}} = resync(a1)

    assert {:ok, rejected} = connect(web, certs, "a1")
    assert upgrade(rejected, "wrong") =~ "400 Bad Request"
    assert :ssl.recv(rejected, 1, 2_000) == {:error, :closed}

    assert {:ok, wrong_path} = connect(web, certs, "a1")
    assert upgrade(wrong_path, "attention-relay/1", "/other") =~ "400 Bad Request"
    assert :ssl.recv(wrong_path, 1, 2_000) == {:error, :closed}

    assert :ok =
             send_request(a1, %{
               "type" => "forward",
               "op_id" => op(3),
               "message" => message("two", "accepted over WebSocket"),
               "ref" => "legacy-send"
             })

    assert %{"type" => "ok", "seq" => 3, "ref" => "legacy-send"} = reply(a1)
    assert %{"type" => "delivery", "seq" => 3, "message_id" => "two"} = reply(a1)

    assert :ok =
             send_request(a1, %{
               "type" => "answer_receipt",
               "receipt_version" => 1,
               "sender_id" => "app:test",
               "message_id" => "one",
               "terminal_seq" => 2,
               "ref" => "legacy-receipt"
             })

    assert %{"type" => "ok", "ref" => "legacy-receipt", "receipt" => receipt} = reply(a1)
    assert receipt["terminal_kind"] == "answer"
    assert receipt["answer"] == "kept"

    assert {:ok, [%{"message_id" => "one"}, _lifecycle, %{"message_id" => "two"}], 3} =
             Relay.events(relay, keys["a1"], 0)

    assert {:ok, a2} = connect(web, certs, "a2")
    assert upgrade(a2) =~ "101 Switching Protocols"

    assert {[
              %{"type" => "delivery", "message" => ^message},
              %{
                "type" => "lifecycle",
                "message_id" => "one",
                "state" => "answered"
              } = lifecycle,
              %{"type" => "delivery", "message_id" => "two", "seq" => 3}
            ], %{"seq" => 3}} = resync(a2)

    refute Map.has_key?(lifecycle, "answer")

    :ssl.close(a1)
    :ssl.close(a2)
  end

  test "origin-session conversation replay leaves the ordinary cursor and older peer intact",
       ctx do
    %{certs: certs, web: web, relay: relay, keys: keys} = ctx
    assert {:ok, old_socket} = connect(web, certs, "a1")
    assert upgrade(old_socket) =~ "101 Switching Protocols"
    assert {[], %{"seq" => 0}} = resync(old_socket)

    assert %{"type" => "ok", "seq" => 1} =
             store_request(relay, keys["a1"], %{
               "type" => "forward",
               "op_id" => op(61),
               "session_handle" => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
               "message" => message("conversation-projection", "Question")
             })

    assert %{"type" => "delivery", "seq" => 1} = reply(old_socket)

    assert {:ok, new_socket} = connect(web, certs, "a2")
    assert upgrade(new_socket) =~ "101 Switching Protocols"
    assert {[%{"type" => "delivery", "seq" => 1}], %{"seq" => 1}} = resync(new_socket)

    assert :ok = send_request(new_socket, %{"type" => "conversation_open", "since" => 0})
    assert %{"type" => "ok"} = reply(new_socket)
    assert %{"type" => "conversation_resynced", "conversation_seq" => 0} = reply(new_socket)

    assert :ok =
             send_request(new_socket, %{
               "type" => "discussion",
               "op_id" => op(62),
               "sender_id" => "app:test",
               "message_id" => "conversation-projection",
               "request_revision" => 1,
               "text" => "Clarify this?"
             })

    assert %{"type" => "ok", "seq" => 1, "event_id" => "2"} = reply(new_socket)

    assert %{"type" => "conversation", "conversation_seq" => 1, "event_id" => "2"} =
             reply(new_socket)

    assert {:error, :timeout} = :ssl.recv(old_socket, 2, 200)

    assert :ok =
             send_request(new_socket, %{"type" => "conversation_ack", "conversation_seq" => 1})

    assert %{"type" => "ok", "conversation_seq" => 1} = reply(new_socket)
    assert {:ok, 1} = Relay.conversation_cursor(relay, keys["a2"])
    assert {:ok, 0} = Relay.acknowledged_cursor(relay, keys["a2"])

    :ssl.close(new_socket)
    assert {:ok, reopened} = connect(web, certs, "a2")
    assert upgrade(reopened) =~ "101 Switching Protocols"
    assert {[%{"type" => "delivery", "seq" => 1}], %{"seq" => 1}} = resync(reopened)
    assert :ok = send_request(reopened, %{"type" => "conversation_open", "since" => 1})
    assert %{"type" => "ok"} = reply(reopened)
    assert %{"type" => "conversation_resynced", "conversation_seq" => 1} = reply(reopened)
    :ssl.close(reopened)
    :ssl.close(old_socket)
  end

  test "origin retraction replays a legacy lifecycle and a separate durable fact", ctx do
    %{certs: certs, web: web, relay: relay, keys: keys} = ctx
    assert {:ok, origin} = connect(web, certs, "a1")
    assert upgrade(origin) =~ "101 Switching Protocols"
    assert {[], %{"seq" => 0}} = resync(origin)
    assert {:ok, reader} = connect(web, certs, "a2")
    assert upgrade(reader) =~ "101 Switching Protocols"
    assert {[], %{"seq" => 0}} = resync(reader)

    assert %{"type" => "ok", "seq" => 1} =
             store_request(relay, keys["a1"], %{
               "type" => "forward",
               "op_id" => op(621),
               "session_handle" => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
               "message" => message("retracted", "Question")
             })

    assert %{"type" => "delivery", "seq" => 1} = reply(origin)
    assert %{"type" => "delivery", "seq" => 1} = reply(reader)
    assert {[%{"type" => "delivery", "seq" => 1}], %{"seq" => 1}} = resync(reader)
    assert :ok = send_request(reader, %{"type" => "ack", "seq" => 1})
    assert %{"type" => "ok", "seq" => 1} = reply(reader)
    :ssl.close(reader)

    assert :ok =
             send_request(origin, %{
               "type" => "conversation_withdraw",
               "op_id" => op(622),
               "sender_id" => "app:test",
               "message_id" => "retracted"
             })

    assert %{
             "type" => "ok",
             "seq" => 2,
             "state" => "retracted",
             "retracted_at" => retracted_at
           } = reply(origin)

    assert is_integer(retracted_at) and retracted_at > 0

    assert %{
             "type" => "lifecycle",
             "seq" => 2,
             "state" => "cleared",
             "terminal_reason" => "retracted",
             "at" => ^retracted_at,
             "retracted_at" => ^retracted_at
           } = reply(origin)

    assert {:ok, replay} = connect(web, certs, "a2")
    assert upgrade(replay) =~ "101 Switching Protocols"
    assert :ok = send_request(replay, %{"type" => "resync", "since" => 1})
    assert %{"type" => "ok"} = reply(replay)
    assert {[lifecycle], %{"seq" => 2}} = drain_barrier(replay, [])
    assert lifecycle["type"] == "lifecycle"
    assert lifecycle["state"] == "cleared"
    assert lifecycle["terminal_reason"] == "retracted"
    assert lifecycle["at"] == retracted_at
    assert lifecycle["retracted_at"] == retracted_at
    assert lifecycle["author"] == "origin"
    assert :ok = send_request(replay, %{"type" => "ack", "seq" => 2})
    assert %{"type" => "ok", "seq" => 2} = reply(replay)

    assert :ok = send_request(replay, %{"type" => "conversation_open", "since" => 0})
    assert %{"type" => "ok"} = reply(replay)

    assert %{
             "type" => "conversation",
             "kind" => "retraction",
             "conversation_seq" => 1,
             "terminal_state" => "retracted",
             "retracted_at" => ^retracted_at,
             "session_handle" => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
             "event_id" => event_id
           } = reply(replay)

    assert event_id == lifecycle["event_id"]
    assert %{"type" => "conversation_resynced", "conversation_seq" => 1} = reply(replay)
    :ssl.close(replay)
    :ssl.close(origin)
  end

  test "R4 inbox snapshots keep retraction projections at their frozen high water", ctx do
    %{certs: certs, web: web, relay: relay, keys: keys} = ctx
    assert :ok = Relay.setup_sender(relay, keys["a2"], "app:test")

    assert {:ok, origin} = connect(web, certs, "a1")
    assert upgrade(origin) =~ "101 Switching Protocols"
    assert {[], %{"seq" => 0}} = resync(origin)

    session_handle = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    target_message = message("page-retract", "Question")

    for n <- 1..21 do
      id = if n == 1, do: "page-retract", else: "page-#{n}"
      text = if n == 1, do: "Question", else: "Message #{n}"

      request = %{
        "type" => "forward",
        "op_id" => op(8000 + n),
        "message" => message(id, text)
      }

      request = if n == 1, do: Map.put(request, "session_handle", session_handle), else: request
      assert %{"type" => "ok", "seq" => ^n} = store_request(relay, keys["a1"], request)
      assert %{"type" => "delivery", "seq" => ^n} = reply(origin)
    end

    assert {:ok, recipient} = connect(web, certs, "a2")
    assert upgrade(recipient) =~ "101 Switching Protocols"

    start = %{
      "type" => "inbox_start",
      "paging_version" => 1,
      "startup_id" => "r4-before-retraction",
      "since" => 0,
      "ref" => "r4-start-before"
    }

    assert :ok = send_request(recipient, start)
    before = reply(recipient)
    assert before["high_water"] == 21
    assert length(before["items"]) == 20
    assert is_binary(before["next_cursor"])

    refute Enum.any?(
             before["items"],
             &(get_in(&1, ["acceptance", "message_id"]) == "page-retract")
           )

    assert :ok =
             send_request(origin, %{
               "type" => "conversation_withdraw",
               "op_id" => op(8100),
               "sender_id" => "app:test",
               "message_id" => "page-retract"
             })

    assert %{"type" => "ok", "seq" => 22, "state" => "retracted", "retracted_at" => retracted_at} =
             reply(origin)

    assert %{"type" => "lifecycle", "seq" => 22, "retracted_at" => ^retracted_at} =
             reply(origin)

    second_page = %{
      "type" => "inbox_page",
      "paging_version" => 1,
      "snapshot_id" => before["snapshot_id"],
      "cursor" => before["next_cursor"],
      "ref" => "r4-page-before"
    }

    assert :ok = send_request(recipient, second_page)
    assert %{"type" => "ok", "high_water" => 21, "items" => [before_item]} = reply(recipient)
    before_acceptance = before_item["acceptance"]
    assert before_acceptance["message_id"] == "page-retract"
    assert before_acceptance["message"] == target_message
    assert before_acceptance["request_revision"] == 1
    assert before_acceptance["session_handle"] == session_handle
    refute Map.has_key?(before_acceptance, "state")
    refute Map.has_key?(before_acceptance, "retracted_at")
    refute Map.has_key?(before_item, "terminal")

    pending_before = %{
      "type" => "answer",
      "op_id" => op(8101),
      "sender_id" => "app:test",
      "message_id" => "page-retract",
      "answer" => "pending at the first snapshot"
    }

    reconcile_before = %{
      "type" => "inbox_reconcile",
      "paging_version" => 1,
      "snapshot_id" => before["snapshot_id"],
      "batch_id" => "r4-before-batch",
      "operations" => [pending_before],
      "ref" => "r4-reconcile-before"
    }

    assert :ok = send_request(recipient, reconcile_before)
    assert %{"type" => "ok", "results" => [not_committed_before]} = reply(recipient)
    assert not_committed_before["status"] == "not_committed_at_snapshot"
    refute Map.has_key?(not_committed_before, "terminal")

    assert :ok =
             send_request(recipient, %{
               "type" => "inbox_checkpoint_ack",
               "paging_version" => 1,
               "snapshot_id" => before["snapshot_id"],
               "high_water" => 21,
               "reconciled_batches" => ["r4-before-batch"],
               "pending_manifest_complete" => true,
               "ref" => "r4-checkpoint-before"
             })

    assert %{"type" => "ok", "checkpoint_accepted" => true} = reply(recipient)

    assert :ok =
             send_request(recipient, %{
               "type" => "inbox_attach",
               "paging_version" => 1,
               "snapshot_id" => before["snapshot_id"],
               "mode" => "replay",
               "since" => 21,
               "ref" => "r4-replay-after"
             })

    assert %{"type" => "ok", "replay_started" => true} = reply(recipient)

    assert %{
             "type" => "lifecycle",
             "seq" => 22,
             "state" => "cleared",
             "terminal_reason" => "retracted",
             "retracted_at" => ^retracted_at
           } = reply(recipient)

    assert %{"type" => "resynced", "seq" => 22} = reply(recipient)
    assert :ok = send_request(recipient, %{"type" => "ack", "seq" => 22})
    assert %{"type" => "ok", "seq" => 22} = reply(recipient)
    :ssl.close(recipient)

    assert {:ok, snapshot_recipient} = connect(web, certs, "a2")
    assert upgrade(snapshot_recipient) =~ "101 Switching Protocols"

    start_after = %{
      "type" => "inbox_start",
      "paging_version" => 1,
      "startup_id" => "r4-after-retraction",
      "since" => 0,
      "ref" => "r4-start-after"
    }

    assert :ok = send_request(snapshot_recipient, start_after)
    after_snapshot = reply(snapshot_recipient)
    assert after_snapshot["high_water"] == 22
    assert is_binary(after_snapshot["next_cursor"])

    assert :ok =
             send_request(snapshot_recipient, %{
               "type" => "inbox_page",
               "paging_version" => 1,
               "snapshot_id" => after_snapshot["snapshot_id"],
               "cursor" => after_snapshot["next_cursor"],
               "ref" => "r4-page-after"
             })

    assert %{"type" => "ok", "high_water" => 22, "items" => [after_item]} =
             reply(snapshot_recipient)

    after_acceptance = after_item["acceptance"]
    assert after_acceptance["message"] == target_message
    assert after_acceptance["request_revision"] == 1
    assert after_acceptance["session_handle"] == session_handle
    assert after_acceptance["state"] == "retracted"
    assert after_acceptance["retracted_at"] == retracted_at

    assert %{
             "seq" => 22,
             "state" => "cleared",
             "terminal_reason" => "retracted",
             "at" => ^retracted_at,
             "retracted_at" => ^retracted_at
           } =
             after_terminal = after_item["terminal"]

    refute Map.has_key?(after_terminal, "request_revision")
    refute Map.has_key?(after_terminal, "event_id")

    pending_after = %{pending_before | "op_id" => op(8102)}

    assert :ok =
             send_request(snapshot_recipient, %{
               "type" => "inbox_reconcile",
               "paging_version" => 1,
               "snapshot_id" => after_snapshot["snapshot_id"],
               "batch_id" => "r4-after-batch",
               "operations" => [pending_after],
               "ref" => "r4-reconcile-after"
             })

    assert %{"type" => "ok", "results" => [not_committed_after]} = reply(snapshot_recipient)
    assert not_committed_after["status"] == "not_committed_at_snapshot"
    assert not_committed_after["terminal"] == after_terminal
    :ssl.close(snapshot_recipient)

    assert {:ok, origin_page} = connect(web, certs, "a1")
    assert upgrade(origin_page) =~ "101 Switching Protocols"

    assert :ok =
             send_request(origin_page, %{
               "type" => "inbox_start",
               "paging_version" => 1,
               "startup_id" => "r4-origin-reconcile",
               "since" => 0,
               "ref" => "r4-origin-start"
             })

    origin_snapshot = reply(origin_page)
    assert origin_snapshot["high_water"] == 22

    assert :ok =
             send_request(origin_page, %{
               "type" => "inbox_reconcile",
               "paging_version" => 1,
               "snapshot_id" => origin_snapshot["snapshot_id"],
               "batch_id" => "r4-origin-batch",
               "operations" => [
                 %{
                   "type" => "forward",
                   "op_id" => op(8001),
                   "message" => target_message,
                   "session_handle" => session_handle
                 }
               ],
               "ref" => "r4-origin-reconcile"
             })

    assert %{"type" => "ok", "results" => [committed]} = reply(origin_page)
    assert committed["status"] == "committed"
    assert committed["result"]["seq"] == 1
    assert committed["result"]["duplicate"] == true
    assert committed["terminal"]["state"] == "cleared"
    assert committed["terminal"]["terminal_reason"] == "retracted"
    assert committed["terminal"]["at"] == retracted_at
    assert committed["terminal"]["retracted_at"] == retracted_at
    assert committed["terminal"]["author"] == "origin"
    refute Map.has_key?(committed["terminal"], "request_revision")
    refute Map.has_key?(committed["terminal"], "event_id")

    :ssl.close(origin_page)
    :ssl.close(origin)
  end

  test "an authenticated conversation opener learns only the currently bound origin HTTPS port",
       %{certs: certs, web: web, relay: relay} do
    assert {:ok, socket} = connect(web, certs, "a2")
    assert upgrade(socket) =~ "101 Switching Protocols"
    assert {[], %{"seq" => 0}} = resync(socket)

    assert :ok = send_request(socket, %{"type" => "conversation_open", "since" => 0})
    refute Map.has_key?(reply(socket), "origin_https_port")
    assert %{"type" => "conversation_resynced", "conversation_seq" => 0} = reply(socket)

    assert {:ok, origin} =
             OriginHTTPS.start_link(
               relay: relay,
               certfile: Path.join(certs, "server.pem"),
               keyfile: Path.join(certs, "server.key"),
               cacertfile: Path.join(certs, "ca.pem"),
               port: 0
             )

    on_exit(fn -> if Process.alive?(origin), do: GenServer.stop(origin) end)
    bound_port = OriginHTTPS.port(origin)
    assert bound_port in 1..65_535

    assert :ok = send_request(socket, %{"type" => "conversation_open", "since" => 0})
    assert %{"type" => "ok", "origin_https_port" => ^bound_port} = reply(socket)
    assert %{"type" => "conversation_resynced", "conversation_seq" => 0} = reply(socket)

    GenServer.stop(origin)
    assert :ok = send_request(socket, %{"type" => "conversation_open", "since" => 0})
    refute Map.has_key?(reply(socket), "origin_https_port")
    assert %{"type" => "conversation_resynced", "conversation_seq" => 0} = reply(socket)
    :ssl.close(socket)
  end

  test "malformed text, fragmentation and bounded oversize recover on one socket",
       ctx do
    %{certs: certs, web: web} = ctx
    assert {:ok, socket} = connect(web, certs, "a1")
    assert upgrade(socket) =~ "attention-relay/1"

    assert :ok = :ssl.send(socket, frame("not-json\n"))
    assert %{"reason" => "bad_frame"} = reply(socket)

    request = JSON.encode!(%{"v" => 1, "type" => "resync", "since" => 0, "ref" => "next"}) <> "\n"
    <<first::binary-size(8), rest::binary>> = request
    assert :ok = :ssl.send(socket, frame(first, 1, false))
    assert :ok = :ssl.send(socket, frame(rest, 0, true))
    assert %{"type" => "ok", "ref" => "next"} = reply(socket)
    assert %{"type" => "resynced", "seq" => 0} = reply(socket)

    assert :ok = :ssl.send(socket, frame("irrelevant", 2))
    assert %{"reason" => "bad_frame"} = reply(socket)

    assert :ok = :ssl.send(socket, frame("ping", 9))
    assert {10, "ping"} = recv_frame(socket)

    assert :ok = send_request(socket, %{"type" => "bogus", "ref" => "after-bad"})
    assert %{"reason" => "unknown_type", "ref" => "after-bad"} = reply(socket)

    too_large = 2 * 1024 * 1024 + 1
    assert :ok = :ssl.send(socket, frame(String.duplicate("x", too_large)))
    assert %{"reason" => "bad_frame"} = reply(socket)
    assert :ok = send_request(socket, %{"type" => "bogus", "ref" => "after-oversize"})
    assert %{"reason" => "unknown_type", "ref" => "after-oversize"} = reply(socket)
    assert {:ok, 0} = Relay.acknowledged_cursor(ctx.relay, ctx.keys["a1"])
    :ssl.close(socket)
  end

  test "near-limit frame followed by another frame preserves both ordered replies over TLS",
       ctx do
    %{certs: certs, web: web, relay: relay, keys: keys} = ctx
    assert {:ok, socket} = connect(web, certs, "a1")
    assert upgrade(socket) =~ "101 Switching Protocols"
    assert {[], %{"seq" => 0}} = resync(socket)

    first = %{
      "v" => 1,
      "type" => "forward",
      "op_id" => op(51),
      "message" => Map.put(message("near-limit", "first"), "body", "")
    }

    empty_line = JSON.encode!(first) <> "\n"
    body = String.duplicate("x", 2 * 1024 * 1024 - byte_size(empty_line))
    first_line = JSON.encode!(put_in(first, ["message", "body"], body)) <> "\n"
    assert byte_size(first_line) == 2 * 1024 * 1024

    second = %{
      "v" => 1,
      "type" => "forward",
      "op_id" => op(52),
      "message" => message("after-limit", "second")
    }

    assert :ok = :ssl.send(socket, [frame(first_line), frame(JSON.encode!(second) <> "\n")])
    assert %{"type" => "ok", "seq" => 1} = reply(socket)
    assert %{"type" => "delivery", "seq" => 1, "message_id" => "near-limit"} = reply(socket)

    assert %{"type" => "ok", "seq" => 2} = reply(socket)
    assert %{"type" => "delivery", "seq" => 2, "message_id" => "after-limit"} = reply(socket)

    assert {:ok, events, 2} = Relay.events(relay, keys["a1"], 0)
    assert length(events) == 2
    :ssl.close(socket)
  end

  test "bounded inbox pages reconcile receipts, checkpoint atomically, and survive restart",
       ctx do
    %{certs: certs, web: web, relay: relay, keys: keys, path: path} = ctx

    assert {:ok, origin} = connect(web, certs, "a1")
    assert upgrade(origin) =~ "101 Switching Protocols"
    assert {[], %{"seq" => 0}} = resync(origin)

    for n <- 1..61 do
      assert %{"type" => "ok", "seq" => ^n} =
               store_request(relay, keys["a1"], %{
                 "type" => "forward",
                 "op_id" => op(1000 + n),
                 "message" => message("m#{n}", "message #{n}")
               })

      assert %{"type" => "delivery", "seq" => ^n, "message_id" => "m" <> _} = reply(origin)
    end

    assert {:ok, device} = connect(web, certs, "a2")
    assert upgrade(device) =~ "101 Switching Protocols"

    assert :ok =
             send_request(device, %{
               "type" => "answer",
               "op_id" => op(2000),
               "sender_id" => "app:test",
               "message_id" => "m1",
               "answer" => "private answer"
             })

    assert %{"type" => "ok", "seq" => 62} = reply(device)

    start_request = %{
      "type" => "inbox_start",
      "paging_version" => 1,
      "startup_id" => "startup-a2",
      "since" => 0,
      "ref" => "start-a2"
    }

    assert :ok = send_request(device, start_request)
    initial = reply(device)
    assert initial["type"] == "ok"
    assert initial["paging_version"] == 1
    assert initial["high_water"] == 62
    assert length(initial["items"]) == 20
    assert initial["receipt_cursor"] == nil
    cursor = initial["next_cursor"]
    assert is_binary(cursor)

    initial_ids = Enum.map(initial["items"], &get_in(&1, ["acceptance", "message_id"]))
    expected_ids = Enum.reverse(Enum.map(42..61, &"m#{&1}"))
    assert initial_ids == expected_ids

    assert :ok = send_request(device, start_request)
    assert reply(device) == initial

    assert :ok = send_request(device, %{"type" => "resync", "since" => 0})
    assert %{"type" => "error", "reason" => "sync_mode_conflict"} = reply(device)

    operations = [
      %{
        "type" => "answer",
        "op_id" => op(2000),
        "sender_id" => "app:test",
        "message_id" => "m1",
        "answer" => "private answer"
      },
      %{
        "type" => "answer",
        "op_id" => op(2001),
        "sender_id" => "app:test",
        "message_id" => "m1",
        "answer" => "never submitted"
      }
    ]

    reconcile_request = %{
      "type" => "inbox_reconcile",
      "paging_version" => 1,
      "snapshot_id" => initial["snapshot_id"],
      "batch_id" => "batch-a2",
      "operations" => operations,
      "ref" => "batch-a2"
    }

    assert :ok = send_request(device, reconcile_request)
    reconciliation = reply(device)
    assert [committed, pending] = reconciliation["results"]
    assert committed["status"] == "committed"
    assert committed["result"]["seq"] == 62
    assert committed["result"]["duplicate"] == true
    assert committed["terminal"]["seq"] == 62
    refute Map.has_key?(committed["terminal"], "answer")
    assert pending["status"] == "not_committed_at_snapshot"
    assert pending["terminal"]["seq"] == 62

    assert :ok =
             send_request(device, %{
               "type" => "answer",
               "op_id" => op(2002),
               "sender_id" => "app:test",
               "message_id" => "m2",
               "answer" => "not accepted before checkpoint"
             })

    assert %{"type" => "error", "reason" => "checkpoint_pending"} = reply(device)

    assert %{"type" => "lifecycle", "seq" => 62, "answer" => "private answer"} = reply(origin)

    assert %{"type" => "ok", "seq" => 63} =
             store_request(relay, keys["a1"], %{
               "type" => "forward",
               "op_id" => op(1062),
               "message" => message("m62", "arrived after snapshot")
             })

    assert %{"type" => "delivery", "seq" => 63} = reply(origin)

    page_request = %{
      "type" => "inbox_page",
      "paging_version" => 1,
      "snapshot_id" => initial["snapshot_id"],
      "cursor" => cursor,
      "ref" => "page-two"
    }

    assert :ok = send_request(device, page_request)
    second_page = reply(device)
    assert second_page["high_water"] == 62
    assert length(second_page["items"]) == 20

    assert Enum.map(second_page["items"], &get_in(&1, ["acceptance", "message_id"])) ==
             Enum.reverse(Enum.map(22..41, &"m#{&1}"))

    assert :ok = send_request(device, page_request)
    assert reply(device) == second_page

    third_page_request = %{
      "type" => "inbox_page",
      "paging_version" => 1,
      "snapshot_id" => initial["snapshot_id"],
      "cursor" => second_page["next_cursor"],
      "ref" => "page-three"
    }

    assert :ok = send_request(device, third_page_request)
    third_page = reply(device)
    assert length(third_page["items"]) == 20

    assert Enum.map(third_page["items"], &get_in(&1, ["acceptance", "message_id"])) ==
             Enum.reverse(Enum.map(2..21, &"m#{&1}"))

    fourth_page_request = %{
      "type" => "inbox_page",
      "paging_version" => 1,
      "snapshot_id" => initial["snapshot_id"],
      "cursor" => third_page["next_cursor"],
      "ref" => "page-four"
    }

    assert :ok = send_request(device, fourth_page_request)
    fourth_page = reply(device)
    assert fourth_page["next_cursor"] == nil

    assert [%{"acceptance" => %{"message_id" => "m1"}, "terminal" => terminal}] =
             fourth_page["items"]

    assert terminal["seq"] == 62
    assert terminal["state"] == "answered"
    refute Map.has_key?(terminal, "answer")

    assert :ok =
             send_request(device, %{
               "type" => "inbox_checkpoint_ack",
               "paging_version" => 1,
               "snapshot_id" => initial["snapshot_id"],
               "high_water" => 62,
               "reconciled_batches" => ["batch-a2"],
               "pending_manifest_complete" => true,
               "ref" => "checkpoint-a2"
             })

    checkpoint = reply(device)
    assert checkpoint["type"] == "ok"
    assert checkpoint["checkpoint_accepted"] == true
    refute Map.has_key?(checkpoint, "duplicate")

    assert :ok = send_request(device, %{"type" => "ack", "seq" => 62})
    assert %{"type" => "error", "reason" => "checkpoint_pending"} = reply(device)

    assert :ok =
             send_request(device, %{
               "type" => "inbox_attach",
               "paging_version" => 1,
               "snapshot_id" => initial["snapshot_id"],
               "mode" => "replay",
               "since" => 62,
               "ref" => "replay-a2"
             })

    assert %{"type" => "ok", "replay_started" => true, "ref" => "replay-a2"} = reply(device)
    assert %{"type" => "delivery", "seq" => 63, "message_id" => "m62"} = reply(device)
    assert %{"type" => "resynced", "seq" => 63} = reply(device)
    assert :ok = send_request(device, %{"type" => "ack", "seq" => 63})
    assert %{"type" => "ok", "seq" => 63} = reply(device)
    :ssl.close(device)

    runtime = restart_runtime(%{ctx | relay: relay, web: web, path: path}, certs)
    assert {:ok, recovered} = connect(runtime.web, certs, "a2")
    assert upgrade(recovered) =~ "101 Switching Protocols"

    assert :ok =
             send_request(recovered, %{
               "type" => "inbox_attach",
               "paging_version" => 1,
               "snapshot_id" => initial["snapshot_id"],
               "mode" => "checkpoint_status",
               "since" => 0,
               "candidate_high_water" => 62,
               "ref" => "status-a2"
             })

    assert %{"type" => "ok", "checkpoint_accepted" => true, "replay_started" => false} =
             reply(recovered)

    assert :ok = send_request(recovered, page_request)
    assert reply(recovered) == second_page
    assert :ok = send_request(recovered, reconcile_request)
    assert reply(recovered) == reconciliation

    assert :ok =
             send_request(recovered, %{
               "type" => "inbox_checkpoint_ack",
               "paging_version" => 1,
               "snapshot_id" => initial["snapshot_id"],
               "high_water" => 62,
               "reconciled_batches" => ["batch-a2"],
               "pending_manifest_complete" => true,
               "ref" => "checkpoint-retry"
             })

    assert %{"type" => "ok", "checkpoint_accepted" => true, "duplicate" => true} =
             reply(recovered)

    assert {:ok, 63} = Relay.acknowledged_cursor(runtime.relay, keys["a2"])
    :ssl.close(recovered)

    assert {:ok, source} = connect(runtime.web, certs, "a1")
    assert upgrade(source) =~ "101 Switching Protocols"

    assert :ok =
             send_request(source, %{
               "type" => "inbox_start",
               "paging_version" => 1,
               "startup_id" => "startup-a1",
               "since" => 0,
               "ref" => "start-a1"
             })

    source_start = reply(source)
    receipt_cursor = source_start["receipt_cursor"]
    assert is_binary(receipt_cursor)

    assert :ok =
             send_request(source, %{
               "type" => "inbox_reconcile",
               "paging_version" => 1,
               "snapshot_id" => source_start["snapshot_id"],
               "batch_id" => "batch-a1",
               "operations" => [
                 %{
                   "type" => "forward",
                   "op_id" => op(1001),
                   "message" => message("m1", "message 1")
                 }
               ],
               "ref" => "reconcile-a1"
             })

    assert %{
             "type" => "ok",
             "results" => [
               %{
                 "status" => "committed",
                 "result" => %{"seq" => 1, "duplicate" => true},
                 "terminal" => %{"seq" => 62, "answer" => "private answer"}
               }
             ]
           } = reply(source)

    receipt_request = %{
      "type" => "inbox_receipts",
      "paging_version" => 1,
      "snapshot_id" => source_start["snapshot_id"],
      "cursor" => receipt_cursor,
      "ref" => "receipt-a1"
    }

    assert :ok = send_request(source, receipt_request)
    receipt_page = reply(source)

    assert [%{"type" => "lifecycle", "seq" => 62, "answer" => "private answer"}] =
             receipt_page["events"]

    refute Map.has_key?(hd(receipt_page["events"]), "message")
    assert receipt_page["next_cursor"] == nil
    :ssl.close(source)

    runtime = restart_runtime(runtime, certs)
    assert {:ok, source_recovered} = connect(runtime.web, certs, "a1")
    assert upgrade(source_recovered) =~ "101 Switching Protocols"

    assert :ok =
             send_request(source_recovered, %{
               "type" => "inbox_attach",
               "paging_version" => 1,
               "snapshot_id" => source_start["snapshot_id"],
               "mode" => "checkpoint_status",
               "since" => 0,
               "candidate_high_water" => 63,
               "ref" => "status-a1"
             })

    assert %{"type" => "ok", "checkpoint_accepted" => false, "replay_started" => false} =
             reply(source_recovered)

    assert :ok = send_request(source_recovered, receipt_request)
    assert reply(source_recovered) == receipt_page

    assert :ok =
             send_request(source_recovered, %{
               "type" => "inbox_checkpoint_ack",
               "paging_version" => 1,
               "snapshot_id" => source_start["snapshot_id"],
               "high_water" => 63,
               "reconciled_batches" => ["batch-a1"],
               "pending_manifest_complete" => true,
               "ref" => "checkpoint-a1"
             })

    assert %{"type" => "ok", "checkpoint_accepted" => true} = reply(source_recovered)
    :ssl.close(source_recovered)
  end

  test "first inbox snapshot does not block a modern origin send", ctx do
    %{relay: relay, keys: keys} = ctx

    for n <- 1..240 do
      raw =
        JSON.encode!(%{
          "v" => 1,
          "type" => "forward",
          "op_id" => op(3000 + n),
          "message" => message("snapshot-#{n}", "message #{n}")
        }) <> "\n"

      assert {:ok, _} = Relay.request_line(relay, keys["a1"], raw)
    end

    assert {:ok, paging_session} = Relay.open_websocket_session(relay, keys["a2"], nil, nil)

    start = %{
      "v" => 1,
      "type" => "inbox_start",
      "paging_version" => 1,
      "startup_id" => "concurrent-start",
      "since" => 0,
      "ref" => "concurrent-start"
    }

    test_gate = self()
    :sys.replace_state(relay, &Map.put(&1, :inbox_snapshot_test_gate, test_gate))
    snapshot_started_at = System.monotonic_time(:microsecond)

    start_task =
      Task.async(fn ->
        Relay.session_request_line(relay, paging_session, JSON.encode!(start) <> "\n")
      end)

    assert_receive {:inbox_snapshot_worker_waiting, worker_pid, token}, 5_000

    next_message = %{
      "type" => "forward",
      "op_id" => op(4000),
      "message" => message("after-snapshot", "after snapshot")
    }

    request_started_at = System.monotonic_time(:microsecond)

    {request_result, request_duration} =
      try do
        result = store_request(relay, keys["a1"], next_message)

        {result, System.monotonic_time(:microsecond) - request_started_at}
      after
        send(worker_pid, {:continue_inbox_snapshot, token})
        :sys.replace_state(relay, &Map.delete(&1, :inbox_snapshot_test_gate))
      end

    assert %{"type" => "ok", "seq" => 241} = request_result

    assert {:ok, [start_line]} = Task.await(start_task, 10_000)
    assert {:ok, start_reply} = JSON.decode_line(start_line)
    assert start_reply["high_water"] == 240
    assert length(start_reply["items"]) == 20

    refute Enum.any?(start_reply["items"], &(&1["acceptance"]["message_id"] == "after-snapshot"))

    snapshot_duration = System.monotonic_time(:microsecond) - snapshot_started_at

    IO.puts(
      "inbox-start fixture: unrelated-request=#{request_duration}us snapshot-ready=#{snapshot_duration}us"
    )
  end

  test "paging reconciliation uses frozen membership for sequence-less receipts", ctx do
    %{certs: certs, web: web} = ctx

    before_snapshot = %{
      "type" => "subscribe_remote",
      "op_id" => op(3001),
      "revision" => 1,
      "sender_ids" => ["app:test"]
    }

    assert {:ok, initial_device} = connect(web, certs, "a2")
    assert upgrade(initial_device) =~ "101 Switching Protocols"
    assert :ok = send_request(initial_device, before_snapshot)
    assert %{"type" => "ok", "revision" => 1} = reply(initial_device)

    start_request = %{
      "type" => "inbox_start",
      "paging_version" => 1,
      "startup_id" => "sequence-less-receipt",
      "since" => 0,
      "ref" => "start-sequence-less"
    }

    assert :ok = send_request(initial_device, start_request)
    initial = reply(initial_device)
    assert initial["high_water"] == 0
    :ssl.close(initial_device)

    after_snapshot = %{
      "type" => "subscribe_remote",
      "op_id" => op(3002),
      "revision" => 2,
      "sender_ids" => ["app:test"]
    }

    assert {:ok, mutation_device} = connect(web, certs, "a2")
    assert upgrade(mutation_device) =~ "101 Switching Protocols"
    assert :ok = send_request(mutation_device, after_snapshot)
    assert %{"type" => "ok", "revision" => 2} = reply(mutation_device)
    :ssl.close(mutation_device)

    assert {:ok, recovery_device} = connect(web, certs, "a2")
    assert upgrade(recovery_device) =~ "101 Switching Protocols"
    assert :ok = send_request(recovery_device, start_request)
    assert reply(recovery_device) == initial

    reconcile = %{
      "type" => "inbox_reconcile",
      "paging_version" => 1,
      "snapshot_id" => initial["snapshot_id"],
      "batch_id" => "sequence-less-receipts",
      "operations" => [before_snapshot, after_snapshot],
      "ref" => "reconcile-sequence-less"
    }

    assert :ok = send_request(recovery_device, reconcile)

    assert %{
             "type" => "ok",
             "results" => [
               %{"op_id" => before_id, "status" => "committed", "result" => %{"revision" => 1}},
               %{"op_id" => after_id, "status" => "not_committed_at_snapshot"}
             ]
           } = reply(recovery_device)

    assert before_id == before_snapshot["op_id"]
    assert after_id == after_snapshot["op_id"]
    :ssl.close(recovery_device)
  end

  test "opt-in supervised listener starts with configured durable Relay and fences on stop",
       ctx do
    %{certs: certs, path: path} = ctx

    {:ok, server} =
      Server.start_link(
        store_path: path <> ".supervised",
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem")
      )

    relay = Server.relay(server)
    assert is_pid(relay)
    assert Server.port(server) > 0
    assert Server.enroll_port(server) > 0
    assert Server.owner_port(server) > 0
    assert :ok = Relay.setup_person(relay, "A")

    assert {:ok, _key} =
             Relay.setup_device_certificate(relay, "A", TLSFixtures.certificate_der!(certs, "a1"))

    {_, web, _, _} =
      Enum.find(Supervisor.which_children(server), fn {id, _, _, _} -> id == :web_socket end)

    assert {:ok, socket} = connect(web, certs, "a1")
    assert upgrade(socket) =~ "101 Switching Protocols"
    assert {[], %{"seq" => 0}} = resync(socket)
    assert :ok = Supervisor.stop(server)
    assert {:error, :closed} = :ssl.recv(socket, 2, 2_000)
  end

  test "unknown enrolled key gets no upgrade or relay data", ctx do
    %{certs: certs, web: web} = ctx
    assert {:ok, unknown} = connect(web, certs, "unknown")
    assert {:error, :closed} = :ssl.recv(unknown, 1, 2_000)
  end

  test "exact G8 readback remains scoped on the WebSocket transport", ctx do
    %{certs: certs, web: web, relay: relay, keys: keys} = ctx
    target = %{"kind" => "device", "id" => keys["a2"], "key_fingerprint" => keys["a2"]}
    grant = op(41)

    assert :ok =
             Relay.grant_enrollment_read_fixture(
               relay,
               "A",
               keys["a1"],
               grant,
               "source-X",
               target,
               "owner-fixture"
             )

    assert {:ok, a1} = connect(web, certs, "a1")
    assert upgrade(a1) =~ "101 Switching Protocols"

    request = %{
      "type" => "enrollment_readback",
      "proof_version" => 1,
      "grant_id" => grant,
      "source_instance_id" => "source-X",
      "target" => target
    }

    assert :ok = send_request(a1, request)
    assert %{"type" => "ok", "proof" => %{"state" => "active", "target" => ^target}} = reply(a1)
    assert :ok = Relay.revoke_enrollment_read_fixture(relay, "A", grant)
    assert :ok = send_request(a1, request)
    assert %{"type" => "error", "reason" => "not_found"} = reply(a1)
    :ssl.close(a1)
  end

  test "stored history and cursor acknowledgements survive SQLite restart", ctx do
    %{certs: certs, web: web, relay: relay, path: path, keys: keys} = ctx
    assert {:ok, a1} = connect(web, certs, "a1")
    assert upgrade(a1) =~ "101 Switching Protocols"
    assert {[], %{"seq" => 0}} = resync(a1)

    forward = %{
      "type" => "forward",
      "op_id" => op(31),
      "message" => message("restart", "persisted")
    }

    assert %{"type" => "ok", "seq" => 1} = store_request(relay, keys["a1"], forward)
    assert %{"type" => "delivery", "seq" => 1} = reply(a1)
    GenServer.stop(web)
    GenServer.stop(relay)

    {:ok, reopened} = Relay.start_link(store_path: path)

    {:ok, reopened_web} =
      WebSocketRelay.start_link(
        relay: reopened,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem")
      )

    on_exit(fn ->
      if Process.alive?(reopened_web), do: GenServer.stop(reopened_web)
      if Process.alive?(reopened), do: GenServer.stop(reopened)
    end)

    assert {:ok, restarted} = connect(reopened_web, certs, "a1")
    assert upgrade(restarted) =~ "101 Switching Protocols"
    assert :ok = send_request(restarted, %{"type" => "ack", "seq" => 1})
    assert %{"type" => "error", "reason" => "invalid_cursor"} = reply(restarted)
    assert {[%{"type" => "delivery", "seq" => 1}], %{"seq" => 1}} = resync(restarted)
    assert :ok = send_request(restarted, %{"type" => "ack", "seq" => 1})
    assert %{"type" => "ok", "seq" => 1} = reply(restarted)
    :ssl.close(restarted)
    GenServer.stop(reopened_web)
    GenServer.stop(reopened)

    {:ok, after_ack} = Relay.start_link(store_path: path)

    {:ok, after_ack_web} =
      WebSocketRelay.start_link(
        relay: after_ack,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem")
      )

    on_exit(fn ->
      if Process.alive?(after_ack_web), do: GenServer.stop(after_ack_web)
      if Process.alive?(after_ack), do: GenServer.stop(after_ack)
    end)

    assert {:ok, 1} = Relay.acknowledged_cursor(after_ack, keys["a1"])
    assert {:ok, current} = connect(after_ack_web, certs, "a1")
    assert upgrade(current) =~ "101 Switching Protocols"
    assert :ok = send_request(current, %{"type" => "resync", "since" => 1})
    assert %{"type" => "ok"} = reply(current)
    assert %{"type" => "resynced", "seq" => 1} = reply(current)
    :ssl.close(current)
  end

  test "listener-only shutdown fences WebSocket workers before returning", ctx do
    %{certs: certs, web: web, relay: relay, keys: keys} = ctx
    assert {:ok, socket} = connect(web, certs, "a1")
    assert upgrade(socket) =~ "101 Switching Protocols"
    assert {[], %{"seq" => 0}} = resync(socket)
    before = :sys.get_state(relay).store.engine

    assert :ok = GenServer.stop(web)
    assert {:error, :closed} = :ssl.recv(socket, 2, 2_000)

    _ =
      send_request(socket, %{
        "type" => "forward",
        "op_id" => op(45),
        "message" => message("late", "denied")
      })

    assert :sys.get_state(relay).store.engine == before
    assert {:ok, [], 0} = Relay.events(relay, keys["a1"], 0)
  end

  test "previously stored large messages remain readable and tenant scoped", ctx do
    %{certs: certs, web: web, relay: relay, keys: keys} = ctx
    assert {:ok, a1} = connect(web, certs, "a1")
    assert {:ok, a2} = connect(web, certs, "a2")
    assert {:ok, b1} = connect(web, certs, "b1")
    for socket <- [a1, a2, b1], do: assert(upgrade(socket) =~ "101")
    assert {[], %{"seq" => 0}} = resync(a1)
    assert {[], %{"seq" => 0}} = resync(a2)
    assert {[], %{"seq" => 0}} = resync(b1)

    body = String.duplicate("Ω", 550_000)

    message =
      Map.merge(message("large", "large"), %{
        "body" => body,
        "nested" => %{"big" => 9_007_199_254_740_993, "null" => nil}
      })

    assert %{"type" => "ok", "seq" => 1} =
             store_request(relay, keys["a1"], %{
               "type" => "forward",
               "op_id" => op(20),
               "message" => message
             })

    assert %{"type" => "delivery", "seq" => 1, "origin" => origin} = reply(a1)
    assert origin == keys["a1"]
    assert {:error, :timeout} = :ssl.recv(b1, 2, 200)
    assert %{"type" => "delivery", "message" => received} = reply(a2)
    assert received == message

    assert %{"type" => "ok", "seq" => 1} =
             store_request(relay, keys["b1"], %{
               "type" => "forward",
               "op_id" => op(21),
               "message" => message("large", "B-only")
             })

    assert %{"type" => "delivery", "seq" => 1, "origin" => b_origin} = reply(b1)
    assert b_origin == keys["b1"]
    assert {:error, :timeout} = :ssl.recv(a1, 2, 200)
    assert {:error, :timeout} = :ssl.recv(a2, 2, 200)
    :ssl.close(a1)
    :ssl.close(a2)
    :ssl.close(b1)
  end
end
