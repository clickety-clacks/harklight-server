defmodule AttentionServer.ClientAPITest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AttentionServer.{JSON, Relay, Store, TLSFixtures, WebSocketRelay}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @opaque_id ~r/\A[cr]_[A-Za-z0-9_-]{16}\z/

  setup_all do
    dir = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(dir) end)
    %{certs: dir}
  end

  setup %{certs: certs} do
    dir =
      Path.join(System.tmp_dir!(), "attention-client-api-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    flags = Path.join(dir, "flags.json")
    File.write!(flags, ~s({"client_api":true}))

    relay =
      start_link_supervised!(
        {Relay,
         store_path: Path.join(dir, "relay.sqlite3"),
         enrollment_fixture_mode: true,
         push_enabled: true,
         feature_flags_path: flags},
        restart: :temporary
      )

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

    web =
      start_link_supervised!(
        {WebSocketRelay,
         relay: relay,
         certfile: Path.join(certs, "server.pem"),
         keyfile: Path.join(certs, "server.key"),
         cacertfile: Path.join(certs, "ca.pem")},
        restart: :temporary
      )

    %{relay: relay, web: web, keys: keys, dir: dir, flags: flags}
  end

  # -- transport helpers ----------------------------------------------------

  defp connect(ctx, name) do
    assert {:ok, socket} =
             :ssl.connect({127, 0, 0, 1}, WebSocketRelay.port(ctx.web),
               versions: [:"tlsv1.3"],
               verify: :verify_peer,
               cacertfile: String.to_charlist(Path.join(ctx.certs, "ca.pem")),
               certfile: String.to_charlist(Path.join(ctx.certs, "#{name}.pem")),
               keyfile: String.to_charlist(Path.join(ctx.certs, "#{name}.key")),
               server_name_indication: ~c"relay.test",
               session_tickets: :disabled,
               active: false,
               mode: :binary,
               packet: :raw
             )

    socket
  end

  defp read_head(socket, buffer \\ <<>>) do
    if String.ends_with?(buffer, "\r\n\r\n") do
      buffer
    else
      assert {:ok, byte} = :ssl.recv(socket, 1, 2_000)
      read_head(socket, buffer <> byte)
    end
  end

  # Opens the device's stream WebSocket and returns its connection ID.
  defp open_stream(ctx, name) do
    socket = connect(ctx, name)

    assert :ok =
             :ssl.send(socket, [
               "GET /relay HTTP/1.1\r\nHost: relay.test\r\nUpgrade: websocket\r\n",
               "Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n",
               "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
               "Sec-WebSocket-Protocol: attention-relay/1\r\n\r\n"
             ])

    head = read_head(socket)
    assert head =~ "HTTP/1.1 101 Switching Protocols\r\n"
    assert [_, connection_id] = Regex.run(~r/\r\nHarklight-Connection-Id: (\S+)\r\n/, head)
    assert connection_id =~ @opaque_id
    {socket, connection_id, head}
  end

  # One HTTPS request on its own connection; returns status, headers, body.
  defp http(ctx, name, method, path, body, extra \\ []) do
    socket = connect(ctx, name)

    headers =
      ["Host: relay.test", "Content-Type: application/json"] ++
        if(body, do: ["Content-Length: #{byte_size(body)}"], else: []) ++ extra

    assert :ok =
             :ssl.send(socket, [
               method,
               " ",
               path,
               " HTTP/1.1\r\n",
               Enum.map(headers, &[&1, "\r\n"]),
               "\r\n",
               body || ""
             ])

    response = read_all(socket, <<>>)
    :ssl.close(socket)
    [head, payload] = String.split(response, "\r\n\r\n", parts: 2)
    [status_line | header_lines] = String.split(head, "\r\n")
    ["HTTP/1.1", status | _] = String.split(status_line, " ")

    fields =
      Map.new(header_lines, fn line ->
        [key, value] = String.split(line, ": ", parts: 2)
        {String.downcase(key), value}
      end)

    assert fields["harklight-request-id"] =~ @opaque_id
    assert fields["content-length"] == Integer.to_string(byte_size(payload))
    {String.to_integer(status), fields, payload}
  end

  defp read_all(socket, buffer) do
    case :ssl.recv(socket, 0, 5_000) do
      {:ok, chunk} -> read_all(socket, buffer <> chunk)
      {:error, :closed} -> buffer
    end
  end

  defp post(ctx, name, frame) do
    assert {200, _fields, payload} =
             http(ctx, name, "POST", "/client/v1", JSON.encode!(Map.put_new(frame, "v", 1)))

    assert {:ok, reply} = JSON.decode_line(payload)
    reply
  end

  defp frame(payload) do
    mask = :crypto.strong_rand_bytes(4)
    length = byte_size(payload)

    header =
      cond do
        length < 126 -> <<1::1, 0::3, 1::4, 1::1, length::7>>
        length < 65_536 -> <<1::1, 0::3, 1::4, 1::1, 126::7, length::16>>
        true -> <<1::1, 0::3, 1::4, 1::1, 127::7, length::64>>
      end

    repeated = :binary.copy(mask, div(length + 3, 4))
    [header, mask, :crypto.exor(payload, binary_part(repeated, 0, length))]
  end

  defp request(socket, payload) do
    assert :ok =
             :ssl.send(socket, frame(JSON.encode!(Map.put_new(payload, "v", 1)) <> "\n"))

    reply(socket)
  end

  defp recv_frame(socket, timeout \\ 2_000) do
    assert {:ok, <<1::1, 0::3, opcode::4, 0::1, length::7>>} = :ssl.recv(socket, 2, timeout)

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

    if length == 0 do
      {opcode, <<>>}
    else
      assert {:ok, payload} = :ssl.recv(socket, length, timeout)
      {opcode, payload}
    end
  end

  defp reply(socket) do
    assert {1, line} = recv_frame(socket)
    assert {:ok, value} = JSON.decode_line(line)
    value
  end

  defp assert_close_reason(socket, code, reason) do
    assert {8, <<^code::16, ^reason::binary>>} = recv_frame(socket)
    assert {:error, :closed} = :ssl.recv(socket, 0, 2_000)
  end

  defp assert_open(socket), do: assert({:error, :timeout} = :ssl.recv(socket, 0, 150))

  defp store_request(relay, key, payload) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put_new(payload, "v", 1)) <> "\n")

    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp op(n), do: "00000000-0000-4000-8000-" <> String.pad_leading("#{n}", 12, "0")

  defp forward(ctx, id, n) do
    assert %{"type" => "ok"} =
             store_request(ctx.relay, ctx.keys["a1"], %{
               "type" => "forward",
               "op_id" => op(n),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => id,
                 "created_at" => n,
                 "title" => "Question #{id}"
               }
             })
  end

  defp sessions_of(relay, key) do
    :sys.get_state(relay).sessions
    |> Map.values()
    |> Enum.filter(&(&1.key == key))
  end

  defp answer(message_id, n, text \\ "yes"),
    do: %{
      "type" => "answer",
      "op_id" => op(n),
      "sender_id" => "app:test",
      "message_id" => message_id,
      "answer" => text
    }

  # -- every read and mutation by request while the stream is held ----------

  test "requests serve every read and mutation while the stream is in checkpoint_pending",
       ctx do
    for {id, n} <- [{"m1", 1}, {"m2", 2}, {"m3", 3}, {"m4", 4}], do: forward(ctx, id, n)

    {stream, stream_id, _} = open_stream(ctx, "a2")

    assert %{"type" => "ok", "snapshot_id" => snapshot_id} =
             request(stream, %{
               "type" => "inbox_start",
               "paging_version" => 1,
               "startup_id" => "startup-a2",
               "since" => 0,
               "ref" => "start-a2"
             })

    # The stream is held by its checkpoint: its own mutations are refused.
    assert %{"type" => "error", "reason" => "checkpoint_pending"} =
             request(stream, answer("m2", 9))

    assert {:ok, %{status: "pending"}} =
             Store.inbox_checkpoint_status(
               :sys.get_state(ctx.relay).store,
               "A",
               ctx.keys["a2"],
               snapshot_id
             )

    # Snapshot reads, with the stream positions they reflect.
    assert %{"type" => "ok", "rows" => rows, "seq" => 4, "conversation_seq" => _} =
             post(ctx, "a2", %{"type" => "inbox_list", "ref" => "list"})

    assert length(rows) == 4

    assert %{"type" => "ok", "ref" => "page", "row" => %{"message_id" => "m1"}} =
             post(ctx, "a2", %{
               "type" => "conversation_page",
               "ref" => "page",
               "sender_id" => "app:test",
               "message_id" => "m1"
             })

    assert %{"type" => "ok", "view_rev" => _} =
             post(ctx, "a2", %{"type" => "view_get", "ref" => "view"})

    assert %{"type" => "ok"} = post(ctx, "a2", %{"type" => "levels_get", "ref" => "levels"})

    # Mark Seen.
    assert %{"type" => "ok", "opened" => true} =
             post(ctx, "a2", %{
               "type" => "message_open",
               "op_id" => op(10),
               "sender_id" => "app:test",
               "message_id" => "m1",
               "request_revision" => 1
             })

    # Discussion reply, the origin's answer to it, and the read position.
    assert %{"type" => "ok", "event_id" => discussion_id} =
             post(ctx, "a2", %{
               "type" => "discussion",
               "op_id" => op(11),
               "sender_id" => "app:test",
               "message_id" => "m1",
               "request_revision" => 1,
               "text" => "Could you clarify?"
             })

    # conversation_reply is an origin frame, not a client API type.
    assert %{"type" => "error", "reason" => "wrong_endpoint"} =
             post(ctx, "a1", %{"type" => "conversation_reply", "op_id" => op(12)})

    assert %{"type" => "ok", "seq" => reply_seq} =
             store_request(ctx.relay, ctx.keys["a1"], %{
               "type" => "conversation_reply",
               "op_id" => op(12),
               "sender_id" => "app:test",
               "message_id" => "m1",
               "request_revision" => 1,
               "in_reply_to" => String.to_integer(discussion_id),
               "text" => "Here is more detail"
             })

    assert %{"type" => "ok", "read_through_conversation_seq" => ^reply_seq} =
             post(ctx, "a2", %{
               "type" => "conversation_read",
               "op_id" => op(13),
               "sender_id" => "app:test",
               "message_id" => "m1",
               "read_through_conversation_seq" => reply_seq
             })

    # Answer, which the held stream was just refused.
    assert %{"type" => "ok", "seq" => _} = post(ctx, "a2", answer("m2", 14))

    # The origin's lifecycle on another item.
    assert %{"type" => "ok"} =
             post(ctx, "a1", %{
               "type" => "lifecycle",
               "op_id" => op(15),
               "sender_id" => "app:test",
               "message_id" => "m4",
               "state" => "cleared",
               "at" => 1
             })

    # No comment: served with its existing reply, not refused by the gate.
    no_comment =
      post(ctx, "a2", %{
        "type" => "answer_no_comment",
        "op_id" => op(16),
        "request_id" => "m3",
        "request_revision" => 1
      })

    refute no_comment["reason"] in ["wrong_endpoint", "checkpoint_pending"]

    # Priority levels.
    assert %{"type" => "ok"} =
             post(ctx, "a2", %{
               "type" => "levels_set",
               "ref" => "levels-set",
               "op_id" => op(17),
               "levels" => [
                 %{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil}
               ]
             })

    # Push registration, status and removal.
    assert %{"type" => "ok"} =
             post(ctx, "a2", %{
               "type" => "push_register",
               "op_id" => op(18),
               "revision" => 1,
               "profile_id" => "harklight-sandbox",
               "address_kind" => "apns_token",
               "address" => String.duplicate("ab", 32)
             })

    assert %{"type" => "ok"} = post(ctx, "a2", %{"type" => "push_status", "ref" => "status"})

    assert %{"type" => "ok"} =
             post(ctx, "a2", %{"type" => "push_remove", "op_id" => op(20), "revision" => 2})

    # The stream is unaffected: open, still paging the same snapshot and
    # still held by its checkpoint. Requests created no session.
    assert_open(stream)
    assert [session] = sessions_of(ctx.relay, ctx.keys["a2"])
    assert session.connection_id == stream_id
    assert session.sync_mode == :paging
    assert session.snapshot_id == snapshot_id

    assert %{"type" => "error", "reason" => "checkpoint_pending"} =
             request(stream, answer("m3", 21))

    :ssl.close(stream)
  end

  # -- nothing is closed by a request ----------------------------------------

  test "a request closes nothing and parallel requests all succeed beside the stream", ctx do
    {stream, _, _} = open_stream(ctx, "a2")
    assert %{"type" => "ok"} = request(stream, %{"type" => "resync", "since" => 0})
    assert %{"type" => "resynced", "seq" => 0} = reply(stream)

    results =
      1..12
      |> Enum.map(fn n ->
        Task.async(fn ->
          if rem(n, 2) == 0 do
            post(ctx, "a2", %{"type" => "inbox_list", "ref" => "list-#{n}"})
          else
            post(ctx, "a2", %{"type" => "levels_get", "ref" => "levels-#{n}"})
          end
        end)
      end)
      |> Task.await_many(10_000)

    assert Enum.all?(results, &(&1["type"] == "ok"))
    assert Enum.map(results, & &1["ref"]) |> Enum.uniq() |> length() == 12

    assert_open(stream)
    assert [_] = sessions_of(ctx.relay, ctx.keys["a2"])

    # The stream still carries live changes.
    forward(ctx, "live", 1)
    assert %{"type" => "delivery", "message_id" => "live"} = reply(stream)
    :ssl.close(stream)
  end

  # -- stream displacement and close reasons ---------------------------------

  test "a new stream closes only the old stream, with superseded_by_new_stream", ctx do
    {old_stream, old_id, _} = open_stream(ctx, "a2")
    {other_device, _, _} = open_stream(ctx, "b1")

    {new_stream, new_id, _} = open_stream(ctx, "a2")
    refute new_id == old_id
    assert_close_reason(old_stream, 4409, "superseded_by_new_stream")

    assert_open(new_stream)
    assert_open(other_device)
    assert [%{connection_id: ^new_id}] = sessions_of(ctx.relay, ctx.keys["a2"])
    Enum.each([new_stream, other_device], &:ssl.close/1)
  end

  test "revocation closes the stream with device_revoked and refuses new requests", ctx do
    {stream, _, _} = open_stream(ctx, "a2")
    assert %{"type" => "ok"} = post(ctx, "a2", %{"type" => "inbox_list"})

    assert :ok = Relay.revoke_device(ctx.relay, ctx.keys["a2"])
    assert_close_reason(stream, 4403, "device_revoked")

    # No HTTP response reaches a revoked device.
    case :ssl.connect({127, 0, 0, 1}, WebSocketRelay.port(ctx.web),
           versions: [:"tlsv1.3"],
           verify: :verify_peer,
           cacertfile: String.to_charlist(Path.join(ctx.certs, "ca.pem")),
           certfile: String.to_charlist(Path.join(ctx.certs, "a2.pem")),
           keyfile: String.to_charlist(Path.join(ctx.certs, "a2.key")),
           server_name_indication: ~c"relay.test",
           session_tickets: :disabled,
           active: false,
           mode: :binary,
           packet: :raw
         ) do
      {:ok, socket} ->
        body = JSON.encode!(%{"v" => 1, "type" => "inbox_list"})

        _ =
          :ssl.send(socket, [
            "POST /client/v1 HTTP/1.1\r\nHost: relay.test\r\nContent-Length: ",
            Integer.to_string(byte_size(body)),
            "\r\n\r\n",
            body
          ])

        assert {:error, _} = :ssl.recv(socket, 0, 2_000)

      {:error, _} ->
        :ok
    end
  end

  test "listener shutdown closes the stream with server_shutdown", ctx do
    {stream, _, _} = open_stream(ctx, "a2")
    assert :ok = GenServer.stop(ctx.web)
    assert_close_reason(stream, 1001, "server_shutdown")
  end

  test "an undecodable frame closes the stream with protocol_error", ctx do
    {stream, _, _} = open_stream(ctx, "a2")
    assert :ok = :ssl.send(stream, <<0xF1, 0x80, 0, 0, 0, 0>>)
    assert %{"type" => "error", "reason" => "bad_frame"} = reply(stream)
    assert_close_reason(stream, 1002, "protocol_error")
  end

  # -- operation-ID dedupe across request and socket -------------------------

  test "the same op_id by request and by socket returns the first outcome", ctx do
    forward(ctx, "m1", 1)
    forward(ctx, "m2", 2)
    {stream, _, _} = open_stream(ctx, "a2")

    # Request first, then the socket.
    assert %{"type" => "ok", "seq" => first_seq} = post(ctx, "a2", answer("m1", 30))

    assert %{"type" => "ok", "seq" => ^first_seq, "duplicate" => true} =
             request(stream, answer("m1", 30))

    # Socket first, then a request.
    discussion = %{
      "type" => "discussion",
      "op_id" => op(31),
      "sender_id" => "app:test",
      "message_id" => "m2",
      "request_revision" => 1,
      "text" => "Question"
    }

    assert %{"type" => "ok", "event_id" => event_id} = request(stream, discussion)

    assert %{"type" => "ok", "event_id" => ^event_id, "duplicate" => true} =
             post(ctx, "a2", discussion)

    # Same op_id, different content.
    assert %{"type" => "error", "reason" => "operation_conflict"} =
             post(ctx, "a2", %{discussion | "text" => "Different question"})

    assert %{"type" => "error", "reason" => "operation_conflict"} =
             request(stream, answer("m1", 30, "no"))

    :ssl.close(stream)
  end

  # -- endpoint refusals, statuses and the flag ------------------------------

  test "stream-only and unknown types are refused wrong_endpoint without side effects", ctx do
    forward(ctx, "m1", 1)

    for frame <- [
          %{"type" => "resync", "since" => 0},
          %{"type" => "ack", "seq" => 1},
          %{"type" => "conversation_open", "since" => 0, "bounded" => true},
          %{"type" => "conversation_ack", "seq" => 1},
          %{"type" => "focus_report", "screen" => "inbox"},
          %{"type" => "inbox_start", "paging_version" => 1, "startup_id" => "s", "since" => 0},
          %{"type" => "inbox_page", "paging_version" => 1},
          %{"type" => "inbox_reconcile", "paging_version" => 1},
          %{"type" => "inbox_receipts", "paging_version" => 1},
          %{"type" => "inbox_checkpoint_ack", "paging_version" => 1},
          %{"type" => "inbox_attach", "paging_version" => 1},
          %{"type" => "forward", "op_id" => op(40), "message" => %{}},
          %{"type" => "no_such_type"}
        ] do
      ref = "refused-" <> frame["type"]

      assert %{"type" => "error", "reason" => "wrong_endpoint", "ref" => ^ref} =
               post(ctx, "a2", Map.put(frame, "ref", ref))
    end

    assert {:ok, 0} = Relay.acknowledged_cursor(ctx.relay, ctx.keys["a2"])

    assert sessions_of(ctx.relay, ctx.keys["a2"]) == []
  end

  test "malformed, oversized and wrong-method requests get HTTP errors", ctx do
    assert {400, _, body} = http(ctx, "a2", "POST", "/client/v1", "not json")
    assert {:ok, %{"reason" => "bad_frame"}} = JSON.decode_line(body)

    assert {400, _, _} = http(ctx, "a2", "POST", "/client/v1", ~s({"v":1}))
    assert {400, _, _} = http(ctx, "a2", "POST", "/client/v1", ~s({"type":"inbox_list"}))
    assert {400, _, _} = http(ctx, "a2", "POST", "/client/v1", nil)

    assert {413, _, body} =
             http(ctx, "a2", "POST", "/client/v1", nil, ["Content-Length: #{3 * 1024 * 1024}"])

    assert {:ok, %{"reason" => "too_large"}} = JSON.decode_line(body)

    assert {405, _, _} = http(ctx, "a2", "GET", "/client/v1", nil)
    assert {404, _, _} = http(ctx, "a2", "POST", "/client/v1/other", "{}")
    assert sessions_of(ctx.relay, ctx.keys["a2"]) == []
  end

  test "with the client_api flag off, /client/v1 answers 404", ctx do
    for contents <- ["{}", ~s({"client_api":false})] do
      File.write!(ctx.flags, contents)

      assert {404, _, body} =
               http(ctx, "a2", "POST", "/client/v1", ~s({"v":1,"type":"inbox_list"}))

      assert {:ok, %{"reason" => "not_found"}} = JSON.decode_line(body)
    end

    File.rm!(ctx.flags)

    capture_log(fn ->
      assert {404, _, _} = http(ctx, "a2", "POST", "/client/v1", ~s({"v":1,"type":"inbox_list"}))
    end)

    # The socket is unaffected by the flag.
    {stream, _, _} = open_stream(ctx, "a2")
    assert %{"type" => "ok", "rows" => []} = request(stream, %{"type" => "inbox_list"})
    :ssl.close(stream)
  end

  # -- legacy clients and connection IDs -------------------------------------

  test "a legacy client is unchanged apart from the connection ID header and close reason",
       ctx do
    forward(ctx, "m1", 1)
    {socket, connection_id, head} = open_stream(ctx, "a2")

    assert head ==
             "HTTP/1.1 101 Switching Protocols\r\n" <>
               "Upgrade: websocket\r\n" <>
               "Connection: Upgrade\r\n" <>
               "Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n" <>
               "Sec-WebSocket-Protocol: attention-relay/1\r\n" <>
               "Harklight-Connection-Id: #{connection_id}\r\n\r\n"

    # No admission frame; reads, sync and mutations all work on the socket.
    assert %{"type" => "ok", "rows" => [_]} = request(socket, %{"type" => "inbox_list"})
    assert %{"type" => "ok"} = request(socket, %{"type" => "resync", "since" => 0})
    assert %{"type" => "delivery", "message_id" => "m1"} = reply(socket)
    assert %{"type" => "resynced", "seq" => 1} = reply(socket)
    assert %{"type" => "ok", "seq" => 2} = request(socket, answer("m1", 50))
    assert %{"type" => "lifecycle", "seq" => 2} = reply(socket)

    # Other non-upgrade requests are still refused as before.
    other = connect(ctx, "a2")
    assert :ok = :ssl.send(other, "GET /elsewhere HTTP/1.1\r\nHost: relay.test\r\n\r\n")
    assert read_head(other) =~ "HTTP/1.1 400 Bad Request\r\n"
    :ssl.close(other)
    :ssl.close(socket)
  end

  test "connection and request IDs are logged with open, close and request outcome", ctx do
    log =
      capture_log([level: :info], fn ->
        {first, first_id, _} = open_stream(ctx, "a2")
        {second, second_id, _} = open_stream(ctx, "a2")
        assert_close_reason(first, 4409, "superseded_by_new_stream")

        assert {200, fields, _} =
                 http(ctx, "a2", "POST", "/client/v1", ~s({"v":1,"type":"resync","since":0}))

        assert [%{connection_id: ^second_id}] = sessions_of(ctx.relay, ctx.keys["a2"])
        send(self(), {:ids, first_id, second_id, fields["harklight-request-id"]})
        :ssl.close(second)
        Process.sleep(200)
      end)

    assert_received {:ids, first_id, second_id, request_id}

    assert log =~
             "event=harklight_connection phase=open connection_id=#{first_id} transport=websocket"

    assert log =~
             "event=harklight_connection phase=open connection_id=#{second_id} transport=websocket"

    assert log =~
             "event=harklight_connection phase=close connection_id=#{first_id} transport=websocket reason=superseded_by_new_stream"

    assert log =~
             ~r/event=harklight_client_request request_id=#{request_id} type=other status=200 reason=wrong_endpoint duration_ms=\d+/

    assert log =~ "phase=close connection_id=#{second_id} transport=websocket"
  end
end
