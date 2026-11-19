defmodule AttentionServer.ConnectionRolesTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AttentionServer.{JSON, Relay, Store, TLSFixtures, WebSocketRelay}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @connection_id ~r/\Ac_[A-Za-z0-9_-]{16}\z/

  setup_all do
    dir = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(dir) end)
    %{certs: dir}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "attention-roles-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    relay =
      start_link_supervised!(
        {Relay,
         store_path: Path.join(dir, "relay.sqlite3"),
         enrollment_fixture_mode: true,
         push_enabled: true},
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

    %{relay: relay, web: web, keys: keys, dir: dir}
  end

  # -- wire helpers ---------------------------------------------------------

  defp connect(web, certs, name) do
    assert {:ok, socket} =
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

    socket
  end

  defp upgrade(socket, path) do
    request = [
      "GET ",
      path,
      " HTTP/1.1\r\n",
      "Host: relay.test\r\n",
      "Upgrade: websocket\r\n",
      "Connection: Upgrade\r\n",
      "Sec-WebSocket-Version: 13\r\n",
      "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
      "Sec-WebSocket-Protocol: attention-relay/1\r\n\r\n"
    ]

    assert :ok = :ssl.send(socket, request)
    read_http(socket, <<>>)
  end

  defp read_http(socket, buffer) do
    if String.ends_with?(buffer, "\r\n\r\n") do
      buffer
    else
      assert {:ok, byte} = :ssl.recv(socket, 1, 2_000)
      read_http(socket, buffer <> byte)
    end
  end

  # Opens a WebSocket for a device. A declared role must be admitted with a
  # connection ID before anything else arrives.
  defp open(ctx, name, role) do
    socket = connect(ctx.web, ctx.certs, name)

    case role do
      nil ->
        assert upgrade(socket, "/relay") =~ "101 Switching Protocols"
        {socket, nil}

      role ->
        assert upgrade(socket, "/relay?role=#{role}") =~ "101 Switching Protocols"

        assert %{
                 "v" => 1,
                 "type" => "connection_admitted",
                 "role" => ^role,
                 "connection_id" => connection_id
               } = reply(socket)

        assert connection_id =~ @connection_id
        {socket, connection_id}
    end
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

  defp send_request(socket, payload),
    do: :ssl.send(socket, frame(JSON.encode!(Map.put_new(payload, "v", 1)) <> "\n"))

  defp request(socket, payload) do
    assert :ok = send_request(socket, payload)
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

    payload =
      if length == 0 do
        <<>>
      else
        assert {:ok, payload} = :ssl.recv(socket, length, timeout)
        payload
      end

    {opcode, payload}
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

  # Brings the device's stream into a paging snapshot whose checkpoint is not
  # yet accepted, the state that refuses the stream's own mutations.
  defp start_pending_snapshot(stream) do
    assert %{"type" => "ok", "snapshot_id" => snapshot_id} =
             request(stream, %{
               "type" => "inbox_start",
               "paging_version" => 1,
               "startup_id" => "startup-a2",
               "since" => 0,
               "ref" => "start-a2"
             })

    snapshot_id
  end

  # -- (1) request connection unaffected by the stream's checkpoint ----------

  test "a request connection reads and mutates while the stream is in checkpoint_pending replay",
       ctx do
    for {id, n} <- [{"m1", 1}, {"m2", 2}, {"m3", 3}], do: forward(ctx, id, n)

    {stream, _} = open(ctx, "a2", "stream")
    snapshot_id = start_pending_snapshot(stream)

    # The stream is held by its checkpoint: its own acknowledgement waits.
    assert %{"type" => "error", "reason" => "checkpoint_pending"} =
             request(stream, %{"type" => "ack", "seq" => 3})

    state = :sys.get_state(ctx.relay)

    assert {:ok, %{status: "pending"}} =
             Store.inbox_checkpoint_status(state.store, "A", ctx.keys["a2"], snapshot_id)

    {requests, _} = open(ctx, "a2", "request")
    assert_open(stream)

    # Reads.
    assert %{"type" => "ok", "rows" => rows, "view_rev" => view_rev} =
             request(requests, %{"type" => "inbox_list", "ref" => "list"})

    assert is_binary(view_rev)
    assert length(rows) == 3

    assert %{"type" => "ok", "row" => %{"message_id" => "m1"}, "view_rev" => _} =
             request(requests, %{
               "type" => "conversation_page",
               "ref" => "page",
               "sender_id" => "app:test",
               "message_id" => "m1"
             })

    assert %{"type" => "ok", "view_rev" => _} =
             request(requests, %{"type" => "view_get", "ref" => "view"})

    assert %{"type" => "ok"} = request(requests, %{"type" => "levels_get", "ref" => "levels"})

    # Mark Seen.
    assert %{"type" => "ok", "opened" => true} =
             request(requests, %{
               "type" => "message_open",
               "op_id" => op(10),
               "sender_id" => "app:test",
               "message_id" => "m1",
               "request_revision" => 1
             })

    # Discussion reply, then the origin answers it and the device records its
    # read position.
    discussion = %{
      "type" => "discussion",
      "op_id" => op(11),
      "sender_id" => "app:test",
      "message_id" => "m1",
      "request_revision" => 1,
      "text" => "Could you clarify?"
    }

    assert %{"type" => "ok", "event_id" => discussion_id} = request(requests, discussion)

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
             request(requests, %{
               "type" => "conversation_read",
               "op_id" => op(13),
               "sender_id" => "app:test",
               "message_id" => "m1",
               "read_through_conversation_seq" => reply_seq
             })

    # Answer: the stream would be refused this with checkpoint_pending.
    answer = %{
      "type" => "answer",
      "op_id" => op(14),
      "sender_id" => "app:test",
      "message_id" => "m2",
      "answer" => "yes"
    }

    assert %{"type" => "ok", "seq" => answer_seq} = request(requests, answer)
    refute Map.has_key?(request(requests, %{answer | "op_id" => op(15)}), "duplicate")

    # Priority levels.
    assert %{"type" => "ok"} =
             request(requests, %{
               "type" => "levels_set",
               "ref" => "levels-set",
               "op_id" => op(16),
               "levels" => [
                 %{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil}
               ]
             })

    # Push registration.
    assert %{"type" => "ok"} =
             request(requests, %{
               "type" => "push_register",
               "op_id" => op(17),
               "revision" => 1,
               "profile_id" => "harklight-sandbox",
               "address_kind" => "apns_token",
               "address" => String.duplicate("ab", 32)
             })

    # Upload: files travel over the upload endpoint, then attach through a
    # discussion reply sent on the request connection.
    bytes = "attached context"
    path = Path.join(ctx.dir, "context.txt")
    File.write!(path, bytes)
    hash = :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

    assert {:ok, %{"upload_id" => upload_id}} =
             Relay.upload_file(
               ctx.relay,
               ctx.keys["a2"],
               %{
                 "op_id" => op(18),
                 "sender_id" => "app:test",
                 "message_id" => "m3",
                 "revision" => 1,
                 "name" => "context.txt"
               },
               path,
               byte_size(bytes),
               hash
             )

    assert %{"type" => "ok", "event_id" => _} =
             request(requests, %{
               "type" => "discussion",
               "op_id" => op(18),
               "sender_id" => "app:test",
               "message_id" => "m3",
               "request_revision" => 1,
               "text" => "See attached",
               "files" => [
                 %{
                   "name" => "context.txt",
                   "upload_id" => upload_id,
                   "size" => byte_size(bytes),
                   "sha256" => hash
                 }
               ]
             })

    # The stream is unaffected: still open, still paging the same snapshot,
    # still held by its checkpoint.
    assert_open(stream)

    [stream_session] = Enum.filter(sessions_of(ctx.relay, ctx.keys["a2"]), &(&1.role == :stream))
    assert stream_session.sync_mode == :paging
    assert stream_session.snapshot_id == snapshot_id

    assert %{"type" => "error", "reason" => "checkpoint_pending"} =
             request(stream, %{"type" => "ack", "seq" => 3})

    # The request connection never entered paging mode or held a cursor.
    [request_session] =
      Enum.filter(sessions_of(ctx.relay, ctx.keys["a2"]), &(&1.role == :request))

    assert request_session.sync_mode == nil
    assert request_session.ready == false

    # Operation-ID dedupe is per device: a retry of the same answer or
    # discussion on a different connection returns the first result.
    {retry, _} = open(ctx, "a2", "request")

    assert %{"type" => "ok", "seq" => ^answer_seq, "duplicate" => true} =
             request(retry, answer)

    assert %{"type" => "ok", "event_id" => ^discussion_id, "duplicate" => true} =
             request(retry, discussion)

    assert_open(stream)
    assert_open(requests)
    Enum.each([stream, requests, retry], &:ssl.close/1)
  end

  # -- (2) a request open closes nothing -----------------------------------

  test "opening request connections closes no other connection", ctx do
    {legacy, _} = open(ctx, "a2", nil)
    assert %{"type" => "ok"} = request(legacy, %{"type" => "resync", "since" => 0})
    assert %{"type" => "resynced", "seq" => 0} = reply(legacy)

    {first, _} = open(ctx, "a2", "request")
    {second, _} = open(ctx, "a2", "request")

    assert_open(legacy)
    assert_open(first)
    assert length(sessions_of(ctx.relay, ctx.keys["a2"])) == 3

    # The stream still receives live events after both request opens.
    forward(ctx, "live", 1)
    assert %{"type" => "delivery", "message_id" => "live"} = reply(legacy)

    assert %{"type" => "ok"} = request(first, %{"type" => "inbox_list"})
    assert %{"type" => "ok"} = request(second, %{"type" => "inbox_list"})
    Enum.each([legacy, first, second], &:ssl.close/1)
  end

  # -- (3) a new stream supersedes only the old stream ---------------------

  test "a new stream closes only the earlier stream, reason first", ctx do
    {old_stream, _} = open(ctx, "a2", "stream")
    {requests, _} = open(ctx, "a2", "request")
    {other_device, _} = open(ctx, "b1", "stream")

    {new_stream, _} = open(ctx, "a2", "stream")
    assert_close_reason(old_stream, 4409, "superseded_by_new_stream")

    assert_open(requests)
    assert_open(other_device)
    assert %{"type" => "ok"} = request(requests, %{"type" => "inbox_list"})

    # A legacy client's new connection is also a new stream: it supersedes the
    # declared stream and still leaves the request connection alone.
    {legacy, _} = open(ctx, "a2", nil)
    assert_close_reason(new_stream, 4409, "superseded_by_new_stream")
    assert_open(requests)
    assert %{"type" => "ok"} = request(requests, %{"type" => "inbox_list"})

    roles = ctx.relay |> sessions_of(ctx.keys["a2"]) |> Enum.map(& &1.role) |> Enum.sort()
    assert roles == [:legacy, :request]
    Enum.each([requests, other_device, legacy], &:ssl.close/1)
  end

  # -- (4) revocation closes everything ------------------------------------

  test "device revocation closes every connection of the device with device_revoked", ctx do
    {stream, _} = open(ctx, "a2", "stream")
    {first, _} = open(ctx, "a2", "request")
    {second, _} = open(ctx, "a2", "request")
    {other_device, _} = open(ctx, "b1", "request")

    assert :ok = Relay.revoke_device(ctx.relay, ctx.keys["a2"])

    for socket <- [stream, first, second],
        do: assert_close_reason(socket, 4403, "device_revoked")

    assert sessions_of(ctx.relay, ctx.keys["a2"]) == []
    assert_open(other_device)
    assert %{"type" => "ok"} = request(other_device, %{"type" => "inbox_list"})
    :ssl.close(other_device)
  end

  test "listener shutdown closes declared roles with server_shutdown", ctx do
    {stream, _} = open(ctx, "a2", "stream")
    {requests, _} = open(ctx, "a2", "request")

    assert :ok = GenServer.stop(ctx.web)

    for socket <- [stream, requests],
        do: assert_close_reason(socket, 1001, "server_shutdown")
  end

  test "a malformed frame closes a declared role with protocol_error", ctx do
    {requests, _} = open(ctx, "a2", "request")

    # Reserved bits set: the frame cannot be decoded.
    assert :ok = :ssl.send(requests, <<0xF1, 0x80, 0, 0, 0, 0>>)
    assert %{"type" => "error", "reason" => "bad_frame"} = reply(requests)
    assert_close_reason(requests, 1002, "protocol_error")
  end

  # -- (5) role refusals ----------------------------------------------------

  test "declared roles refuse the other role's frames with wrong_connection_role", ctx do
    forward(ctx, "m1", 1)
    {stream, _} = open(ctx, "a2", "stream")
    {requests, _} = open(ctx, "a2", "request")

    stream_refused = [
      %{
        "type" => "answer",
        "op_id" => op(20),
        "sender_id" => "app:test",
        "message_id" => "m1",
        "answer" => "no"
      },
      %{
        "type" => "message_open",
        "op_id" => op(21),
        "sender_id" => "app:test",
        "message_id" => "m1",
        "request_revision" => 1
      },
      %{"type" => "inbox_list"},
      %{"type" => "conversation_page", "sender_id" => "app:test", "message_id" => "m1"},
      %{"type" => "view_get"},
      %{"type" => "levels_get"}
    ]

    for frame <- stream_refused do
      ref = "stream-" <> frame["type"]

      assert %{"type" => "error", "reason" => "wrong_connection_role", "ref" => ^ref} =
               request(stream, Map.put(frame, "ref", ref))
    end

    request_refused = [
      %{"type" => "resync", "since" => 0},
      %{"type" => "ack", "seq" => 1},
      %{"type" => "conversation_open", "since" => 0},
      %{"type" => "conversation_ack", "seq" => 1},
      %{
        "type" => "inbox_start",
        "paging_version" => 1,
        "startup_id" => "startup-request",
        "since" => 0
      },
      %{"type" => "focus_report", "screen" => "inbox"}
    ]

    for frame <- request_refused do
      ref = "request-" <> frame["type"]

      assert %{"type" => "error", "reason" => "wrong_connection_role", "ref" => ^ref} =
               request(requests, Map.put(frame, "ref", ref))
    end

    # Nothing was committed and neither connection changed sync state.
    assert {:ok, [_], 1} = Relay.events(ctx.relay, ctx.keys["a2"], 0)

    for session <- sessions_of(ctx.relay, ctx.keys["a2"]) do
      assert session.sync_mode == nil
      assert session.ready == false
    end

    # The stream still syncs; the request connection still serves.
    assert %{"type" => "ok"} = request(stream, %{"type" => "resync", "since" => 0})
    assert %{"type" => "delivery", "message_id" => "m1"} = reply(stream)
    assert %{"type" => "resynced", "seq" => 1} = reply(stream)
    assert %{"type" => "ok", "rows" => [_]} = request(requests, %{"type" => "inbox_list"})
    Enum.each([stream, requests], &:ssl.close/1)
  end

  # -- (6) legacy clients ---------------------------------------------------

  test "a client that declares no role behaves exactly as before", ctx do
    forward(ctx, "m1", 1)
    socket = connect(ctx.web, ctx.certs, "a2")
    # The upgrade response is byte-for-byte today's: no role, no connection ID.
    assert upgrade(socket, "/relay") ==
             "HTTP/1.1 101 Switching Protocols\r\n" <>
               "Upgrade: websocket\r\n" <>
               "Connection: Upgrade\r\n" <>
               "Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n" <>
               "Sec-WebSocket-Protocol: attention-relay/1\r\n\r\n"

    # No admission frame: the first frame answers the client's first request.
    assert %{"type" => "ok", "rows" => [_]} = request(socket, %{"type" => "inbox_list"})

    # Reads, sync and mutations all stay available on one connection.
    assert %{"type" => "ok"} = request(socket, %{"type" => "resync", "since" => 0})
    assert %{"type" => "delivery", "message_id" => "m1"} = reply(socket)
    assert %{"type" => "resynced", "seq" => 1} = reply(socket)

    assert %{"type" => "ok", "seq" => 2} =
             request(socket, %{
               "type" => "answer",
               "op_id" => op(30),
               "sender_id" => "app:test",
               "message_id" => "m1",
               "answer" => "yes"
             })

    assert %{"type" => "lifecycle", "seq" => 2} = reply(socket)

    # A second legacy connection displaces the first, which sees the transport
    # close with no close frame, as today.
    replacement = connect(ctx.web, ctx.certs, "a2")
    assert upgrade(replacement, "/relay") =~ "101 Switching Protocols"
    assert {:error, :closed} = :ssl.recv(socket, 0, 2_000)
    assert [%{role: :legacy}] = sessions_of(ctx.relay, ctx.keys["a2"])

    # Revocation also closes a legacy connection without a close frame.
    assert :ok = Relay.revoke_device(ctx.relay, ctx.keys["a2"])
    assert {:error, :closed} = :ssl.recv(replacement, 0, 2_000)
  end

  test "an unknown role is refused at the upgrade", ctx do
    for path <- ["/relay?role=observer", "/relay?role=", "/relay?mode=request"] do
      socket = connect(ctx.web, ctx.certs, "a2")
      assert upgrade(socket, path) =~ "400 Bad Request"
      :ssl.close(socket)
    end

    assert sessions_of(ctx.relay, ctx.keys["a2"]) == []
  end

  # -- (7) connection IDs ---------------------------------------------------

  test "every connection gets a connection ID, sent on admission and logged", ctx do
    log =
      capture_log([level: :info], fn ->
        {stream, stream_id} = open(ctx, "a2", "stream")
        {requests, request_id} = open(ctx, "a2", "request")
        assert stream_id != request_id

        sessions = sessions_of(ctx.relay, ctx.keys["a2"])

        assert Enum.sort(Enum.map(sessions, & &1.connection_id)) ==
                 Enum.sort([stream_id, request_id])

        assert %{"reason" => "wrong_connection_role"} =
                 request(requests, %{"type" => "resync", "since" => 0})

        {legacy, nil} = open(ctx, "a2", nil)
        assert_close_reason(stream, 4409, "superseded_by_new_stream")

        [legacy_session] =
          Enum.filter(sessions_of(ctx.relay, ctx.keys["a2"]), &(&1.role == :legacy))

        assert legacy_session.connection_id =~ @connection_id
        send(self(), {:ids, stream_id, request_id, legacy_session.connection_id})
        Enum.each([requests, legacy], &:ssl.close/1)
        Process.sleep(200)
      end)

    assert_received {:ids, stream_id, request_id, legacy_id}

    assert log =~
             "event=harklight_connection phase=open connection_id=#{stream_id} role=stream transport=websocket"

    assert log =~
             "event=harklight_connection phase=open connection_id=#{request_id} role=request transport=websocket"

    assert log =~
             "event=harklight_connection phase=open connection_id=#{legacy_id} role=legacy transport=websocket"

    assert log =~
             "event=harklight_connection phase=refused connection_id=#{request_id} role=request transport=websocket frame_type=resync reason=wrong_connection_role"

    assert log =~
             "event=harklight_connection phase=close connection_id=#{stream_id} role=stream transport=websocket reason=superseded_by_new_stream"

    assert log =~ "phase=close connection_id=#{request_id} role=request"
  end
end
