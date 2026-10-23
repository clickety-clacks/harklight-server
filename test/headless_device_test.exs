defmodule AttentionServer.HeadlessDeviceTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    HeadlessDevice,
    HeadlessEnrollment,
    JSON,
    Relay,
    TLSFixtures,
    WebSocketRelay
  }

  alias AttentionServer.JSON.DecimalToken

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "harklight-device-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")
    {:ok, relay} = Relay.start_link(store_path: path)
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

    {:ok, web} =
      WebSocketRelay.start_link(
        relay: relay,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem")
      )

    endpoint = %{
      address: {127, 0, 0, 1},
      name: "relay.test",
      port: WebSocketRelay.port(web),
      ca_file: Path.join(certs, "ca.pem")
    }

    identities =
      Map.new(~w(a1 a2 b1), fn name ->
        identity = Path.join(dir, name)
        File.mkdir!(identity)
        File.chmod!(identity, 0o700)
        File.cp!(Path.join(certs, "#{name}.pem"), Path.join(identity, "identity.pem"))
        File.cp!(Path.join(certs, "#{name}.key"), Path.join(identity, "identity.key"))
        File.chmod!(Path.join(identity, "identity.key"), 0o600)

        File.write!(
          Path.join(identity, "enrollment.json"),
          JSON.encode!(%{
            "person_id" => if(name == "b1", do: "B", else: "A"),
            "device_id" => keys[name]
          }) <> "\n"
        )

        File.write!(
          Path.join(identity, "server.json"),
          JSON.encode!(%{
            "address" => "127.0.0.1",
            "name" => "relay.test",
            "port" => 8443,
            "ca_sha256" =>
              :crypto.hash(:sha256, File.read!(endpoint.ca_file))
              |> Base.encode16(case: :lower)
          }) <> "\n"
        )

        {name, identity}
      end)

    on_exit(fn ->
      if Process.alive?(web), do: GenServer.stop(web)
      if Process.alive?(relay), do: GenServer.stop(relay)
    end)

    %{
      dir: dir,
      path: path,
      relay: relay,
      web: web,
      endpoint: endpoint,
      identities: identities,
      keys: keys
    }
  end

  test "device command sends exact M, recipient reads and answers, origin gets durable proof",
       ctx do
    %{
      dir: dir,
      endpoint: endpoint,
      identities: identities,
      keys: keys,
      relay: relay,
      web: web,
      path: path,
      certs: certs
    } = ctx

    message = %{
      "sender_id" => "app:test",
      "message_id" => "request-1",
      "created_at" => 1,
      "title" => "Choose one",
      "body" => "Sender-supplied document",
      "answer_ui" => %{"kind" => "choice", "items" => ["yes", "no"]},
      "extra" => %{
        "large" => 9_007_199_254_740_993,
        "precise" => %DecimalToken{raw: "1.234567890123456789"}
      }
    }

    answer = %{
      "choice" => "yes",
      "annotation" => nil,
      "large" => 9_007_199_254_740_993,
      "precise" => %DecimalToken{raw: "0.1234567890123456789"}
    }

    message_file = Path.join(dir, "message.json")
    answer_file = Path.join(dir, "answer.json")
    File.write!(message_file, JSON.encode!(message) <> "\n")
    File.write!(answer_file, JSON.encode!(answer) <> "\n")

    old_shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(old_shell) end)

    assert :ok = task("send", identities["a1"], endpoint, ["--message", message_file])
    assert_receive {:mix_shell, :info, [line]}

    assert {:ok, %{"type" => "ok", "seq" => 1, "op_id" => send_op}} =
             JSON.decode_line(line <> "\n")

    assert :ok = task("read", identities["a2"], endpoint, [])
    assert_receive {:mix_shell, :info, [line]}

    assert {:ok, %{"cursor" => 1, "events" => [%{"message" => ^message} = delivery]}} =
             JSON.decode_line(line <> "\n")

    assert delivery["origin"] == keys["a1"]
    assert delivery["as"] == "knock"

    assert :ok = task("read", identities["b1"], endpoint, [])
    assert_receive {:mix_shell, :info, [line]}
    assert {:ok, %{"cursor" => 0, "events" => []}} = JSON.decode_line(line <> "\n")

    assert :ok =
             task("respond", identities["a2"], endpoint, [
               "--sender",
               "app:test",
               "--message-id",
               "request-1",
               "--answer",
               answer_file
             ])

    assert_receive {:mix_shell, :info, [line]}

    assert {:ok, %{"type" => "ok", "seq" => 2, "op_id" => answer_op}} =
             JSON.decode_line(line <> "\n")

    assert :ok =
             task("receipt", identities["a1"], endpoint, [
               "--sender",
               "app:test",
               "--message-id",
               "request-1"
             ])

    assert_receive {:mix_shell, :info, [line]}
    assert {:ok, %{"receipt" => proof}} = JSON.decode_line(line <> "\n")
    assert proof["answer"] == answer
    assert proof["has_answer"] == true
    assert proof["terminal_kind"] == "answer"
    assert proof["acceptance_seq"] == 1
    assert proof["terminal_seq"] == 2

    assert proof["actor"] == %{
             "kind" => "device",
             "id" => keys["a2"],
             "key_fingerprint" => keys["a2"]
           }

    assert {:ok, %{"cursor" => 2, "events" => [origin, lifecycle]}} =
             HeadlessDevice.read(identities["a1"], endpoint)

    refute Map.has_key?(origin, "message")
    assert lifecycle["answer"] == answer
    refute String.contains?(JSON.encode!(origin), "Sender-supplied document")

    assert {:ok, %{"seq" => 1, "duplicate" => true, "op_id" => ^send_op}} =
             HeadlessDevice.send_message(identities["a1"], endpoint, message_file)

    assert {:ok, %{"seq" => 2, "duplicate" => true, "op_id" => ^answer_op}} =
             HeadlessDevice.respond(
               identities["a2"],
               endpoint,
               "app:test",
               "request-1",
               answer_file
             )

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

    endpoint = %{endpoint | port: WebSocketRelay.port(reopened_web)}

    assert {:ok, %{"receipt" => ^proof}} =
             HeadlessDevice.receipt(identities["a1"], endpoint, "app:test", "request-1")

    assert {:ok, %{"cursor" => 2, "events" => [_, _]}} =
             HeadlessDevice.read(identities["a2"], endpoint)

    assert {:ok, 2} = Relay.acknowledged_cursor(reopened, keys["a2"])
  end

  test "device path denies wrong tenant, missing sender right and substituted identity", ctx do
    %{dir: dir, endpoint: endpoint, identities: identities, keys: keys} = ctx
    message_file = Path.join(dir, "message.json")
    answer_file = Path.join(dir, "null.json")

    File.write!(
      message_file,
      JSON.encode!(%{
        "sender_id" => "app:test",
        "message_id" => "ungranted",
        "created_at" => 1,
        "title" => "No sender right"
      }) <> "\n"
    )

    assert {:error, {:server, %{"reason" => "sender_not_authorized"}}} =
             HeadlessDevice.send_message(identities["a2"], endpoint, message_file)

    assert {:ok, %{"seq" => 1}} =
             HeadlessDevice.send_message(identities["a1"], endpoint, message_file)

    assert {:ok, %{"events" => [%{"message_id" => "ungranted"}]}} =
             HeadlessDevice.read(identities["a2"], endpoint)

    File.write!(answer_file, "null\n")

    assert {:error, :not_in_inbox} =
             HeadlessDevice.respond(
               identities["b1"],
               endpoint,
               "app:test",
               "ungranted",
               answer_file
             )

    assert {:error, :not_origin_message} =
             HeadlessDevice.receipt(identities["b1"], endpoint, "app:test", "ungranted")

    assert {:ok, %{"seq" => 2}} =
             HeadlessDevice.respond(
               identities["a2"],
               endpoint,
               "app:test",
               "ungranted",
               answer_file
             )

    assert {:ok, %{"receipt" => proof}} =
             HeadlessDevice.receipt(identities["a1"], endpoint, "app:test", "ungranted")

    assert proof["has_answer"] == true
    assert Map.has_key?(proof, "answer") and is_nil(proof["answer"])

    wrong = Path.join(dir, "wrong")
    File.cp_r!(identities["a1"], wrong)

    File.write!(
      Path.join(wrong, "enrollment.json"),
      JSON.encode!(%{"person_id" => "A", "device_id" => keys["a2"]}) <> "\n"
    )

    assert {:error, :identity_or_server_mismatch} = HeadlessDevice.read(wrong, endpoint)

    assert {:ok, 1} = Relay.acknowledged_cursor(ctx.relay, keys["a2"])

    File.rm!(Path.join([identities["a2"], "events", "00000000000000000001.json"]))
    assert {:error, :replay_failed} = HeadlessDevice.read(identities["a2"], endpoint)
    assert {:ok, 1} = Relay.acknowledged_cursor(ctx.relay, keys["a2"])
  end

  test "accepted send recovers from its receipt after an unreadable reply and keeps server errors",
       ctx do
    %{dir: dir, relay: relay, certs: certs, identities: identities} = ctx

    {:ok, fixture} =
      AttentionServer.HeadlessDeviceResponseFixture.start_link(
        relay: relay,
        certs: certs,
        owner: self(),
        drop_first_forward_response: true
      )

    on_exit(fn -> AttentionServer.HeadlessDeviceResponseFixture.stop(fixture) end)

    endpoint = %{ctx.endpoint | port: fixture.port}

    message = %{
      "sender_id" => "app:test",
      "message_id" => "receipt-after-invalid-reply",
      "created_at" => 1,
      "title" => "Accepted before the response was lost",
      "body" => "Check the same durable request ID"
    }

    message_file = Path.join(dir, "receipt-after-invalid-reply.json")
    File.write!(message_file, JSON.encode!(message) <> "\n")

    assert {:ok, %{"type" => "ok", "seq" => 1, "op_id" => op_id}} =
             HeadlessDevice.send_message(identities["a1"], endpoint, message_file)

    assert_receive {:forward_request, ^op_id, "receipt-after-invalid-reply"}
    assert_receive {:accepted_response, %{"type" => "ok", "ref" => ref, "seq" => 1}}
    refute_receive {:forward_request, _, _}, 0
    assert is_binary(ref)

    assert {:ok, %{"status" => "pending", "acceptance_seq" => 1}} =
             HeadlessDevice.receipt(
               identities["a1"],
               endpoint,
               message["sender_id"],
               message["message_id"]
             )

    File.write!(
      message_file,
      JSON.encode!(%{message | "message_id" => "rejected-send"}) <> "\n"
    )

    assert {:error, {:server, %{"type" => "error", "reason" => "sender_not_authorized"}}} =
             HeadlessDevice.send_message(identities["a2"], endpoint, message_file)

    assert is_binary(op_id)
  end

  test "recipient can read and answer a near-limit message accepted from a sender", ctx do
    %{dir: dir, endpoint: endpoint, identities: identities} = ctx

    message = %{
      "sender_id" => "app:test",
      "message_id" => "large-request",
      "created_at" => 1,
      "title" => "T",
      "body" => String.duplicate("x", 2_096_948)
    }

    message_file = Path.join(dir, "large-message.json")
    answer_file = Path.join(dir, "large-answer.json")
    File.write!(message_file, JSON.encode!(message) <> "\n")
    File.write!(answer_file, "null\n")

    assert {:ok, %{"type" => "ok", "seq" => 1}} =
             HeadlessDevice.send_message(identities["a1"], endpoint, message_file)

    assert :ok = HeadlessEnrollment.verify_relay(identities["a2"], endpoint)

    assert {:ok, %{"cursor" => 1, "events" => [%{"message" => ^message} = delivery]}} =
             HeadlessDevice.read(identities["a2"], endpoint)

    assert byte_size(JSON.encode!(delivery)) > 2 * 1024 * 1024

    assert {:ok, %{"type" => "ok", "seq" => 1, "duplicate" => true}} =
             HeadlessDevice.send_message(identities["a1"], endpoint, message_file)

    assert {:ok, %{"type" => "ok", "seq" => 2}} =
             HeadlessDevice.respond(
               identities["a2"],
               endpoint,
               "app:test",
               "large-request",
               answer_file
             )

    assert {:ok, %{"receipt" => %{"has_answer" => true, "answer" => nil}}} =
             HeadlessDevice.receipt(identities["a1"], endpoint, "app:test", "large-request")
  end

  defp task(command, directory, endpoint, additional) do
    Mix.Tasks.Attention.Device.run([
      command,
      "--directory",
      directory,
      "--connect",
      "127.0.0.1",
      "--name",
      endpoint.name,
      "--port",
      Integer.to_string(endpoint.port),
      "--ca",
      endpoint.ca_file
      | additional
    ])
  end
end

defmodule AttentionServer.HeadlessDeviceResponseFixture do
  alias AttentionServer.{DeviceIdentity, EnrolledLeafVerify, JSON, Relay}

  @guid "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
  @timeout 5_000

  def start_link(opts) do
    relay = Keyword.fetch!(opts, :relay)
    certs = Keyword.fetch!(opts, :certs)
    owner = Keyword.fetch!(opts, :owner)

    with {:ok, listener} <-
           :ssl.listen(0,
             ip: {127, 0, 0, 1},
             active: false,
             mode: :binary,
             packet: :raw,
             send_timeout: 1_000,
             send_timeout_close: true,
             reuseaddr: true,
             versions: [:"tlsv1.3"],
             verify: :verify_peer,
             verify_fun: EnrolledLeafVerify.device(relay),
             fail_if_no_peer_cert: true,
             cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
             certfile: String.to_charlist(Path.join(certs, "server.pem")),
             keyfile: String.to_charlist(Path.join(certs, "server.key")),
             session_tickets: :disabled,
             early_data: :disabled
           ),
         {:ok, {_ip, port}} <- :ssl.sockname(listener) do
      listener_id =
        spawn_link(fn ->
          receive do
            :start ->
              accept_loop(
                listener,
                relay,
                owner,
                self(),
                Keyword.get(opts, :drop_first_forward_response, false)
              )
          end
        end)

      with :ok <- Relay.register_tls_listener(relay, listener_id) do
        send(listener_id, :start)
        {:ok, %{listener: listener, listener_id: listener_id, port: port}}
      else
        error ->
          :ssl.close(listener)
          Process.exit(listener_id, :shutdown)
          error
      end
    end
  end

  def stop(%{listener: listener, listener_id: listener_id}) do
    _ = :ssl.close(listener)
    Process.exit(listener_id, :shutdown)
    :ok
  end

  defp accept_loop(listener, relay, owner, listener_id, drop_first_forward_response) do
    case :ssl.transport_accept(listener) do
      {:ok, transport} ->
        next_drop =
          case serve_transport(
                 transport,
                 relay,
                 owner,
                 listener_id,
                 drop_first_forward_response
               ) do
            {:ok, next_drop} ->
              next_drop

            {:error, reason} ->
              send(owner, {:response_fixture_error, reason})
              drop_first_forward_response
          end

        accept_loop(listener, relay, owner, listener_id, next_drop)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        send(owner, {:response_fixture_error, reason})
        :ok
    end
  end

  defp serve_transport(transport, relay, owner, listener_id, drop_first_forward_response) do
    with {:ok, socket} <- :ssl.handshake(transport, @timeout),
         {:ok, certificate} <- :ssl.peercert(socket),
         {:ok, key} <- DeviceIdentity.from_certificate(certificate),
         :ok <- upgrade(socket),
         {:ok, session} <- Relay.open_websocket_session(relay, key, socket, listener_id) do
      result =
        request_loop(socket, relay, session, owner, drop_first_forward_response)

      _ = Relay.close_session(relay, session)
      _ = :ssl.close(socket)
      result
    end
  end

  defp upgrade(socket) do
    with {:ok, headers} <- read_headers(socket, <<>>),
         key when is_binary(key) <- header(headers, "sec-websocket-key"),
         accept <- Base.encode64(:crypto.hash(:sha, key <> @guid)),
         :ok <-
           :ssl.send(socket, [
             "HTTP/1.1 101 Switching Protocols\r\n",
             "Upgrade: websocket\r\n",
             "Connection: Upgrade\r\n",
             "Sec-WebSocket-Accept: ",
             accept,
             "\r\nSec-WebSocket-Protocol: attention-relay/1\r\n\r\n"
           ]) do
      :ok
    else
      _ -> {:error, :bad_upgrade}
    end
  end

  defp header(headers, wanted) do
    headers
    |> String.split("\r\n")
    |> Enum.find_value(fn line ->
      case String.split(line, ":", parts: 2) do
        [name, value] ->
          if String.downcase(name) == wanted, do: String.trim(value)

        _ ->
          nil
      end
    end)
  end

  defp read_headers(socket, bytes) when byte_size(bytes) <= 16_384 do
    if String.ends_with?(bytes, "\r\n\r\n") do
      {:ok, bytes}
    else
      case :ssl.recv(socket, 1, @timeout) do
        {:ok, byte} -> read_headers(socket, bytes <> byte)
        error -> error
      end
    end
  end

  defp read_headers(_socket, _bytes), do: {:error, :headers_too_large}

  defp request_loop(socket, relay, session, owner, drop_first_forward_response) do
    case read_client_text(socket) do
      {:ok, payload} ->
        case JSON.decode_line(payload) do
          {:ok, %{"type" => "forward"} = request} when drop_first_forward_response ->
            send(owner, {:forward_request, request["op_id"], request["message"]["message_id"]})

            case Relay.session_request_line(relay, session, payload) do
              {:ok, frames} ->
                capture_accepted_response(owner, frames)

                unexpected = JSON.encode!(%{"v" => 1, "type" => "unmatched"}) <> "\n"
                :ok = Relay.session_send(relay, session, [unexpected])
                {:ok, false}

              error ->
                error
            end

          {:ok, _request} ->
            with {:ok, frames} <- Relay.session_request_line(relay, session, payload),
                 :ok <- Relay.session_send(relay, session, frames),
                 :ok <- drain_and_send(relay, session) do
              request_loop(socket, relay, session, owner, drop_first_forward_response)
            end

          error ->
            error
        end

      error ->
        if error == {:error, :closed}, do: {:ok, drop_first_forward_response}, else: error
    end
  end

  defp capture_accepted_response(owner, frames) do
    Enum.find_value(frames, fn frame ->
      case JSON.decode_line(frame) do
        {:ok, %{"type" => "ok", "seq" => seq} = response} when is_integer(seq) ->
          send(owner, {:accepted_response, response})
          response

        _ ->
          nil
      end
    end)
  end

  defp drain_and_send(relay, session) do
    case Relay.session_drain(relay, session) do
      {:ok, pushed} -> Relay.session_send(relay, session, pushed)
      {:error, :resync_required} -> :ok
      error -> error
    end
  end

  defp read_client_text(socket) do
    with {:ok, <<fin::1, rsv::3, opcode::4, masked::1, length::7>>} <-
           :ssl.recv(socket, 2, @timeout),
         true <- fin == 1 and rsv == 0 and opcode == 1 and masked == 1,
         {:ok, size} <- frame_size(socket, length),
         {:ok, mask} <- :ssl.recv(socket, 4, @timeout),
         {:ok, payload} <- :ssl.recv(socket, size, @timeout) do
      repeated = :binary.copy(mask, div(size + 3, 4))
      {:ok, :crypto.exor(payload, binary_part(repeated, 0, size))}
    else
      error -> error
    end
  end

  defp frame_size(_socket, size) when size < 126, do: {:ok, size}

  defp frame_size(socket, 126) do
    with {:ok, <<size::16>>} <- :ssl.recv(socket, 2, @timeout), do: {:ok, size}
  end

  defp frame_size(socket, 127) do
    with {:ok, <<size::64>>} <- :ssl.recv(socket, 8, @timeout), do: {:ok, size}
  end
end
