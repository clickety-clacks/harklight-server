defmodule AttentionServer.WebSocketRelay do
  @moduledoc """
  TLS 1.3 device-certificate WebSocket entry for the `/relay` v1 wire.

  The authenticated device session is opened only after the exact WebSocket
  subprotocol is negotiated. Relay owns application-frame sends and fencing.
  """

  use GenServer

  alias AttentionServer.{DeviceIdentity, Diagnostics, EnrolledLeafVerify, JSON, Relay}

  @handshake_timeout 3_000
  @poll_ms 100
  @max_header 16 * 1024
  @max_message 2 * 1024 * 1024
  @max_recoverable_oversize 4 * 1024 * 1024
  @protocol "attention-relay/1"
  @guid "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
  @bad_frame ~s({"v":1,"type":"error","reason":"bad_frame"}\n)

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
  def port(server), do: GenServer.call(server, :port)

  @doc "Encode one server-to-client text message without masking."
  def encode_text(payload) when is_binary(payload), do: encode_frame(1, payload)

  @impl true
  def init(opts) do
    with {:ok, relay} <-
           Diagnostics.result(
             :device_relay,
             :configuration,
             Keyword.fetch(opts, :relay),
             :unavailable
           ),
         {:ok, certfile} <-
           Diagnostics.result(
             :device_relay,
             :configuration,
             Keyword.fetch(opts, :certfile),
             :unavailable
           ),
         {:ok, keyfile} <-
           Diagnostics.result(
             :device_relay,
             :configuration,
             Keyword.fetch(opts, :keyfile),
             :unavailable
           ),
         {:ok, cacertfile} <-
           Diagnostics.result(
             :device_relay,
             :configuration,
             Keyword.fetch(opts, :cacertfile),
             :unavailable
           ),
         {:ok, listener} <-
           Diagnostics.result(
             :device_relay,
             :listener_start,
             listen(opts, relay, certfile, keyfile, cacertfile),
             :unavailable
           ),
         {:ok, {_ip, port}} <-
           Diagnostics.result(
             :device_relay,
             :listener_address,
             :ssl.sockname(listener),
             :unavailable
           ) do
      case Diagnostics.result(
             :device_relay,
             :worker_start,
             start_workers(listener, relay, self()),
             :unavailable
           ) do
        {:ok, supervisor} ->
          {:ok, %{listener: listener, supervisor: supervisor, relay: relay, port: port}}

        error ->
          :ssl.close(listener)
          {:stop, error}
      end
    else
      error -> {:stop, error}
    end
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}

  @impl true
  def terminate(_reason, state) do
    try do
      Relay.unregister_tls_listener(state.relay, self())
    catch
      :exit, reason ->
        Diagnostics.failure(:device_relay, :server_terminate, reason, :unavailable)
        :ok
    end

    :ssl.close(state.listener)
    Supervisor.stop(state.supervisor, :normal)
  end

  defp listen(opts, relay, certfile, keyfile, cacertfile) do
    :ssl.listen(Keyword.get(opts, :port, 0),
      ip: Keyword.get(opts, :ip, {127, 0, 0, 1}),
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
      cacertfile: String.to_charlist(cacertfile),
      certfile: String.to_charlist(certfile),
      keyfile: String.to_charlist(keyfile),
      session_tickets: :disabled,
      early_data: :disabled
    )
  end

  defp start_workers(listener, relay, listener_id) do
    case Relay.register_tls_listener(relay, listener_id) do
      :ok ->
        case Task.Supervisor.start_link() do
          {:ok, supervisor} ->
            case Task.Supervisor.start_child(supervisor, fn ->
                   accept_loop(listener, relay, listener_id, supervisor)
                 end) do
              {:ok, _acceptor} ->
                {:ok, supervisor}

              error ->
                Supervisor.stop(supervisor, :normal)
                Relay.unregister_tls_listener(relay, listener_id)
                error
            end

          error ->
            Relay.unregister_tls_listener(relay, listener_id)
            error
        end

      error ->
        error
    end
  end

  defp accept_loop(listener, relay, listener_id, supervisor) do
    case :ssl.transport_accept(listener) do
      {:ok, socket} ->
        case Task.Supervisor.start_child(supervisor, fn -> await_socket(relay, listener_id) end) do
          {:ok, worker} ->
            case :ssl.controlling_process(socket, worker) do
              :ok ->
                send(worker, {:socket, socket})

              {:error, reason} ->
                Diagnostics.failure(:device_relay, :socket_control, reason, :closed)
                send(worker, :abort)
                :ssl.close(socket)
            end

          {:error, reason} ->
            Diagnostics.failure(:device_relay, :worker_start, reason, :closed)
            :ssl.close(socket)
        end

        accept_loop(listener, relay, listener_id, supervisor)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        Diagnostics.failure(:device_relay, :accept, reason, :retry_100ms)
        Process.sleep(100)
        accept_loop(listener, relay, listener_id, supervisor)
    end
  end

  defp await_socket(relay, listener_id) do
    receive do
      {:socket, socket} -> serve(socket, relay, listener_id)
      :abort -> :ok
    end
  end

  defp serve(socket, relay, listener_id) do
    try do
      with {:ok, tls_socket} <-
             Diagnostics.result(
               :device_relay,
               :tls_handshake,
               :ssl.handshake(socket, @handshake_timeout),
               :closed
             ),
           {:ok, certificate} <-
             Diagnostics.result(
               :device_relay,
               :peer_certificate,
               :ssl.peercert(tls_socket),
               :closed
             ),
           {:ok, key} <-
             Diagnostics.result(
               :device_relay,
               :identity,
               DeviceIdentity.from_certificate(certificate),
               :closed
             ),
           {:ok, _person_cursor} <-
             Diagnostics.result(
               :device_relay,
               :authentication,
               Relay.acknowledged_cursor(relay, key),
               :closed
             ),
           {:ok, buffer} <- upgrade(tls_socket),
           {:ok, session} <-
             Diagnostics.result(
               :device_relay,
               :session_setup,
               Relay.open_websocket_session(relay, key, tls_socket, listener_id),
               :closed
             ) do
        loop(tls_socket, relay, session, buffer, nil)
        Relay.close_session(relay, session)
      end
    rescue
      error ->
        Diagnostics.failure(:device_relay, :relay_boundary, error, :outcome_unknown)
        :ok
    catch
      :exit, reason ->
        Diagnostics.failure(:device_relay, :relay_boundary, reason, :outcome_unknown)
        :ok
    after
      :ssl.close(socket)
    end
  end

  defp upgrade(socket) do
    with {:ok, headers, buffer} <- read_headers(socket, <<>>),
         {:ok, key} <- validate_upgrade(headers),
         accept <- Base.encode64(:crypto.hash(:sha, key <> @guid)),
         :ok <-
           :ssl.send(socket, [
             "HTTP/1.1 101 Switching Protocols\r\n",
             "Upgrade: websocket\r\n",
             "Connection: Upgrade\r\n",
             "Sec-WebSocket-Accept: ",
             accept,
             "\r\nSec-WebSocket-Protocol: ",
             @protocol,
             "\r\n\r\n"
           ]) do
      {:ok, buffer}
    else
      {:error, reason} ->
        Diagnostics.failure(:device_relay, :websocket_upgrade, reason, :closed)

        _ =
          :ssl.send(
            socket,
            "HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
          )

        {:error, :bad_upgrade}

      other ->
        Diagnostics.failure(:device_relay, :websocket_upgrade, other, :closed)

        _ =
          :ssl.send(
            socket,
            "HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
          )

        {:error, :bad_upgrade}
    end
  end

  defp read_headers(socket, buffer) when byte_size(buffer) <= @max_header do
    case :binary.match(buffer, "\r\n\r\n") do
      {at, 4} ->
        header = binary_part(buffer, 0, at)
        rest = binary_part(buffer, at + 4, byte_size(buffer) - at - 4)
        {:ok, header, rest}

      :nomatch ->
        case :ssl.recv(socket, 0, @handshake_timeout) do
          {:ok, chunk} -> read_headers(socket, buffer <> chunk)
          error -> error
        end
    end
  end

  defp read_headers(_, _), do: {:error, :header_too_large}

  defp validate_upgrade(headers) do
    case String.split(headers, "\r\n") do
      ["GET /relay HTTP/1.1" | lines] ->
        with {:ok, fields} <- parse_headers(lines, %{}),
             true <- token_ci?(fields["connection"], "upgrade"),
             true <- String.downcase(fields["upgrade"] || "") == "websocket",
             true <- fields["sec-websocket-version"] == "13",
             true <- token?(fields["sec-websocket-protocol"], @protocol),
             {:ok, key} <- valid_key(fields["sec-websocket-key"]),
             true <- is_binary(fields["host"]) and fields["host"] != "" do
          {:ok, key}
        else
          _ -> {:error, :bad_upgrade}
        end

      _ ->
        {:error, :bad_upgrade}
    end
  end

  defp parse_headers([], fields), do: {:ok, fields}

  defp parse_headers([line | rest], fields) do
    case String.split(line, ":", parts: 2) do
      [name, value] ->
        key = name |> String.trim() |> String.downcase()

        if key != "" and not Map.has_key?(fields, key),
          do: parse_headers(rest, Map.put(fields, key, String.trim(value))),
          else: {:error, :bad_header}

      _ ->
        {:error, :bad_header}
    end
  end

  defp token?(value, wanted) when is_binary(value),
    do: value |> String.split(",") |> Enum.map(&String.trim/1) |> Enum.member?(wanted)

  defp token?(_, _), do: false

  defp token_ci?(value, wanted) when is_binary(value),
    do: value |> String.downcase() |> token?(wanted)

  defp token_ci?(_, _), do: false

  defp valid_key(value) when is_binary(value) do
    case Base.decode64(value) do
      {:ok, decoded} when byte_size(decoded) == 16 -> {:ok, value}
      _ -> {:error, :bad_key}
    end
  end

  defp valid_key(_), do: {:error, :bad_key}

  defp loop(socket, relay, session, buffer, fragment) do
    case decode_frame(buffer) do
      {:ok, {fin, opcode, payload}, rest} ->
        case handle_frame(socket, relay, session, fin, opcode, payload, fragment) do
          {:ok, next_fragment} -> loop(socket, relay, session, rest, next_fragment)
          _ -> :ok
        end

      :more ->
        # Bound only an incomplete frame. A single TLS read can contain the
        # end of this frame and following valid frames; parse the first frame
        # before deciding whether the remaining buffer is oversized.
        if byte_size(buffer) > @max_message + 14 do
          _ = protocol_error(relay, session)
          :ok
        else
          case :ssl.recv(socket, 0, @poll_ms) do
            {:ok, chunk} ->
              loop(socket, relay, session, buffer <> chunk, fragment)

            {:error, :timeout} ->
              case Relay.session_drain(relay, session) do
                {:ok, frames} ->
                  case Relay.session_send(relay, session, frames) do
                    :ok ->
                      loop(socket, relay, session, buffer, fragment)

                    {:error, reason} ->
                      Diagnostics.failure(:device_relay, :response_write, reason, :closed)

                    other ->
                      Diagnostics.failure(:device_relay, :response_write, other, :closed)
                  end

                {:error, :resync_required} ->
                  loop(socket, relay, session, buffer, fragment)

                {:error, reason} ->
                  Diagnostics.failure(:device_relay, :response_drain, reason, :closed)

                other ->
                  Diagnostics.failure(:device_relay, :response_drain, other, :closed)
              end

            {:error, reason} ->
              Diagnostics.failure(:device_relay, :request_read, reason, :closed)

            other ->
              Diagnostics.failure(:device_relay, :request_read, other, :closed)
          end
        end

      {:oversize, bytes, data} ->
        with :ok <- protocol_error(relay, session),
             {:ok, rest} <- discard_payload(socket, bytes, data) do
          loop(socket, relay, session, rest, nil)
        else
          {:error, reason} ->
            Diagnostics.failure(:device_relay, :request_read, reason, :closed)
            :ok

          other ->
            Diagnostics.failure(:device_relay, :request_read, other, :closed)
            :ok
        end

      {:error, reason} ->
        Diagnostics.failure(:device_relay, :request_read, reason, :closed)
        _ = Relay.session_send(relay, session, [@bad_frame])
        :ok
    end
  end

  defp handle_frame(socket, _relay, _session, true, 8, payload, _fragment) do
    _ =
      Diagnostics.result(
        :device_relay,
        :response_write,
        :ssl.send(socket, encode_frame(8, payload)),
        :closed
      )

    :closed
  end

  defp handle_frame(socket, _relay, _session, true, 9, payload, fragment) do
    case Diagnostics.result(
           :device_relay,
           :response_write,
           :ssl.send(socket, encode_frame(10, payload)),
           :closed
         ) do
      :ok -> {:ok, fragment}
      _ -> :closed
    end
  end

  defp handle_frame(_socket, _relay, _session, true, 10, _payload, fragment),
    do: {:ok, fragment}

  defp handle_frame(_socket, relay, session, fin, 1, payload, nil) do
    if fin,
      do: process_text(relay, session, payload),
      else: {:ok, payload}
  end

  defp handle_frame(_socket, relay, session, fin, 0, payload, fragment)
       when is_binary(fragment) do
    message = fragment <> payload

    cond do
      byte_size(message) > @max_message ->
        _ = protocol_error(relay, session)
        :closed

      fin ->
        process_text(relay, session, message)

      true ->
        {:ok, message}
    end
  end

  defp handle_frame(_socket, relay, session, _fin, _opcode, _payload, _fragment) do
    _ = protocol_error(relay, session)
    {:ok, nil}
  end

  defp process_text(relay, session, payload) do
    operation_id = request_operation_id(payload)

    frames =
      if String.valid?(payload) and byte_size(payload) <= @max_message do
        case Relay.session_request_line(relay, session, payload) do
          {:ok, reply} ->
            report_wire_errors(reply, operation_id)
            reply

          {:error, reason} ->
            Diagnostics.failure(
              :device_relay,
              :request_dispatch,
              reason,
              :outcome_unknown,
              operation_id
            )

            :fenced

          other ->
            Diagnostics.failure(
              :device_relay,
              :request_dispatch,
              other,
              :outcome_unknown,
              operation_id
            )

            :fenced
        end
      else
        Diagnostics.failure(
          :device_relay,
          :request_read,
          if(String.valid?(payload), do: :too_large, else: :invalid_json),
          :rejected,
          operation_id
        )

        [@bad_frame]
      end

    case frames do
      :fenced ->
        :closed

      frames when is_list(frames) ->
        case Relay.session_send(relay, session, frames) do
          :ok ->
            case Relay.session_drain(relay, session) do
              {:ok, pushed} ->
                case Relay.session_send(relay, session, pushed) do
                  :ok ->
                    {:ok, nil}

                  {:error, reason} ->
                    Diagnostics.failure(
                      :device_relay,
                      :response_write,
                      reason,
                      :outcome_unknown,
                      operation_id
                    )

                    :closed
                end

              {:error, :resync_required} ->
                {:ok, nil}

              {:error, reason} ->
                Diagnostics.failure(
                  :device_relay,
                  :response_drain,
                  reason,
                  :outcome_unknown,
                  operation_id
                )

                :closed

              other ->
                Diagnostics.failure(
                  :device_relay,
                  :response_drain,
                  other,
                  :outcome_unknown,
                  operation_id
                )

                :closed
            end

          {:error, reason} ->
            Diagnostics.failure(
              :device_relay,
              :response_write,
              reason,
              :outcome_unknown,
              operation_id
            )

            :closed
        end
    end
  end

  defp request_operation_id(payload) do
    case JSON.decode_line(payload) do
      {:ok, request} -> Diagnostics.operation_id(request)
      _ -> nil
    end
  end

  defp protocol_error(relay, session) do
    Diagnostics.result(
      :device_relay,
      :response_write,
      Relay.session_send(relay, session, [@bad_frame]),
      :closed
    )
  end

  defp report_wire_errors(frames, operation_id) when is_list(frames) do
    Enum.each(frames, fn frame ->
      case JSON.decode_line(frame) do
        {:ok, %{"type" => "error", "reason" => reason}} when is_binary(reason) ->
          outcome = if reason == "storage_unavailable", do: :outcome_unknown, else: :rejected
          Diagnostics.failure(:device_relay, :request_dispatch, reason, outcome, operation_id)

        _ ->
          :ok
      end
    end)
  end

  defp report_wire_errors(_frames, _operation_id), do: :ok

  defp decode_frame(buffer) when byte_size(buffer) < 2, do: :more

  defp decode_frame(<<fin::1, rsv::3, opcode::4, masked::1, length::7, rest::binary>>) do
    cond do
      rsv != 0 or masked != 1 -> {:error, :protocol}
      opcode >= 8 and (fin != 1 or length > 125) -> {:error, :protocol}
      true -> decode_length(fin == 1, opcode, length, rest)
    end
  end

  defp decode_length(fin, opcode, 126, rest) when byte_size(rest) >= 2 do
    <<length::16, data::binary>> = rest
    decode_payload(fin, opcode, length, data)
  end

  defp decode_length(fin, opcode, 127, rest) when byte_size(rest) >= 8 do
    <<length::64, data::binary>> = rest
    decode_payload(fin, opcode, length, data)
  end

  defp decode_length(_fin, _opcode, length, _rest) when length in [126, 127], do: :more
  defp decode_length(fin, opcode, length, rest), do: decode_payload(fin, opcode, length, rest)

  defp decode_payload(_fin, _opcode, length, rest)
       when length > @max_message and length <= @max_recoverable_oversize,
       do: {:oversize, length + 4, rest}

  defp decode_payload(_fin, _opcode, length, _rest) when length > @max_recoverable_oversize,
    do: {:error, :too_large}

  defp decode_payload(_fin, _opcode, length, rest) when byte_size(rest) < length + 4,
    do: :more

  defp decode_payload(fin, opcode, length, data) do
    <<mask::binary-size(4), payload::binary-size(length), rest::binary>> = data
    repeated = :binary.copy(mask, div(length + 3, 4))
    key = binary_part(repeated, 0, length)
    {:ok, {fin, opcode, :crypto.exor(payload, key)}, rest}
  end

  defp discard_payload(_socket, bytes, data) when byte_size(data) >= bytes do
    {:ok, binary_part(data, bytes, byte_size(data) - bytes)}
  end

  defp discard_payload(socket, bytes, data),
    do: discard_remaining(socket, bytes - byte_size(data))

  defp discard_remaining(_socket, 0), do: {:ok, <<>>}

  defp discard_remaining(socket, bytes) do
    step = min(bytes, 8192)

    case :ssl.recv(socket, step, @handshake_timeout) do
      {:ok, _discarded} -> discard_remaining(socket, bytes - step)
      error -> error
    end
  end

  defp encode_frame(opcode, payload) do
    length = byte_size(payload)

    header =
      cond do
        length < 126 -> <<1::1, 0::3, opcode::4, 0::1, length::7>>
        length < 65_536 -> <<1::1, 0::3, opcode::4, 0::1, 126::7, length::16>>
        true -> <<1::1, 0::3, opcode::4, 0::1, 127::7, length::64>>
      end

    [header, payload]
  end
end
