defmodule AttentionServer.HeadlessDevice do
  @moduledoc """
  Enrolled-device command path for the existing TLS `/relay` WebSocket wire.

  Replayed events are stored locally before their cursor is acknowledged. A
  request's operation ID and exact JSON are stored before it is sent, so an
  uncertain network result can be retried with the same semantics.
  """

  import Bitwise

  alias AttentionServer.{DeviceIdentity, HeadlessTLS, JSON}

  @max_frame 2 * 1024 * 1024
  # A server projection can repeat sender/message IDs outside the accepted M,
  # or pair a terminal answer with those IDs. Both pieces originate in bounded
  # client requests, so allow two request frames plus projection metadata.
  @max_server_frame 2 * @max_frame + 16_384
  @timeout 5_000

  def read(directory, endpoint) do
    with_session(directory, endpoint, fn socket ->
      with {:ok, high} <- sync(directory, socket),
           {:ok, events} <- stored_events(directory) do
        {:ok, %{"cursor" => high, "events" => events}}
      end
    end)
  end

  def respond(directory, endpoint, sender, message_id, answer_file) do
    with :ok <- existing_private_directory(directory),
         {:ok, answer} <- read_document(answer_file),
         {:ok, frame} <-
           saved_operation(directory, %{
             "type" => "answer",
             "sender_id" => sender,
             "message_id" => message_id,
             "answer" => answer
           }),
         {:ok, result} <-
           with_session(directory, endpoint, fn socket ->
             with {:ok, _} <- sync(directory, socket),
                  {:ok, events} <- stored_events(directory),
                  true <- received_message?(events, sender, message_id, device_id(directory)) do
               request(socket, frame)
             else
               false -> {:error, :not_in_inbox}
               other -> other
             end
           end) do
      {:ok, Map.put(result, "op_id", frame["op_id"])}
    end
  end

  defp with_session(directory, endpoint, fun) do
    with {:ok, _} <- enrolled_identity(directory, endpoint),
         {:ok, socket} <-
           HeadlessTLS.connect(endpoint, {
             Path.join(directory, "identity.pem"),
             Path.join(directory, "identity.key")
           }) do
      try do
        with :ok <- upgrade(socket, endpoint.name), do: fun.(socket)
      after
        :ssl.close(socket)
      end
    end
  end

  defp enrolled_identity(directory, endpoint) do
    with :ok <- existing_private_directory(directory),
         {:ok, saved} <- read_json(Path.join(directory, "server.json")),
         {:ok, ca} <- File.read(endpoint.ca_file),
         true <-
           saved["name"] == endpoint.name and
             saved["address"] == address_string(endpoint.address) and
             saved["ca_sha256"] == hex(:crypto.hash(:sha256, ca)),
         {:ok, result} <- read_json(Path.join(directory, "enrollment.json")),
         expected when is_binary(expected) <- result["device_id"],
         {:ok, pem} <- File.read(Path.join(directory, "identity.pem")),
         [{:Certificate, der, :not_encrypted}] <- :public_key.pem_decode(pem),
         {:ok, ^expected} <- DeviceIdentity.from_certificate(der),
         {:ok, key_stat} <- File.stat(Path.join(directory, "identity.key")),
         true <- (key_stat.mode &&& 0o077) == 0 do
      {:ok, expected}
    else
      _ -> {:error, :identity_or_server_mismatch}
    end
  rescue
    _ -> {:error, :identity_or_server_mismatch}
  end

  defp device_id(directory) do
    case read_json(Path.join(directory, "enrollment.json")) do
      {:ok, %{"device_id" => id}} -> id
      _ -> nil
    end
  end

  defp address_string(address) when is_tuple(address),
    do: address |> :inet.ntoa() |> List.to_string()

  defp address_string(address) when is_list(address), do: List.to_string(address)

  defp upgrade(socket, name) do
    key = Base.encode64(:crypto.strong_rand_bytes(16))
    accept = Base.encode64(:crypto.hash(:sha, key <> "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))

    with :ok <-
           :ssl.send(socket, [
             "GET /relay HTTP/1.1\r\nHost: ",
             name,
             "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n",
             "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: ",
             key,
             "\r\nSec-WebSocket-Protocol: attention-relay/1\r\n\r\n"
           ]),
         {:ok, headers} <- read_headers(socket, <<>>),
         true <-
           String.starts_with?(headers, "HTTP/1.1 101 ") and
             String.contains?(headers, "Sec-WebSocket-Accept: #{accept}\r\n") and
             String.contains?(
               String.downcase(headers),
               "sec-websocket-protocol: attention-relay/1\r\n"
             ) do
      :ok
    else
      _ -> {:error, :bad_upgrade}
    end
  end

  defp read_headers(_socket, bytes) when byte_size(bytes) > 16_384,
    do: {:error, :bad_upgrade}

  defp read_headers(socket, bytes) do
    if String.ends_with?(bytes, "\r\n\r\n") do
      {:ok, bytes}
    else
      case :ssl.recv(socket, 1, @timeout) do
        {:ok, byte} -> read_headers(socket, bytes <> byte)
        other -> other
      end
    end
  end

  defp sync(directory, socket) do
    with {:ok, cursor} <- local_cursor(directory),
         :ok <- send_frame(socket, %{"type" => "resync", "since" => cursor, "ref" => "sync"}),
         {:ok, %{"type" => "ok", "ref" => "sync"}} <- read_frame(socket),
         {:ok, high} <- replay(directory, socket, cursor),
         :ok <- acknowledge(directory, socket, cursor, high) do
      {:ok, high}
    else
      _ -> {:error, :replay_failed}
    end
  end

  defp replay(directory, socket, previous) do
    case read_frame(socket) do
      {:ok, %{"type" => "delivery", "seq" => seq} = event}
      when is_integer(seq) and seq == previous + 1 ->
        with :ok <- store_event(directory, event), do: replay(directory, socket, seq)

      {:ok, %{"type" => "lifecycle", "seq" => seq} = event}
      when is_integer(seq) and seq == previous + 1 ->
        with :ok <- store_event(directory, event), do: replay(directory, socket, seq)

      {:ok, %{"type" => "resynced", "seq" => ^previous}} ->
        {:ok, previous}

      _ ->
        {:error, :invalid_replay}
    end
  end

  defp acknowledge(_directory, _socket, cursor, cursor), do: :ok

  defp acknowledge(directory, socket, _cursor, high) do
    with {:ok, %{"type" => "ok", "seq" => ^high}} <-
           request(socket, %{"type" => "ack", "seq" => high}),
         :ok <- write_private(Path.join(directory, "cursor.json"), %{"seq" => high}) do
      :ok
    end
  end

  defp local_cursor(directory) do
    cursor =
      case read_json(Path.join(directory, "cursor.json")) do
        {:ok, %{"seq" => seq}} when is_integer(seq) and seq >= 0 -> {:ok, seq}
        {:error, :enoent} -> {:ok, 0}
        _ -> {:error, :invalid_local_cursor}
      end

    with {:ok, seq} <- cursor,
         {:ok, events} <- stored_events(directory),
         true <-
           length(events) >= seq and
             Enum.all?(Enum.with_index(events, 1), fn {event, expected} ->
               event["seq"] == expected
             end) do
      {:ok, seq}
    else
      _ -> {:error, :invalid_local_cursor}
    end
  end

  defp store_event(directory, %{"seq" => seq} = event) do
    path =
      Path.join([
        directory,
        "events",
        String.pad_leading(Integer.to_string(seq), 20, "0") <> ".json"
      ])

    with :ok <- private_directory(Path.dirname(path)) do
      case read_json(path) do
        {:ok, ^event} ->
          :ok

        {:error, :enoent} ->
          case write_private(path, event, :new) do
            {:error, :eexist} ->
              if read_json(path) == {:ok, event}, do: :ok, else: {:error, :event_conflict}

            other ->
              other
          end

        _ ->
          {:error, :event_conflict}
      end
    end
  end

  defp stored_events(directory) do
    path = Path.join(directory, "events")

    case File.ls(path) do
      {:error, :enoent} ->
        {:ok, []}

      {:ok, names} ->
        names
        |> Enum.filter(&Regex.match?(~r/\A[0-9]{20}\.json\z/, &1))
        |> Enum.sort()
        |> Enum.reduce_while({:ok, []}, fn name, {:ok, events} ->
          case read_json(Path.join(path, name)) do
            {:ok, %{"seq" => seq} = event} when is_integer(seq) ->
              if String.pad_leading(Integer.to_string(seq), 20, "0") <> ".json" == name do
                {:cont, {:ok, [event | events]}}
              else
                {:halt, {:error, :invalid_local_event}}
              end

            _ ->
              {:halt, {:error, :invalid_local_event}}
          end
        end)
        |> case do
          {:ok, events} -> {:ok, Enum.reverse(events)}
          other -> other
        end

      error ->
        error
    end
  end

  defp received_message?(events, sender, id, device) do
    Enum.any?(events, fn event ->
      event["type"] == "delivery" and event["sender_id"] == sender and
        event["message_id"] == id and event["origin"] != device and
        Map.has_key?(event, "message")
    end)
  end

  defp request(socket, frame) do
    ref = hex(:crypto.strong_rand_bytes(8))

    with :ok <- send_frame(socket, Map.put(frame, "ref", ref)) do
      reply(socket, ref, 0)
    end
  end

  defp reply(_socket, _ref, count) when count >= 1_024,
    do: {:error, :too_many_live_events}

  defp reply(socket, ref, count) do
    case read_frame(socket) do
      {:ok, %{"ref" => ^ref, "type" => "ok"} = response} ->
        {:ok, response}

      {:ok, %{"ref" => ^ref, "type" => "error"} = response} ->
        {:error, {:server, response}}

      {:ok, %{"type" => kind}} when kind in ["delivery", "lifecycle"] ->
        reply(socket, ref, count + 1)

      _ ->
        {:error, :invalid_reply}
    end
  end

  defp send_frame(socket, frame) do
    payload = JSON.encode!(Map.put_new(frame, "v", 1)) <> "\n"
    size = byte_size(payload)

    if size > @max_frame do
      {:error, :request_too_large}
    else
      mask = :crypto.strong_rand_bytes(4)
      repeated = :binary.copy(mask, div(size + 3, 4))
      encoded = :crypto.exor(payload, binary_part(repeated, 0, size))

      header =
        cond do
          size < 126 -> <<1::1, 0::3, 1::4, 1::1, size::7>>
          size < 65_536 -> <<1::1, 0::3, 1::4, 1::1, 126::7, size::16>>
          true -> <<1::1, 0::3, 1::4, 1::1, 127::7, size::64>>
        end

      :ssl.send(socket, [header, mask, encoded])
    end
  end

  defp read_frame(socket) do
    with {:ok, <<1::1, 0::3, 1::4, 0::1, length::7>>} <- :ssl.recv(socket, 2, @timeout),
         {:ok, size} <- frame_size(socket, length),
         true <- size in 1..@max_server_frame,
         {:ok, payload} <- :ssl.recv(socket, size, @timeout),
         {:ok, value} <- JSON.decode_line(payload, @max_server_frame) do
      {:ok, value}
    else
      _ -> {:error, :invalid_server_frame}
    end
  end

  defp frame_size(_socket, size) when size < 126, do: {:ok, size}

  defp frame_size(socket, 126) do
    with {:ok, <<size::16>>} <- :ssl.recv(socket, 2, @timeout), do: {:ok, size}
  end

  defp frame_size(socket, 127) do
    with {:ok, <<size::64>>} <- :ssl.recv(socket, 8, @timeout), do: {:ok, size}
  end

  defp saved_operation(directory, semantic) do
    digest = hex(:crypto.hash(:sha256, JSON.encode!(semantic)))
    path = Path.join([directory, "operations", digest <> ".json"])

    with :ok <- private_directory(Path.dirname(path)) do
      case read_json(path) do
        {:ok, %{"request" => ^semantic, "op_id" => id}} when is_binary(id) ->
          {:ok, Map.put(semantic, "op_id", id)}

        {:error, :enoent} ->
          id = uuid_v4()

          case write_private(path, %{"request" => semantic, "op_id" => id}, :new) do
            :ok -> {:ok, Map.put(semantic, "op_id", id)}
            {:error, :eexist} -> saved_operation(directory, semantic)
            other -> other
          end

        _ ->
          {:error, :operation_conflict}
      end
    end
  end

  defp uuid_v4 do
    <<a::32, b::16, c::16, d::16, e::48>> = :crypto.strong_rand_bytes(16)

    Enum.join(
      [
        integer_hex(a, 8),
        integer_hex(b, 4),
        integer_hex(0x4000 ||| (c &&& 0x0FFF), 4),
        integer_hex(0x8000 ||| (d &&& 0x3FFF), 4),
        integer_hex(e, 12)
      ],
      "-"
    )
  end

  defp integer_hex(value, width),
    do: value |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(width, "0")

  defp hex(bytes), do: Base.encode16(bytes, case: :lower)

  defp private_directory(path) do
    case File.mkdir(path) do
      :ok ->
        File.chmod(path, 0o700)

      {:error, :eexist} ->
        existing_private_directory(path)

      error ->
        error
    end
  end

  defp existing_private_directory(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :directory, mode: mode}} ->
        if (mode &&& 0o077) == 0, do: :ok, else: {:error, :unsafe_local_directory}

      _ ->
        {:error, :unsafe_local_directory}
    end
  end

  defp write_private(path, value, mode \\ :replace) when mode in [:replace, :new] do
    temporary = path <> ".new-" <> integer_hex(System.unique_integer([:positive]), 1)

    with {:ok, io} <- File.open(temporary, [:write, :exclusive, :binary]) do
      result =
        with :ok <- File.chmod(temporary, 0o600),
             :ok <- IO.binwrite(io, JSON.encode!(value) <> "\n"),
             :ok <- :file.sync(io) do
          :ok
        end

      _ = File.close(io)

      case result do
        :ok ->
          installed =
            if mode == :new,
              do: File.ln(temporary, path),
              else: File.rename(temporary, path)

          _ = File.rm(temporary)

          case installed do
            :ok ->
              case System.cmd("sync", [Path.dirname(path)], stderr_to_stdout: true) do
                {_, 0} -> :ok
                _ -> {:error, :local_save_failed}
              end

            {:error, :eexist} ->
              {:error, :eexist}

            _ ->
              {:error, :local_save_failed}
          end

        _ ->
          _ = File.rm(temporary)
          {:error, :local_save_failed}
      end
    end
  end

  defp read_document(path) do
    with {:ok, contents} <- File.read(path),
         true <- byte_size(contents) + 1 <= @max_frame,
         {:ok, value} <- JSON.decode_line(contents <> "\n") do
      {:ok, value}
    else
      _ -> {:error, :invalid_document}
    end
  end

  defp read_json(path) do
    with {:ok, contents} <- File.read(path),
         do: JSON.decode_line(contents, @max_server_frame)
  end
end
