defmodule AttentionServer.OriginHTTPS do
  @moduledoc """
  Device-certificate HTTPS commands and content-free origin-session SSE notices.
  The origin feed remains the authoritative replay source after every notice.
  """

  use GenServer

  alias AttentionServer.{
    ConversationFiles,
    DeviceIdentity,
    Diagnostics,
    EnrolledLeafVerify,
    JSON,
    Message,
    OriginWire,
    Relay
  }

  @timeout 5_000
  @max_header 16 * 1024
  @max_body 2 * 1024 * 1024
  @keepalive_ms 15_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
  def port(server), do: GenServer.call(server, :port)

  @impl true
  def init(opts) do
    with {:ok, relay} <-
           Diagnostics.result(
             :origin_https,
             :configuration,
             Keyword.fetch(opts, :relay),
             :unavailable
           ),
         {:ok, certfile} <-
           Diagnostics.result(
             :origin_https,
             :configuration,
             Keyword.fetch(opts, :certfile),
             :unavailable
           ),
         {:ok, keyfile} <-
           Diagnostics.result(
             :origin_https,
             :configuration,
             Keyword.fetch(opts, :keyfile),
             :unavailable
           ),
         {:ok, cacertfile} <-
           Diagnostics.result(
             :origin_https,
             :configuration,
             Keyword.fetch(opts, :cacertfile),
             :unavailable
           ),
         {:ok, listener} <-
           Diagnostics.result(
             :origin_https,
             :listener_start,
             listen(opts, relay, certfile, keyfile, cacertfile),
             :unavailable
           ),
         {:ok, {_ip, port}} <-
           Diagnostics.result(
             :origin_https,
             :listener_address,
             :ssl.sockname(listener),
             :unavailable
           ),
         {:ok, supervisor} <-
           Diagnostics.result(
             :origin_https,
             :worker_start,
             Task.Supervisor.start_link(),
             :unavailable
           ) do
      case Diagnostics.result(
             :origin_https,
             :worker_start,
             Task.Supervisor.start_child(supervisor, fn ->
               accept_loop(listener, relay, supervisor)
             end),
             :unavailable
           ) do
        {:ok, _} ->
          case Diagnostics.result(
                 :origin_https,
                 :session_setup,
                 Relay.register_origin_listener(relay, self(), port),
                 :unavailable
               ) do
            :ok ->
              {:ok, %{listener: listener, supervisor: supervisor, relay: relay, port: port}}

            error ->
              :ssl.close(listener)
              Supervisor.stop(supervisor, :normal)
              {:stop, error}
          end

        error ->
          :ssl.close(listener)
          Supervisor.stop(supervisor, :normal)
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
      Relay.unregister_origin_listener(state.relay, self())
    catch
      :exit, reason ->
        Diagnostics.failure(:origin_https, :server_terminate, reason, :unavailable)
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
      certfile: String.to_charlist(certfile),
      keyfile: String.to_charlist(keyfile),
      cacertfile: String.to_charlist(cacertfile),
      verify: :verify_peer,
      verify_fun: EnrolledLeafVerify.device(relay),
      fail_if_no_peer_cert: true,
      session_tickets: :disabled,
      early_data: :disabled
    )
  end

  defp accept_loop(listener, relay, supervisor) do
    case :ssl.transport_accept(listener) do
      {:ok, socket} ->
        case Task.Supervisor.start_child(supervisor, fn -> await_socket(relay) end) do
          {:ok, worker} ->
            case :ssl.controlling_process(socket, worker) do
              :ok ->
                send(worker, {:socket, socket})

              {:error, reason} ->
                Diagnostics.failure(:origin_https, :socket_control, reason, :closed)
                send(worker, :abort)
                :ssl.close(socket)
            end

          {:error, reason} ->
            Diagnostics.failure(:origin_https, :worker_start, reason, :closed)
            :ssl.close(socket)
        end

        accept_loop(listener, relay, supervisor)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        Diagnostics.failure(:origin_https, :accept, reason, :retry_100ms)
        Process.sleep(100)
        accept_loop(listener, relay, supervisor)
    end
  end

  defp await_socket(relay) do
    receive do
      {:socket, socket} -> serve(socket, relay)
      :abort -> :ok
    end
  end

  defp serve(socket, relay) do
    try do
      with {:ok, tls} <-
             Diagnostics.result(
               :origin_https,
               :tls_handshake,
               :ssl.handshake(socket, @timeout),
               :closed
             ),
           {:ok, certificate} <-
             Diagnostics.result(:origin_https, :peer_certificate, :ssl.peercert(tls), :closed),
           {:ok, key} <-
             Diagnostics.result(
               :origin_https,
               :identity,
               DeviceIdentity.from_certificate(certificate),
               :closed
             ),
           {:ok, _cursor} <-
             Diagnostics.result(
               :origin_https,
               :authentication,
               Relay.acknowledged_cursor(relay, key),
               :closed
             ) do
        case read_request(tls) do
          {:ok, {:post, request}} ->
            operation_id = Diagnostics.operation_id(request)

            _ =
              Diagnostics.result(
                :origin_https,
                :response_write,
                :ssl.send(
                  tls,
                  OriginWire.http_response(200, OriginWire.command(relay, key, request))
                ),
                :outcome_unknown,
                operation_id
              )

          {:ok, {:stream, handle}} ->
            stream(tls, relay, key, handle)

          {:ok, {:upload, attrs, size, remainder}} ->
            upload(tls, relay, key, attrs, size, remainder)

          {:ok, {:fetch_file, id}} ->
            fetch_file(tls, relay, key, id)

          {:error, reason} ->
            {status, message} = transport_error(reason)
            _ = Diagnostics.result(:origin_https, :request_read, {:error, reason}, :rejected)

            _ =
              Diagnostics.result(
                :origin_https,
                :response_write,
                :ssl.send(tls, OriginWire.http_response(status, OriginWire.error(message))),
                :closed
              )
        end
      end
    rescue
      error ->
        Diagnostics.failure(:origin_https, :relay_boundary, error, :outcome_unknown)
        :ok
    catch
      :exit, reason ->
        Diagnostics.failure(:origin_https, :relay_boundary, reason, :outcome_unknown)
        :ok
    after
      :ssl.close(socket)
    end
  end

  defp upload(socket, relay, key, attrs, size, remainder) do
    cap =
      case Relay.origin_file_limits(relay, key) do
        {:ok, %{"upload_cap_bytes_per_file" => value}} ->
          value

        {:error, reason} ->
          Diagnostics.failure(
            :origin_https,
            :upload_begin,
            reason,
            :rejected,
            Diagnostics.operation_id(attrs)
          )

          :unavailable

        other ->
          Diagnostics.failure(:origin_https, :upload_begin, other, :rejected)
          :unavailable
      end

    cond do
      cap == :unavailable ->
        Diagnostics.result(
          :origin_https,
          :response_write,
          :ssl.send(socket, OriginWire.http_response(403, OriginWire.error("forbidden"))),
          :closed,
          Diagnostics.operation_id(attrs)
        )

      is_integer(cap) and size > cap ->
        Diagnostics.failure(
          :origin_https,
          :upload_begin,
          :upload_cap_exceeded,
          :rejected,
          Diagnostics.operation_id(attrs)
        )

        Diagnostics.result(
          :origin_https,
          :response_write,
          :ssl.send(
            socket,
            OriginWire.http_response(
              413,
              OriginWire.error("upload_cap_exceeded")
              |> Map.put("upload_cap_bytes_per_file", cap)
            )
          ),
          :closed,
          Diagnostics.operation_id(attrs)
        )

      true ->
        case Relay.upload_begin(relay, key, attrs, size) do
          {:ok, lease} ->
            try do
              case read_upload(socket, remainder, size, Diagnostics.operation_id(attrs)) do
                {:ok, path, hash} ->
                  result = Relay.upload_file(relay, key, attrs, path, size, hash, lease)

                  case File.rm(path) do
                    :ok ->
                      :ok

                    {:error, :enoent} ->
                      :ok

                    {:error, reason} ->
                      Diagnostics.failure(
                        :origin_https,
                        :file_cleanup,
                        reason,
                        :unavailable,
                        Diagnostics.operation_id(attrs)
                      )
                  end

                  case result do
                    {:ok, reply} ->
                      Diagnostics.result(
                        :origin_https,
                        :response_write,
                        :ssl.send(
                          socket,
                          OriginWire.http_response(
                            200,
                            Map.merge(reply, %{"v" => 1, "type" => "ok"})
                          )
                        ),
                        :outcome_unknown,
                        Diagnostics.operation_id(attrs)
                      )

                    {:error, reason} ->
                      Diagnostics.failure(
                        :origin_https,
                        :upload_commit,
                        reason,
                        :rejected,
                        Diagnostics.operation_id(attrs)
                      )

                      status =
                        case reason do
                          :upload_cap_exceeded -> 413
                          :forbidden -> 403
                          :upload_conflict -> 409
                          :upload_discarded -> 409
                          :invalid_upload -> 400
                          _ -> 503
                        end

                      Diagnostics.result(
                        :origin_https,
                        :response_write,
                        :ssl.send(
                          socket,
                          OriginWire.http_response(
                            status,
                            OriginWire.error(Atom.to_string(reason))
                          )
                        ),
                        :closed,
                        Diagnostics.operation_id(attrs)
                      )
                  end

                {:error, reason} ->
                  Diagnostics.failure(
                    :origin_https,
                    :upload_read,
                    reason,
                    :rejected,
                    Diagnostics.operation_id(attrs)
                  )

                  status = if reason == :storage_unavailable, do: 503, else: 400

                  Diagnostics.result(
                    :origin_https,
                    :response_write,
                    :ssl.send(
                      socket,
                      OriginWire.http_response(status, OriginWire.error(Atom.to_string(reason)))
                    ),
                    :closed,
                    Diagnostics.operation_id(attrs)
                  )
              end
            after
              _ =
                Diagnostics.result(
                  :origin_https,
                  :upload_commit,
                  Relay.upload_end(relay, lease),
                  :outcome_unknown,
                  Diagnostics.operation_id(attrs)
                )
            end

          {:error, reason} ->
            Diagnostics.failure(
              :origin_https,
              :upload_begin,
              reason,
              :rejected,
              Diagnostics.operation_id(attrs)
            )

            status = if reason == :forbidden, do: 403, else: 400

            Diagnostics.result(
              :origin_https,
              :response_write,
              :ssl.send(
                socket,
                OriginWire.http_response(status, OriginWire.error(Atom.to_string(reason)))
              ),
              :closed,
              Diagnostics.operation_id(attrs)
            )
        end
    end
  end

  defp fetch_file(socket, relay, key, id) do
    case Relay.fetch_file(relay, key, id) do
      {:ok, upload, path} ->
        operation_id = Diagnostics.operation_id(upload)

        header = [
          "HTTP/1.1 200 OK\r\n",
          "Content-Type: application/octet-stream\r\n",
          "Content-Length: #{upload.size}\r\n",
          "X-Harklight-Sha256: #{upload.sha256}\r\n",
          "Cache-Control: no-store\r\n",
          "Connection: close\r\n\r\n"
        ]

        case Diagnostics.result(
               :origin_https,
               :response_write,
               :ssl.send(socket, header),
               :outcome_unknown,
               operation_id
             ) do
          :ok ->
            case File.open(path, [:read, :binary]) do
              {:ok, file} ->
                try do
                  send_file_chunks(socket, file, operation_id)
                after
                  case File.close(file) do
                    :ok ->
                      :ok

                    {:error, reason} ->
                      Diagnostics.failure(
                        :origin_https,
                        :file_read,
                        reason,
                        :outcome_unknown,
                        operation_id
                      )
                  end
                end

              {:error, reason} ->
                Diagnostics.failure(:origin_https, :file_read, reason, :unavailable, operation_id)
                {:error, reason}
            end

          {:error, _reason} = error ->
            error
        end

      {:error, :storage_unavailable} ->
        Diagnostics.failure(:origin_https, :file_fetch, :storage_unavailable, :unavailable)

        Diagnostics.result(
          :origin_https,
          :response_write,
          :ssl.send(
            socket,
            OriginWire.http_response(503, OriginWire.error("storage_unavailable"))
          ),
          :closed
        )

      {:error, reason} ->
        Diagnostics.failure(:origin_https, :file_fetch, reason, :rejected)

        Diagnostics.result(
          :origin_https,
          :response_write,
          :ssl.send(socket, OriginWire.http_response(404, OriginWire.error("not_found"))),
          :closed
        )

      other ->
        Diagnostics.failure(:origin_https, :file_fetch, other, :outcome_unknown)
        other
    end
  end

  defp send_file_chunks(socket, file, operation_id) do
    case IO.binread(file, 65_536) do
      :eof ->
        :ok

      bytes when is_binary(bytes) ->
        case Diagnostics.result(
               :origin_https,
               :response_write,
               :ssl.send(socket, bytes),
               :outcome_unknown,
               operation_id
             ) do
          :ok -> send_file_chunks(socket, file, operation_id)
          error -> error
        end

      {:error, reason} ->
        Diagnostics.failure(:origin_https, :file_read, reason, :unavailable, operation_id)
        {:error, :storage_unavailable}

      other ->
        Diagnostics.failure(:origin_https, :file_read, other, :unavailable, operation_id)
        {:error, :storage_unavailable}
    end
  end

  defp read_upload(_socket, remainder, size, _operation_id) when byte_size(remainder) > size,
    do: {:error, :bad_frame}

  defp read_upload(socket, remainder, size, operation_id) do
    path =
      Path.join(
        System.tmp_dir!(),
        "harklight-upload-" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
      )

    case File.open(path, [:write, :exclusive, :binary]) do
      {:ok, file} ->
        result =
          read_upload_chunks(
            socket,
            file,
            remainder,
            size,
            :crypto.hash_init(:sha256),
            operation_id
          )

        case File.close(file) do
          :ok ->
            :ok

          {:error, reason} ->
            Diagnostics.failure(:origin_https, :upload_read, reason, :closed, operation_id)
        end

        case result do
          {:ok, hash} ->
            {:ok, path, hash}

          error ->
            case File.rm(path) do
              :ok ->
                :ok

              {:error, reason} ->
                Diagnostics.failure(
                  :upload_cleanup,
                  :file_cleanup,
                  reason,
                  :unavailable,
                  operation_id
                )
            end

            error
        end

      {:error, reason} ->
        Diagnostics.failure(:origin_https, :upload_read, reason, :closed, operation_id)
        {:error, :storage_unavailable}
    end
  end

  defp read_upload_chunks(_socket, _file, <<>>, 0, hash, _operation_id),
    do: {:ok, Base.encode16(:crypto.hash_final(hash), case: :lower)}

  defp read_upload_chunks(socket, file, bytes, remaining, hash, operation_id)
       when byte_size(bytes) <= remaining do
    with :ok <- IO.binwrite(file, bytes) do
      remaining = remaining - byte_size(bytes)
      hash = :crypto.hash_update(hash, bytes)

      if remaining == 0 do
        {:ok, Base.encode16(:crypto.hash_final(hash), case: :lower)}
      else
        case :ssl.recv(socket, min(65_536, remaining), @timeout) do
          {:ok, next} ->
            read_upload_chunks(socket, file, next, remaining, hash, operation_id)

          {:error, reason} ->
            Diagnostics.failure(:origin_https, :upload_read, reason, :closed, operation_id)
            {:error, :incomplete_upload}
        end
      end
    else
      {:error, reason} ->
        Diagnostics.failure(:origin_https, :upload_read, reason, :closed, operation_id)
        {:error, :storage_unavailable}

      other ->
        Diagnostics.failure(:origin_https, :upload_read, other, :closed, operation_id)
        {:error, :storage_unavailable}
    end
  end

  defp stream(socket, relay, key, handle) do
    case Relay.origin_stream_attach(relay, key, handle, self()) do
      :ok ->
        try do
          with {:ok, %{"cursor" => cursor}} <-
                 Diagnostics.result(
                   :origin_https,
                   :stream_status,
                   Relay.origin_status(relay, key, handle),
                   :closed
                 ),
               :ok <- stream_send(socket, stream_headers(), :response_write),
               :ok <- stream_send(socket, change_notice(cursor), :stream_write) do
            stream_loop(socket, cursor)
          else
            {:error, reason} ->
              {:error, reason}

            other ->
              Diagnostics.failure(:origin_https, :stream_status, other, :closed)
              other
          end
        after
          _ =
            Diagnostics.result(
              :origin_https,
              :stream_detach,
              Relay.origin_stream_detach(relay, key, handle, self()),
              :unavailable
            )
        end

      {:error, reason} ->
        Diagnostics.failure(:origin_https, :stream_attach, reason, :rejected)

        _ =
          stream_send(
            socket,
            OriginWire.http_response(200, OriginWire.error(Atom.to_string(reason))),
            :response_write
          )
    end
  end

  defp stream_send(socket, bytes, phase),
    do: Diagnostics.result(:origin_https, phase, :ssl.send(socket, bytes), :closed)

  defp stream_loop(socket, cursor) do
    case :ssl.setopts(socket, active: :once) do
      :ok ->
        stream_receive(socket, cursor)

      {:error, reason} ->
        Diagnostics.failure(:origin_https, :stream_read, reason, :closed)
        :ok

      other ->
        Diagnostics.failure(:origin_https, :stream_read, other, :closed)
        :ok
    end
  end

  defp stream_receive(socket, cursor) do
    receive do
      {:origin_state_changed, next} when is_integer(next) and next > cursor ->
        case stream_send(socket, change_notice(next), :stream_write) do
          :ok ->
            stream_loop(socket, next)

          _ ->
            :ok
        end

      {:ssl, ^socket, _bytes} ->
        Diagnostics.failure(:origin_https, :stream_read, :bad_frame, :closed)
        :ok

      {:ssl_closed, ^socket} ->
        :ok

      {:ssl_error, ^socket, reason} ->
        Diagnostics.failure(:origin_https, :stream_read, reason, :closed)
        :ok
    after
      @keepalive_ms ->
        case stream_send(socket, ": keepalive\n\n", :stream_write) do
          :ok -> stream_loop(socket, cursor)
          _ -> :ok
        end
    end
  end

  defp stream_headers do
    [
      "HTTP/1.1 200 OK\r\n",
      "Content-Type: text/event-stream; charset=utf-8\r\n",
      "Cache-Control: no-store\r\n",
      "Connection: close\r\n\r\n"
    ]
  end

  defp change_notice(cursor),
    do: ["event: change\n", "data: ", JSON.encode!(%{"cursor" => cursor}), "\n\n"]

  defp transport_error(:method_not_allowed), do: {405, "method_not_allowed"}
  defp transport_error(:not_found), do: {404, "not_found"}
  defp transport_error(:invalid_json), do: {400, "invalid_json"}
  defp transport_error(_), do: {400, "bad_frame"}

  defp read_request(socket) do
    with {:ok, header, remainder} <- read_header(socket, <<>>),
         {:ok, target} <- parse_header(header) do
      case target do
        {:post, size} ->
          with {:ok, body} <- read_body(socket, remainder, size),
               true <- String.valid?(body),
               {:ok, request} <- JSON.decode_line(body <> "\n", @max_body + 1),
               true <- is_map(request) and not is_struct(request) do
            {:ok, {:post, request}}
          else
            _ -> {:error, :invalid_json}
          end

        {:stream, handle} ->
          if remainder == <<>>, do: {:ok, {:stream, handle}}, else: {:error, :bad_frame}

        {:upload, attrs, size} ->
          {:ok, {:upload, attrs, size, remainder}}

        {:fetch_file, id} ->
          if remainder == <<>>, do: {:ok, {:fetch_file, id}}, else: {:error, :bad_frame}
      end
    else
      error -> error
    end
  rescue
    error ->
      Diagnostics.failure(:origin_https, :request_read, error, :rejected)
      {:error, :bad_frame}
  end

  defp read_header(socket, buffer) do
    case :binary.match(buffer, "\r\n\r\n") do
      {offset, 4} when offset <= @max_header ->
        {:ok, binary_part(buffer, 0, offset),
         binary_part(buffer, offset + 4, byte_size(buffer) - offset - 4)}

      {_, 4} ->
        {:error, :bad_frame}

      :nomatch when byte_size(buffer) > @max_header ->
        {:error, :bad_frame}

      :nomatch ->
        case :ssl.recv(socket, 0, @timeout) do
          {:ok, chunk} when byte_size(buffer) + byte_size(chunk) <= @max_header + @max_body + 4 ->
            read_header(socket, buffer <> chunk)

          {:error, reason} ->
            {:error, reason}

          other ->
            {:error, other}
        end
    end
  end

  defp parse_header(header) do
    case String.split(header, "\r\n") do
      [request_line | fields] ->
        case String.split(request_line, " ") do
          ["POST", "/origin/v1", "HTTP/1.1"] ->
            parse_post_headers(fields)

          ["POST", "/origin/v1/upload", "HTTP/1.1"] ->
            parse_upload_headers(fields)

          ["GET", "/origin/v1/files/" <> id, "HTTP/1.1"] ->
            parse_fetch_headers(id, fields)

          [method, path, "HTTP/1.1"] when method != "POST" and path == "/origin/v1" ->
            {:error, :method_not_allowed}

          ["GET", path, "HTTP/1.1"] ->
            parse_stream_headers(path, fields)

          [_, _, "HTTP/1.1"] ->
            {:error, :not_found}

          _ ->
            {:error, :bad_frame}
        end

      _ ->
        {:error, :bad_frame}
    end
  end

  defp parse_post_headers(fields) do
    with {:ok, headers} <- headers(fields, %{}),
         true <- json_content_type?(headers["content-type"]),
         false <- Map.has_key?(headers, "transfer-encoding"),
         {:ok, size} <- content_length(headers["content-length"]) do
      {:ok, {:post, size}}
    else
      _ -> {:error, :bad_frame}
    end
  end

  defp parse_upload_headers(fields) do
    with {:ok, headers} <- headers(fields, %{}),
         true <- headers["content-type"] == "application/octet-stream",
         false <- Map.has_key?(headers, "transfer-encoding"),
         {:ok, size} <- upload_content_length(headers["content-length"]),
         {:ok, name} <- Base.url_decode64(headers["x-harklight-file-name"], padding: false),
         true <- ConversationFiles.valid_name?(name),
         true <- Message.uuid_v4?(headers["x-harklight-op-id"]),
         true <- Message.sender_id?(headers["x-harklight-sender-id"]),
         true <- Message.nonempty?(headers["x-harklight-message-id"]),
         {:ok, revision} <- positive_int(headers["x-harklight-request-revision"]) do
      {:ok,
       {:upload,
        %{
          "op_id" => headers["x-harklight-op-id"],
          "sender_id" => headers["x-harklight-sender-id"],
          "message_id" => headers["x-harklight-message-id"],
          "revision" => revision,
          "name" => name
        }, size}}
    else
      _ -> {:error, :bad_frame}
    end
  end

  defp parse_fetch_headers(id, fields) do
    with true <- Message.uuid_v4?(id),
         {:ok, headers} <- headers(fields, %{}),
         false <- Map.has_key?(headers, "transfer-encoding"),
         false <- Map.has_key?(headers, "content-length") do
      {:ok, {:fetch_file, id}}
    else
      _ -> {:error, :not_found}
    end
  end

  defp upload_content_length(value) do
    if is_binary(value) and Regex.match?(~r/\A(?:0|[1-9][0-9]*)\z/, value),
      do: {:ok, String.to_integer(value)},
      else: {:error, :bad_frame}
  end

  defp positive_int(value) do
    if is_binary(value) and Regex.match?(~r/\A[1-9][0-9]*\z/, value),
      do: {:ok, String.to_integer(value)},
      else: {:error, :bad_frame}
  end

  defp parse_stream_headers(path, fields) do
    with {:ok, headers} <- headers(fields, %{}),
         false <- Map.has_key?(headers, "transfer-encoding"),
         false <- Map.has_key?(headers, "content-length"),
         ["/origin/v1/stream?session", handle] <- String.split(path, "="),
         true <- Message.uuid_v4?(handle) do
      {:ok, {:stream, handle}}
    else
      _ -> {:error, :not_found}
    end
  end

  defp headers([], found), do: {:ok, found}

  defp headers([field | rest], found) do
    case :binary.split(field, ":") do
      [name, value] ->
        key = String.downcase(name)

        if Regex.match?(~r/\A[a-z0-9-]+\z/, key) and not Map.has_key?(found, key),
          do: headers(rest, Map.put(found, key, String.trim(value))),
          else: {:error, :bad_frame}

      _ ->
        {:error, :bad_frame}
    end
  end

  defp json_content_type?(value) when is_binary(value),
    do: String.downcase(value) in ["application/json", "application/json; charset=utf-8"]

  defp json_content_type?(_), do: false

  defp content_length(value) when is_binary(value) do
    if Regex.match?(~r/\A[1-9][0-9]{0,6}\z/, value) do
      size = String.to_integer(value)
      if size <= @max_body, do: {:ok, size}, else: {:error, :bad_frame}
    else
      {:error, :bad_frame}
    end
  end

  defp content_length(_), do: {:error, :bad_frame}

  defp read_body(_socket, buffer, size) when byte_size(buffer) == size, do: {:ok, buffer}
  defp read_body(_socket, buffer, size) when byte_size(buffer) > size, do: {:error, :bad_frame}

  defp read_body(socket, buffer, size) do
    case :ssl.recv(socket, size - byte_size(buffer), @timeout) do
      {:ok, chunk} -> read_body(socket, buffer <> chunk, size)
      {:error, reason} -> {:error, reason}
      other -> {:error, other}
    end
  end
end
