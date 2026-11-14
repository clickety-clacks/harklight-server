defmodule AttentionServer.DashboardBrowserHTTP do
  @moduledoc false

  import Plug.Conn

  alias AttentionServer.{ConversationFiles, Diagnostics, JSON, Message}

  @cookie "__Host-harklight"
  @max_command_body 2 * 1024 * 1024
  @upload_read_chunk 65_536
  @file_command_types ~w(file_limits discard_uploads)

  def upgrade(conn, opts) do
    conn = fetch_query_params(conn)

    if exact_origin?(conn, opts) do
      cookie = browser_cookie(conn)
      recipient = Map.get(conn.query_params, "recipient")

      Plug.Conn.upgrade_adapter(
        conn,
        :websocket,
        {AttentionServer.DashboardBrowserWebSocket,
         [backend: opts.backend, cookie: cookie, recipient: recipient],
         [timeout: :infinity, max_frame_size: @max_command_body]}
      )
    else
      json_error(conn, 403, "forbidden")
    end
  rescue
    _ -> send_resp(conn, 400, "Bad request")
  end

  defp browser_cookie(conn) do
    conn = fetch_cookies(conn)
    Map.get(conn.req_cookies, @cookie)
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  def command(conn, opts) do
    with true <- exact_origin?(conn, opts),
         {:ok, recipient, conn} <- recipient(conn),
         {:ok, request, conn} <- read_json(conn, @max_command_body),
         {:ok, cookie, conn} <- cookie(conn) do
      if request["type"] in @file_command_types do
        case backend_call(opts.backend, :browser_file_command, {cookie, recipient, request}) do
          {:ok, reply} when is_map(reply) -> wire_command_reply(conn, reply)
          {:error, :session_ended} -> session_redirect(conn, opts)
          {:error, :forbidden} -> json_error(conn, 403, "forbidden")
          {:error, :storage_unavailable} -> json_error(conn, 503, "storage_unavailable")
          {:error, reason} -> json_error(conn, 200, Atom.to_string(reason))
          reply when is_map(reply) -> wire_command_reply(conn, reply)
          _ -> json_error(conn, 503, "storage_unavailable")
        end
      else
        wire_command_reply(conn, %{
          "v" => 1,
          "type" => "error",
          "reason" => "invalid_field",
          "field" => "type"
        })
      end
    else
      false -> json_error(conn, 403, "forbidden")
      {:error, :forbidden, conn} -> json_error(conn, 403, "forbidden")
      {:error, :invalid_request, conn} -> json_error(conn, 400, "invalid_request")
      {:error, :invalid_field, conn} -> json_error(conn, 400, "invalid_field")
      {:error, _reason, conn} -> json_error(conn, 400, "invalid_request")
    end
  end

  def upload(conn, opts) do
    with true <- exact_origin?(conn, opts),
         {:ok, recipient, conn} <- recipient(conn),
         {:ok, attrs, size} <- upload_attributes(conn),
         {:ok, cookie, conn} <- cookie(conn) do
      case backend_call(opts.backend, :browser_file_limits, {cookie, recipient}) do
        {:ok, %{"upload_cap_bytes_per_file" => cap}} ->
          upload_with_limit(conn, opts, cookie, recipient, attrs, size, cap)

        {:error, :session_ended} ->
          session_redirect(conn, opts)

        {:error, :storage_unavailable} ->
          json_error(conn, 503, "storage_unavailable")

        _ ->
          json_error(conn, 403, "forbidden")
      end
    else
      false -> json_error(conn, 403, "forbidden")
      {:error, :forbidden, conn} -> json_error(conn, 403, "forbidden")
      {:error, :invalid_request, conn} -> json_error(conn, 400, "invalid_upload")
      {:error, :invalid_field, conn} -> json_error(conn, 400, "invalid_upload")
      {:error, _reason, conn} -> json_error(conn, 400, "invalid_upload")
    end
  end

  def fetch_file(conn, opts, upload_id) do
    with true <- Message.uuid_v4?(upload_id),
         {:ok, cookie, conn} <- cookie(conn) do
      case backend_call(opts.backend, :browser_fetch_file, {cookie, upload_id}) do
        {:ok, %{size: size, sha256: sha256, name: name}, path}
        when is_integer(size) and is_binary(sha256) and is_binary(name) ->
          stream_download(conn, size, sha256, name, path)

        {:error, :session_ended} ->
          session_redirect(conn, opts)

        {:error, :storage_unavailable} ->
          json_error(conn, 503, "storage_unavailable")

        _ ->
          json_error(conn, 404, "not_found")
      end
    else
      false -> json_error(conn, 404, "not_found")
      {:error, _reason, conn} -> json_error(conn, 400, "not_found")
    end
  end

  defp upload_with_limit(conn, opts, cookie, recipient, attrs, size, cap) do
    cond do
      is_integer(cap) and size > cap ->
        json_error(conn, 413, "upload_cap_exceeded", %{"upload_cap_bytes_per_file" => cap})

      cap != "none" and not is_integer(cap) ->
        json_error(conn, 503, "storage_unavailable")

      true ->
        case backend_call(opts.backend, :browser_upload_begin, {
               cookie,
               recipient,
               attrs,
               size,
               self()
             }) do
          {:ok, lease} ->
            try do
              case read_upload_body(conn, size) do
                {:ok, path, sha256, conn} ->
                  try do
                    result =
                      backend_call(
                        opts.backend,
                        :browser_upload_file,
                        {cookie, recipient, attrs, path, size, sha256, lease}
                      )

                    upload_reply(conn, opts, result)
                  after
                    _ = File.rm(path)
                  end

                {:error, reason, conn} ->
                  json_error(
                    conn,
                    if(reason == :storage_unavailable, do: 503, else: 400),
                    Atom.to_string(reason)
                  )
              end
            after
              _ = backend_call(opts.backend, :browser_upload_end, {cookie, recipient, lease})
            end

          {:error, :session_ended} ->
            session_redirect(conn, opts)

          {:error, reason} ->
            json_error(conn, status(reason), Atom.to_string(reason))

          _ ->
            json_error(conn, 503, "storage_unavailable")
        end
    end
  end

  defp upload_reply(conn, _opts, {:ok, reply}) when is_map(reply) do
    reply
    |> Map.merge(%{"v" => 1, "type" => "ok"})
    |> then(&wire_reply(conn, &1))
  end

  defp upload_reply(conn, opts, {:error, :session_ended}), do: session_redirect(conn, opts)

  defp upload_reply(conn, _opts, {:error, reason}),
    do: json_error(conn, status(reason), Atom.to_string(reason))

  defp upload_reply(conn, _opts, reply) when is_map(reply), do: wire_reply(conn, reply)
  defp upload_reply(conn, _opts, _), do: json_error(conn, 503, "storage_unavailable")

  defp recipient(conn) do
    conn = fetch_query_params(conn)
    recipient = Map.get(conn.query_params, "recipient")

    if AttentionServer.Message.uuid_v4?(recipient) do
      {:ok, recipient, conn}
    else
      {:error, :forbidden, conn}
    end
  rescue
    _ -> {:error, :forbidden, conn}
  end

  defp cookie(conn) do
    conn = fetch_cookies(conn)

    case Map.get(conn.req_cookies, @cookie) do
      value when is_binary(value) -> {:ok, value, conn}
      _ -> {:error, :forbidden, conn}
    end
  rescue
    _ -> {:error, :forbidden, conn}
  end

  defp upload_attributes(conn) do
    headers = %{
      "content-type" => get_req_header(conn, "content-type"),
      "content-length" => get_req_header(conn, "content-length"),
      "transfer-encoding" => get_req_header(conn, "transfer-encoding"),
      "name" => get_req_header(conn, "x-harklight-file-name"),
      "op_id" => get_req_header(conn, "x-harklight-op-id"),
      "sender_id" => get_req_header(conn, "x-harklight-sender-id"),
      "message_id" => get_req_header(conn, "x-harklight-message-id"),
      "revision" => get_req_header(conn, "x-harklight-request-revision")
    }

    with ["application/octet-stream"] <- headers["content-type"],
         [] <- headers["transfer-encoding"],
         [content_length] <- headers["content-length"],
         {:ok, size} <- content_length(content_length),
         [encoded_name] <- headers["name"],
         {:ok, name} <- Base.url_decode64(encoded_name, padding: false),
         true <- ConversationFiles.valid_name?(name),
         [op_id] <- headers["op_id"],
         true <- Message.uuid_v4?(op_id),
         [sender_id] <- headers["sender_id"],
         true <- Message.sender_id?(sender_id),
         [message_id] <- headers["message_id"],
         true <- Message.nonempty?(message_id),
         [revision_value] <- headers["revision"],
         {:ok, revision} <- positive_integer(revision_value) do
      {:ok,
       %{
         "op_id" => op_id,
         "sender_id" => sender_id,
         "message_id" => message_id,
         "revision" => revision,
         "name" => name
       }, size}
    else
      _ -> {:error, :invalid_field, conn}
    end
  end

  defp read_json(conn, length) do
    with [content_type] <- get_req_header(conn, "content-type"),
         true <- json_content_type?(content_type) do
      case read_body(conn, length: length, read_length: length) do
        {:ok, body, conn} ->
          case JSON.decode_line(body <> "\n", length + 1) do
            {:ok, value} when is_map(value) and not is_struct(value) ->
              {:ok, value, conn}

            _ ->
              {:error, :invalid_request, conn}
          end

        {:more, _body, conn} ->
          {:error, :invalid_request, conn}

        {:error, _reason} ->
          {:error, :invalid_request, conn}
      end
    else
      _ -> {:error, :invalid_request, conn}
    end
  rescue
    _ -> {:error, :invalid_request, conn}
  end

  defp read_upload_body(conn, size) do
    path =
      Path.join(
        System.tmp_dir!(),
        "harklight-browser-upload-" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
      )

    case File.open(path, [:write, :exclusive, :binary]) do
      {:ok, file} ->
        result =
          read_upload_chunks(
            conn,
            file,
            size,
            0,
            :crypto.hash_init(:sha256)
          )

        close_result = File.close(file)

        case {result, close_result} do
          {{:ok, conn, hash}, :ok} ->
            {:ok, path, :crypto.hash_final(hash) |> Base.encode16(case: :lower), conn}

          {{:error, reason, conn}, _} ->
            _ = File.rm(path)
            {:error, reason, conn}

          {_, {:error, _reason}} ->
            _ = File.rm(path)
            {:error, :storage_unavailable, conn}
        end

      {:error, _reason} ->
        {:error, :storage_unavailable, conn}
    end
  end

  defp read_upload_chunks(conn, _file, size, received, hash) when received == size do
    case read_body(conn, length: 1, read_length: 1) do
      {:ok, <<>>, conn} -> {:ok, conn, hash}
      {:ok, _extra, conn} -> {:error, :invalid_upload, conn}
      {:more, _extra, conn} -> {:error, :invalid_upload, conn}
      {:error, _reason} -> {:error, :invalid_upload, conn}
    end
  end

  defp read_upload_chunks(conn, file, size, received, hash) do
    remaining = size - received
    amount = min(@upload_read_chunk, remaining)

    case read_body(conn, length: amount, read_length: amount) do
      {status, bytes, conn} when status in [:ok, :more] and is_binary(bytes) ->
        next_received = received + byte_size(bytes)

        cond do
          byte_size(bytes) == 0 and status == :more ->
            {:error, :incomplete_upload, conn}

          next_received > size ->
            {:error, :invalid_upload, conn}

          IO.binwrite(file, bytes) != :ok ->
            {:error, :storage_unavailable, conn}

          status == :ok and next_received != size ->
            {:error, :incomplete_upload, conn}

          next_received == size and status == :more ->
            {:error, :invalid_upload, conn}

          next_received == size ->
            {:ok, conn, :crypto.hash_update(hash, bytes)}

          true ->
            read_upload_chunks(conn, file, size, next_received, :crypto.hash_update(hash, bytes))
        end

      {:error, _reason} ->
        {:error, :incomplete_upload, conn}

      _ ->
        {:error, :incomplete_upload, conn}
    end
  end

  defp stream_download(conn, size, sha256, name, path) do
    case File.open(path, [:read, :binary]) do
      {:ok, file} ->
        close_result = File.close(file)

        if close_result == :ok do
          conn
          |> put_resp_header("content-type", "application/octet-stream")
          |> put_resp_header("content-length", Integer.to_string(size))
          |> put_resp_header("x-harklight-sha256", sha256)
          |> put_resp_header(
            "content-disposition",
            "attachment; filename*=UTF-8''" <> URI.encode(name, &URI.char_unreserved?/1)
          )
          |> send_file(200, path, 0, size)
        else
          Diagnostics.failure(:dashboard_http, :file_fetch, :storage_unavailable, :unavailable)
          json_error(conn, 503, "storage_unavailable")
        end

      {:error, _reason} ->
        Diagnostics.failure(:dashboard_http, :file_fetch, :storage_unavailable, :unavailable)
        json_error(conn, 503, "storage_unavailable")
    end
  end

  defp exact_origin?(conn, opts) do
    expected = expected_origin(conn, opts)
    get_req_header(conn, "origin") == [expected]
  end

  defp expected_origin(_conn, %{expected_origin: origin}) when is_binary(origin), do: origin

  defp expected_origin(conn, _opts) do
    if conn.port == 443,
      do: "https://" <> conn.host,
      else: "https://" <> conn.host <> ":" <> Integer.to_string(conn.port)
  end

  defp json_content_type?(value) do
    case String.split(value, ";", trim: true) |> Enum.map(&String.trim/1) do
      [type] ->
        String.downcase(type) == "application/json"

      [type, charset] ->
        String.downcase(type) == "application/json" and
          String.downcase(charset) == "charset=utf-8"

      _ ->
        false
    end
  end

  defp content_length(value) do
    if Regex.match?(~r/\A(?:0|[1-9][0-9]*)\z/, value),
      do: {:ok, String.to_integer(value)},
      else: {:error, :invalid_upload}
  end

  defp positive_integer(value) do
    if Regex.match?(~r/\A[1-9][0-9]*\z/, value),
      do: {:ok, String.to_integer(value)},
      else: {:error, :invalid_upload}
  end

  defp wire_reply(conn, %{"type" => "error", "reason" => reason} = reply),
    do: json_response(conn, status(reason), reply)

  defp wire_reply(conn, reply), do: json_response(conn, 200, reply)

  defp wire_command_reply(conn, reply), do: json_response(conn, 200, reply)

  defp json_error(conn, status, reason, fields \\ %{}) do
    json_response(
      conn,
      status,
      Map.merge(%{"v" => 1, "type" => "error", "reason" => reason}, fields)
    )
  end

  defp json_response(conn, status, body) do
    conn
    |> put_resp_content_type("application/json", "utf-8")
    |> send_resp(status, JSON.encode!(body))
  end

  defp status(reason) when reason in [:forbidden, :session_ended], do: 403
  defp status(reason) when reason in [:not_found, :unknown_message], do: 404
  defp status(:upload_cap_exceeded), do: 413
  defp status(reason) when reason in [:upload_conflict, :upload_discarded], do: 409
  defp status(:storage_unavailable), do: 503
  defp status("forbidden"), do: 403
  defp status("session_ended"), do: 403
  defp status("not_found"), do: 404
  defp status("unknown_message"), do: 404
  defp status("upload_cap_exceeded"), do: 413
  defp status(reason) when reason in ["upload_conflict", "upload_discarded"], do: 409
  defp status("storage_unavailable"), do: 503
  defp status(_), do: 400

  defp session_redirect(conn, opts) do
    location = if opts.hosted, do: opts.hosted_addresses.sign_in || "/sign-in", else: "/sign-in"

    conn
    |> delete_resp_cookie(@cookie,
      secure: true,
      http_only: true,
      same_site: "Lax",
      path: "/"
    )
    |> put_resp_header("location", location)
    |> send_resp(303, "")
  end

  defp backend_call({module, context}, operation, value),
    do: apply(module, operation, [context, value])

  defp backend_call(module, operation, value) when is_atom(module),
    do: apply(module, operation, [value])
end
