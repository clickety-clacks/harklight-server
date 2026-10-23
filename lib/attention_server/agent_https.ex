defmodule AttentionServer.AgentHTTPS do
  @moduledoc """
  Separate mutual-TLS HTTPS endpoint for the owner-enrolled agent principal.
  Each connection carries one bounded `POST /agent/v1` JSON request and closes.
  TLS enrollment is proved before any HTTP request is read or processed.
  """

  use GenServer

  alias AttentionServer.{AgentWire, DeviceIdentity, Diagnostics, EnrolledLeafVerify, JSON, Relay}

  @timeout 5_000
  @max_header 16 * 1024
  @max_body 2 * 1024 * 1024

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
  def port(server), do: GenServer.call(server, :port)

  @impl true
  def init(opts) do
    with {:ok, relay} <-
           Diagnostics.result(
             :agent_https,
             :configuration,
             Keyword.fetch(opts, :relay),
             :unavailable
           ),
         {:ok, certfile} <-
           Diagnostics.result(
             :agent_https,
             :configuration,
             Keyword.fetch(opts, :certfile),
             :unavailable
           ),
         {:ok, keyfile} <-
           Diagnostics.result(
             :agent_https,
             :configuration,
             Keyword.fetch(opts, :keyfile),
             :unavailable
           ),
         {:ok, cacertfile} <-
           Diagnostics.result(
             :agent_https,
             :configuration,
             Keyword.fetch(opts, :cacertfile),
             :unavailable
           ),
         {:ok, listener} <-
           Diagnostics.result(
             :agent_https,
             :listener_start,
             listen(opts, relay, certfile, keyfile, cacertfile),
             :unavailable
           ),
         {:ok, {_ip, port}} <-
           Diagnostics.result(
             :agent_https,
             :listener_address,
             :ssl.sockname(listener),
             :unavailable
           ),
         {:ok, supervisor} <-
           Diagnostics.result(
             :agent_https,
             :worker_start,
             Task.Supervisor.start_link(),
             :unavailable
           ) do
      case Diagnostics.result(
             :agent_https,
             :worker_start,
             Task.Supervisor.start_child(supervisor, fn ->
               accept_loop(listener, relay, supervisor)
             end),
             :unavailable
           ) do
        {:ok, _} ->
          {:ok, %{listener: listener, supervisor: supervisor, port: port}}

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
      verify_fun: EnrolledLeafVerify.agent(relay),
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
                Diagnostics.failure(:agent_https, :socket_control, reason, :closed)
                send(worker, :abort)
                :ssl.close(socket)
            end

          {:error, reason} ->
            Diagnostics.failure(:agent_https, :worker_start, reason, :closed)
            :ssl.close(socket)
        end

        accept_loop(listener, relay, supervisor)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        Diagnostics.failure(:agent_https, :accept, reason, :retry_100ms)
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
               :agent_https,
               :tls_handshake,
               :ssl.handshake(socket, @timeout),
               :closed
             ),
           {:ok, certificate} <-
             Diagnostics.result(:agent_https, :peer_certificate, :ssl.peercert(tls), :closed),
           {:ok, key} <-
             Diagnostics.result(
               :agent_https,
               :identity,
               DeviceIdentity.from_certificate(certificate),
               :closed
             ),
           {:ok, _person, _id, revision} <-
             Diagnostics.result(
               :agent_https,
               :authentication,
               Relay.agent_identity(relay, key),
               :closed
             ) do
        case read_request(tls) do
          {:ok, request} ->
            _ =
              Diagnostics.result(
                :agent_https,
                :request_dispatch,
                Relay.agent_send_https(relay, key, revision, request, tls),
                :automatic,
                Diagnostics.operation_id(request)
              )

          {:error, reason} ->
            {status, message} = transport_error(reason)

            _ =
              Diagnostics.result(
                :agent_https,
                :request_read,
                {:error, reason},
                :closed
              )

            _ =
              Diagnostics.result(
                :agent_https,
                :response_write,
                :ssl.send(tls, AgentWire.http_response(status, AgentWire.error(message))),
                :closed
              )
        end
      end
    rescue
      error ->
        Diagnostics.failure(:agent_https, :relay_boundary, error, :outcome_unknown)
        :ok
    catch
      :exit, reason ->
        Diagnostics.failure(:agent_https, :relay_boundary, reason, :outcome_unknown)
        :ok
    after
      :ssl.close(socket)
    end
  end

  defp transport_error(:method_not_allowed), do: {405, "method_not_allowed"}
  defp transport_error(:not_found), do: {404, "not_found"}
  defp transport_error(:invalid_json), do: {400, "invalid_json"}
  defp transport_error(_), do: {400, "bad_frame"}

  defp read_request(socket) do
    with {:ok, header, remainder} <- read_header(socket, <<>>),
         {:ok, size} <- parse_header(header),
         {:ok, body} <- read_body(socket, remainder, size),
         true <- String.valid?(body) do
      case JSON.decode_line(body <> "\n", @max_body + 1) do
        {:ok, request} when is_map(request) and not is_struct(request) -> {:ok, request}
        _ -> {:error, :invalid_json}
      end
    else
      {:error, :method_not_allowed} = error -> error
      {:error, :not_found} = error -> error
      {:error, :bad_frame} = error -> error
      _ -> {:error, :invalid_json}
    end
  rescue
    error ->
      Diagnostics.failure(:agent_https, :request_read, error, :rejected)
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
          {:ok, chunk} when byte_size(buffer) + byte_size(chunk) <= @max_body + @max_header + 4 ->
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
          [method, _path, "HTTP/1.1"] when method != "POST" ->
            {:error, :method_not_allowed}

          ["POST", path, "HTTP/1.1"] when path != "/agent/v1" ->
            {:error, :not_found}

          ["POST", "/agent/v1", "HTTP/1.1"] ->
            with {:ok, headers} <- headers(fields, %{}),
                 true <- json_content_type?(headers["content-type"]),
                 false <- Map.has_key?(headers, "transfer-encoding"),
                 {:ok, size} <- content_length(headers["content-length"]) do
              {:ok, size}
            else
              _ -> {:error, :bad_frame}
            end

          _ ->
            {:error, :bad_frame}
        end

      _ ->
        {:error, :bad_frame}
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
