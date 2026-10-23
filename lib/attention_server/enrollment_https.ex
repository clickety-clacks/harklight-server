defmodule AttentionServer.EnrollmentHTTPS do
  @moduledoc """
  Narrow TLS-only pending enrollment and exact own-device cleanup service.

  Each connection accepts one bounded HTTP/1.1 JSON POST, sends one response,
  and closes. Pending status/cancellation uses the submitter's 256-bit secret;
  removal alone requires a TLS-proved retained device SPKI. Neither exposes an
  owner operation, other-device mutation or relay traffic.
  """

  use GenServer

  alias AttentionServer.{DeviceIdentity, Diagnostics, EnrolledLeafVerify, JSON, Relay}

  @timeout 3_000
  @max_header 8 * 1024
  @max_body 32 * 1024
  @max_certificate 16 * 1024

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
  def port(server), do: GenServer.call(server, :port)

  @impl true
  def init(opts) do
    with {:ok, relay} <-
           Diagnostics.result(
             :enrollment_https,
             :configuration,
             Keyword.fetch(opts, :relay),
             :unavailable
           ),
         {:ok, certfile} <-
           Diagnostics.result(
             :enrollment_https,
             :configuration,
             Keyword.fetch(opts, :certfile),
             :unavailable
           ),
         {:ok, keyfile} <-
           Diagnostics.result(
             :enrollment_https,
             :configuration,
             Keyword.fetch(opts, :keyfile),
             :unavailable
           ),
         {:ok, listener} <-
           Diagnostics.result(
             :enrollment_https,
             :listener_start,
             listen(opts, certfile, keyfile),
             :unavailable
           ),
         {:ok, {_ip, port}} <-
           Diagnostics.result(
             :enrollment_https,
             :listener_address,
             :ssl.sockname(listener),
             :unavailable
           ),
         {:ok, supervisor} <-
           Diagnostics.result(
             :enrollment_https,
             :worker_start,
             Task.Supervisor.start_link(),
             :unavailable
           ) do
      case Diagnostics.result(
             :enrollment_https,
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

  defp listen(opts, certfile, keyfile) do
    # Enrollment remains usable without a client certificate. If offered, TLS
    # proves possession; removal alone uses the retained exact device identity.
    peer_options =
      case Keyword.get(opts, :cacertfile) do
        nil ->
          [verify: :verify_none]

        ca ->
          [
            verify: :verify_peer,
            fail_if_no_peer_cert: false,
            cacertfile: String.to_charlist(ca),
            verify_fun: EnrolledLeafVerify.removal(Keyword.fetch!(opts, :relay))
          ]
      end

    :ssl.listen(
      Keyword.get(opts, :port, 0),
      peer_options ++
        [
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
          session_tickets: :disabled,
          early_data: :disabled
        ]
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
                Diagnostics.failure(:enrollment_https, :socket_control, reason, :closed)
                send(worker, :abort)
                :ssl.close(socket)
            end

          {:error, reason} ->
            Diagnostics.failure(:enrollment_https, :worker_start, reason, :closed)
            :ssl.close(socket)
        end

        accept_loop(listener, relay, supervisor)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        Diagnostics.failure(:enrollment_https, :accept, reason, :retry_100ms)
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
               :enrollment_https,
               :tls_handshake,
               :ssl.handshake(socket, @timeout),
               :closed
             ) do
        {status, response, operation_id} =
          case read_request(tls) do
            {:ok, "/device/remove", body} ->
              {status, response} = remove_device(relay, tls, body)
              {status, response, Diagnostics.operation_id(body)}

            {:ok, path, body} ->
              {status, response} = dispatch(relay, path, body)
              {status, response, Diagnostics.operation_id(body)}

            {:error, :not_found} ->
              {404, %{"error" => "not_found"}, nil}

            {:error, reason} ->
              Diagnostics.failure(:enrollment_https, :request_read, reason, :rejected)
              {400, %{"error" => "bad_request"}, nil}
          end

        Diagnostics.response_error(
          :enrollment_https,
          :request_dispatch,
          response,
          operation_id
        )

        _ =
          Diagnostics.result(
            :enrollment_https,
            :response_write,
            send_json(tls, status, response),
            :outcome_unknown,
            operation_id
          )
      end
    rescue
      error ->
        Diagnostics.failure(:enrollment_https, :relay_boundary, error, :outcome_unknown)
        :ok
    catch
      :exit, reason ->
        Diagnostics.failure(:enrollment_https, :relay_boundary, reason, :outcome_unknown)
        :ok
    after
      :ssl.close(socket)
    end
  end

  defp read_request(socket) do
    try do
      with {:ok, header, remainder} <- read_header(socket, <<>>),
           {:ok, path, size} <- parse_header(header),
           {:ok, body} <- read_body(socket, remainder, size),
           {:ok, request} <- JSON.decode_line(body <> "\n") do
        {:ok, path, request}
      else
        {:error, reason} ->
          Diagnostics.failure(:enrollment_https, :request_read, reason, :rejected)
          {:error, :bad_request}

        other ->
          Diagnostics.failure(:enrollment_https, :request_read, other, :rejected)
          {:error, :bad_request}
      end
    rescue
      error ->
        Diagnostics.failure(:enrollment_https, :request_read, error, :rejected)
        {:error, :bad_request}
    end
  end

  defp read_header(socket, buffer) do
    case :binary.match(buffer, "\r\n\r\n") do
      {offset, 4} when offset <= @max_header ->
        {:ok, binary_part(buffer, 0, offset),
         binary_part(buffer, offset + 4, byte_size(buffer) - offset - 4)}

      {_, 4} ->
        {:error, :too_large}

      :nomatch when byte_size(buffer) > @max_header ->
        {:error, :too_large}

      :nomatch ->
        case :ssl.recv(socket, 0, @timeout) do
          {:ok, chunk} -> read_header(socket, buffer <> chunk)
          {:error, reason} -> {:error, reason}
          other -> {:error, other}
        end
    end
  end

  defp parse_header(header) do
    case String.split(header, "\r\n") do
      ["POST " <> rest | fields] ->
        with [path, "HTTP/1.1"] <- String.split(rest, " ", trim: true),
             true <-
               path in [
                 "/enroll",
                 "/enroll/status",
                 "/enroll/resume",
                 "/enroll/cancel",
                 "/device/remove"
               ],
             {:ok, headers} <- headers(fields, %{}),
             "application/json" <- Map.get(headers, "content-type"),
             size when is_integer(size) and size in 1..@max_body <-
               content_length(Map.get(headers, "content-length")),
             false <- Map.has_key?(headers, "transfer-encoding") do
          {:ok, path, size}
        else
          _ -> {:error, :bad_request}
        end

      _ ->
        {:error, :bad_request}
    end
  end

  defp headers([], found), do: {:ok, found}

  defp headers([field | rest], found) do
    case :binary.split(field, ":") do
      [name, value] ->
        key = String.downcase(name)

        if key != "" and not Map.has_key?(found, key),
          do: headers(rest, Map.put(found, key, String.trim(value))),
          else: {:error, :bad_header}

      _ ->
        {:error, :bad_header}
    end
  end

  defp content_length(value) when is_binary(value) do
    if Regex.match?(~r/\A[1-9][0-9]{0,5}\z/, value), do: String.to_integer(value), else: nil
  end

  defp content_length(_), do: nil

  defp read_body(_socket, buffer, size) when byte_size(buffer) >= size,
    do: {:ok, binary_part(buffer, 0, size)}

  defp read_body(socket, buffer, size) do
    case :ssl.recv(socket, 0, @timeout) do
      {:ok, chunk} -> read_body(socket, buffer <> chunk, size)
      {:error, reason} -> {:error, reason}
      other -> {:error, other}
    end
  end

  defp dispatch(
         relay,
         "/enroll",
         %{"invite_token" => token, "public_certificate" => pem, "status_secret" => secret} =
           request
       )
       when is_binary(pem) and byte_size(pem) <= @max_certificate do
    if Enum.sort(Map.keys(request)) in [
         ~w(invite_token public_certificate status_secret),
         ~w(invite_token label public_certificate status_secret)
       ] do
      with {:ok, der} <- certificate_der(pem),
           {:ok, result} <-
             Diagnostics.result(
               :enrollment_https,
               :request_dispatch,
               Relay.invited_enroll(
                 relay,
                 token,
                 der,
                 Map.get(request, "label", "New device"),
                 secret
               ),
               :automatic
             ) do
        {200, result}
      else
        {:error, :expired} -> {410, %{"error" => "expired"}}
        {:error, :used} -> {409, %{"error" => "used"}}
        {:error, :key_pending} -> {409, %{"error" => "key_pending"}}
        {:error, :key_in_use} -> {409, %{"error" => "key_in_use"}}
        {:error, :capacity} -> {503, %{"error" => "capacity"}}
        {:error, :storage_unavailable} -> {503, %{"error" => "unavailable"}}
        {:error, :refused} -> {404, %{"error" => "refused"}}
        _ -> {400, %{"error" => "invalid_request"}}
      end
    else
      {400, %{"error" => "invalid_request"}}
    end
  end

  defp dispatch(relay, "/enroll", %{"public_certificate" => pem, "label" => label} = request)
       when map_size(request) == 2 and is_binary(pem) and is_binary(label) and
              byte_size(pem) <= @max_certificate do
    with {:ok, der} <- certificate_der(pem),
         {:ok, result} <-
           Diagnostics.result(
             :enrollment_https,
             :request_dispatch,
             Relay.pending_enroll(relay, der, label),
             :automatic
           ) do
      {200,
       %{
         "approval_code" => result.approval_code,
         "status_secret" => result.status_secret,
         "expires_at" => result.expires_at,
         "status_expires_at" => result.status_expires_at
       }}
    else
      {:error, :capacity} -> {503, %{"error" => "capacity"}}
      {:error, :key_pending} -> {409, %{"error" => "key_pending"}}
      {:error, :key_in_use} -> {409, %{"error" => "key_in_use"}}
      {:error, :clock_unavailable} -> {503, %{"error" => "unavailable"}}
      {:error, :storage_unavailable} -> {503, %{"error" => "unavailable"}}
      _ -> {400, %{"error" => "invalid_request"}}
    end
  end

  defp dispatch(
         relay,
         "/enroll/cancel",
         %{
           "status_secret" => secret,
           "approval_code" => code,
           "expected_device_id" => key,
           "expected_expires_at" => deadline
         } = request
       )
       when map_size(request) == 4 and is_binary(secret) and is_binary(code) and
              is_binary(key) and is_integer(deadline) do
    case Diagnostics.result(
           :enrollment_https,
           :request_dispatch,
           Relay.pending_cancel(relay, secret, code, key, deadline),
           :automatic
         ) do
      {:ok, status} ->
        {200, status}

      {:error, :stale_request, current} ->
        {409, %{"error" => "stale_request", "current" => current}}

      {:error, :storage_unavailable} ->
        {503, %{"error" => "unavailable"}}

      _ ->
        {404, %{"error" => "not_found"}}
    end
  end

  defp dispatch(relay, "/enroll/status", %{"status_secret" => secret} = request)
       when map_size(request) == 1 and is_binary(secret) do
    case Diagnostics.result(
           :enrollment_https,
           :request_dispatch,
           Relay.pending_status(relay, secret),
           :automatic
         ) do
      {:ok, status} -> {200, status}
      {:error, :storage_unavailable} -> {503, %{"error" => "unavailable"}}
      _ -> {404, %{"error" => "not_found"}}
    end
  end

  defp dispatch(_relay, "/enroll/status", _request), do: {404, %{"error" => "not_found"}}

  defp dispatch(
         relay,
         "/enroll/resume",
         %{"status_secret" => secret, "public_certificate" => pem} = request
       )
       when is_binary(secret) and is_binary(pem) and byte_size(pem) <= @max_certificate do
    label = Map.get(request, "label")

    if Enum.sort(Map.keys(request)) in [
         ~w(public_certificate status_secret),
         ~w(label public_certificate status_secret)
       ] and
         ((is_nil(label) and not Map.has_key?(request, "label")) or
            (is_binary(label) and byte_size(label) in 1..256)) do
      with {:ok, der} <- certificate_der(pem),
           {:ok, _key} <- DeviceIdentity.from_certificate(der),
           {:ok, result} <-
             Diagnostics.result(
               :enrollment_https,
               :request_dispatch,
               Relay.pending_resume(relay, secret, der),
               :automatic
             ) do
        {200, result}
      else
        {:error, :invalid_certificate} -> {400, %{"error" => "invalid_request"}}
        {:error, :invalid_request} -> {400, %{"error" => "invalid_request"}}
        {:error, :conflict} -> {409, %{"error" => "conflict"}}
        {:error, :storage_unavailable} -> {503, %{"error" => "unavailable"}}
        {:error, :clock_unavailable} -> {503, %{"error" => "unavailable"}}
        _ -> {404, %{"error" => "not_found"}}
      end
    else
      {400, %{"error" => "invalid_request"}}
    end
  end

  defp dispatch(_relay, "/enroll/resume", _request), do: {400, %{"error" => "invalid_request"}}

  defp dispatch(_relay, _path, _request), do: {400, %{"error" => "invalid_request"}}

  defp remove_device(relay, socket, %{"expected_device_id" => expected} = request)
       when map_size(request) == 1 and is_binary(expected) do
    with {:ok, cert} <- :ssl.peercert(socket),
         {:ok, key} <- DeviceIdentity.from_certificate(cert),
         true <- key == expected,
         {:ok, status} <-
           Diagnostics.result(
             :enrollment_https,
             :request_dispatch,
             Relay.device_remove(relay, key),
             :automatic
           ) do
      {200, status}
    else
      {:error, :storage_unavailable} -> {503, %{"error" => "unavailable"}}
      _ -> {404, %{"error" => "not_found"}}
    end
  end

  defp remove_device(_relay, _socket, _request), do: {400, %{"error" => "invalid_request"}}

  defp certificate_der(pem) do
    try do
      case :public_key.pem_decode(pem) do
        [{:Certificate, der, :not_encrypted}] when byte_size(der) <= @max_certificate ->
          {:ok, der}

        _ ->
          {:error, :invalid_certificate}
      end
    rescue
      error ->
        Diagnostics.failure(:enrollment_https, :identity, error, :rejected)
        {:error, :invalid_certificate}
    end
  end

  defp send_json(socket, status, value) do
    body = JSON.encode!(value)

    reason =
      case status do
        200 -> "OK"
        400 -> "Bad Request"
        404 -> "Not Found"
        409 -> "Conflict"
        410 -> "Gone"
        503 -> "Service Unavailable"
      end

    :ssl.send(socket, [
      "HTTP/1.1 #{status} #{reason}\r\n",
      "Content-Type: application/json\r\n",
      "Content-Length: #{byte_size(body)}\r\n",
      "Cache-Control: no-store\r\n",
      "Connection: close\r\n\r\n",
      body
    ])
  end
end
