defmodule AttentionServer.HeadlessEnrollment do
  @moduledoc """
  Headless requesting-device enrollment. The private key is generated and
  retained in a new local 0700 directory; the server receives only its public
  certificate. A status secret is stored locally and sent only in a POST body.
  """

  alias AttentionServer.{DeviceIdentity, HeadlessTLS, JSON}

  @pending_file "pending.json"
  @result_file "enrollment.json"
  @server_file "server.json"
  # Replayed delivery and terminal frames can pair two bounded request inputs.
  @max_server_frame 2 * (2 * 1024 * 1024) + 16_384

  def start(directory, endpoint, label) when is_binary(directory) and is_binary(label) do
    with true <- byte_size(label) in 1..256,
         :ok <- create_directory(directory),
         :ok <- save_server(directory, endpoint),
         :ok <- generate_identity(directory),
         {:ok, result} <- submit_existing(directory, endpoint, label) do
      {:ok, result}
    else
      false -> {:error, :invalid_label}
      error -> error
    end
  end

  def start(_, _, _), do: {:error, :invalid_request}

  @doc "Warn before a locally held certificate reaches its final 30 days."
  def certificate_lifecycle(directory),
    do: HeadlessTLS.certificate_lifecycle(Path.join(directory, "identity.pem"))

  @doc "Retry submission with the same locally held key after a transport failure."
  def submit_existing(directory, endpoint, label) do
    with :ok <- verify_server(directory, endpoint),
         {:error, :enoent} <- File.read(Path.join(directory, @pending_file)),
         {:ok, certificate} <- File.read(Path.join(directory, "identity.pem")),
         {:ok, 200,
          %{
            "approval_code" => code,
            "status_secret" => secret,
            "expires_at" => deadline,
            "status_expires_at" => status_deadline
          }} <-
           HeadlessTLS.post_json(endpoint, "/enroll", %{
             "public_certificate" => certificate,
             "label" => label
           }),
         true <- valid_response?(code, secret, deadline, status_deadline),
         :ok <-
           write_private(Path.join(directory, @pending_file), %{
             "approval_code" => code,
             "status_secret" => secret,
             "expires_at" => deadline,
             "status_expires_at" => status_deadline
           }) do
      {:ok, %{approval_code: code, expires_at: deadline}}
    else
      {:ok, existing} when is_binary(existing) -> retry_saved(directory, endpoint, label)
      {:ok, 409, %{"error" => "key_pending"}} -> {:error, :key_pending}
      {:ok, 409, %{"error" => "key_in_use"}} -> {:error, :key_in_use}
      {:ok, 503, %{"error" => "capacity"}} -> {:error, :capacity}
      {:error, :wrong_server} -> {:error, :wrong_server}
      {:error, :enoent} -> {:error, :missing_identity}
      _ -> {:error, :indeterminate_submission}
    end
  end

  defp retry_saved(directory, endpoint, label) do
    case status(directory, endpoint) do
      {:ok, %{"state" => "expired"}} ->
        with :ok <- archive_pending(directory) do
          submit_existing(directory, endpoint, label)
        end

      {:error, :not_found} = result ->
        case read_json(Path.join(directory, @pending_file)) do
          {:ok, %{"expires_at" => deadline}} when is_integer(deadline) ->
            if deadline <= System.system_time(:millisecond) do
              with :ok <- archive_pending(directory) do
                submit_existing(directory, endpoint, label)
              end
            else
              result
            end

          _ ->
            result
        end

      other ->
        other
    end
  end

  defp archive_pending(directory) do
    source = Path.join(directory, @pending_file)

    with {:ok, %{"approval_code" => code}} <- read_json(source),
         true <- is_binary(code) and Regex.match?(~r/\A[A-Z2-7]{10}\z/, code),
         destination = Path.join(directory, "pending-#{code}.json"),
         false <- File.exists?(destination),
         :ok <- File.rename(source, destination),
         :ok <- sync_directory(destination) do
      :ok
    else
      _ -> {:error, :pending_archive_failed}
    end
  end

  def status(directory, endpoint) do
    with :ok <- verify_server(directory, endpoint),
         {:ok, pending} <- read_json(Path.join(directory, @pending_file)),
         %{"status_secret" => secret} when is_binary(secret) <- pending,
         {:ok, 200, result} <-
           HeadlessTLS.post_json(endpoint, "/enroll/status", %{"status_secret" => secret}) do
      case result do
        %{"state" => "approved", "person_id" => person, "device_id" => device} ->
          with :ok <-
                 write_private(Path.join(directory, @result_file), %{
                   "person_id" => person,
                   "device_id" => device
                 }) do
            {:ok, result}
          end

        %{"state" => state} when state in ["pending", "expired", "revoked"] ->
          {:ok, result}

        _ ->
          {:error, :invalid_server_response}
      end
    else
      {:ok, 404, _} -> {:error, :not_found}
      {:ok, _, _} -> {:error, :server_rejected}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :invalid_pending_state}
    end
  end

  @doc "Prove the approved local key reaches /relay and its first replay barrier; never ACK unseen content."
  def verify_relay(directory, endpoint) do
    certificate_path = Path.join(directory, "identity.pem")
    key_path = Path.join(directory, "identity.key")

    with {:ok, public_result} <- read_json(Path.join(directory, @result_file)),
         %{"device_id" => expected} when is_binary(expected) <- public_result,
         {:ok, pem} <- File.read(certificate_path),
         [{:Certificate, der, :not_encrypted}] <- :public_key.pem_decode(pem),
         {:ok, ^expected} <- DeviceIdentity.from_certificate(der),
         {:ok, socket} <- HeadlessTLS.connect(endpoint, {certificate_path, key_path}) do
      try do
        with :ok <- upgrade(socket, endpoint.name),
             :ok <- send_resync(socket),
             {:ok, %{"type" => "ok"}} <- read_frame(socket),
             {:ok, %{"type" => "resynced"}} <- read_barrier(socket, 0) do
          :ok
        end
      after
        :ssl.close(socket)
      end
    else
      _ -> {:error, :relay_proof_failed}
    end
  rescue
    _ -> {:error, :relay_proof_failed}
  end

  defp upgrade(socket, name) do
    key = :crypto.strong_rand_bytes(16) |> Base.encode64()

    request = [
      "GET /relay HTTP/1.1\r\n",
      "Host: #{name}\r\n",
      "Upgrade: websocket\r\n",
      "Connection: Upgrade\r\n",
      "Sec-WebSocket-Version: 13\r\n",
      "Sec-WebSocket-Key: #{key}\r\n",
      "Sec-WebSocket-Protocol: attention-relay/1\r\n\r\n"
    ]

    with :ok <- :ssl.send(socket, request),
         {:ok, headers} <- read_headers(socket, <<>>),
         true <- String.starts_with?(headers, "HTTP/1.1 101 "),
         true <-
           String.contains?(
             String.downcase(headers),
             "sec-websocket-protocol: attention-relay/1\r\n"
           ),
         accept <-
           Base.encode64(:crypto.hash(:sha, key <> "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")),
         true <- String.contains?(headers, "Sec-WebSocket-Accept: #{accept}\r\n") do
      :ok
    else
      _ -> {:error, :bad_upgrade}
    end
  end

  defp read_headers(_socket, bytes) when byte_size(bytes) > 16_384,
    do: {:error, :headers_too_large}

  defp read_headers(socket, bytes) do
    if String.ends_with?(bytes, "\r\n\r\n") do
      {:ok, bytes}
    else
      case :ssl.recv(socket, 1, 3_000) do
        {:ok, byte} -> read_headers(socket, bytes <> byte)
        error -> error
      end
    end
  end

  defp send_resync(socket) do
    payload = JSON.encode!(%{"v" => 1, "type" => "resync", "since" => 0}) <> "\n"
    size = byte_size(payload)
    mask = :crypto.strong_rand_bytes(4)
    repeated = :binary.copy(mask, div(size + 3, 4))
    masked = :crypto.exor(payload, binary_part(repeated, 0, size))
    :ssl.send(socket, [<<1::1, 0::3, 1::4, 1::1, size::7>>, mask, masked])
  end

  defp read_frame(socket) do
    with {:ok, <<1::1, 0::3, 1::4, 0::1, length::7>>} <- :ssl.recv(socket, 2, 3_000),
         {:ok, size} <- frame_size(socket, length),
         true <- size in 1..@max_server_frame,
         {:ok, payload} <- :ssl.recv(socket, size, 3_000),
         {:ok, value} <- JSON.decode_line(payload, @max_server_frame) do
      {:ok, value}
    else
      _ -> {:error, :bad_relay_frame}
    end
  end

  defp frame_size(_socket, size) when size < 126, do: {:ok, size}

  defp frame_size(socket, 126) do
    with {:ok, <<size::16>>} <- :ssl.recv(socket, 2, 3_000), do: {:ok, size}
  end

  defp frame_size(socket, 127) do
    with {:ok, <<size::64>>} <- :ssl.recv(socket, 8, 3_000), do: {:ok, size}
  end

  defp read_barrier(_socket, count) when count >= 1_024, do: {:error, :too_many_events}

  defp read_barrier(socket, count) do
    case read_frame(socket) do
      {:ok, %{"type" => "resynced"} = barrier} ->
        {:ok, barrier}

      {:ok, %{"type" => type}} when type in ["delivery", "lifecycle"] ->
        read_barrier(socket, count + 1)

      _ ->
        {:error, :bad_replay}
    end
  end

  defp create_directory(directory) do
    case File.mkdir(directory) do
      :ok -> File.chmod(directory, 0o700)
      {:error, :eexist} -> {:error, :existing_identity}
      error -> error
    end
  end

  defp generate_identity(directory) do
    args = [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-sha256",
      "-days",
      "365",
      "-keyout",
      "identity.key",
      "-out",
      "identity.pem",
      "-subj",
      "/CN=attention-device",
      "-addext",
      "basicConstraints=critical,CA:FALSE",
      "-addext",
      "keyUsage=critical,digitalSignature",
      "-addext",
      "extendedKeyUsage=clientAuth"
    ]

    case System.cmd("openssl", args, cd: directory, stderr_to_stdout: true) do
      {_, 0} -> File.chmod(Path.join(directory, "identity.key"), 0o600)
      _ -> {:error, :key_generation_failed}
    end
  rescue
    _ -> {:error, :key_generation_failed}
  end

  defp write_private(path, value) do
    temporary = path <> ".new-" <> Integer.to_string(System.unique_integer([:positive]))

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
          case File.rename(temporary, path) do
            :ok ->
              sync_directory(path)

            error ->
              _ = File.rm(temporary)
              error
          end

        error ->
          _ = File.rm(temporary)
          error
      end
    end
  end

  defp sync_directory(path) do
    case System.cmd("sync", [Path.dirname(path)], stderr_to_stdout: true) do
      {_, 0} -> :ok
      _ -> {:error, :sync_failed}
    end
  rescue
    _ -> {:error, :sync_failed}
  end

  defp read_json(path) do
    with {:ok, line} <- File.read(path),
         {:ok, value} <- JSON.decode_line(line) do
      {:ok, value}
    end
  end

  defp valid_response?(code, secret, deadline, status_deadline) do
    is_binary(code) and Regex.match?(~r/\A[A-Z2-7]{10}\z/, code) and
      is_binary(secret) and Regex.match?(~r/\A[0-9a-f]{64}\z/, secret) and
      is_integer(deadline) and deadline > System.system_time(:millisecond) and
      is_integer(status_deadline) and status_deadline == deadline + 86_400_000
  end

  defp save_server(directory, endpoint) do
    with {:ok, identity} <- server_identity(endpoint),
         :ok <- write_private(Path.join(directory, @server_file), identity) do
      :ok
    end
  end

  defp verify_server(directory, endpoint) do
    with {:ok, expected} <- read_json(Path.join(directory, @server_file)),
         {:ok, actual} <- server_identity(endpoint),
         true <- expected == actual do
      :ok
    else
      _ -> {:error, :wrong_server}
    end
  end

  defp server_identity(%{address: address, name: name, port: port, ca_file: ca}) do
    with {:ok, ca_bytes} <- File.read(ca) do
      host =
        if is_tuple(address),
          do: address |> :inet.ntoa() |> List.to_string(),
          else: List.to_string(address)

      {:ok,
       %{
         "address" => host,
         "name" => name,
         "port" => port,
         "ca_sha256" => :crypto.hash(:sha256, ca_bytes) |> Base.encode16(case: :lower)
       }}
    end
  end
end
