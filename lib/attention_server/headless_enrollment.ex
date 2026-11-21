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

  def start(directory, endpoint, label) when is_binary(directory) and is_binary(label) do
    with true <- byte_size(label) >= 1,
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

      {:error, :not_found} ->
        with :ok <- archive_pending(directory) do
          submit_existing(directory, endpoint, label)
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

  @doc """
  Prove the approved local key reaches the device port: one authenticated
  `GET /v2/inbox?limit=1` with the saved identity. The key is admitted and the
  server answers for its person; nothing is acknowledged or stored.
  """
  def verify_access(directory, endpoint) do
    certificate_path = Path.join(directory, "identity.pem")
    key_path = Path.join(directory, "identity.key")

    with {:ok, public_result} <- read_json(Path.join(directory, @result_file)),
         %{"device_id" => expected} when is_binary(expected) <- public_result,
         {:ok, pem} <- File.read(certificate_path),
         [{:Certificate, der, :not_encrypted}] <- :public_key.pem_decode(pem),
         {:ok, ^expected} <- DeviceIdentity.from_certificate(der),
         {:ok, 200, %{"rev" => rev, "items" => items}}
         when is_integer(rev) and is_list(items) <-
           HeadlessTLS.v2_get(endpoint, {certificate_path, key_path}, "/v2/inbox?limit=1") do
      :ok
    else
      _ -> {:error, :access_proof_failed}
    end
  rescue
    _ -> {:error, :access_proof_failed}
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
      valid_expiry_pair?(deadline, status_deadline)
  end

  defp valid_expiry_pair?(nil, nil), do: true

  defp valid_expiry_pair?(deadline, status_deadline)
       when is_integer(deadline) and is_integer(status_deadline) do
    deadline > System.system_time(:millisecond) and status_deadline == deadline + 86_400_000
  end

  defp valid_expiry_pair?(_deadline, _status_deadline), do: false

  defp save_server(directory, endpoint) do
    with {:ok, identity} <- server_identity(endpoint),
         :ok <- write_private(Path.join(directory, @server_file), identity) do
      :ok
    end
  end

  defp verify_server(directory, endpoint) do
    with {:ok, %{} = expected} <- read_json(Path.join(directory, @server_file)),
         {:ok, actual} <- server_identity(endpoint),
         true <- Map.take(expected, ["address", "name", "port", "ca_sha256"]) == actual do
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
