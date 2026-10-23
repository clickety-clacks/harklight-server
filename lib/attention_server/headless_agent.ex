defmodule AttentionServer.HeadlessAgent do
  @moduledoc "Local service-key generation and one-request authenticated agent client."

  alias AttentionServer.{DeviceIdentity, HeadlessTLS}

  def create_identity(directory) when is_binary(directory) do
    with :ok <- File.mkdir(directory),
         :ok <- File.chmod(directory, 0o700),
         :ok <- generate(directory),
         {:ok, fingerprint} <- fingerprint(Path.join(directory, "identity.pem")) do
      {:ok, fingerprint}
    else
      {:error, :eexist} -> {:error, :existing_identity}
      error -> error
    end
  end

  def create_identity(_), do: {:error, :invalid_directory}

  def fingerprint(certificate_path) do
    with {:ok, pem} <- File.read(certificate_path),
         [{:Certificate, der, :not_encrypted}] <- :public_key.pem_decode(pem),
         {:ok, fingerprint} <- DeviceIdentity.from_certificate(der) do
      {:ok, fingerprint}
    else
      _ -> {:error, :invalid_certificate}
    end
  end

  def request(directory, endpoint, request) do
    certificate = Path.join(directory, "identity.pem")
    key = Path.join(directory, "identity.key")

    with {:ok, _fingerprint} <- fingerprint(certificate) do
      wire =
        request
        |> Map.put("v", 1)
        |> then(fn fields ->
          if fields["type"] == "answer", do: Map.put(fields, "type", "respond"), else: fields
        end)
        |> then(fn fields ->
          if Map.has_key?(fields, "contains"),
            do: fields |> Map.put("text", fields["contains"]) |> Map.delete("contains"),
            else: fields
        end)

      case HeadlessTLS.agent_https_request(endpoint, certificate, key, wire) do
        {:ok, 200, %{"type" => "ok"} = response} ->
          {:ok, response}

        {:ok, 200, %{"type" => "error", "reason" => reason} = response} ->
          {:error, {:application, reason, response}}

        {:ok, 200, _} ->
          {:error, :invalid_server_response}

        {:ok, status, response} ->
          {:error, {:http, status, response}}

        error ->
          error
      end
    end
  end

  defp generate(directory) do
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
      "/CN=attention-agent",
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
end
