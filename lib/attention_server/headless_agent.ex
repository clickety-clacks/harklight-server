defmodule AttentionServer.HeadlessAgent do
  @moduledoc "Local agent service-key generation and certificate fingerprints."

  alias AttentionServer.DeviceIdentity

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
