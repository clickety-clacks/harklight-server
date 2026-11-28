defmodule AttentionServer.Migration.MaterialIdentity do
  @moduledoc false
  alias AttentionServer.DeviceIdentity
  alias AttentionServer.Migration.{Frames, MaterialProvenance}

  @owner_files ~w(material/person-id material/owner/identity.pem material/owner/identity.key)
  @supported ["material/server.env" | @owner_files]

  # These are typed standard self-host identity paths. Unknown material needs
  # an explicit continuity mapping; bytes and a bundle digest alone are not proof
  # that an identity belongs to the source Server.
  def validate(bundle, manifest) do
    names = Map.keys(manifest.material)
    with {:ok, tls} <- MaterialProvenance.tls_paths(bundle, manifest.material, manifest.material_provenance["files"]),
         supported = @supported ++ Map.values(tls) ++ ["material/tls/ca.key"],
         true <- map_size(tls) == 3,
         true <- Enum.all?(@owner_files, &(&1 in names)),
         true <- Enum.all?(names, &(&1 in supported)),
         {:ok, person_bytes} <- File.read(Path.join(bundle, "material/person-id")),
         person = String.trim(person_bytes),
         {:ok, fingerprint} <- certificate(Path.join(bundle, "material/owner/identity.pem")),
         {:ok, ^fingerprint} <- private_key(Path.join(bundle, "material/owner/identity.key")),
         {:ok, ^fingerprint} <- owner(bundle, manifest, person),
         :ok <- tls_identity(bundle, manifest, tls) do
      :ok
    else
      false -> {:error, :unsupported_or_missing_identity_material}
      {:ok, _} -> {:error, :material_identity_source_mismatch}
      {:error, _} = error -> error
    end
  end

  defp tls_identity(bundle, manifest, tls) do
    cert = Path.join(bundle, tls["cert"])
    key = Path.join(bundle, tls["key"])
    ca = Path.join(bundle, tls["ca"])
    with {:ok, fingerprint} <- certificate(cert),
         {:ok, ^fingerprint} <- private_key(key),
         :ok <- ca_key(bundle, manifest, ca),
         {_, 0} <- System.cmd("openssl", ["verify", "-no_check_time", "-CAfile", ca, cert], stderr_to_stdout: true),
         {:ok, ca_der} <- certificate_der(ca),
         digest = Base.encode16(:crypto.hash(:sha256, ca_der), case: :lower),
         {:ok, :ok} <- invitation_pins(bundle, manifest, digest) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :material_tls_identity_mismatch}
    end
  rescue
    _ -> {:error, :material_tls_identity_mismatch}
  end

  defp ca_key(bundle, manifest, ca) do
    if Map.has_key?(manifest.material, "material/tls/ca.key") do
      with {:ok, fingerprint} <- certificate(ca),
           {:ok, ^fingerprint} <- private_key(Path.join(bundle, "material/tls/ca.key")) do
        :ok
      else
        _ -> {:error, :material_tls_identity_mismatch}
      end
    else
      :ok
    end
  end

  defp invitation_pins(bundle, manifest, digest) do
    Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, :ok, fn
      {{:row, :join_invitations, _}, %{configuration: configuration}}, :ok ->
        case Map.fetch(configuration, "ca_sha256") do
          :error -> {:ok, :ok}
          {:ok, ^digest} -> {:ok, :ok}
          _ -> {:error, :material_invitation_ca_mismatch}
        end
      {{:row, :join_invitations, _}, _}, :ok -> {:error, :invalid_source_invitation}
      _, :ok -> {:ok, :ok}
    end)
  end

  defp owner(bundle, manifest, person) do
    Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, nil, fn
      {{:row, :owners, ^person}, %{key: key}}, nil -> {:ok, key}
      {{:row, :owners, ^person}, _}, _ -> {:error, :invalid_source_owner}
      _, found -> {:ok, found}
    end)
  end

  defp certificate(path) do
    with {:ok, der} <- certificate_der(path), do: DeviceIdentity.from_certificate(der)
  end

  defp certificate_der(path) do
    with {:ok, pem} <- File.read(path),
         [{:Certificate, der, :not_encrypted}] <- :public_key.pem_decode(pem) do
      {:ok, der}
    else
      _ -> {:error, :invalid_material_certificate}
    end
  rescue
    _ -> {:error, :invalid_material_certificate}
  end

  defp private_key(path) do
    case System.cmd("openssl", ["pkey", "-in", path, "-pubout", "-outform", "DER"], stderr_to_stdout: true) do
      {spki, 0} -> {:ok, Base.encode16(:crypto.hash(:sha256, spki), case: :lower)}
      _ -> {:error, :invalid_material_private_key}
    end
  rescue
    _ -> {:error, :material_key_tool_unavailable}
  end
end
