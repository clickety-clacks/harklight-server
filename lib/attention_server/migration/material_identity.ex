defmodule AttentionServer.Migration.MaterialIdentity do
  @moduledoc false
  alias AttentionServer.DeviceIdentity
  alias AttentionServer.Migration.Frames

  @owner_files ~w(material/person-id material/owner/identity.pem material/owner/identity.key)
  @supported ["material/server.env" | @owner_files]

  # These are typed standard self-host identity paths. Unknown material needs
  # an explicit continuity mapping; bytes and a bundle digest alone are not proof
  # that an identity belongs to the source Server.
  def validate(bundle, manifest) do
    names = Map.keys(manifest.material)
    with true <- Enum.all?(@owner_files, &(&1 in names)),
         true <- Enum.all?(names, &(&1 in @supported)),
         {:ok, person_bytes} <- File.read(Path.join(bundle, "material/person-id")),
         person = String.trim(person_bytes),
         {:ok, fingerprint} <- certificate(Path.join(bundle, "material/owner/identity.pem")),
         {:ok, ^fingerprint} <- private_key(Path.join(bundle, "material/owner/identity.key")),
         {:ok, ^fingerprint} <- owner(bundle, manifest, person) do
      :ok
    else
      false -> {:error, :unsupported_or_missing_identity_material}
      {:ok, _} -> {:error, :material_identity_source_mismatch}
      {:error, _} = error -> error
    end
  end

  defp owner(bundle, manifest, person) do
    Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, nil, fn
      {{:row, :owners, ^person}, %{key: key}}, nil -> {:ok, key}
      {{:row, :owners, ^person}, _}, _ -> {:error, :invalid_source_owner}
      _, found -> {:ok, found}
    end)
  end

  defp certificate(path) do
    with {:ok, pem} <- File.read(path),
         [{:Certificate, der, :not_encrypted}] <- :public_key.pem_decode(pem) do
      DeviceIdentity.from_certificate(der)
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
