defmodule AttentionServer.Migration.VerifyMaterial do
  @moduledoc false
  import Bitwise
  alias AttentionServer.JSON
  alias AttentionServer.Migration.{Bundle, ImportMaterial, MaterialIdentity, Source}

  # Unlike ImportMaterial.run, validation never repairs a missing file, changes
  # a mode, claims a directory or publishes a completion marker.
  def run(bundle, %{id: id, source: digest, schema: schema}, destination) do
    with {:ok, manifest, summary} <- Bundle.verify(bundle),
         true <- digest == Base.decode16!(summary.sha256, case: :lower),
         :ok <- MaterialIdentity.validate(bundle, manifest),
         files = Enum.map(Enum.sort(manifest.material), fn {"material/" <> path, info} ->
           %{"path" => path, "size" => info.size, "sha256" => info.sha256}
         end),
         bytes = JSON.encode!(ImportMaterial.inventory(id, summary.sha256, schema, files)),
         :ok <- private_mode(destination, :directory, 0o700),
         :ok <- marker(destination, "OWNER.json", bytes),
         :ok <- marker(destination, "COMPLETE.json", bytes),
         {:ok, actual} <- tree(destination, "", MapSet.new()),
         true <- actual == expected_tree(files),
         :ok <- each(files, fn file -> content(Path.join([destination, "files", file["path"]]), file) end),
         {:ok, ^manifest, ^summary} <- Bundle.verify(bundle) do
      {:ok, %{"root" => Path.expand(Path.join(destination, "files")),
        "manifest_sha256" => Base.encode16(:crypto.hash(:sha256, bytes), case: :lower),
        "files" => files}}
    else
      false -> {:error, :material_source_or_inventory_mismatch}
      {:error, _} = error -> error
      _ -> {:error, :material_verification_failed}
    end
  end

  defp marker(root, name, expected) do
    path = Path.join(root, name)
    with :ok <- private_mode(path, :regular, 0o600),
         {:ok, ^expected} <- File.read(path) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :material_identity_mismatch}
    end
  end

  defp expected_tree(files) do
    Enum.reduce(files, MapSet.new(["OWNER.json", "COMPLETE.json", "files"]), fn file, acc ->
      path = Path.join("files", file["path"])
      parents(path, MapSet.put(acc, path))
    end)
  end
  defp parents("files", acc), do: acc
  defp parents(path, acc) do
    parent = Path.dirname(path)
    parents(parent, MapSet.put(acc, parent))
  end

  defp tree(root, relative, found) do
    with {:ok, names} <- File.ls(Path.join(root, relative)) do
      Enum.reduce_while(names, {:ok, found}, fn name, {:ok, acc} ->
        relative_path = Path.join(relative, name)
        path = Path.join(root, relative_path)
        result = case File.lstat(path) do
          {:ok, %{type: :directory}} ->
            with :ok <- private_mode(path, :directory, 0o700),
              do: tree(root, relative_path, MapSet.put(acc, relative_path))
          {:ok, %{type: :regular}} ->
            with :ok <- private_mode(path, :regular, 0o600),
              do: {:ok, MapSet.put(acc, relative_path)}
          _ -> {:error, :material_permissions_mismatch}
        end
        case result do
          {:ok, _} -> {:cont, result}
          error -> {:halt, error}
        end
      end)
    end
  end

  defp content(path, %{"size" => size, "sha256" => digest}) do
    case Source.file_observation(path) do
      {:ok, %{size: ^size, sha256: ^digest}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, :material_content_mismatch}
    end
  end
  defp private_mode(path, type, mode) do
    case File.lstat(path) do
      {:ok, stat} when stat.type == type ->
        if (stat.mode &&& 0o777) == mode, do: :ok, else: {:error, :material_permissions_mismatch}
      _ -> {:error, :material_permissions_mismatch}
    end
  end
  defp each(rows, fun), do: Enum.reduce_while(rows, :ok, fn row, :ok ->
    case fun.(row) do
      :ok -> {:cont, :ok}
      error -> {:halt, error}
    end
  end)
end
