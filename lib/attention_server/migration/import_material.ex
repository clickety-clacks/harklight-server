defmodule AttentionServer.Migration.ImportMaterial do
  @moduledoc false
  import Bitwise
  alias AttentionServer.Migration.{Bundle, Source}
  alias AttentionServer.JSON

  # Private staging only: never writes to the original state or starts Server.
  # A complete result is input to final validation, not permission to activate.
  def run(bundle, %{id: id, source: digest, schema: schema}, destination) do
    with {:ok, manifest, summary} <- Bundle.verify(bundle),
         true <- digest == Base.decode16!(summary.sha256, case: :lower),
         :ok <- AttentionServer.Migration.MaterialIdentity.validate(bundle, manifest),
         files = Enum.map(Enum.sort(manifest.material), fn {"material/" <> path, info} ->
           %{"path" => path, "size" => info.size, "sha256" => info.sha256}
         end),
         inventory = %{"version" => 1, "import_id" => id,
           "source_sha256" => summary.sha256,
           "schema_sha256" => Base.encode16(schema, case: :lower), "files" => files},
         bytes = JSON.encode!(inventory),
         :ok <- claim(destination, bytes),
         :ok <- private_parents(Path.join(destination, "files")),
         :ok <- copy_files(bundle, destination, files),
         :ok <- private_mode(destination, :directory, 0o700),
         :ok <- publish(Path.join(destination, "COMPLETE.json"), bytes) do
      {:ok, %{"root" => Path.expand(Path.join(destination, "files")),
        "manifest_sha256" => Base.encode16(:crypto.hash(:sha256, bytes), case: :lower),
        "files" => files}}
    else
      false -> {:error, :import_source_mismatch}
      {:error, _} = error -> error
    end
  rescue
    File.Error -> {:error, :material_file_failure}
  end

  defp claim(root, bytes) do
    case File.mkdir(root) do
      :ok ->
        with :ok <- File.chmod(root, 0o700), do: publish(Path.join(root, "OWNER.json"), bytes)
      {:error, :eexist} ->
        with :ok <- private_mode(root, :directory, 0o700) do
        case File.read(Path.join(root, "OWNER.json")) do
          {:ok, ^bytes} ->
            with :ok <- private_mode(root, :directory, 0o700),
                 :ok <- private_mode(Path.join(root, "OWNER.json"), :regular, 0o600),
                 do: existing_completion(root)
          {:error, :enoent} ->
            # Recover creation interrupted before the atomic owner marker rename.
            case File.ls(root) do
              {:ok, []} -> with :ok <- File.chmod(root, 0o700), do: publish(Path.join(root, "OWNER.json"), bytes)
              {:ok, ["OWNER.json.partial"]} -> publish(Path.join(root, "OWNER.json"), bytes)
              _ -> {:error, :material_destination_unowned}
            end
          _ -> {:error, :material_identity_mismatch}
        end
        end
      error -> error
    end
  end

  defp copy_files(bundle, destination, files) do
    Enum.reduce_while(files, :ok, fn %{"path" => relative} = expected, :ok ->
      source = Path.join([bundle, "material", relative])
      target = Path.join([destination, "files", relative])
      case copy_file(source, target, expected, destination) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp copy_file(source, target, expected, root) do
    case File.stat(target) do
      {:ok, _} ->
        with :ok <- private_ancestors(Path.dirname(target), root),
             :ok <- private_mode(target, :regular, 0o600), do: verify(target, expected)
      {:error, :enoent} ->
        temporary = target <> ".partial"
        with :ok <- private_parents(Path.dirname(target)),
             :ok <- private_ancestors(Path.dirname(target), root),
             :ok <- remove_partial(temporary),
             {:ok, file} <- File.open(temporary, [:write, :binary, :exclusive]) do
          result = try do
            with :ok <- File.chmod(temporary, 0o600),
                 :ok <- stream(source, file),
                 :ok <- :file.sync(file),
                 :ok <- verify(temporary, expected) do
              :ok
            end
          after
            File.close(file)
          end
          with :ok <- result, do: File.rename(temporary, target)
        end
      error -> error
    end
  end

  defp stream(source, file) do
    Enum.reduce_while(File.stream!(source, [], 1_048_576), :ok, fn bytes, :ok ->
      case IO.binwrite(file, bytes) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp verify(path, %{"size" => size, "sha256" => hash}) do
    case Source.file_observation(path) do
      {:ok, %{size: ^size, sha256: ^hash}} -> :ok
      _ -> {:error, :material_content_mismatch}
    end
  end

  defp publish(path, bytes) do
    case File.read(path) do
      {:ok, ^bytes} -> private_mode(path, :regular, 0o600)
      {:ok, _} -> {:error, :material_identity_mismatch}
      {:error, :enoent} ->
        temporary = path <> ".partial"
        with :ok <- remove_partial(temporary),
             {:ok, file} <- File.open(temporary, [:write, :binary, :exclusive]) do
          result = try do
            with :ok <- File.chmod(temporary, 0o600),
                 :ok <- IO.binwrite(file, bytes), do: :file.sync(file)
          after
            File.close(file)
          end
          with :ok <- result, do: File.rename(temporary, path)
        end
      error -> error
    end
  end

  defp private_parents(path) do
    case File.mkdir(path) do
      :ok -> File.chmod(path, 0o700)
      {:error, :enoent} ->
        with :ok <- private_parents(Path.dirname(path)),
             :ok <- File.mkdir(path), do: File.chmod(path, 0o700)
      {:error, :eexist} -> private_mode(path, :directory, 0o700)
      error -> error
    end
  end

  defp private_ancestors(root, root), do: private_mode(root, :directory, 0o700)
  defp private_ancestors(path, root) do
    with :ok <- private_mode(path, :directory, 0o700),
         do: private_ancestors(Path.dirname(path), root)
  end

  defp existing_completion(root) do
    path = Path.join(root, "COMPLETE.json")
    case File.stat(path) do
      {:error, :enoent} -> :ok
      {:ok, _} -> private_mode(path, :regular, 0o600)
      _ -> {:error, :material_permissions_mismatch}
    end
  end

  defp private_mode(path, type, mode) do
    case File.stat(path) do
      {:ok, stat} when stat.type == type ->
        if (stat.mode &&& 0o777) == mode, do: :ok, else: {:error, :material_permissions_mismatch}
      _ -> {:error, :material_permissions_mismatch}
    end
  end

  defp remove_partial(path) do
    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      error -> error
    end
  end
end
