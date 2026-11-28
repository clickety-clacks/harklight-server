defmodule AttentionServer.Migration.ImportReceipt do
  @moduledoc false
  import Bitwise
  alias AttentionServer.{HeadlessV2Write, JSON}
  alias AttentionServer.Migration.Bundle
  alias AttentionServer.Repository.Postgres.Schema

  # Persist before any destination claim. A resumed receipt always retains its
  # originally pinned schema digest, even if this package ships later migrations.
  def open(path, bundle) do
    with {:ok, _, summary} <- Bundle.verify(bundle),
         {:ok, source} <- Base.decode16(summary.sha256, case: :lower),
         {:ok, identity} <- existing_or_new(path, source),
         true <- identity.source == source do
      {:ok, identity}
    else
      false -> {:error, :import_source_mismatch}
      error -> error
    end
  end

  def load(path) do
    with {:ok, stat} <- File.stat(path),
         true <- stat.type == :regular and (stat.mode &&& 0o777) == 0o600,
         {:ok, bytes} <- File.read(path),
         {:ok, %{"contract_version" => 1, "import_id" => id,
           "source_sha256" => source, "schema_sha256" => schema}} <- JSON.decode_line(bytes <> "\n"),
         true <- is_binary(id) and byte_size(id) > 0,
         {:ok, source} <- digest(source),
         {:ok, schema} <- digest(schema) do
      {:ok, %{id: id, source: source, schema: schema}}
    else
      {:error, :enoent} -> {:error, :receipt_missing}
      _ -> {:error, :invalid_import_receipt}
    end
  end

  def document(%{id: id, source: source, schema: schema}) do
    %{"contract_version" => 1, "import_id" => id,
      "source_sha256" => Base.encode16(source, case: :lower),
      "schema_sha256" => Base.encode16(schema, case: :lower)}
  end

  defp existing_or_new(path, source) do
    case load(path) do
      {:ok, identity} -> {:ok, identity}
      {:error, :receipt_missing} -> create(path, source)
      error -> error
    end
  end

  defp create(path, source) do
    with {:ok, stat} <- File.stat(Path.dirname(path)),
         true <- stat.type == :directory and (stat.mode &&& 0o777) == 0o700 do
      partial = path <> ".partial"
      case load(partial) do
        {:ok, %{source: ^source} = identity} ->
          with :ok <- File.rename(partial, path), do: {:ok, identity}
        {:ok, _} -> {:error, :import_source_mismatch}
        _ ->
          # This file cannot have authorized a database claim: publication of
          # the final receipt is the prerequisite for that later operation.
          _ = File.rm(partial)
          identity = %{id: HeadlessV2Write.uuid_v4(), source: source, schema: Schema.digest()}
          with {:ok, file} <- File.open(partial, [:write, :binary, :exclusive]) do
            result = try do
              with :ok <- File.chmod(partial, 0o600),
                   :ok <- IO.binwrite(file, JSON.encode!(document(identity))), do: :file.sync(file)
            after
              File.close(file)
            end
            with :ok <- result, :ok <- File.rename(partial, path), do: {:ok, identity}
          end
      end
    else
      _ -> {:error, :private_receipt_directory_required}
    end
  end

  defp digest(hex) when is_binary(hex) and byte_size(hex) == 64 do
    Base.decode16(hex, case: :lower)
  end
  defp digest(_), do: {:error, :invalid_digest}
end
