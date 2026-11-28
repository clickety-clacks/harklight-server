defmodule AttentionServer.Migration.CLI do
  @moduledoc false
  alias AttentionServer.Migration.Bundle

  # Capability discovery is read-only and does not open a receipt or database.
  # Status alone does not imply that the complete upgrade pipeline is available.
  def run(["capabilities"]) do
    {:ok, %{"contract_version" => 1, "command" => "capabilities",
      "commands" => ["export", "verify", "import", "status", "validate"], "migration_ready" => false}}
  end

  def run([command | args]) when command in ["import", "validate"] do
    {opts, rest, invalid} = OptionParser.parse(args, strict: [bundle: :string, receipt: :string])
    with true <- rest == [] and invalid == [] and length(opts) == 2,
         {:ok, bundle} <- required(opts, :bundle),
         {:ok, receipt} <- required(opts, :receipt) do
      case command do
        "import" -> AttentionServer.Migration.ImportPipeline.run(System.get_env(), bundle, receipt)
        "validate" ->
          with {:ok, identity} <- AttentionServer.Migration.ImportReceipt.load(receipt) do
            AttentionServer.Migration.Connection.with_connection(System.get_env(), fn db ->
              AttentionServer.Migration.ValidateImport.run(db, identity, bundle,
                AttentionServer.Migration.ImportPipeline.material_root(receipt))
            end)
          end
      end
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_arguments}
    end
  end

  def run(["status", "--receipt", path]) do
    with {:ok, identity} <- AttentionServer.Migration.ImportReceipt.load(path) do
      AttentionServer.Migration.Connection.with_connection(System.get_env(), fn db ->
        AttentionServer.Migration.ImportStatus.read(db, identity)
      end)
    end
  end

  def run(["verify", directory]) do
    case Bundle.verify(directory) do
      {:ok, manifest, summary} -> {:ok, public_summary(manifest, summary, "verify")}
      error -> error
    end
  end

  def run(["export" | args]) do
    {opts, rest, invalid} =
      OptionParser.parse(args,
        strict: [
          source: :string,
          output: :string,
          unit: :string,
          supervisor: :string,
          stopped_evidence: :string,
          pidfile: :string,
          material_map: :string
        ]
      )

    with true <- rest == [] and invalid == [],
         {:ok, source} <- required(opts, :source),
         {:ok, destination} <- required(opts, :output),
         {:ok, supervisor} <- supervisor(opts),
         {:ok, material_path} <- required(opts, :material_map),
         {:ok, material} <- material(material_path),
         options = [supervisor: supervisor, pidfile: opts[:pidfile], material: material],
         {:ok, _} <- Bundle.export(source, destination, options),
         {:ok, manifest, summary} <- Bundle.verify(destination) do
      {:ok, public_summary(manifest, summary, "export")}
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_arguments}
    end
  end

  def run(_), do: {:error, :invalid_arguments}

  def main(args) do
    case run(args) do
      {:ok, summary} ->
        IO.puts(AttentionServer.JSON.encode!(summary))

      {:error, reason} ->
        # Do not print source values, paths, configuration or identity material.
        IO.puts(:stderr, "Migration refused: " <> error_code(reason))
        System.halt(2)
    end
  end

  defp required(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and byte_size(value) > 0 -> {:ok, value}
      _ -> {:error, :invalid_arguments}
    end
  end

  defp supervisor(opts) do
    case {opts[:unit], opts[:supervisor], opts[:stopped_evidence]} do
      {unit, nil, nil} when is_binary(unit) and byte_size(unit) > 0 ->
        {:ok, {:systemd, unit}}

      {nil, name, evidence}
      when is_binary(name) and byte_size(name) > 0 and
             is_binary(evidence) and byte_size(evidence) > 0 ->
        {:ok, {:operator, name, evidence}}

      _ ->
        {:error, :supervisor_required}
    end
  end

  defp material(path) do
    with {:ok, bytes} <- File.read(path),
         {:ok, files} when is_map(files) <- AttentionServer.JSON.decode_line(bytes <> "\n"),
         true <-
           Enum.all?(files, fn {name, source} ->
             is_binary(name) and is_binary(source) and Path.type(source) == :absolute
           end) do
      {:ok, files}
    else
      _ -> {:error, :invalid_material_map}
    end
  end

  defp public_summary(manifest, summary, command) do
    %{
      "contract_version" => 1,
      "command" => command,
      "result" => "verified_export",
      "bundle_version" => manifest.version,
      "manifest_sha256" => summary.sha256,
      "record_count" => manifest.source.records.count,
      "table_count" => map_size(manifest.source.tables),
      "file_count" => map_size(manifest.data.files),
      "material_count" => map_size(manifest.material)
    }
  end

  defp error_code(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp error_code({reason, _}) when is_atom(reason), do: Atom.to_string(reason)
  defp error_code(_), do: "migration_failed"
end
