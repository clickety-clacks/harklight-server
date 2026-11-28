defmodule AttentionServer.Migration.BundleValidation do
  @moduledoc false
  alias AttentionServer.Migration.{Frames, MaterialProvenance, Provenance, Source, SourceSchema}

  # Integrity validation is separate from target-schema and relational validation.
  # A successful result here never authorizes database activation.
  def validate(root, %{
        version: 2,
        source: source,
        provenance: receipt,
        data: data,
        material: material,
        material_provenance: material_provenance
      }) do
    with :ok <- Provenance.validate(receipt, source.source_sha256, source.source_bytes),
         :ok <- sealed_source(root, source),
         :ok <-
           sequence(Path.join(root, "records.frames"), data.records, source.records, &record/1),
         true <- Map.keys(data.tables) |> Enum.sort() == Enum.sort(Map.keys(source.tables)),
         :ok <- tables(root, data.tables, source.tables),
         :ok <- files(root, data.files, "files/uploads/"),
         :ok <- files(root, material, "material/"),
         :ok <- MaterialProvenance.verify(root, material, material_provenance),
         :ok <- sealed_source(root, source) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_bundle_manifest}
    end
  rescue
    KeyError -> {:error, :invalid_bundle_manifest}
    BadMapError -> {:error, :invalid_bundle_manifest}
  end

  def validate(_, %{version: 1}), do: {:error, :legacy_bundle_requires_reexport}
  def validate(_, _), do: {:error, :invalid_bundle_manifest}

  # Every bundle consumer shares the same effective SQLite seal. Never open,
  # checkpoint, delete or truncate source sidecars to obtain an admissible copy.
  def sealed_source(root, source) do
    with :ok <- no_pending_writes(root),
         :ok <- file(root, "source.sqlite3", %{size: source.source_bytes, sha256: source.source_sha256}),
         :ok <- no_pending_writes(root), do: :ok
  end

  defp no_pending_writes(root) do
    Enum.reduce_while(["-wal", "-journal"], :ok, fn suffix, :ok ->
      case File.lstat(Path.join(root, "source.sqlite3" <> suffix)) do
        {:error, :enoent} -> {:cont, :ok}
        {:ok, %{type: :regular, size: 0}} -> {:cont, :ok}
        _ -> {:halt, {:error, :bundle_source_pending_writes}}
      end
    end)
  end

  defp record({address, value}) do
    if SourceSchema.valid_record?(address, value),
      do: :ok,
      else: {:error, :unsupported_source_record_address}
  end

  defp record(_), do: {:error, :invalid_source_record}

  defp tables(root, actual, expected) do
    Enum.reduce_while(actual, :ok, fn {table, entry}, :ok ->
      info = Map.fetch!(expected, table)

      valid =
        entry.file == "tables/" <> table <> ".frames" and
          Map.get(SourceSchema.tables(), table) == info.columns and entry.columns == info.columns

      if valid do
        check = fn row ->
          if is_list(row) and length(row) == length(info.columns),
            do: :ok,
            else: {:error, :invalid_table_row}
        end

        case sequence(Path.join(root, entry.file), entry.summary, info, check) do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end
      else
        {:halt, {:error, :invalid_bundle_table}}
      end
    end)
  end

  defp sequence(path, summary, expected, check) do
    initial = {0, :crypto.hash_init(:sha256)}

    result =
      Frames.fold(path, summary, initial, fn value, {count, hash} ->
        with :ok <- check.(value) do
          bytes = :erlang.term_to_binary(value, [:deterministic, {:minor_version, 2}])
          {:ok, {count + 1, :crypto.hash_update(hash, [<<byte_size(bytes)::64>>, bytes])}}
        end
      end)

    with {:ok, {count, hash}} <- result,
         true <- count == expected.count,
         true <- Base.encode16(:crypto.hash_final(hash), case: :lower) == expected.sha256 do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :export_content_mismatch}
    end
  end

  defp files(root, entries, prefix) when is_map(entries) do
    Enum.reduce_while(entries, :ok, fn {relative, info}, :ok ->
      safe =
        is_binary(relative) and String.starts_with?(relative, prefix) and
          Enum.all?(String.split(relative, "/"), &(&1 not in ["", ".", ".."])) and
          not String.contains?(relative, "\\")

      if safe do
        case file(root, relative, info) do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end
      else
        {:halt, {:error, :invalid_bundle_file}}
      end
    end)
  end

  defp files(_, _, _), do: {:error, :invalid_bundle_files}

  defp file(root, relative, %{size: size, sha256: hash}) do
    case Source.file_observation(Path.join(root, relative)) do
      {:ok, %{size: ^size, sha256: ^hash}} -> :ok
      _ -> {:error, :bundle_file_mismatch}
    end
  end

  defp file(_, _, _), do: {:error, :invalid_bundle_file}
end
