defmodule AttentionServer.Migration.Bundle do
  @moduledoc false
  alias AttentionServer.Migration.{BundleValidation, Format32, Frames, Source, MaterialProvenance}
  alias AttentionServer.Store.RecordCodec
  alias Exqlite.Sqlite3

  def export(path, destination, opts, observer \\ &Source.observe/2) do
    with {:ok, captured} <- Source.capture(path, destination, opts, observer),
         {:ok, material_before} <- MaterialProvenance.capture(Keyword.get(opts, :material, %{})),
         {:ok, inventory} <-
           Format32.preflight(captured.copy,
             provenance: captured.provenance,
             scratch_root: destination
           ),
         {:ok, data} <- export_data(captured.copy, path <> ".uploads", destination, inventory),
         {:ok, material} <- copy_material(Keyword.get(opts, :material, %{}), destination),
         {:ok, material_provenance} <- MaterialProvenance.confirm(material_before, destination, material),
         {:ok, receipt} <- Source.confirm(path, captured.provenance, opts, observer),
         manifest = %{
           version: 2,
           source: inventory,
           provenance: receipt,
           data: data,
           material: material,
           material_provenance: material_provenance
         },
         :ok <- BundleValidation.validate(destination, manifest),
         {:ok, summary} <- Frames.write(Path.join(destination, "manifest.frames"), [manifest]),
         :ok <- seal(destination, summary) do
      {:ok, summary}
    end
  rescue
    File.Error -> {:error, :export_file_failure}
  catch
    {:export_error, reason} -> {:error, reason}
  end

  def verify(root) do
    # Load persisted atoms without starting Server processes, including in a cold release VM.
    AttentionServer.Store.StoredTerms.load_atoms()

    Enum.each(
      [
        AttentionServer.Relay,
        AttentionServer.Groups,
        AttentionServer.SharedMessages,
        AttentionServer.SharedAnswers,
        AttentionServer.SharedPush,
        AttentionServer.SharedReminders,
        AttentionServer.Migration.SourceFormat,
        AttentionServer.Migration.Provenance,
        Format32,
        Source,
        BundleValidation,
        MaterialProvenance
      ],
      &Code.ensure_loaded!/1
    )

    with {:ok, bytes} <- File.read(Path.join(root, "COMPLETE.json")),
         {:ok, %{"version" => version} = marker} <- AttentionServer.JSON.decode_line(bytes <> "\n"),
         :ok <- supported_version(version),
         %{"count" => 1, "bytes" => size, "sha256" => hash} <- marker,
         summary = %{count: 1, bytes: size, sha256: hash},
         {:ok, [manifest]} <-
           Frames.fold(Path.join(root, "manifest.frames"), summary, [], fn value, acc ->
             {:ok, [value | acc]}
           end),
         :ok <- BundleValidation.validate(root, manifest) do
      {:ok, manifest, summary}
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_bundle_manifest}
    end
  end

  defp supported_version(2), do: :ok
  defp supported_version(1), do: {:error, :legacy_bundle_requires_reexport}
  defp supported_version(_), do: {:error, :unsupported_bundle_version}

  defp export_data(copy, uploads, destination, inventory) do
    with :ok <- private_directory(Path.join(destination, "tables")),
         {:ok, db} <- Sqlite3.open(copy, mode: :readonly) do
      try do
        with {:ok, records} <- Frames.write(Path.join(destination, "records.frames"), records(db)),
             true <- records.count == inventory.records.count,
             {:ok, tables} <- export_tables(db, destination, inventory.tables),
             {:ok, files} <- copy_references(db, uploads, destination, inventory.tables) do
          {:ok, %{records: records, tables: tables, files: files}}
        else
          false -> {:error, :export_count_mismatch}
          error -> error
        end
      after
        Sqlite3.close(db)
      end
    end
  end

  defp export_tables(db, destination, tables) do
    Enum.reduce_while(Enum.sort(tables), {:ok, %{}}, fn {table, info}, {:ok, acc} ->
      columns = Enum.map_join(info.columns, ",", &quote_name/1)
      sql = "SELECT #{columns} FROM #{quote_name(table)} ORDER BY #{columns}"
      relative = "tables/" <> table <> ".frames"

      case Frames.write(Path.join(destination, relative), rows(db, sql)) do
        {:ok, summary} when summary.count == info.count ->
          {:cont,
           {:ok, Map.put(acc, table, %{file: relative, columns: info.columns, summary: summary})}}

        {:ok, _} ->
          {:halt, {:error, :export_count_mismatch}}

        error ->
          {:halt, error}
      end
    end)
  end

  defp records(db) do
    rows(db, "SELECT key,payload,checksum FROM relay_records ORDER BY key")
    |> Stream.map(fn [key, payload, hash] ->
      case RecordCodec.decode_row({key, payload, hash}) do
        {:ok, address, value} -> {address, value}
        _ -> throw({:export_error, :invalid_source_record})
      end
    end)
  end

  defp rows(db, sql) do
    Stream.resource(
      fn ->
        case Sqlite3.prepare(db, sql) do
          {:ok, stmt} -> stmt
          _ -> throw({:export_error, :source_table_read_failed})
        end
      end,
      fn stmt ->
        case Sqlite3.step(db, stmt) do
          {:row, row} -> {[row], stmt}
          :done -> {:halt, stmt}
          _ -> throw({:export_error, :source_table_read_failed})
        end
      end,
      &Sqlite3.release(db, &1)
    )
  end

  defp copy_references(db, source, destination, tables) do
    uploads =
      records(db)
      |> Stream.flat_map(fn
        {{:row, :uploads, id}, %{id: id, size: size, sha256: hash}} ->
          [{component!(id), size, hash}]

        {{:row, :uploads, _}, _} ->
          throw({:export_error, :invalid_upload_metadata})

        _ ->
          []
      end)

    large =
      optional_rows(
        db,
        tables,
        "relay_large_data_refs",
        "SELECT DISTINCT id,size,sha256 FROM relay_large_data_refs"
      )
      |> Stream.map(fn [id, size, hash] -> {component!(id), size, hash} end)

    staged =
      optional_rows(
        db,
        tables,
        "client_staged_files",
        "SELECT file_id,size,sha256 FROM client_staged_files"
      )
      |> Stream.map(fn [id, size, hash] ->
        {"client-api-v2-staged/" <> component!(id), size, hash}
      end)

    Stream.concat([uploads, large, staged])
    |> Enum.reduce_while({:ok, %{}}, fn {relative, size, hash}, {:ok, acc} ->
      # IDs are single path components; the staging prefix above is fixed.
      relative =
        if String.starts_with?(relative, "client-api-v2-staged/"),
          do: relative,
          else: component!(relative)

      target = "files/uploads/" <> relative
      expected = %{size: size, sha256: hash}

      case Map.fetch(acc, target) do
        {:ok, ^expected} ->
          {:cont, {:ok, acc}}

        {:ok, _} ->
          {:halt, {:error, :conflicting_file_reference}}

        :error ->
          case copy_file(Path.join(source, relative), Path.join(destination, target), expected) do
            {:ok, ^expected} -> {:cont, {:ok, Map.put(acc, target, expected)}}
            error -> {:halt, error}
          end
      end
    end)
  end

  defp optional_rows(db, tables, table, sql),
    do: if(Map.has_key?(tables, table), do: rows(db, sql), else: [])

  defp copy_material(files, destination) when is_map(files) do
    Enum.reduce_while(Enum.sort(files), {:ok, %{}}, fn {name, source}, {:ok, acc} ->
      relative = "material/" <> relative_name!(name)

      case copy_file(source, Path.join(destination, relative), nil) do
        {:ok, info} -> {:cont, {:ok, Map.put(acc, relative, info)}}
        error -> {:halt, error}
      end
    end)
  end

  defp copy_file(source, target, expected) do
    with {:ok, before} <- Source.file_observation(source),
         facts = Map.take(before, [:size, :sha256]),
         true <- is_nil(expected) or facts == expected,
         :ok <- private_directory(Path.dirname(target)),
         false <- File.exists?(target),
         :ok <- File.cp(source, target),
         :ok <- File.chmod(target, 0o600),
         {:ok, copied} <- Source.file_observation(target),
         true <- Map.take(copied, [:size, :sha256]) == facts,
         {:ok, ^before} <- Source.file_observation(source) do
      {:ok, facts}
    else
      _ -> {:error, :export_file_mismatch}
    end
  end

  defp private_directory(path) do
    with :ok <- File.mkdir_p(path), do: File.chmod(path, 0o700)
  end

  defp relative_name!(name) when is_binary(name) do
    parts = String.split(name, "/")
    Enum.each(parts, &component!/1)
    Enum.join(parts, "/")
  end

  defp relative_name!(_), do: throw({:export_error, :invalid_file_name})

  defp component!(name) when is_binary(name) do
    if name not in ["", ".", ".."] and Path.basename(name) == name and
         not String.contains?(name, ["/", "\\"]),
       do: name,
       else: throw({:export_error, :invalid_file_name})
  end

  defp component!(_), do: throw({:export_error, :invalid_file_name})
  defp quote_name(name), do: "\"" <> String.replace(name, "\"", "\"\"") <> "\""

  # Completion is a final marker; failures before here leave no importable bundle.
  defp seal(destination, summary) do
    bytes =
      AttentionServer.JSON.encode!(%{
        "version" => 2,
        "count" => summary.count,
        "bytes" => summary.bytes,
        "sha256" => summary.sha256
      })

    path = Path.join(destination, "COMPLETE.json")

    with {:ok, file} <- File.open(path, [:write, :binary, :exclusive]) do
      try do
        with :ok <- File.chmod(path, 0o600), :ok <- IO.binwrite(file, bytes), do: :file.sync(file)
      after
        File.close(file)
      end
    end
  end
end
