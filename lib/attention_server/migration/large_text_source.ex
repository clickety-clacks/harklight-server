defmodule AttentionServer.Migration.LargeTextSource do
  @moduledoc false
  alias AttentionServer.{LargeData, LargeData.Ref}
  alias AttentionServer.Migration.{Bundle, Frames, SourceSchema}
  alias AttentionServer.Store.{RecordCodec, RecordTable}
  alias Exqlite.Sqlite3

  # Inspect only the verified export's immutable database copy. A reference
  # requires an exact source path and marker; marker-looking literal text alone
  # never becomes a PostgreSQL reference. Neither original state nor PG is opened.
  def scan(bundle, consume) when is_function(consume, 1) do
    with {:ok, manifest, _} <- Bundle.verify(bundle) do
      case Map.fetch(manifest.data.tables, "relay_large_data_refs") do
        :error -> {:ok, 0}
        {:ok, table} ->
          with {:ok, db} <- Sqlite3.open(Path.join(bundle, "source.sqlite3"), mode: :readonly) do
            try do
              Frames.fold(Path.join(bundle, table.file), table.summary, 0, fn row, count ->
                with {:ok, reference} <- reference(db, row, Path.join(bundle, "files/uploads")),
                     :ok <- consume.(reference) do
                  {:ok, count + 1}
                end
              end)
            after
              Sqlite3.close(db)
            end
          end
      end
    end
  end

  def reference(db, [encoded_path, marker, id, size, hash], directory)
      when is_binary(id) and is_binary(marker) and is_binary(hash) and is_binary(directory) do
    ref = %Ref{id: id, size: size, sha256: hash, path: Path.join(directory, id)}
    with true <- LargeData.valid_refs?(ref),
         true <- LargeData.persist_refs(ref) == marker,
         {:ok, steps} <- decode_path(encoded_path),
         {:ok, address, path} <- record_path(steps),
         :ok <- known_address(address),
         :ok <- source_value(db, address, path, marker),
         :ok <- LargeData.verify(ref) do
      {:ok, %{address: address, path: path, marker: marker,
        id: id, size: size, sha256: hash, file: ref.path}}
    else
      false -> {:error, :invalid_large_text_source_reference}
      {:error, _} = error -> error
      _ -> {:error, :large_text_source_reference_mismatch}
    end
  end
  def reference(_, _, _), do: {:error, :invalid_large_text_source_reference}

  defp known_address(address) do
    if SourceSchema.valid_record?(address, %{}),
      do: :ok, else: {:error, :unsupported_large_text_source_path}
  end

  defp source_value(db, address, path, marker) do
    key = RecordCodec.encode_key(address)
    with {:ok, [[^key, payload, checksum]]} <- RecordTable.select(db,
           "SELECT key,payload,checksum FROM relay_records WHERE key=?", [{:blob, key}]),
         {:ok, ^address, value} <- RecordCodec.decode_row({key, payload, checksum}),
         {:ok, ^marker} <- at(value, path) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :large_text_source_reference_mismatch}
    end
  end

  defp record_path([{:map, :origin_feeds}, {:map, scope}, {:map, :entries}, {:map, seq} | path]),
    do: {:ok, {:origin_feed, scope, {:entry, seq}}, path}
  defp record_path([{:map, :advisory_feeds}, {:map, scope}, {:map, :entries}, {:map, seq} | path]),
    do: {:ok, {:advisory, scope, {:entry, seq}}, path}
  defp record_path([{:map, field}, {:map, key} | path]), do: {:ok, {:row, field, key}, path}
  defp record_path([{:map, field} | path]), do: {:ok, {:field, field}, path}
  defp record_path(_), do: {:error, :unsupported_large_text_source_path}

  defp decode_path(bytes) when is_binary(bytes) do
    case :erlang.binary_to_term(bytes, [:safe]) do
      path when is_list(path) -> {:ok, path}
      _ -> {:error, :invalid_large_text_source_path}
    end
  rescue
    ArgumentError -> {:error, :invalid_large_text_source_path}
  end
  defp decode_path(_), do: {:error, :invalid_large_text_source_path}

  defp at(value, []), do: {:ok, value}
  defp at(value, [{:map, key} | rest]) when is_map(value) do
    with {:ok, next} <- Map.fetch(value, key), do: at(next, rest)
  end
  defp at(value, [{:list, index} | rest]) when is_list(value) and is_integer(index) and index >= 0 do
    with {:ok, next} <- Enum.fetch(value, index), do: at(next, rest)
  end
  defp at(value, [{:tuple, index} | rest]) when is_tuple(value) and is_integer(index) and index >= 0 and index < tuple_size(value),
    do: at(elem(value, index), rest)
  defp at(_, _), do: {:error, :invalid_large_text_source_path}
end
