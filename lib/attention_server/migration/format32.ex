defmodule AttentionServer.Migration.Format32 do
  @moduledoc """
  Read-only inventory of a stopped FORMAT32 backup. The caller must first stop
  every writer and keep the original backup offline. This does not fence a
  running Server and is not an import or activation command.
  """

  alias AttentionServer.Migration.{SourceFormat, SourceSchema}
  alias AttentionServer.Store.{RecordCodec, RecordTable, StoredTerms}
  alias Exqlite.Sqlite3

  @required ~w(relay_state relay_records relay_record_generation relay_index_state)

  def preflight(path, opts) do
    if Keyword.get(opts, :stopped) == true and is_binary(Keyword.get(opts, :scratch_root)) do
      inspect_copy(Path.expand(path), Keyword.fetch!(opts, :scratch_root))
    else
      {:error, :stopped_backup_required}
    end
  end

  defp inspect_copy(path, root) do
    dir =
      Path.join(
        root,
        "format32-" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
      )

    with :ok <- File.mkdir(dir) do
      try do
        copy = Path.join(dir, "source.sqlite3")

        with :ok <- File.chmod(dir, 0o700),
             :ok <- checkpointed(path),
             {:ok, digest, bytes} <- hash_file(path),
             :ok <- File.cp(path, copy),
             :ok <- File.chmod(copy, 0o600),
             {:ok, ^digest, ^bytes} <- hash_file(copy),
             {:ok, manifest} <- inspect_database(copy),
             :ok <- checkpointed(path),
             {:ok, ^digest, ^bytes} <- hash_file(path) do
          {:ok, Map.merge(manifest, %{source_sha256: digest, source_bytes: bytes})}
        else
          {:ok, _, _} -> {:error, :source_changed}
          {:error, _} = error -> error
          _ -> {:error, :invalid_source}
        end
      after
        File.rm_rf(dir)
      end
    end
  end

  # SQLite may create transient sidecars even on a read-only connection. Open
  # only the disposable copy so the protected backup stays byte-for-byte intact.
  defp inspect_database(path) do
    with {:ok, db} <- Sqlite3.open(path, mode: :readonly) do
      try do
        with :ok <- Sqlite3.execute(db, "PRAGMA query_only = ON"),
             {:ok, [["ok"]]} <- RecordTable.select(db, "PRAGMA integrity_check"),
             {:ok, tables} <- schema(db),
             {:ok, info} <- anchor(db),
             {:ok, records} <- records(db),
             {:ok, inventory} <- table_inventory(db, tables) do
          {:ok, %{manifest_version: 1, anchor: info, records: records, tables: inventory}}
        else
          {:error, reason} when is_atom(reason) or is_tuple(reason) -> {:error, reason}
          _ -> {:error, :invalid_source}
        end
      after
        Sqlite3.close(db)
      end
    end
  end

  defp anchor(db) do
    with {:ok, [[version, payload, checksum]]} <-
           RecordTable.select(
             db,
             "SELECT version, payload, checksum FROM relay_state WHERE id = 1"
           ),
         {:ok, generation} <- RecordTable.generation(db) do
      SourceFormat.validate(payload, checksum, version, generation)
    else
      _ -> {:error, :invalid_source_anchor}
    end
  end

  defp schema(db) do
    with {:ok, rows} <-
           RecordTable.select(
             db,
             "SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name"
           ) do
      tables = List.flatten(rows)
      known = SourceSchema.tables()

      cond do
        not Enum.all?(@required, &(&1 in tables)) ->
          {:error, :unsupported_source_format}

        not Enum.all?(tables, &Map.has_key?(known, &1)) ->
          {:error, :unknown_source_table}

        true ->
          Enum.reduce_while(tables, {:ok, []}, fn table, {:ok, checked} ->
            case RecordTable.select(db, "PRAGMA table_info(#{quote_identifier(table)})") do
              {:ok, columns} ->
                names = Enum.map(columns, &Enum.at(&1, 1))

                if names == Map.fetch!(known, table),
                  do: {:cont, {:ok, [{table, names} | checked]}},
                  else: {:halt, {:error, {:unsupported_source_columns, table}}}

              _ ->
                {:halt, {:error, :invalid_source_schema}}
            end
          end)
      end
    end
  end

  defp records(db) do
    :ok = StoredTerms.load_atoms()

    Enum.each(
      [
        AttentionServer.Groups,
        AttentionServer.SharedMessages,
        AttentionServer.SharedAnswers,
        AttentionServer.SharedPush,
        AttentionServer.SharedReminders
      ],
      &Code.ensure_loaded!/1
    )

    with {:ok, stmt} <-
           Sqlite3.prepare(db, "SELECT key, payload, checksum FROM relay_records ORDER BY key") do
      try do
        digest_rows(db, stmt, :crypto.hash_init(:sha256), 0, fn [key, payload, checksum] ->
          case RecordCodec.decode_row({key, payload, checksum}) do
            {:ok, address, value} -> {:ok, {address, value}}
            _ -> {:error, :invalid_source_record}
          end
        end)
      after
        Sqlite3.release(db, stmt)
      end
    end
  end

  defp table_inventory(db, tables) do
    Enum.reduce_while(tables, {:ok, %{}}, fn {table, columns}, {:ok, acc} ->
      columns_sql = Enum.map_join(columns, ",", &quote_identifier/1)
      sql = "SELECT #{columns_sql} FROM #{quote_identifier(table)} ORDER BY #{columns_sql}"

      case inventory_rows(db, sql) do
        {:ok, summary} -> {:cont, {:ok, Map.put(acc, table, Map.put(summary, :columns, columns))}}
        _ -> {:halt, {:error, :source_table_read_failed}}
      end
    end)
  end

  defp inventory_rows(db, sql) do
    with {:ok, stmt} <- Sqlite3.prepare(db, sql) do
      try do
        digest_rows(db, stmt, :crypto.hash_init(:sha256), 0, &{:ok, &1})
      after
        Sqlite3.release(db, stmt)
      end
    end
  end

  defp digest_rows(db, stmt, hash, count, decode) do
    case Sqlite3.step(db, stmt) do
      {:row, row} ->
        with {:ok, value} <- decode.(row) do
          bytes = :erlang.term_to_binary(value, [:deterministic, {:minor_version, 2}])
          next = :crypto.hash_update(hash, [<<byte_size(bytes)::64>>, bytes])
          digest_rows(db, stmt, next, count + 1, decode)
        end

      :done ->
        {:ok, %{count: count, sha256: Base.encode16(:crypto.hash_final(hash), case: :lower)}}

      _ ->
        {:error, :source_table_read_failed}
    end
  end

  defp quote_identifier(name), do: "\"" <> String.replace(name, "\"", "\"\"") <> "\""

  defp checkpointed(path) do
    Enum.reduce_while(["-wal", "-journal"], :ok, fn suffix, :ok ->
      case File.stat(path <> suffix) do
        {:error, :enoent} -> {:cont, :ok}
        {:ok, %{size: 0}} -> {:cont, :ok}
        _ -> {:halt, {:error, :store_not_checkpointed}}
      end
    end)
  end

  defp hash_file(path) do
    with {:ok, %{type: :regular}} <- File.lstat(path) do
      {hash, count} =
        path
        |> File.stream!([], 1024 * 1024)
        |> Enum.reduce({:crypto.hash_init(:sha256), 0}, fn bytes, {hash, count} ->
          {:crypto.hash_update(hash, bytes), count + byte_size(bytes)}
        end)

      {:ok, Base.encode16(:crypto.hash_final(hash), case: :lower), count}
    else
      _ -> {:error, :invalid_source_file}
    end
  rescue
    File.Error -> {:error, :source_read_failed}
  end
end
