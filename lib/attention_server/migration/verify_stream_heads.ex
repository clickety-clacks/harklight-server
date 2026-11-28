defmodule AttentionServer.Migration.VerifyStreamHeads do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, BundleValidation, Frames, ImportSession, VerifyFacts}
  alias AttentionServer.Store.RecordTable
  alias Exqlite.Sqlite3

  # The query seam permits deterministic isolated source-change tests. The CLI
  # never accepts a callback; normal callers use the real PostgreSQL connection.
  def run(db, identity, bundle, query \\ &ImportSession.query/3) do
    with {:ok, _} <- VerifyFacts.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, source} <- Sqlite3.open(Path.join(bundle, "source.sqlite3"), mode: :readonly) do
      try do
        with :ok <- temporary_heads(source),
             {:ok, epoch} <- epoch(source, manifest),
             :ok <- explicit(source, bundle, manifest),
             {:ok, values} <- retained(source, bundle, manifest, epoch),
             :ok <- identity(db, values, epoch, query),
             :ok <- pages(db, source, nil, query),
             {:ok, [[count]]} <- RecordTable.select(source, "SELECT count(*) FROM temp.expected_heads"),
             {:ok, %{rows: [[^count]]}} <- query.(db, "SELECT count(*) FROM harklight.stream_heads", []),
             :ok <- source_unchanged(bundle, manifest) do
          {:ok, %{heads: count, epoch: epoch}}
        else
          {:error, _} = error -> error
          _ -> {:error, :stream_head_count_mismatch}
        end
      after
        Sqlite3.close(source)
      end
    end
  end

  defp source_unchanged(bundle, manifest) do
    case BundleValidation.sealed_source(bundle, manifest.source) do
      :ok -> :ok
      _ -> {:error, :stream_source_changed}
    end
  end

  # The source database is read-only. This connection-local temporary index
  # deduplicates implicit persons on disk instead of retaining all IDs on BEAM.
  # Closing the connection discards it; no destination or source table is written.
  defp temporary_heads(source) do
    Sqlite3.execute(source, """
      PRAGMA temp_store=FILE;
      PRAGMA temp.cache_size=-2048;
      CREATE TEMP TABLE expected_heads (
        person_id TEXT NOT NULL, origin_key TEXT NOT NULL,
        head INTEGER NOT NULL, floor INTEGER NOT NULL, seed INTEGER NOT NULL,
        PRIMARY KEY(person_id,origin_key)
      ) WITHOUT ROWID;
      """)
  end

  defp epoch(source, manifest) do
    if Map.has_key?(manifest.data.tables, "client_change_log_epoch") do
      case RecordTable.select(source, "SELECT epoch FROM client_change_log_epoch WHERE id=1") do
        {:ok, []} -> {:ok, 1}
        {:ok, [[epoch]]} -> {:ok, epoch}
        error -> error
      end
    else
      {:ok, 1}
    end
  end

  defp explicit(source, bundle, manifest) do
    Enum.reduce_while(~w(client_change_log_meta agent_change_log_meta), :ok, fn name, :ok ->
      result = case Map.fetch(manifest.data.tables, name) do
        :error -> {:ok, :ok}
        {:ok, table} -> Frames.fold(Path.join(bundle, table.file), table.summary, :ok,
          fn row, :ok ->
            values = if name == "client_change_log_meta" do
              [person, head, floor, seed] = row
              [person, "", head, floor, seed]
            else
              row
            end
            with :ok <- RecordTable.run(source,
              "INSERT INTO temp.expected_heads VALUES(?,?,?,?,?)", values), do: {:ok, :ok}
          end)
      end
      case result do
        {:ok, :ok} -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp retained(source, bundle, manifest, epoch) do
    Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, %{}, fn
      {{:field, field}, value}, acc when field in [:relay_instance_id, :agent_cursor_key] ->
        {:ok, Map.put(acc, field, value)}
      {{:row, field, key}, _}, acc when field in [:messages, :conversation_events, :bounce_levels, :alert_choices] ->
        person = if field in [:messages, :conversation_events], do: elem(key, 0), else: key
        with :ok <- RecordTable.run(source,
          "INSERT OR IGNORE INTO temp.expected_heads VALUES(?,'',?,?,?)", [person, epoch, epoch, epoch]),
          do: {:ok, acc}
      _, acc -> {:ok, acc}
    end)
  end

  defp identity(db, %{relay_instance_id: instance, agent_cursor_key: key}, epoch, query) do
    case query.(db,
      "SELECT instance_id,cursor_signing_key,cursor_epoch FROM harklight.server_identity", []) do
      {:ok, %{rows: [[^instance, ^key, ^epoch]]}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, :cursor_identity_mismatch}
    end
  end
  defp identity(_, _, _, _), do: {:error, :cursor_identity_missing}

  defp pages(db, source, after_key, query) do
    {where, params} = case after_key do
      nil -> {"", []}
      {person, origin} -> {"WHERE (person_id,origin_key)>(?,?)", [person, origin]}
    end
    with {:ok, rows} <- RecordTable.select(source,
           "SELECT person_id,origin_key,head,floor,seed FROM temp.expected_heads " <> where <>
             " ORDER BY person_id,origin_key LIMIT 256", params),
         :ok <- compare(db, rows, query) do
      case rows do
        [] -> :ok
        _ ->
          [person, origin | _] = List.last(rows)
          pages(db, source, {person, origin}, query)
      end
    end
  end

  defp compare(db, rows, query) do
    Enum.reduce_while(rows, :ok, fn [person, origin, head, floor, seed], :ok ->
      case query.(db,
        "SELECT head,floor,seed_revision FROM harklight.stream_heads WHERE person_id=$1 AND origin_key=$2", [person, origin]) do
        {:ok, %{rows: [[^head, ^floor, ^seed]]}} -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
        _ -> {:halt, {:error, :stream_head_mismatch}}
      end
    end)
  end
end
