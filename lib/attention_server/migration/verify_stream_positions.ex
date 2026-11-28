defmodule AttentionServer.Migration.VerifyStreamPositions do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportSession, ImportStreams, VerifyFacts}
  alias AttentionServer.Store.RecordTable
  alias Exqlite.Sqlite3

  @tables ~w(client_resource_revs client_resource_created client_changes agent_changes agent_item_advisory_changes)

  # Compare retained changes and revision indexes, including creation rows left
  # by reconciliation. Stream-head and signing-identity validation are separate.
  def run(db, identity, bundle) do
    with {:ok, _} <- VerifyFacts.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, source} <- Sqlite3.open(Path.join(bundle, "source.sqlite3"), mode: :readonly) do
      try do
        with {:ok, counts} <- tables(db, source, bundle, manifest),
             :ok <- counts(db, counts) do
          {:ok, counts}
        end
      after
        Sqlite3.close(source)
      end
    end
  end

  defp tables(db, source, bundle, manifest) do
    Enum.reduce_while(@tables, {:ok, %{revisions: 0, changes: 0}}, fn name, {:ok, counts} ->
      result = case Map.fetch(manifest.data.tables, name) do
        :error -> {:ok, counts}
        {:ok, table} -> Frames.fold(Path.join(bundle, table.file), table.summary, counts,
          fn row, acc -> verify(db, source, manifest, name, row, acc) end)
      end
      case result do
        {:ok, _} -> {:cont, result}
        error -> {:halt, error}
      end
    end)
  end

  defp verify(db, source, manifest, "client_resource_revs", [person, id, rev], acc) do
    with {:ok, at} <- scalar(source, manifest, "client_resource_created", "created_at_ms", person, id),
         {:ok, kind} <- ImportStreams.person_kind(id),
         :ok <- revision(db, person, "", kind, id, rev, at) do
      {:ok, %{acc | revisions: acc.revisions + 1}}
    end
  end

  defp verify(db, source, manifest, "client_resource_created", [person, id, at], acc) do
    with {:ok, explicit} <- scalar(source, manifest, "client_resource_revs", "rev", person, id),
         {:ok, rev} <- fallback(source, manifest, person, explicit),
         {:ok, kind} <- ImportStreams.person_kind(id),
         :ok <- revision(db, person, "", kind, id, rev, at) do
      {:ok, %{acc | revisions: acc.revisions + if(is_nil(explicit), do: 1, else: 0)}}
    end
  end

  defp verify(db, _, _, "client_changes", [person, rev, kind, id, at], acc) do
    with :ok <- change(db, person, "", kind, id, rev, nil, at),
      do: {:ok, %{acc | changes: acc.changes + 1}}
  end

  defp verify(db, _, _, table, [person, agent, id, rev, session, at], acc)
       when table in ~w(agent_changes agent_item_advisory_changes) do
    with {:ok, kind} <- ImportStreams.agent_kind(id),
         :ok <- revision(db, person, agent, kind, id, rev, nil),
         :ok <- change(db, person, agent, kind, id, rev, session, at) do
      {:ok, %{acc | revisions: acc.revisions + 1,
        changes: acc.changes + if(is_nil(at), do: 0, else: 1)}}
    end
  end

  defp scalar(source, manifest, table, column, person, id) do
    if Map.has_key?(manifest.data.tables, table) do
      case RecordTable.select(source,
        "SELECT " <> column <> " FROM " <> table <> " WHERE person_id=? AND resource_id=?", [person, id]) do
        {:ok, []} -> {:ok, nil}
        {:ok, [[value]]} -> {:ok, value}
        error -> error
      end
    else
      {:ok, nil}
    end
  end

  defp fallback(_, _, _, rev) when is_integer(rev), do: {:ok, rev}
  defp fallback(source, manifest, person, nil) do
    meta = if Map.has_key?(manifest.data.tables, "client_change_log_meta"),
      do: RecordTable.select(source, "SELECT seed_rev FROM client_change_log_meta WHERE person_id=?", [person]),
      else: {:ok, []}
    case meta do
      {:ok, [[seed]]} -> {:ok, seed}
      {:ok, []} ->
        if Map.has_key?(manifest.data.tables, "client_change_log_epoch") do
          case RecordTable.select(source, "SELECT epoch FROM client_change_log_epoch WHERE id=1") do
            {:ok, [[epoch]]} -> {:ok, epoch}
            {:ok, []} -> {:ok, 1}
            error -> error
          end
        else
          {:ok, 1}
        end
      error -> error
    end
  end

  defp revision(db, person, origin, kind, id, rev, at) do
    case ImportSession.query(db,
      "SELECT revision,first_seen_at_ms FROM harklight.stream_resource_revisions WHERE person_id=$1 AND origin_key=$2 AND resource_kind=$3 AND resource_id=$4", [person, origin, kind, id]) do
      {:ok, %{rows: [[^rev, ^at]]}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, :stream_revision_mismatch}
    end
  end

  defp change(db, person, origin, kind, id, rev, session, at) do
    expected = if is_nil(at), do: [], else: [[kind, id, session, at]]
    case ImportSession.query(db,
      "SELECT resource_kind,resource_id,session_handle,created_at_ms FROM harklight.stream_changes WHERE person_id=$1 AND origin_key=$2 AND revision=$3", [person, origin, rev]) do
      {:ok, %{rows: ^expected}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, :stream_change_mismatch}
    end
  end

  defp counts(db, expected) do
    Enum.reduce_while([revisions: "stream_resource_revisions", changes: "stream_changes"], :ok,
      fn {field, table}, :ok ->
        count = Map.fetch!(expected, field)
        case ImportSession.query(db, "SELECT count(*) FROM harklight." <> table, []) do
          {:ok, %{rows: [[^count]]}} -> {:cont, :ok}
          {:error, _} = error -> {:halt, error}
          _ -> {:halt, {:error, {:stream_count_mismatch, field}}}
        end
      end)
  end
end
