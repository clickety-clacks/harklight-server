defmodule AttentionServer.Migration.ImportStreams do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, ImportAccounts, ImportRows, ImportSession, ImportTables}

  @tables ~w(client_change_log_meta agent_change_log_meta implicit_person_meta client_resource_revs client_resource_created client_changes agent_changes agent_item_advisory_changes)

  def run(db, identity, root) do
    with :ok <- ImportAccounts.run(db, identity, root),
         :ok <- ImportTables.run(db, identity, root),
         {:ok, manifest, _} <- Bundle.verify(root) do
      Enum.reduce_while(@tables, :ok, fn table, :ok ->
        result = if table == "implicit_person_meta" do
          implicit_heads(db, identity, root, manifest)
        else
          import_table(db, identity, root, manifest, table)
        end
        case result do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end
      end)
    end
  end

  defp import_table(db, identity, root, manifest, table) do
        case Map.fetch(manifest.data.tables, table) do
          :error -> :ok
          {:ok, source} ->
            case ImportRows.run(db, identity, "streams/" <> table <> "/v1",
                   Path.join(root, source.file), source.summary, fn _ -> true end,
                   fn tx, rows -> each(rows, &row(tx, table, &1)) end) do
              {:ok, _} -> :ok
              error -> error
            end
        end
  end

  # Match ChangeLog.seeded?/2 using retained facts, not independent allocation
  # highs. Explicit metadata has already been copied and always wins.
  defp implicit_heads(db, identity, root, manifest) do
    select = fn
      {{:row, field, _}, _} -> field in [:messages, :conversation_events, :bounce_levels, :alert_choices]
      _ -> false
    end
    case ImportRows.run(db, identity, "streams/implicit_person_meta/v1",
           Path.join(root, "records.frames"), manifest.data.records, select,
           fn tx, rows ->
             each(rows, fn {{:row, field, key}, _} ->
               person = if field in [:messages, :conversation_events], do: elem(key, 0), else: key
               case ImportSession.query(tx,
                      """
                      INSERT INTO harklight.stream_heads(person_id,origin_key,head,floor,seed_revision)
                      SELECT $1,'',cursor_epoch,cursor_epoch,cursor_epoch FROM harklight.server_identity
                      ON CONFLICT(person_id,origin_key) DO NOTHING
                      """, [person]) do
                 {:ok, %{num_rows: count}} when count in [0, 1] -> :ok
                 {:ok, _} -> {:error, :stream_reference_mismatch}
                 error -> error
               end
             end)
           end) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  def person_kind("item:" <> _), do: {:ok, "item"}
  def person_kind("shared-item:" <> _), do: {:ok, "item"}
  def person_kind("entry:" <> _), do: {:ok, "entry"}
  def person_kind("shared-entry:" <> _), do: {:ok, "entry"}
  def person_kind(id) when id in ~w(counts shared-counts group-counts), do: {:ok, "counts"}
  def person_kind("settings"), do: {:ok, "settings"}
  def person_kind("availability-writer:" <> _), do: {:ok, "availability_writer"}
  def person_kind(_), do: {:error, :unknown_person_stream_resource}

  def agent_kind("item:" <> _), do: {:ok, "message"}
  def agent_kind("entry:" <> _), do: {:ok, "entry"}
  def agent_kind("advisory:" <> _), do: {:ok, "advisory"}
  def agent_kind("item-advisory:" <> _), do: {:ok, "advisory"}
  def agent_kind("shared-message:" <> _), do: {:ok, "shared_message"}
  def agent_kind("shared-entry:" <> _), do: {:ok, "shared_entry"}
  def agent_kind("shared-seen:" <> _), do: {:ok, "shared_seen"}
  def agent_kind("shared-answer:" <> _), do: {:ok, "shared_answer"}
  def agent_kind("shared-advisory:" <> _), do: {:ok, "shared_advisory"}
  def agent_kind(_), do: {:error, :unknown_agent_stream_resource}

  defp row(db, "client_change_log_meta", [person, head, floor, seed]),
    do: head(db, person, "", head, floor, seed)

  defp row(db, "agent_change_log_meta", [person, agent, head, floor, seed]),
    do: head(db, person, agent, head, floor, seed)

  defp row(db, "client_resource_revs", [person, id, rev]) do
    with {:ok, kind} <- person_kind(id), do: revision(db, person, "", kind, id, rev)
  end

  defp row(db, "client_resource_created", [person, id, at]) do
    with {:ok, kind} <- person_kind(id) do
      execute(db,
        """
        INSERT INTO harklight.stream_resource_revisions
          (person_id,origin_key,resource_kind,resource_id,revision,first_seen_at_ms)
        SELECT head.person_id,head.origin_key,$3,$4,coalesce(resource.revision,head.seed_revision),$5
        FROM harklight.stream_heads head
        LEFT JOIN harklight.stream_resource_revisions resource
          ON resource.person_id=head.person_id AND resource.origin_key=head.origin_key
          AND resource.resource_kind=$3 AND resource.resource_id=$4
        WHERE head.person_id=$1 AND head.origin_key=$2
          AND coalesce(resource.revision,head.seed_revision) > 0
        ON CONFLICT(person_id,origin_key,resource_kind,resource_id)
        DO UPDATE SET first_seen_at_ms=excluded.first_seen_at_ms
        """,
        [person, "", kind, id, at])
    end
  end

  defp row(db, "client_changes", [person, rev, kind, id, at]) do
    with {:ok, canonical} <- person_kind(id),
         true <- kind in ~w(item item_removed entry counts settings),
         true <- canonical == kind or (kind == "item_removed" and canonical == "item") do
      change(db, person, "", kind, id, rev, nil, at)
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_person_feed_kind}
    end
  end

  defp row(db, table, [person, agent, id, rev, session, at])
       when table in ~w(agent_changes agent_item_advisory_changes) do
    with {:ok, kind} <- agent_kind(id),
         :ok <- revision(db, person, agent, kind, id, rev) do
      if is_nil(at), do: :ok, else: change(db, person, agent, kind, id, rev, session, at)
    end
  end

  defp row(_, _, _), do: {:error, :unsupported_stream_row}

  defp head(db, person, origin, head, floor, seed),
    do: execute(db,
      "INSERT INTO harklight.stream_heads(person_id,origin_key,head,floor,seed_revision) VALUES ($1,$2,$3,$4,$5)",
      [person, origin, head, floor, seed])

  defp revision(db, person, origin, kind, id, rev),
    do: execute(db,
      """
      INSERT INTO harklight.stream_resource_revisions(person_id,origin_key,resource_kind,resource_id,revision)
      SELECT person_id,origin_key,$3,$4,$5 FROM harklight.stream_heads
      WHERE person_id=$1 AND origin_key=$2 AND head >= $5
      """, [person, origin, kind, id, rev])

  defp change(db, person, origin, kind, id, rev, session, at),
    do: execute(db,
      """
      INSERT INTO harklight.stream_changes(person_id,origin_key,resource_kind,resource_id,revision,session_handle,created_at_ms)
      SELECT person_id,origin_key,$3,$4,$5,$6,$7 FROM harklight.stream_heads
      WHERE person_id=$1 AND origin_key=$2 AND head >= $5
      """, [person, origin, kind, id, rev, session, at])

  defp each(entries, fun),
    do: Enum.reduce_while(entries, :ok, fn entry, :ok ->
      case fun.(entry) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)

  defp execute(db, sql, params) do
    case ImportSession.query(db, sql, params) do
      {:ok, %{num_rows: 1}} -> :ok
      {:ok, _} -> {:error, :stream_reference_mismatch}
      error -> error
    end
  end
end
