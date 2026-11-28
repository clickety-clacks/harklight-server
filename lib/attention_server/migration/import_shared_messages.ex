defmodule AttentionServer.Migration.ImportSharedMessages do
  @moduledoc false
  alias AttentionServer.JSON
  alias AttentionServer.Migration.{Bundle, ImportLargeText, ImportOrganizations, ImportRows, ImportSession}
  alias AttentionServer.Repository.Postgres.Attributes

  # Explicit schema 005 mapping. Never creates a principal or resurrects a
  # deleted group to make historical provenance satisfy live authority.
  def run(db, identity, bundle) do
    with {:ok, %{rows: [[version]]}} when is_integer(version) and version >= 5 <- ImportSession.query(db,
           "SELECT max(version) FROM harklight.schema_migrations", []),
         :ok <- ImportOrganizations.run(db, identity, bundle),
         {:ok, _} <- ImportLargeText.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, _} <- phase(db, identity, bundle, manifest, :shared_messages, &messages/2),
         {:ok, _} <- phase(db, identity, bundle, manifest, :shared_recipients, &recipients/2) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :shared_history_schema_required}
    end
  end

  defp phase(db, identity, bundle, manifest, field, write) do
    ImportRows.run(db, identity, "shared/" <> Atom.to_string(field) <> "/v1",
      Path.join(bundle, "records.frames"), manifest.data.records,
      fn {{:row, source, _}, _} -> source == field; _ -> false end, write)
  end

  defp messages(db, rows) do
    each(rows, fn {{:row, :shared_messages, id} = address, source} ->
      with {:ok, %{id: ^id} = record} <- ImportLargeText.project(db, address, source),
           :ok <- execute(db, """
             INSERT INTO harklight.messages
               (id,kind,person_id,origin_key,sender_id,message_id,session_handle,group_id,
                revision,accepted_at_ms,accepted_position,free_order,terminal,attributes)
             VALUES($1,'shared',$2,$3,$4,$1,NULL,(SELECT id FROM harklight.groups WHERE id=$5),
                    $6,$7,NULL,NULL,$8::text::jsonb,$9::text::jsonb)
             """, [id, record.sender_person, record.sender_key, record.sender_id,
               Map.get(record, :group_id), record.revision, record["created_at_ms"],
               optional(Map.get(record, :terminal)), encode(record)]),
           :ok <- revisions(db, id, record) do
        :ok
      else
        {:error, _} = error -> error
        _ -> {:error, :shared_source_identity_mismatch}
      end
    end)
  end

  defp revisions(db, id, record) do
    history = Map.get(record, :history, %{})
    versions = Map.put(history, record.revision, %{message: record.message})
    each(Enum.sort(versions), fn {revision, %{message: message}} ->
      at = cond do
        revision == 1 -> record["created_at_ms"]
        revision == record.revision -> Map.get(record, :revision_accepted_at_ms)
        true -> nil
      end
      execute(db,
        "INSERT INTO harklight.message_revisions(message_id,revision,accepted_at_ms,body) VALUES($1,$2,$3,$4::text::jsonb)",
        [id, revision, at, encode(message)])
    end)
  end

  defp recipients(db, rows) do
    each(rows, fn {{:row, :shared_recipients, {id, person}} = address, source} ->
      with {:ok, record} <- ImportLargeText.project(db, address, source) do
        execute(db, """
          INSERT INTO harklight.message_recipients(message_id,person_id,item_id,seen_revision,attributes)
          SELECT id,$2,$3,$4,$5::text::jsonb FROM harklight.messages WHERE id=$1 AND kind='shared'
          """, [id, person, record.item_id, Map.get(record, :seen_revision), encode(record)])
      end
    end)
  end

  defp optional(nil), do: nil
  defp optional(value), do: encode(value)
  defp encode(value), do: JSON.encode!(Attributes.encode(value))
  defp execute(db, sql, params) do
    case ImportSession.query(db, sql, params) do
      {:ok, %{num_rows: 1}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, :shared_reference_mismatch}
    end
  end
  defp each(rows, write) do
    Enum.reduce_while(rows, :ok, fn row, :ok ->
      case write.(row) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end
end
