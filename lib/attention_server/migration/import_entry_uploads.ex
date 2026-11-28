defmodule AttentionServer.Migration.ImportEntryUploads do
  @moduledoc false
  alias AttentionServer.JSON
  alias AttentionServer.Migration.{Bundle, ImportEntries, ImportRows, ImportSession, ImportUploads}
  alias AttentionServer.Repository.Postgres.Attributes

  def run(db, identity, bundle) do
    with {:ok, %{rows: [[version]]}} when is_integer(version) and version >= 6 <- ImportSession.query(db,
           "SELECT max(version) FROM harklight.schema_migrations", []),
         :ok <- ImportEntries.run(db, identity, bundle),
         :ok <- ImportUploads.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, _} <- ImportRows.run(db, identity, "ordinary/entry-uploads/v1",
           Path.join(bundle, "records.frames"), manifest.data.records,
           &match?({{:row, :conversation_events, _}, _}, &1), &write/2) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :entry_upload_schema_required}
    end
  end

  defp write(db, rows) do
    each(rows, fn {{:row, :conversation_events, scope}, event} ->
      with {:ok, {id, _, _, revision}} <- ImportEntries.fields(scope, event) do
        each(Enum.with_index(Map.get(event.payload, "files", [])), fn {file, ordinal} ->
          link(db, event, id, revision, ordinal, file)
        end)
      end
    end)
  end

  # Inline bytes remain in the exact typed entry body and consume their original
  # ordinal. Only durable uploads need a relational reference for cleanup/readback.
  defp link(_, _, _, _, _, %{"tier" => "inline"}), do: :ok
  defp link(db, %{key: {person, sender, external}, payload: payload}, entry_id, revision, ordinal,
       %{"tier" => "upload", "upload_id" => upload_id} = file) do
    with {:ok, %{rows: [[body]]}} <- ImportSession.query(db,
           "SELECT body FROM harklight.compatibility_facts WHERE family='uploads' AND address=$1",
           [Attributes.key({:row, :uploads, upload_id})]),
         {:ok, upload} <- Attributes.decode(body),
         true <- upload.person == person and upload.sender_id == sender and
           upload.message_id == external and upload.revision == revision and
           upload.op_id == payload["op_id"] and is_nil(Map.get(upload, :shared_entry_id)),
         true <- file == %{"tier" => "upload", "upload_id" => upload_id,
           "name" => upload.name, "size" => upload.size, "sha256" => upload.sha256},
         {:ok, hash} <- Base.decode16(upload.sha256, case: :lower),
         {:ok, %{num_rows: 1}} <- ImportSession.query(db, """
           INSERT INTO harklight.conversation_entry_uploads(conversation_id,entry_id,ordinal,upload_id,attributes)
           SELECT e.conversation_id,e.id,$5,u.id,$6::text::jsonb
           FROM harklight.messages m
           JOIN harklight.conversations c ON c.message_id=m.id
           JOIN harklight.conversation_entries e ON e.conversation_id=c.id AND e.id=$4
           JOIN harklight.uploads u ON u.id=$7
           WHERE m.kind='ordinary' AND m.person_id=$1 AND m.sender_id=$2 AND m.message_id=$3
             AND e.request_revision=$8 AND u.person_id=$1 AND u.owner_key=$9
             AND u.completed_at_ms IS NOT NULL AND u.size_bytes=$10 AND u.sha256=$11
             AND u.attributes=$12::text::jsonb
           """, [person, sender, external, entry_id, ordinal, encode(file), upload_id,
             revision, upload.uploader, upload.size, hash, encode(upload)]) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :ordinary_entry_upload_scope_mismatch}
    end
  end
  defp link(_, _, _, _, _, _), do: {:error, :ordinary_entry_upload_scope_mismatch}

  defp encode(value), do: JSON.encode!(Attributes.encode(value))
  defp each(rows, fun) do
    Enum.reduce_while(rows, :ok, fn row, :ok ->
      case fun.(row) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end
end
