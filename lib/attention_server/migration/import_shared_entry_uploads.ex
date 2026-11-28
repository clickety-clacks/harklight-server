defmodule AttentionServer.Migration.ImportSharedEntryUploads do
  @moduledoc false
  alias AttentionServer.JSON
  alias AttentionServer.Migration.{Bundle, ImportRows, ImportSession, ImportSharedConversations, ImportUploads}
  alias AttentionServer.Repository.Postgres.Attributes

  # Staged against the core-owned v6 contract. No schema creation or startup use.
  def run(db, identity, bundle) do
    with {:ok, %{rows: [[version]]}} when is_integer(version) and version >= 6 <- ImportSession.query(db,
           "SELECT max(version) FROM harklight.schema_migrations", []),
         :ok <- ImportSharedConversations.run(db, identity, bundle),
         :ok <- ImportUploads.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, _} <- ImportRows.run(db, identity, "shared/entry-uploads/v1",
           Path.join(bundle, "records.frames"), manifest.data.records,
           &match?({{:row, :shared_entries, _}, _}, &1), &write/2) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :entry_upload_schema_required}
    end
  end

  defp write(db, rows) do
    each(rows, fn {{:row, :shared_entries, id}, %{id: id} = entry} ->
      with {:ok, record} <- source(db, :shared_messages, entry.shared_id),
           {:ok, person} <- author(entry.author, record) do
        each(Enum.with_index(Map.get(entry, :files, [])), fn {file, ordinal} ->
          link(db, record, entry, person, ordinal, file)
        end)
      end
    end)
  end

  defp author(:sender, record), do: {:ok, record.sender_person}
  defp author({:person, person}, _), do: {:ok, person}
  defp author(_, _), do: {:error, :invalid_shared_entry_author}

  defp link(db, record, entry, person, ordinal, %{"upload_id" => id} = file) do
    with {:ok, upload} <- source(db, :uploads, id),
         true <- upload.person == person and upload.message_id == record.id and
           upload.sender_id == record.sender_id and Map.get(upload, :shared_entry_id) == entry.id and
           upload.revision == entry.request_revision,
         [expected] = AttentionServer.ClientAPI.SharedAttachments.references([{upload, nil}]),
         true <- expected == file,
         {:ok, hash} <- Base.decode16(upload.sha256, case: :lower),
         {:ok, %{num_rows: 1}} <- ImportSession.query(db, """
           INSERT INTO harklight.conversation_entry_uploads(conversation_id,entry_id,ordinal,upload_id,attributes)
           SELECT e.conversation_id,e.id,$3,u.id,$4::text::jsonb
           FROM harklight.conversation_entries e
           JOIN harklight.conversations c ON c.id=e.conversation_id
           JOIN harklight.messages m ON m.id=c.message_id AND m.kind='shared'
           JOIN harklight.uploads u ON u.id=$5
           WHERE m.id=$1 AND e.id=$2 AND e.request_revision=$6
             AND u.person_id=$7 AND u.owner_key=$8 AND u.completed_at_ms IS NOT NULL
             AND u.size_bytes=$9 AND u.sha256=$10 AND u.attributes=$11::text::jsonb
           """, [record.id, entry.id, ordinal, encode(file), id, entry.request_revision,
             person, upload.uploader, upload.size, hash, encode(upload)]) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :shared_entry_upload_scope_mismatch}
    end
  end
  defp link(_, _, _, _, _, _), do: {:error, :shared_entry_upload_scope_mismatch}

  defp source(db, family, key) do
    case ImportSession.query(db,
      "SELECT body FROM harklight.compatibility_facts WHERE family=$1 AND address=$2",
      [Atom.to_string(family), Attributes.key({:row, family, key})]) do
      {:ok, %{rows: [[body]]}} -> Attributes.decode(body)
      {:error, _} = error -> error
      _ -> {:error, :shared_entry_upload_source_missing}
    end
  end
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
