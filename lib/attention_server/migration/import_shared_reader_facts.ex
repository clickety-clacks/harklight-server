defmodule AttentionServer.Migration.ImportSharedReaderFacts do
  @moduledoc false
  alias AttentionServer.{JSON, SharedMessages}
  alias AttentionServer.Migration.{Bundle, ImportRows, ImportSession, ImportSharedMessages, ImportUploads}
  alias AttentionServer.Repository.Postgres.Attributes

  def run(db, identity, bundle) do
    with :ok <- ImportSharedMessages.run(db, identity, bundle),
         :ok <- ImportUploads.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         :ok <- phase(db, identity, bundle, manifest, "people", [:persons]),
         :ok <- phase(db, identity, bundle, manifest, "organizations", [:local_organizations, :hosted_organizations]),
         :ok <- phase(db, identity, bundle, manifest, "messages", [:shared_messages]),
         :ok <- phase(db, identity, bundle, manifest, "recipients", [:shared_recipients]) do
      :ok
    end
  end

  defp phase(db, identity, bundle, manifest, name, fields) do
    select = fn {{:row, field, _}, _} -> field in fields; _ -> false end
    case ImportRows.run(db, identity, "shared/reader-" <> name <> "/v1",
      Path.join(bundle, "records.frames"), manifest.data.records, select,
      fn tx, rows -> each(rows, &write(tx, &1)) end) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  defp write(db, {{:row, :persons, person}, _}),
    do: fact(db, "shared_read_access", person, false)

  defp write(db, {{:row, :local_organizations, _}, %MapSet{} = members}),
    do: each(Enum.sort(members), &fact(db, "shared_read_access", &1, true))

  defp write(db, {{:row, :hosted_organizations, _}, %{members: members}}),
    do: each(Enum.sort(members), &fact(db, "shared_read_access", &1, true))

  defp write(db, {{:row, :shared_messages, id}, record}) do
    with :ok <- if(Map.has_key?(record, :sender_frozen_revision),
           do: fact(db, "shared_read_access", record.sender_person, true), else: :ok) do
      revisions = Map.keys(Map.get(record, :history, %{})) ++ [record.revision]
      each(Enum.sort(Enum.uniq(revisions)), fn revision ->
        historical = SharedMessages.historical_record(record, revision)
        files = Map.get(historical, :files, [])
        with :ok <- each(Enum.with_index(files), fn {file, ordinal} ->
               link(db, record, revision, ordinal, file)
             end) do
          fact(db, "shared_revision_files", {id, revision}, files)
        end
      end)
    end
  end

  defp write(db, {{:row, :shared_recipients, {id, person}}, recipient}) do
    with {:ok, %{rows: [[body]]}} <- ImportSession.query(db,
           "SELECT body FROM harklight.compatibility_facts WHERE family='shared_messages' AND address=$1",
           [Attributes.key({:row, :shared_messages, id})]),
         {:ok, record} <- Attributes.decode(body),
         :ok <- fact(db, "shared_revision_scope", {person, recipient.item_id},
           %{group?: Map.has_key?(record, :group_id), frozen_revision: get_in(recipient, [:frozen, :revision])}),
         :ok <- if(Map.has_key?(recipient, :frozen),
           do: fact(db, "shared_read_access", person, true), else: :ok) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :shared_reader_source_mismatch}
    end
  end

  defp link(db, record, revision, ordinal, %{"upload_id" => id} = file) do
    with {:ok, %{rows: [[source_body]]}} <- ImportSession.query(db,
           "SELECT body FROM harklight.compatibility_facts WHERE family='uploads' AND address=$1",
           [Attributes.key({:row, :uploads, id})]),
         {:ok, source} <- Attributes.decode(source_body),
         true <- source.person == record.sender_person and source.uploader == record.sender_key and
           source.message_id == record.id and source.sender_id == record.sender_id and
           not Map.has_key?(source, :shared_entry_id) and source.revision <= revision,
         original = SharedMessages.historical_record(record, source.revision),
         true <- Enum.any?(Map.get(original, :files, []), &(&1["upload_id"] == id)),
         [expected] = AttentionServer.ClientAPI.SharedAttachments.references([{source, nil}]),
         true <- file == expected,
         {:ok, hash} <- Base.decode16(source.sha256, case: :lower),
         {:ok, %{num_rows: 1}} <- ImportSession.query(db, """
           INSERT INTO harklight.message_uploads(message_id,revision,ordinal,upload_id,attributes)
           SELECT $1,$2,$3,id,$4::text::jsonb FROM harklight.uploads
           WHERE id=$5 AND person_id=$6 AND owner_key=$7 AND completed_at_ms IS NOT NULL
             AND size_bytes=$8 AND sha256=$9 AND attributes=$10::text::jsonb
           """, [record.id, revision, ordinal, JSON.encode!(Attributes.encode(file)), id,
             source.person, source.uploader, source.size, hash, JSON.encode!(Attributes.encode(source))]) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :shared_upload_scope_mismatch}
    end
  end
  defp link(_, _, _, _, _), do: {:error, :shared_upload_scope_mismatch}

  defp fact(db, family, key, value) do
    case ImportSession.query(db, """
      INSERT INTO harklight.compatibility_facts(family,address,body) VALUES($1,$2,$3::text::jsonb)
      ON CONFLICT(family,address) DO UPDATE SET body=EXCLUDED.body
      """, [family, Attributes.key(key), JSON.encode!(Attributes.encode(value))]) do
      {:ok, %{num_rows: 1}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, :shared_reader_fact_mismatch}
    end
  end

  defp each(rows, fun) do
    Enum.reduce_while(rows, :ok, fn row, :ok ->
      case fun.(row) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end
end
