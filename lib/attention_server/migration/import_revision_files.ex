defmodule AttentionServer.Migration.ImportRevisionFiles do
  @moduledoc false
  alias AttentionServer.{JSON, ClientAPI.Resources}
  alias AttentionServer.Migration.{Bundle, ImportRecipients, ImportRows, ImportSession, ImportUploads}
  alias AttentionServer.Repository.Postgres.Attributes

  def run(db, identity, bundle) do
    with :ok <- ImportUploads.run(db, identity, bundle),
         :ok <- ImportRecipients.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, _} <- ImportRows.run(db, identity, "messages/revision-files/v1",
           Path.join(bundle, "records.frames"), manifest.data.records,
           &match?({{:row, :messages, _}, _}, &1), &write/2) do
      :ok
    end
  end

  defp write(db, rows) do
    each(rows, fn {{:row, :messages, {person, _, _}}, record} ->
      revisions = Map.get(record, :revisions, %{1 => record.message})
      each(Enum.sort(Map.keys(revisions)), fn revision ->
        files = record |> Map.get(:files_by_revision, %{}) |> Map.get(revision, [])
        with {:ok, uploads} <- metadata(db, person, files),
             projected = Resources.revision_file_refs(record.accepted_seq, revision, files, uploads),
             {:ok, %{num_rows: 1}} <- ImportSession.query(db,
               "INSERT INTO harklight.compatibility_facts(family,address,body) VALUES('ordinary_revision_files',$1,$2::text::jsonb)",
               [Attributes.key({person, Resources.item_id(record), revision}), JSON.encode!(Attributes.encode(projected))]) do
          :ok
        else
          {:error, _} = error -> error
          _ -> {:error, :revision_file_projection_mismatch}
        end
      end)
    end)
  end

  # Only metadata for this revision's references is read. Upload bytes and link
  # authority are independently imported before this bounded reader projection.
  defp metadata(db, person, files) do
    Enum.reduce_while(files, {:ok, %{}}, fn
      %{"tier" => "upload", "upload_id" => id}, {:ok, acc} ->
        case ImportSession.query(db,
          "SELECT body FROM harklight.compatibility_facts WHERE family='uploads' AND address=$1",
          [Attributes.key({:row, :uploads, id})]) do
          {:ok, %{rows: []}} -> {:cont, {:ok, acc}}
          {:ok, %{rows: [[body]]}} ->
            case Attributes.decode(body) do
              {:ok, %{person: ^person} = value} -> {:cont, {:ok, Map.put(acc, id, value)}}
              _ -> {:halt, {:error, :revision_file_owner_mismatch}}
            end
          error -> {:halt, error}
        end
      _, result -> {:cont, result}
    end)
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
