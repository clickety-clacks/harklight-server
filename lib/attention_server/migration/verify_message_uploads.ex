defmodule AttentionServer.Migration.VerifyMessageUploads do
  @moduledoc false
  alias AttentionServer.SharedMessages
  alias AttentionServer.Migration.{Bundle, Frames, ImportMessages, ImportSession, VerifyUploads}
  alias AttentionServer.Repository.Postgres.Attributes

  # Final validation must check both kinds. A scoped check also supports each
  # independently checkpointed history projection while the database is loading.
  def run(db, identity, bundle, kind) when kind in [:ordinary, :shared] do
    with {:ok, _} <- VerifyUploads.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, count} <- Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, 0,
           fn row, count -> verify(db, kind, row, count) end),
         {:ok, %{rows: [[^count]]}} <- ImportSession.query(db,
           "SELECT count(*) FROM harklight.message_uploads u JOIN harklight.messages m ON m.id=u.message_id WHERE m.kind=$1",
           [Atom.to_string(kind)]) do
      {:ok, %{kind: kind, links: count}}
    else
      {:error, _} = error -> error
      _ -> {:error, {:message_upload_count_mismatch, kind}}
    end
  end

  defp verify(db, :ordinary, {{:row, :messages, key}, record}, count) do
    revisions(db, :ordinary, ImportMessages.id(key, record),
      Map.get(record, :files_by_revision, %{}), count)
  end
  defp verify(db, :shared, {{:row, :shared_messages, id}, record}, count) do
    revisions = Map.get(record, :history, %{}) |> Map.keys() |> Enum.concat([record.revision]) |> Enum.uniq()
    files = Enum.map(revisions, fn revision ->
      {revision, Map.get(SharedMessages.historical_record(record, revision), :files, [])}
    end)
    revisions(db, :shared, id, files, count)
  end
  defp verify(_, _, _, count), do: {:ok, count}

  defp revisions(db, kind, id, revisions, count) do
    Enum.reduce_while(revisions, {:ok, count}, fn {revision, files}, {:ok, acc} ->
      case files(db, kind, id, revision, files, acc) do
        {:ok, _} = result -> {:cont, result}
        error -> {:halt, error}
      end
    end)
  end

  defp files(db, kind, id, revision, files, count) do
    Enum.reduce_while(Enum.with_index(files), {:ok, count}, fn {file, ordinal}, {:ok, acc} ->
      case {kind, file} do
        {:ordinary, %{"tier" => "inline"}} -> {:cont, {:ok, acc}}
        {_, %{"upload_id" => upload}} ->
          expected = [upload, Attributes.encode(file), Atom.to_string(kind)]
          case ImportSession.query(db,
            "SELECT u.upload_id,u.attributes,m.kind FROM harklight.message_uploads u JOIN harklight.messages m ON m.id=u.message_id WHERE u.message_id=$1 AND u.revision=$2 AND u.ordinal=$3",
            [id, revision, ordinal]) do
            {:ok, %{rows: [^expected]}} -> {:cont, {:ok, acc + 1}}
            {:error, _} = error -> {:halt, error}
            _ -> {:halt, mismatch(kind, id, revision, ordinal)}
          end
        _ -> {:halt, {:error, :unsupported_attachment_reference}}
      end
    end)
  end

  defp mismatch(kind, id, revision, ordinal), do:
    {:error, {:message_upload_mismatch, kind,
      Base.encode16(:crypto.hash(:sha256, id), case: :lower), revision, ordinal}}
end
