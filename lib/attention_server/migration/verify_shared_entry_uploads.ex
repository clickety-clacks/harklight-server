defmodule AttentionServer.Migration.VerifySharedEntryUploads do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportSession, VerifySharedConversations, VerifyUploads}
  alias AttentionServer.Repository.Postgres.Attributes

  # Shared links only; ordinary entry links require their own source mapping.
  # Global shared-kind count rejects extra links without consuming all rows.
  def run(db, identity, bundle) do
    with {:ok, _} <- VerifySharedConversations.run(db, identity, bundle),
         {:ok, _} <- VerifyUploads.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, count} <- Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, 0,
           fn row, count -> verify(db, row, count) end),
         {:ok, %{rows: [[^count]]}} <- ImportSession.query(db, """
           SELECT count(*) FROM harklight.conversation_entry_uploads u
           JOIN harklight.conversations c ON c.id=u.conversation_id
           JOIN harklight.messages m ON m.id=c.message_id WHERE m.kind='shared'
           """, []) do
      {:ok, %{shared_entry_links: count}}
    else
      {:error, _} = error -> error
      _ -> {:error, :shared_entry_upload_count_mismatch}
    end
  end

  defp verify(db, {{:row, :shared_entries, id}, entry}, count) do
    Enum.reduce_while(Enum.with_index(Map.get(entry, :files, [])), {:ok, count},
      fn {file, ordinal}, {:ok, acc} ->
        expected = [Map.fetch!(file, "upload_id"), Attributes.encode(file), entry.request_revision, "shared"]
        case ImportSession.query(db, """
          SELECT u.upload_id,u.attributes,e.request_revision,m.kind
          FROM harklight.conversation_entry_uploads u
          JOIN harklight.conversation_entries e ON e.conversation_id=u.conversation_id AND e.id=u.entry_id
          JOIN harklight.conversations c ON c.id=e.conversation_id
          JOIN harklight.messages m ON m.id=c.message_id
          WHERE u.conversation_id=$1 AND u.entry_id=$2 AND u.ordinal=$3
          """, [entry.shared_id, id, ordinal]) do
          {:ok, %{rows: [^expected]}} -> {:cont, {:ok, acc + 1}}
          {:error, _} = error -> {:halt, error}
          _ -> {:halt, {:error, {:shared_entry_upload_mismatch,
            Base.encode16(:crypto.hash(:sha256, id), case: :lower), ordinal}}}
        end
      end)
  end
  defp verify(_, _, count), do: {:ok, count}
end
