defmodule AttentionServer.Migration.VerifyEntryUploads do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportEntries, ImportSession, VerifyConversations, VerifyUploads}
  alias AttentionServer.Repository.Postgres.Attributes

  # Ordinary entry links only. Shared links have their own exact source verifier.
  def run(db, identity, bundle) do
    with {:ok, _} <- VerifyConversations.run(db, identity, bundle),
         {:ok, _} <- VerifyUploads.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, count} <- Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, 0,
           fn row, count -> verify(db, row, count) end),
         {:ok, %{rows: [[^count]]}} <- ImportSession.query(db, """
           SELECT count(*) FROM harklight.conversation_entry_uploads u
           JOIN harklight.conversations c ON c.id=u.conversation_id
           JOIN harklight.messages m ON m.id=c.message_id WHERE m.kind='ordinary'
           """, []) do
      {:ok, %{ordinary_entry_links: count}}
    else
      {:error, _} = error -> error
      _ -> {:error, :ordinary_entry_upload_count_mismatch}
    end
  end

  defp verify(db, {{:row, :conversation_events, scope}, event}, count) do
    with {:ok, {id, _, _, revision}} <- ImportEntries.fields(scope, event) do
      {person, sender, external} = event.key
      Enum.reduce_while(Enum.with_index(Map.get(event.payload, "files", [])), {:ok, count},
        fn {file, ordinal}, {:ok, acc} ->
          if file["tier"] == "inline" do
            {:cont, {:ok, acc}}
          else
            expected = [Map.fetch!(file, "upload_id"), Attributes.encode(file), revision]
            case ImportSession.query(db, """
              SELECT u.upload_id,u.attributes,e.request_revision
              FROM harklight.conversation_entry_uploads u
              JOIN harklight.conversation_entries e ON e.conversation_id=u.conversation_id AND e.id=u.entry_id
              JOIN harklight.conversations c ON c.id=e.conversation_id
              JOIN harklight.messages m ON m.id=c.message_id
              WHERE m.kind='ordinary' AND m.person_id=$1 AND m.sender_id=$2 AND m.message_id=$3
                AND u.entry_id=$4 AND u.ordinal=$5
              """, [person, sender, external, id, ordinal]) do
              {:ok, %{rows: [^expected]}} -> {:cont, {:ok, acc + 1}}
              {:error, _} = error -> {:halt, error}
              _ -> {:halt, {:error, {:ordinary_entry_upload_mismatch,
                Base.encode16(:crypto.hash(:sha256, Attributes.key(scope)), case: :lower), ordinal}}}
            end
          end
        end)
    end
  end
  defp verify(_, _, count), do: {:ok, count}
end
