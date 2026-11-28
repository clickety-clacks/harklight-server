defmodule AttentionServer.Migration.VerifySharedConversations do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportLargeText, ImportSession, VerifySharedMessages}
  alias AttentionServer.Repository.Postgres.Attributes

  def run(db, identity, bundle) do
    with {:ok, _} <- VerifySharedMessages.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, counts} <- Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records,
           %{conversations: 0, entries: 0, answers: 0, read_positions: 0}, fn row, counts ->
             case verify(db, row) do
               {:ok, nil} -> {:ok, counts}
               {:ok, field} -> {:ok, Map.update!(counts, field, &(&1 + 1))}
               error -> error
             end
           end),
         :ok <- counts(db, counts) do
      {:ok, counts}
    end
  end

  defp verify(db, {{:row, :shared_messages, id}, _}) do
    compare(db, :conversations, id,
      "SELECT message_id,attributes FROM harklight.conversations WHERE id=$1",
      [id], [id, Attributes.encode(%{shared_id: id})])
  end

  defp verify(db, {{:row, :shared_entries, id} = address, source}) do
    with {:ok, record} <- ImportLargeText.project(db, address, source) do
      compare(db, :entries, id,
        "SELECT position,request_revision,kind,body FROM harklight.conversation_entries WHERE conversation_id=$1 AND id=$2",
        [record.shared_id, id], [record.position, record.request_revision, "reply", Attributes.encode(record)])
    end
  end

  defp verify(db, {{:row, :shared_answers, id} = address, source}) do
    with {:ok, record} <- ImportLargeText.project(db, address, source) do
      compare(db, :answers, id,
        "SELECT message_id,person_id,request_revision,body FROM harklight.answers WHERE id=$1",
        [id], [record.shared_id, record.person, record.request_revision, Attributes.encode(record)])
    end
  end

  defp verify(db, {{:row, :shared_recipients, {id, person}}, %{read_position: position}}) do
    compare(db, :read_positions, id,
      "SELECT position FROM harklight.read_positions WHERE conversation_id=$1 AND person_id=$2",
      [id, person], [position])
  end
  defp verify(_, _), do: {:ok, nil}

  defp compare(db, field, id, sql, params, expected) do
    case ImportSession.query(db, sql, params) do
      {:ok, %{rows: [^expected]}} -> {:ok, field}
      {:error, _} = error -> error
      _ -> {:error, {:shared_conversation_mismatch, field,
        Base.encode16(:crypto.hash(:sha256, id), case: :lower)}}
    end
  end

  defp counts(db, expected) do
    queries = [
      conversations: "SELECT count(*) FROM harklight.conversations c JOIN harklight.messages m ON m.id=c.message_id WHERE m.kind='shared'",
      entries: "SELECT count(*) FROM harklight.conversation_entries e JOIN harklight.conversations c ON c.id=e.conversation_id JOIN harklight.messages m ON m.id=c.message_id WHERE m.kind='shared'",
      answers: "SELECT count(*) FROM harklight.answers a JOIN harklight.messages m ON m.id=a.message_id WHERE m.kind='shared'",
      read_positions: "SELECT count(*) FROM harklight.read_positions r JOIN harklight.conversations c ON c.id=r.conversation_id JOIN harklight.messages m ON m.id=c.message_id WHERE m.kind='shared'"
    ]
    Enum.reduce_while(queries, :ok, fn {field, sql}, :ok ->
      count = Map.fetch!(expected, field)
      case ImportSession.query(db, sql, []) do
        {:ok, %{rows: [[^count]]}} -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
        _ -> {:halt, {:error, {:shared_conversation_count_mismatch, field}}}
      end
    end)
  end
end
