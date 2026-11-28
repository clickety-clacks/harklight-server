defmodule AttentionServer.Migration.VerifyConversations do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportEntries, ImportLargeText, ImportMessages, ImportSession, VerifyMessages}
  alias AttentionServer.Repository.Postgres.Attributes

  def run(db, identity, bundle) do
    with {:ok, %{ordinary_messages: messages}} <- VerifyMessages.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         path = Path.join(bundle, "records.frames"),
         {:ok, {entries, reads}} <- Frames.fold(path, manifest.data.records, {0, 0}, fn
           {{:row, :conversation_events, scope} = address, event}, {entries, reads} ->
             with :ok <- entry(db, address, scope, event), do: {:ok, {entries + 1, reads}}
           {{:row, :chat_read_positions, key}, position}, {entries, reads} ->
             with :ok <- read_position(db, key, position), do: {:ok, {entries, reads + 1}}
           _, counts -> {:ok, counts}
         end),
         :ok <- counts(db, messages, entries, reads),
         {:ok, :ok} <- Frames.fold(path, manifest.data.records, :ok, fn
           {{:row, :messages, key}, record}, :ok ->
             with :ok <- summary(db, key, record), do: {:ok, :ok}
           _, :ok -> {:ok, :ok}
         end) do
      {:ok, %{conversations: messages, entries: entries, read_positions: reads}}
    end
  end

  defp message(db, key) do
    with {:ok, %{rows: [[body]]}} <- ImportSession.query(db,
           "SELECT body FROM harklight.compatibility_facts WHERE family='messages' AND address=$1",
           [Attributes.key({:row, :messages, key})]),
         {:ok, record} <- Attributes.decode(body) do
      {:ok, ImportMessages.id(key, record)}
    else
      {:error, _} = error -> error
      _ -> {:error, :conversation_source_missing}
    end
  end

  defp entry(db, address, scope, event) do
    with {:ok, id} <- message(db, event.key),
         {:ok, {row_id, position, kind, revision}} <- ImportEntries.fields(scope, event),
         {:ok, projected} <- ImportLargeText.project(db, address, event),
         body = Attributes.encode(projected),
         {:ok, %{rows: [[^position, ^kind, ^revision, ^body]]}} <- ImportSession.query(db,
           "SELECT position,kind,request_revision,body FROM harklight.conversation_entries WHERE conversation_id=$1 AND id=$2", [id, row_id]) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :conversation_entry_mismatch}
    end
  end

  defp read_position(db, {person, _, _} = key, position) do
    with {:ok, id} <- message(db, key),
         {:ok, %{rows: [[^position]]}} <- ImportSession.query(db,
           "SELECT position FROM harklight.read_positions WHERE conversation_id=$1 AND person_id=$2", [id, person]) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :conversation_read_position_mismatch}
    end
  end

  defp counts(db, messages, entries, reads) do
    tables = [{"conversations", "t.message_id", messages},
      {"conversation_entries", "c.message_id", entries}, {"read_positions", "c.message_id", reads}]
    Enum.reduce_while(tables, :ok, fn {table, message, expected}, :ok ->
      join = if table == "conversations", do: "", else: " JOIN harklight.conversations c ON c.id=t.conversation_id"
      sql = "SELECT count(*) FROM harklight." <> table <> " t" <> join <>
        " JOIN harklight.messages m ON m.id=" <> message <> " WHERE m.kind='ordinary'"
      case ImportSession.query(db, sql, []) do
        {:ok, %{rows: [[^expected]]}} -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
        _ -> {:halt, {:error, {:conversation_count_mismatch, table}}}
      end
    end)
  end

  defp summary(db, {person, _, _} = key, record) do
    id = ImportMessages.id(key, record)
    with {:ok, %{rows: [[^id, body, read]]}} <- ImportSession.query(db,
           "SELECT c.message_id,c.attributes,coalesce(r.position,0) FROM harklight.conversations c LEFT JOIN harklight.read_positions r ON r.conversation_id=c.id AND r.person_id=$2 WHERE c.id=$1", [id, person]),
         {:ok, expected} <- pages(db, id, read, -1,
           %{source_key: key, chat_started: false, latest_origin_reply_seq: nil, unread_replies: 0}),
         true <- Attributes.encode(expected) == body do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :conversation_summary_mismatch}
    end
  end

  # Rows have already been compared field-for-field with source above. Keep
  # only scalar aggregates while traversing them in original journal order.
  defp pages(db, id, read, after_position, summary) do
    with {:ok, %{rows: rows}} <- ImportSession.query(db,
           "SELECT position,body FROM harklight.conversation_entries WHERE conversation_id=$1 AND position>$2 ORDER BY position LIMIT 256", [id, after_position]),
         {:ok, next} <- Enum.reduce_while(rows, {:ok, summary}, fn [position, body], {:ok, acc} ->
           case Attributes.decode(body) do
             {:ok, %{payload: payload}} ->
               reply = payload["kind"] == "reply" and payload["author"] == "origin"
               {:cont, {:ok, %{acc | chat_started: acc.chat_started or payload["kind"] in ["discussion", "reply"],
                 latest_origin_reply_seq: if(reply, do: position, else: acc.latest_origin_reply_seq),
                 unread_replies: acc.unread_replies + if(reply and position > read, do: 1, else: 0)}}}
             _ -> {:halt, {:error, :conversation_entry_mismatch}}
           end
         end) do
      case rows do
        [] -> {:ok, next}
        _ -> pages(db, id, read, hd(List.last(rows)), next)
      end
    end
  end
end
