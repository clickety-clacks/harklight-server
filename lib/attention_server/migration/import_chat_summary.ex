defmodule AttentionServer.Migration.ImportChatSummary do
  @moduledoc false
  alias AttentionServer.JSON
  alias AttentionServer.Migration.{Bundle, ImportEntries, ImportMessages, ImportRows, ImportSession}
  alias AttentionServer.Repository.Postgres.Attributes

  def run(db, identity, bundle) do
    with :ok <- ImportEntries.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, _} <- ImportRows.run(db, identity, "conversations/summary/v1",
           Path.join(bundle, "records.frames"), manifest.data.records,
           &match?({{:row, :messages, _}, _}, &1), &write/2) do
      :ok
    end
  end

  defp write(db, rows) do
    Enum.reduce_while(rows, :ok, fn {{:row, :messages, {person, _, _} = key}, record}, :ok ->
      id = ImportMessages.id(key, record)
      result = with {:ok, %{rows: [[body, read_through]]}} <- ImportSession.query(db,
                      "SELECT c.attributes,coalesce(r.position,0) FROM harklight.conversations c LEFT JOIN harklight.read_positions r ON r.conversation_id=c.id AND r.person_id=$2 WHERE c.id=$1",
                      [id, person]),
                    {:ok, attributes} when is_map(attributes) <- Attributes.decode(body),
                    {:ok, summary} <- pages(db, id, read_through, -1,
                      %{chat_started: false, latest_origin_reply_seq: nil, unread_replies: 0}),
                    {:ok, %{num_rows: 1}} <- ImportSession.query(db,
                      "UPDATE harklight.conversations SET attributes=$2::text::jsonb WHERE id=$1",
                      [id, JSON.encode!(Attributes.encode(Map.merge(attributes, summary)))]) do
        :ok
      else
        {:error, _} = error -> error
        _ -> {:error, :conversation_summary_reference_mismatch}
      end
      case result do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  # Read a bounded page, retaining only three scalar summary values. Updating
  # the summary and checkpoint together makes retries recompute, not double count.
  defp pages(db, id, read_through, after_position, summary) do
    with {:ok, %{rows: rows}} <- ImportSession.query(db,
           "SELECT position,body FROM harklight.conversation_entries WHERE conversation_id=$1 AND position>$2 ORDER BY position LIMIT 256",
           [id, after_position]),
         {:ok, next} <- Enum.reduce_while(rows, {:ok, summary}, fn [position, body], {:ok, acc} ->
           case Attributes.decode(body) do
             {:ok, %{payload: payload}} when is_map(payload) ->
               origin_reply = payload["kind"] == "reply" and payload["author"] == "origin"
               updated = %{acc |
                 chat_started: acc.chat_started or payload["kind"] in ["discussion", "reply"],
                 latest_origin_reply_seq: if(origin_reply, do: position, else: acc.latest_origin_reply_seq),
                 unread_replies: acc.unread_replies + if(origin_reply and position > read_through, do: 1, else: 0)}
               {:cont, {:ok, updated}}
             _ -> {:halt, {:error, :invalid_conversation_summary_event}}
           end
         end) do
      case rows do
        [] -> {:ok, next}
        _ -> pages(db, id, read_through, hd(List.last(rows)), next)
      end
    end
  end
end
