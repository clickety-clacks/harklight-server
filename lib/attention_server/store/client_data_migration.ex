defmodule AttentionServer.Store.ClientDataMigration do
  @moduledoc false

  alias AttentionServer.{Message, RouteBIndex}

  def prepare(engine) do
    messages =
      update_values(engine.messages, fn record ->
        record = Map.update!(record, :message, &Message.type_seen_only/1)

        case record do
          %{revisions: revisions} ->
            %{record | revisions: update_values(revisions, &Message.type_seen_only/1)}

          _ ->
            record
        end
      end)

    conversations =
      update_values(engine.conversation_events, fn event ->
        Map.update!(event, :payload, &type_revision/1)
      end)

    feeds =
      update_values(engine.origin_feeds, fn feed ->
        %{feed | entries: update_values(feed.entries, &type_revision/1)}
      end)

    %{
      engine
      | messages: messages,
        conversation_events: mark_chat_without_receipts(engine, conversations),
        origin_feeds: feeds
    }
  end

  # An old transport receipt or ACK1 proved service but carried no handoff
  # expectation. A newer service declaration or explicit outcome supersedes it.
  defp mark_chat_without_receipts(engine, conversations) do
    {legacy, authoritative} =
      Enum.reduce(conversations, {MapSet.new(), MapSet.new()}, fn
        {_, %{key: key, payload: %{"event_id" => id} = payload}}, {old, known}
        when is_binary(id) ->
          target = {key, id}

          case delivery_fact(payload) do
            :legacy -> {MapSet.put(old, target), known}
            :authoritative -> {old, MapSet.put(known, target)}
            nil -> {old, known}
          end

        _, acc ->
          acc
      end)

    legacy
    |> MapSet.difference(authoritative)
    |> Enum.reduce(conversations, fn {{person, _, _} = key, id}, acc ->
      seq = RouteBIndex.entry_position(engine.route_b_index, key, id)

      case Map.get(acc, {person, seq}) do
        %{payload: %{"kind" => "discussion", "author" => "person"} = payload} = fact ->
          Map.put(acc, {person, seq}, %{fact | payload: Map.put(payload, "receipt_expected", false)})

        _ ->
          acc
      end
    end)
  end

  defp update_values(map, update) do
    Enum.reduce(map, map, fn {key, value}, acc ->
      case update.(value) do
        ^value -> acc
        updated -> Map.put(acc, key, updated)
      end
    end)
  end

  defp type_revision(%{"kind" => "revision", "message" => message} = entry),
    do: %{entry | "message" => Message.type_seen_only(message)}

  defp type_revision(entry), do: entry

  defp delivery_fact(%{"kind" => "receipt", "delivered" => true}), do: :legacy

  defp delivery_fact(%{"kind" => "delivery_ack", "stage" => "ack1"} = ack) do
    if ack["r25_served"] == true or ack["ack2_expected"] == true,
      do: :authoritative,
      else: :legacy
  end

  defp delivery_fact(%{"kind" => "delivery_ack", "stage" => "ack2", "outcome" => outcome})
       when outcome in ["handed", "not_handed"],
       do: :authoritative

  defp delivery_fact(_), do: nil
end
