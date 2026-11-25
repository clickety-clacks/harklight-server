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

    # The old caption was derived from these retained facts. Emit new Entry
    # revisions so clients that already hold a snapshot get the new projection.
    # The facts themselves remain unchanged.
    entries =
      for {{person, _}, %{key: key, payload: payload}} <- engine.conversation_events,
          legacy_delivery?(payload),
          seq = RouteBIndex.entry_position(engine.route_b_index, key, payload["event_id"]),
          is_integer(seq),
          %{payload: %{"author" => "person"}} <- [engine.conversation_events[{person, seq}]],
          do: {person, seq}

    {%{engine | messages: messages, conversation_events: conversations, origin_feeds: feeds}, entries}
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

  defp legacy_delivery?(%{"kind" => "receipt", "delivered" => true}), do: true

  defp legacy_delivery?(%{"kind" => "delivery_ack", "stage" => "ack1"} = ack),
    do: ack["r25_served"] != true and ack["ack2_expected"] != true

  defp legacy_delivery?(_), do: false
end
