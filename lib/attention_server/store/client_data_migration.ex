defmodule AttentionServer.Store.ClientDataMigration do
  @moduledoc false

  alias AttentionServer.{Engine, Message, RouteBIndex}

  def prepare(engine) do
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

    Engine.with_touches(engine, fn tracked ->
      next =
        tracked
        |> update_rows(:messages, &type_record/1)
        |> update_rows(:conversation_events, &type_event/1)
        |> type_origin_feed_revisions()

      {next, entries}
    end)
  end

  defp type_event(event), do: Map.update!(event, :payload, &type_revision/1)

  defp type_record(record) do
    record = Map.update!(record, :message, &Message.type_seen_only/1)

    case record do
      %{revisions: revisions} ->
        %{record | revisions: update_values(revisions, &Message.type_seen_only/1)}

      _ ->
        record
    end
  end

  defp update_rows(engine, field, update) do
    Enum.reduce(Map.fetch!(engine, field), engine, fn {key, value}, acc ->
      case update.(value) do
        ^value -> acc
        next -> Engine.put_row(acc, field, key, next)
      end
    end)
  end

  defp type_origin_feed_revisions(engine) do
    for {scope, feed} <- engine.origin_feeds, {seq, entry} <- feed.entries, reduce: engine do
      acc ->
        case type_revision(entry) do
          ^entry ->
            acc

          typed ->
            put_in(acc.origin_feeds[scope].entries[seq], typed)
            |> Engine.touch_fact({:origin_feed, scope, {:entry, seq}})
        end
    end
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
