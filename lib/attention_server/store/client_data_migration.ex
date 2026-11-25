defmodule AttentionServer.Store.ClientDataMigration do
  @moduledoc false

  alias AttentionServer.{Engine, Message, RouteBIndex}

  def prepare(engine) do
    {next, :ok, touched} =
      Engine.with_touches(engine, fn tracked ->
        next =
          tracked
          |> update_rows(:messages, &type_record/1)
          |> update_rows(:conversation_events, &type_event/1)
          |> type_origin_feed_revisions()
          |> mark_chat_without_receipts()

        {next, :ok}
      end)

    {next, touched}
  end

  defp mark_chat_without_receipts(engine) do
    {legacy, authoritative} =
      Enum.reduce(engine.conversation_events, {MapSet.new(), MapSet.new()}, fn
        {_, %{key: key, payload: %{"event_id" => id} = payload}}, {old, known}
        when is_binary(id) ->
          target = {key, id}

          case delivery_fact(payload) do
            :legacy -> {MapSet.put(old, target), known}
            :authoritative -> {old, MapSet.put(known, target)}
            nil -> {old, known}
          end

        _, acc -> acc
      end)

    legacy
    |> MapSet.difference(authoritative)
    |> Enum.reduce(engine, fn {{person, _, _} = key, id}, acc ->
      seq = RouteBIndex.entry_position(engine.route_b_index, key, id)

      case Map.get(acc.conversation_events, {person, seq}) do
        %{payload: %{"kind" => "discussion", "author" => "person"} = payload} = fact ->
          updated = %{fact | payload: Map.put(payload, "receipt_expected", false)}
          Engine.put_row(acc, :conversation_events, {person, seq}, updated)

        _ -> acc
      end
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
