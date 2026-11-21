defmodule AttentionServer.ReplyPriority do
  @moduledoc false

  alias AttentionServer.{Message, RouteBIndex}

  # The selected ranking emits scores. Baseline and fallback derive the
  # displayed level from the same message urgency.
  def project(engine, key, position, record) do
    priority = %{"level" => Message.effective_urgency(record.message)}

    case get_in(engine.reply_group_history, [key, :scores, position]) do
      score when is_number(score) -> Map.put(priority, "score", score)
      _ -> priority
    end
  end

  def changed_score_entries(_person, previous, current) when previous === current, do: []

  def changed_score_entries(person, previous, current) do
    changed =
      for {position, score} <- current,
          Map.get(previous, position) != score,
          do: {person, position}

    removed =
      for {position, _score} <- previous,
          not Map.has_key?(current, position),
          do: {person, position}

    changed ++ removed
  end

  def affected_entries(old, next, message_keys, entry_keys) do
    urgency_entries =
      for {person, _, _} = key <- message_keys,
          %{message: before} <- [Map.get(old.messages, key)],
          %{message: current} <- [Map.get(next.messages, key)],
          Message.effective_urgency(before) != Message.effective_urgency(current),
          position <- RouteBIndex.origin_reply_positions(next.route_b_index, key),
          do: {person, position}

    read_items =
      for entry_key <- entry_keys,
          %{key: key, payload: %{"kind" => "read_position"}} <- [
            Map.get(next.conversation_events, entry_key)
          ],
          into: MapSet.new(),
          do: key

    cleared_scores =
      Enum.flat_map(read_items, fn {person, _, _} = key ->
        previous = get_in(old.reply_group_history, [key, :scores]) || %{}
        current = get_in(next.reply_group_history, [key, :scores]) || %{}
        changed_score_entries(person, previous, current)
      end)

    urgency_entries ++ cleared_scores
  end
end
