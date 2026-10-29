defmodule AttentionServer.Placement.Algorithm do
  @moduledoc false

  @callback place([map()], [map()], term()) :: [map()]
  @callback reassess([map()], [map()]) :: :none | :whole_set
  @callback compose([map()], map()) :: {:ok, map()} | {:error, atom()}
end

defmodule AttentionServer.Placement do
  @moduledoc false

  alias AttentionServer.Engine
  alias AttentionServer.Message
  alias AttentionServer.Placement.{Baseline, JevSet}

  @algorithms %{"baseline" => Baseline, "jev-set" => JevSet}

  def algorithms, do: Map.keys(@algorithms)

  def algorithm(id), do: Map.get(@algorithms, id)

  def place(id, items, previous, result) do
    case algorithm(id) do
      nil -> Baseline.place(items, previous, result)
      implementation -> implementation.place(items, previous, result)
    end
  end

  def reassess(%Engine{} = engine, person, previous) do
    case algorithm(choice(engine, person)) do
      nil -> :none
      implementation -> implementation.reassess(items(engine, person), previous || [])
    end
  end

  def compose(id, items, connection) do
    case algorithm(id) do
      nil -> {:error, :not_supported}
      implementation -> implementation.compose(items, connection)
    end
  end

  def entry(%{identity: {sender, id}, message: message} = item) do
    entry = %{
      "sender_id" => sender,
      "message_id" => id,
      "priority" => Map.get(item, :priority, Message.effective_urgency(message))
    }

    entry =
      case Map.get(message, "thread") do
        thread when is_binary(thread) and thread != "" -> Map.put(entry, "thread", thread)
        _ -> entry
      end

    case Message.effective_by_when(message) do
      due_at when is_integer(due_at) -> Map.put(entry, "due_at", due_at)
      _ -> entry
    end
  end

  @doc "Groups the displayed order without changing the engine's order or item priorities."
  def grouped(entries) when is_list(entries) do
    groups =
      Enum.reduce(entries, %{}, fn entry, groups ->
        case group_key(entry) do
          nil -> groups
          key -> Map.update(groups, key, [entry], &[entry | &1])
        end
      end)

    groups = Map.new(groups, fn {key, reversed} -> {key, Enum.reverse(reversed)} end)

    {shown_reversed, _emitted} =
      Enum.reduce(entries, {[], MapSet.new()}, fn entry, {shown, emitted} ->
        case group_key(entry) do
          nil ->
            {[entry | shown], emitted}

          key ->
            if MapSet.member?(emitted, key) do
              {shown, emitted}
            else
              {Enum.reverse(Map.fetch!(groups, key), shown), MapSet.put(emitted, key)}
            end
        end
      end)

    Enum.reverse(shown_reversed)
  end

  def grouped(entries), do: entries

  defp group_key(%{"sender_id" => sender, "thread" => thread})
       when is_binary(sender) and is_binary(thread) and thread != "",
       do: {sender, thread}

  defp group_key(_), do: nil

  def choice(%Engine{} = engine, person),
    do: Map.get(engine.algorithm_choices, person, "baseline")

  def items(%Engine{} = engine, person) do
    engine.messages
    |> Enum.flat_map(fn
      {{^person, sender, id}, %{terminal: nil} = record} ->
        message = record.message
        {placement_seq, group_thread} = inherited_placement(engine, {person, sender, id}, record)

        [
          %{
            identity: {sender, id},
            message:
              case group_thread do
                thread when is_binary(thread) -> Map.put(message, "thread", thread)
                _ -> Map.delete(message, "thread")
              end,
            accepted_seq: record.accepted_seq,
            placement_seq: placement_seq,
            revision: record.revision,
            origin_target: Map.get(record, :placement_origin_target)
          }
        ]

      _ ->
        []
    end)
  end

  def baseline(%Engine{} = engine, person), do: Baseline.place(items(engine, person), [], nil)

  defp inherited_placement(engine, key, record),
    do: inherited_placement(engine, key, record, MapSet.new())

  defp inherited_placement(engine, key, record, seen) do
    if MapSet.member?(seen, key) do
      {Map.get(record, :placement_seq, record.accepted_seq), Map.get(record, :group_thread)}
    else
      seen = MapSet.put(seen, key)

      case Map.get(record, :placement_origin_target) do
        {_, _, _} = target_key ->
          case Map.get(engine.messages, target_key) do
            %{accepted_seq: _} = target ->
              inherited_placement(engine, target_key, target, seen)

            _ ->
              {Map.get(record, :placement_seq, record.accepted_seq),
               Map.get(record, :group_thread)}
          end

        _ ->
          original =
            Map.get(record, :revisions, %{1 => record.message}) |> Map.get(1, record.message)

          {Map.get(record, :accepted_seq, 0), Message.valid_thread(original)}
      end
    end
  end

  def identity(%{"sender_id" => sender, "message_id" => id}), do: {sender, id}
  def identity(%{identity: identity}), do: identity

  def valid_permutation?(items, identities) when is_list(items) and is_list(identities) do
    expected = Enum.map(items, & &1.identity)

    length(expected) == length(identities) and
      length(Enum.uniq(identities)) == length(identities) and
      MapSet.new(expected) == MapSet.new(identities)
  end

  def valid_permutation?(_, _), do: false
end

defmodule AttentionServer.Placement.Baseline do
  @moduledoc false
  @behaviour AttentionServer.Placement.Algorithm

  alias AttentionServer.Message

  @impl true
  def place(items, _previous, _result) do
    items
    |> ordered_items()
    |> Enum.map(&with_effective_priority(&1, Message.effective_urgency(&1.message)))
  end

  def ordered_items(items) when is_list(items) do
    Enum.sort_by(items, fn item ->
      deadline = Message.effective_by_when(item.message)
      urgency = urgency_rank(Message.effective_urgency(item.message))

      {-urgency, not is_integer(deadline), if(is_integer(deadline), do: deadline, else: 0),
       -Map.get(item, :placement_seq, item.accepted_seq), elem(item.identity, 0),
       elem(item.identity, 1)}
    end)
  end

  defp urgency_rank("now"), do: 3
  defp urgency_rank("high"), do: 2
  defp urgency_rank("normal"), do: 1
  defp urgency_rank("low"), do: 0

  defp with_effective_priority(item, priority) do
    item
    |> Map.put(:priority, priority)
    |> AttentionServer.Placement.entry()
  end

  @impl true
  def reassess(_items, _previous), do: :none

  @impl true
  def compose(_items, _connection), do: {:error, :not_supported}
end

defmodule AttentionServer.Placement.JevSet do
  @moduledoc false
  @behaviour AttentionServer.Placement.Algorithm

  alias AttentionServer.Message
  alias AttentionServer.Placement
  alias AttentionServer.Placement.Baseline
  alias AttentionServer.JevClient
  alias AttentionServer.JevRanking

  @impl true
  def place(items, _previous, {:scored, scores}) do
    scored_place(items, scores, MapSet.new())
  end

  def place(items, _previous, {:ranked, scores, overflow}) do
    scored_place(items, scores, overflow)
  end

  def place(items, previous, _result), do: retained_place(items, previous)

  defp scored_place(items, scores, overflow) do
    items_by_identity = Map.new(items, &{&1.identity, &1})
    baseline = Baseline.ordered_items(items)

    baseline
    |> Enum.with_index()
    |> Enum.map(fn {item, index} ->
      score = Map.get(scores, item.identity)

      priority =
        cond do
          MapSet.member?(overflow, item.identity) -> "normal"
          valid_score?(score) -> score_priority(score)
          true -> Message.effective_urgency(item.message)
        end

      sort_score =
        cond do
          MapSet.member?(overflow, item.identity) -> 1.0
          valid_score?(score) -> score
          true -> urgency_score(Message.effective_urgency(item.message))
        end

      {sort_score, index, Map.fetch!(items_by_identity, item.identity), priority}
    end)
    |> Enum.sort_by(fn {score, index, _item, _priority} -> {-score, index} end)
    |> Enum.map_reduce(0, fn {_score, _index, item, priority}, top_count ->
      {priority, top_count} = cap_priority(priority, top_count)

      item
      |> Map.put(:priority, priority)
      |> Placement.entry()
      |> then(&{&1, top_count})
    end)
    |> elem(0)
  end

  defp retained_place(items, previous) do
    items_by_identity = Map.new(items, &{&1.identity, &1})
    baseline = Placement.place("baseline", items, [], nil)

    replacements_by_target =
      Enum.reduce(items, %{}, fn item, replacements ->
        case Map.get(item, :origin_target) do
          {_, _} = target -> Map.put_new(replacements, target, item)
          _ -> replacements
        end
      end)

    {retained, placed} =
      Enum.reduce(previous, {[], MapSet.new()}, fn entry, {retained, placed} ->
        identity = Placement.identity(entry)

        cond do
          Map.has_key?(items_by_identity, identity) ->
            item = Map.fetch!(items_by_identity, identity)
            priority = valid_priority(entry["priority"], Message.effective_urgency(item.message))

            {retained ++ [item |> Map.put(:priority, priority) |> Placement.entry()],
             MapSet.put(placed, item.identity)}

          Map.has_key?(replacements_by_target, identity) ->
            item = Map.fetch!(replacements_by_target, identity)
            priority = valid_priority(entry["priority"], Message.effective_urgency(item.message))

            {retained ++ [item |> Map.put(:priority, priority) |> Placement.entry()],
             MapSet.put(placed, item.identity)}

          true ->
            {retained, placed}
        end
      end)

    new_entries =
      Enum.filter(baseline, &(not MapSet.member?(placed, Placement.identity(&1))))

    (retained ++ new_entries)
    |> Enum.map_reduce(0, fn entry, count ->
      {priority, next_count} = cap_priority(entry["priority"], count)
      {Map.put(entry, "priority", priority), next_count}
    end)
    |> elem(0)
  end

  defp cap_priority(priority, count) when priority in ["high", "now"] do
    if count < JevRanking.top_tier_cap(), do: {priority, count + 1}, else: {"normal", count}
  end

  defp cap_priority(priority, count), do: {priority, count}

  defp valid_priority(value, _fallback) when value in ["low", "normal", "high", "now"],
    do: value

  defp valid_priority(_, fallback), do: fallback

  defp valid_score?(value), do: is_number(value) and value >= 0 and value <= 3

  defp score_priority(score) when score < 0.5, do: "low"
  defp score_priority(score) when score < 1.5, do: "normal"
  defp score_priority(score) when score < 2.5, do: "high"
  defp score_priority(_), do: "now"

  defp urgency_score("low"), do: 0.0
  defp urgency_score("normal"), do: 1.0
  defp urgency_score("high"), do: 2.0
  defp urgency_score("now"), do: 3.0

  @impl true
  def reassess(_items, _previous), do: :whole_set

  @impl true
  def compose(items, connection),
    do:
      JevClient.compose(
        Baseline.ordered_items(items),
        connection.endpoint,
        connection.model,
        Map.get(connection, :now, System.system_time(:millisecond))
      )
end
