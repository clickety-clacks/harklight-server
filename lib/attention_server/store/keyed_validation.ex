defmodule AttentionServer.Store.KeyedValidation do
  @moduledoc false

  def source_index(messages) do
    Enum.reduce(messages, %{}, fn {key, record}, index ->
      case source_key(record) do
        nil -> index
        source -> Map.put(index, source, key)
      end
    end)
  end

  def source_changes(index, old_messages, next_messages, changed_items) do
    cleared =
      Enum.reduce(changed_items, index, fn key, current ->
        case source_key(Map.get(old_messages, key)) do
          nil -> current
          source -> Map.delete(current, source)
        end
      end)

    Enum.reduce_while(changed_items, {:ok, cleared}, fn key, {:ok, current} ->
      case source_key(Map.get(next_messages, key)) do
        nil ->
          {:cont, {:ok, current}}

        source ->
          if Map.has_key?(current, source),
            do: {:halt, {:error, :source_binding_conflict}},
            else: {:cont, {:ok, Map.put(current, source, key)}}
      end
    end)
  end

  defp source_key(%{source_binding: binding}) when is_map(binding) do
    {binding["source_instance_id"], binding["request_id"], binding["request_revision"]}
  end

  defp source_key(_), do: nil

  # The committed Engine satisfied the full validator. Each changed sequence
  # must still be inside its person's high-water mark, and the live row count
  # must change by exactly the same amount as that high-water mark.
  def sequences(old, next, touched) do
    touched = MapSet.new(touched)

    with {:ok, deltas} <- sequence_deltas(old, next, touched),
         true <-
           Enum.all?(deltas, fn {{highs, person}, delta} ->
             old_high = Map.get(Map.fetch!(old, highs), person, 0)
             next_high = Map.get(Map.fetch!(next, highs), person, 0)

             is_integer(next_high) and next_high >= 0 and
               delta == next_high - old_high and
               (old_high == next_high or MapSet.member?(touched, {:row, highs, person})) and
               (next_high == 0 or Map.has_key?(next.persons, person))
           end) do
      :ok
    else
      _ -> {:error, :invalid_sequence}
    end
  end

  defp sequence_deltas(old, next, touched) do
    Enum.reduce_while(touched, {:ok, %{}}, fn
      {:row, field, person}, {:ok, deltas} when field in [:persons, :conversation_highs] ->
        {:cont, {:ok, Map.put_new(deltas, {field, person}, 0)}}

      {:row, field, {person, seq} = key}, {:ok, deltas}
      when field in [:events, :conversation_events] ->
        highs = if field == :events, do: :persons, else: :conversation_highs
        old_event = Map.fetch(Map.fetch!(old, field), key)
        new_event = Map.fetch(Map.fetch!(next, field), key)
        high = Map.get(Map.fetch!(next, highs), person, 0)

        if valid_event?(new_event, next, field, person, seq, high) do
          delta = present(new_event) - present(old_event)
          {:cont, {:ok, Map.update(deltas, {highs, person}, delta, &(&1 + delta))}}
        else
          {:halt, {:error, :invalid_sequence}}
        end

      _, acc ->
        {:cont, acc}
    end)
  end

  defp valid_event?(:error, _next, _field, _person, seq, high),
    do: is_integer(seq) and is_integer(high) and seq > high

  defp valid_event?(
         {:ok, %{key: {person, _, _} = key, kind: kind}},
         next,
         field,
         person,
         seq,
         high
       ) do
    is_integer(seq) and seq > 0 and is_integer(high) and seq <= high and
      valid_kind?(field, kind) and Map.has_key?(next.messages, key)
  end

  defp valid_event?(_, _, _, _, _, _), do: false

  defp valid_kind?(:events, kind), do: kind in [:delivery, :lifecycle]
  defp valid_kind?(:conversation_events, kind), do: kind == :conversation

  defp present({:ok, _}), do: 1
  defp present(:error), do: 0
end
