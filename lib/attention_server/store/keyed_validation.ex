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

  def authority_index(engine) do
    for field <- [:messages, :uploads, :receipts],
        key <- Map.keys(Map.fetch!(engine, field)),
        reduce: %{} do
      index -> edit_authority_index(index, engine, {:row, field, key}, :add)
    end
  end

  def authority_changes(index, old, next, touched) do
    updated =
      Enum.reduce(touched, index, fn address, acc ->
        acc
        |> edit_authority_index(old, address, :remove)
        |> edit_authority_index(next, address, :add)
      end)

    affected =
      Enum.reduce(touched, MapSet.new(), fn
        {:row, :persons, person}, keys ->
          if Map.has_key?(old.persons, person) != Map.has_key?(next.persons, person),
            do: MapSet.union(keys, Map.get(updated, {:person, person}, MapSet.new())),
            else: keys

        {:row, field, actor}, keys when field in [:devices, :agents] ->
          before = owner_identity(Map.get(Map.fetch!(old, field), actor))
          after_identity = owner_identity(Map.get(Map.fetch!(next, field), actor))

          if before == after_identity,
            do: keys,
            else: MapSet.union(keys, Map.get(updated, {:actor, actor}, MapSet.new()))

        _, keys ->
          keys
      end)

    {updated, affected}
  end

  defp owner_identity(nil), do: nil
  defp owner_identity(record), do: Map.take(record, [:person, :id])

  defp edit_authority_index(index, engine, address, operation) do
    Enum.reduce(authority_owners(engine, address), index, fn owner, acc ->
      rows = Map.get(acc, owner, MapSet.new())

      next =
        if operation == :add, do: MapSet.put(rows, address), else: MapSet.delete(rows, address)

      if MapSet.size(next) == 0, do: Map.delete(acc, owner), else: Map.put(acc, owner, next)
    end)
  end

  defp authority_owners(engine, {:row, :messages, {person, _, _} = key}) do
    case Map.get(engine.messages, key) do
      nil ->
        []

      record ->
        actors = [record.origin, get_in(record, [:terminal, :actor])] |> Enum.reject(&is_nil/1)
        [{:person, person} | Enum.map(actors, &{:actor, &1})]
    end
  end

  defp authority_owners(engine, {:row, :uploads, id}) do
    case Map.get(engine.uploads, id) do
      nil -> []
      upload -> [{:person, upload.person}, {:actor, upload.uploader}]
    end
  end

  defp authority_owners(engine, {:row, :receipts, {person, actor, _} = key}) do
    if Map.has_key?(engine.receipts, key), do: [{:person, person}, {:actor, actor}], else: []
  end

  defp authority_owners(_engine, _address), do: []

  def upload_index(engine) do
    for {scope, feed} <- engine.origin_feeds, {seq, entry} <- feed.entries, reduce: %{} do
      index -> add_upload_links(index, {scope, seq}, entry)
    end
  end

  def upload_changes(index, old, next, touched) do
    {index, changed, uploads} =
      Enum.reduce(touched, {index, MapSet.new(), []}, fn
        {:origin_feed, scope, {:entry, seq}}, {index, changed, uploads} ->
          key = {scope, seq}
          before_entry = get_in(old.origin_feeds, [scope, :entries, seq])
          after_entry = get_in(next.origin_feeds, [scope, :entries, seq])

          index =
            Enum.reduce(upload_ids(before_entry), index, fn id, acc ->
              owners = MapSet.delete(Map.get(acc, id, MapSet.new()), key)
              if MapSet.size(owners) == 0, do: Map.delete(acc, id), else: Map.put(acc, id, owners)
            end)

          {add_upload_links(index, key, after_entry), MapSet.put(changed, key), uploads}

        {:row, :uploads, id}, {index, changed, uploads} ->
          {index, changed, [id | uploads]}

        _, acc ->
          acc
      end)

    related = Enum.reduce(uploads, changed, &MapSet.union(Map.get(index, &1, MapSet.new()), &2))

    entries =
      for {{person, _} = scope, seq} <- related,
          entry = get_in(next.origin_feeds, [scope, :entries, seq]),
          do: {person, entry}

    {:ok, index, entries}
  end

  defp add_upload_links(index, key, entry) do
    Enum.reduce(upload_ids(entry), index, fn id, acc ->
      Map.update(acc, id, MapSet.new([key]), &MapSet.put(&1, key))
    end)
  end

  defp upload_ids(nil), do: []

  defp upload_ids(entry) do
    for %{"tier" => "upload", "upload_id" => id} <- Map.get(entry, "files", []), do: id
  end

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
