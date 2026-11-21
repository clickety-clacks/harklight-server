defmodule AttentionServer.PushIndex do
  @moduledoc false

  defstruct people: %{}, addresses: %{}, stopped: MapSet.new(), queued: {0, nil}, claims: {0, nil}

  def build(bindings, delivery_states) do
    stopped = for {person, "STOPPED"} <- delivery_states, into: MapSet.new(), do: person

    Enum.reduce(bindings, %__MODULE__{stopped: stopped}, fn {key, binding}, index ->
      update(index, key, nil, binding)
    end)
  end

  def update(nil, _key, _old, _new), do: nil
  def update(index, _key, binding, binding), do: index

  def update(index, {person, _} = key, old, new) do
    index = remove(index, person, key, old)
    add(index, person, key, new)
  end

  def keys(index, person), do: Map.get(index.people, person, [])
  def owner(index, address), do: Map.get(index.addresses, address)

  def expired(index, cutoff) do
    index.claims
    |> :gb_trees.iterator()
    |> expired_keys(cutoff, [])
  end

  def ready(index, now) do
    case :gb_trees.next(:gb_trees.iterator(index.queued)) do
      {{at, key}, true, _next} when at <= now -> key
      _ -> nil
    end
  end

  def set_stopped(nil, _person, _stopped, _bindings), do: nil

  def set_stopped(index, person, stopped?, bindings) do
    queued =
      Enum.reduce(keys(index, person), index.queued, fn key, tree ->
        at = queued_at(Map.fetch!(bindings, key))
        if stopped?, do: drop_deadline(tree, at, key), else: put_deadline(tree, at, key)
      end)

    stopped =
      if stopped?,
        do: MapSet.put(index.stopped, person),
        else: MapSet.delete(index.stopped, person)

    %{index | queued: queued, stopped: stopped}
  end

  defp expired_keys(iter, cutoff, keys) do
    case :gb_trees.next(iter) do
      {{at, key}, true, next} when at <= cutoff -> expired_keys(next, cutoff, [key | keys])
      _ -> keys
    end
  end

  defp add(index, person, key, %{active: true, address: address} = binding) do
    keys = Map.get(index.people, person, MapSet.new())

    %{
      index
      | people: Map.put(index.people, person, MapSet.put(keys, key)),
        addresses: Map.put(index.addresses, address, key),
        queued: put_deadline(index.queued, eligible_at(index, person, binding), key),
        claims: put_deadline(index.claims, claimed_at(binding), key)
    }
  end

  defp add(index, _person, _key, _binding), do: index

  defp remove(index, person, key, %{active: true, address: address} = binding) do
    keys = MapSet.delete(Map.fetch!(index.people, person), key)

    people =
      if MapSet.size(keys) == 0,
        do: Map.delete(index.people, person),
        else: Map.put(index.people, person, keys)

    %{
      index
      | people: people,
        addresses: Map.delete(index.addresses, address),
        queued: drop_deadline(index.queued, queued_at(binding), key),
        claims: drop_deadline(index.claims, claimed_at(binding), key)
    }
  end

  defp remove(index, _person, _key, _binding), do: index

  defp eligible_at(index, person, binding) do
    if not MapSet.member?(index.stopped, person), do: queued_at(binding)
  end

  defp queued_at(%{state: state, next_at: at} = binding)
       when state in ["queued", "waiting_retry"] and is_integer(at) do
    if binding.pending_high > 0 or binding.pending_conversation_high > 0, do: at
  end

  defp queued_at(_), do: nil

  defp claimed_at(%{state: "sending", claim_at: at}) when is_integer(at), do: at
  defp claimed_at(_), do: nil

  defp put_deadline(tree, nil, _key), do: tree
  defp put_deadline(tree, at, key), do: :gb_trees.enter({at, key}, true, tree)

  defp drop_deadline(tree, nil, _key), do: tree
  defp drop_deadline(tree, at, key), do: :gb_trees.delete_any({at, key}, tree)
end
