defmodule AttentionServer.Store.Transition do
  @moduledoc false

  alias AttentionServer.Engine

  @separate [
    :advisory_feeds,
    :advisory_id_index,
    :open_default_index,
    :route_b_index,
    :touched_keys
  ]
  @fields Map.new(Map.drop(Map.from_struct(%Engine{}), @separate), fn
            {field, %MapSet{}} -> {field, :member}
            {field, value} when is_map(value) -> {field, :row}
            {field, _value} -> {field, :field}
          end)

  @type row :: {:row, atom(), term()}
  @type member :: {:member, atom(), term()}
  @type advisory_entry :: {:advisory, term(), {:entry, term()}}
  @type advisory_receipt :: {:advisory, term(), {:receipt, term()}}
  @type removable :: row() | member() | advisory_entry() | advisory_receipt()
  @type address :: {:field, atom()} | {:advisory, term(), :high} | removable()
  @type operation :: {:put, term()} | :delete
  @type t :: %{optional(address()) => operation()}

  @spec new() :: t()
  def new, do: %{}

  @spec put(t(), address(), term()) :: t()
  def put(transition, address, value), do: add(transition, address, {:put, value})

  @spec delete(t(), removable()) :: t()
  def delete(transition, address), do: add(transition, address, :delete)

  defp add(transition, address, operation) do
    if valid_operation?(address, operation) do
      Map.put(transition, address, operation)
    else
      raise ArgumentError, "invalid transition operation for #{inspect(address)}"
    end
  end

  # Each root has one address kind. A root write cannot overlap a keyed
  # write, and an advisory feed is only writable through its separate facts.
  defp valid_operation?({:field, field}, {:put, value}),
    do: @fields[field] == :field and not is_map(value)

  defp valid_operation?({:row, field, _}, _), do: @fields[field] == :row

  defp valid_operation?({:member, field, _}, operation),
    do: @fields[field] == :member and member_operation?(operation)

  defp valid_operation?({:advisory, _, :high}, {:put, high}),
    do: is_integer(high) and high >= 0

  defp valid_operation?({:advisory, _, {:entry, _}}, _), do: true

  defp valid_operation?({:advisory, _, {:receipt, _}}, operation),
    do: member_operation?(operation)

  defp valid_operation?(_, _), do: false

  defp member_operation?(operation), do: operation in [:delete, {:put, true}]

  # Return the next Engine and the net operations for persistence and projection.
  # Inspect only the addresses written by the transition.
  @spec apply(t(), %Engine{}) :: {%Engine{}, t()}
  def apply(transition, %Engine{} = previous) do
    Enum.reduce(transition, {previous, new()}, fn {address, operation}, {engine, changes} ->
      if current(previous, address) === operation do
        {engine, changes}
      else
        {write(engine, address, operation), Map.put(changes, address, operation)}
      end
    end)
  end

  defp current(engine, {:field, field}), do: {:put, Map.fetch!(engine, field)}

  defp current(engine, {:row, field, key}) do
    row_at(Map.fetch!(engine, field), key)
  end

  defp current(engine, {:member, field, key}) do
    member_at(Map.fetch!(engine, field), key)
  end

  defp current(engine, {:advisory, scope, fact}) do
    case Map.fetch(engine.advisory_feeds, scope) do
      {:ok, feed} -> advisory_fact(feed, fact)
      :error -> :delete
    end
  end

  defp advisory_fact(feed, :high), do: {:put, feed.high}
  defp advisory_fact(feed, {:entry, seq}), do: row_at(feed.entries, seq)
  defp advisory_fact(feed, {:receipt, id}), do: member_at(feed.receipts, id)

  defp row_at(rows, key) do
    case Map.fetch(rows, key) do
      {:ok, value} -> {:put, value}
      :error -> :delete
    end
  end

  defp member_at(members, key) do
    if MapSet.member?(members, key), do: {:put, true}, else: :delete
  end

  defp write(engine, {:field, field}, {:put, value}), do: Map.replace!(engine, field, value)

  defp write(engine, {:row, field, key}, {:put, value}) do
    Map.update!(engine, field, &Map.put(&1, key, value))
  end

  defp write(engine, {:row, field, key}, :delete) do
    Map.update!(engine, field, &Map.delete(&1, key))
  end

  defp write(engine, {:member, field, key}, {:put, true}) do
    Map.update!(engine, field, &MapSet.put(&1, key))
  end

  defp write(engine, {:member, field, key}, :delete) do
    Map.update!(engine, field, &MapSet.delete(&1, key))
  end

  defp write(engine, {:advisory, scope, fact}, operation) do
    empty = %{high: 0, entries: %{}, receipts: MapSet.new()}

    feeds =
      Map.update(
        engine.advisory_feeds,
        scope,
        write_advisory(empty, fact, operation),
        &write_advisory(&1, fact, operation)
      )

    %{engine | advisory_feeds: feeds}
  end

  defp write_advisory(feed, :high, {:put, high}), do: %{feed | high: high}

  defp write_advisory(feed, {:entry, seq}, {:put, entry}),
    do: %{feed | entries: Map.put(feed.entries, seq, entry)}

  defp write_advisory(feed, {:entry, seq}, :delete),
    do: %{feed | entries: Map.delete(feed.entries, seq)}

  defp write_advisory(feed, {:receipt, id}, {:put, true}),
    do: %{feed | receipts: MapSet.put(feed.receipts, id)}

  defp write_advisory(feed, {:receipt, id}, :delete),
    do: %{feed | receipts: MapSet.delete(feed.receipts, id)}
end
