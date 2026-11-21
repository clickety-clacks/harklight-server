defmodule AttentionServer.Store.Transition do
  @moduledoc false

  alias AttentionServer.Engine

  @type row :: {:row, atom(), term()}
  @type member :: {:member, atom(), term()}
  @type address :: {:field, atom()} | row() | member()
  @type operation :: {:put, term()} | :delete
  @type t :: %{optional(address()) => operation()}

  @spec new() :: t()
  def new, do: %{}

  @spec put(t(), address(), term()) :: t()
  def put(transition, address, value), do: Map.put(transition, address, {:put, value})

  @spec delete(t(), row() | member()) :: t()
  def delete(transition, {:row, _, _} = address), do: Map.put(transition, address, :delete)
  def delete(transition, {:member, _, _} = address), do: Map.put(transition, address, :delete)

  # Return the next Engine and the net operations for persistence and projection.
  # Inspect only the addresses written by the transition.
  @spec apply(t(), %Engine{}) :: {%Engine{}, t()}
  def apply(transition, %Engine{} = previous) do
    Enum.reduce(transition, {previous, new()}, fn {address, operation}, {engine, changes} ->
      if current(engine, address) === operation do
        {engine, changes}
      else
        {write(engine, address, operation), Map.put(changes, address, operation)}
      end
    end)
  end

  defp current(engine, {:field, field}), do: {:put, Map.fetch!(engine, field)}

  defp current(engine, {:row, field, key}) do
    case engine |> Map.fetch!(field) |> Map.fetch(key) do
      {:ok, value} -> {:put, value}
      :error -> :delete
    end
  end

  defp current(engine, {:member, field, key}) do
    if MapSet.member?(Map.fetch!(engine, field), key), do: {:put, true}, else: :delete
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
end
