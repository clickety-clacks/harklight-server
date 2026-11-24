defmodule AttentionServer.AgentAPI.FeedRenderer do
  @moduledoc false

  alias AttentionServer.AgentAPI.Advisories
  alias AttentionServer.ClientAPI.{AgentResources, Projection}

  def render_many(%{store: %{engine: engine}} = state, %{person: person, key: key}, rows) do
    Projection.probe(:feed)
    scope = {person, key}

    entries =
      for %{resource: {:entry, position}} <- rows,
          %{key: engine_key} <- [Map.get(engine.conversation_events, {person, position})],
          do: {engine_key, position}

    entries =
      entries
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
      |> Enum.flat_map(fn {engine_key, positions} ->
        with {:ok, raw} <- AgentResources.raw_entries(state, scope, engine_key, positions),
             {:ok, wire} <- AgentResources.render_entries(state, scope, raw) do
          Enum.map(wire, &{&1["position"], &1})
        else
          _ -> []
        end
      end)
      |> Map.new()

    Enum.map(rows, fn row -> {row, render_row(state, scope, row, entries)} end)
  end

  defp render_row(state, scope, %{resource: {:message, key}, rev: rev}, _entries) do
    case AgentResources.message(state, scope, key) do
      {:ok, %{"rev" => ^rev}} = rendered -> rendered
      _ -> {:error, :storage_unavailable}
    end
  end

  defp render_row(_state, _scope, %{resource: {:entry, position}, rev: rev}, entries) do
    case Map.fetch(entries, position) do
      {:ok, %{"rev" => ^rev} = entry} -> {:ok, entry}
      _ -> {:error, :storage_unavailable}
    end
  end

  defp render_row(
         %{store: %{engine: engine}},
         scope,
         %{resource: {:advisory, {item, id}}},
         _entries
       ) do
    Advisories.item_change(engine, scope, item, id)
  end

  defp render_row(
         %{store: %{engine: engine}},
         {person, key},
         %{resource: {:advisory, id}, session: session},
         _entries
       ) do
    Advisories.change(engine, {person, key, session}, id)
  end
end
