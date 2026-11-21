defmodule AttentionServer.AgentAPI.FeedRenderer do
  @moduledoc false

  alias AttentionServer.AgentAPI.{Advisories, Focus}
  alias AttentionServer.ClientAPI.AgentResources

  def render_many(%{store: %{engine: engine}} = state, %{person: person, key: key}, rows) do
    scope = {person, key}

    entries =
      for %{resource: {:entry, position}} <- rows,
          %{key: engine_key} <- [Map.get(engine.conversation_events, {person, position})],
          do: {engine_key, position}

    entries =
      entries
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
      |> Enum.flat_map(fn {engine_key, positions} ->
        positions = MapSet.new(positions)

        with {:ok, raw} <- AgentResources.raw_entries(state, scope, engine_key),
             page = Enum.filter(raw, &MapSet.member?(positions, &1["position"])),
             {:ok, wire} <- AgentResources.render_entries(state, scope, page) do
          Enum.map(wire, &{&1["position"], &1})
        else
          _ -> []
        end
      end)
      |> Map.new()

    Enum.map(rows, fn row -> {row, render_row(state, scope, row, entries)} end)
  end

  def focus(ctx) do
    case Focus.get(ctx.relay, ctx.principal) do
      {:ok, projection} -> {:ok, %{"focus" => projection}}
      error -> error
    end
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
         {person, key},
         %{resource: {:advisory, id}, session: session},
         _entries
       ) do
    Advisories.change(engine, {person, key, session}, id)
  end
end
