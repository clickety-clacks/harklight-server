defmodule AttentionServer.AgentAPI.FeedRenderer do
  @moduledoc false

  alias AttentionServer.AgentAPI.Focus
  alias AttentionServer.ClientAPI.{AgentResources, Resources}

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
        case AgentResources.entries(state, scope, engine_key) do
          {:ok, wire_entries} ->
            positions = MapSet.new(positions)

            for %{"position" => position} = entry <- wire_entries,
                MapSet.member?(positions, position),
                do: {position, entry}

          {:error, _reason} ->
            []
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
    scope = {person, key, session}

    with {:ok, sequence} <- Map.fetch(engine.advisory_id_index, {scope, id}),
         %{entries: %{^sequence => advisory}, receipts: receipts} <-
           Map.get(engine.advisory_feeds, scope),
         %{"sender_id" => sender, "message_id" => message_id} <- advisory,
         {:ok, item_id} <- Resources.item_id(engine, {person, sender, message_id}) do
      advisory =
        advisory
        |> Map.drop(~w(kind sender_id message_id session_handle advisory_seq origin_cursor_at_decision))
        |> Map.put("delivered", MapSet.member?(receipts, id))

      {:ok, %{"item_id" => item_id, "advisory" => advisory}}
    else
      _ -> {:error, :storage_unavailable}
    end
  end
end
