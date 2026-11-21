defmodule AttentionServer.AgentAPI.Focus do
  @moduledoc false

  alias AttentionServer.{Relay, RouteBIndex}
  alias AttentionServer.ClientAPI.Resources

  def get(relay, %{kind: :device, person: person, key: key}),
    do: GenServer.call(relay, {:client_api, __MODULE__, {:get, person, key}}, :infinity)

  def get(_relay, _principal), do: {:error, :forbidden}

  def handle_call({:get, person, key}, _from, state) do
    result =
      with {:ok, clients} <- Relay.agent_focus(state, person, key) do
        {:ok, %{"clients" => Enum.map(clients, &render(state.store.engine, person, &1))}}
      end

    {:reply, result, state}
  end

  defp render(engine, person, client) do
    base =
      client
      |> Map.take(~w(device_name screen in_front connected))
      |> Map.put("device_id", client["client_id"])
      |> Map.put("reported_at_ms", client["reported_at"])

    key = {person, client["sender_id"], client["message_id"]}

    case Resources.item_id(engine, key) do
      {:ok, item_id} ->
        base = Map.put(base, "item_id", item_id)
        id = client["visible_event_id"]

        case RouteBIndex.entry_position(engine.route_b_index, key, id) do
          position when is_integer(position) ->
            Map.put(base, "visible_entry_id", Resources.entry_id(position))

          _ ->
            base
        end

      _ ->
        base
    end
  end
end
