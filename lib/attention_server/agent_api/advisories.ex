defmodule AttentionServer.AgentAPI.Advisories do
  @moduledoc false

  alias AttentionServer.{AgentChangeLog, BounceDecision, RouteBIndex, Store}
  alias AttentionServer.ClientAPI.Resources

  def stamp_decisions(%Store{change_log: false}, next, _keys), do: {:ok, next}

  def stamp_decisions(%Store{} = store, next, keys) do
    next
    |> BounceDecision.advisory_feed_keys_for_changes(keys)
    |> Enum.reduce_while({:ok, next}, fn {person, agent, _} = scope, {:ok, current} ->
      previous = get_in(store.engine.advisory_feeds, [scope, :high]) || 0
      high = get_in(next.advisory_feeds, [scope, :high]) || 0

      if high > previous do
        case AgentChangeLog.head(store, {person, agent}) do
          {:ok, head} ->
            stamped =
              Enum.reduce((previous + 1)..high, current, fn sequence, engine ->
                put_in(engine.advisory_feeds[scope].entries[sequence]["decided_at_rev"], head)
              end)

            {:cont, {:ok, stamped}}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
      else
        {:cont, {:ok, current}}
      end
    end)
  end

  def for_message(engine, {person, key}, {person, _, _} = item) do
    with %{origin: ^key, session_handle: session} when is_binary(session) <-
           Map.get(engine.messages, item),
         %{entries: entries, receipts: receipts} <-
           Map.get(engine.advisory_feeds, {person, key, session}) do
      engine.route_b_index
      |> RouteBIndex.advisory_sequences(item)
      |> Enum.sort()
      |> Enum.map(fn seq -> render(Map.fetch!(entries, seq), receipts) end)
    else
      _ -> []
    end
  end

  def change(engine, {person, _, _} = scope, id) do
    with {:ok, sequence} <- Map.fetch(engine.advisory_id_index, {scope, id}),
         %{entries: %{^sequence => advisory}, receipts: receipts} <-
           Map.get(engine.advisory_feeds, scope),
         %{"sender_id" => sender, "message_id" => message_id} <- advisory,
         {:ok, item_id} <- Resources.item_id(engine, {person, sender, message_id}) do
      {:ok, %{"item_id" => item_id, "advisory" => render(advisory, receipts)}}
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  defp render(advisory, receipts) do
    advisory
    |> Map.drop(
      ~w(kind sender_id message_id session_handle advisory_seq origin_cursor_at_decision)
    )
    |> baseline_decision(advisory["advisory_type"])
    |> Map.put("delivered", MapSet.member?(receipts, advisory["advisory_id"]))
  end

  # Advisories decided before the agent log existed have no agent cursor.
  defp baseline_decision(advisory, "unseen_interval_elapsed"),
    do: Map.put_new(advisory, "decided_at_rev", 0)

  defp baseline_decision(advisory, _type), do: advisory
end
