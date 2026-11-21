defmodule AttentionServer.AgentAPI.FeedActivity do
  @moduledoc false

  alias AttentionServer.{Engine, RouteBIndex, Store}

  @recent_ms 60_000
  @empty %{open: %{}, batches: %{}}

  def observe(%{relay: relay, principal: %{person: person, key: key}, params: %{session: session}}, action) do
    send(GenServer.whereis(relay), {:agent_feed_activity, action, {person, key, session}})
    :ok
  catch
    :error, :badarg -> :ok
  end

  def listening?(state, {person, agent, session}) do
    activity = Map.get(state, :agent_feed_activity, @empty)
    covered?(activity, {person, agent, nil}) or covered?(activity, {person, agent, session})
  end

  def handle_info(action, {person, agent, session} = scope, state) do
    previous = Map.get(state, :agent_feed_activity, @empty)
    current = update(previous, scope, action)
    next_state = Map.put(state, :agent_feed_activity, current)

    if covered?(previous, scope) == covered?(current, scope) do
      next_state
    else
      sessions =
        if session do
          [session]
        else
          RouteBIndex.origin_sessions(state.store.engine.route_b_index, person, agent)
        end

      update_availability(next_state, previous, sessions, person, agent)
    end
  end

  defp covered?(activity, scope),
    do: Map.has_key?(activity.open, scope) or Map.has_key?(activity.batches, scope)

  defp update(activity, scope, :open) do
    update_in(activity.open, &Map.update(&1, scope, 1, fn count -> count + 1 end))
  end

  defp update(activity, scope, :close) do
    update_in(activity.open, fn open ->
      case Map.get(open, scope) do
        count when count > 1 -> Map.put(open, scope, count - 1)
        _ -> Map.delete(open, scope)
      end
    end)
  end

  defp update(activity, scope, :batch) do
    case Map.get(activity.batches, scope) do
      {timer, _} -> Process.cancel_timer(timer)
      nil -> :ok
    end

    token = make_ref()
    timer = Process.send_after(self(), {:agent_feed_activity, {:expire, token}, scope}, @recent_ms)
    put_in(activity.batches[scope], {timer, token})
  end

  defp update(activity, scope, {:expire, token}) do
    case Map.get(activity.batches, scope) do
      {_, ^token} -> update_in(activity.batches, &Map.delete(&1, scope))
      _ -> activity
    end
  end

  defp update_availability(state, previous, sessions, person, agent) do
    store = state.store
    previous_state = Map.put(state, :agent_feed_activity, previous)

    next =
      Enum.reduce(sessions, store.engine, fn session, engine ->
        scope = {person, agent, session}
        listening = listening?(state, scope)

        if listening != listening?(previous_state, scope) and
             match?(%{gone: false}, Map.get(engine.origin_sessions, scope)) do
          status = if listening, do: "listening", else: "not_listening"
          detached_at = if listening, do: nil, else: state.clock.()
          Engine.origin_availability(engine, person, agent, session, status, detached_at)
        else
          engine
        end
      end)

    case Store.commit(store, next, %{person: person, actor: agent, action: "agent_feed_availability"}) do
      {:ok, committed} -> %{state | store: committed}
      _ -> state
    end
  end
end
