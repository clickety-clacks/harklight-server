defmodule AttentionServer.AgentAPI.FeedActivity do
  @moduledoc false

  alias AttentionServer.{Engine, RouteBIndex, Store}

  @recent_ms 60_000
  @projection_page_size 100
  @empty %{open: %{}, batches: %{}}

  def observe(
        %{relay: relay, principal: %{person: person, key: key}, params: %{session: session}},
        action
      ) do
    send(GenServer.whereis(relay), {:agent_feed_activity, action, {person, key, session}})
    :ok
  catch
    :error, :badarg -> :ok
  end

  def listening?(state, {person, agent, session}) do
    activity = Map.get(state, :agent_feed_activity, @empty)
    covered?(activity, {person, agent, nil}) or covered?(activity, {person, agent, session})
  end

  def reset(state) do
    engine = state.store.engine

    scopes =
      for {{person, _, _} = item,
           %{terminal: nil, origin: origin, session_handle: session} = record} <- engine.messages,
          is_binary(session),
          stale_listening?(engine, item, record),
          do: {person, origin, session}

    enqueue(state, Enum.uniq(scopes))
  end

  defp stale_listening?(engine, {person, _, _} = item, record) do
    payload =
      get_in(engine.route_b_index, [:conversation_summary_by_item, item, :availability, :payload]) ||
        get_in(engine.events, [{person, record.accepted_seq}, :payload])

    match?(%{"availability_source" => "agent_feed", "availability" => "listening"}, payload)
  end

  def handle_info(:project, nil, state), do: project(state)

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

      scopes =
        for handle <- sessions,
            session_scope = {person, agent, handle},
            listening?(state, session_scope) != listening?(next_state, session_scope),
            do: session_scope

      enqueue(next_state, scopes)
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

    timer =
      Process.send_after(self(), {:agent_feed_activity, {:expire, token}, scope}, @recent_ms)

    put_in(activity.batches[scope], {timer, token})
  end

  defp update(activity, scope, {:expire, token}) do
    case Map.get(activity.batches, scope) do
      {_, ^token} -> update_in(activity.batches, &Map.delete(&1, scope))
      _ -> activity
    end
  end

  defp enqueue(state, scopes) do
    jobs = Map.get(state, :agent_availability_jobs, :queue.new())
    ready? = :queue.is_empty(jobs)
    jobs = Enum.reduce(scopes, jobs, &:queue.in({&1, nil}, &2))
    state = Map.put(state, :agent_availability_jobs, jobs)
    if ready?, do: project(state), else: state
  end

  defp project(state) do
    jobs = Map.fetch!(state, :agent_availability_jobs)

    case :queue.out(jobs) do
      {{:value, {scope, keys}}, rest} ->
        keys = keys || RouteBIndex.session_messages(state.store.engine.route_b_index, scope)
        {page, remaining} = Enum.split(keys, @projection_page_size)
        next_jobs = if remaining == [], do: rest, else: :queue.in({scope, remaining}, rest)
        state = apply_page(state, scope, page)

        unless :queue.is_empty(next_jobs), do: send(self(), {:agent_feed_activity, :project, nil})
        Map.put(state, :agent_availability_jobs, next_jobs)

      {:empty, _} ->
        state
    end
  end

  defp apply_page(state, {person, agent, _} = scope, keys) do
    store = state.store

    with %{gone: false} <- Map.get(store.engine.origin_sessions, scope),
         listening = listening?(state, scope),
         status = if(listening, do: "listening", else: "not_listening"),
         detached_at = if(listening, do: nil, else: state.clock.()),
         next = Engine.agent_feed_availability(store.engine, scope, status, detached_at, keys),
         {:ok, committed} <-
           Store.commit(store, next, %{
             person: person,
             actor: agent,
             action: "agent_feed_availability"
           }) do
      %{state | store: committed}
    else
      _ -> state
    end
  end
end
