defmodule AttentionServer.AgentAPI.FeedActivity do
  @moduledoc false

  alias AttentionServer.{ChangeLog, Conversation, Engine, RouteBIndex, Store}

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

  def activity(state), do: Map.get(state, :agent_feed_activity, @empty)

  def listening?(state, scope),
    do: listening?(state.store.engine, activity(state), scope)

  def listening?(engine, activity, {person, origin, _} = scope) do
    covered?(activity, scope) or
      (RouteBIndex.v2_session?(engine.route_b_index, scope) and
         covered?(activity, {person, origin, nil}))
  end

  # Render only the availability committed with the Item's revision.
  def item_availability(engine, {person, _, _} = key) do
    case Map.get(engine.messages, key) do
      %{origin: origin, session_handle: handle} = record when is_binary(handle) ->
        session = Map.get(engine.origin_sessions, {person, origin, handle})
        legacy = legacy_availability(engine, key, record)
        {status, stamp, _} = published(session, record, legacy)
        render(status, stamp)

      _ ->
        nil
    end
  end

  defp published(session, record, legacy) do
    case get_in(session, ["client_availability", record.accepted_seq]) do
      {status, stamp, token} -> {status, stamp, token}
      {status, stamp} -> {status, stamp, nil}
      nil -> {baseline_status(record, legacy), legacy["last_detached_at"], nil}
    end
  end

  defp baseline_status(record, legacy) do
    legacy["availability"] ||
      if(unsupported?(record, nil, legacy), do: "unsupported", else: "not_listening")
  end

  defp legacy_availability(engine, {person, _, _} = key, record) do
    get_in(engine.route_b_index, [:conversation_summary_by_item, key, :availability, :payload]) ||
      get_in(engine.events, [{person, record.accepted_seq}, :payload]) || %{}
  end

  defp desired_status(engine, activity, {person, origin, _} = scope, session, record, legacy) do
    cond do
      Map.get(session || %{}, :gone, false) or
          Engine.authorized_person(engine, origin) != {:ok, person} ->
        "gone"

      unsupported?(record, session, legacy) ->
        "unsupported"

      listening?(engine, activity, scope) ->
        "listening"

      true ->
        "not_listening"
    end
  end

  defp render(status, detached_at) do
    state = %{"state" => status}

    if status != "listening" and is_integer(detached_at) and detached_at > 0,
      do: Map.put(state, "last_detached_at_ms", detached_at),
      else: state
  end

  defp unsupported?(record, session, legacy) do
    not Map.get(record, :agent_api_v2, false) and
      case Map.get(session || %{}, :support) do
        nil -> legacy["availability"] == "unsupported"
        support -> is_binary(support["unsupported_reason"])
      end
  end

  def reset(state, rollback_token) do
    token = if is_binary(rollback_token) and rollback_token != "", do: rollback_token
    state = Map.put(state, :availability_rollback_token, token)

    keys =
      for {key, %{session_handle: handle}} <- state.store.engine.messages,
          is_binary(handle),
          do: key

    enqueue(state, keys)
  end

  def refresh_session(state, scope) do
    enqueue(state, RouteBIndex.session_messages(state.store.engine.route_b_index, scope))
  end

  def refresh_origin(state, person, origin) do
    index = state.store.engine.route_b_index

    keys =
      for handle <- RouteBIndex.origin_sessions(index, person, origin),
          key <- RouteBIndex.session_messages(index, {person, origin, handle}),
          do: key

    enqueue(state, keys)
  end

  def handle_info(:project, nil, state), do: project(state)

  def handle_info(action, {person, origin, session} = scope, state) do
    before = state
    state = maybe_reopen(state, scope, action)
    previous = activity(state)
    current = update(previous, scope, action)
    state = Map.put(state, :agent_feed_activity, current)
    engine = state.store.engine

    if covered?(previous, scope) == covered?(current, scope) and
         before.store.engine.origin_sessions == engine.origin_sessions do
      state
    else
      index = engine.route_b_index

      handles =
        if session, do: [session], else: RouteBIndex.origin_sessions(index, person, origin)

      keys =
        for handle <- handles,
            session_scope = {person, origin, handle},
            listening?(engine, previous, session_scope) !=
              listening?(engine, current, session_scope) or
              Map.get(before.store.engine.origin_sessions, session_scope) !=
                Map.get(engine.origin_sessions, session_scope),
            key <- RouteBIndex.session_messages(index, session_scope),
            do: key

      enqueue(state, keys)
    end
  end

  defp maybe_reopen(state, {person, origin, _} = scope, :open) do
    store = state.store
    next = Conversation.reopen(store.engine, scope, :scoped_stream)

    if next == store.engine do
      state
    else
      case Store.commit(store, next, %{person: person, actor: origin, action: "origin_reopen"}) do
        {:ok, committed} -> %{state | store: committed}
        {:error, _, _} -> state
      end
    end
  end

  defp maybe_reopen(state, _, _), do: state

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

  defp enqueue(state, []), do: state

  defp enqueue(state, keys) do
    pending = Map.get(state, :agent_availability_jobs, MapSet.new())
    state = Map.put(state, :agent_availability_jobs, Enum.into(keys, pending))
    if MapSet.size(pending) == 0, do: project(state), else: state
  end

  defp project(state) do
    pending = Map.fetch!(state, :agent_availability_jobs)
    page = Enum.take(pending, @projection_page_size)

    case apply_page(state, page) do
      {:ok, state} ->
        remaining = Enum.reduce(page, pending, &MapSet.delete(&2, &1))

        if MapSet.size(remaining) > 0, do: send(self(), {:agent_feed_activity, :project, nil})
        Map.put(state, :agent_availability_jobs, remaining)

      {:error, _} ->
        Process.send_after(self(), {:agent_feed_activity, :project, nil}, 250)
        state
    end
  end

  defp apply_page(state, keys) do
    store = state.store
    activity = activity(state)

    result =
      Enum.reduce_while(keys, {:ok, store.engine, []}, fn key, {:ok, engine, changed} ->
        case prepare_item(store, engine, key, activity, state.clock, state.availability_rollback_token) do
          {:ok, ^engine} -> {:cont, {:ok, engine, changed}}
          {:ok, updated, true} -> {:cont, {:ok, updated, [key | changed]}}
          error -> {:halt, error}
        end
      end)

    with {:ok, next, changed} <- result do
      if changed == [] do
        {:ok, state}
      else
        audit = %{person: "system", actor: "server", action: "item_availability", item_refresh: changed}

        case Store.commit(store, next, audit) do
          {:ok, committed} -> {:ok, %{state | store: committed}}
          {:error, reason, _} -> {:error, reason}
        end
      end
    end
  end

  defp prepare_item(store, engine, {person, _, _} = key, activity, clock, token) do
    with %{origin: origin, session_handle: handle} = record when is_binary(handle) <-
           Map.get(engine.messages, key),
         scope = {person, origin, handle},
         %{} = session <- Map.get(engine.origin_sessions, scope) do
      legacy = legacy_availability(engine, key, record)
      status = desired_status(engine, activity, scope, session, record, legacy)
      {previous, stamp, applied_token} = published(session, record, legacy)
      prior = render(previous, stamp)
      legacy_state = render(baseline_status(record, legacy), legacy["last_detached_at"])
      rollback_mismatch? = is_binary(legacy["availability"]) and prior != legacy_state
      forced? = is_binary(token) and token != applied_token and rollback_mismatch?

      with {:ok, refresh?} <- refresh?(store, key, previous != status or forced?, rollback_mismatch?) do
        if refresh? do
          detached_at =
            if previous == "listening" and status == "not_listening", do: clock.(), else: stamp

          repaired_token = if is_binary(token), do: token, else: applied_token
          item = {status, detached_at, repaired_token}
          published = Map.get(session, "client_availability", %{})

          updated =
            Map.put(session, "client_availability", Map.put(published, record.accepted_seq, item))

          next = Engine.put_row(engine, :origin_sessions, scope, updated)
          {:ok, next, true}
        else
          {:ok, engine}
        end
      end
    else
      _ -> {:ok, engine}
    end
  end

  defp refresh?(_store, _key, true, _), do: {:ok, true}
  defp refresh?(store, key, false, true), do: ChangeLog.old_item_writer?(store, key)
  defp refresh?(_store, _key, false, false), do: {:ok, false}
end
