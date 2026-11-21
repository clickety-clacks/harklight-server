defmodule AttentionServer.RouteBIndex do
  @moduledoc false

  def new do
    %{
      resolved_messages_by_person: %{},
      sent_messages_by_agent: %{},
      agent_entry_pages: %{},
      agent_results: %{},
      conversations_by_item: %{},
      agent_feed?: false,
      entry_positions: %{},
      advisory_sequences: %{},
      messages_by_origin_session: %{},
      request_keys_by_id: nil,
      covered_sessions_by_origin: %{},
      push_bindings: nil,
      conversation_summary_by_item: %{},
      unresolved_count_by_person: %{}
    }
  end

  def rebuild(engine) do
    index = %{new() | agent_feed?: Map.get(engine.route_b_index, :agent_feed?, false)}

    index =
      if is_map(engine.route_b_index[:request_keys_by_id]),
        do: %{index | request_keys_by_id: %{}},
        else: index

    index =
      Enum.reduce(engine.messages, index, fn {key, record}, current ->
        put_message(current, key, nil, record)
      end)

    index =
      Enum.reduce(engine.conversation_events, index, fn {{person, seq}, event}, current ->
        put_conversation_event(current, person, seq, event.key, event.payload)
      end)

    index
    |> index_advisories(engine.advisory_feeds)
    |> index_covered_sessions(engine.origin_sessions)
    |> index_push_bindings(engine)
  end

  def enable_keyed_lookups(engine) do
    index =
      Enum.reduce(engine.messages, %{engine.route_b_index | request_keys_by_id: %{}}, fn
        {key, record}, index ->
          index = put_request_key(index, key, nil)

          if index.agent_feed?, do: index, else: put_session_message(index, key, nil, record)
      end)

    index =
      index
      |> index_covered_sessions(engine.origin_sessions)
      |> Map.put(:push_bindings, AttentionServer.PushIndex.build(engine.push_bindings))

    %{engine | route_b_index: index}
  end

  defp index_push_bindings(%{request_keys_by_id: lookup} = index, engine) when is_map(lookup) do
    %{index | push_bindings: AttentionServer.PushIndex.build(engine.push_bindings)}
  end

  defp index_push_bindings(index, _engine), do: index

  defp index_covered_sessions(%{request_keys_by_id: lookup} = index, sessions)
       when is_map(lookup) do
    Enum.reduce(sessions, %{index | covered_sessions_by_origin: %{}}, fn {key, session}, acc ->
      put_origin_session(acc, key, nil, session)
    end)
  end

  defp index_covered_sessions(index, _sessions), do: index

  def put_origin_session(
        %{request_keys_by_id: lookup} = index,
        {person, origin, handle},
        prior,
        next
      )
      when is_map(lookup) do
    old? = get_in(prior, [:support, "origin_listener"]) == true
    new? = get_in(next, [:support, "origin_listener"]) == true

    if old? == new? do
      index
    else
      scope = {person, origin}
      covered = Map.get(index.covered_sessions_by_origin, scope, MapSet.new())
      updated = if new?, do: MapSet.put(covered, handle), else: MapSet.delete(covered, handle)

      scopes =
        if MapSet.size(updated) == 0,
          do: Map.delete(index.covered_sessions_by_origin, scope),
          else: Map.put(index.covered_sessions_by_origin, scope, updated)

      %{index | covered_sessions_by_origin: scopes}
    end
  end

  def put_origin_session(index, _key, _prior, _next), do: index

  defp put_request_key(%{request_keys_by_id: lookup} = index, {person, _, id} = key, nil)
       when is_map(lookup) do
    %{index | request_keys_by_id: Map.update(lookup, {person, id}, [key], &[key | &1])}
  end

  defp put_request_key(index, _key, _previous), do: index

  def enable_agent_feed(%{route_b_index: %{agent_feed?: true}} = engine), do: engine

  def enable_agent_feed(engine) do
    index = %{engine.route_b_index | agent_feed?: true}

    index =
      Enum.reduce(engine.messages, index, fn {key, record}, current ->
        current =
          if is_nil(current.request_keys_by_id),
            do: put_session_message(current, key, nil, record),
            else: current

        put_agent_message(current, key, nil, record)
      end)

    index =
      Enum.reduce(engine.conversation_events, index, fn {{_, seq}, event}, current ->
        current
        |> put_entry_position(event.key, seq, event.payload)
        |> put_agent_entry(event.key, seq, event.payload)
      end)

    %{engine | route_b_index: index_advisories(index, engine.advisory_feeds)}
  end

  defp index_advisories(%{agent_feed?: true} = index, feeds) do
    Enum.reduce(feeds, index, fn {{person, _, _}, feed}, current ->
      Enum.reduce(feed.entries, current, fn {seq, item}, acc ->
        put_advisory(acc, {person, item["sender_id"], item["message_id"]}, seq)
      end)
    end)
  end

  defp index_advisories(index, _feeds), do: index

  def put_message(index, {person, sender, id} = key, previous, record) do
    index = put_request_key(index, key, previous)
    index = put_session_message(index, key, previous, record)
    index = put_agent_message(index, key, previous, record)
    previous_resolved? = match?(%{terminal: terminal} when not is_nil(terminal), previous)
    current_resolved? = not is_nil(record.terminal)

    index =
      case {previous_resolved?, current_resolved?} do
        {false, true} ->
          order = Map.get(index.resolved_messages_by_person, person, :gb_trees.empty())
          order_key = {-record.accepted_seq, sender, id}
          order = :gb_trees.enter(order_key, key, order)
          put_in(index, [:resolved_messages_by_person, person], order)

        {true, false} ->
          order = Map.get(index.resolved_messages_by_person, person, :gb_trees.empty())
          order_key = {-record.accepted_seq, sender, id}
          order = :gb_trees.delete_any(order_key, order)
          put_in(index, [:resolved_messages_by_person, person], order)

        _ ->
          index
      end

    delta = unresolved_delta(previous, record)

    if delta == 0 do
      index
    else
      count = Map.get(index.unresolved_count_by_person, person, 0) + delta
      count = max(count, 0)
      put_in(index, [:unresolved_count_by_person, person], count)
    end
  end

  def message_page(index, scope, filters),
    do: Map.get(index.sent_messages_by_agent, {scope, filters}, :gb_trees.empty())

  def entry_page(index, key),
    do: Map.get(index.agent_entry_pages, key, :gb_trees.empty())

  def result_revision(index, key, position),
    do: Enum.count(Map.get(index.agent_results, key, []), &(&1 <= position))

  defp put_agent_message(
         %{agent_feed?: true} = index,
         {person, sender, id} = key,
         nil,
         %{origin: agent} = record
       )
       when is_binary(agent) do
    dimensions = [{"sender", sender}]

    dimensions =
      case Map.get(record, :session_handle) do
        session when is_binary(session) -> [{"session", session} | dimensions]
        _ -> dimensions
      end

    dimensions =
      if Map.get(record, :agent_api_v2, false),
        do: dimensions,
        else: [{"legacy_message_id", id} | dimensions]

    filters =
      Enum.reduce(dimensions, [%{}], fn {name, value}, sets ->
        sets ++ Enum.map(sets, &Map.put(&1, name, value))
      end)

    pages =
      Enum.reduce(filters, index.sent_messages_by_agent, fn filter, pages ->
        page = {{person, agent}, filter}
        tree = Map.get(pages, page, :gb_trees.empty())
        Map.put(pages, page, :gb_trees.enter(-record.accepted_seq, key, tree))
      end)

    %{index | sent_messages_by_agent: pages}
  end

  defp put_agent_message(index, _key, _previous, _record), do: index

  defp put_agent_entry(%{agent_feed?: true} = index, key, seq, %{"kind" => kind})
       when kind in ~w(discussion reply revision result availability retraction) do
    page = Map.get(index.agent_entry_pages, key, :gb_trees.empty())
    index = put_in(index.agent_entry_pages[key], :gb_trees.enter(-seq, seq, page))

    if kind == "result",
      do:
        update_in(index.agent_results, &Map.update(&1, key, [seq], fn seqs -> [seq | seqs] end)),
      else: index
  end

  defp put_agent_entry(index, _key, _seq, _payload), do: index

  def put_conversation_event(index, person, seq, {row_person, _sender, _id} = key, payload)
      when row_person == person do
    tree = Map.get(index.conversations_by_item, key, :gb_trees.empty())
    tree = :gb_trees.enter({-seq, seq}, seq, tree)
    index = put_in(index, [:conversations_by_item, key], tree)

    index = put_entry_position(index, key, seq, payload)
    index = put_agent_entry(index, key, seq, payload)

    summary =
      Map.get(index.conversation_summary_by_item, key, %{
        latest_seq: 0,
        latest_origin_reply_seq: nil,
        chat_started: false,
        availability: nil
      })

    summary = %{summary | latest_seq: max(summary.latest_seq, seq)}

    summary =
      if payload["kind"] == "reply" and payload["author"] == "origin" do
        %{summary | latest_origin_reply_seq: max(summary.latest_origin_reply_seq || 0, seq)}
      else
        summary
      end

    summary =
      if payload["kind"] in ["discussion", "reply"] do
        %{summary | chat_started: true}
      else
        summary
      end

    summary =
      if payload["kind"] == "availability" do
        case summary.availability do
          %{seq: previous_seq} when previous_seq >= seq -> summary
          _ -> %{summary | availability: %{seq: seq, payload: payload}}
        end
      else
        summary
      end

    put_in(index, [:conversation_summary_by_item, key], summary)
  end

  def put_advisory(%{agent_feed?: true} = index, item, seq),
    do:
      update_in(
        index.advisory_sequences,
        &Map.update(&1, item, [seq], fn seqs -> [seq | seqs] end)
      )

  def put_advisory(index, _item, _seq), do: index

  def advisory_sequences(index, item), do: Map.get(index.advisory_sequences, item, [])

  def entry_position(index, key, id), do: Map.get(index.entry_positions, {key, id})

  def session_messages(index, {person, origin, session}),
    do: get_in(index.messages_by_origin_session, [{person, origin}, session]) || []

  def origin_sessions(index, person, origin),
    do: index.messages_by_origin_session |> Map.get({person, origin}, %{}) |> Map.keys()

  defp put_entry_position(%{agent_feed?: true} = index, key, seq, %{
         "kind" => kind,
         "event_id" => id
       })
       when kind in ["discussion", "reply", "revision"] and is_binary(id),
       do: put_in(index.entry_positions[{key, id}], seq)

  defp put_entry_position(index, _key, _seq, _payload), do: index

  # Message ownership and session are fixed at acceptance.
  defp put_session_message(
         %{agent_feed?: agent_feed?, request_keys_by_id: request_keys} = index,
         {person, _, _} = key,
         nil,
         %{origin: origin, session_handle: session}
       )
       when (agent_feed? or is_map(request_keys)) and is_binary(origin) and is_binary(session) do
    update_in(index.messages_by_origin_session, fn sessions ->
      Map.update(sessions, {person, origin}, %{session => [key]}, fn own ->
        Map.update(own, session, [key], &[key | &1])
      end)
    end)
  end

  defp put_session_message(index, _key, _previous, _record), do: index

  defp unresolved_delta(previous, record) do
    previous_unresolved? = match?(%{terminal: nil}, previous)
    current_unresolved? = match?(%{terminal: nil}, record)

    cond do
      current_unresolved? and not previous_unresolved? -> 1
      previous_unresolved? and not current_unresolved? -> -1
      true -> 0
    end
  end
end
