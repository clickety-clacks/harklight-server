defmodule AttentionServer.BounceDecision do
  @moduledoc false

  alias AttentionServer.{BouncePolicy, Engine, Message}

  @no_record_sentence "Harklight has no record that you saw this message."
  @advisory_fields ~w(advisory_id advisory_seq accepted_at_ms author bounce_after_days due_at_ms evaluated_at_ms kind level_name message_id origin_cursor_at_decision request_revision sender_id session_handle text advisory_type)

  def levels(%Engine{} = engine, person) do
    case Map.get(engine.bounce_levels, person) do
      nil -> []
      config -> BouncePolicy.public_levels(config.levels)
    end
  end

  def configure_levels(%Engine{} = engine, person, levels, now_ms)
      when is_binary(person) and is_list(levels) and is_integer(now_ms) do
    case Map.get(engine.bounce_levels, person) do
      %{levels: current} = config when is_list(current) ->
        if BouncePolicy.equivalent_levels?(current, levels) do
          {:ok, engine, config}
        else
          updated = %{config | levels: levels}
          {:ok, put_configuration(engine, person, updated), updated}
        end

      nil ->
        config = %{
          levels: levels,
          configured_at_ms: now_ms,
          after_delivery_seq: Map.get(engine.persons, person, 0),
          after_conversation_seq: Map.get(engine.conversation_highs, person, 0)
        }

        {:ok, put_configuration(engine, person, config), config}

      config ->
        updated = %{config | levels: levels}
        {:ok, put_configuration(engine, person, updated), updated}
    end
  end

  def record_initial(%Engine{} = engine, key, record) do
    person = elem(key, 0)
    config = Map.get(engine.bounce_levels, person)
    accepted_at = Map.get(record, :accepted_at, 0)
    accepted_seq = Map.get(record, :accepted_seq, 0)

    revision = %{
      raw_revision: 1,
      accepted_at_ms: accepted_at,
      qualified: is_map(config) and accepted_seq > config.after_delivery_seq,
      selection: nil
    }

    state = %{
      current_substantive_revision: 1,
      raw_to_substantive: %{1 => 1},
      revisions: %{1 => revision}
    }

    put_revision_state(engine, key, state)
  end

  def record_revision(
        %Engine{} = engine,
        key,
        record,
        raw_revision,
        revised_message,
        files,
        accepted_at,
        conversation_seq
      ) do
    state = Map.get(engine.bounce_revisions, key) || legacy_revision_state(record)

    previous_raw =
      Map.get(state.raw_to_substantive, record.revision, state.current_substantive_revision)

    previous_revision = Map.fetch!(state.revisions, previous_raw)
    previous_files = get_in(record, [:files_by_revision, record.revision]) || []

    if BouncePolicy.same_substance?(record.message, previous_files, revised_message, files) do
      state = %{
        state
        | raw_to_substantive: Map.put(state.raw_to_substantive, raw_revision, previous_raw),
          current_substantive_revision: previous_raw
      }

      {put_revision_state(engine, key, state), previous_raw, false}
    else
      config = Map.get(engine.bounce_levels, elem(key, 0))
      sub_revision = state.current_substantive_revision + 1
      previous_revision = supersede_pending(previous_revision)

      current = %{
        raw_revision: raw_revision,
        accepted_at_ms: accepted_at,
        qualified: is_map(config) and conversation_seq > config.after_conversation_seq,
        selection: nil
      }

      state = %{
        state
        | current_substantive_revision: sub_revision,
          raw_to_substantive: Map.put(state.raw_to_substantive, raw_revision, sub_revision),
          revisions:
            state.revisions
            |> Map.put(previous_raw, previous_revision)
            |> Map.put(sub_revision, current)
      }

      {put_revision_state(engine, key, state), sub_revision, true}
    end
  end

  def substantive_revision(%Engine{} = engine, key, record, raw_revision) do
    state = Map.get(engine.bounce_revisions, key) || legacy_revision_state(record)

    case Map.fetch(state.raw_to_substantive, raw_revision) do
      {:ok, sub_revision} ->
        {put_revision_state(engine, key, state), sub_revision}

      :error ->
        {engine, nil}
    end
  end

  def record_open(%Engine{} = engine, key, record, raw_revision) do
    case substantive_revision(engine, key, record, raw_revision) do
      {next, sub_revision} when is_integer(sub_revision) ->
        opened =
          MapSet.put(
            next.opened_revisions,
            Tuple.insert_at(key, tuple_size(key), sub_revision)
          )

        {:ok, %{next | opened_revisions: opened}, sub_revision}

      {_next, nil} ->
        {:error, :invalid_revision}
    end
  end

  def advisory_supported?(%Engine{} = engine, person, origin, handle) do
    match?({:ok, _}, supported_session(engine, person, origin, handle))
  end

  def advisory_cursor(%Engine{} = engine, person, origin, handle) do
    Map.get(engine.advisory_feeds, {person, origin, handle}, empty_advisory_feed()).high
  end

  def record_scores(%Engine{} = engine, person, entries, now_ms)
      when is_binary(person) and is_list(entries) and is_integer(now_ms) do
    {next, _keys} = record_scores_with_changes(engine, person, entries, now_ms)
    next
  end

  def record_scores(%Engine{} = engine, _person, _entries, _now_ms), do: engine

  @doc false
  def record_scores_with_changes(%Engine{} = engine, person, entries, now_ms)
      when is_binary(person) and is_list(entries) and is_integer(now_ms) do
    {engine, keys} =
      Enum.reduce(entries, {engine, []}, fn
        %{"score" => score} = entry, {current, keys}
        when not is_map_key(entry, "conversation_seq") ->
          key = {person, entry["sender_id"], entry["message_id"]}
          {record_score(current, key, score), [key | keys]}

        _, acc ->
          acc
      end)

    keys = Enum.uniq(keys)
    {evaluate_due_for_keys(engine, keys, now_ms), keys}
  end

  def record_scores_with_changes(%Engine{} = engine, _person, _entries, _now_ms),
    do: {engine, []}

  def evaluate_due(%Engine{} = engine, now_ms) when is_integer(now_ms) do
    {next, _keys} = evaluate_due_with_changes(engine, now_ms)
    next
  end

  def evaluate_due(%Engine{} = engine, _now_ms), do: engine

  @doc false
  def evaluate_due_with_changes(%Engine{} = engine, now_ms) when is_integer(now_ms) do
    evaluate_due_from_index(engine, now_ms, [])
  end

  def evaluate_due_with_changes(%Engine{} = engine, _now_ms), do: {engine, []}

  @doc false
  def advisory_feed_keys_for_changes(%Engine{} = engine, keys) when is_list(keys) do
    keys
    |> Enum.reduce(MapSet.new(), fn
      {person, _sender, _message_id} = key, changed ->
        case Map.get(engine.messages, key) do
          %{origin: origin, session_handle: handle}
          when is_binary(origin) and is_binary(handle) ->
            MapSet.put(changed, {person, origin, handle})

          _ ->
            changed
        end

      _, changed ->
        changed
    end)
    |> MapSet.to_list()
  end

  def advisory_feed_keys_for_changes(%Engine{}, _keys), do: []

  def next_deadline(%Engine{} = engine) do
    case :gb_trees.is_empty(engine.bounce_deadline_index) do
      true ->
        nil

      false ->
        {{due_at, _key, _sub_revision}, _value} = :gb_trees.smallest(engine.bounce_deadline_index)
        due_at
    end
  end

  def requeue_supported_session(%Engine{} = engine, session_key) do
    waiting = Map.get(engine.bounce_waiting_routes, session_key, MapSet.new())

    {engine, still_waiting} =
      Enum.reduce(waiting, {engine, MapSet.new()}, fn {key, sub_revision} = item,
                                                      {current, retained} ->
        case current_pending(current, key, sub_revision) do
          {due_at, record} when is_integer(due_at) ->
            if route_status(current, key, record) == :supported do
              {put_deadline(current, due_at, key, sub_revision), retained}
            else
              {current, MapSet.put(retained, item)}
            end

          _ ->
            {current, retained}
        end
      end)

    waiting_routes =
      if MapSet.size(still_waiting) == 0 do
        Map.delete(engine.bounce_waiting_routes, session_key)
      else
        Map.put(engine.bounce_waiting_routes, session_key, still_waiting)
      end

    %{engine | bounce_waiting_routes: waiting_routes}
  end

  def rebuild_indexes(%Engine{} = engine) do
    old_waiting = flatten_waiting(engine.bounce_waiting_routes)

    engine = %{
      engine
      | bounce_deadline_index: :gb_trees.empty(),
        bounce_waiting_routes: %{},
        bounce_discussed_messages: discussed_message_index(engine),
        bounce_origin_entries: origin_entry_index(engine),
        advisory_id_index: build_advisory_id_index(engine.advisory_feeds)
    }

    Enum.reduce(engine.bounce_revisions, engine, fn {key, state}, current ->
      case current_pending(current, key, state.current_substantive_revision) do
        {due_at, record} when is_integer(due_at) ->
          item = {key, state.current_substantive_revision}

          if MapSet.member?(old_waiting, item) and
               route_status(current, key, record) != :supported do
            defer_pending(current, key, state.current_substantive_revision)
          else
            put_deadline(current, due_at, key, state.current_substantive_revision)
          end

        _ ->
          current
      end
    end)
  end

  def advisory_feed(%Engine{} = engine, key, handle, since) do
    with {:ok, person} <- Engine.authorized_person(engine, key),
         true <- Message.uuid_v4?(handle),
         true <- is_integer(since) and since >= 0,
         {:ok, _session} <- supported_session(engine, person, key, handle) do
      feed = Map.get(engine.advisory_feeds, {person, key, handle}, empty_advisory_feed())

      if since <= feed.high do
        entries =
          if since == feed.high do
            []
          else
            Enum.map((since + 1)..feed.high, fn sequence ->
              entry = Map.fetch!(feed.entries, sequence)

              Map.put(entry, "delivered", MapSet.member?(feed.receipts, entry["advisory_id"]))
            end)
          end

        {:ok,
         %{
           "advisory_v1" => true,
           "session_handle" => handle,
           "cursor" => feed.high,
           "entries" => entries
         }}
      else
        {:error, :invalid_cursor}
      end
    else
      false -> {:error, :invalid_field}
      {:error, :not_found} -> {:error, :not_found}
      {:error, :forbidden} -> {:error, :forbidden}
      _ -> {:error, :invalid_cursor}
    end
  end

  def advisory_ack(%Engine{} = engine, key, handle, advisory_id) do
    with {:ok, person} <- Engine.authorized_person(engine, key),
         true <- Message.uuid_v4?(handle),
         true <- Message.uuid_v4?(advisory_id),
         {:ok, _session} <- supported_session(engine, person, key, handle) do
      feed_key = {person, key, handle}
      feed = Map.get(engine.advisory_feeds, feed_key, empty_advisory_feed())

      case Map.fetch(engine.advisory_id_index, {feed_key, advisory_id}) do
        :error ->
          {:error, :not_found}

        {:ok, _sequence} ->
          if MapSet.member?(feed.receipts, advisory_id) do
            {:ok, engine,
             %{
               "session_handle" => handle,
               "advisory_id" => advisory_id,
               "delivered" => true,
               "already_delivered" => true
             }}
          else
            updated = %{feed | receipts: MapSet.put(feed.receipts, advisory_id)}

            {:ok, %{engine | advisory_feeds: Map.put(engine.advisory_feeds, feed_key, updated)},
             %{
               "session_handle" => handle,
               "advisory_id" => advisory_id,
               "delivered" => true,
               "already_delivered" => false
             }}
          end
      end
    else
      false -> {:error, :invalid_field}
      {:error, :not_found} -> {:error, :not_found}
      error -> error
    end
  end

  def valid_state?(%Engine{} = engine, :incremental) do
    is_map(engine.bounce_levels) and is_map(engine.bounce_revisions) and
      is_struct(engine.opened_revisions, MapSet) and
      is_struct(engine.bounce_discussed_messages, MapSet) and
      is_struct(engine.bounce_origin_entries, MapSet) and is_map(engine.bounce_waiting_routes) and
      is_map(engine.advisory_id_index) and
      valid_deadline_tree?(engine.bounce_deadline_index)
  rescue
    _ -> false
  end

  def valid_state?(%Engine{} = engine, :full) do
    valid_configurations?(engine) and valid_revision_states?(engine) and
      valid_opened_revisions?(engine) and valid_advisory_feeds?(engine) and
      valid_indexes?(engine)
  rescue
    _ -> false
  end

  def valid_state?(%Engine{} = engine), do: valid_state?(engine, :full)

  def valid_state?(_, _), do: false
  def valid_state?(_), do: false

  def advisory_text(revision, evaluated_at_ms)
      when is_integer(revision) and revision > 0 and is_integer(evaluated_at_ms) do
    @no_record_sentence <>
      " This notice concerns revision #{revision}, using records available at #{evaluated_at_ms}. " <>
      "Please decide whether this message is still valid and worth resending, and whether " <>
      "increased importance warrants raising its priority."
  end

  defp record_score(engine, {person, sender, message_id} = key, score)
       when is_binary(sender) and is_binary(message_id) do
    with %{message: message} = record <- Map.get(engine.messages, key),
         true <- Message.valid_message_type?(message["message_type"]),
         %{levels: levels} = _config <- Map.get(engine.bounce_levels, person),
         state <- Map.get(engine.bounce_revisions, key) || legacy_revision_state(record),
         sub_revision when is_integer(sub_revision) <- state.current_substantive_revision,
         %{qualified: true, selection: nil} = revision <- Map.get(state.revisions, sub_revision),
         {:ok, selected} <- BouncePolicy.select_level(levels, score) do
      due_at =
        if is_integer(selected["interval_ms"]),
          do: revision.accepted_at_ms + selected["interval_ms"],
          else: nil

      selection = %{
        "level_name" => selected["name"],
        "bounce_after_days" => selected["bounce_after_days"],
        "interval_ms" => selected["interval_ms"],
        "due_at_ms" => due_at,
        "decision" => if(is_nil(due_at), do: "disabled", else: "pending"),
        "advisory_id" => nil,
        "reason" => nil
      }

      updated_revision = %{revision | selection: selection}
      updated = %{state | revisions: Map.put(state.revisions, sub_revision, updated_revision)}
      put_revision_state(engine, key, updated)
    else
      _ -> engine
    end
  end

  defp record_score(engine, _key, _score), do: engine

  defp evaluate_due_from_index(engine, now_ms, changed_keys) do
    case :gb_trees.is_empty(engine.bounce_deadline_index) do
      true ->
        {engine, Enum.reverse(changed_keys)}

      false ->
        {{due_at, key, sub_revision}, _value} = :gb_trees.smallest(engine.bounce_deadline_index)

        if due_at <= now_ms do
          engine
          |> remove_deadline(due_at, key, sub_revision)
          |> evaluate_message(key, sub_revision, now_ms)
          |> then(&evaluate_due_from_index(&1, now_ms, [key | changed_keys]))
        else
          {engine, Enum.reverse(changed_keys)}
        end
    end
  end

  defp evaluate_due_for_keys(engine, keys, now_ms) do
    Enum.reduce(keys, engine, fn key, current ->
      case current_pending(current, key, current_substantive(current, key)) do
        {due_at, _record} when due_at <= now_ms ->
          current
          |> remove_deadline(due_at, key, current_substantive(current, key))
          |> evaluate_message(key, current_substantive(current, key), now_ms)

        _ ->
          current
      end
    end)
  end

  defp current_substantive(engine, key) do
    case Map.get(engine.bounce_revisions, key) do
      %{current_substantive_revision: sub_revision} -> sub_revision
      _ -> nil
    end
  end

  defp current_pending(engine, key, sub_revision) when is_integer(sub_revision) do
    with %{current_substantive_revision: ^sub_revision} = state <-
           Map.get(engine.bounce_revisions, key),
         %{selection: %{"decision" => "pending", "due_at_ms" => due_at}}
         when is_integer(due_at) <- Map.get(state.revisions, sub_revision),
         record when is_map(record) <- Map.get(engine.messages, key) do
      {due_at, record}
    else
      _ -> nil
    end
  end

  defp current_pending(_engine, _key, _sub_revision), do: nil

  defp evaluate_message(engine, key, sub_revision, now_ms) do
    state = Map.get(engine.bounce_revisions, key)
    record = Map.get(engine.messages, key)

    if is_map(state) and is_map(record) do
      case {state.current_substantive_revision, Map.get(state.revisions, sub_revision)} do
        {^sub_revision,
         %{selection: %{"decision" => "pending", "due_at_ms" => due_at} = selection} = revision}
        when is_integer(due_at) and due_at <= now_ms ->
          evaluate_selection(
            engine,
            key,
            record,
            state,
            sub_revision,
            revision,
            selection,
            now_ms
          )

        _ ->
          engine
      end
    else
      engine
    end
  end

  defp evaluate_selection(engine, key, record, state, sub_revision, revision, selection, now_ms) do
    reason = suppression_reason(engine, key, record, state, sub_revision, now_ms)

    cond do
      reason ->
        update_selection(engine, key, state, sub_revision, selection, "suppressed", reason, nil)

      true ->
        case route_status(engine, key, record) do
          :supported ->
            append_advisory(engine, key, state, sub_revision, revision, selection, now_ms)

          _ ->
            defer_pending(engine, key, sub_revision)
        end
    end
  end

  defp suppression_reason(engine, key, record, state, sub_revision, now_ms) do
    cond do
      state.current_substantive_revision != sub_revision ->
        "superseded"

      not Map.get(state.revisions[sub_revision], :qualified, false) ->
        "not_qualified"

      not is_nil(record.terminal) ->
        "terminal"

      default_answer_due?(record, now_ms) ->
        "default_answered"

      not Message.valid_message_type?(record.message["message_type"]) ->
        "ineligible_message"

      person_discussed?(engine, key) ->
        "person_discussion"

      MapSet.member?(
        engine.opened_revisions,
        Tuple.insert_at(key, tuple_size(key), sub_revision)
      ) ->
        "opened"

      true ->
        nil
    end
  end

  defp default_answer_due?(record, now_ms) do
    accepted_at = Map.get(record, :revision_accepted_at, Map.get(record, :accepted_at))

    case Message.default_answer(record.message, accepted_at) do
      %{at: at} when is_integer(at) -> at <= now_ms
      _ -> false
    end
  end

  defp person_discussed?(engine, key),
    do: MapSet.member?(engine.bounce_discussed_messages, key)

  defp route_status(engine, {person, _sender, _message_id} = key, record) do
    origin = Map.get(record, :origin)
    handle = Map.get(record, :session_handle)
    device = Map.get(engine.devices, origin)

    cond do
      not is_binary(origin) or not Message.uuid_v4?(handle) ->
        :unroutable

      not match?(%{person: ^person, revoked: false}, device) ->
        :unroutable

      not MapSet.member?(
        engine.bounce_origin_entries,
        {person, origin, handle, elem(key, 1), elem(key, 2)}
      ) ->
        :unroutable

      true ->
        case Map.get(engine.origin_sessions, {person, origin, handle}) do
          %{gone: false, support: %{"advisory_v1" => true, "unsupported_reason" => nil}} ->
            :supported

          _ ->
            :unsupported
        end
    end
  end

  defp supported_session(engine, person, origin, handle) do
    case Map.get(engine.origin_sessions, {person, origin, handle}) do
      %{support: %{"advisory_v1" => true, "unsupported_reason" => nil}, gone: false} = session ->
        {:ok, session}

      nil ->
        {:error, :not_found}

      _ ->
        {:error, :not_found}
    end
  end

  defp append_advisory(engine, key, state, sub_revision, revision, selection, now_ms) do
    {person, sender, message_id} = key
    record = Map.fetch!(engine.messages, key)
    origin = record.origin
    handle = record.session_handle
    feed_key = {person, origin, handle}
    feed = Map.get(engine.advisory_feeds, feed_key, empty_advisory_feed())
    sequence = feed.high + 1
    advisory_id = uuid_v4()
    origin_cursor = get_in(engine.origin_feeds, [{person, origin}, :high]) || 0

    entry = %{
      "kind" => "advisory",
      "advisory_type" => "unseen_interval_elapsed",
      "author" => "server",
      "advisory_id" => advisory_id,
      "advisory_seq" => sequence,
      "sender_id" => sender,
      "message_id" => message_id,
      "session_handle" => handle,
      "request_revision" => sub_revision,
      "accepted_at_ms" => revision.accepted_at_ms,
      "due_at_ms" => selection["due_at_ms"],
      "evaluated_at_ms" => now_ms,
      "level_name" => selection["level_name"],
      "bounce_after_days" => selection["bounce_after_days"],
      "origin_cursor_at_decision" => origin_cursor,
      "text" => advisory_text(sub_revision, now_ms)
    }

    updated_feed = %{
      feed
      | high: sequence,
        entries: Map.put(feed.entries, sequence, entry)
    }

    engine = %{
      engine
      | advisory_feeds: Map.put(engine.advisory_feeds, feed_key, updated_feed),
        advisory_id_index:
          Map.put(engine.advisory_id_index, {feed_key, advisory_id}, sequence)
    }

    update_selection(engine, key, state, sub_revision, selection, "bounced", nil, advisory_id)
  end

  defp update_selection(
         engine,
         key,
         state,
         sub_revision,
         selection,
         decision,
         reason,
         advisory_id
       ) do
    updated_selection =
      selection
      |> Map.put("decision", decision)
      |> Map.put("reason", reason)
      |> Map.put("advisory_id", advisory_id)

    revision = Map.fetch!(state.revisions, sub_revision)

    updated_state = %{
      state
      | revisions:
          Map.put(state.revisions, sub_revision, %{revision | selection: updated_selection})
    }

    put_revision_state(engine, key, updated_state)
  end

  defp pending_revision(state) when is_map(state) do
    sub_revision = Map.get(state, :current_substantive_revision)

    case get_in(state, [:revisions, sub_revision, :selection]) do
      %{"decision" => "pending", "due_at_ms" => due_at} when is_integer(due_at) ->
        {sub_revision, due_at}

      _ ->
        nil
    end
  end

  defp pending_revision(_), do: nil

  defp put_revision_state(engine, key, state) do
    old_state = Map.get(engine.bounce_revisions, key)
    engine = remove_revision_index(engine, key, old_state)
    engine = %{engine | bounce_revisions: Map.put(engine.bounce_revisions, key, state)}
    add_revision_index(engine, key, state)
  end

  defp remove_revision_index(engine, key, state) do
    case pending_revision(state) do
      {sub_revision, due_at} ->
        engine
        |> remove_deadline(due_at, key, sub_revision)
        |> remove_waiting_route(key, sub_revision)

      nil ->
        engine
    end
  end

  defp add_revision_index(engine, key, state) do
    case pending_revision(state) do
      {sub_revision, due_at} -> put_deadline(engine, due_at, key, sub_revision)
      nil -> engine
    end
  end

  defp put_deadline(engine, due_at, key, sub_revision) do
    index_key = {due_at, key, sub_revision}

    %{
      engine
      | bounce_deadline_index: :gb_trees.enter(index_key, true, engine.bounce_deadline_index)
    }
  end

  defp remove_deadline(engine, due_at, key, sub_revision) do
    index_key = {due_at, key, sub_revision}
    tree = engine.bounce_deadline_index

    if :gb_trees.is_defined(index_key, tree),
      do: %{engine | bounce_deadline_index: :gb_trees.delete(index_key, tree)},
      else: engine
  end

  defp defer_pending(engine, key, sub_revision) do
    case current_pending(engine, key, sub_revision) do
      {due_at, record} ->
        {person, _sender, _message_id} = key
        session_key = {person, Map.get(record, :origin), Map.get(record, :session_handle)}
        item = {key, sub_revision}

        engine
        |> remove_deadline(due_at, key, sub_revision)
        |> then(fn current ->
          waiting = Map.get(current.bounce_waiting_routes, session_key, MapSet.new())

          %{
            current
            | bounce_waiting_routes:
                Map.put(current.bounce_waiting_routes, session_key, MapSet.put(waiting, item))
          }
        end)

      _ ->
        engine
    end
  end

  defp remove_waiting_route(engine, key, sub_revision) do
    {person, _sender, _message_id} = key

    session_key =
      case Map.get(engine.messages, key) do
        record when is_map(record) ->
          {person, Map.get(record, :origin), Map.get(record, :session_handle)}

        _ ->
          nil
      end

    case Map.get(engine.bounce_waiting_routes, session_key) do
      %MapSet{} = waiting ->
        remaining = MapSet.delete(waiting, {key, sub_revision})

        waiting_routes =
          if MapSet.size(remaining) == 0,
            do: Map.delete(engine.bounce_waiting_routes, session_key),
            else: Map.put(engine.bounce_waiting_routes, session_key, remaining)

        %{engine | bounce_waiting_routes: waiting_routes}

      _ ->
        engine
    end
  end

  defp flatten_waiting(waiting_routes) do
    Enum.reduce(waiting_routes, MapSet.new(), fn {_session_key, entries}, acc ->
      MapSet.union(acc, entries)
    end)
  end

  defp discussed_message_index(engine) do
    Enum.reduce(engine.conversation_events, MapSet.new(), fn {_event_key, event}, acc ->
      if event.payload["kind"] == "discussion" and event.payload["author"] == "person",
        do: MapSet.put(acc, event.key),
        else: acc
    end)
  end

  defp origin_entry_index(engine) do
    Enum.reduce(engine.origin_feeds, MapSet.new(), fn {{person, origin}, feed}, acc ->
      Enum.reduce(feed.entries, acc, fn {_sequence, entry}, index ->
        if entry["kind"] == "revision" and entry["author"] == "origin" and
             is_binary(entry["session_handle"]) and is_binary(entry["sender_id"]) and
             is_binary(entry["message_id"]) do
          MapSet.put(
            index,
            {person, origin, entry["session_handle"], entry["sender_id"], entry["message_id"]}
          )
        else
          index
        end
      end)
    end)
  end

  defp valid_indexes?(engine) do
    pending = pending_revisions(engine)
    waiting = flatten_waiting(engine.bounce_waiting_routes)
    queued = :gb_trees.to_list(engine.bounce_deadline_index)

    queued_ids =
      Enum.reduce(queued, MapSet.new(), fn {{due_at, key, sub_revision}, true}, acc ->
        if Map.get(pending, {key, sub_revision}) == due_at,
          do: MapSet.put(acc, {key, sub_revision}),
          else: acc
      end)

    expected_queued = MapSet.difference(Map.keys(pending) |> MapSet.new(), waiting)

    Enum.all?(engine.bounce_waiting_routes, fn {session_key, entries} ->
      is_tuple(session_key) and tuple_size(session_key) == 3 and is_struct(entries, MapSet) and
        Enum.all?(entries, fn {key, sub_revision} = item ->
          case {Map.get(pending, item), Map.get(engine.messages, key)} do
            {due_at, record} when is_integer(due_at) and is_map(record) ->
              session_key == bounce_session_key(key, record)

            _ ->
              false
          end
        end)
    end) and
      MapSet.size(waiting) + MapSet.size(queued_ids) == map_size(pending) and
      MapSet.equal?(queued_ids, expected_queued) and
      MapSet.equal?(engine.bounce_discussed_messages, discussed_message_index(engine)) and
      MapSet.equal?(engine.bounce_origin_entries, origin_entry_index(engine)) and
      engine.advisory_id_index == build_advisory_id_index(engine.advisory_feeds)
  end

  defp pending_revisions(engine) do
    Enum.reduce(engine.bounce_revisions, %{}, fn {key, state}, acc ->
      case pending_revision(state) do
        {sub_revision, due_at} -> Map.put(acc, {key, sub_revision}, due_at)
        nil -> acc
      end
    end)
  end

  defp bounce_session_key({person, _sender, _message_id}, record),
    do: {person, Map.get(record, :origin), Map.get(record, :session_handle)}

  defp valid_deadline_tree?(tree) do
    is_tuple(tree) and tuple_size(tree) == 2 and is_integer(elem(tree, 0)) and
      elem(tree, 0) >= 0 and :gb_trees.size(tree) == elem(tree, 0)
  rescue
    _ -> false
  end

  defp supersede_pending(%{selection: %{"decision" => "pending"} = selection} = revision) do
    %{
      revision
      | selection: Map.merge(selection, %{"decision" => "suppressed", "reason" => "superseded"})
    }
  end

  defp supersede_pending(revision), do: revision

  defp legacy_revision_state(record) do
    revisions = Map.get(record, :revisions, %{1 => record.message})
    files_by_revision = Map.get(record, :files_by_revision, %{})
    max_raw = Map.get(record, :revision, 1)
    accepted_at = Map.get(record, :revision_accepted_at, Map.get(record, :accepted_at, 0))

    {raw_to_substantive, substantive, current_substantive} =
      Enum.reduce(1..max(max_raw, 1), {%{}, %{}, 0}, fn raw_revision,
                                                        {raw_map, sub_map, current_sub} ->
        message =
          Map.get(
            revisions,
            raw_revision,
            if(raw_revision == max_raw, do: record.message, else: %{})
          )

        files = Map.get(files_by_revision, raw_revision, [])

        previous_raw = raw_revision - 1
        previous_sub = Map.get(raw_map, previous_raw)
        previous_message = Map.get(revisions, previous_raw)
        previous_files = Map.get(files_by_revision, previous_raw, [])

        sub_revision =
          if is_integer(previous_sub) and
               BouncePolicy.same_substance?(
                 previous_message || %{},
                 previous_files,
                 message,
                 files
               ),
             do: previous_sub,
             else: current_sub + 1

        raw_map = Map.put(raw_map, raw_revision, sub_revision)

        sub_map =
          if Map.has_key?(sub_map, sub_revision) do
            sub_map
          else
            Map.put(sub_map, sub_revision, %{
              raw_revision: raw_revision,
              accepted_at_ms: if(raw_revision == max_raw, do: accepted_at, else: 0),
              qualified: false,
              selection: nil
            })
          end

        {raw_map, sub_map, max(current_sub, sub_revision)}
      end)

    %{
      current_substantive_revision: current_substantive,
      raw_to_substantive: raw_to_substantive,
      revisions: substantive
    }
  end

  defp put_configuration(engine, person, config),
    do: %{engine | bounce_levels: Map.put(engine.bounce_levels, person, config)}

  defp empty_advisory_feed, do: %{high: 0, entries: %{}, receipts: MapSet.new()}

  defp build_advisory_id_index(advisory_feeds) do
    Enum.reduce(advisory_feeds, %{}, fn
      {feed_key, %{entries: entries}}, index when is_map(entries) ->
        Enum.reduce(entries, index, fn
          {sequence, %{"advisory_id" => advisory_id}}, current ->
            Map.put(current, {feed_key, advisory_id}, sequence)

          _, current ->
            current
        end)

      _, index ->
        index
    end)
  end

  defp uuid_v4 do
    <<a::binary-size(6), b::8, c::8, d::8, e::binary-size(7)>> = :crypto.strong_rand_bytes(16)

    bytes =
      <<a::binary, Bitwise.bor(Bitwise.band(b, 15), 64), c, Bitwise.bor(Bitwise.band(d, 63), 128),
        e::binary>>

    <<p1::binary-size(4), p2::binary-size(2), p3::binary-size(2), p4::binary-size(2),
      p5::binary-size(6)>> = bytes

    Enum.map_join([p1, p2, p3, p4, p5], "-", &Base.encode16(&1, case: :lower))
  end

  defp valid_configurations?(engine) do
    is_map(engine.bounce_levels) and
      Enum.all?(engine.bounce_levels, fn {person, config} ->
        Map.has_key?(engine.persons, person) and is_map(config) and
          Map.keys(config) |> Enum.sort() ==
            [:after_conversation_seq, :after_delivery_seq, :configured_at_ms, :levels] and
          BouncePolicy.valid_normalized_levels?(config.levels) and
          is_integer(config.configured_at_ms) and config.configured_at_ms >= 0 and
          is_integer(config.after_delivery_seq) and config.after_delivery_seq >= 0 and
          config.after_delivery_seq <= Map.get(engine.persons, person, 0) and
          is_integer(config.after_conversation_seq) and config.after_conversation_seq >= 0 and
          config.after_conversation_seq <= Map.get(engine.conversation_highs, person, 0)
      end)
  end

  defp valid_revision_states?(engine) do
    is_map(engine.bounce_revisions) and
      Enum.all?(engine.bounce_revisions, fn {key = {person, sender, message_id}, state} ->
        record = Map.get(engine.messages, key)

        map_size_ok =
          match?(%{revision: revision} when is_integer(revision) and revision > 0, record)

        map_size_ok and Map.has_key?(engine.persons, person) and Message.sender_id?(sender) and
          is_binary(message_id) and message_id != "" and is_map(state) and
          Map.keys(state) |> Enum.sort() ==
            [:current_substantive_revision, :raw_to_substantive, :revisions] and
          is_integer(state.current_substantive_revision) and
          state.current_substantive_revision > 0 and
          state.current_substantive_revision <= map_size(state.revisions) and
          is_map(state.raw_to_substantive) and is_map(state.revisions) and
          map_size(state.raw_to_substantive) == record.revision and
          Enum.all?(1..record.revision, fn raw_revision ->
            sub_revision = Map.get(state.raw_to_substantive, raw_revision)
            is_integer(sub_revision) and Map.has_key?(state.revisions, sub_revision)
          end) and
          Map.get(state.raw_to_substantive, record.revision) == state.current_substantive_revision and
          Enum.all?(state.revisions, fn {sub_revision, revision} ->
            is_integer(sub_revision) and sub_revision > 0 and is_map(revision) and
              Map.keys(revision) |> Enum.sort() == [
                :accepted_at_ms,
                :qualified,
                :raw_revision,
                :selection
              ] and
              is_integer(revision.raw_revision) and revision.raw_revision > 0 and
              Map.get(state.raw_to_substantive, revision.raw_revision) == sub_revision and
              is_integer(revision.accepted_at_ms) and revision.accepted_at_ms >= 0 and
              is_boolean(revision.qualified) and
              valid_selection?(revision.selection, revision.accepted_at_ms)
          end)
      end)
  end

  defp valid_selection?(nil, _accepted_at), do: true

  defp valid_selection?(selection, accepted_at) when is_map(selection) do
    Map.keys(selection) |> Enum.sort() ==
      [
        "advisory_id",
        "bounce_after_days",
        "decision",
        "due_at_ms",
        "interval_ms",
        "level_name",
        "reason"
      ] and
      is_binary(selection["level_name"]) and selection["level_name"] != "" and
      valid_interval_selection?(selection, accepted_at) and
      selection["decision"] in ["disabled", "pending", "bounced", "suppressed"] and
      valid_selection_decision?(selection)
  end

  defp valid_selection?(_, _), do: false

  defp valid_interval_selection?(
         %{"bounce_after_days" => nil, "interval_ms" => nil, "due_at_ms" => nil},
         _
       ),
       do: true

  defp valid_interval_selection?(selection, accepted_at) do
    with {:ok, expected_interval} <- BouncePolicy.interval_ms(selection["bounce_after_days"]),
         due_at when is_integer(due_at) and due_at >= accepted_at <- selection["due_at_ms"] do
      expected_interval == selection["interval_ms"] and due_at == accepted_at + expected_interval
    else
      _ -> false
    end
  end

  defp valid_selection_decision?(%{
         "decision" => "disabled",
         "advisory_id" => nil,
         "reason" => nil,
         "interval_ms" => nil
       }),
       do: true

  defp valid_selection_decision?(%{
         "decision" => "pending",
         "advisory_id" => nil,
         "reason" => nil,
         "interval_ms" => interval
       })
       when is_integer(interval) and interval > 0, do: true

  defp valid_selection_decision?(%{
         "decision" => "bounced",
         "advisory_id" => id,
         "reason" => nil
       }),
       do: Message.uuid_v4?(id)

  defp valid_selection_decision?(%{
         "decision" => "suppressed",
         "advisory_id" => nil,
         "reason" => reason
       }),
       do:
         reason in [
           "superseded",
           "not_qualified",
           "terminal",
           "default_answered",
           "ineligible_message",
           "person_discussion",
           "opened"
         ]

  defp valid_selection_decision?(_), do: false

  defp valid_opened_revisions?(engine) do
    is_struct(engine.opened_revisions, MapSet) and
      Enum.all?(engine.opened_revisions, fn {person, sender, message_id, sub_revision} ->
        key = {person, sender, message_id}

        Map.has_key?(engine.messages, key) and Map.has_key?(engine.persons, person) and
          Message.sender_id?(sender) and is_binary(message_id) and message_id != "" and
          match?(
            %{revisions: revisions} when is_map_key(revisions, sub_revision),
            Map.get(engine.bounce_revisions, key)
          )
      end)
  end

  defp valid_advisory_feeds?(engine) do
    is_map(engine.advisory_feeds) and
      Enum.all?(engine.advisory_feeds, fn {{person, origin, handle} = feed_key, feed} ->
        origin_session = Map.get(engine.origin_sessions, feed_key)

        is_map(origin_session) and Map.get(engine.devices, origin) != nil and
          Map.get(engine.devices[origin], :person) == person and Message.uuid_v4?(handle) and
          is_map(feed) and Map.keys(feed) |> Enum.sort() == [:entries, :high, :receipts] and
          is_integer(feed.high) and feed.high >= 0 and is_map(feed.entries) and
          map_size(feed.entries) == feed.high and is_struct(feed.receipts, MapSet) and
          Enum.all?(sequence_range(feed.high), fn sequence ->
            case Map.get(feed.entries, sequence) do
              nil -> false
              entry -> valid_advisory_entry?(engine, person, origin, handle, sequence, entry)
            end
          end) and
          Enum.all?(feed.receipts, fn advisory_id ->
            Message.uuid_v4?(advisory_id) and
              Enum.any?(feed.entries, fn {_seq, entry} -> entry["advisory_id"] == advisory_id end)
          end)
      end)
  end

  defp valid_advisory_entry?(engine, person, origin, handle, sequence, entry) do
    key = {person, entry["sender_id"], entry["message_id"]}
    state = Map.get(engine.bounce_revisions, key)
    sub_revision = entry["request_revision"]
    selection = get_in(state, [:revisions, sub_revision, :selection])
    record = Map.get(engine.messages, key)

    is_map(entry) and Map.keys(entry) |> Enum.sort() == Enum.sort(@advisory_fields) and
      entry["kind"] == "advisory" and entry["advisory_type"] == "unseen_interval_elapsed" and
      entry["author"] == "server" and Message.uuid_v4?(entry["advisory_id"]) and
      entry["advisory_seq"] == sequence and entry["session_handle"] == handle and
      Message.sender_id?(entry["sender_id"]) and is_binary(entry["message_id"]) and
      entry["message_id"] != "" and
      is_integer(sub_revision) and sub_revision > 0 and
      Enum.all?(
        ~w(accepted_at_ms due_at_ms evaluated_at_ms origin_cursor_at_decision),
        &(is_integer(entry[&1]) and entry[&1] >= 0)
      ) and
      entry["due_at_ms"] >= entry["accepted_at_ms"] and
      entry["origin_cursor_at_decision"] <= get_in(engine.origin_feeds, [{person, origin}, :high]) and
      is_binary(entry["level_name"]) and entry["level_name"] != "" and
      is_binary(entry["text"]) and
      entry["text"] == advisory_text(sub_revision, entry["evaluated_at_ms"]) and
      is_map(record) and record.origin == origin and record.session_handle == handle and
      is_map(selection) and selection["decision"] == "bounced" and
      selection["advisory_id"] == entry["advisory_id"] and
      selection["level_name"] == entry["level_name"] and
      selection["bounce_after_days"] == entry["bounce_after_days"] and
      selection["due_at_ms"] == entry["due_at_ms"]
  end

  defp sequence_range(0), do: []
  defp sequence_range(high), do: 1..high
end
