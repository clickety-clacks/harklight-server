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
    engine =
      Enum.reduce(entries, engine, fn
        %{"score" => score} = entry, current when not is_map_key(entry, "conversation_seq") ->
          key = {person, entry["sender_id"], entry["message_id"]}
          record_score(current, key, score)

        _, current ->
          current
      end)

    evaluate_due(engine, now_ms)
  end

  def record_scores(%Engine{} = engine, _person, _entries, _now_ms), do: engine

  def evaluate_due(%Engine{} = engine, now_ms) when is_integer(now_ms) do
    engine.bounce_revisions
    |> Map.keys()
    |> Enum.reduce(engine, fn key, current -> evaluate_message(current, key, now_ms) end)
  end

  def evaluate_due(%Engine{} = engine, _now_ms), do: engine

  def next_deadline(%Engine{} = engine) do
    for {key, state} <- engine.bounce_revisions,
        {sub_revision, revision} <- state.revisions,
        sub_revision == state.current_substantive_revision,
        %{
          "decision" => "pending",
          "due_at_ms" => due_at
        } <- [revision.selection],
        is_integer(due_at),
        record when not is_nil(record) <- [Map.get(engine.messages, key)],
        route_status(engine, key, record) == :supported do
      due_at
    end
    |> Enum.min(&<=/2, fn -> nil end)
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

      case Enum.find(feed.entries, fn {_seq, entry} -> entry["advisory_id"] == advisory_id end) do
        nil ->
          {:error, :not_found}

        {_seq, _entry} ->
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

  def valid_state?(%Engine{} = engine) do
    valid_configurations?(engine) and valid_revision_states?(engine) and
      valid_opened_revisions?(engine) and valid_advisory_feeds?(engine)
  rescue
    _ -> false
  end

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

  defp evaluate_message(engine, key, now_ms) do
    state = Map.get(engine.bounce_revisions, key)
    record = Map.get(engine.messages, key)

    if is_map(state) and is_map(record) do
      sub_revision = state.current_substantive_revision

      case Map.get(state.revisions, sub_revision) do
        %{selection: %{"decision" => "pending", "due_at_ms" => due_at} = selection} = revision
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
            engine
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

  defp person_discussed?(engine, key) do
    Enum.any?(engine.conversation_events, fn {_event_key, event} ->
      event.key == key and event.payload["author"] == "person" and
        event.payload["kind"] == "discussion"
    end)
  end

  defp route_status(engine, {person, _sender, _message_id} = key, record) do
    origin = Map.get(record, :origin)
    handle = Map.get(record, :session_handle)
    device = Map.get(engine.devices, origin)

    cond do
      not is_binary(origin) or not Message.uuid_v4?(handle) ->
        :unroutable

      not match?(%{person: ^person, revoked: false}, device) ->
        :unroutable

      not original_session_entry?(engine, person, origin, handle, key) ->
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

  defp original_session_entry?(engine, person, origin, handle, {_, sender, message_id}) do
    case Map.get(engine.origin_feeds, {person, origin}) do
      %{entries: entries} ->
        Enum.any?(entries, fn {_seq, entry} ->
          entry["kind"] == "revision" and entry["author"] == "origin" and
            entry["session_handle"] == handle and entry["sender_id"] == sender and
            entry["message_id"] == message_id
        end)

      _ ->
        false
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

    engine = %{engine | advisory_feeds: Map.put(engine.advisory_feeds, feed_key, updated_feed)}
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

  defp put_revision_state(engine, key, state),
    do: %{engine | bounce_revisions: Map.put(engine.bounce_revisions, key, state)}

  defp empty_advisory_feed, do: %{high: 0, entries: %{}, receipts: MapSet.new()}

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
