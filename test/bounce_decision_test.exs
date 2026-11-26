defmodule AttentionServer.BounceDecisionTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{BounceDecision, BouncePolicy, Engine}

  @person "person-a"
  @origin "origin-device"
  @sender "app:test"
  @message_id "bounce-message"
  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @accepted_at 1_000
  @levels [
    %{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil},
    %{"name" => "Follow up", "lower_bound" => 0.5, "bounce_after_days" => 1.5},
    %{"name" => "Urgent", "lower_bound" => 0.8, "bounce_after_days" => 2.25}
  ]

  defp key, do: {@person, @sender, @message_id}
  defp origin_feed_key, do: {@person, @origin}
  defp advisory_feed_key, do: {@person, @origin, @handle}

  defp message(title \\ "Question") do
    %{
      "sender_id" => @sender,
      "message_id" => @message_id,
      "created_at" => 1,
      "title" => title,
      "answer_ui" => %{
        "schema" => "attention-client.actions/1",
        "actions" => [
          %{
            "id" => "seen",
            "label" => "Seen",
            "kind" => "value",
            "value" => %{"acknowledgment" => "seen"}
          }
        ]
      },
      "message_type" => "fyi"
    }
  end

  defp attachment(upload_id, hash \\ "abc") do
    %{
      "name" => "context.txt",
      "size" => 3,
      "sha256" => hash,
      "upload_id" => upload_id
    }
  end

  defp fixture(opts \\ []) do
    configured? = Keyword.get(opts, :configured?, true)
    supported? = Keyword.get(opts, :supported?, true)
    accepted_at = Keyword.get(opts, :accepted_at, @accepted_at)
    original_message = message()
    original_files = [attachment("upload-1")]

    {:ok, engine} = Engine.setup_person(Engine.new(), @person)
    {:ok, engine} = Engine.setup_device(engine, @person, @origin)
    {:ok, engine} = Engine.setup_sender(engine, @origin, @sender)

    engine = %{engine | conversation_highs: Map.put(engine.conversation_highs, @person, 0)}

    engine =
      if configured? do
        {:ok, levels} = BouncePolicy.normalize_levels(@levels)
        {:ok, configured, _} = BounceDecision.configure_levels(engine, @person, levels, 500)
        configured
      else
        engine
      end

    record = %{
      message: original_message,
      origin: @origin,
      accepted_seq: 1,
      accepted_at: accepted_at,
      revision_accepted_at: accepted_at,
      terminal: nil,
      session_handle: @handle,
      revision: 1,
      revisions: %{1 => original_message},
      files_by_revision: %{1 => original_files}
    }

    session = %{
      gone: false,
      support: %{
        "advisory_v1" => supported?,
        "unsupported_reason" => nil
      }
    }

    feed_entry = %{
      "kind" => "revision",
      "author" => "origin",
      "session_handle" => @handle,
      "sender_id" => @sender,
      "message_id" => @message_id
    }

    engine = %{
      engine
      | persons: Map.put(engine.persons, @person, 1),
        messages: Map.put(engine.messages, key(), record),
        origin_sessions: Map.put(engine.origin_sessions, advisory_feed_key(), session),
        origin_feeds:
          Map.put(engine.origin_feeds, origin_feed_key(), %{
            high: 1,
            entries: %{1 => feed_entry}
          })
    }

    engine = BounceDecision.rebuild_indexes(engine)
    {BounceDecision.record_initial(engine, key(), record), record}
  end

  defp score(engine, value, now_ms) do
    BounceDecision.record_scores(
      engine,
      @person,
      [%{"sender_id" => @sender, "message_id" => @message_id, "score" => value}],
      now_ms
    )
  end

  defp selection(engine, revision \\ 1) do
    case Map.get(engine.bounce_revisions, key()) do
      %{revisions: revisions} -> get_in(revisions, [revision, :selection])
      _ -> nil
    end
  end

  test "defaults schedule an unscored message without storing a table" do
    {engine, _record} = fixture(configured?: false)
    refute Map.has_key?(engine.bounce_levels, @person)

    assert BounceDecision.levels(engine, @person) ==
             BouncePolicy.public_levels(BouncePolicy.default_levels())

    assert selection(engine)["level_name"] == "Normal"
    assert BounceDecision.next_deadline(engine) == @accepted_at + 259_200_000

    selected = score(engine, %AttentionServer.JSON.DecimalToken{raw: "0.8"}, @accepted_at + 1)

    assert selection(selected)["level_name"] == "Urgent"
    assert selection(selected)["bounce_after_days"] == 0.5
    assert selection(selected)["due_at_ms"] == @accepted_at + 43_200_000

    due = BounceDecision.evaluate_due(selected, @accepted_at + 43_200_000)
    assert selection(due)["decision"] == "bounced"
    assert BounceDecision.advisory_cursor(due, @person, @origin, @handle) == 1

    repeated = BounceDecision.evaluate_due(due, @accepted_at + 86_400_000)
    assert BounceDecision.advisory_cursor(repeated, @person, @origin, @handle) == 1
    refute Map.has_key?(repeated.bounce_levels, @person)
  end

  test "owner table replaces defaults for an unscored eligible revision" do
    {engine, _record} = fixture(configured?: false)

    assert {:ok, owner_levels} =
             BouncePolicy.normalize_levels([
               %{"name" => "Client label", "lower_bound" => 0, "bounce_after_days" => 2}
             ])

    assert {:ok, configured, _config} =
             BounceDecision.configure_levels(engine, @person, owner_levels, @accepted_at + 1)

    assert BounceDecision.levels(configured, @person) ==
             BouncePolicy.public_levels(owner_levels)

    selected = score(configured, 0.9, @accepted_at + 2)
    assert selection(selected)["level_name"] == "Client label"
    assert selection(selected)["bounce_after_days"] == 2
    assert selection(selected)["due_at_ms"] == @accepted_at + 172_800_000
  end

  test "pre-default retained messages stay ineligible while later revisions qualify" do
    {engine, record} = fixture(configured?: false)
    state = engine.bounce_revisions[key()]
    revision = %{state.revisions[1] | qualified: false, selection: nil}
    retained_state = %{state | revisions: Map.put(state.revisions, 1, revision)}

    retained = %{
      engine
      | bounce_revisions: Map.put(engine.bounce_revisions, key(), retained_state)
    }

    assert BounceDecision.levels(retained, @person) ==
             BouncePolicy.public_levels(BouncePolicy.default_levels())

    assert selection(score(retained, 0.9, @accepted_at + 43_200_000)) == nil

    revised_message = message("Changed after defaults") |> Map.put("urgency", "high")
    revised_files = [attachment("upload-2", "def")]
    revised_at = @accepted_at + 10

    {revised, 2, true} =
      BounceDecision.record_revision(
        retained,
        key(),
        record,
        2,
        revised_message,
        revised_files,
        revised_at,
        1
      )

    revised_record = %{
      record
      | message: revised_message,
        revision: 2,
        revision_accepted_at: revised_at,
        revisions: Map.put(record.revisions, 2, revised_message),
        files_by_revision: Map.put(record.files_by_revision, 2, revised_files)
    }

    revised = %{
      revised
      | messages: Map.put(revised.messages, key(), revised_record),
        conversation_highs: Map.put(revised.conversation_highs, @person, 1)
    }

    selected_revision = score(revised, 0.8, revised_at + 1)
    assert selection(selected_revision, 1) == nil
    assert selection(selected_revision, 2)["level_name"] == "Urgent"
    assert selection(selected_revision, 2)["due_at_ms"] == revised_at + 43_200_000

    rebuilt = BounceDecision.rebuild_indexes(retained)
    assert selection(score(rebuilt, 0.9, @accepted_at + 43_200_000)) == nil

    legacy = %{retained | bounce_revisions: Map.delete(retained.bounce_revisions, key())}
    assert selection(score(legacy, 0.9, @accepted_at + 43_200_000)) == nil
    assert BounceDecision.next_deadline(legacy) == nil
    refute Map.has_key?(legacy.bounce_levels, @person)
  end

  test "a score uses the inclusive level and can reset a pending acceptance-based deadline" do
    {engine, _record} = fixture()

    assert BounceDecision.levels(engine, @person) ==
             BouncePolicy.public_levels(engine.bounce_levels[@person].levels)

    selected = score(engine, %AttentionServer.JSON.DecimalToken{raw: "0.8"}, 1_050)

    assert selection(selected)["level_name"] == "Urgent"
    assert selection(selected)["bounce_after_days"] == 2.25
    assert selection(selected)["due_at_ms"] == @accepted_at + 194_400_000

    {:ok, updated_levels} =
      BouncePolicy.normalize_levels([
        %{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil},
        %{"name" => "Follow up", "lower_bound" => 0.5, "bounce_after_days" => 1},
        %{"name" => "Urgent", "lower_bound" => 0.8, "bounce_after_days" => 7}
      ])

    {:ok, edited, _} = BounceDecision.configure_levels(selected, @person, updated_levels, 4_000)
    rescored = score(edited, 0.9, 9_000)

    assert selection(rescored)["level_name"] == "Urgent"
    assert BounceDecision.next_deadline(rescored) == @accepted_at + 604_800_000
  end

  test "Progress gets no score deadline and a later marker suppresses a pending advisory" do
    {engine, record} = fixture()
    progress_message = Map.put(record.message, "progress", nil)
    progress_record = %{record | message: progress_message, revisions: %{1 => progress_message}}
    marked = %{engine | messages: Map.put(engine.messages, key(), progress_record)}

    scored_marked = score(marked, 0.9, 1_050)
    assert selection(scored_marked) == nil
    assert BounceDecision.next_deadline(scored_marked) == nil

    pending = score(engine, 0.9, 1_050)
    due_at = selection(pending)["due_at_ms"]
    pending_marked = %{pending | messages: Map.put(pending.messages, key(), progress_record)}
    evaluated = BounceDecision.evaluate_due(pending_marked, due_at + 1)

    assert selection(evaluated)["decision"] == "suppressed"
    assert selection(evaluated)["reason"] == "ineligible_message"
    assert BounceDecision.advisory_cursor(evaluated, @person, @origin, @handle) == 0
  end

  test "substantive revisions get their own timing; identical replay preserves it" do
    {engine, record} = fixture()
    first = score(engine, 0.75, 1_050)
    first_due = selection(first)["due_at_ms"]

    revised_message = message("Changed question") |> Map.put("urgency", "high")
    revised_files = [attachment("upload-2", "def")]

    revision_two = %{
      record
      | message: revised_message,
        revision: 2,
        revision_accepted_at: 5_000,
        revisions: Map.put(record.revisions, 2, revised_message),
        files_by_revision: Map.put(record.files_by_revision, 2, revised_files)
    }

    {engine, 2, true} =
      BounceDecision.record_revision(
        first,
        key(),
        record,
        2,
        revised_message,
        revised_files,
        5_000,
        1
      )

    engine = %{
      engine
      | messages: Map.put(engine.messages, key(), revision_two),
        conversation_highs: Map.put(engine.conversation_highs, @person, 1)
    }

    selected_two = score(engine, 0.9, 5_050)
    second_due = 5_000 + 194_400_000

    assert selection(selected_two, 1)["decision"] == "suppressed"
    assert selection(selected_two, 1)["reason"] == "superseded"
    assert selection(selected_two, 2)["due_at_ms"] == second_due
    refute selection(selected_two, 2)["due_at_ms"] == first_due

    replay_message = revised_message
    replay_files = [attachment("upload-replayed", "def")]

    revision_three = %{
      revision_two
      | revision: 3,
        revision_accepted_at: 9_000,
        revisions: Map.put(revision_two.revisions, 3, replay_message),
        files_by_revision: Map.put(revision_two.files_by_revision, 3, replay_files)
    }

    {engine, 2, false} =
      BounceDecision.record_revision(
        selected_two,
        key(),
        revision_two,
        3,
        replay_message,
        replay_files,
        9_000,
        2
      )

    engine = %{
      engine
      | messages: Map.put(engine.messages, key(), revision_three),
        conversation_highs: Map.put(engine.conversation_highs, @person, 2)
    }

    assert engine.bounce_revisions[key()].raw_to_substantive[3] == 2
    assert selection(engine, 2)["due_at_ms"] == second_due
  end

  test "due decisions suppress opened, discussed and terminal messages" do
    {engine, record} = fixture()
    selected = score(engine, 0.8, 1_050)
    due_at = selection(selected)["due_at_ms"]

    {:ok, opened, 1} = BounceDecision.record_open(selected, key(), record, 1)
    opened_at_due = BounceDecision.evaluate_due(opened, due_at)
    assert selection(opened_at_due)["reason"] == "opened"
    assert BounceDecision.advisory_cursor(opened_at_due, @person, @origin, @handle) == 0

    discussed = %{
      selected
      | conversation_events: %{
          1 => %{key: key(), payload: %{"author" => "person", "kind" => "discussion"}}
        }
    }

    discussed = BounceDecision.rebuild_indexes(discussed)
    discussed_at_due = BounceDecision.evaluate_due(discussed, due_at)
    assert selection(discussed_at_due)["reason"] == "person_discussion"

    terminal_record = %{record | terminal: :answered}
    terminal = %{selected | messages: Map.put(selected.messages, key(), terminal_record)}
    terminal_at_due = BounceDecision.evaluate_due(terminal, due_at)
    assert selection(terminal_at_due)["reason"] == "terminal"
  end

  test "the upgrade cutoff is strict and a retained open marker can be reconsidered once" do
    cutoff = 1_794_925_014_000

    for accepted <- [cutoff, cutoff + 1] do
      {engine, record} = fixture(accepted_at: accepted)
      {:ok, opened, 1} = BounceDecision.record_open(engine, key(), record, 1)
      selected = score(opened, 0.9, accepted)
      due = selection(selected)["due_at_ms"]

      # This is the old store's decision when an opened revision crossed its deadline.
      historical =
        put_in(selected.bounce_revisions[key()].revisions[1].selection, %{
          selection(selected)
          | "decision" => "suppressed",
            "reason" => "opened"
        })

      decided =
        historical
        |> BounceDecision.rebuild_indexes()
        |> BounceDecision.resume_decisions()
        |> BounceDecision.evaluate_due(due)

      notices = Map.get(decided.messages[key()], "item_advisories", %{})

      if accepted == cutoff do
        assert notices == %{}
      else
        assert [%{"advisory_type" => "soft_bounce"}] = Map.values(notices)
      end

      assert decided ==
               decided
               |> BounceDecision.resume_decisions()
               |> BounceDecision.evaluate_due(due + 1)
    end
  end

  test "a recorded late Open cannot turn an overdue unopened notice into a soft bounce" do
    accepted = 1_794_925_014_001
    {engine, record} = fixture(accepted_at: accepted)
    {:ok, opened, 1} = BounceDecision.record_open(engine, key(), record, 1)
    opened = BounceDecision.remember_open_time(opened, key(), 1, accepted + 300_000_000)
    decided = score(opened, 0.9, accepted + 300_000_001)

    assert [%{"advisory_type" => "unseen_interval_elapsed"}] =
             Map.values(decided.messages[key()]["item_advisories"])

    assert decided.messages[key()]["full_bounced_at_ms"] == accepted + 300_000_001
  end

  test "both bounce kinds wait for the selected default priority interval" do
    accepted = 1_794_925_014_001

    for {level, value, hours} <- [
          {"Urgent", 0.8, 12},
          {"High", 0.6, 24},
          {"Normal", 0.3, 72},
          {"Low", 0.1, 168}
        ],
        opened? <- [false, true] do
      {engine, record} = fixture(configured?: false, accepted_at: accepted)

      engine =
        if opened? do
          {:ok, opened, 1} = BounceDecision.record_open(engine, key(), record, 1)
          opened
        else
          engine
        end

      selected = score(engine, value, accepted)
      due = accepted + hours * 3_600_000
      assert %{"level_name" => ^level, "due_at_ms" => ^due} = selection(selected)
      assert BounceDecision.evaluate_due(selected, due - 1) == selected

      decided = BounceDecision.evaluate_due(selected, due)
      assert decided.messages[key()].terminal == nil
      assert BounceDecision.evaluate_due(decided, due + 1) == decided

      if opened? do
        assert [%{"advisory_type" => "soft_bounce", "evaluated_at_ms" => ^due}] =
                 Map.values(decided.messages[key()]["item_advisories"])

        refute BounceDecision.fully_bounced?(decided.messages[key()])
      else
        assert decided.messages[key()]["full_bounced_at_ms"] == due

        assert decided.advisory_feeds[advisory_feed_key()].entries[1]["evaluated_at_ms"] == due
      end
    end
  end

  test "a pre-cutoff Open still suppresses a notice waiting on support" do
    {engine, record} = fixture(supported?: false)
    selected = score(engine, 0.9, @accepted_at)
    due = selection(selected)["due_at_ms"]
    waiting = BounceDecision.evaluate_due(selected, due)
    assert selection(waiting)["decision"] == "pending"

    {:ok, opened, 1} = BounceDecision.record_open(waiting, key(), record, 1)
    opened = BounceDecision.remember_open_time(opened, key(), 1, due + 1)
    supported = put_in(opened.origin_sessions[advisory_feed_key()].support["advisory_v1"], true)
    resumed = BounceDecision.requeue_supported_session(supported, advisory_feed_key())
    decided = BounceDecision.evaluate_due(resumed, due + 2)

    assert selection(decided)["reason"] == "opened"
    assert BounceDecision.advisory_cursor(decided, @person, @origin, @handle) == 0
  end

  test "an undeclared session supports v2 advisories and leaves legacy messages pending" do
    {engine, _record} = fixture()
    engine = update_in(engine.origin_sessions[advisory_feed_key()], &Map.delete(&1, :support))
    selected = score(engine, 0.9, 1_050)
    due_at = selection(selected)["due_at_ms"]

    pending = BounceDecision.evaluate_due(selected, due_at)
    assert selection(pending)["decision"] == "pending"
    assert BounceDecision.advisory_cursor(pending, @person, @origin, @handle) == 0

    v2 = put_in(selected.messages[key()][:agent_api_v2], true)
    bounced = BounceDecision.evaluate_due(v2, due_at)
    assert selection(bounced)["decision"] == "bounced"
    assert BounceDecision.advisory_cursor(bounced, @person, @origin, @handle) == 1
  end

  test "advisory append and acknowledgement report every record they write" do
    {engine, _record} = fixture(supported?: false)
    selected = score(engine, 0.9, 1_050)
    due_at = selection(selected)["due_at_ms"]
    pending = BounceDecision.evaluate_due(selected, due_at)
    session = pending.origin_sessions[advisory_feed_key()]
    supported = put_in(session.support["advisory_v1"], true)
    pending = put_in(pending.origin_sessions[advisory_feed_key()], supported)
    pending = BounceDecision.requeue_supported_session(pending, advisory_feed_key())

    {bounced, :ok, keys} =
      Engine.with_touches(pending, fn current ->
        {BounceDecision.evaluate_due(current, due_at + 50), :ok}
      end)

    assert selection(bounced)["decision"] == "bounced"
    assert keys == AttentionServer.TouchOracle.diff(pending, bounced)
    assert MapSet.member?(keys, {:advisory, advisory_feed_key(), :high})
    assert MapSet.member?(keys, {:advisory, advisory_feed_key(), {:entry, 1}})

    [entry] = Map.values(bounced.advisory_feeds[advisory_feed_key()].entries)

    {acked, :ok, keys} =
      Engine.with_touches(bounced, fn current ->
        {:ok, current, _reply} =
          BounceDecision.advisory_ack(current, @origin, @handle, entry["advisory_id"])

        {current, :ok}
      end)

    assert keys == AttentionServer.TouchOracle.diff(bounced, acked)

    assert keys ==
             MapSet.new([{:advisory, advisory_feed_key(), {:receipt, entry["advisory_id"]}}])
  end

  test "unsupported sessions retain due work; the original session's feed and ack stay separate" do
    {engine, _record} = fixture(supported?: false)
    selected = score(engine, 0.9, 1_050)
    due_at = selection(selected)["due_at_ms"]
    pending = BounceDecision.evaluate_due(selected, due_at)

    assert selection(pending)["decision"] == "pending"
    assert BounceDecision.advisory_cursor(pending, @person, @origin, @handle) == 0
    assert BounceDecision.next_deadline(pending) == nil

    session = pending.origin_sessions[advisory_feed_key()]
    supported = put_in(session.support["advisory_v1"], true)
    pending = put_in(pending.origin_sessions[advisory_feed_key()], supported)
    pending = BounceDecision.requeue_supported_session(pending, advisory_feed_key())
    assert BounceDecision.next_deadline(pending) == due_at
    bounced = BounceDecision.evaluate_due(pending, due_at + 50)

    assert selection(bounced)["decision"] == "bounced"
    assert BounceDecision.advisory_cursor(bounced, @person, @origin, @handle) == 1
    assert bounced.route_b_index.advisory_sequences == %{}
    assert bounced.origin_feeds[origin_feed_key()].high == 1
    assert bounced.events == engine.events

    feed = bounced.advisory_feeds[advisory_feed_key()]
    assert feed.high == 1
    assert [entry] = Map.values(feed.entries)
    assert entry["advisory_seq"] == 1
    assert entry["origin_cursor_at_decision"] == 1
    refute MapSet.member?(feed.receipts, entry["advisory_id"])

    assert entry["text"] =~
             "Harklight has no record that you saw this message. This notice concerns revision 1, using records available at #{due_at + 50}."

    assert entry["text"] =~
             "Please decide whether this message is still valid and worth resending"

    assert bounced.advisory_id_index[{advisory_feed_key(), entry["advisory_id"]}] == 1
    rebuilt = BounceDecision.rebuild_indexes(%{bounced | advisory_id_index: %{}})

    assert rebuilt.advisory_id_index[{advisory_feed_key(), entry["advisory_id"]}] == 1

    assert {:error, :not_found} =
             BounceDecision.advisory_ack(
               bounced,
               @origin,
               "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
               entry["advisory_id"]
             )

    assert {:ok, acked, first_ack} =
             BounceDecision.advisory_ack(rebuilt, @origin, @handle, entry["advisory_id"])

    assert first_ack["delivered"] and not first_ack["already_delivered"]

    assert {:ok, same, duplicate_ack} =
             BounceDecision.advisory_ack(acked, @origin, @handle, entry["advisory_id"])

    assert duplicate_ack["delivered"] and duplicate_ack["already_delivered"]
    assert same == acked

    assert MapSet.member?(
             acked.advisory_feeds[advisory_feed_key()].receipts,
             entry["advisory_id"]
           )

    assert BounceDecision.valid_state?(acked)
  end

  test "advisory feed change keys include only the originating sessions of changed messages" do
    {engine, _record} = fixture()
    other_key = {@person, @sender, "other-bounce-message"}

    other_record = %{
      origin: "other-origin",
      session_handle: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    }

    engine = %{engine | messages: Map.put(engine.messages, other_key, other_record)}

    assert BounceDecision.advisory_feed_keys_for_changes(
             engine,
             [key(), key(), {"missing", "s", "m"}]
           ) == [advisory_feed_key()]
  end

  test "deadline and suppression indexes stay narrow across large retained history" do
    {engine, _record} = fixture()
    selected = score(engine, 0.8, 1_050)
    due_at = selection(selected)["due_at_ms"]
    unrelated_count = 5_000

    unrelated =
      Map.new(1..unrelated_count, fn number ->
        id = "retained-#{number}"
        key = {@person, @sender, id}

        revision = %{
          raw_revision: 1,
          accepted_at_ms: 1,
          qualified: false,
          selection: nil
        }

        state = %{
          current_substantive_revision: 1,
          raw_to_substantive: %{1 => 1},
          revisions: %{1 => revision}
        }

        {key, {state, %{message: %{}, origin: @origin, session_handle: @handle, revision: 1}}}
      end)

    {revisions, messages} =
      Enum.reduce(unrelated, {selected.bounce_revisions, selected.messages}, fn
        {key, {state, record}}, {all_revisions, all_messages} ->
          {Map.put(all_revisions, key, state), Map.put(all_messages, key, record)}
      end)

    other_origin_entries =
      Map.new(2..(unrelated_count + 1), fn sequence ->
        {sequence,
         %{
           "kind" => "revision",
           "author" => "origin",
           "session_handle" => @handle,
           "sender_id" => @sender,
           "message_id" => "retained-#{sequence - 1}"
         }}
      end)

    feed = selected.origin_feeds[origin_feed_key()]

    feed = %{
      feed
      | high: unrelated_count + 1,
        entries: Map.merge(feed.entries, other_origin_entries)
    }

    discussion_events =
      Map.new(1..unrelated_count, fn sequence ->
        key = {@person, @sender, "chat-#{sequence}"}

        {sequence,
         %{kind: :conversation, key: key, payload: %{"kind" => "reply", "author" => "origin"}}}
      end)

    large = %{
      selected
      | bounce_revisions: revisions,
        messages: messages,
        origin_feeds: Map.put(selected.origin_feeds, origin_feed_key(), feed),
        conversation_events: discussion_events
    }

    indexed = BounceDecision.rebuild_indexes(large)

    assert :gb_trees.size(indexed.bounce_deadline_index) == 1
    assert MapSet.size(indexed.bounce_origin_entries) == unrelated_count + 1
    assert MapSet.size(indexed.bounce_discussed_messages) == 0
    assert BounceDecision.next_deadline(indexed) == due_at

    due = BounceDecision.evaluate_due(indexed, due_at)
    assert selection(due)["decision"] == "bounced"
    assert BounceDecision.advisory_cursor(due, @person, @origin, @handle) == 1
  end
end
