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
      accepted_at: @accepted_at,
      revision_accepted_at: @accepted_at,
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
    engine.bounce_revisions[key()].revisions[revision].selection
  end

  test "unconfigured and pre-configuration messages remain unselected" do
    {engine, _record} = fixture(configured?: false)
    refute Map.has_key?(engine.bounce_levels, @person)
    assert [] == BounceDecision.levels(engine, @person)

    {:ok, levels} = BouncePolicy.normalize_levels(@levels)
    {:ok, configured, _} = BounceDecision.configure_levels(engine, @person, levels, 2_000)

    assert selection(score(configured, 0.9, 10_000)) == nil
  end

  test "first applied score fixes an inclusive level and acceptance-based deadline" do
    {engine, _record} = fixture()
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

    assert selection(rescored) == selection(selected)
    assert BounceDecision.next_deadline(rescored) == @accepted_at + 194_400_000
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

    replayed = score(engine, 0.4, 10_000)
    assert replayed.bounce_revisions[key()].raw_to_substantive[3] == 2
    assert selection(replayed, 2)["due_at_ms"] == second_due
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

    discussed_at_due = BounceDecision.evaluate_due(discussed, due_at)
    assert selection(discussed_at_due)["reason"] == "person_discussion"

    terminal_record = %{record | terminal: :answered}
    terminal = %{selected | messages: Map.put(selected.messages, key(), terminal_record)}
    terminal_at_due = BounceDecision.evaluate_due(terminal, due_at)
    assert selection(terminal_at_due)["reason"] == "terminal"
  end

  test "unsupported sessions retain due work; original-session fetch and ack stay separate" do
    {engine, _record} = fixture(supported?: false)
    selected = score(engine, 0.9, 1_050)
    due_at = selection(selected)["due_at_ms"]
    pending = BounceDecision.evaluate_due(selected, due_at)

    assert selection(pending)["decision"] == "pending"
    assert BounceDecision.advisory_cursor(pending, @person, @origin, @handle) == 0

    session = pending.origin_sessions[advisory_feed_key()]
    supported = put_in(session.support["advisory_v1"], true)
    pending = put_in(pending.origin_sessions[advisory_feed_key()], supported)
    bounced = BounceDecision.evaluate_due(pending, due_at + 50)

    assert selection(bounced)["decision"] == "bounced"
    assert BounceDecision.advisory_cursor(bounced, @person, @origin, @handle) == 1
    assert bounced.origin_feeds[origin_feed_key()].high == 1
    assert bounced.events == engine.events

    assert {:ok, fetched} = BounceDecision.advisory_feed(bounced, @origin, @handle, 0)
    assert fetched["advisory_v1"]
    assert fetched["cursor"] == 1
    assert [entry] = fetched["entries"]
    assert entry["advisory_seq"] == 1
    assert entry["origin_cursor_at_decision"] == 1
    assert entry["delivered"] == false

    assert entry["text"] =~
             "Harklight has no record that you saw this message. This notice concerns revision 1, using records available at #{due_at + 50}."

    assert entry["text"] =~
             "Please decide whether this message is still valid and worth resending"

    assert {:error, :invalid_cursor} = BounceDecision.advisory_feed(bounced, @origin, @handle, 2)

    assert {:error, :not_found} =
             BounceDecision.advisory_ack(
               bounced,
               @origin,
               "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
               entry["advisory_id"]
             )

    assert {:ok, acked, first_ack} =
             BounceDecision.advisory_ack(bounced, @origin, @handle, entry["advisory_id"])

    assert first_ack["delivered"] and not first_ack["already_delivered"]

    assert {:ok, same, duplicate_ack} =
             BounceDecision.advisory_ack(acked, @origin, @handle, entry["advisory_id"])

    assert duplicate_ack["delivered"] and duplicate_ack["already_delivered"]
    assert same == acked

    assert {:ok, fetched_after_ack} = BounceDecision.advisory_feed(acked, @origin, @handle, 0)
    assert hd(fetched_after_ack["entries"])["delivered"]
    assert BounceDecision.valid_state?(acked)
  end
end
