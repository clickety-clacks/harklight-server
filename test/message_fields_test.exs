defmodule AttentionServer.MessageFieldsTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Engine, Message, Placement}

  @answer_ui %{
    "schema" => "attention-client.actions/1",
    "actions" => [
      %{"id" => "choice", "label" => "Later", "kind" => "value", "value" => %{"n" => 2}},
      %{
        "id" => "choice-again",
        "label" => "Later too",
        "kind" => "value",
        "value" => %{"n" => 2}
      },
      %{"id" => "reply", "label" => "Reply", "kind" => "text"}
    ]
  }

  defp message do
    %{
      "sender_id" => "app:test",
      "message_id" => "message-1",
      "created_at" => 1,
      "title" => "A request",
      "answer_ui" => @answer_ui
    }
  end

  test "invalid optional fields do not reject or rewrite a message and warnings follow contract order" do
    raw =
      message()
      |> Map.merge(%{
        "urgency" => "urgent",
        "default" => %{"value" => %{"n" => 2}, "at" => 1},
        "by_when" => "tomorrow",
        "thread" => "",
        "choice_styles" => %{"choice" => 7},
        "stake" => 7,
        "future_member" => %{"kept" => true}
      })

    assert :ok = Message.validate(raw)
    assert {^raw, warnings} = Message.normalize_optional_fields(raw, 1, false)

    assert warnings == [
             %{"field" => "urgency", "reason" => "invalid"},
             %{"field" => "default", "reason" => "past"},
             %{"field" => "by_when", "reason" => "invalid"},
             %{"field" => "thread", "reason" => "invalid"},
             %{"field" => "choice_styles", "reason" => "invalid"},
             %{"field" => "stake", "reason" => "invalid"}
           ]

    assert Message.effective_urgency(raw) == "normal"
    assert Message.effective_by_when(raw) == nil
    assert Message.effective_stake(raw) == ""
    assert Message.valid_thread(raw) == nil
    assert raw["future_member"] == %{"kept" => true}
  end

  test "default-on style hold removes only choice_styles and warns held" do
    raw =
      message()
      |> Map.merge(%{
        "urgency" => "high",
        "by_when" => 123,
        "thread" => "release",
        "stake" => "A one-week delay",
        "choice_styles" => %{"choice" => "destructive", "reply" => "primary"},
        "future_member" => true
      })

    assert {normalized, [%{"field" => "choice_styles", "reason" => "held"}]} =
             Message.normalize_optional_fields(raw, 1, true)

    assert normalized == Map.delete(raw, "choice_styles")
    assert normalized["urgency"] == "high"
    assert normalized["by_when"] == 123
    assert normalized["thread"] == "release"
    assert normalized["stake"] == "A one-week delay"
    assert normalized["future_member"] == true
  end

  test "null optional members mean absent except that the style hold warns and removes styles" do
    raw =
      message()
      |> Map.merge(%{
        "urgency" => nil,
        "default" => %{"value" => %{"n" => 2}, "at" => 200},
        "by_when" => nil,
        "thread" => nil,
        "choice_styles" => nil,
        "stake" => nil
      })

    assert {normalized, [%{"field" => "choice_styles", "reason" => "held"}]} =
             Message.normalize_optional_fields(raw, 100, true)

    assert normalized == Map.delete(raw, "choice_styles")
    assert Message.effective_urgency(raw) == "normal"
    assert Message.effective_by_when(raw) == nil
    assert Message.valid_thread(raw) == nil
    assert Message.effective_stake(raw) == ""

    assert Message.default_answer(raw, 100) == %{
             value: %{"n" => 2},
             at: 200,
             text: "Later"
           }

    assert Message.default_answer(Map.put(raw, "answer_ui", %{"actions" => []}), 100) == nil

    assert {^raw, []} = Message.normalize_optional_fields(raw, 100, false)
  end

  test "default sweep index contains only valid open message defaults" do
    defaulted =
      message()
      |> Map.put("default", %{"value" => %{"n" => 2}, "at" => 200})

    active = %{message: defaulted, terminal: nil, accepted_at: 100}
    terminal = %{active | terminal: %{state: "answered"}}

    invalid = %{
      active
      | message: Map.put(defaulted, "default", %{"value" => %{"n" => 2}, "at" => 100})
    }

    no_default = %{active | message: message()}

    engine =
      %{
        Engine.new()
        | messages: %{
            {"person", "app:test", "active"} => active,
            {"person", "app:test", "terminal"} => terminal,
            {"person", "app:test", "invalid"} => invalid,
            {"person", "app:test", "none"} => no_default
          }
      }
      |> Engine.rebuild_default_index()

    assert Engine.default_index(engine) == %{{"person", "app:test", "active"} => 200}
  end

  test "the style hold can be disabled and invalid style entries produce one field warning" do
    raw =
      message()
      |> Map.put("choice_styles", %{"choice" => "destructive"})

    assert {^raw, []} = Message.normalize_optional_fields(raw, 1, false)

    partly_invalid =
      Map.put(raw, "choice_styles", %{"choice" => "destructive", "unknown" => "primary"})

    assert {^partly_invalid, [%{"field" => "choice_styles", "reason" => "invalid"}]} =
             Message.normalize_optional_fields(partly_invalid, 1, false)
  end

  test "server grouping moves later sender-thread members after each group's first member" do
    entries = [
      %{"sender_id" => "app:a", "message_id" => "a1", "thread" => "x", "priority" => "now"},
      %{"sender_id" => "app:b", "message_id" => "b1", "thread" => "x", "priority" => "high"},
      %{"sender_id" => "app:a", "message_id" => "plain", "priority" => "normal"},
      %{"sender_id" => "app:a", "message_id" => "a2", "thread" => "x", "priority" => "low"},
      %{"sender_id" => "app:b", "message_id" => "b2", "thread" => "x", "priority" => "normal"},
      %{"sender_id" => "app:a", "message_id" => "a3", "thread" => "x", "priority" => "normal"}
    ]

    assert Enum.map(Placement.grouped(entries), & &1["message_id"]) == [
             "a1",
             "a2",
             "a3",
             "b1",
             "b2",
             "plain"
           ]

    grouped = Placement.grouped(entries)

    assert Enum.map(grouped, & &1["priority"]) == [
             "now",
             "low",
             "normal",
             "high",
             "normal",
             "normal"
           ]
  end

  test "cross-id supersede chains inherit the original sequence and thread" do
    person = "person-a"
    sender = "app:test"
    source_key = {person, sender, "source"}
    first_key = {person, sender, "replacement-1"}
    second_key = {person, sender, "replacement-2"}
    other_key = {person, sender, "other"}

    source_revision = %{"thread" => "original-thread"}
    source_current = %{"thread" => "revised-thread", "urgency" => "normal"}

    source = %{
      accepted_seq: 2,
      revision: 2,
      revisions: %{1 => source_revision, 2 => source_current},
      message: source_current,
      terminal: %{state: "superseded"}
    }

    first_message = %{"thread" => "wrong-thread", "urgency" => "normal"}

    first = %{
      accepted_seq: 3,
      revision: 1,
      revisions: %{1 => first_message},
      message: first_message,
      terminal: %{state: "superseded"},
      placement_seq: 2,
      group_thread: "original-thread",
      placement_origin_target: source_key
    }

    second_message = %{"thread" => "also-wrong", "urgency" => "normal"}

    second = %{
      accepted_seq: 4,
      revision: 1,
      revisions: %{1 => second_message},
      message: second_message,
      terminal: nil,
      placement_seq: 3,
      group_thread: "original-thread",
      placement_origin_target: first_key
    }

    other_message = %{"urgency" => "normal"}

    other = %{
      accepted_seq: 1,
      revision: 1,
      revisions: %{1 => other_message},
      message: other_message,
      terminal: nil
    }

    engine =
      Engine.new()
      |> Map.put(:messages, %{
        source_key => source,
        first_key => first,
        second_key => second,
        other_key => other
      })

    assert [replacement, other_entry] = Placement.baseline(engine, person)
    assert replacement["message_id"] == "replacement-2"
    assert replacement["thread"] == "original-thread"
    assert replacement["priority"] == "normal"
    assert other_entry["message_id"] == "other"
    refute Map.has_key?(other_entry, "thread")
  end
end
