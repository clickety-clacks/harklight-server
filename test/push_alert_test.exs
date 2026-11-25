defmodule AttentionServer.PushAlertTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{APNS, Engine, JSON}

  test "selects the newest unresolved delivery as the alert trigger across a large gap" do
    person = "person-a"
    device = String.duplicate("a", 64)
    high = 10_000_000
    key = {person, "app:test", "latest"}
    now = 1_000

    record = %{
      message: %{
        "sender_id" => "app:test",
        "message_id" => "latest",
        "created_at" => 1,
        "title" => "Actual title",
        "display_name" => "  Agent\nλ / name  ",
        "body" => "new work"
      },
      origin: device,
      accepted_seq: high - 1,
      terminal: nil
    }

    binding = %{
      revision: 1,
      binding_id: "00000000-0000-4000-8000-000000000001",
      profile_id: "harklight-sandbox",
      address: String.duplicate("ab", 32),
      active: true,
      registered_at: now,
      state: "queued",
      pending_high: high,
      accepted_high: 0,
      pending_conversation_high: 0,
      accepted_conversation_high: 0,
      deadline_at: now + 60_000,
      next_at: now,
      attempt_count: 0,
      last_attempt_at: nil,
      last_outcome: nil,
      claim_id: nil,
      claim_high: nil,
      claim_conversation_high: nil,
      claim_at: nil
    }

    engine = %{
      Engine.new()
      | persons: %{person => high},
        events: %{
          {person, high} => %{kind: :lifecycle, key: key},
          {person, high - 1} => %{kind: :delivery, key: key}
        },
        messages: %{key => record},
        push_bindings: %{{person, device} => binding}
    }

    assert {:ok, _claimed, claim} = Engine.claim_push(engine, now)
    assert claim.presentation == :alert
    assert claim.high == high
    assert claim.alert["title"] == "  Agent\nλ / name  "
    assert claim.preserve_alert_title
    assert claim.alert["body"] == "new work"
    assert claim.locator["item_id"] == "i#{high - 1}"
    assert claim.badge == 1
  end

  test "uses sender id and falls back to the actual title for an empty body" do
    person = "person-a"
    device = String.duplicate("a", 64)
    message_id = "fallback"
    key = {person, "app:test", message_id}
    now = 1_000
    body = String.duplicate("x", 158) <> "👩‍👩‍👧‍👦" <> "yz"

    record = %{
      message: %{
        "sender_id" => "app:test",
        "message_id" => message_id,
        "created_at" => 1,
        "title" => " Actual \n title ",
        "body" => " \t\n "
      },
      origin: device,
      accepted_seq: 1,
      terminal: nil
    }

    claim_for = fn message ->
      event_record = %{record | message: message}

      binding = %{
        revision: 1,
        binding_id: "00000000-0000-4000-8000-000000000001",
        profile_id: "harklight-sandbox",
        address: String.duplicate("ab", 32),
        active: true,
        registered_at: now,
        state: "queued",
        pending_high: 1,
        accepted_high: 0,
        pending_conversation_high: 0,
        accepted_conversation_high: 0,
        deadline_at: now + 60_000,
        next_at: now,
        attempt_count: 0,
        last_attempt_at: nil,
        last_outcome: nil,
        claim_id: nil,
        claim_high: nil,
        claim_conversation_high: nil,
        claim_at: nil
      }

      engine = %{
        Engine.new()
        | persons: %{person => 1},
          events: %{{person, 1} => %{kind: :delivery, key: key}},
          messages: %{key => event_record},
          push_bindings: %{{person, device} => binding}
      }

      {:ok, _claimed, claim} = Engine.claim_push(engine, now)
      claim
    end

    fallback = claim_for.(record.message)
    assert fallback.alert["title"] == "app:test"
    assert fallback.alert["body"] == "Actual title"
    refute fallback.alert["body_truncated"]

    bounded = claim_for.(%{record.message | "body" => body})
    assert bounded.alert["body"] == String.duplicate("x", 158) <> "👩‍👩‍👧‍👦…"
    assert length(String.graphemes(bounded.alert["body"])) == 160
    assert bounded.alert["body_truncated"]
  end

  @person "person-a"
  @device String.duplicate("a", 64)
  @origin String.duplicate("b", 64)

  defp push_binding(overrides) do
    Map.merge(
      %{
        revision: 1,
        binding_id: "00000000-0000-4000-8000-000000000001",
        profile_id: "harklight-sandbox",
        address: String.duplicate("ab", 32),
        active: true,
        registered_at: 1_000,
        state: "queued",
        pending_high: 0,
        accepted_high: 0,
        pending_conversation_high: 0,
        accepted_conversation_high: 0,
        deadline_at: 1_000,
        next_at: 1_000,
        attempt_count: 0,
        last_attempt_at: nil,
        last_outcome: nil,
        claim_id: nil,
        claim_high: nil,
        claim_conversation_high: nil,
        claim_at: nil
      },
      overrides
    )
  end

  # Builds an engine whose person has one delivery event per entry, in order.
  # Each entry is {sender, message_id, display_name | nil, terminal | nil}.
  defp engine_with_deliveries(entries, binding_overrides) do
    {events, messages} =
      entries
      |> Enum.with_index(1)
      |> Enum.reduce({%{}, %{}}, fn {{sender, id, name, terminal}, seq}, {events, messages} ->
        key = {@person, sender, id}

        record = %{
          message: %{
            "sender_id" => sender,
            "message_id" => id,
            "created_at" => seq,
            "title" => "Request #{seq}"
          },
          origin: @origin,
          accepted_seq: seq,
          terminal: terminal
        }

        record =
          if name,
            do:
              Map.put(record, "sender_display", %{
                "v" => 1,
                "sender_id" => sender,
                "origin" => @origin,
                "name" => name
              }),
            else: record

        {Map.put(events, {@person, seq}, %{kind: :delivery, key: key}),
         Map.put(messages, key, record)}
      end)

    high = length(entries)

    %{
      Engine.new()
      | persons: %{@person => high},
        events: events,
        messages: messages,
        push_bindings: %{
          {@person, @device} => push_binding(Map.merge(%{pending_high: high}, binding_overrides))
        }
    }
  end

  test "only low and normal FYIs are passive with the default alert choice" do
    for type <- ["fyi", "request", nil], urgency <- ["low", "normal", "high", "now", nil] do
      message =
        %{"message_type" => type, "urgency" => urgency}
        |> Map.reject(fn {_field, value} -> is_nil(value) end)

      engine = deliveries_with_messages([message])
      assert {:ok, _, claim} = Engine.claim_push(engine, 1_000)
      payload = push_payload(claim)

      if type == "fyi" and urgency in ["low", "normal", nil] do
        assert claim.quiet
        assert payload["aps"]["interruption-level"] == "passive"
        refute Map.has_key?(payload["aps"], "sound")
      else
        refute Map.has_key?(payload["aps"], "interruption-level")
        assert payload["aps"]["sound"] == "default"
      end

      assert payload["aps"]["badge"] == 1
      assert payload["aps"]["alert"] == Map.take(claim.alert, ["title", "body"])
    end
  end

  test "grouped FYIs are passive unless one alerts, or the person wants all alerts" do
    fyi = %{"message_type" => "fyi", "urgency" => "normal"}
    request = %{"message_type" => "request", "urgency" => "low"}
    engine = deliveries_with_messages([fyi, fyi])
    assert {:ok, _, quiet} = Engine.claim_push(engine, 1_000)

    engine = Map.put(engine, :alert_choices, %{@person => "all"})
    assert {:ok, _, all} = Engine.claim_push(engine, 1_000)

    passive = push_payload(quiet)
    active = push_payload(all)
    assert active["aps"]["sound"] == "default"
    refute Map.has_key?(active["aps"], "interruption-level")

    assert passive["aps"] ==
             Map.put(Map.delete(active["aps"], "sound"), "interruption-level", "passive")

    assert Map.delete(passive, "aps") == Map.delete(active, "aps")

    assert {:ok, _, mixed} =
             [request, fyi] |> deliveries_with_messages() |> Engine.claim_push(1_000)

    refute Map.has_key?(push_payload(mixed)["aps"], "interruption-level")
  end

  defp deliveries_with_messages(messages) do
    entries = Enum.map(1..length(messages), &{"app:test", "item-#{&1}", nil, nil})
    engine = engine_with_deliveries(entries, %{})

    records =
      Map.new(engine.messages, fn {key, record} ->
        message = Enum.at(messages, record.accepted_seq - 1)
        {key, %{record | message: Map.merge(record.message, message)}}
      end)

    %{engine | messages: records}
  end

  defp push_payload(claim), do: APNS.request_payload(put_in(claim.locator["rev"], 1))

  test "several new requests group into one push with a count, sender names and the newest item" do
    engine =
      engine_with_deliveries(
        [
          {"app:alpha", "one", "Alice", nil},
          {"app:beta", "two", nil, nil},
          {"app:alpha", "three", "Alice", nil}
        ],
        %{}
      )

    assert {:ok, _claimed, claim} = Engine.claim_push(engine, 1_000)
    assert claim.presentation == :alert
    assert claim.high == 3
    assert claim.alert == %{"title" => "3 new requests", "body" => "Alice, app:beta"}
    assert claim.locator["item_id"] == "i3"
    assert claim.badge == 3
  end

  test "grouped pushes carry exact agent display names separately from sender IDs" do
    engine =
      engine_with_deliveries(
        [
          {"app:alpha", "one", nil, nil},
          {"app:beta", "two", nil, nil}
        ],
        %{}
      )

    messages =
      Map.new(engine.messages, fn {key, record} ->
        display_name = if elem(key, 1) == "app:alpha", do: "  Alpha\nAgent  ", else: "βeta"
        {key, %{record | message: Map.put(record.message, "display_name", display_name)}}
      end)

    assert {:ok, _claimed, claim} = Engine.claim_push(%{engine | messages: messages}, 1_000)
    assert claim.alert == %{"title" => "2 new requests", "body" => "  Alpha\nAgent  , βeta"}
  end

  test "the grouped count covers only requests gathered since the last push" do
    engine =
      engine_with_deliveries(
        [
          {"app:test", "one", nil, nil},
          {"app:test", "two", nil, nil},
          {"app:test", "three", nil, nil},
          {"app:test", "four", nil, nil}
        ],
        %{accepted_high: 2}
      )

    assert {:ok, _claimed, claim} = Engine.claim_push(engine, 1_000)
    assert claim.alert["title"] == "2 new requests"
    assert claim.badge == 4
  end

  test "grouped push retains every full exact display name above 160 graphemes" do
    first_name = String.duplicate("名", 90) <> "A"
    second_name = String.duplicate("界", 90) <> "B"

    engine =
      engine_with_deliveries([{"app:alpha", "one", nil, nil}, {"app:beta", "two", nil, nil}], %{})

    messages =
      Map.new(engine.messages, fn {key, record} ->
        display_name = if elem(key, 1) == "app:alpha", do: first_name, else: second_name
        {key, %{record | message: Map.put(record.message, "display_name", display_name)}}
      end)

    assert {:ok, _claimed, claim} = Engine.claim_push(%{engine | messages: messages}, 1_000)

    assert claim.alert == %{
             "title" => "2 new requests",
             "body" => first_name <> ", " <> second_name
           }

    assert claim.grouped_alert_names == [first_name, second_name]
    assert claim.preserve_alert_body
  end

  test "the badge is the server-side Inbox count: answered and retracted items are resolved" do
    answered = %{state: "answered", seq: 4, at: 5}
    retracted = %{state: "retracted", seq: 5, at: 6}

    engine =
      engine_with_deliveries(
        [
          {"app:test", "open-but-synced", nil, nil},
          {"app:test", "answered", nil, answered},
          {"app:test", "retracted", nil, retracted}
        ],
        %{accepted_high: 1}
      )

    assert {:ok, _claimed, claim} = Engine.claim_push(engine, 1_000)
    assert claim.presentation == :background
    assert claim.badge == 1

    empty =
      engine_with_deliveries([{"app:test", "answered", nil, answered}], %{})

    assert {:ok, _claimed, empty_claim} = Engine.claim_push(empty, 1_000)
    assert empty_claim.presentation == :background
    assert empty_claim.badge == 0
  end

  test "a retry waits only for its backoff, not a fixed spacing" do
    engine = engine_with_deliveries([{"app:test", "one", nil, nil}], %{})
    assert {:ok, claimed, claim} = Engine.claim_push(engine, 1_000)

    assert {:ok, retried} =
             Engine.finish_push(claimed, claim, %{state: "retry", retry_after_ms: 0}, 1_001)

    binding = retried.push_bindings[{@person, @device}]
    assert binding.state == "waiting_retry"
    assert binding.next_at == 1_001 + 1_000
  end

  test "pending push remains claimable after the former hint deadline" do
    now = 3_600_001

    engine =
      engine_with_deliveries(
        [{"app:test", "past-deadline", nil, nil}],
        %{deadline_at: now - 1, next_at: now - 1}
      )

    assert {:ok, claimed, claim} = Engine.claim_push(engine, now)
    assert claim.high == 1
    assert claimed.push_bindings[{@person, @device}].state == "sending"
  end

  test "same-endpoint re-registration immediately queues blocked pending work" do
    engine = engine_with_deliveries([{"app:test", "blocked", nil, nil}], %{next_at: 1_000})
    {:ok, engine} = Engine.setup_device(engine, @person, @device)

    assert {:ok, sending, claim} = Engine.claim_push(engine, 1_000)
    assert sending.push_bindings[{@person, @device}].state == "sending"

    assert {:ok, blocked} =
             Engine.finish_push(
               sending,
               claim,
               %{state: "blocked_configuration"},
               1_001
             )

    blocked_binding = blocked.push_bindings[{@person, @device}]
    assert blocked_binding.state == "blocked_configuration"
    assert blocked_binding.pending_high == 1
    assert blocked_binding.next_at == nil

    register_again = %{
      "v" => 1,
      "type" => "push_register",
      "op_id" => "00000000-0000-4000-8000-000000000103",
      "revision" => 2,
      "profile_id" => "harklight-sandbox",
      "address_kind" => "apns_token",
      "address" => String.duplicate("ab", 32)
    }

    {registered, {:ok, _reply}} =
      Engine.request_line(blocked, @device, JSON.encode!(register_again) <> "\n")

    registered_binding = registered.push_bindings[{@person, @device}]
    assert registered_binding.state == "queued"
    assert registered_binding.pending_high == 1
    assert registered_binding.next_at == registered_binding.registered_at
  end

  defp registered_push_engine do
    {:ok, engine} = Engine.setup_person(Engine.new(), @person)
    {:ok, engine} = Engine.setup_device(engine, @person, @device)
    {:ok, engine} = Engine.setup_device(engine, @person, @origin)
    {:ok, engine} = Engine.setup_sender(engine, @origin, "app:test")

    register = %{
      "v" => 1,
      "type" => "push_register",
      "op_id" => "00000000-0000-4000-8000-000000000101",
      "revision" => 1,
      "profile_id" => "harklight-sandbox",
      "address_kind" => "apns_token",
      "address" => String.duplicate("ab", 32)
    }

    {engine, {:ok, _reply}} = Engine.request_line(engine, @device, JSON.encode!(register) <> "\n")

    forward = fn engine, id, op, fields, at ->
      frame = %{
        "v" => 1,
        "type" => "forward",
        "op_id" => "00000000-0000-4000-8000-0000000002" <> op,
        "message" =>
          Map.merge(
            %{
              "sender_id" => "app:test",
              "message_id" => id,
              "created_at" => 1,
              "title" => "Request #{id}"
            },
            fields
          )
      }

      {engine, {:ok, reply}} =
        Engine.request_line(engine, @origin, JSON.encode!(frame) <> "\n", at)

      {:ok, %{"seq" => seq}} = JSON.decode_line(reply)
      {engine, seq}
    end

    {engine, forward}
  end

  test "an alerting delivery skips spacing behind a quiet delivery" do
    for first_push <- [:sent, :in_flight, :queued] do
      {engine, forward} = registered_push_engine()
      now = System.system_time(:millisecond)

      fyi = %{
        "message_type" => "fyi",
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
        }
      }

      {engine, 1} = forward.(engine, "quiet", "01", fyi, now)
      accepted = %{state: "provider_accepted", http_status: 200}

      {engine, first} =
        case first_push do
          :queued ->
            {engine, nil}

          _ ->
            assert {:ok, engine, first} = Engine.claim_push(engine, now)
            assert first.quiet

            if first_push == :sent do
              assert {:ok, engine} = Engine.finish_push(engine, first, accepted, now)
              {engine, first}
            else
              {engine, first}
            end
        end

      request_at = now + 60_000
      {engine, 2} = forward.(engine, "question", "02", %{}, request_at)

      engine =
        if first_push == :in_flight do
          assert {:ok, engine} = Engine.finish_push(engine, first, accepted, request_at)
          engine
        else
          engine
        end

      assert {:ok, _, claim} = Engine.claim_push(engine, request_at)
      assert claim.high == 2
      refute Map.get(claim, :quiet)
    end
  end

  test "the first request is pushed at once and follow-ups within five minutes go out as one push" do
    {engine, forward} = registered_push_engine()

    binding = fn engine -> engine.push_bindings[{@person, @device}] end

    # The first unsent request is eligible immediately.
    before = System.system_time(:millisecond)
    {engine, 1} = forward.(engine, "one", "01", %{}, System.system_time(:millisecond))
    after_first = System.system_time(:millisecond)

    opened = binding.(engine)
    assert opened.state == "queued"
    assert opened.next_at in before..after_first

    claimed_at = System.system_time(:millisecond)
    assert {:ok, engine, first} = Engine.claim_push(engine, claimed_at)
    assert first.high == 1
    assert first.alert["body"] == "Request one"
    assert binding.(engine).state == "sending"
    assert binding.(engine).next_at == nil

    # Requests arriving while it is in flight wait for the end of the window
    # that started with that push and join one push.
    {engine, 2} = forward.(engine, "two", "02", %{}, System.system_time(:millisecond))
    window_end = claimed_at + 300_000
    in_flight = binding.(engine)
    assert in_flight.state == "sending"
    assert in_flight.next_at == window_end

    {engine, 3} = forward.(engine, "three", "03", %{}, System.system_time(:millisecond))
    assert binding.(engine).next_at == window_end
    assert binding.(engine).pending_high == 3

    accepted = %{state: "provider_accepted", http_status: 200}
    assert {:ok, engine} = Engine.finish_push(engine, first, accepted, claimed_at + 1)
    queued = binding.(engine)
    assert queued.state == "queued"
    assert queued.next_at == window_end
    assert queued.accepted_high == 1

    assert {:ok, _engine, nil} = Engine.claim_push(engine, window_end - 1)
    assert {:ok, engine, second} = Engine.claim_push(engine, window_end)
    assert second.high == 3
    assert second.alert == %{"title" => "2 new requests", "body" => "app:test"}
    assert second.badge == 3

    assert {:ok, engine} = Engine.finish_push(engine, second, accepted, window_end + 1)
    assert binding.(engine).state == "ready"
    assert binding.(engine).next_at == nil
  end

  describe "push address takeover" do
    @other_person "00000000-0000-4000-8000-000000000009"
    @other_device "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb1"

    defp register_frame(op, revision, address) do
      %{
        "v" => 1,
        "type" => "push_register",
        "op_id" => "00000000-0000-4000-8000-0000000003" <> op,
        "revision" => revision,
        "profile_id" => "harklight-sandbox",
        "address_kind" => "apns_token",
        "address" => address
      }
    end

    test "the newest registration of an address takes it from another device of the same person" do
      {:ok, engine} = Engine.setup_person(Engine.new(), @person)
      {:ok, engine} = Engine.setup_device(engine, @person, @device)
      {:ok, engine} = Engine.setup_device(engine, @person, @origin)
      address = String.duplicate("ab", 32)

      {engine, {:ok, _}} =
        Engine.request_line(engine, @device, JSON.encode!(register_frame("01", 1, address)) <> "\n")

      {engine, {:ok, reply}} =
        Engine.request_line(engine, @origin, JSON.encode!(register_frame("02", 1, address)) <> "\n")

      assert {:ok, %{"revision" => 1}} = JSON.decode_line(reply)
      refute engine.push_bindings[{@person, @device}].active
      assert engine.push_bindings[{@person, @device}].address == nil
      assert engine.push_bindings[{@person, @origin}].active
      assert engine.push_bindings[{@person, @origin}].address == address

      # The device that lost the address can register again with a newer revision.
      {engine, {:ok, _}} =
        Engine.request_line(engine, @device, JSON.encode!(register_frame("03", 2, address)) <> "\n")

      assert engine.push_bindings[{@person, @device}].active
      refute engine.push_bindings[{@person, @origin}].active
    end

    test "the newest registration takes an address from a device of another person" do
      {:ok, engine} = Engine.setup_person(Engine.new(), @person)
      {:ok, engine} = Engine.setup_person(engine, @other_person)
      {:ok, engine} = Engine.setup_device(engine, @person, @device)
      {:ok, engine} = Engine.setup_device(engine, @other_person, @other_device)
      address = String.duplicate("cd", 32)

      {engine, {:ok, _}} =
        Engine.request_line(engine, @device, JSON.encode!(register_frame("11", 1, address)) <> "\n")

      {engine, {:ok, _}} =
        Engine.request_line(
          engine,
          @other_device,
          JSON.encode!(register_frame("12", 1, address)) <> "\n"
        )

      refute engine.push_bindings[{@person, @device}].active
      assert engine.push_bindings[{@other_person, @other_device}].active
    end
  end
end
