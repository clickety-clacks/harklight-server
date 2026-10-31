defmodule AttentionServer.PushAlertTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Engine, JSON}

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
    assert claim.locator["message_id"] == "latest"
    assert claim.locator["acceptance_seq"] == high - 1
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

  @window_ms 300_000
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
        deadline_at: 1_000 + 3_600_000,
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

  test "several new requests group into one push with a count, sender names and no locator" do
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
    refute Map.has_key?(claim, :locator)
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

  test "the first unsent request opens a five-minute window that later requests ride on" do
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

    forward = fn engine, id, op ->
      frame = %{
        "v" => 1,
        "type" => "forward",
        "op_id" => "00000000-0000-4000-8000-0000000002" <> op,
        "message" => %{
          "sender_id" => "app:test",
          "message_id" => id,
          "created_at" => 1,
          "title" => "Request #{id}"
        }
      }

      {engine, {:ok, reply}} = Engine.request_line(engine, @origin, JSON.encode!(frame) <> "\n")
      {:ok, %{"seq" => seq}} = JSON.decode_line(reply)
      {engine, seq}
    end

    binding = fn engine -> engine.push_bindings[{@person, @device}] end

    before = System.system_time(:millisecond)
    {engine, 1} = forward.(engine, "one", "01")
    after_first = System.system_time(:millisecond)

    opened = binding.(engine)
    assert opened.state == "queued"
    assert opened.next_at in (before + @window_ms)..(after_first + @window_ms)
    assert opened.deadline_at == opened.next_at - @window_ms + 3_600_000

    # Nothing goes out before the window closes.
    assert {:ok, _engine, nil} = Engine.claim_push(engine, opened.next_at - 1)

    # A second request inside the window does not move the send time.
    {engine, 2} = forward.(engine, "two", "02")
    assert binding.(engine).next_at == opened.next_at
    assert binding.(engine).pending_high == 2

    assert {:ok, engine, claim} = Engine.claim_push(engine, opened.next_at)
    assert claim.high == 2
    assert claim.alert == %{"title" => "2 new requests", "body" => "app:test"}
    assert claim.badge == 2
    assert binding.(engine).state == "sending"
    assert binding.(engine).next_at == nil

    # A request that arrives while that push is in flight opens the next window.
    before_third = System.system_time(:millisecond)
    {engine, 3} = forward.(engine, "three", "03")
    after_third = System.system_time(:millisecond)
    in_flight = binding.(engine)
    assert in_flight.state == "sending"
    assert in_flight.next_at in (before_third + @window_ms)..(after_third + @window_ms)

    accepted = %{state: "provider_accepted", http_status: 200}
    assert {:ok, engine} = Engine.finish_push(engine, claim, accepted, opened.next_at + 1)
    queued = binding.(engine)
    assert queued.state == "queued"
    assert queued.next_at == in_flight.next_at
    assert queued.accepted_high == 2

    assert {:ok, _engine, nil} = Engine.claim_push(engine, queued.next_at - 1)
    assert {:ok, engine, second} = Engine.claim_push(engine, queued.next_at)
    assert second.high == 3
    assert second.alert["title"] == "app:test"
    assert second.alert["body"] == "Request three"
    assert second.locator["message_id"] == "three"
    assert second.badge == 3

    assert {:ok, engine} = Engine.finish_push(engine, second, accepted, queued.next_at + 1)
    assert binding.(engine).state == "ready"
    assert binding.(engine).next_at == nil
  end
end
