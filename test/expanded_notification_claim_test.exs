defmodule AttentionServer.ExpandedNotificationClaimTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{APNS, Engine, LargeData}

  @person "synthetic-person"
  @device "synthetic-device"

  test "actual gathered requests preserve title order and duplicates independently of badge and senders" do
    engine = engine(["earlier", "Repeated", "Repeated", "resolved", "雪"], 1)
    resolved = {@person, "app:synthetic", "4"}
    engine = put_in(engine.messages[resolved].terminal, %{"type" => "answered"})
    {:ok, _next, claim} = claim_push(engine)

    assert claim.expanded == %{titles: ["Repeated", "Repeated", "雪"], total_count: 3}
    assert claim.badge == 4
    assert claim.grouped_alert_names == ["Synthetic sender"]

    payload = APNS.request_payload(claim)

    assert payload["attention"]["expanded"] == %{
             "v" => 1,
             "titles" => ["Repeated", "Repeated", "雪"],
             "total_count" => 3
           }

    assert strip_expansion(payload) == APNS.request_payload(Map.delete(claim, :expanded))
    assert_sync(payload, engine, "i5")
  end

  test "claim projection retains refs without reading their content inside Engine" do
    ref = %LargeData.Ref{id: "synthetic", sha256: "synthetic", size: 3_000_000, path: nil}
    {:ok, _next, claim} = claim_push(engine(["first", ref, "last"]))
    assert claim.expanded == %{titles: ["first", ref, "last"], total_count: 3}

    payload = APNS.request_payload(claim)

    assert payload["attention"]["expanded"] == %{
             "v" => 1,
             "titles" => ["first"],
             "total_count" => 3
           }

    assert strip_expansion(payload) == APNS.request_payload(Map.delete(claim, :expanded))
  end

  test "single claims keep the already bounded and fitted preview and complete identity" do
    engine = engine(["Actual request title"])
    key = {@person, "app:synthetic", "1"}
    engine = put_in(engine.messages[key].message["body"], String.duplicate("x", 200))
    {:ok, _next, claim} = claim_push(engine)
    assert claim.expanded == :single
    assert claim.alert["body"] == String.duplicate("x", 159) <> "…"

    base = APNS.request_payload(Map.delete(claim, :expanded))
    actual = APNS.request_payload(claim)
    assert actual["aps"]["category"] == "harklight.expanded.single.v1"
    assert strip_expansion(actual) == base
    assert actual["aps"]["alert"]["body"] == base["aps"]["alert"]["body"]
    assert_sync(actual, engine, "i1")
  end

  test "terminal-only claims remain background without a category or envelope" do
    engine = engine(["resolved"])
    key = {@person, "app:synthetic", "1"}
    engine = put_in(engine.messages[key].terminal, %{"type" => "answered"})
    {:ok, _next, claim} = claim_push(engine)
    assert claim.presentation == :background
    refute Map.has_key?(claim, :expanded)
    payload = APNS.request_payload(claim)
    assert payload["aps"] == %{"content-available" => 1, "badge" => 0}

    assert_sync(payload, engine, "i1")
  end

  test "a real chat claim uses the single category and keeps its chat destination" do
    engine = engine(["request"])
    key = {@person, "app:synthetic", "1"}

    engine = %{
      engine
      | conversation_events: %{
          {@person, 1} => %{
            key: key,
            payload: %{"kind" => "reply", "author" => "origin", "text" => "Actual reply"}
          }
        }
    }

    engine =
      update_in(engine.push_bindings[{@person, @device}], fn binding ->
        %{binding | pending_high: 0, accepted_high: 1, pending_conversation_high: 1}
      end)

    {:ok, _next, claim} = claim_push(engine)
    assert claim.expanded == :single
    assert claim.locator["target"] == "chat"
    payload = APNS.request_payload(claim)
    assert payload["aps"]["category"] == "harklight.expanded.single.v1"
    assert payload["aps"]["alert"]["body"] == "Actual reply"
    assert_sync(payload, engine, "i1", "chat")
    assert strip_expansion(payload) == APNS.request_payload(Map.delete(claim, :expanded))
  end

  test "conversation catch-up without an alert or badge carries a sync locator" do
    engine = engine(["request"])
    key = {@person, "app:synthetic", "1"}

    engine =
      engine
      |> put_in([Access.key(:conversation_events), {@person, 2}], %{
        key: key,
        payload: %{"kind" => "reply", "author" => "person"}
      })
      |> update_in([Access.key(:push_bindings), {@person, @device}], fn binding ->
        %{binding | pending_high: 0, accepted_high: 1, pending_conversation_high: 2}
      end)

    {:ok, _next, claim} = claim_push(engine)
    assert claim.presentation == :background
    payload = APNS.request_payload(Map.delete(claim, :badge))
    assert payload["aps"] == %{"content-available" => 1}
    assert_sync(payload, engine, "i1")
  end

  test "grouped fitting can omit the whole locator to keep more complete names" do
    engine = engine(["First", "Second"])
    {:ok, _next, claim} = claim_push(engine)
    long = String.duplicate("a", 3_900)
    payload = APNS.request_payload(%{claim | grouped_alert_names: [long, "Small"]})

    assert payload["aps"]["alert"]["body"] == long <> ", Small"

    assert Map.delete(payload["attention"], "expanded") ==
             %{"v" => 2, "type" => "sync", "server" => engine.relay_instance_id}

    assert byte_size(AttentionServer.JSON.encode!(payload)) <= 4_096
  end

  defp claim_push(engine) do
    {:ok, next, claim} = Engine.claim_push(engine, 1_000)
    {:ok, next, put_in(claim.locator["rev"], 42)}
  end

  defp assert_sync(payload, engine, item_id, target \\ nil) do
    locator = %{
      "v" => 2,
      "person_id" => @person,
      "device_id" => @device,
      "binding_id" => engine.push_bindings[{@person, @device}].binding_id,
      "item_id" => item_id,
      "rev" => 42
    }

    locator = if target, do: Map.put(locator, "target", target), else: locator

    assert Map.delete(payload["attention"], "expanded") == %{
             "v" => 2,
             "type" => "sync",
             "server" => engine.relay_instance_id,
             "locator" => locator
           }

    assert byte_size(AttentionServer.JSON.encode!(payload)) <= 4_096
  end

  defp strip_expansion(payload) do
    payload
    |> update_in(["aps"], &Map.delete(&1, "category"))
    |> update_in(["attention"], &Map.delete(&1, "expanded"))
  end

  defp engine(titles, accepted_high \\ 0) do
    pairs = Enum.with_index(titles, 1)

    messages =
      Map.new(pairs, fn {title, seq} ->
        key = {@person, "app:synthetic", Integer.to_string(seq)}

        {key,
         %{
           message: %{"title" => title, "display_name" => "Synthetic sender"},
           accepted_seq: seq,
           origin: "synthetic-origin",
           terminal: nil
         }}
      end)

    events =
      Map.new(pairs, fn {_title, seq} ->
        {{@person, seq},
         %{kind: :delivery, key: {@person, "app:synthetic", Integer.to_string(seq)}}}
      end)

    binding = %{
      revision: 1,
      binding_id: "00000000-0000-4000-8000-000000000001",
      profile_id: "harklight-sandbox",
      address: String.duplicate("ab", 32),
      active: true,
      registered_at: 1_000,
      state: "queued",
      pending_high: length(titles),
      accepted_high: accepted_high,
      pending_conversation_high: 0,
      accepted_conversation_high: 0,
      next_at: 1_000,
      attempt_count: 0,
      last_attempt_at: nil,
      claim_id: nil,
      claim_high: nil,
      claim_conversation_high: nil,
      claim_at: nil
    }

    %{
      Engine.new()
      | persons: %{@person => length(titles)},
        messages: messages,
        events: events,
        push_bindings: %{{@person, @device} => binding}
    }
  end
end
