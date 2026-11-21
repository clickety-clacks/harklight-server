defmodule AttentionServer.Store.KeyedValidationTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Engine, Store.KeyedValidation}

  @person "person"
  @item {@person, "sender", "item"}

  test "preflight refuses malformed addresses and browser clocks" do
    old = Engine.new()

    for address <- [
          {:row, :unknown, "key"},
          {:row, :conversation_cursors, {"person", "reader", "extra"}},
          {:row, :origin_actions, "person"},
          {:row, :push_bindings, "person"},
          {:member, :conversation_opened, {"person", "reader", 1}},
          {:origin_feed, {"person", "origin"}, {:receipt, 1}}
        ] do
      assert {:error, :invalid_fact} = KeyedValidation.preflight(old, old, MapSet.new([address]))
    end

    next = %{old | browser_sign_in_clock_high_water: -5}
    touched = MapSet.new([{:field, :browser_sign_in_clock_high_water}])
    assert {:error, :invalid_fact} = KeyedValidation.preflight(old, next, touched)

    assert :ok =
             KeyedValidation.preflight(
               old,
               %{next | browser_sign_in_clock_high_water: 5},
               touched
             )
  end

  test "preflight checks an origin append locally and refuses gaps or rewrites" do
    scope = {@person, "origin"}

    entry = %{
      "feed_seq" => 1,
      "event_id" => "1",
      "kind" => "discussion",
      "author" => "person",
      "session_handle" => "00000000-0000-4000-8000-000000000001"
    }

    old = %{Engine.new() | persons: %{@person => 0}, devices: %{"origin" => %{person: @person}}}
    next = put_in(old.origin_feeds[scope], %{high: 1, entries: %{1 => entry}})
    high = {:origin_feed, scope, :high}
    first = {:origin_feed, scope, {:entry, 1}}
    touched = MapSet.new([high, first])
    assert :ok = KeyedValidation.preflight(old, next, touched)

    assert {:error, :invalid_fact} =
             KeyedValidation.preflight(old, next, MapSet.delete(touched, high))

    assert {:error, :invalid_fact} =
             KeyedValidation.preflight(old, next, MapSet.delete(touched, first))

    for {invalid, address} <- [
          {put_in(next.origin_feeds[scope].high, 0), high},
          {put_in(next.origin_feeds[scope].entries, %{}), first},
          {put_in(next.origin_feeds[scope].entries[1]["kind"], "unknown"), first},
          {put_in(next.origin_feeds[scope].entries[2], %{entry | "feed_seq" => 2}),
           {:origin_feed, scope, {:entry, 2}}}
        ] do
      assert {:error, :invalid_fact} =
               KeyedValidation.preflight(next, invalid, MapSet.new([address]))
    end
  end

  test "accepts contiguous delivery and conversation changes" do
    old = %{Engine.new() | persons: %{@person => 0}, messages: %{@item => %{}}}

    next = %{
      old
      | persons: %{@person => 1},
        events: %{{@person, 1} => %{kind: :delivery, key: @item}},
        conversation_highs: %{@person => 1},
        conversation_events: %{{@person, 1} => %{kind: :conversation, key: @item}}
    }

    touched = [
      {:row, :persons, @person},
      {:row, :events, {@person, 1}},
      {:row, :conversation_highs, @person},
      {:row, :conversation_events, {@person, 1}}
    ]

    assert :ok = KeyedValidation.sequences(old, next, touched)
    assert {:error, :invalid_sequence} = KeyedValidation.sequences(old, next, tl(touched))
  end

  test "rejects high-water advances without their rows and rows without their target" do
    old = %{Engine.new() | persons: %{@person => 0}}
    advanced = %{old | persons: %{@person => 1}}

    assert {:error, :invalid_sequence} =
             KeyedValidation.sequences(old, advanced, [{:row, :persons, @person}])

    next = %{advanced | events: %{{@person, 1} => %{kind: :delivery, key: @item}}}
    touched = [{:row, :persons, @person}, {:row, :events, {@person, 1}}]
    assert {:error, :invalid_sequence} = KeyedValidation.sequences(old, next, touched)
  end

  test "upload changes visit only the indexed owning entries and message revisions" do
    scope = {@person, "origin"}
    files = [%{"tier" => "upload", "upload_id" => "file"}]
    target = %{"files" => files}
    unrelated = %{"files" => [%{"tier" => "upload", "upload_id" => "other"}]}
    entry = {:origin_feed, scope, {:entry, 1}}
    message = {:row, :messages, @item}
    upload = {:row, :uploads, "file"}

    engine = %{
      Engine.new()
      | messages: %{@item => %{files_by_revision: %{1 => files, 2 => files}}},
        origin_feeds: %{scope => %{entries: %{1 => target, 2 => unrelated}}}
    }

    index = KeyedValidation.upload_index(engine)

    assert {:ok, ^index, related} =
             KeyedValidation.upload_changes(index, engine, engine, [upload])

    assert related == MapSet.new([entry, message])

    next = put_in(engine.origin_feeds[scope].entries, %{2 => unrelated})

    assert {:ok, updated, related} =
             KeyedValidation.upload_changes(index, engine, next, [entry, upload])

    assert related == MapSet.new([entry, message])
    assert updated["file"] == MapSet.new([message])
    assert updated["other"] == index["other"]

    removed = %{next | messages: %{}}

    assert {:ok, final, _related} =
             KeyedValidation.upload_changes(updated, next, removed, [message])

    refute Map.has_key?(final, "file")
    assert final["other"] == index["other"]
  end
end
