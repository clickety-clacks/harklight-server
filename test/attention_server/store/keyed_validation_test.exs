defmodule AttentionServer.Store.KeyedValidationTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Engine, Store.KeyedValidation}

  @person "person"
  @item {@person, "sender", "item"}

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

  test "upload changes visit only the indexed owning entries" do
    scope = {@person, "origin"}
    target = %{"files" => [%{"tier" => "upload", "upload_id" => "file"}]}
    unrelated = %{"files" => [%{"tier" => "upload", "upload_id" => "other"}]}

    engine = %{
      Engine.new()
      | origin_feeds: %{scope => %{entries: %{1 => target, 2 => unrelated}}}
    }

    index = KeyedValidation.upload_index(engine)

    assert {:ok, ^index, [{@person, ^target}]} =
             KeyedValidation.upload_changes(index, engine, engine, [{:row, :uploads, "file"}])

    next = put_in(engine.origin_feeds[scope].entries, %{2 => unrelated})

    assert {:ok, updated, []} =
             KeyedValidation.upload_changes(index, engine, next, [
               {:origin_feed, scope, {:entry, 1}}
             ])

    refute Map.has_key?(updated, "file")
    assert updated["other"] == index["other"]
  end
end
