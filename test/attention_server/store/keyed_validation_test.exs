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
end
