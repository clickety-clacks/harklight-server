defmodule AttentionServer.Store.TransitionTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Engine, Store.Transition}

  test "materializes only the last operation for each touched record" do
    key = {"person", "sender", "changed"}
    untouched = {"person", "sender", "untouched"}
    removed = {"person", "sender", "removed"}
    previous = %{Engine.new() | messages: %{untouched => :kept, removed => :old}}

    transition =
      Transition.new()
      |> Transition.put({:row, :messages, key}, :first)
      |> Transition.put({:row, :messages, key}, :last)
      |> Transition.delete({:row, :messages, removed})
      |> Transition.put({:row, :messages, untouched}, :kept)

    assert {next, changes} = Transition.apply(transition, previous)
    assert next.messages == %{key => :last, untouched => :kept}

    assert changes == %{
             {:row, :messages, key} => {:put, :last},
             {:row, :messages, removed} => :delete
           }
  end

  test "distinguishes scalars, set members, and nil row values" do
    opened = {"person", "sender", "item", 1}
    previous = %{Engine.new() | opened_revisions: MapSet.new([opened])}

    transition =
      Transition.new()
      |> Transition.put({:field, :pending_clock_high_water}, 3)
      |> Transition.put({:member, :opened_revisions, :another}, true)
      |> Transition.delete({:member, :opened_revisions, opened})
      |> Transition.delete({:row, :messages, :temporary})
      |> Transition.put({:row, :messages, :temporary}, nil)

    assert {next, changes} = Transition.apply(transition, previous)
    assert next.pending_clock_high_water == 3
    assert next.opened_revisions == MapSet.new([:another])
    assert Map.fetch!(next.messages, :temporary) == nil
    assert changes[{:row, :messages, :temporary}] == {:put, nil}
  end

  test "a transition that cancels its own changes leaves no persistence work" do
    previous = %{Engine.new() | messages: %{existing: :value}}

    transition =
      Transition.new()
      |> Transition.put({:row, :messages, :absent}, :temporary)
      |> Transition.delete({:row, :messages, :absent})
      |> Transition.delete({:row, :messages, :existing})
      |> Transition.put({:row, :messages, :existing}, :value)

    assert {^previous, changes} = Transition.apply(transition, previous)
    assert map_size(changes) == 0
  end
end
