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

    assert changes == %{
             {:field, :pending_clock_high_water} => {:put, 3},
             {:member, :opened_revisions, :another} => {:put, true},
             {:member, :opened_revisions, opened} => :delete,
             {:row, :messages, :temporary} => {:put, nil}
           }
  end

  test "a transition that cancels its own changes leaves no persistence work" do
    previous = %{
      Engine.new()
      | messages: %{existing: :value},
        opened_revisions: MapSet.new([:existing])
    }

    transition =
      Transition.new()
      |> Transition.put({:row, :messages, :absent}, :temporary)
      |> Transition.delete({:row, :messages, :absent})
      |> Transition.delete({:row, :messages, :existing})
      |> Transition.put({:row, :messages, :existing}, :value)
      |> Transition.put({:field, :pending_clock_high_water}, 4)
      |> Transition.put({:field, :pending_clock_high_water}, 0)
      |> Transition.put({:member, :opened_revisions, :absent}, true)
      |> Transition.delete({:member, :opened_revisions, :absent})
      |> Transition.delete({:member, :opened_revisions, :existing})
      |> Transition.put({:member, :opened_revisions, :existing}, true)

    assert {^previous, changes} = Transition.apply(transition, previous)
    assert map_size(changes) == 0
  end

  test "rejects collection replacement and malformed membership before applying" do
    previous = Engine.new()
    transition = Transition.put(Transition.new(), {:row, :messages, :item}, :value)

    assert_raise ArgumentError, fn ->
      Transition.put(transition, {:field, :messages}, %{})
    end

    assert_raise ArgumentError, fn ->
      Transition.put(transition, {:field, :opened_revisions}, MapSet.new())
    end

    assert_raise ArgumentError, fn ->
      Transition.put(transition, {:member, :opened_revisions, :item}, false)
    end

    assert {next, %{{:row, :messages, :item} => {:put, :value}}} =
             Transition.apply(transition, previous)

    assert next.messages == %{item: :value}
  end

  test "appends and acknowledges advisories without replacing retained feed rows" do
    scope = {"person", "origin", "session"}
    feed = %{high: 1, entries: %{1 => :old}, receipts: MapSet.new()}
    previous = %{Engine.new() | advisory_feeds: %{scope => feed}}

    transition =
      Transition.new()
      |> Transition.put({:advisory, scope, :high}, 2)
      |> Transition.put({:advisory, scope, {:entry, 2}}, :new)
      |> Transition.put({:advisory, scope, {:receipt, :old_id}}, true)

    assert {next, changes} = Transition.apply(transition, previous)

    assert next.advisory_feeds[scope] == %{
             high: 2,
             entries: %{1 => :old, 2 => :new},
             receipts: MapSet.new([:old_id])
           }

    assert changes == transition

    assert_raise ArgumentError, fn ->
      Transition.put(transition, {:row, :advisory_feeds, scope}, feed)
    end
  end
end
