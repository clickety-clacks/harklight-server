defmodule AttentionServer.PlacementTest do
  use ExUnit.Case, async: true

  alias AttentionServer.Placement
  alias AttentionServer.Placement.JevSet

  test "retained placement preserves order and scores with subquadratic list construction" do
    smaller = placement_words(6_000)
    larger = placement_words(12_000)

    assert larger < smaller * 3,
           "doubling the retained items increased allocated words from #{smaller} to #{larger}"
  end

  defp placement_words(count) do
    items =
      for sequence <- 1..count do
        %{
          identity: {"app:test", Integer.to_string(sequence)},
          message: %{},
          accepted_seq: sequence
        }
      end

    previous = Enum.map(items, &Placement.entry(Map.put(&1, :score, 0.75)))
    trace = :trace.session_create(:retained_placement_allocations, self(), [])
    target = {JevSet, :retained_place, 2}
    pid = self()

    try do
      Code.ensure_loaded!(JevSet)
      assert :trace.function(trace, target, true, [:call_memory]) == 1
      assert :trace.process(trace, pid, true, [:call]) == 1
      assert Placement.place("jev-set", items, previous, nil) == previous
      assert {:call_memory, [{^pid, 1, words}]} = :trace.info(trace, target, :call_memory)
      words
    after
      :trace.session_destroy(trace)
    end
  end
end
