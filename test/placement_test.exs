defmodule AttentionServer.PlacementTest do
  use ExUnit.Case, async: true

  alias AttentionServer.Placement

  test "retained placement preserves order and scores with subquadratic list construction" do
    smaller = placement_reductions(6_000)
    larger = placement_reductions(12_000)

    assert larger < smaller * 3,
           "doubling the retained items increased reductions from #{smaller} to #{larger}"
  end

  defp placement_reductions(count) do
    items =
      for sequence <- 1..count do
        %{
          identity: {"app:test", Integer.to_string(sequence)},
          message: %{},
          accepted_seq: sequence
        }
      end

    previous = Enum.map(items, &Placement.entry(Map.put(&1, :score, 0.75)))
    {:reductions, before} = Process.info(self(), :reductions)
    retained = Placement.place("jev-set", items, previous, nil)
    {:reductions, after_placement} = Process.info(self(), :reductions)

    assert retained == previous
    after_placement - before
  end
end
