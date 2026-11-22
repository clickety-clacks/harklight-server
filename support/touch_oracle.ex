defmodule AttentionServer.TouchOracle do
  @moduledoc false
  # Test-only oracle for Engine touch reporting: every record whose stored
  # encoding differs between two engines, as RecordCodec addresses. This is
  # exactly the set a keyed commit must write, including feed highs,
  # entries, delivery acks, advisory receipts and bounce rows.

  alias AttentionServer.LargeData
  alias AttentionServer.Store.RecordCodec

  def diff(old, next) do
    before = records(old)
    current = records(next)

    MapSet.union(MapSet.new(Map.keys(before)), MapSet.new(Map.keys(current)))
    |> Enum.reject(&(Map.fetch(before, &1) === Map.fetch(current, &1)))
    |> MapSet.new(&elem(:erlang.binary_to_term(&1), 1))
  end

  defp records(engine) do
    engine
    |> Map.put(:touched_keys, nil)
    |> LargeData.persist_refs()
    |> RecordCodec.encode()
    |> Map.new(fn {key, payload, _checksum} -> {key, payload} end)
  end
end
