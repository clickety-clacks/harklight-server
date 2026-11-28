defmodule AttentionServer.Migration.VerifyAllocationBounds do
  @moduledoc false
  alias AttentionServer.Migration.{Frames, ImportSession}
  alias AttentionServer.Repository.Postgres.Attributes

  # Called after exact source-fact and counter equivalence. Check each retained
  # value against its own allocator, which is equivalent to checking the maximum
  # without accumulating a map of every person, origin or session in memory.
  def run(db, path, summary) do
    case Frames.fold(path, summary, :ok, fn row, :ok ->
      with :ok <- verify(db, row), do: {:ok, :ok}
    end) do
      {:ok, :ok} -> :ok
      error -> error
    end
  end

  defp verify(db, {{:row, :messages, {person, _, _}}, record}) do
    with :ok <- bound(db, {person, "", "", "accepted"}, Map.fetch!(record, :accepted_seq)),
         :ok <- optional_order(db, person, Map.get(record, :free_order)), do: :ok
  end
  defp verify(db, {{:row, :events, {person, seq}}, _}),
    do: bound(db, {person, "", "", "accepted"}, seq)
  defp verify(db, {{:row, :conversation_events, {person, seq}}, event}) do
    with :ok <- bound(db, {person, "", "", "conversation"}, seq),
         :ok <- optional_order(db, person, Map.get(event, :free_order)), do: :ok
  end
  defp verify(db, {{:origin_feed, {person, origin}, {part, seq}}, _})
       when part in [:entry, :delivery_ack],
    do: bound(db, {person, origin, "", "origin_feed"}, seq)
  defp verify(db, {{:advisory, {person, origin, handle}, {:entry, seq}}, _}),
    do: bound(db, {person, origin, handle, "advisory_feed"}, seq)

  # Also check the exact counters here so the focused bound tests can exercise
  # a faithfully copied but inconsistent source, independently of the bundle gate.
  defp verify(db, {{:row, field, person}, value})
       when field in [:persons, :free_order_highs, :conversation_highs] do
    kind = %{persons: "accepted", free_order_highs: "free_order", conversation_highs: "conversation"}[field]
    equal(db, {person, "", "", kind}, value)
  end
  defp verify(db, {{:origin_feed, {person, origin}, :high}, value}),
    do: equal(db, {person, origin, "", "origin_feed"}, value)
  defp verify(db, {{:advisory, {person, origin, handle}, :high}, value}),
    do: equal(db, {person, origin, handle, "advisory_feed"}, value)
  defp verify(_, _), do: :ok

  # Older records without arrival ordering have no retained free-order value.
  defp optional_order(_, _, nil), do: :ok
  defp optional_order(db, person, value), do: bound(db, {person, "", "", "free_order"}, value)

  defp bound(db, scope, value) when is_integer(value) and value >= 0 do
    case high(db, scope) do
      {:ok, %{rows: [[high]]}} when is_integer(high) and high >= value -> :ok
      {:error, _} = error -> error
      _ -> mismatch(scope)
    end
  end
  defp bound(_, scope, _), do: mismatch(scope)

  defp equal(db, scope, value) do
    case high(db, scope) do
      {:ok, %{rows: [[^value]]}} -> :ok
      {:error, _} = error -> error
      _ -> mismatch(scope)
    end
  end

  defp high(db, scope), do: ImportSession.query(db,
    "SELECT high FROM harklight.allocation_highs WHERE person_id=$1 AND origin_key=$2 AND session_handle=$3 AND kind=$4",
    Tuple.to_list(scope))

  defp mismatch({_, _, _, kind} = scope), do: {:error, {:allocation_bound_mismatch, kind,
    Base.encode16(:crypto.hash(:sha256, Attributes.key(scope)), case: :lower)}}
end
