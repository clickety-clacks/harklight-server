defmodule AttentionServer.Migration.ImportRows do
  @moduledoc false
  alias AttentionServer.Migration.{Frames, ImportSession}

  # Each named projection selects a deterministic subsequence of an immutable
  # framed source. Its source prefix and checkpoint advance with its row writes.
  def run(db, identity, family, path, summary, select, write) do
    with {:ok, checkpoint} <- ImportSession.checkpoint(db, identity, family),
         initial = %{
           position: %{ordinal: 0, digest: :crypto.hash(:sha256, <<>>)},
           checkpoint: checkpoint,
           pending: []
         },
         {:ok, state} <-
           Frames.fold(path, summary, initial, fn row, state ->
             if select.(row),
               do: consume(db, identity, family, row, state, write),
               else: {:ok, state}
           end),
         true <- state.position.ordinal >= checkpoint.ordinal,
         {:ok, final} <- flush(db, identity, family, state, write) do
      {:ok, final.checkpoint}
    else
      false -> {:error, :checkpoint_source_mismatch}
      error -> error
    end
  end

  defp consume(db, identity, family, row, state, write) do
    next = ImportSession.advance(state.position, [row])
    state = %{state | position: next}

    cond do
      next.ordinal < state.checkpoint.ordinal ->
        {:ok, state}

      next.ordinal == state.checkpoint.ordinal ->
        if next == state.checkpoint, do: {:ok, state}, else: {:error, :checkpoint_source_mismatch}

      true ->
        state = %{state | pending: [row | state.pending]}

        if length(state.pending) == 256,
          do: flush(db, identity, family, state, write),
          else: {:ok, state}
    end
  end

  defp flush(_, _, _, %{pending: []} = state, _), do: {:ok, state}

  defp flush(db, identity, family, state, write) do
    with {:ok, checkpoint} <-
           ImportSession.batch(
             db,
             identity,
             family,
             state.checkpoint,
             Enum.reverse(state.pending),
             write
           ) do
      {:ok, %{state | checkpoint: checkpoint, pending: []}}
    end
  end
end
