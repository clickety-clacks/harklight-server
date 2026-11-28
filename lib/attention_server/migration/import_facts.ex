defmodule AttentionServer.Migration.ImportFacts do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportSession, SourceSchema}
  alias AttentionServer.Repository.Postgres.Attributes

  # This lossless staging pass preserves addressed Engine facts. Normalized
  # projections, side tables, file chunks and final validation follow separately.
  # It deliberately cannot validate or activate an import.
  def run(connection, identity, root) do
    with {:ok, manifest, summary} <- Bundle.verify(root),
         true <- identity.source == Base.decode16!(summary.sha256, case: :lower),
         {:ok, _} <- ImportSession.claim(connection, identity),
         {:ok, checkpoint} <- ImportSession.checkpoint(connection, identity, "records"),
         initial = %{
           position:
             ImportSession.advance(%{ordinal: 0, digest: :crypto.hash(:sha256, <<>>)}, []),
           checkpoint: checkpoint,
           pending: []
         },
         {:ok, state} <-
           Frames.fold(
             Path.join(root, "records.frames"),
             manifest.data.records,
             initial,
             &consume(connection, identity, &1, &2)
           ),
         true <- state.position.ordinal >= checkpoint.ordinal,
         {:ok, final} <- flush(connection, identity, state) do
      {:ok, final.checkpoint}
    else
      false -> {:error, :import_source_mismatch}
      {:error, _} = error -> error
    end
  end

  defp consume(connection, identity, entry, state) do
    next = ImportSession.advance(state.position, [entry])
    state = %{state | position: next}

    cond do
      next.ordinal < state.checkpoint.ordinal ->
        {:ok, state}

      next.ordinal == state.checkpoint.ordinal ->
        if next == state.checkpoint, do: {:ok, state}, else: {:error, :checkpoint_source_mismatch}

      true ->
        state = %{state | pending: [entry | state.pending]}

        if length(state.pending) == 256,
          do: flush(connection, identity, state),
          else: {:ok, state}
    end
  end

  defp flush(_, _, %{pending: []} = state), do: {:ok, state}

  defp flush(connection, identity, state) do
    entries = Enum.reverse(state.pending)

    with {:ok, checkpoint} <-
           ImportSession.batch(
             connection,
             identity,
             "records",
             state.checkpoint,
             entries,
             &write/2
           ) do
      {:ok, %{state | checkpoint: checkpoint, pending: []}}
    end
  end

  defp write(db, entries) do
    Enum.reduce_while(entries, :ok, fn
      {address, value}, :ok ->
        if SourceSchema.valid_record?(address, value) do
          body = AttentionServer.JSON.encode!(Attributes.encode(value))

          case ImportSession.query(
                 db,
                 """
                 INSERT INTO harklight.compatibility_facts (family, address, body)
                 VALUES ($1, $2, $3::text::jsonb)
                 """,
                 [family(address), Attributes.key(address), body]
               ) do
            {:ok, _} -> {:cont, :ok}
            error -> {:halt, error}
          end
        else
          {:halt, {:error, :unsupported_source_record_address}}
        end

      _, :ok ->
        {:halt, {:error, :invalid_source_record}}
    end)
  end

  defp family({:field, field}), do: Atom.to_string(field)
  defp family({kind, field, _}) when kind in [:row, :member], do: Atom.to_string(field)
  defp family({:origin_feed, _, _}), do: "origin_feeds"
  defp family({:advisory, _, _}), do: "advisory_feeds"
end
