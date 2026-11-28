defmodule AttentionServer.Migration.VerifyFacts do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportSession, ImportStatus, SourceSchema, SourceTables}
  alias AttentionServer.Repository.Postgres.Attributes

  # Read-only source-fact equivalence. Normalized projections, material closure
  # and wire manifests have separate gates; this never validates or activates.
  def run(db, identity, bundle) do
    with {:ok, manifest, summary} <- Bundle.verify(bundle),
         true <- identity.source == Base.decode16!(summary.sha256, case: :lower),
         {:ok, %{"phase" => phase}} when phase in ["loading", "validated", "active"] <- ImportStatus.read(db, identity),
         {:ok, counts} <- records(db, bundle, manifest),
         {:ok, counts} <- tables(db, bundle, manifest, counts),
         :ok <- counts(db, counts) do
      {:ok, %{source_sha256: summary.sha256, families: counts, rows: Enum.sum(Map.values(counts))}}
    else
      false -> {:error, :import_source_mismatch}
      {:error, _} = error -> error
      _ -> {:error, :import_not_claimed}
    end
  end

  defp records(db, bundle, manifest) do
    families = SourceSchema.record_families() |> Map.values() |> List.flatten() |> Enum.map(&Atom.to_string/1)
    counts = Map.new(families ++ ["origin_feeds", "advisory_feeds"], &{&1, 0})
    Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, counts, fn {address, value}, acc ->
      family = family(address)
      with true <- SourceSchema.valid_record?(address, value),
           :ok <- compare(db, family, address, value) do
        {:ok, Map.update!(acc, family, &(&1 + 1))}
      else
        false -> {:error, :unsupported_source_record_address}
        error -> error
      end
    end)
  end

  defp tables(db, bundle, manifest, counts) do
    Enum.reduce_while(Enum.sort(SourceSchema.tables()), {:ok, counts}, fn {table, _}, {:ok, acc} ->
      case SourceTables.disposition(table) do
        {:durable, _} ->
          result = case Map.fetch(manifest.data.tables, table) do
            :error -> {:ok, 0}
            {:ok, source} ->
              Frames.fold(Path.join(bundle, source.file), source.summary, 0, fn values, count ->
                with {:ok, address, _} <- SourceTables.fact(table, values),
                     :ok <- compare(db, table, address, values), do: {:ok, count + 1}
              end)
          end
          case result do
            {:ok, count} -> {:cont, {:ok, Map.put(acc, table, count)}}
            error -> {:halt, error}
          end
        disposition when disposition in [:derived, :source_only] -> {:cont, {:ok, acc}}
        error -> {:halt, error}
      end
    end)
  end

  defp compare(db, family, address, expected) do
    with {:ok, %{rows: [[body]]}} <- ImportSession.query(db,
           "SELECT body FROM harklight.compatibility_facts WHERE family=$1 AND address=$2",
           [family, Attributes.key(address)]),
         {:ok, actual} <- Attributes.decode(body),
         true <- actual === expected do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :source_fact_mismatch}
    end
  end

  defp counts(db, expected) do
    Enum.reduce_while(expected, :ok, fn {family, count}, :ok ->
      case ImportSession.query(db,
        "SELECT count(*) FROM harklight.compatibility_facts WHERE family=$1", [family]) do
        {:ok, %{rows: [[^count]]}} -> {:cont, :ok}
        {:ok, _} -> {:halt, {:error, :source_fact_count_mismatch}}
        error -> {:halt, error}
      end
    end)
  end

  defp family({:field, field}), do: Atom.to_string(field)
  defp family({kind, field, _}) when kind in [:row, :member], do: Atom.to_string(field)
  defp family({:origin_feed, _, _}), do: "origin_feeds"
  defp family({:advisory, _, _}), do: "advisory_feeds"
end
