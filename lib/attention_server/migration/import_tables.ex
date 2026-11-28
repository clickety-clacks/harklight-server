defmodule AttentionServer.Migration.ImportTables do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, ImportFacts, ImportRows, ImportSession, SourceTables}
  alias AttentionServer.Repository.Postgres.Attributes
  alias AttentionServer.JSON

  # Lossless staging only. Runtime projections and overlay application are
  # separate required stages before validation; this cannot publish the target.
  def run(db, identity, root) do
    with {:ok, _} <- ImportFacts.run(db, identity, root),
         {:ok, manifest, _} <- Bundle.verify(root) do
      Enum.reduce_while(Enum.sort(manifest.data.tables), :ok, fn {table, source}, :ok ->
        case stage(db, identity, root, table, source) do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end
      end)
    end
  end

  defp stage(db, identity, root, table, source) do
    case SourceTables.disposition(table) do
      {:durable, _} ->
        case ImportRows.run(
               db, identity, "sqlite/" <> table <> "/v1",
               Path.join(root, source.file), source.summary,
               fn _ -> true end, &write(&1, table, &2)
             ) do
          {:ok, _} -> :ok
          error -> error
        end

      disposition when disposition in [:derived, :source_only] -> :ok
      error -> error
    end
  end

  defp write(db, table, rows) do
    Enum.reduce_while(rows, :ok, fn values, :ok ->
      with {:ok, address, _row} <- SourceTables.fact(table, values),
           {:ok, %{num_rows: 1}} <- ImportSession.query(
             db,
             "INSERT INTO harklight.compatibility_facts(family,address,body) VALUES ($1,$2,$3::text::jsonb)",
             [table, Attributes.key(address), JSON.encode!(Attributes.encode(values))]
           ) do
        {:cont, :ok}
      else
        {:error, _} = error -> {:halt, error}
        _ -> {:halt, {:error, :import_row_count_mismatch}}
      end
    end)
  end
end
