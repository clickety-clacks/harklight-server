defmodule AttentionServer.Migration.VerifyReceipts do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportReceipts, ImportSession, ImportStatus, SourceTables}
  alias AttentionServer.Repository.Postgres.Attributes
  alias AttentionServer.{JSON, ClientAPI.Idempotency}

  @tables ~w(client_idempotency harklight_app_receipts)
  # These have no source-to-runtime cutover projector. Historical ACKs and
  # snapshots must not be turned into synthetic handoffs, leases or baselines.
  @empty_tables ~w(handoff_receipts jobs baseline_snapshot_items baseline_snapshots)

  def run(db, identity, bundle) do
    with {:ok, manifest, summary} <- Bundle.verify(bundle),
         true <- identity.source == Base.decode16!(summary.sha256, case: :lower),
         {:ok, %{"phase" => phase}} when phase in ["loading", "validated"] <- ImportStatus.read(db, identity),
         {:ok, counts} <- tables(db, bundle, manifest),
         :ok <- count(db, "operation_receipts", Enum.sum(Map.values(counts))),
         :ok <- empty_tables(db),
         {:ok, ^manifest, ^summary} <- Bundle.verify(bundle) do
      {:ok, %{receipts: counts, empty_tables: @empty_tables}}
    else
      false -> {:error, :import_source_mismatch}
      {:error, _} = error -> error
      _ -> {:error, :receipt_verification_requires_staged_import}
    end
  end

  defp tables(db, bundle, manifest) do
    Enum.reduce_while(@tables, {:ok, %{}}, fn table, {:ok, counts} ->
      result = case Map.fetch(manifest.data.tables, table) do
        :error -> {:ok, 0}
        {:ok, source} ->
          Frames.fold(Path.join(bundle, source.file), source.summary, 0, fn values, n ->
            with :ok <- row(db, table, values), do: {:ok, n + 1}
          end)
      end
      with {:ok, n} <- result,
           {:ok, %{rows: [[^n]]}} <- ImportSession.query(db,
             "SELECT count(*) FROM harklight.compatibility_facts WHERE family=$1", [table]) do
        {:cont, {:ok, Map.put(counts, table, n)}}
      else
        {:error, _} = error -> {:halt, error}
        _ -> {:halt, {:error, {:receipt_source_count_mismatch, table}}}
      end
    end)
  end

  defp row(db, table, [scope, key, digest, status, body, at | flags] = values) do
    with {:ok, {person, origin}} <- ImportReceipts.scope(scope),
         {:ok, address, _} <- SourceTables.fact(table, values),
         {:ok, hash} when byte_size(hash) == 32 <- Base.decode16(digest, case: :lower),
         {:ok, decoded} when is_map(decoded) <- JSON.decode_line(body <> "\n"),
         {:ok, expires, original} <- retention(table, at, flags),
         :ok <- compare(db, table, address,
           "SELECT body FROM harklight.compatibility_facts WHERE family=$1 AND address=$2",
           [table, Attributes.key(address)], [Attributes.encode(values)]),
         :ok <- compare(db, table, address,
           "SELECT digest,status,response,created_at_ms,expires_at_ms,original_response FROM harklight.operation_receipts WHERE person_id=$1 AND origin_key=$2 AND namespace=$3 AND operation_key=$4",
           [person, origin, table, key], [hash, status, Attributes.encode(decoded), at, expires, original]) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_source_receipt}
    end
  end
  defp row(_, _, _), do: {:error, :invalid_source_receipt}

  defp retention("client_idempotency", at, []),
    do: {:ok, at + Idempotency.retention_ms(), false}
  defp retention("harklight_app_receipts", _at, [original]) when original in [0, 1],
    do: {:ok, nil, original == 1}
  defp retention(_, _, _), do: {:error, :invalid_source_receipt}

  defp compare(db, table, address, sql, params, expected) do
    case ImportSession.query(db, sql, params) do
      {:ok, %{rows: [^expected]}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, {:receipt_state_mismatch, table,
        Base.encode16(:crypto.hash(:sha256, Attributes.key(address)), case: :lower)}}
    end
  end

  defp empty_tables(db) do
    Enum.reduce_while(@empty_tables, :ok, fn table, :ok ->
      case count(db, table, 0) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp count(db, table, expected) do
    case ImportSession.query(db, "SELECT count(*) FROM harklight." <> table, []) do
      {:ok, %{rows: [[^expected]]}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, {:receipt_count_mismatch, table}}
    end
  end
end
