defmodule AttentionServer.Migration.ValidateImport do
  @moduledoc false
  alias AttentionServer.Migration.{ImportReceipt, ImportSession, ValidationReport}

  # Holding the same row lock used by checkpoint batches makes the complete
  # comparison and validated publication one operation. Never accepts a caller's
  # report or digest, and never transitions to active. Checkpoint batches also
  # version that row: a validator queued behind a committed batch must refuse
  # its old snapshot with serialization_failure. Repeating the command starts
  # a fresh complete comparison; no earlier report is reused.
  def run(connection, identity, bundle, material_root) do
    Postgrex.transaction(connection, fn db ->
      with {:ok, _} <- ImportSession.query(db, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", []),
           {:ok, _} <- ImportSession.query(db, "SET LOCAL lock_timeout = '5s'", []),
           {:ok, phase, previous} <- lock(db, identity),
           {:ok, report} <- ValidationReport.in_transaction(db, identity, bundle, material_root),
           {:ok, digest} <- Base.decode16(report["sha256"], case: :lower),
           :ok <- publish(db, identity, phase, previous, digest) do
        Map.merge(ImportReceipt.document(identity), %{"command" => "validate", "phase" => "validated",
          "validation_sha256" => report["sha256"], "report" => report["report"],
          "material" => report["material"]})
      else
        {:error, reason} -> Postgrex.rollback(db, reason)
      end
    end, timeout: 300_000)
    |> normalize_result()
  end

  defp normalize_result({:error, %Postgrex.Error{postgres: %{code: :serialization_failure}}}),
    do: {:error, :validation_retry_required}
  defp normalize_result({:error, %Postgrex.Error{postgres: %{pg_code: "40001"}}}),
    do: {:error, :validation_retry_required}
  defp normalize_result(result), do: result

  defp lock(db, %{id: id, source: source, schema: schema}) do
    case ImportSession.query(db, """
      SELECT import_id,source_sha256,schema_sha256,phase,manifest_sha256
      FROM harklight.import_state WHERE singleton=true FOR UPDATE
      """, []) do
      {:ok, %{rows: [[^id, ^source, ^schema, "loading", nil]]}} -> {:ok, "loading", nil}
      {:ok, %{rows: [[^id, ^source, ^schema, "validated", digest]]}} -> {:ok, "validated", digest}
      {:ok, %{rows: [[^id, ^source, ^schema, "active", _]]}} -> {:error, :import_already_active}
      {:ok, _} -> {:error, :import_identity_mismatch}
      {:error, _} = error -> error
    end
  end

  defp publish(_, _, "validated", digest, digest), do: :ok
  defp publish(_, _, "validated", _, _), do: {:error, :validated_report_mismatch}
  defp publish(db, identity, "loading", nil, digest) do
    case ImportSession.query(db, """
      UPDATE harklight.import_state SET phase='validated',manifest_sha256=$4
      WHERE singleton=true AND import_id=$1 AND source_sha256=$2 AND schema_sha256=$3
        AND phase='loading' AND manifest_sha256 IS NULL
      """, [identity.id, identity.source, identity.schema, digest]) do
      {:ok, %{num_rows: 1}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, :validation_publication_mismatch}
    end
  end
end
