defmodule AttentionServer.Repository.Postgres.Operations do
  @moduledoc false

  alias AttentionServer.Repository.Postgres.{Attributes, Repo, Transaction}

  # Run one operation in the checked-out transaction that owns its effects.
  # Reserving the exact unique operation address makes concurrent retries
  # wait for the winning commit or rollback. The reservation is never useful
  # outside that transaction.
  def execute({person, origin, namespace, key}, digest, at_ms, expires_at_ms, write)
      when is_function(write, 0) do
    if Repo.in_transaction?() do
      scope = [person, origin, namespace, key]

      case reserve(scope, digest, at_ms, expires_at_ms) do
        {:ok, %{num_rows: 1}} -> commit_outcome(scope, write.())
        {:ok, %{num_rows: 0}} -> replay(scope, digest)
        {:error, _} = error -> error
      end
    else
      {:error, :transaction_required}
    end
  end

  defp reserve(scope, digest, at_ms, expires_at_ms) do
    Transaction.query(
      """
      INSERT INTO harklight.operation_receipts
        (person_id, origin_key, namespace, operation_key, digest, status,
         response, created_at_ms, expires_at_ms, original_response)
      VALUES ($1, $2, $3, $4, $5, 0, '{}', $6, $7, true)
      ON CONFLICT DO NOTHING
      """,
      scope ++ [digest, at_ms, expires_at_ms]
    )
  end

  defp commit_outcome(scope, {:ok, status, body}) when status in 200..599 do
    case Transaction.query(
           """
           UPDATE harklight.operation_receipts SET status = $5, response = $6
           WHERE person_id = $1 AND origin_key = $2 AND namespace = $3
             AND operation_key = $4 AND status = 0
           """,
           scope ++ [status, Attributes.encode(body)]
         ) do
      {:ok, %{num_rows: 1}} -> {:ok, {status, body, false}}
      {:ok, _} -> Repo.rollback(:receipt_unavailable)
      {:error, reason} -> Repo.rollback(reason)
    end
  end

  # A callback failure must remove the reservation even if a caller would
  # otherwise catch and ignore its returned error.
  defp commit_outcome(_scope, {:error, reason}), do: Repo.rollback(reason)
  defp commit_outcome(_scope, _), do: Repo.rollback(:invalid_operation_result)

  defp replay(scope, digest) do
    with {:ok, %{rows: [[^digest, status, encoded]]}} when status in 200..599 <-
           Transaction.query(
             """
             SELECT digest, status, response FROM harklight.operation_receipts
             WHERE person_id = $1 AND origin_key = $2 AND namespace = $3 AND operation_key = $4
             """,
             scope
           ),
         {:ok, body} <- Attributes.decode(encoded) do
      {:ok, {status, body, true}}
    else
      {:ok, %{rows: [[other, _, _]]}} when other != digest -> {:error, :idempotency_conflict}
      _ -> {:error, :receipt_unavailable}
    end
  end
end
