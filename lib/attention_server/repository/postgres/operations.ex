defmodule AttentionServer.Repository.Postgres.Operations do
  @moduledoc false

  alias AttentionServer.ClientAPI.{Idempotency, Writes}
  alias AttentionServer.Repository.Postgres.{Attributes, Repo, Transaction}

  @retained "harklight_app_receipts"
  @recent "client_idempotency"

  # Run one operation in the checked-out transaction that owns its effects.
  # Reserving the exact unique operation address makes concurrent retries
  # wait for the winning commit or rollback. The reservation is never useful
  # outside that transaction.
  def execute(scope, digest, at_ms, expires_at_ms, write) when is_function(write, 0) do
    with {:ok, {status, body, true}, replayed?} <-
           execute_receipt(scope, digest, at_ms, expires_at_ms, true, write) do
      {:ok, {status, body, replayed?}}
    else
      {:ok, _, _} -> {:error, :receipt_unavailable}
      {:error, _} = error -> error
    end
  end

  # The caller authorizes the operation and provides a scoped committed
  # resource projection on this same transaction. Person clients share a key
  # space. Agent keys are scoped to their origin and only use recent receipts.
  def execute_client({person, origin, key}, digest, at_ms, write, project)
      when is_function(write, 0) and is_function(project, 1) do
    if Repo.in_transaction?() do
      case retained(person, origin, key, digest, at_ms, project) do
        {:ok, nil} -> recent({person, origin, @recent, key}, digest, at_ms, write, project)
        result -> result
      end
    else
      {:error, :transaction_required}
    end
  end

  defp recent(scope, digest, at_ms, write, project) do
    expires_at_ms = at_ms + Idempotency.retention_ms()

    with {:ok, {status, evidence, false}, replayed?} <-
           execute_receipt(scope, digest, at_ms, expires_at_ms, false, write),
         {:replay, ^status, body} <- Writes.render_receipt(evidence, status, :recent, project) do
      {:ok, {status, body, replayed?}}
    else
      {:ok, _, _} -> {:error, :receipt_unavailable}
      {:error, _} = error -> error
    end
  end

  defp retained(person, "", key, digest, at_ms, project) do
    case lookup([person, "", @retained, key], digest, at_ms) do
      {:ok, {status, _body, true}} ->
        with {:ok, encoded} <- original_body(person, key, digest, status) do
          {:ok, {status, {:encoded_json, encoded}, true}}
        end

      {:ok, {status, evidence, false}} ->
        with {:replay, status, body} <- Writes.render_receipt(evidence, status, :retained, project) do
          {:ok, {status, body, true}}
        end

      other ->
        other
    end
  end

  defp retained(_person, _origin, _key, _digest, _at_ms, _project), do: {:ok, nil}

  defp original_body(person, key, digest, status) do
    address = Attributes.key({person, key})
    source_digest = Base.encode16(digest, case: :lower)

    with {:ok, %{rows: [[encoded]]}} <-
           Transaction.query(
             "SELECT body FROM harklight.compatibility_facts WHERE family = $1 AND address = $2",
             [@retained, address]
           ),
         {:ok, [^person, ^key, ^source_digest, ^status, raw, _at, 1]} when is_binary(raw) <-
           Attributes.decode(encoded) do
      {:ok, raw}
    else
      _ -> {:error, :receipt_unavailable}
    end
  end

  defp execute_receipt(scope, digest, at_ms, expires_at_ms, original?, write) do
    if Repo.in_transaction?() do
      scope = Tuple.to_list(scope)

      case reserve(scope, digest, at_ms, expires_at_ms, original?) do
        {:ok, %{num_rows: 1}} -> commit_outcome(scope, write.(), original?)
        {:ok, %{num_rows: 0}} -> replay(scope, digest, at_ms)
        {:error, _} = error -> error
      end
    else
      {:error, :transaction_required}
    end
  end

  defp reserve(scope, digest, at_ms, expires_at_ms, original_response) do
    Transaction.query(
      """
      INSERT INTO harklight.operation_receipts
        (person_id, origin_key, namespace, operation_key, digest, status,
         response, created_at_ms, expires_at_ms, original_response)
      VALUES ($1, $2, $3, $4, $5, 0, '{}', $6, $7, $8)
      ON CONFLICT (person_id, origin_key, namespace, operation_key) DO UPDATE
      SET digest = excluded.digest, status = excluded.status, response = excluded.response,
          created_at_ms = excluded.created_at_ms, expires_at_ms = excluded.expires_at_ms,
          original_response = excluded.original_response
      WHERE operation_receipts.expires_at_ms < excluded.created_at_ms
      """,
      scope ++ [digest, at_ms, expires_at_ms, original_response]
    )
  end

  defp commit_outcome(scope, {:ok, status, body}, original?) when status in 200..599 do
    case Transaction.query(
           """
           UPDATE harklight.operation_receipts SET status = $5, response = $6
           WHERE person_id = $1 AND origin_key = $2 AND namespace = $3
             AND operation_key = $4 AND status = 0
           """,
           scope ++ [status, Attributes.encode(body)]
         ) do
      {:ok, %{num_rows: 1}} -> {:ok, {status, body, original?}, false}
      {:ok, _} -> Repo.rollback(:receipt_unavailable)
      {:error, reason} -> Repo.rollback(reason)
    end
  end

  # A callback failure must remove the reservation even if a caller would
  # otherwise catch and ignore its returned error.
  defp commit_outcome(_scope, {:error, reason}, _original?), do: Repo.rollback(reason)
  defp commit_outcome(_scope, _, _original?), do: Repo.rollback(:invalid_operation_result)

  defp replay(scope, digest, at_ms) do
    case lookup(scope, digest, at_ms) do
      {:ok, {_status, _body, _original?} = receipt} -> {:ok, receipt, true}
      {:ok, nil} -> {:error, :receipt_unavailable}
      {:error, _} = error -> error
    end
  end

  defp lookup(scope, digest, at_ms) do
    case Transaction.query(
           """
           SELECT digest, status, response, original_response FROM harklight.operation_receipts
           WHERE person_id = $1 AND origin_key = $2 AND namespace = $3 AND operation_key = $4
             AND (expires_at_ms IS NULL OR expires_at_ms >= $5)
           """,
           scope ++ [at_ms]
         ) do
      {:ok, %{rows: [[^digest, status, encoded, original?]]}} when status in 200..599 ->
        with {:ok, body} <- Attributes.decode(encoded) do
          {:ok, {status, body, original?}}
        else
          _ -> {:error, :receipt_unavailable}
        end

      {:ok, %{rows: [[other, _, _, _]]}} when other != digest ->
        {:error, :idempotency_conflict}

      {:ok, %{rows: []}} ->
        {:ok, nil}

      _ ->
        {:error, :receipt_unavailable}
    end
  end
end
