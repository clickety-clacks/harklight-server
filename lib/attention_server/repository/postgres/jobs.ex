defmodule AttentionServer.Repository.Postgres.Jobs do
  @moduledoc false

  alias AttentionServer.Repository.Postgres.{Attributes, Transaction}

  # The token belongs to one claim attempt and is allocated by the caller
  # before any retry. No provider work runs on the checked-out transaction.
  def claim(kind, now_ms, until_ms, token)
      when kind in ["push", "reminder"] and is_integer(now_ms) and is_integer(until_ms) and
             until_ms > now_ms and is_binary(token) and token != "" do
    Transaction.run([], fn _ ->
      case Transaction.query(
             """
             WITH candidate AS (
               SELECT id FROM harklight.jobs
               WHERE kind = $1 AND completed_at_ms IS NULL AND ready_at_ms <= $2
                 AND (claim_until_ms IS NULL OR claim_until_ms <= $2)
               ORDER BY ready_at_ms, id COLLATE "C"
               FOR UPDATE SKIP LOCKED LIMIT 1
             )
             UPDATE harklight.jobs AS jobs
             SET claim_token = $3, claim_until_ms = $4, attempts = jobs.attempts + 1
             FROM candidate WHERE jobs.kind = $1 AND jobs.id = candidate.id
             RETURNING jobs.id, jobs.person_id, jobs.attempts, jobs.body
             """,
             [kind, now_ms, token, until_ms]
           ) do
        {:ok, %{rows: [[id, person, attempts, encoded]]}} ->
          with {:ok, body} <- Attributes.decode(encoded) do
            {:ok, %{kind: kind, id: id, person: person, attempts: attempts, token: token, body: body}}
          end

        {:ok, %{rows: []}} ->
          {:ok, nil}

        {:error, _} = error ->
          error
      end
    end)
  end

  def complete(%{kind: kind, id: id, token: token}, now_ms) do
    resolve(kind, id, token, now_ms, now_ms, nil)
  end

  def retry_at(%{kind: kind, id: id, token: token}, now_ms, ready_at_ms)
      when is_integer(ready_at_ms) and ready_at_ms >= now_ms do
    resolve(kind, id, token, now_ms, nil, ready_at_ms)
  end

  defp resolve(kind, id, token, now_ms, completed_at_ms, ready_at_ms) do
    Transaction.run([], fn _ ->
      case Transaction.query(
             """
             UPDATE harklight.jobs
             SET completed_at_ms = $5, ready_at_ms = coalesce($6, ready_at_ms),
                 claim_token = NULL, claim_until_ms = NULL
             WHERE kind = $1 AND id = $2 AND claim_token = $3 AND claim_until_ms > $4
               AND completed_at_ms IS NULL
             """,
             [kind, id, token, now_ms, completed_at_ms, ready_at_ms]
           ) do
        {:ok, %{num_rows: 1}} -> {:ok, :recorded}
        {:ok, %{num_rows: 0}} -> {:error, :claim_lost}
        {:error, _} = error -> error
      end
    end)
  end
end
