defmodule AttentionServer.Repository.Postgres.Jobs do
  @moduledoc false

  alias AttentionServer.Repository.Postgres.{Attributes, Transaction}

  @max_attempts 5

  # The token belongs to one claim attempt and is allocated by the caller
  # before any retry. No provider work runs on the checked-out transaction.
  def claim(kind, now_ms, until_ms, token)
      when kind in ["push", "reminder"] and is_integer(now_ms) and is_integer(until_ms) and
             until_ms > now_ms and is_binary(token) and token != "" do
    Transaction.run([], fn _ ->
      with :ok <- expire_final_claim(kind, now_ms) do
        claim_available(kind, now_ms, until_ms, token)
      end
    end)
  end

  # Each poll can retire one exhausted job and claim one eligible job.
  defp expire_final_claim(kind, now_ms) do
    case Transaction.query(
           """
           WITH exhausted AS (
             SELECT id FROM harklight.jobs
             WHERE kind = $1 AND completed_at_ms IS NULL AND ready_at_ms <= $2
               AND attempts >= $3 AND (claim_until_ms IS NULL OR claim_until_ms <= $2)
             ORDER BY ready_at_ms, id COLLATE "C"
             FOR UPDATE SKIP LOCKED LIMIT 1
           )
           UPDATE harklight.jobs AS jobs
           SET completed_at_ms = $2, claim_token = NULL, claim_until_ms = NULL
           FROM exhausted WHERE jobs.kind = $1 AND jobs.id = exhausted.id
           RETURNING jobs.id, jobs.attempts
           """,
           [kind, now_ms, @max_attempts]
         ) do
      {:ok, %{rows: [[id, attempts]]}} ->
        record_exhaustion(kind, id, attempts, now_ms, :lease_expired)

      {:ok, %{rows: []}} ->
        :ok

      {:error, _} = error ->
        error
    end
  end

  defp claim_available(kind, now_ms, until_ms, token) do
    case Transaction.query(
           """
           WITH candidate AS (
             SELECT id FROM harklight.jobs
             WHERE kind = $1 AND completed_at_ms IS NULL AND ready_at_ms <= $2
               AND attempts < $5 AND (claim_until_ms IS NULL OR claim_until_ms <= $2)
             ORDER BY ready_at_ms, id COLLATE "C"
             FOR UPDATE SKIP LOCKED LIMIT 1
           )
           UPDATE harklight.jobs AS jobs
           SET claim_token = $3, claim_until_ms = $4, attempts = jobs.attempts + 1
           FROM candidate WHERE jobs.kind = $1 AND jobs.id = candidate.id
           RETURNING jobs.id, jobs.person_id, jobs.attempts, jobs.body
           """,
           [kind, now_ms, token, until_ms, @max_attempts]
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
  end

  def complete(%{kind: kind, id: id, token: token}, now_ms) do
    resolve(kind, id, token, now_ms, nil)
  end

  def retry_at(%{kind: kind, id: id, token: token}, now_ms, ready_at_ms)
      when is_integer(ready_at_ms) and ready_at_ms >= now_ms do
    resolve(kind, id, token, now_ms, ready_at_ms)
  end

  # Lease expiry allows another claim. The current token can still record its
  # result until another transaction replaces it or terminalizes the job.
  defp resolve(kind, id, token, now_ms, ready_at_ms) do
    Transaction.run([], fn _ ->
      case Transaction.query(
             """
             UPDATE harklight.jobs
             SET completed_at_ms = CASE WHEN $5::bigint IS NULL OR attempts >= $6 THEN $4::bigint END,
                 ready_at_ms = CASE WHEN attempts < $6 THEN coalesce($5, ready_at_ms)
                                   ELSE ready_at_ms END,
                 claim_token = NULL, claim_until_ms = NULL
             WHERE kind = $1 AND id = $2 AND claim_token = $3 AND completed_at_ms IS NULL
             RETURNING attempts
             """,
             [kind, id, token, now_ms, ready_at_ms, @max_attempts]
           ) do
        {:ok, %{rows: [[attempts]]}} when attempts >= @max_attempts and ready_at_ms != nil ->
          with :ok <- record_exhaustion(kind, id, attempts, now_ms, :retry_exhausted) do
            {:ok, :exhausted}
          end

        {:ok, %{rows: [[_attempts]]}} ->
          {:ok, :recorded}

        {:ok, %{rows: []}} ->
          {:error, :claim_lost}

        {:error, _} = error ->
          error
      end
    end)
  end

  # Keep the v1 job body intact. This typed fact distinguishes exhaustion
  # from success and commits with the job's terminal timestamp.
  defp record_exhaustion(kind, id, attempts, now_ms, reason) do
    body = Attributes.encode(%{attempts: attempts, at_ms: now_ms, reason: reason})

    with {:ok, _} <-
           Transaction.query(
             """
             INSERT INTO harklight.compatibility_facts (family, address, body)
             VALUES ('job_exhaustion', $1, $2)
             """,
             [Attributes.key({kind, id}), body]
           ) do
      :ok
    end
  end
end
