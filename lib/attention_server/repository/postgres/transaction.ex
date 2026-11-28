defmodule AttentionServer.Repository.Postgres.Transaction do
  @moduledoc false

  alias AttentionServer.Repository.Postgres.Repo
  alias Ecto.Adapters.SQL

  @timeout 5_000
  @sql_opts [timeout: @timeout, log: false]

  # Schema inspection is read-only. The installer and explicit migrations
  # own DDL independently of application startup.
  def verify_schema do
    case SQL.query(Repo, "SELECT max(version) FROM harklight.schema_migrations", [], @sql_opts) do
      {:ok, %{rows: [[1]]}} -> :ok
      {:ok, _} -> {:error, :unsupported_schema}
      {:error, _} -> {:error, :schema_unavailable}
    end
  end

  def verify_ready do
    with :ok <- verify_schema(),
         {:ok, %{rows: [[true]]}} <-
           query(
             """
             SELECT EXISTS (SELECT 1 FROM harklight.import_state WHERE phase = 'active')
                AND EXISTS (SELECT 1 FROM harklight.server_identity)
             """,
             []
           ) do
      :ok
    else
      {:error, reason} when reason in [:schema_unavailable, :unsupported_schema] ->
        {:error, reason}

      _ ->
        {:error, :postgres_not_active}
    end
  end

  # The caller provides exact heads after resolving the request's authority
  # and resources. All allocation and effects run on the checked-out primary.
  def run(streams, write) when is_function(write, 1) do
    Repo.transaction(
      fn ->
        with {:ok, _} <- query("SET LOCAL lock_timeout = '2s'", []),
             {:ok, _} <- query("SET LOCAL statement_timeout = '4s'", []),
             {:ok, heads} <- lock_heads(Enum.sort(Enum.uniq(streams))),
             {:ok, result} <- write.(heads) do
          result
        else
          {:error, reason} -> Repo.rollback(reason)
        end
      end,
      @sql_opts
    )
  end

  defp lock_heads(streams) do
    Enum.reduce_while(streams, {:ok, %{}}, fn {person, origin} = scope, {:ok, heads} ->
      case lock_head(person, origin) do
        {:ok, head} -> {:cont, {:ok, Map.put(heads, scope, head)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp lock_head(person, origin) do
    with {:ok, _} <-
           query(
             """
             INSERT INTO harklight.stream_heads (person_id, origin_key)
             VALUES ($1, $2) ON CONFLICT DO NOTHING
             """,
             [person, origin]
           ),
         {:ok, %{rows: [[head]]}} <-
           query(
             """
             SELECT head FROM harklight.stream_heads
             WHERE person_id = $1 AND origin_key = $2 FOR UPDATE
             """,
             [person, origin]
           ) do
      {:ok, head}
    end
  end

  # Append at most one batch per named stream per transaction. A caller
  # reusing a stale head gets an error and rolls back rather than rewinding it.
  def append(heads, {person, origin} = scope, changes)
      when is_list(changes) do
    with true <- Repo.in_transaction?(),
         {:ok, previous_head} <- Map.fetch(heads, scope),
         {:ok, head} <- append_rows(person, origin, previous_head, changes),
         {:ok, %{num_rows: 1}} <-
           query(
             """
             UPDATE harklight.stream_heads SET head = $4
             WHERE person_id = $1 AND origin_key = $2 AND head = $3
             """,
             [person, origin, previous_head, head]
           ) do
      {:ok, head}
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_stream_head}
    end
  end

  defp append_rows(person, origin, head, changes) do
    Enum.reduce_while(changes, {:ok, head}, fn change, {:ok, previous} ->
      revision = previous + 1

      case append_row(person, origin, revision, change) do
        :ok -> {:cont, {:ok, revision}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp append_row(person, origin, revision, change) do
    resource = [person, origin, change.kind, change.resource_id]

    with {:ok, _} <-
           query(
             """
             DELETE FROM harklight.stream_changes AS changes
             USING harklight.stream_resource_revisions AS resources
             WHERE resources.person_id = $1 AND resources.origin_key = $2
               AND resources.resource_kind = $3 AND resources.resource_id = $4
               AND changes.person_id = resources.person_id
               AND changes.origin_key = resources.origin_key
               AND changes.revision = resources.revision
             """,
             resource
           ),
         {:ok, _} <-
           query(
             """
             INSERT INTO harklight.stream_changes
               (person_id, origin_key, resource_kind, resource_id, revision,
                session_handle, created_at_ms)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             """,
             resource ++ [revision, change.session_handle, change.created_at_ms]
           ),
         {:ok, _} <-
           query(
             """
             INSERT INTO harklight.stream_resource_revisions
               (person_id, origin_key, resource_kind, resource_id, revision, first_seen_at_ms)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (person_id, origin_key, resource_kind, resource_id)
             DO UPDATE SET revision = excluded.revision
             """,
             resource ++ [revision, change.first_seen_at_ms]
           ) do
      :ok
    end
  end

  def query(sql, params), do: SQL.query(Repo, sql, params, @sql_opts)
end
