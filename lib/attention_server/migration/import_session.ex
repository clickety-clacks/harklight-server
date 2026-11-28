defmodule AttentionServer.Migration.ImportSession do
  @moduledoc false

  alias AttentionServer.Repository.Postgres.Schema

  @query_opts [timeout: 15_000]
  @empty_digest :crypto.hash(:sha256, <<>>)

  # Caller opens an explicit migrator connection to a new unpublished database
  # and applies the pinned schema beforehand. This module never runs DDL or
  # starts the runtime pool, and never transitions an import out of loading.
  def claim(connection, %{id: id, source: source, schema: schema} = identity)
      when is_binary(id) and byte_size(id) > 0 and byte_size(source) == 32 and
             byte_size(schema) == 32 do
    version = Schema.version()

    transaction(connection, fn db ->
      with {:ok, _} <- query(db, "SELECT pg_advisory_xact_lock(1212240455, 1)", []),
           {:ok, %{rows: [[^version]]}} <-
             query(db, "SELECT max(version) FROM harklight.schema_migrations", []),
           {:ok, %{rows: rows}} <- state(db) do
        case rows do
          [] ->
            with :ok <- empty_destination(db),
                 {:ok, _} <-
                   query(
                     db,
                     """
                     INSERT INTO harklight.import_state (import_id, source_sha256, schema_sha256, phase)
                     VALUES ($1, $2, $3, 'loading')
                     """,
                     [id, source, schema]
                   ) do
              {:ok, :claimed}
            end

          _ ->
            with :ok <- owns_loading(rows, identity), do: {:ok, :resumed}
        end
      else
        {:error, _} = error -> error
        _ -> {:error, :unsupported_schema}
      end
    end)
  end

  def claim(_, _), do: {:error, :invalid_import_identity}

  def checkpoint(connection, identity, family) do
    transaction(connection, fn db ->
      with {:ok, %{rows: rows}} <- state(db),
           :ok <- owns_loading(rows, identity),
           {:ok, checkpoint} <- read_checkpoint(db, identity.id, family) do
        {:ok, checkpoint}
      end
    end)
  end

  # Row writes and checkpoint advancement are one commit. A caller restarting
  # after an ambiguous connection loss reads the checkpoint and resumes there.
  # The callback performs database writes only; it must have no external effects.
  def batch(connection, identity, family, expected, entries, write)
      when is_binary(family) and is_list(entries) and length(entries) in 1..256 and
             is_function(write, 2) do
    transaction(connection, fn db ->
      with {:ok, %{rows: rows}} <- state(db),
           :ok <- owns_loading(rows, identity),
           {:ok, current} <- read_checkpoint(db, identity.id, family),
           true <- current == expected,
           :ok <- write.(db, entries),
           next = advance(current, entries),
           {:ok, _} <-
             query(
               db,
               """
               INSERT INTO harklight.import_checkpoints (import_id, family, last_ordinal, cumulative_sha256)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (import_id, family) DO UPDATE
               SET last_ordinal = EXCLUDED.last_ordinal, cumulative_sha256 = EXCLUDED.cumulative_sha256
               """,
               [identity.id, family, next.ordinal, next.digest]
             ) do
        {:ok, next}
      else
        false -> {:error, :checkpoint_changed}
        {:error, _} = error -> error
        _ -> {:error, :invalid_import_batch}
      end
    end)
  end

  def batch(_, _, _, _, _, _), do: {:error, :invalid_import_batch}

  def advance(%{ordinal: ordinal, digest: digest}, entries) do
    Enum.reduce(entries, %{ordinal: ordinal, digest: digest}, fn entry, current ->
      bytes = :erlang.term_to_binary(entry, [:deterministic, {:minor_version, 2}])

      %{
        ordinal: current.ordinal + 1,
        digest: :crypto.hash(:sha256, [current.digest, <<byte_size(bytes)::64>>, bytes])
      }
    end)
  end

  def query(db, sql, params), do: Postgrex.query(db, sql, params, @query_opts)

  defp transaction(connection, operation) do
    Postgrex.transaction(
      connection,
      fn db ->
        with {:ok, _} <- query(db, "SET LOCAL lock_timeout = '5s'", []),
             {:ok, _} <- query(db, "SET LOCAL statement_timeout = '15s'", []),
             {:ok, value} <- operation.(db) do
          value
        else
          {:error, reason} -> Postgrex.rollback(db, reason)
        end
      end,
      @query_opts
    )
  end

  defp state(db),
    do:
      query(
        db,
        """
        SELECT import_id, source_sha256, schema_sha256, phase
        FROM harklight.import_state WHERE singleton = true FOR UPDATE
        """,
        []
      )

  defp owns_loading([[id, source, schema, "loading"]], %{id: id, source: source, schema: schema}),
    do: :ok

  defp owns_loading([[_, _, _, phase]], _) when phase != "loading",
    do: {:error, :import_not_loading}

  defp owns_loading(_, _), do: {:error, :import_identity_mismatch}

  defp read_checkpoint(db, id, family) do
    case query(
           db,
           """
           SELECT last_ordinal, cumulative_sha256 FROM harklight.import_checkpoints
           WHERE import_id = $1 AND family = $2
           """,
           [id, family]
         ) do
      {:ok, %{rows: []}} -> {:ok, %{ordinal: 0, digest: @empty_digest}}
      {:ok, %{rows: [[ordinal, digest]]}} -> {:ok, %{ordinal: ordinal, digest: digest}}
      {:error, _} = error -> error
    end
  end

  defp empty_destination(db) do
    with {:ok, %{rows: tables}} <-
           query(
             db,
             """
             SELECT schemaname, tablename FROM pg_catalog.pg_tables
             WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
             ORDER BY schemaname, tablename
             """,
             []
           ) do
      Enum.reduce_while(tables, :ok, fn
        ["harklight", "schema_migrations"], :ok ->
          {:cont, :ok}

        ["harklight", table], :ok ->
          sql = "SELECT EXISTS (SELECT 1 FROM harklight." <> quote_name(table) <> ")"

          case query(db, sql, []) do
            {:ok, %{rows: [[false]]}} -> {:cont, :ok}
            {:ok, _} -> {:halt, {:error, :destination_not_empty}}
            error -> {:halt, error}
          end

        _, :ok ->
          {:halt, {:error, :destination_not_empty}}
      end)
    end
  end

  defp quote_name(value), do: "\"" <> String.replace(value, "\"", "\"\"") <> "\""
end
