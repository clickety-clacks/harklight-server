defmodule AttentionServer.Migration.ImportAuth do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, ImportAccounts, ImportRows, ImportSession, ImportTables, SourceTables}
  alias AttentionServer.Repository.Postgres.Attributes
  alias AttentionServer.JSON

  @tables ~w(harklight_browser_sessions harklight_device_sessions)

  def run(db, identity, root) do
    with :ok <- ImportAccounts.run(db, identity, root),
         :ok <- ImportTables.run(db, identity, root),
         {:ok, manifest, _} <- Bundle.verify(root) do
      Enum.reduce_while(@tables, :ok, fn table, :ok ->
        case Map.fetch(manifest.data.tables, table) do
          :error -> {:cont, :ok}
          {:ok, source} ->
            case ImportRows.run(
                   db, identity, "auth/" <> table <> "/v1",
                   Path.join(root, source.file), source.summary,
                   fn _ -> true end, &write(&1, table, &2)
                 ) do
              {:ok, _} -> {:cont, :ok}
              error -> {:halt, error}
            end
        end
      end)
    end
  end

  defp write(db, table, rows) do
    Enum.reduce_while(rows, :ok, fn values, :ok ->
      with {:ok, _, row} <- SourceTables.fact(table, values),
           :ok <- session(db, table, row) do
        {:cont, :ok}
      else
        error -> {:halt, error}
      end
    end)
  end

  defp session(db, "harklight_browser_sessions", row) do
    case row["session_hash"] do
      hash when is_binary(hash) and byte_size(hash) == 32 ->
        insert(db, hash, row["person_id"], row["owner_key"], "browser", "owner",
          row["owner_revision"], row)
      _ -> {:error, :invalid_browser_session_hash}
    end
  end

  defp session(db, "harklight_device_sessions", row) do
    with hash when is_binary(hash) <- row["session_hash"],
         {:ok, bytes} when byte_size(bytes) == 32 <- Base.decode16(hash, case: :lower),
         pending when pending in [0, 1] <- row["pending"] do
      insert(db, bytes, row["person_id"], row["device_id"], "native", "device", nil,
        %{pending: pending == 1, public_key: row["public_key"], signed_in_at_ms: row["signed_in_at_ms"]})
    else
      _ -> {:error, :invalid_native_session}
    end
  end

  defp insert(db, hash, person, key, kind, principal_kind, revision, attributes) do
    # Keep sessions tied to existing principals even when currently revoked or
    # at another revision. Runtime predicates decide whether they authenticate.
    # Orphaned historical sessions remain in exact SQL facts; invent no authority.
    case ImportSession.query(
           db,
           """
           INSERT INTO harklight.auth_sessions
             (token_hash,person_id,principal_key,kind,principal_revision,expires_at_ms,attributes)
           SELECT $1,p.person_id,p.key,$4,COALESCE($6::bigint,p.revision),NULL,$7::text::jsonb
           FROM harklight.principals p
           WHERE p.person_id=$2 AND p.key=$3 AND p.kind=$5
           """,
           [hash, person, key, kind, principal_kind, revision, JSON.encode!(Attributes.encode(attributes))]
         ) do
      {:ok, %{num_rows: count}} when count in [0, 1] -> :ok
      {:ok, _} -> {:error, :import_row_count_mismatch}
      error -> error
    end
  end
end
