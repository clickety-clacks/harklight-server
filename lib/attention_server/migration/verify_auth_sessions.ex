defmodule AttentionServer.Migration.VerifyAuthSessions do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportSession, SourceTables, VerifyAuthority}
  alias AttentionServer.Repository.Postgres.Attributes

  def run(db, identity, bundle) do
    with {:ok, _} <- VerifyAuthority.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, count} <- tables(db, bundle, manifest),
         {:ok, %{rows: [[^count]]}} <- ImportSession.query(db,
           "SELECT count(*) FROM harklight.auth_sessions", []) do
      {:ok, %{auth_sessions: count}}
    else
      {:error, _} = error -> error
      _ -> {:error, :auth_session_count_mismatch}
    end
  end

  defp tables(db, bundle, manifest) do
    Enum.reduce_while(~w(harklight_browser_sessions harklight_device_sessions), {:ok, 0},
      fn table, {:ok, count} ->
        result = case Map.fetch(manifest.data.tables, table) do
          :error -> {:ok, count}
          {:ok, source} -> Frames.fold(Path.join(bundle, source.file), source.summary, count,
            fn values, acc ->
              with {:ok, _, row} <- SourceTables.fact(table, values),
                   {:ok, n} <- session(db, table, row), do: {:ok, acc + n}
            end)
        end
        case result do
          {:ok, _} -> {:cont, result}
          error -> {:halt, error}
        end
      end)
  end

  defp session(db, "harklight_browser_sessions", row) do
    case row["session_hash"] do
      hash when is_binary(hash) and byte_size(hash) == 32 ->
        compare(db, hash, row["person_id"], row["owner_key"], "browser", "owner",
          row["owner_revision"], row)
      _ -> {:error, :invalid_browser_session_hash}
    end
  end
  defp session(db, "harklight_device_sessions", row) do
    with hash when is_binary(hash) <- row["session_hash"],
         {:ok, bytes} when byte_size(bytes) == 32 <- Base.decode16(hash, case: :lower),
         pending when pending in [0, 1] <- row["pending"] do
      compare(db, bytes, row["person_id"], row["device_id"], "native", "device", nil,
        %{pending: pending == 1, public_key: row["public_key"], signed_in_at_ms: row["signed_in_at_ms"]})
    else
      _ -> {:error, :invalid_native_session}
    end
  end

  # VerifyAuthority already proved this principal projection against the source.
  # Revocation does not erase historical sessions; runtime authentication applies
  # its own revocation/revision predicates. Orphans remain raw facts only.
  defp compare(db, hash, person, key, kind, principal_kind, revision, attributes) do
    with {:ok, %{rows: principal}} <- ImportSession.query(db,
           "SELECT revision FROM harklight.principals WHERE person_id=$1 AND key=$2 AND kind=$3",
           [person, key, principal_kind]),
         {:ok, %{rows: actual}} <- ImportSession.query(db,
           "SELECT person_id,principal_key,kind,principal_revision,expires_at_ms,attributes FROM harklight.auth_sessions WHERE token_hash=$1", [hash]) do
      expected = case principal do
        [] -> []
        [[source_revision]] -> [[person, key, kind, revision || source_revision, nil, Attributes.encode(attributes)]]
      end
      if actual === expected, do: {:ok, length(expected)}, else: mismatch(hash)
    end
  end

  defp mismatch(hash), do: {:error, {:auth_session_mismatch,
    Base.encode16(:crypto.hash(:sha256, hash), case: :lower)}}
end
