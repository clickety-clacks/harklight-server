defmodule AttentionServer.Migration.VerifyAuthority do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportSession, VerifyFacts}
  alias AttentionServer.Repository.Postgres.Attributes

  # Compare normalized authority without changing credentials, grants or lifecycle.
  def run(db, identity, bundle) do
    with {:ok, _} <- VerifyFacts.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, counts} <- Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records,
           %{principals: 0, grants: 0, sessions: 0}, fn entry, counts ->
             verify(db, entry, counts)
           end),
         :ok <- counts(db, counts) do
      {:ok, counts}
    end
  end

  defp verify(db, {{:row, :owners, person}, value}, counts) do
    principal(db, value.key, person, "owner", value.revision, false, value, counts)
  end
  defp verify(db, {{:row, field, key}, value}, counts) when field in [:devices, :agents] do
    principal(db, key, value.person, if(field == :devices, do: "device", else: "agent"),
      value.enrollment_revision, value.revoked, value, counts)
  end
  defp verify(db, {{:row, :sender_grants, {person, key}}, patterns}, counts) do
    Enum.reduce_while(patterns, {:ok, counts}, fn pattern, {:ok, acc} ->
      case ImportSession.query(db,
        "SELECT p.person_id FROM harklight.sender_grants g JOIN harklight.principals p ON p.key=g.principal_key WHERE g.principal_key=$1 AND g.pattern=$2",
        [key, pattern]) do
        {:ok, %{rows: [[^person]]}} -> {:cont, {:ok, Map.update!(acc, :grants, &(&1 + 1))}}
        {:error, _} = error -> {:halt, error}
        _ -> {:halt, mismatch(:grant, {person, key, pattern})}
      end
    end)
  end
  defp verify(db, {{:row, :origin_sessions, {person, origin, handle} = key}, value}, counts) do
    expected = Attributes.encode(value)
    case ImportSession.query(db,
      "SELECT attributes FROM harklight.origin_sessions WHERE person_id=$1 AND origin_key=$2 AND handle=$3",
      [person, origin, handle]) do
      {:ok, %{rows: [[^expected]]}} -> {:ok, Map.update!(counts, :sessions, &(&1 + 1))}
      {:error, _} = error -> error
      _ -> mismatch(:origin_session, key)
    end
  end
  defp verify(_, _, counts), do: {:ok, counts}

  defp principal(db, key, person, kind, revision, revoked, value, counts) do
    expected = [person, kind, revision, revoked, Attributes.encode(value)]
    case ImportSession.query(db,
      "SELECT person_id,kind,revision,revoked,attributes FROM harklight.principals WHERE key=$1", [key]) do
      {:ok, %{rows: [^expected]}} -> {:ok, Map.update!(counts, :principals, &(&1 + 1))}
      {:error, _} = error -> error
      _ -> mismatch(:principal, key)
    end
  end

  defp counts(db, counts) do
    Enum.reduce_while([principals: "principals", grants: "sender_grants", sessions: "origin_sessions"],
      :ok, fn {field, table}, :ok ->
        expected = Map.fetch!(counts, field)
        case ImportSession.query(db, "SELECT count(*) FROM harklight." <> table, []) do
          {:ok, %{rows: [[^expected]]}} -> {:cont, :ok}
          {:error, _} = error -> {:halt, error}
          _ -> {:halt, {:error, {:authority_count_mismatch, field}}}
        end
      end)
  end

  defp mismatch(field, address), do: {:error, {:authority_mismatch, field,
    Base.encode16(:crypto.hash(:sha256, Attributes.key(address)), case: :lower)}}
end
