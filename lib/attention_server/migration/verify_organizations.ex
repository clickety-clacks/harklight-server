defmodule AttentionServer.Migration.VerifyOrganizations do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportSession, ImportStatus}
  alias AttentionServer.Repository.Postgres.Attributes

  @tables ~w(hosted_accounts organizations organization_members groups group_members group_invitations)

  def run(db, identity, bundle) do
    with {:ok, manifest, summary} <- Bundle.verify(bundle),
         true <- identity.source == Base.decode16!(summary.sha256, case: :lower),
         {:ok, %{"phase" => phase}} when phase in ["loading", "validated"] <- ImportStatus.read(db, identity),
         {:ok, state} <- Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records,
           %{id: nil, local: nil, counts: Map.new(@tables, &{&1, 0})}, &row(db, &1, &2)),
         true <- is_binary(state.id) and state.id != "",
         :ok <- count(db, "hosted_accounts", state.counts["hosted_accounts"]),
         :ok <- compare(db, :local, state.id,
           "SELECT hosted_ref,attributes FROM harklight.organizations WHERE id=$1",
           [state.id], [nil, Attributes.encode(state.local)]),
         {:ok, local_count} <- local_members(db, bundle, manifest, state),
         counts = state.counts |> Map.update!("organizations", &(&1 + 1))
           |> Map.update!("organization_members", &(&1 + local_count)),
         :ok <- counts(db, counts),
         {:ok, ^manifest, ^summary} <- Bundle.verify(bundle) do
      {:ok, counts}
    else
      false -> {:error, :organization_source_mismatch}
      {:error, _} = error -> error
      _ -> {:error, :organization_verification_requires_staged_import}
    end
  end

  defp row(_, {{:field, :relay_instance_id}, id}, state), do: {:ok, %{state | id: id}}
  defp row(_, {{:row, :local_organizations, :default}, %MapSet{} = members}, state),
    do: {:ok, %{state | local: members}}
  defp row(_, {{:row, :local_organizations, _}, _}, _), do: {:error, :unsupported_local_organization}
  defp row(db, {{:row, :hosted_accounts, ref}, value}, state) do
    with :ok <- compare(db, :hosted_account, ref,
           "SELECT person_id,attributes FROM harklight.hosted_accounts WHERE external_ref=$1",
           [ref], [value.person, Attributes.encode(value)]) do
      {:ok, bump(state, "hosted_accounts", 1)}
    end
  end
  defp row(db, {{:row, :hosted_organizations, ref}, value}, state) do
    with :ok <- compare(db, :organization, ref,
           "SELECT hosted_ref,attributes FROM harklight.organizations WHERE id=$1",
           [value.id], [ref, Attributes.encode(value)]),
         :ok <- each(value.members, fn person -> member(db, value.id, person) end) do
      {:ok, state |> bump("organizations", 1) |> bump("organization_members", MapSet.size(value.members))}
    end
  end
  defp row(db, {{:row, :groups, id}, value}, state) do
    hosted = if is_nil(value.hosted_ref), do: nil, else: elem(value.hosted_ref, 1)
    with :ok <- compare(db, :group, id,
           "SELECT organization_id,manager_id,name,hosted_ref,attributes FROM harklight.groups WHERE id=$1",
           [id], [value.organization_id, value.manager, value.name, hosted, Attributes.encode(value)]),
         :ok <- each(value.members, fn {person, muted} ->
           compare(db, :group_member, {id, person},
             "SELECT muted,attributes FROM harklight.group_members WHERE group_id=$1 AND person_id=$2",
             [id, person], [muted, %{}])
         end),
         :ok <- each(value.invitations, fn {person, invitation} ->
           compare(db, :invitation, invitation,
             "SELECT group_id,person_id,state,attributes FROM harklight.group_invitations WHERE id=$1",
             [invitation], [id, person, "pending", %{}])
         end) do
      {:ok, state |> bump("groups", 1) |> bump("group_members", map_size(value.members))
        |> bump("group_invitations", map_size(value.invitations))}
    end
  end
  defp row(_, _, state), do: {:ok, state}

  # The Hosted table was compared and counted against source before this scan.
  # Scan source owners, not destination principals, for legacy default membership.
  defp local_members(db, bundle, manifest, state) do
    with {:ok, n} <- Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, 0, fn
      {{:row, :owners, person}, _}, n ->
        if is_nil(state.local) or MapSet.member?(state.local, person) do
          case ImportSession.query(db, "SELECT count(*) FROM harklight.hosted_accounts WHERE person_id=$1", [person]) do
            {:ok, %{rows: [[0]]}} ->
              with :ok <- member(db, state.id, person), do: {:ok, n + 1}
            {:ok, %{rows: [[1]]}} -> {:ok, n}
            {:error, _} = error -> error
            _ -> {:error, :organization_source_mismatch}
          end
        else
          {:ok, n}
        end
      _, n -> {:ok, n}
    end) do
      if is_nil(state.local) or MapSet.size(state.local) == n,
        do: {:ok, n}, else: {:error, :invalid_local_organization_membership}
    end
  end

  defp member(db, org, person), do: compare(db, :organization_member, {org, person},
    "SELECT true FROM harklight.organization_members WHERE organization_id=$1 AND person_id=$2",
    [org, person], [true])

  defp bump(state, table, n), do: %{state | counts: Map.update!(state.counts, table, &(&1 + n))}
  defp compare(db, kind, key, sql, params, expected) do
    case ImportSession.query(db, sql, params) do
      {:ok, %{rows: [^expected]}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, {:organization_state_mismatch, kind,
        Base.encode16(:crypto.hash(:sha256, Attributes.key(key)), case: :lower)}}
    end
  end
  defp counts(db, counts), do: each(counts, fn {table, n} -> count(db, table, n) end)
  defp count(db, table, n) do
    case ImportSession.query(db, "SELECT count(*) FROM harklight." <> table, []) do
      {:ok, %{rows: [[^n]]}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, {:organization_count_mismatch, table}}
    end
  end
  defp each(rows, fun), do: Enum.reduce_while(rows, :ok, fn row, :ok ->
    case fun.(row) do
      :ok -> {:cont, :ok}
      error -> {:halt, error}
    end
  end)
end
