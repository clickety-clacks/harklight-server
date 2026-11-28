defmodule AttentionServer.Migration.ImportOrganizations do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportAccounts, ImportRows, ImportSession}
  alias AttentionServer.Repository.Postgres.Attributes
  alias AttentionServer.JSON

  def run(db, identity, root) do
    with :ok <- ImportAccounts.run(db, identity, root),
         {:ok, manifest, _} <- Bundle.verify(root),
         {:ok, local} <- local_members(root, manifest),
         :ok <- phase(db, identity, root, manifest, "hosted_accounts", [:hosted_accounts]),
         :ok <- phase(db, identity, root, manifest, "hosted", [:hosted_organizations]),
         {:ok, _} <-
           ImportRows.run(
             db,
             identity,
             "organizations/local/v1",
             Path.join(root, "records.frames"),
             manifest.data.records,
             &match?({{:field, :relay_instance_id}, _}, &1),
             fn tx, entries ->
               each(entries, fn {{:field, :relay_instance_id}, id} ->
                 local_organization(tx, id, local)
               end)
             end
           ),
         :ok <- phase(db, identity, root, manifest, "groups", [:groups]) do
      :ok
    end
  end

  defp local_members(root, manifest) do
    Frames.fold(Path.join(root, "records.frames"), manifest.data.records, nil, fn
      {{:row, :local_organizations, :default}, %MapSet{} = members}, _ -> {:ok, members}
      {{:row, :local_organizations, _}, _}, _ -> {:error, :unsupported_local_organization}
      _, acc -> {:ok, acc}
    end)
  end

  defp phase(db, identity, root, manifest, name, fields) do
    select = fn
      {{:row, field, _}, _} -> field in fields
      _ -> false
    end

    case ImportRows.run(
           db,
           identity,
           "organizations/" <> name <> "/v1",
           Path.join(root, "records.frames"),
           manifest.data.records,
           select,
           fn tx, entries -> each(entries, &row(tx, &1)) end
         ) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  defp local_organization(db, id, members) do
    with :ok <- organization(db, id, nil, members) do
      if is_nil(members) do
        # Engine.local_organization_defaults/1: only owners, excluding Hosted.
        case ImportSession.query(
               db,
               """
               INSERT INTO harklight.organization_members(organization_id,person_id)
               SELECT $1,p.person_id FROM harklight.principals p
               WHERE p.kind='owner' AND NOT EXISTS
                 (SELECT 1 FROM harklight.hosted_accounts h WHERE h.person_id=p.person_id)
               """,
               [id]
             ) do
          {:ok, _} -> :ok
          error -> error
        end
      else
        each(Enum.sort(members), fn person ->
          execute(
            db,
            """
            INSERT INTO harklight.organization_members(organization_id,person_id)
            SELECT $1,p.person_id FROM harklight.principals p
            WHERE p.kind='owner' AND p.person_id=$2 AND NOT EXISTS
              (SELECT 1 FROM harklight.hosted_accounts h WHERE h.person_id=p.person_id)
            """,
            [id, person]
          )
        end)
      end
    end
  end

  defp row(db, {{:row, :hosted_accounts, ref}, %{person: person} = value}),
    do:
      execute(
        db,
        "INSERT INTO harklight.hosted_accounts(external_ref,person_id,attributes) VALUES ($1,$2,$3::text::jsonb)",
        [ref, person, encode(value)]
      )

  defp row(db, {{:row, :hosted_organizations, ref}, %{id: id, members: %MapSet{} = members} = value}) do
    with :ok <- organization(db, id, ref, value) do
      each(Enum.sort(members), fn person ->
        execute(
          db,
          """
          INSERT INTO harklight.organization_members(organization_id,person_id)
          SELECT $1,person_id FROM harklight.hosted_accounts WHERE person_id=$2
          """,
          [id, person]
        )
      end)
    end
  end

  defp row(db, {{:row, :groups, id}, %{id: id, members: members, invitations: invitations} = value})
       when is_map(members) and is_map(invitations) do
    with {:ok, hosted_ref} <- hosted_ref(db, value),
         :ok <- execute(
           db,
           "INSERT INTO harklight.groups(id,organization_id,manager_id,name,hosted_ref,attributes) VALUES ($1,$2,$3,$4,$5,$6::text::jsonb)",
           [id, value.organization_id, value.manager, value.name, hosted_ref, encode(value)]
         ),
         :ok <- each(Enum.sort(members), fn {person, muted} ->
           execute(
             db,
             """
             INSERT INTO harklight.group_members(group_id,person_id,muted)
             SELECT $1,person_id,$3 FROM harklight.organization_members
             WHERE organization_id=$4 AND person_id=$2
             """,
             [id, person, muted, value.organization_id]
           )
         end) do
      each(Enum.sort(invitations), fn {person, invitation} ->
        execute(
          db,
          """
          INSERT INTO harklight.group_invitations(id,group_id,person_id,state)
          SELECT $1,$2,person_id,'pending' FROM harklight.organization_members
          WHERE organization_id=$4 AND person_id=$3
          """,
          [invitation, id, person, value.organization_id]
        )
      end)
    end
  end

  defp row(_, _), do: {:error, :unsupported_organization_fact}

  defp hosted_ref(db, %{hosted_ref: {business, ref}, manager: nil, organization_id: id}) do
    case ImportSession.query(db, "SELECT hosted_ref FROM harklight.organizations WHERE id=$1", [id]) do
      {:ok, %{rows: [[^business]]}} -> {:ok, ref}
      {:error, _} = error -> error
      _ -> {:error, :group_organization_mismatch}
    end
  end

  defp hosted_ref(db, %{hosted_ref: nil, manager: manager, organization_id: id})
       when is_binary(manager) do
    case ImportSession.query(db, "SELECT instance_id FROM harklight.server_identity", []) do
      {:ok, %{rows: [[^id]]}} -> {:ok, nil}
      {:error, _} = error -> error
      _ -> {:error, :group_organization_mismatch}
    end
  end

  defp hosted_ref(_, _), do: {:error, :invalid_group_custody}

  defp organization(db, id, hosted_ref, attributes),
    do:
      execute(
        db,
        "INSERT INTO harklight.organizations(id,hosted_ref,attributes) VALUES ($1,$2,$3::text::jsonb)",
        [id, hosted_ref, encode(attributes)]
      )

  defp each(entries, fun),
    do: Enum.reduce_while(entries, :ok, fn entry, :ok ->
      case fun.(entry) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)

  defp encode(value), do: JSON.encode!(Attributes.encode(value))

  defp execute(db, sql, params) do
    case ImportSession.query(db, sql, params) do
      {:ok, %{num_rows: 1}} -> :ok
      {:ok, _} -> {:error, :import_row_count_mismatch}
      error -> error
    end
  end
end
