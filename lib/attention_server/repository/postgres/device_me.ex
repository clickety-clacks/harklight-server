defmodule AttentionServer.Repository.Postgres.DeviceMe do
  @moduledoc false

  alias AttentionServer.ClientAPI.PeopleProfiles
  alias AttentionServer.Repository.Postgres.{Attributes, Certificates, Transaction}

  def read(person, key, mode, profile?) do
    case Transaction.query(
           """
           SELECT identity.instance_id, person.delivery_state, person.profile, hosted.attributes,
                  ARRAY(SELECT organization_id FROM harklight.organization_members
                        WHERE person_id = person.id ORDER BY organization_id COLLATE "C"),
                  ARRAY(SELECT pattern FROM harklight.sender_grants
                        WHERE principal_key = $2 ORDER BY pattern COLLATE "C"),
                  auth.body, roster.body
           FROM harklight.people AS person
           CROSS JOIN harklight.server_identity AS identity
           LEFT JOIN harklight.hosted_accounts AS hosted ON hosted.person_id = person.id
           LEFT JOIN harklight.compatibility_facts AS auth
             ON auth.family = 'harklight_auth_schema' AND auth.address = $3
           LEFT JOIN harklight.compatibility_facts AS roster
             ON roster.family = 'harklight_migration_roster' AND roster.address = $4
           WHERE person.id = $1
           """,
           [person, key, Attributes.key(1), Attributes.key(key)]
         ) do
      {:ok, %{rows: [[instance, delivery, profile, hosted, organizations, senders, auth, roster]]}} ->
        with {:ok, profile} <- person_profile(profile, hosted, profile?) do
          {:ok, %{
            instance: instance,
            paused?: delivery == "STOPPED",
            organizations: organizations,
            sender_names: senders,
            migration?: Certificates.migration_available?(key, person, mode, auth, roster),
            profile: profile
          }}
        end

      _ ->
        {:error, :storage_unavailable}
    end
  end

  defp person_profile(_profile, _hosted, false), do: {:ok, nil}
  defp person_profile(profile, nil, true) when profile == %{},
    do: {:ok, PeopleProfiles.default_profile(nil)}

  defp person_profile(profile, hosted, true) when profile == %{} do
    with {:ok, %{label: label}} <- Attributes.decode(hosted) do
      {:ok, PeopleProfiles.default_profile(label)}
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  defp person_profile(profile, _hosted, true) do
    with {:ok, profile} when is_map(profile) <- Attributes.decode(profile),
         true <- PeopleProfiles.valid?(profile) do
      {:ok, profile}
    else
      _ -> {:error, :storage_unavailable}
    end
  end
end
