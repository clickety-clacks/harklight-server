defmodule AttentionServer.Repository.Postgres.Certificates do
  @moduledoc false

  alias AttentionServer.Repository.Postgres.{Attributes, Transaction}
  alias AttentionServer.Store.AuthSchema

  def resolve(key, mode) when mode in [:self_hosted, :saas] do
    case Transaction.query(
           """
           SELECT principal.person_id, principal.kind, auth.body, roster.body,
                  EXISTS (
                    SELECT 1 FROM harklight.sender_grants WHERE principal_key = principal.key
                  ),
                  EXISTS (
                    SELECT 1 FROM harklight.auth_sessions
                    WHERE person_id = principal.person_id AND principal_key = principal.key AND kind = 'native'
                  )
           FROM harklight.principals AS principal
           LEFT JOIN harklight.compatibility_facts AS auth
             ON auth.family = 'harklight_auth_schema' AND auth.address = $2
           LEFT JOIN harklight.compatibility_facts AS roster
             ON roster.family = 'harklight_migration_roster' AND roster.address = $3
           WHERE principal.key = $1 AND NOT principal.revoked
           """,
           [key, Attributes.key(1), Attributes.key(key)]
         ) do
      {:ok, %{rows: [[person, "agent" | _]]}} ->
        {:ok, person, :agent}

      {:ok, %{rows: [[person, "device", auth, roster, sender?, native?]]}} ->
        device(key, person, mode, auth, roster, sender?, native?)

      {:ok, _} ->
        {:error, :unauthorized}

      {:error, _} ->
        {:error, :storage_unavailable}
    end
  end

  # FORMAT32 predates the auth generation on some stores. On those stores
  # an enrolled device certificate needs no roster or sender grant.
  defp device(_key, person, _mode, nil, _roster, _sender?, _native?), do: {:ok, person, :device}

  defp device(key, person, mode, auth, roster, sender?, native?) do
    generation = AuthSchema.supported_generation()

    with {:ok, [1, ^generation, disabled]} when disabled in [0, 1] <- Attributes.decode(auth),
         {:ok, migration} <- roster_row(roster, key) do
      if eligible?(migration, person, mode, disabled, sender?, native?) do
        {:ok, person, :device}
      else
        {:error, :unauthorized}
      end
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  defp roster_row(nil, _key), do: {:ok, nil}

  defp roster_row(encoded, key) do
    with {:ok, [^key, person, phase]}
         when is_binary(person) and phase in ["legacy", "pending", "activated", "removed"] <-
           Attributes.decode(encoded) do
      {:ok, {person, phase}}
    else
      _ -> {:error, :invalid_roster}
    end
  end

  defp eligible?(nil, _person, _mode, _disabled, sender?, native?), do: sender? and not native?

  defp eligible?({person, phase}, person, mode, disabled, _sender?, _native?),
    do: phase in ["legacy", "pending"] and mode == :self_hosted and disabled == 0

  defp eligible?(_roster, _person, _mode, _disabled, _sender?, _native?), do: false
end
