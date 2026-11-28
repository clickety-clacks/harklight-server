defmodule AttentionServer.Repository.Postgres.OwnerSettings do
  @moduledoc false

  alias AttentionServer.{BounceDecision, BouncePolicy}
  alias AttentionServer.Repository.Postgres.{Attributes, Settings, Transaction}

  def levels(person, key) do
    case Settings.owner_view(person, key) do
      {:ok, view} ->
        {:ok, %{"levels" => BounceDecision.levels_from_configuration(view.attributes[:bounce_levels])}}

      {:error, reason} ->
        failure(reason)
    end
  end

  def save_levels(person, key, rows, at) do
    Transaction.run([], fn _ ->
      with {:ok, view} <- Settings.lock_owner(person, key),
           {:ok, levels} <- BouncePolicy.normalize_levels(rows) do
        generation = %{
          configured_at_ms: at,
          after_delivery_seq: view.accepted_high,
          after_conversation_seq: view.conversation_high
        }

        configuration = BounceDecision.level_configuration(view.attributes[:bounce_levels], levels, generation)
        attributes = Map.put(view.attributes, :bounce_levels, configuration)

        with {:ok, revision} <- Settings.write(person, view, attributes, at),
             :ok <- audit(person, key, revision, at) do
          {:ok, %{"levels" => BouncePolicy.public_levels(configuration.levels)}}
        end
      end
    end)
    |> case do
      {:ok, _} = reply -> reply
      {:error, reason} -> failure(reason)
    end
  end

  defp audit(_person, _key, nil, _at), do: :ok

  defp audit(person, key, revision, at) do
    address = Attributes.key({person, revision})

    body =
      Attributes.encode(%{
        person_id: person,
        actor_id: key,
        action: "dashboard_levels",
        at_ms: at
      })

    with {:ok, _} <-
           Transaction.query(
             "INSERT INTO harklight.compatibility_facts (family, address, body) VALUES ('runtime_audit', $1, $2)",
             [address, body]
           ) do
      :ok
    end
  end

  defp failure(:unauthorized), do: {:error, :forbidden}
  defp failure(:invalid_levels), do: {:error, :invalid_levels}
  defp failure(_), do: {:error, :storage_unavailable}
end
