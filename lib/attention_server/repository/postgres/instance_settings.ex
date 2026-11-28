defmodule AttentionServer.Repository.Postgres.InstanceSettings do
  @moduledoc false

  alias AttentionServer.JevClient
  alias AttentionServer.Repository.Postgres.{Attributes, Transaction}

  # This is the durable part of the dashboard command. The caller resolves
  # runtime placement and provider status after a committed result.
  def save(_authority, {algorithm, _endpoint, _model}, _at)
      when algorithm not in ["baseline", "jev-set"],
      do: {:error, :unknown_algorithm}

  def save(
        %{deployment_mode: :self_hosted, configured_owner_person: person, person: person, key: key},
        {algorithm, endpoint, model},
        at
      ) do
    operation = Ecto.UUID.generate()

    Transaction.run([], fn _ ->
      with :ok <- lock_instance(person, key),
           {:ok, endpoint} <- JevClient.normalize_endpoint(endpoint),
           {:ok, model} <- JevClient.normalize_model(model),
           {:ok, current} <- read(person),
           :ok <- update(person, key, current, algorithm, endpoint, model, operation, at) do
        {:ok, %{person_id: person, algorithm_id: algorithm, endpoint: endpoint || "", model: model || ""}}
      end
    end)
    |> result()
  end

  def save(_authority, _settings, _at), do: {:error, :forbidden}

  # The instance row serializes edits to the instance-wide connection. The
  # affected person's row also serializes writes to its settings envelope.
  defp lock_instance(person, key) do
    case Transaction.query(
           """
           WITH authority AS MATERIALIZED (
             SELECT person_id FROM harklight.principals
             WHERE person_id = $1 AND key = $2 AND kind = 'owner' AND NOT revoked
             FOR SHARE
           )
           SELECT person.id FROM authority
           JOIN harklight.people AS person ON person.id = authority.person_id
           CROSS JOIN harklight.server_identity AS instance
           FOR NO KEY UPDATE OF person, instance
           """,
           [person, key]
         ) do
      {:ok, %{rows: [[^person]]}} -> :ok
      {:ok, %{rows: []}} -> {:error, :forbidden}
      {:error, _} = error -> error
    end
  end

  defp read(person) do
    with {:ok, %{rows: [[attrs, endpoint, model]]}} <-
           Transaction.query(
             """
             SELECT person.settings, endpoint.body, model.body
             FROM harklight.people AS person
             LEFT JOIN harklight.compatibility_facts AS endpoint
               ON endpoint.family = 'jev_endpoint' AND endpoint.address = $2
             LEFT JOIN harklight.compatibility_facts AS model
               ON model.family = 'jev_model' AND model.address = $3
             WHERE person.id = $1
             """,
             [person, scalar_address(:jev_endpoint), scalar_address(:jev_model)]
           ),
         {:ok, settings} when is_map(settings) <- decode_settings(attrs),
         {:ok, endpoint} <- decode_scalar(endpoint),
         {:ok, model} <- decode_scalar(model) do
      {:ok, %{settings: settings, endpoint: endpoint, model: model}}
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  defp update(person, key, current, algorithm, endpoint, model, operation, at) do
    settings = current.settings

    if Map.get(settings, :algorithm_choices, "baseline") == algorithm and
         current.endpoint == endpoint and current.model == model do
      :ok
    else
      with :ok <- write_algorithm(person, settings, algorithm),
           :ok <- write_scalar(:jev_endpoint, current.endpoint, endpoint),
           :ok <- write_scalar(:jev_model, current.model, model) do
        write_audit(person, key, operation, at)
      end
    end
  end

  defp write_algorithm(person, settings, algorithm) do
    if Map.get(settings, :algorithm_choices, "baseline") == algorithm do
      :ok
    else
      attrs = Attributes.encode(Map.put(settings, :algorithm_choices, algorithm))

      with {:ok, _} <-
             Transaction.query("UPDATE harklight.people SET settings = $2 WHERE id = $1", [person, attrs]) do
        :ok
      end
    end
  end

  defp write_scalar(_field, value, value), do: :ok

  defp write_scalar(field, _old, value) do
    with {:ok, _} <-
           Transaction.query(
             """
             INSERT INTO harklight.compatibility_facts (family, address, body) VALUES ($1, $2, $3)
             ON CONFLICT (family, address) DO UPDATE SET body = excluded.body
             """,
             [Atom.to_string(field), scalar_address(field), Attributes.encode(value)]
           ) do
      :ok
    end
  end

  defp write_audit(person, key, operation, at) do
    body = %{person_id: person, actor_id: key, action: "prioritization_settings", at_ms: at}

    with {:ok, _} <-
           Transaction.query(
             "INSERT INTO harklight.compatibility_facts (family, address, body) VALUES ('runtime_audit', $1, $2)",
             [Attributes.key({person, operation}), Attributes.encode(body)]
           ) do
      :ok
    end
  end

  defp scalar_address(field), do: Attributes.key({:field, field})

  defp decode_scalar(nil), do: {:ok, nil}
  defp decode_scalar(value), do: Attributes.decode(value)

  defp decode_settings(value) when value == %{}, do: {:ok, value}
  defp decode_settings(value), do: Attributes.decode(value)

  defp result({:ok, _} = result), do: result

  defp result({:error, reason}) when reason in [:forbidden, :invalid_endpoint, :invalid_model],
    do: {:error, reason}

  defp result({:error, _}), do: {:error, :storage_unavailable}
end
