defmodule AttentionServer.Repository.Postgres.PersonSettings do
  @moduledoc false

  alias AttentionServer.{BounceDecision, BouncePolicy, Engine, JSON}
  alias AttentionServer.ClientAPI.{Idempotency, Writes}
  alias AttentionServer.Repository.Postgres.{Attributes, Operations, Settings, Transaction}

  def execute(principal, {kind, value, if_match} = request, key, at) do
    operation = key || Ecto.UUID.generate()
    {route, member} = if kind == :alerts, do: {"alerts", "alerts"}, else: {"priority-levels", "levels"}
    digest = Idempotency.digest("PUT", ["settings", route], %{member => value, "if_match" => if_match})
    digest = Base.decode16!(digest, case: :lower)

    Transaction.run([], fn _ ->
      with {:ok, view} <- Settings.lock_browser(principal, at) do
        apply_request(principal, view, request, key, operation, digest, at)
      end
    end)
    |> result()
  end

  defp apply_request(principal, view, request, nil, operation, _digest, at) do
    with :ok <- mutate(principal, view, request, operation, at) do
      current(principal.person, 200, false)
    end
  end

  defp apply_request(principal, view, request, key, operation, digest, at) do
    Operations.execute_client(
      {principal.person, "", key},
      digest,
      at,
      fn ->
        with :ok <- mutate(principal, view, request, operation, at) do
          {:ok, 200, %{"settings" => true}}
        end
      end,
      fn
        %{"settings" => true} -> render_settings(principal.person)
        _ -> {:error, :receipt_unavailable}
      end
    )
  end

  defp current(person, status, replayed?) do
    with {:ok, body} <- render_settings(person) do
      {:ok, {status, body, replayed?}}
    end
  end

  defp render_settings(person) do
    with {:ok, view} <- Settings.for_person(person) do
      {:ok, Settings.render(view)}
    end
  end

  defp mutate(principal, view, {kind, value, if_match}, operation, at) do
    %{"settings" => payload} = Settings.render(view)

    if Writes.settings_match?(payload, if_match) do
      with {:ok, semantic, fields, attributes} <- prepare_change(kind, value, view, at) do
        receipt = %{semantic: JSON.canonical(semantic), reply: Map.put(fields, "op_id", operation)}
        domain_operation(principal, view, attributes, operation, receipt, at)
      end
    else
      {:error, :precondition_failed}
    end
  end

  defp prepare_change(:alerts, "all", view, _at) do
    {:ok, %{"type" => "alerts_set", "alerts" => "all"}, %{"alerts" => "all"},
     Map.put(view.attributes, :alert_choices, "all")}
  end

  defp prepare_change(:alerts, "important", view, _at) do
    {:ok, %{"type" => "alerts_set", "alerts" => "important"}, %{"alerts" => "important"},
     Map.delete(view.attributes, :alert_choices)}
  end

  defp prepare_change(:alerts, _value, _view, _at), do: {:error, {:invalid_field, "alerts"}}

  defp prepare_change(:priority_levels, value, view, at) do
    with {:ok, levels} <- BouncePolicy.normalize_levels(value) do
      generation = %{
        configured_at_ms: at,
        after_delivery_seq: view.accepted_high,
        after_conversation_seq: view.conversation_high
      }

      configuration = BounceDecision.level_configuration(view.attributes[:bounce_levels], levels, generation)

      {:ok, Engine.levels_semantic(levels), %{"levels" => BouncePolicy.public_levels(configuration.levels)},
       Map.put(view.attributes, :bounce_levels, configuration)}
    else
      {:error, :invalid_levels} -> {:error, {:invalid_field, "levels"}}
    end
  end

  defp domain_operation(principal, view, attributes, operation, %{semantic: semantic} = receipt, at) do
    person = principal.person
    actor = Writes.browser_actor_key(person, principal.session_id)
    address = Attributes.key({:row, :receipts, {person, actor, operation}})

    case Transaction.query(
           "SELECT body FROM harklight.compatibility_facts WHERE family = 'receipts' AND address = $1",
           [address]
         ) do
      {:ok, %{rows: []}} ->
        with {:ok, _revision} <- Settings.write(person, view, attributes, at),
             {:ok, _} <-
               Transaction.query(
                 "INSERT INTO harklight.compatibility_facts (family, address, body) VALUES ('receipts', $1, $2)",
                 [address, Attributes.encode(receipt)]
               ) do
          :ok
        end

      {:ok, %{rows: [[encoded]]}} ->
        case Attributes.decode(encoded) do
          {:ok, %{semantic: ^semantic}} -> :ok
          {:ok, %{semantic: _other}} -> {:error, :idempotency_conflict}
          _ -> {:error, :receipt_unavailable}
        end

      {:error, _} = error ->
        error
    end
  end

  defp result({:ok, {status, body, replayed?}}), do: {:ok, status, body, replayed?}
  defp result({:error, {token, field}}) when token == :invalid_field, do: {:error, token, field}

  defp result({:error, token})
       when token in [:unauthorized, :account_paused, :precondition_failed, :idempotency_conflict, :retry_later],
       do: {:error, token}

  defp result({:error, _}), do: {:error, :storage_unavailable}
end
