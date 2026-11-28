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
      with {:ok, view} <- lock_view(principal, at) do
        apply_request(principal, view, request, key, operation, digest, at)
      end
    end)
    |> result()
  end

  defp lock_view(%{kind: :browser} = principal, at), do: Settings.lock_browser(principal, at)
  defp lock_view(%{kind: :device} = principal, at), do: Settings.lock_native(principal, at)

  defp apply_request(principal, view, request, nil, operation, _digest, at) do
    with :ok <- mutate(principal, view, request, operation, at),
         {:ok, body} <- render_settings(principal) do
      {:ok, {200, body, false}}
    end
  end

  defp apply_request(%{kind: :device} = principal, view, request, key, operation, digest, at) do
    Operations.execute_native_client(
      {principal.person, key},
      digest,
      at,
      fn ->
        with :ok <- mutate(principal, view, request, operation, at),
             {:ok, body} <- render_settings(principal) do
          {:ok, 200, body}
        end
      end,
      &project_receipt(&1, principal)
    )
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
      &project_receipt(&1, principal)
    )
  end

  defp project_receipt(%{"settings" => true}, principal), do: render_settings(principal)
  defp project_receipt(_receipt, _principal), do: {:error, :receipt_unavailable}

  defp render_settings(principal) do
    with {:ok, view} <- Settings.for_person(principal.person) do
      Settings.project(view, principal)
    end
  end

  defp mutate(principal, view, {kind, value, if_match}, operation, at) do
    with {:ok, %{"settings" => payload}} <- Settings.project(view, principal),
         true <- Writes.settings_match?(payload, if_match) do
      with {:ok, semantic, fields, attributes} <- prepare_change(kind, value, view, at) do
        receipt = %{semantic: JSON.canonical(semantic), reply: Map.put(fields, "op_id", operation)}
        domain_operation(principal, view, attributes, operation, receipt, at)
      end
    else
      false -> {:error, :precondition_failed}
      {:error, _} = error -> error
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
    actor = receipt_actor(principal)
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

  defp receipt_actor(%{kind: :browser, person: person, session_id: session}),
    do: Writes.browser_actor_key(person, session)

  defp receipt_actor(%{kind: :device, key: key}), do: key

  defp result({:ok, {status, body, replayed?}}), do: {:ok, status, body, replayed?}
  defp result({:error, {token, field}}) when token == :invalid_field, do: {:error, token, field}

  defp result({:error, token})
       when token in [:unauthorized, :forbidden, :account_paused, :precondition_failed, :idempotency_conflict, :retry_later],
       do: {:error, token}

  defp result({:error, _}), do: {:error, :storage_unavailable}
end
