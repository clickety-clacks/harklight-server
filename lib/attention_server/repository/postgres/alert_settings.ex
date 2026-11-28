defmodule AttentionServer.Repository.Postgres.AlertSettings do
  @moduledoc false

  alias AttentionServer.JSON
  alias AttentionServer.ClientAPI.{Idempotency, Writes}
  alias AttentionServer.Repository.Postgres.{Attributes, Operations, Settings, Transaction}

  def execute(principal, value, if_match, key, at) do
    operation = key || Ecto.UUID.generate()
    digest = Idempotency.digest("PUT", ["settings", "alerts"], %{"alerts" => value, "if_match" => if_match})
    digest = Base.decode16!(digest, case: :lower)

    Transaction.run([], fn _ ->
      with {:ok, view} <- Settings.lock_browser(principal, at) do
        apply_request(principal, view, value, if_match, key, operation, digest, at)
      end
    end)
    |> result()
  end

  defp apply_request(principal, view, value, if_match, nil, operation, _digest, at) do
    with :ok <- mutate(principal, view, value, if_match, operation, at) do
      current(principal.person, 200, false)
    end
  end

  defp apply_request(principal, view, value, if_match, key, operation, digest, at) do
    Operations.execute_client(
      {principal.person, "", key},
      digest,
      at,
      fn ->
        with :ok <- mutate(principal, view, value, if_match, operation, at) do
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

  defp mutate(principal, view, value, if_match, operation, at) do
    %{"settings" => payload} = Settings.render(view)

    cond do
      not Writes.settings_match?(payload, if_match) ->
        {:error, :precondition_failed}

      value not in ["important", "all"] ->
        {:error, {:invalid_field, "alerts"}}

      true ->
        domain_operation(principal, view, value, operation, at)
    end
  end

  defp domain_operation(principal, view, value, operation, at) do
    person = principal.person
    actor = Writes.browser_actor_key(person, principal.session_id)
    address = Attributes.key({:row, :receipts, {person, actor, operation}})
    semantic = JSON.canonical(%{"type" => "alerts_set", "alerts" => value})

    case Transaction.query(
           "SELECT body FROM harklight.compatibility_facts WHERE family = 'receipts' AND address = $1",
           [address]
         ) do
      {:ok, %{rows: []}} ->
        receipt = %{semantic: semantic, reply: %{"op_id" => operation, "alerts" => value}}

        with :ok <- update_alerts(person, view, value, at),
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

  defp update_alerts(person, view, value, at) do
    attributes = view.attributes
    next = if value == "all", do: Map.put(attributes, :alert_choices, value), else: Map.delete(attributes, :alert_choices)

    if next == attributes do
      :ok
    else
      scope = {person, ""}
      change = %{kind: "settings", resource_id: "settings", session_handle: nil, created_at_ms: at, first_seen_at_ms: nil}

      with {:ok, head} <- Transaction.lock_stream(scope, view.seed),
           {:ok, _} <- Transaction.query("UPDATE harklight.people SET settings = $2 WHERE id = $1", [person, Attributes.encode(next)]),
           {:ok, _} <- Transaction.append(%{scope => head}, scope, [change]) do
        :ok
      end
    end
  end

  defp result({:ok, {status, body, replayed?}}), do: {:ok, status, body, replayed?}
  defp result({:error, {token, field}}) when token == :invalid_field, do: {:error, token, field}

  defp result({:error, token})
       when token in [:unauthorized, :account_paused, :precondition_failed, :idempotency_conflict, :retry_later],
       do: {:error, token}

  defp result({:error, _}), do: {:error, :storage_unavailable}
end
