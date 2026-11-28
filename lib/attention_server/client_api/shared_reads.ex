defmodule AttentionServer.ClientAPI.SharedReads do
  @moduledoc false

  alias AttentionServer.{AgentChangeLog, ChangeLog, Engine, SharedMessages}
  alias AttentionServer.ClientAPI.{Auth, Errors, Reads, SharedCapability}
  alias AttentionServer.Repository.RecipientItems
  alias AttentionServer.Repository.Postgres.{Identities, Settings, SharedAccess, Transaction}

  def respond(conn, ctx, direction, action, fallback) do
    case SharedCapability.parse(conn) do
      {:ok, %{shared?: false}} ->
        fallback.()

      {:ok, %{shared?: true} = capability} ->
        id = ctx.params.item_id

        result =
          GenServer.call(
            ctx.relay,
            {:client_api, __MODULE__, {ctx.principal, direction, action, id, capability}},
            :infinity
          )

        case result do
          :legacy -> fallback.()
          _ -> Reads.respond(conn, result)
        end

      {:error, token, field} ->
        AttentionServer.ClientAPI.Errors.send_error(conn, token, field: field)
    end
  end

  @doc false
  def get_postgres_revision(conn, %{params: %{item_id: id, n: raw_revision}}, features) do
    result = Transaction.snapshot(fn ->
      with {:ok, %{kind: :browser, person: person}} <- Auth.web(conn, Identities, :postgres),
           :ok <- Settings.check_active(person),
           {:ok, %{shared?: true} = capability} <- SharedCapability.parse(conn),
           :ok <- SharedAccess.check(person, capability, features),
           {:ok, scope} <- RecipientItems.shared_revision_scope(:postgres, person, id, capability.groups?),
           {:ok, revision} <- Reads.revision_number(raw_revision) do
        RecipientItems.shared_revision(:postgres, scope, revision)
      else
        {:ok, _native} -> {:error, :not_found}
        {:error, :unavailable} -> {:error, :retry_later}
        {:error, token, field} -> {:error, {token, field}}
        {:error, _} = error -> error
      end
    end)

    case result do
      {:error, {token, field}} when is_binary(token) -> Reads.respond(conn, {:error, token, field})
      {:ok, body} -> Errors.send_streamed_json(conn, 200, body, &Reads.streamed_read_headers/2)
      error -> Reads.respond(conn, error)
    end
  end

  @doc false
  def handle_call({who, direction, action, id, capability}, _from, state) do
    result =
      with {:ok, principal} <- Reads.client_principal(state.store.engine, who),
           true <- SharedCapability.readable?(state, principal.person, capability) || {:error, :unsupported} do
        if String.starts_with?(id, "shared_"),
          do: read(state.store, principal, direction, action, id, capability),
          else: :legacy
      end

    {:reply, result, state}
  end

  defp read(store, %{kind: kind, person: person}, :person, action, id, capability) when kind in [:device, :browser] do
    with {:ok, message, recipient} <- SharedMessages.recipient_message(store.engine, person, id),
         true <- SharedCapability.visible?(message, capability) || {:error, :not_found},
         {:ok, head} <- ChangeLog.head(store, person),
         {:ok, rev} <- ChangeLog.rev(store, person, ChangeLog.shared_item_resource(message.id)) do
      cutoff = get_in(recipient, [:frozen, :revision])
      historical = SharedMessages.historical_record(message, cutoff)

      case action do
        {:entries, params} ->
          high = get_in(recipient, [:frozen, :entry_high])
          entries = SharedMessages.entries(store.engine, message, high)

          page_entries(entries, params, head, fn entry ->
            case ChangeLog.rev(store, person, ChangeLog.shared_entry_resource(entry.id)) do
              {:ok, entry_rev} -> {:ok, SharedMessages.person_entry(recipient, person, entry, entry_rev)}
              {:error, :not_found} when is_map_key(message, :group_id) ->
                {:ok, SharedMessages.person_entry(recipient, person, entry, rev)}
              error -> error
            end
          end, &SharedMessages.recipient_entry_id(&1.id, person))

        action ->
          resource(action, head, historical, fn ->
            {"item", SharedMessages.person_item(store.engine, person, message, recipient, rev)}
          end)
      end
    end
  end

  defp read(store, %{kind: :device, person: person, key: origin}, :sender, action, id, capability) do
    scope = {person, origin}

    with %{sender_person: ^person, sender_key: ^origin} = message <- store.engine.shared_messages[id],
         true <- SharedCapability.visible?(message, capability),
         true <- Engine.shared_sender_allowed?(store.engine, person, origin, message.sender_id),
         {:ok, head} <- AgentChangeLog.head(store, scope),
         {:ok, rev} <- AgentChangeLog.rev(store, scope, {:shared_message, id}) do
      historical = SharedMessages.historical_record(message, Map.get(message, :sender_frozen_revision))
      case action do
        {:entries, params} ->
          high = Map.get(message, :sender_frozen_entry_high)
          entries = SharedMessages.entries(store.engine, message, high)

          page_entries(entries, params, head, fn entry ->
            with {:ok, rev} <- AgentChangeLog.rev(store, scope, {:shared_entry, entry.id}) do
              {:ok, SharedMessages.sender_entry(message, entry, rev)}
            end
          end, & &1.id)

        action ->
          resource(action, head, historical, fn -> {"message", SharedMessages.sender_message(store.engine, message, rev)} end)
      end
    else
      result when result in [nil, false] -> {:error, :not_found}
      %{} -> {:error, :not_found}
      error -> error
    end
  end

  defp read(_store, _principal, _direction, _action, _id, _capability), do: {:error, :not_found}

  defp resource(:summary, head, _record, render) do
    {field, value} = render.()
    {:ok, %{"rev" => head, field => value}}
  end

  defp resource({:revision, n}, head, record, _render) do
    with {:ok, revision} <- Reads.revision_number(n),
         true <- revision <= record.revision || {:error, :not_found} do
      historical = SharedMessages.historical_record(record, revision)
      {:ok, %{"rev" => head, "revision" => SharedMessages.revision(historical)}}
    end
  end

  defp page_entries(entries, params, head, render, id_of) do
    with {:ok, limit} <- Reads.limit(params),
         {:ok, before} <- before(entries, params["before"], id_of) do
      eligible = if before, do: Enum.take_while(entries, &(&1.position < before)), else: entries
      page = Enum.take(eligible, -limit)

      Enum.reduce_while(page, {:ok, []}, fn entry, {:ok, rendered} ->
        case render.(entry) do
          {:ok, wire} -> {:cont, {:ok, [wire | rendered]}}
          error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, rendered} ->
          {:ok, %{"rev" => head, "entries" => Enum.reverse(rendered), "older" => length(eligible) > limit}}
        error -> error
      end
    end
  end

  defp before(_entries, nil, _id_of), do: {:ok, nil}
  defp before(entries, id, id_of) do
    case Enum.find(entries, &(id_of.(&1) == id)) do
      %{position: position} -> {:ok, position}
      nil -> {:error, :not_found}
    end
  end
end
