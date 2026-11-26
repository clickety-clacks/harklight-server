defmodule AttentionServer.ClientAPI.GroupRoutes do
  @moduledoc false

  @behaviour AttentionServer.ClientAPI.Routes

  import Plug.Conn, only: [get_req_header: 2]

  alias AttentionServer.{AgentChangeLog, ChangeLog, ConversationFiles, Groups, Store}
  alias AttentionServer.AgentAPI.FeedStep
  alias AttentionServer.ClientAPI.{AgentWrites, Errors, Idempotency, PeopleProfiles, Reads, SharedCapability, Writes}

  @impl true
  def routes do
    [
      {"GET", ["groups"], :list},
      {"GET", ["agent", "groups"], :agent_list},
      {"POST", ["groups"], :create},
      {"PATCH", ["groups", :group_id], :rename},
      {"POST", ["groups", :group_id, "invitations"], :invite},
      {"POST", ["group-invitations", :invitation_id, "accept"], :accept},
      {"POST", ["group-invitations", :invitation_id, "decline"], :decline},
      {"POST", ["groups", :group_id, "leave"], :leave},
      {"PUT", ["groups", :group_id, "mute"], :mute}
    ]
  end

  def list(conn, ctx), do: read(conn, ctx, :person)
  def agent_list(conn, ctx), do: read(conn, ctx, :agent)

  def create(conn, ctx), do: write(conn, ctx, :create, ["name"])
  def rename(conn, ctx), do: write(conn, ctx, :rename, ["name"])
  def invite(conn, ctx), do: write(conn, ctx, :invite, ["person_id"])
  def accept(conn, ctx), do: write(conn, ctx, :accept, [])
  def decline(conn, ctx), do: write(conn, ctx, :decline, [])
  def leave(conn, ctx), do: write(conn, ctx, :leave, [])
  def mute(conn, ctx), do: write(conn, ctx, :mute, ["muted"])

  defp read(conn, ctx, kind) do
    with :ok <- capability(conn),
         result <- call(ctx.relay, {:read, ctx.principal, kind}) do
      respond(conn, result)
    else
      error -> respond(conn, error)
    end
  end

  defp write(conn, ctx, operation, members) do
    with :ok <- capability(conn),
         :ok <- call(ctx.relay, {:write_enabled, ctx.principal, operation}),
         :ok <- no_precondition(conn),
         {:ok, key} <- Writes.required_key(conn),
         {:ok, body, conn} <- AgentWrites.json_body(conn, []),
         :ok <- Writes.only_members(body, members) do
      digest = Idempotency.digest(conn.method, conn.path_info, body)

      case call(ctx.relay, {:write, ctx.principal, operation, ctx.params, body, key, digest}) do
        {:ok, response, replay?} -> Writes.respond(conn, {:ok, 200, response, replay?})
        error -> respond(conn, error)
      end
    else
      {:error, token, field, response} -> Errors.send_error(response, token, field: field)
      error -> respond(conn, error)
    end
  end

  defp respond(conn, {:error, token, field}), do: Errors.send_error(conn, to_string(token), field: field)
  defp respond(conn, result), do: Reads.respond(conn, result)

  defp call(relay, request), do: GenServer.call(relay, {:client_api, __MODULE__, request}, :infinity)

  defp capability(conn) do
    case SharedCapability.parse_groups(conn) do
      {:ok, %{groups?: true}} -> :ok
      {:ok, _} -> {:error, :not_found}
      error -> error
    end
  end

  defp no_precondition(conn) do
    if get_req_header(conn, "if-match") == [],
      do: :ok,
      else: {:error, :invalid_field, "if_match"}
  end

  def handle_call({:read, who, kind}, _from, state) do
    engine = state.store.engine

    result =
      with {:ok, principal} <- Reads.client_principal(engine, who),
           :ok <- available(state, principal.person) do
        list_projection(state.store, principal, kind)
      end

    {:reply, result, state}
  end

  def handle_call({:write_enabled, who, operation}, _from, state),
    do: {:reply, write_enabled(state, who, operation), state}

  def handle_call({:write, who, operation, params, body, key, digest}, _from, state) do
    with :ok <- write_enabled(state, who, operation),
         {:ok, %{person: person, key: actor}} <- Reads.client_principal(state.store.engine, who),
         {:ok, nil} <- Idempotency.lookup(state.store, person, key),
         {:ok, next, result} <- apply_operation(state.store.engine, person, operation, params, body),
         audit = %{person: person, actor: actor, action: "group_" <> Atom.to_string(operation),
                   group_receipt: {person, key, digest, result}},
         {:ok, store} <- commit(state.store, next, audit),
         {:ok, %{body: reply}} <- Idempotency.lookup(store, person, key) do
      {:reply, {:ok, reply, false}, %{state | store: store}}
    else
      {:ok, %{digest: ^digest, body: reply}} -> {:reply, {:ok, reply, true}, state}
      {:ok, %{}} -> {:reply, {:error, :idempotency_conflict}, state}
      {:error, _, _} -> {:reply, {:error, :storage_unavailable}, state}
      error -> {:reply, error, state}
    end
  end

  defp write_enabled(state, who, operation) do
    with {:ok, %{person: person, kind: kind}} when kind in [:device, :browser] <-
           Reads.client_principal(state.store.engine, who),
         :ok <- available(state, person),
         true <- state.deployment_mode == :self_hosted or operation == :mute do
      :ok
    else
      _ -> {:error, :not_found}
    end
  end

  def available(%{groups: true, store: %{mode: :keyed}} = state, person) do
    if Reads.shared_available?(state, person), do: :ok, else: {:error, :not_found}
  end

  def available(_state, _person), do: {:error, :not_found}

  defp list_projection(store, %{person: person, kind: kind}, :person) when kind in [:device, :browser] do
    engine = store.engine
    groups = Groups.visible(engine, person)

    with {:ok, rev} <- ChangeLog.head(store, person) do
      {:ok, %{
        "rev" => rev,
        "groups" => Enum.map(groups, &group_wire(engine, &1, person)),
        "invitations" =>
          Enum.map(Groups.invitations(engine, person), &invitation_wire(engine, &1, person))
      }}
    end
  end

  defp list_projection(store, %{person: person} = principal, :agent) do
    with {:ok, scope} <- FeedStep.authorized(store.engine, principal),
         {:ok, rev} <- AgentChangeLog.head(store, scope) do
      groups =
        store.engine
        |> Groups.visible(person)
        |> Enum.filter(&Map.has_key?(&1.members, person))
        |> Enum.map(&identity/1)
      {:ok, %{"rev" => rev, "groups" => groups}}
    end
  end

  defp list_projection(_store, _principal, _kind), do: {:error, :not_found}

  defp apply_operation(engine, person, :create, _params, %{"name" => name}),
    do: group_result(Groups.create(engine, ConversationFiles.new_upload_id(), person, name))

  defp apply_operation(engine, person, :rename, %{group_id: id}, %{"name" => name}),
    do: group_result(Groups.rename(engine, id, person, name))

  defp apply_operation(engine, person, :invite, %{group_id: id}, %{"person_id" => target}) when is_binary(target) do
    with {:ok, next, group, _id} <- Groups.invite(engine, id, person, target, ConversationFiles.new_upload_id()) do
      {:ok, next, {:invitation, group.id, target}}
    end
  end

  defp apply_operation(engine, person, :accept, %{invitation_id: invitation}, _body),
    do: group_result(Groups.respond(engine, invitation, person, true))

  defp apply_operation(engine, person, :decline, %{invitation_id: invitation}, _body) do
    with {:ok, next, _group} <- Groups.respond(engine, invitation, person, false), do: {:ok, next, :none}
  end

  defp apply_operation(engine, person, :leave, %{group_id: id}, _body) do
    with {:ok, next} <- Groups.leave(engine, id, person), do: {:ok, next, :none}
  end

  defp apply_operation(engine, person, :mute, %{group_id: id}, %{"muted" => muted?}) when is_boolean(muted?),
    do: group_result(Groups.mute(engine, id, person, muted?))

  defp apply_operation(_engine, _person, _operation, _params, _body),
    do: {:error, :invalid_field, "body"}

  defp group_result({:ok, next, group}), do: {:ok, next, {:group, group.id}}
  defp group_result({:error, :invalid_name}), do: {:error, :invalid_field, "name"}
  defp group_result(error), do: error

  defp commit(store, engine, %{group_receipt: receipt} = audit) do
    if MapSet.size(engine.touched_keys) == 0 do
      with :ok <- save_receipt(store, engine, receipt), do: {:ok, store}
    else
      Store.commit(store, engine, audit)
    end
  end

  def save_receipt(store, engine, {person, key, digest, result}) do
    with {:ok, rev} <- ChangeLog.head(store, person) do
      fields =
        case result do
          {:group, id} -> %{"group" => group_wire(engine, Map.fetch!(engine.groups, id), person)}
          {:invitation, id, target} ->
            %{"invitation" => invitation_wire(engine, Map.fetch!(engine.groups, id), target)}
          :none -> %{}
        end

      Idempotency.save(store, person, key, digest, 200, Map.put(fields, "rev", rev))
    end
  end

  def identity(group), do: %{"id" => group.id, "organization_id" => group.organization_id, "name" => group.name}

  defp group_wire(engine, group, person) do
    identity(group)
    |> Map.merge(%{
      "manager" => group.manager && PeopleProfiles.speaker(engine, group.manager),
      "members" => group.members |> Map.keys() |> Enum.sort() |> Enum.map(&PeopleProfiles.speaker(engine, &1)),
      "member" => Map.has_key?(group.members, person),
      "muted" => Map.get(group.members, person, false)
    })
  end

  defp invitation_wire(engine, group, person) do
    %{"id" => Map.fetch!(group.invitations, person), "group_id" => group.id, "group_name" => group.name,
      "person_id" => person, "invited_by" => PeopleProfiles.speaker(engine, group.manager)}
  end
end
