defmodule AttentionServer.ClientAPI.AgentFocus do
  @moduledoc "The agent view of device focus and the held-show request."

  @behaviour AttentionServer.ClientAPI.Routes
  alias AttentionServer.{Engine, Relay}

  alias AttentionServer.ClientAPI.{
    AgentWrites,
    Errors,
    Focus,
    Idempotency,
    Reads,
    Resources,
    Writes
  }

  @impl true
  def routes do
    [
      {"GET", ["agent", "focus"], :get_focus},
      {"POST", ["agent", "show"], :show}
    ]
  end

  def get_focus(conn, ctx) do
    with :ok <- Writes.device_only(ctx.principal) do
      Reads.respond(conn, call(ctx, {:focus, ctx.principal}))
    else
      {:error, token, field} -> Errors.send_error(conn, token, field: field)
    end
  end

  def show(conn, ctx) do
    with :ok <- Writes.device_only(ctx.principal),
         {:ok, key} <- Writes.required_key(conn),
         {:ok, body, conn} <- AgentWrites.json_body(conn, []),
         :ok <- Writes.only_members(body, ~w(item_id target device_id device_name)),
         :ok <- validate_show(body) do
      digest = Idempotency.digest("POST", conn.path_info, body)
      Writes.respond(conn, call(ctx, {:show, ctx.principal, key, digest, body}))
    else
      {:error, token, field, response} -> Errors.send_error(response, token, field: field)
      {:error, token, field} -> Errors.send_error(conn, token, field: field)
    end
  end

  defp call(ctx, request),
    do: GenServer.call(ctx.relay, {:client_api, __MODULE__, request}, :infinity)

  @doc false
  def handle_call({:focus, %{person: person, key: agent}}, _from, state) do
    {:reply, snapshot(state, person, agent), state}
  end

  def handle_call({:show, %{person: person, key: agent}, key, digest, body}, _from, state) do
    receipt_key = {person, agent, key}

    with :ok <- permitted(state, person, agent, :push_view) do
      case Map.get(state.agent_show_receipts, receipt_key) do
        %{digest: ^digest, body: result} ->
          {:reply, {:ok, 200, result, true}, state}

        %{} ->
          {:reply, {:error, "idempotency_conflict", nil, false}, state}

        nil ->
          create_show(state, person, agent, body, receipt_key, digest)
      end
    else
      {:error, token} -> {:reply, {:error, to_string(token), nil, false}, state}
    end
  end

  def snapshot(state, person, agent) do
    with :ok <- permitted(state, person, agent, :see_focus),
         {:reply, {:ok, %{"clients" => clients}}, _} <-
           Relay.apply_agent_command(state, {:focus_get, agent}) do
      {:ok, project(state, person, clients)}
    end
  end

  def project(state, person, clients) do
    %{"clients" => Enum.map(clients, &project_client(state.store.engine, person, &1))}
  end

  defp project_client(engine, person, client) do
    device =
      client
      |> Map.take(~w(device_name screen in_front connected))
      |> Map.merge(%{
        "device_id" => client["client_id"],
        "reported_at_ms" => client["reported_at"]
      })

    key = {person, client["sender_id"], client["message_id"]}

    with {:ok, item_id} <- Resources.item_id(engine, key) do
      device = Map.put(device, "item_id", item_id)

      case visible_entry(engine, key, client["visible_event_id"]) do
        nil -> device
        entry_id -> Map.put(device, "visible_entry_id", entry_id)
      end
    else
      _ -> device
    end
  end

  defp visible_entry(_engine, _key, nil), do: nil

  defp visible_entry(engine, {person, _, _} = key, id) do
    engine.route_b_index.conversations_by_item
    |> Map.get(key, :gb_trees.empty())
    |> :gb_trees.iterator()
    |> Stream.unfold(fn iterator ->
      case :gb_trees.next(iterator) do
        :none -> nil
        {_, seq, next} -> {seq, next}
      end
    end)
    |> Enum.find_value(fn seq ->
      case get_in(engine.conversation_events, [{person, seq}, :payload, "event_id"]) do
        ^id -> Resources.entry_id(seq)
        _ -> nil
      end
    end)
  end

  defp create_show(state, person, agent, body, receipt_key, digest) do
    with {:ok, {^person, sender, message_id}} <-
           Resources.item_key(state.store.engine, person, body["item_id"]),
         frame = %{"sender_id" => sender, "message_id" => message_id, "target" => body["target"]},
         frame = device_selector(frame, body),
         {:reply, {:ok, resolved}, next} <-
           Relay.apply_agent_command(state, {:view_show, agent, frame}),
         device = resolved["client_id"],
         %{} = wire <-
           Focus.show(next.store.engine, device, next.held_shows[device], next.clock.()) do
      response = %{
        "show_id" => wire["show_id"],
        "device_id" => device,
        "device_name" => resolved["device_name"],
        "state" => resolved["delivery"]
      }

      now = next.clock.()
      window = Idempotency.retention_ms()

      receipts =
        Map.reject(next.agent_show_receipts, fn {_, receipt} -> now - receipt.at_ms >= window end)

      receipt = %{digest: digest, body: response, at_ms: now}

      {:reply, {:ok, 200, response, false},
       %{next | agent_show_receipts: Map.put(receipts, receipt_key, receipt)}}
    else
      {:error, :not_found} ->
        {:reply, {:error, "not_found", nil, false}, state}

      {:reply, {:error, {token, _field}}, _} ->
        {:reply, {:error, to_string(token), nil, false}, state}

      {:reply, {:error, token}, _} ->
        {:reply, {:error, to_string(token), nil, false}, state}

      _ ->
        {:reply, {:error, "retry_later", nil, false}, state}
    end
  end

  defp device_selector(frame, %{"device_id" => id}), do: Map.put(frame, "target_client", id)

  defp device_selector(frame, %{"device_name" => name}),
    do: Map.put(frame, "target_device_name", name)

  defp device_selector(frame, _), do: frame

  defp validate_show(body) do
    cond do
      not valid_id?(body["item_id"]) ->
        {:error, "invalid_field", "item_id"}

      body["target"] not in ["message", "chat"] ->
        {:error, "invalid_field", "target"}

      Map.has_key?(body, "device_id") and
          (not is_binary(body["device_id"]) or
             not Regex.match?(~r/\A[0-9a-f]{64}\z/, body["device_id"]) or
             Map.has_key?(body, "device_name")) ->
        {:error, "invalid_field", "device_id"}

      Map.has_key?(body, "device_name") and
          (not is_binary(body["device_name"]) or body["device_name"] == "") ->
        {:error, "invalid_field", "device_name"}

      true ->
        :ok
    end
  end

  defp valid_id?(id), do: is_binary(id) and byte_size(id) in 1..128

  defp permitted(state, person, agent, permission) do
    with true <- state.store.change_log,
         {:ok, ^person} <- Engine.authorized_person(state.store.engine, agent),
         false <- Engine.delivery_stopped?(state.store.engine, person) do
      if get_in(state.store.engine.devices, [agent, permission]) == true,
        do: :ok,
        else: {:error, :forbidden}
    else
      true -> {:error, :account_paused}
      false -> {:error, :not_found}
      _ -> {:error, :unauthorized}
    end
  end
end
