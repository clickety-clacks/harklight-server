defmodule AttentionServer.ClientAPI.AgentShow do
  @moduledoc "The enrolled-agent show request and its memory-only receipt."

  @behaviour AttentionServer.ClientAPI.Routes
  alias AttentionServer.{Engine, Relay}

  alias AttentionServer.ClientAPI.{
    AgentWrites,
    Errors,
    Focus,
    Idempotency,
    Resources,
    Router,
    Writes
  }

  @impl true
  def routes, do: [{"POST", ["agent", "show"], :show}]

  def show(conn, ctx) do
    with :ok <- Writes.device_only(ctx.principal),
         {:ok, key} <- Writes.required_key(conn),
         {:ok, body, conn} <- AgentWrites.json_body(conn, []),
         :ok <- Writes.only_members(body, ~w(item_id target device_id device_name)),
         :ok <- validate_show(body) do
      digest = Idempotency.digest("POST", conn.path_info, body)
      show_id = "s" <> Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
      Writes.respond(conn, execute(ctx, key, digest, body, show_id))
    else
      {:error, token, field, response} -> Errors.send_error(response, token, field: field)
      {:error, token, field} -> Errors.send_error(conn, token, field: field)
    end
  end

  defp execute(ctx, key, digest, body, show_id) do
    result_ref = Process.alias()
    deadline = System.monotonic_time(:millisecond) + Router.request_timeout_ms()

    result =
      try do
        case call(ctx, {:show, ctx.principal, key, digest, body, show_id, result_ref}) do
          {:wait_for_writer, writer, receipt_key, response} ->
            state = await_writer(writer, result_ref, deadline)
            {:finish_show, receipt_key, result_ref, %{response | "state" => state}}

          :wait_for_receipt ->
            await_receipt(result_ref, deadline)

          outcome ->
            outcome
        end
      after
        close_result_ref(result_ref)
      end

    case result do
      {:finish_show, _, _, _} = finish -> call(ctx, finish)
      outcome -> outcome
    end
  end

  defp close_result_ref(ref) do
    Process.unalias(ref)

    receive do
      {^ref, _} -> :ok
    after
      0 -> :ok
    end
  end

  defp await_writer(writer, ref, deadline) do
    monitor = Process.monitor(writer)
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    try do
      receive do
        {^ref, :chunk_accepted} -> "sent"
        {^ref, :held} -> "held"
        {:DOWN, ^monitor, :process, ^writer, _} -> "held"
      after
        remaining -> "held"
      end
    after
      Process.demonitor(monitor, [:flush])
    end
  end

  defp await_receipt(ref, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {^ref, {:show_receipt, response}} -> {:ok, 200, response, true}
    after
      remaining -> {:error, "retry_later", nil, false}
    end
  end

  defp call(ctx, request),
    do: GenServer.call(ctx.relay, {:client_api, __MODULE__, request}, :infinity)

  @doc false
  def handle_call(
        {:show, %{person: person, key: agent}, key, digest, body, show_id, result_ref},
        {caller, _tag},
        state
      ) do
    scope = Idempotency.agent_scope(person, agent)
    receipt_key = {scope, key}

    with :ok <- permitted(state, person, agent, :push_view),
         {:ok, nil} <- lookup(state, scope, key) do
      create_show(state, person, agent, body, {show_id, result_ref, caller}, receipt_key, digest)
    else
      {:ok, %{digest: ^digest, owner: owner, waiters: waiters} = receipt} ->
        if Process.alive?(owner) do
          receipt = %{receipt | waiters: [result_ref | waiters]}
          next = put_in(state.agent_show_receipts[receipt_key], receipt)
          {:reply, :wait_for_receipt, next}
        else
          # The accepting request exited after queuing the show. Keep its held
          # fallback and original ID available to a retry.
          {:reply, {:ok, 200, receipt.body, true}, state}
        end

      {:ok, %{digest: ^digest, status: status, body: result}} when is_map(result) ->
        {:reply, {:ok, status, result, true}, state}

      {:ok, %{}} ->
        {:reply, {:error, "idempotency_conflict", nil, false}, state}

      {:error, token} when token in [:forbidden, :unauthorized, :not_found, :account_paused] ->
        {:reply, {:error, to_string(token), nil, false}, state}

      {:error, _} ->
        {:reply, {:error, "storage_unavailable", nil, false}, state}
    end
  end

  def handle_call({:finish_show, receipt_key, result_ref, response}, _from, state) do
    case Map.get(state.agent_show_receipts, receipt_key) do
      %{result_ref: ^result_ref, waiters: waiters} = receipt ->
        receipt = receipt |> Map.drop([:owner, :result_ref, :waiters]) |> Map.put(:body, response)
        next = put_in(state.agent_show_receipts[receipt_key], receipt)
        Enum.each(waiters, &send(&1, {&1, {:show_receipt, response}}))
        {:reply, {:ok, 200, response, false}, next}

      _ ->
        {:reply, {:error, "retry_later", nil, false}, state}
    end
  end

  @doc false
  def lookup(_state, _scope, nil), do: {:ok, nil}

  def lookup(state, scope, key) do
    case Map.get(state.agent_show_receipts, {scope, key}) do
      %{at_ms: at_ms} = receipt ->
        if state.clock.() - at_ms < Idempotency.retention_ms(),
          do: {:ok, receipt},
          else: Idempotency.lookup_encoded(state.store, scope, key)

      nil ->
        Idempotency.lookup_encoded(state.store, scope, key)
    end
  end

  defp create_show(state, person, agent, body, {show_id, result_ref, caller}, receipt_key, digest) do
    with {:ok, {^person, sender, message_id}} <-
           Resources.item_key(state.store.engine, person, body["item_id"]),
         frame = %{"sender_id" => sender, "message_id" => message_id, "target" => body["target"]},
         frame = device_selector(frame, body),
         {{:ok, resolved}, next} <- Relay.apply_agent_show(state, agent, frame, show_id) do
      response = %{
        "show_id" => show_id,
        "device_id" => resolved["client_id"],
        "device_name" => resolved["device_name"],
        "state" => resolved["delivery"]
      }

      receipt = %{digest: digest, status: 200, body: response, at_ms: next.clock.()}

      case Focus.offer_show(resolved["client_id"], show_id, result_ref) do
        nil ->
          next = put_in(next.agent_show_receipts[receipt_key], receipt)
          {:reply, {:ok, 200, response, false}, next}

        writer ->
          reservation = Map.merge(receipt, %{owner: caller, result_ref: result_ref, waiters: []})
          next = put_in(next.agent_show_receipts[receipt_key], reservation)
          {:reply, {:wait_for_writer, writer, receipt_key, response}, next}
      end
    else
      {:error, token} ->
        {:reply, {:error, to_string(token), nil, false}, state}

      {{:error, {token, _field}}, _} ->
        {:reply, {:error, to_string(token), nil, false}, state}

      {{:error, token}, _} ->
        {:reply, {:error, to_string(token), nil, false}, state}
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
