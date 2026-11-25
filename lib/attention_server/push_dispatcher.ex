defmodule AttentionServer.PushDispatcher do
  @moduledoc false

  use GenServer

  alias AttentionServer.{APNS, Diagnostics}

  @initial_retry_interval 250
  @maximum_retry_interval 30_000
  @maximum_backoff_exponent 7
  @stable_interval 30_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))

  def apple_config(dispatcher), do: GenServer.call(dispatcher, :apple_config)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    state = %{
      opts: opts,
      prepared_apns: nil,
      child: nil,
      retry_attempt: 0,
      stable_timer: nil
    }

    {:ok, start_dispatcher(state)}
  end

  @impl true
  def handle_call(:apple_config, _from, state), do: {:reply, state.prepared_apns, state}

  @impl true
  def format_status(status), do: Map.update!(status, :state, &Map.delete(&1, :prepared_apns))

  @impl true
  def handle_info(:start_dispatcher, %{child: nil} = state) do
    {:noreply, start_dispatcher(state)}
  end

  def handle_info({:EXIT, child, reason}, %{child: child} = state) do
    cancel_timer(state.stable_timer)

    state = %{state | child: nil, stable_timer: nil}
    {:noreply, schedule_retry(state, {:worker_exit, reason})}
  end

  def handle_info({:reset_retry_attempt, child}, %{child: child} = state) do
    {:noreply, %{state | retry_attempt: 0, stable_timer: nil}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{child: child}) when is_pid(child) do
    if Process.alive?(child), do: Process.exit(child, :shutdown)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  defp start_dispatcher(state) do
    case safe_start_worker(state.opts) do
      {:ok, child, apns} when is_pid(child) ->
        stable_timer = Process.send_after(self(), {:reset_retry_attempt, child}, @stable_interval)
        %{state | child: child, stable_timer: stable_timer, prepared_apns: apns}

      {:error, reason} ->
        schedule_retry(state, reason)

      :ignore ->
        schedule_retry(state, :ignored)

      other ->
        schedule_retry(state, {:invalid_start_result, other})
    end
  end

  defp safe_start_worker(opts) do
    apns =
      case Keyword.fetch(opts, :apns) do
        {:ok, config} -> APNS.prepare!(config)
        :error -> nil
      end

    opts = Keyword.put(opts, :prepared_apns, apns)

    case AttentionServer.PushDispatcher.Worker.start_link(opts) do
      {:ok, worker} -> {:ok, worker, apns}
      other -> other
    end
  rescue
    error ->
      {:error, error}
  catch
    kind, reason ->
      {:error, {kind, reason}}
  end

  defp schedule_retry(state, reason) do
    operation = if state.opts[:push_relay_url], do: :push_relay, else: :apns_dispatch
    Diagnostics.failure(operation, :configuration, reason, :retry)
    delay = retry_interval(state.retry_attempt)
    Process.send_after(self(), :start_dispatcher, delay)

    %{
      state
      | retry_attempt: state.retry_attempt + 1,
        stable_timer: nil
    }
  end

  defp retry_interval(attempt) do
    exponent = min(attempt, @maximum_backoff_exponent)
    min(@initial_retry_interval * Integer.pow(2, exponent), @maximum_retry_interval)
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(timer), do: Process.cancel_timer(timer)
end

defmodule AttentionServer.PushDispatcher.Worker do
  @moduledoc false

  use GenServer

  require Logger

  alias AttentionServer.{APNS, Diagnostics, PushRelayClient, Relay}

  @idle_interval 1_000
  # Provisional until the bounded path records real provider attempt durations.
  @default_attempt_timeout_ms 20_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    relay = Keyword.fetch!(opts, :relay)
    {transport, send_push, operation, attempt_timeout_ms} = transport(opts, relay)
    :ok = Relay.enable_push(relay)

    state = %{
      relay: relay,
      apns: transport,
      apns_send: send_push,
      operation: operation,
      attempt_timeout_ms: attempt_timeout_ms,
      cached_token: nil
    }

    if operation == :push_relay do
      {:ok, state, {:continue, :register}}
    else
      send(self(), :dispatch)
      {:ok, state}
    end
  rescue
    error ->
      operation = if opts[:push_relay_url], do: :push_relay, else: :apns_dispatch
      Diagnostics.failure(operation, :configuration, error, :unavailable)

      reason =
        if operation == :apns_dispatch,
          do: :invalid_apns_configuration,
          else: :invalid_push_relay_configuration

      {:stop, reason}
  end

  defp transport(opts, relay) do
    case Keyword.fetch(opts, :apns) do
      {:ok, apns_opts} ->
        apns = Keyword.get_lazy(opts, :prepared_apns, fn -> APNS.prepare!(apns_opts) end)

        {apns, Keyword.get(opts, :apns_send, &APNS.send/3), :apns_dispatch,
         Keyword.get(apns_opts, :attempt_timeout_ms, @default_attempt_timeout_ms)}

      :error ->
        url = Keyword.fetch!(opts, :push_relay_url)

        config =
          PushRelayClient.prepare!(url, Relay.push_server_id(relay), opts[:push_relay_cacerts])

        {config, &PushRelayClient.send/3, :push_relay, @default_attempt_timeout_ms}
    end
  end

  @impl true
  def handle_continue(:register, state) do
    cached_token =
      case PushRelayClient.register(state.apns) do
        {:ok, key} ->
          key

        {:error, cause, _delay} ->
          Diagnostics.failure(:push_relay, :configuration, cause, :retry)
          nil
      end

    send(self(), :dispatch)
    {:noreply, %{state | cached_token: cached_token}}
  end

  @impl true
  def handle_info(:dispatch, state) do
    now_ms = System.system_time(:millisecond)

    case relay_call(state.operation, :push_claim, nil, fn ->
           Relay.claim_push(state.relay, now_ms)
         end) do
      {:ok, nil} ->
        schedule(@idle_interval, state)

      {:ok, claim} ->
        {outcome, cached_token} = send_attempt(state, claim)

        case relay_call(state.operation, :push_finish, claim.claim_id, fn ->
               Relay.finish_push(state.relay, claim, outcome, System.system_time(:millisecond))
             end) do
          :ok ->
            :ok

          {:error, reason} ->
            Diagnostics.failure(
              state.operation,
              :push_finish,
              reason,
              :retry,
              claim.claim_id
            )

          other ->
            Diagnostics.failure(
              state.operation,
              :push_finish,
              other,
              :retry,
              claim.claim_id
            )
        end

        send(self(), :dispatch)
        {:noreply, %{state | cached_token: cached_token}}

      {:error, reason} ->
        Diagnostics.failure(state.operation, :push_claim, reason, :retry)
        schedule(@idle_interval, state)

      other ->
        Diagnostics.failure(state.operation, :push_claim, other, :retry)
        schedule(@idle_interval, state)
    end
  end

  @impl true
  def format_status(status) do
    Map.update!(status, :state, &Map.drop(&1, [:cached_token, :apns]))
  end

  defp send_attempt(state, claim) do
    # The linked task owns the passive Mint connection. Stop it before handing
    # the serial dispatch lane to the next claim.
    started_at = System.monotonic_time(:millisecond)
    task = Task.async(fn -> state.apns_send.(state.apns, claim, state.cached_token) end)

    {result, outcome} =
      case Task.yield(task, state.attempt_timeout_ms) do
        {:ok, {%{state: outcome}, _cached_token} = result} ->
          {result, outcome}

        nil ->
          _ = Task.shutdown(task, :brutal_kill)
          Diagnostics.failure(state.operation, :push_request, :timeout, :retry, claim.claim_id)
          {{%{state: "retry"}, state.cached_token}, "timeout"}
      end

    duration_ms = System.monotonic_time(:millisecond) - started_at
    event = if state.operation == :push_relay, do: "push_relay_attempt", else: "apns_attempt"
    Logger.info("event=harklight_#{event} duration_ms=#{duration_ms} outcome=#{outcome}")
    result
  end

  defp schedule(delay, state) do
    Process.send_after(self(), :dispatch, delay)
    {:noreply, state}
  end

  defp relay_call(operation, phase, operation_id, fun) do
    fun.()
  rescue
    error ->
      Diagnostics.failure(operation, phase, error, :outcome_unknown, operation_id)
      reraise error, __STACKTRACE__
  catch
    kind, reason ->
      Diagnostics.failure(operation, phase, reason, :outcome_unknown, operation_id)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end
end
