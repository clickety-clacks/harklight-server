defmodule AttentionServer.PushDispatcher do
  @moduledoc false

  use GenServer

  alias AttentionServer.Diagnostics

  @initial_retry_interval 250
  @maximum_retry_interval 30_000
  @maximum_backoff_exponent 7
  @stable_interval 30_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    state = %{
      opts: opts,
      child: nil,
      retry_timer: nil,
      retry_attempt: 0,
      stable_timer: nil
    }

    {:ok, start_dispatcher(state)}
  end

  @impl true
  def handle_info(:start_dispatcher, %{child: nil} = state) do
    {:noreply, start_dispatcher(%{state | retry_timer: nil})}
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
      {:ok, child} when is_pid(child) ->
        stable_timer = Process.send_after(self(), {:reset_retry_attempt, child}, @stable_interval)
        %{state | child: child, retry_timer: nil, stable_timer: stable_timer}

      {:error, reason} ->
        schedule_retry(state, reason)

      :ignore ->
        schedule_retry(state, :ignored)

      other ->
        schedule_retry(state, {:invalid_start_result, other})
    end
  end

  defp safe_start_worker(opts) do
    AttentionServer.PushDispatcher.Worker.start_link(opts)
  rescue
    error ->
      {:error, error}
  catch
    kind, reason ->
      {:error, {kind, reason}}
  end

  defp schedule_retry(state, reason \\ :start_failed) do
    Diagnostics.failure(:apns_dispatch, :configuration, reason, :retry)
    delay = retry_interval(state.retry_attempt)
    timer = Process.send_after(self(), :start_dispatcher, delay)

    %{
      state
      | retry_timer: timer,
        retry_attempt: state.retry_attempt + 1,
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

  alias AttentionServer.{APNS, Diagnostics, Relay}

  @idle_interval 1_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    relay = Keyword.fetch!(opts, :relay)
    apns = opts |> Keyword.fetch!(:apns) |> APNS.prepare!()
    :ok = Relay.enable_push(relay)

    state = %{
      relay: relay,
      apns: apns,
      cached_token: nil
    }

    send(self(), :dispatch)
    {:ok, state}
  rescue
    error ->
      Diagnostics.failure(:apns_dispatch, :configuration, error, :unavailable)
      {:stop, :invalid_apns_configuration}
  end

  @impl true
  def handle_info(:dispatch, state) do
    now_ms = System.system_time(:millisecond)

    case relay_call(:push_claim, nil, fn -> Relay.claim_push(state.relay, now_ms) end) do
      {:ok, nil} ->
        schedule(@idle_interval, state)

      {:ok, claim} ->
        {outcome, cached_token} = APNS.send(state.apns, claim, state.cached_token)

        case relay_call(:push_finish, claim.claim_id, fn ->
               Relay.finish_push(state.relay, claim, outcome, System.system_time(:millisecond))
             end) do
          :ok ->
            :ok

          {:error, reason} ->
            Diagnostics.failure(
              :apns_dispatch,
              :push_finish,
              reason,
              :retry,
              claim.claim_id
            )

          other ->
            Diagnostics.failure(
              :apns_dispatch,
              :push_finish,
              other,
              :retry,
              claim.claim_id
            )
        end

        send(self(), :dispatch)
        {:noreply, %{state | cached_token: cached_token}}

      {:error, reason} ->
        Diagnostics.failure(:apns_dispatch, :push_claim, reason, :retry)
        schedule(@idle_interval, state)

      other ->
        Diagnostics.failure(:apns_dispatch, :push_claim, other, :retry)
        schedule(@idle_interval, state)
    end
  end

  defp schedule(delay, state) do
    Process.send_after(self(), :dispatch, delay)
    {:noreply, state}
  end

  defp relay_call(phase, operation_id, fun) do
    fun.()
  rescue
    error ->
      Diagnostics.failure(:apns_dispatch, phase, error, :outcome_unknown, operation_id)
      reraise error, __STACKTRACE__
  catch
    kind, reason ->
      Diagnostics.failure(:apns_dispatch, phase, reason, :outcome_unknown, operation_id)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end
end
