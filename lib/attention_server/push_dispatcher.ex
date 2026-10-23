defmodule AttentionServer.PushDispatcher do
  @moduledoc false

  use GenServer

  alias AttentionServer.{APNS, Diagnostics, Relay}

  @idle_interval 1_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    state = %{
      relay: Keyword.fetch!(opts, :relay),
      apns: opts |> Keyword.fetch!(:apns) |> APNS.prepare!(),
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
