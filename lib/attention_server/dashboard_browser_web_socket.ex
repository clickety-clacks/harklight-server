defmodule AttentionServer.DashboardBrowserWebSocket do
  @moduledoc false

  @behaviour WebSock

  alias AttentionServer.{Diagnostics, JSON}

  # The hosted proxy's idle boundary is 120 seconds. A one-minute ping keeps
  # the upgraded connection active inside that configured window.
  @keepalive_ms 60_000
  @poll_ms 100

  @impl WebSock
  def init(opts) do
    state = %{
      backend: Keyword.fetch!(opts, :backend),
      cookie: Keyword.get(opts, :cookie),
      recipient: Keyword.get(opts, :recipient),
      browser_session_id: nil,
      relay: nil,
      session_id: nil
    }

    cond do
      not AttentionServer.Message.uuid_v4?(state.recipient) ->
        {:stop, :normal, {1008, "invalid_recipient"}, state}

      not is_binary(state.cookie) ->
        {:stop, :normal, {4401, "session_ended"}, state}

      true ->
        case backend_call(state.backend, :open_browser_connection, {
               state.cookie,
               state.recipient,
               self()
             }) do
          {:ok, %{relay: relay, browser_session_id: browser_session_id, session_id: session_id}} ->
            Process.send_after(self(), :browser_poll, @poll_ms)
            Process.send_after(self(), :browser_keepalive, @keepalive_ms)

            {:ok,
             %{
               state
               | relay: relay,
                 browser_session_id: browser_session_id,
                 session_id: session_id
             }}

          {:error, :inactive_account} ->
            {:stop, :normal, {4403, "inactive_account"}, state}

          _ ->
            {:stop, :normal, {4401, "session_ended"}, state}
        end
    end
  end

  @impl WebSock
  def handle_in({payload, opcode: :text}, state) do
    line = if String.ends_with?(payload, "\n"), do: payload, else: payload <> "\n"

    case backend_call(
           state.backend,
           :browser_session_request_line,
           {state.cookie, state.browser_session_id, state.session_id, line}
         ) do
      {:ok, lines} ->
        {:push, Enum.map(lines, &{:text, &1}), state}

      {:error, :session_ended} ->
        {:stop, :normal, {4401, "session_ended"}, state}

      {:error, :replaced} ->
        {:stop, :normal, {4409, "replaced"}, state}

      {:error, :inactive_account} ->
        {:push, {:text, error_line("inactive_account", line)}, state}

      {:error, :forbidden} ->
        {:stop, :normal, {4401, "session_ended"}, state}

      _ ->
        {:ok, state}
    end
  end

  def handle_in({_payload, opcode: :binary}, state) do
    reply = JSON.encode!(%{"v" => 1, "type" => "error", "reason" => "bad_frame"})
    {:push, {:text, reply}, state}
  end

  @impl WebSock
  def handle_control({_payload, opcode: :pong}, state), do: {:ok, state}
  def handle_control({_payload, opcode: :ping}, state), do: {:ok, state}

  @impl WebSock
  def handle_info(:browser_poll, state) do
    Process.send_after(self(), :browser_poll, @poll_ms)

    case backend_call(
           state.backend,
           :browser_session_drain,
           {state.cookie, state.browser_session_id, state.session_id}
         ) do
      {:ok, lines} -> {:push, Enum.map(lines, &{:text, &1}), state}
      {:error, :resync_required} -> {:ok, state}
      {:error, :session_ended} -> {:stop, :normal, {4401, "session_ended"}, state}
      {:error, :inactive_account} -> {:stop, :normal, {4403, "inactive_account"}, state}
      {:error, :forbidden} -> {:stop, :normal, {4401, "session_ended"}, state}
      _ -> {:ok, state}
    end
  end

  def handle_info(:browser_keepalive, state) do
    Process.send_after(self(), :browser_keepalive, @keepalive_ms)
    {:push, {:ping, <<>>}, state}
  end

  def handle_info({:browser_session_closed, code, reason}, state),
    do: {:stop, :normal, {code, reason}, state}

  def handle_info({:browser_connection_replaced, _session_id}, state),
    do: {:stop, :normal, {4409, "replaced"}, state}

  def handle_info({:attention_fenced, _session_id}, state),
    do: {:stop, :normal, {4401, "session_ended"}, state}

  def handle_info(_message, state), do: {:ok, state}

  @impl WebSock
  def terminate(_reason, %{relay: relay, session_id: session_id} = state)
      when not is_nil(relay) and not is_nil(session_id) do
    try do
      backend_call(state.backend, :close_browser_connection, {state.cookie, session_id})
    catch
      :exit, reason ->
        Diagnostics.failure(:dashboard_http, :session_end, reason, :unavailable)
        :ok
    end
  end

  def terminate(_reason, _state), do: :ok

  defp backend_call({module, context}, operation, value),
    do: apply(module, operation, [context, value])

  defp backend_call(module, operation, value) when is_atom(module),
    do: apply(module, operation, [value])

  defp error_line(reason, raw) do
    frame =
      case JSON.decode_line(raw) do
        {:ok, %{} = value} -> value
        _ -> %{}
      end

    reply =
      %{"v" => 1, "type" => "error", "reason" => reason}
      |> then(fn reply ->
        if is_binary(Map.get(frame, "ref")), do: Map.put(reply, "ref", frame["ref"]), else: reply
      end)

    JSON.encode!(reply) <> "\n"
  end
end
