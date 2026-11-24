defmodule AttentionServer.HTTP2FeedHandler do
  @moduledoc false

  use ThousandIsland.Handler

  alias Bandit.DelegatingHandler
  alias ThousandIsland.Socket

  @impl ThousandIsland.Handler
  def handle_connection(socket, state) do
    case DelegatingHandler.handle_connection(socket, state) do
      {:continue, %{handler_module: Bandit.HTTP2.Handler} = state} ->
        # An SSE client can receive heartbeats without sending any HTTP/2 frames.
        # Keep the preface deadline and let TCP detect a lost peer after setup.
        case Socket.setopts(socket, keepalive: true) do
          :ok -> {:continue, state, {:persistent, :infinity}}
          {:error, reason} -> {:error, reason, state}
        end

      result ->
        result
    end
  end

  @impl ThousandIsland.Handler
  defdelegate handle_data(data, socket, state), to: DelegatingHandler
  @impl ThousandIsland.Handler
  defdelegate handle_shutdown(socket, state), to: DelegatingHandler
  @impl ThousandIsland.Handler
  defdelegate handle_close(socket, state), to: DelegatingHandler
  @impl ThousandIsland.Handler
  defdelegate handle_timeout(socket, state), to: DelegatingHandler
  @impl ThousandIsland.Handler
  defdelegate handle_error(error, socket, state), to: DelegatingHandler

  @impl GenServer
  defdelegate handle_call(message, from, state), to: DelegatingHandler
  @impl GenServer
  defdelegate handle_cast(message, state), to: DelegatingHandler
  @impl GenServer
  defdelegate handle_info(message, state), to: DelegatingHandler
end
