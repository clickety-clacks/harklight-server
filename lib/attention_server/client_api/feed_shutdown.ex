defmodule AttentionServer.ClientAPI.FeedShutdown do
  @moduledoc """
  Ends every open change feed with `closing: server_shutdown` when the server
  stops gracefully (docs/client-api-v2.md §6).

  The server starts it after every listener, so it stops before them: a
  supervisor stops its children in reverse order. In `terminate/2` it tells
  each open feed to close, and waits briefly for them to finish, so each
  client receives its `closing` event before its listener goes down.
  """

  use GenServer

  @registry AttentionServer.ChangeLog.Registry
  @scope {:closing, :server}
  @default_drain_ms 1_000

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      # Long enough for the drain below.
      shutdown: Keyword.get(opts, :drain_ms, @default_drain_ms) + 1_000
    }
  end

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc "The calling feed is told `{:client_closing, :server, :server_shutdown}` at shutdown."
  def subscribe, do: Registry.register(@registry, @scope, nil) |> then(fn _ -> :ok end)

  @doc "Ends `subscribe/0` for the calling process."
  def unsubscribe, do: Registry.unregister(@registry, @scope)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    {:ok, %{drain_ms: Keyword.get(opts, :drain_ms, @default_drain_ms)}}
  end

  @impl true
  def terminate(_reason, %{drain_ms: drain_ms}) do
    if Process.whereis(@registry) do
      Registry.dispatch(@registry, @scope, fn entries ->
        for {pid, _} <- entries, do: send(pid, {:client_closing, :server, :server_shutdown})
      end)

      drain(System.monotonic_time(:millisecond) + drain_ms)
    end

    :ok
  end

  # Waits until every feed has sent its closing and released its subscription.
  defp drain(deadline) do
    cond do
      Registry.lookup(@registry, @scope) == [] ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        :ok

      true ->
        Process.sleep(10)
        drain(deadline)
    end
  end
end
