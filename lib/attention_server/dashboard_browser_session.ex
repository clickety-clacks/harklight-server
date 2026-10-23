defmodule AttentionServer.DashboardBrowserSession do
  @moduledoc """
  Short-lived browser cookies after Relay consumes a durable one-use sign-in token.

  The injected consumer returns only a committed owner principal and issuer
  revision. This process never issues, stores, or reuses sign-in links. Every
  cookie read rechecks current owner authority; restart drops all cookies.
  """

  use GenServer

  @session_ms 12 * 60 * 60 * 1_000
  @limit 1_024

  alias AttentionServer.Diagnostics

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  def redeem(server, token), do: GenServer.call(server, {:redeem, token})
  def resolve(server, cookie), do: GenServer.call(server, {:resolve, cookie})
  def end_session(server, cookie), do: GenServer.call(server, {:end_session, cookie})

  @impl true
  def init(opts) do
    {:ok,
     %{
       relay: Keyword.fetch!(opts, :relay),
       consume: Keyword.fetch!(opts, :consume),
       sessions: %{},
       session_limit: Keyword.get(opts, :session_limit, @limit),
       high_water: 0,
       now: Keyword.get(opts, :now, fn -> System.system_time(:millisecond) end)
     }}
  end

  @impl true
  def handle_call({:redeem, token}, _from, state) do
    state = state |> compact() |> prune_stale_at_capacity()

    if map_size(state.sessions) >= state.session_limit do
      {:reply, {:error, :capacity}, state}
    else
      consume_and_mint(state, token)
    end
  end

  def handle_call({:resolve, cookie}, _from, state) do
    state = compact(state)

    case Map.get(state.sessions, digest(cookie)) do
      %{person: person, key: key, revision: revision} = session ->
        case Diagnostics.result(
               :dashboard_http,
               :authentication,
               AttentionServer.Relay.owner_revision(state.relay, person, key),
               :rejected
             ) do
          {:ok, ^revision} ->
            principal = %{person_id: person, owner_key: key}

            principal =
              case Map.get(session, :display_name) do
                name when is_binary(name) -> Map.put(principal, :display_name, name)
                _ -> principal
              end

            {:reply, {:ok, principal}, state}

          _ ->
            {:reply, {:error, :invalid_session}, state}
        end

      _ ->
        {:reply, {:error, :invalid_session}, state}
    end
  end

  def handle_call({:end_session, cookie}, _from, state) do
    {:reply, :ok, %{state | sessions: Map.delete(state.sessions, digest(cookie))}}
  end

  defp consume_and_mint(state, token) do
    case Diagnostics.result(
           :dashboard_http,
           :request_dispatch,
           state.consume.(token),
           :rejected
         ) do
      {:ok,
       %{person_id: person, owner_key: key, owner_revision: revision, scope: :person_owner} =
           principal} ->
        case Diagnostics.result(
               :dashboard_http,
               :authentication,
               AttentionServer.Relay.owner_revision(state.relay, person, key),
               :rejected
             ) do
          {:ok, ^revision} ->
            cookie = :crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false)
            expires_at = state.high_water + @session_ms
            session = %{person: person, key: key, revision: revision, expires_at: expires_at}

            session =
              case Map.get(principal, :display_name) do
                name when is_binary(name) -> Map.put(session, :display_name, name)
                _ -> session
              end

            {:reply, {:ok, cookie, expires_at}, put_in(state.sessions[digest(cookie)], session)}

          _ ->
            {:reply, {:error, :invalid_link}, state}
        end

      _ ->
        {:reply, {:error, :invalid_link}, state}
    end
  end

  defp compact(state) do
    now = max(state.now.(), state.high_water)

    %{
      state
      | high_water: now,
        sessions: Map.reject(state.sessions, fn {_, session} -> session.expires_at <= now end)
    }
  end

  defp prune_stale_at_capacity(state) do
    if map_size(state.sessions) < state.session_limit do
      state
    else
      sessions =
        Map.filter(state.sessions, fn {_hash, %{person: person, key: key, revision: revision}} ->
          Diagnostics.result(
            :dashboard_http,
            :authentication,
            AttentionServer.Relay.owner_revision(state.relay, person, key),
            :rejected
          ) == {:ok, revision}
        end)

      %{state | sessions: sessions}
    end
  end

  defp digest(value) when is_binary(value), do: :crypto.hash(:sha256, value)
  defp digest(_), do: :invalid
end
