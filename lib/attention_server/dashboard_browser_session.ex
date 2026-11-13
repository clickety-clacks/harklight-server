defmodule AttentionServer.DashboardBrowserSession do
  @moduledoc """
  Short-lived browser cookies after Relay consumes a durable one-use sign-in token.

  The injected consumer returns only a committed owner principal and issuer
  revision. This process never issues, stores, or reuses sign-in links. Every
  cookie read rechecks current owner authority; restart drops all cookies.
  """

  use GenServer

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
       sessions: %{}
     }}
  end

  @impl true
  def handle_call({:redeem, token}, _from, state) do
    consume_and_mint(state, token)
  end

  def handle_call({:resolve, cookie}, _from, state) do
    session_hash = digest(cookie)

    case Map.get(state.sessions, session_hash) do
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

          {:ok, _new_revision} ->
            invalidate_session(state, session_hash)

          {:error, :forbidden} ->
            invalidate_session(state, session_hash)

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
            session = %{person: person, key: key, revision: revision}

            session =
              case Map.get(principal, :display_name) do
                name when is_binary(name) -> Map.put(session, :display_name, name)
                _ -> session
              end

            {:reply, {:ok, cookie, nil}, put_in(state.sessions[digest(cookie)], session)}

          _ ->
            {:reply, {:error, :invalid_link}, state}
        end

      _ ->
        {:reply, {:error, :invalid_link}, state}
    end
  end

  defp invalidate_session(state, session_hash) do
    {:reply, {:error, :invalid_session},
     %{state | sessions: Map.delete(state.sessions, session_hash)}}
  end

  defp digest(value) when is_binary(value), do: :crypto.hash(:sha256, value)
  defp digest(_), do: :invalid
end
