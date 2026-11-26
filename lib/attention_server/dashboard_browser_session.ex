defmodule AttentionServer.DashboardBrowserSession do
  @moduledoc """
  Browser sessions resolved through Relay, with volatile sessions for the pre-S1 path.

  The injected consumer returns only a committed owner principal and issuer
  revision. Every cookie read rechecks current owner authority.
  """

  use GenServer

  alias AttentionServer.{BrowserAuth, Diagnostics, Relay}

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  def redeem(server, token), do: GenServer.call(server, {:redeem, token}, :infinity)
  def resolve(server, cookie), do: GenServer.call(server, {:resolve, cookie}, :infinity)
  def end_session(server, cookie), do: GenServer.call(server, {:end_session, cookie}, :infinity)

  @doc false
  # The checked session behind a cookie, for the client API's browser principal.
  def context(server, cookie), do: GenServer.call(server, {:browser_context, cookie}, :infinity)

  @impl true
  def init(opts) do
    {:ok,
     %{
       relay: Keyword.fetch!(opts, :relay),
       consume: Keyword.get(opts, :consume),
       sessions: %{}
     }}
  end

  @impl true
  def handle_call({:redeem, token}, _from, state) do
    consume_and_mint(state, token)
  end

  def handle_call({:resolve, cookie}, _from, state) do
    case checked_session(state, cookie) do
      {:ok, _session_hash, session, next} ->
        principal = %{person_id: session.person, owner_key: session.key}

        principal =
          case Map.get(session, :display_name) do
            name when is_binary(name) -> Map.put(principal, :display_name, name)
            _ -> principal
          end

        {:reply, {:ok, principal}, next}

      {:error, reason, next} ->
        {:reply, {:error, reason}, next}
    end
  end

  def handle_call({:end_session, cookie}, _from, state) do
    session_hash = digest(cookie)
    case end_browser_session(state.relay, session_hash) do
      :ok -> {:reply, :ok, %{state | sessions: Map.delete(state.sessions, session_hash)}}
      error -> {:reply, error, state}
    end
  end

  def handle_call({:browser_context, cookie}, _from, state) do
    case checked_session(state, cookie) do
      {:ok, session_hash, session, next} ->
        {:reply,
         {:ok,
          %{
            relay: state.relay,
            browser_session_id: session_hash,
            person: session.person,
            key: session.key,
            revision: session.revision
          }}, next}

      {:error, reason, next} ->
        {:reply, {:error, browser_error(reason)}, next}
    end
  end

  defp checked_session(state, cookie) do
    session_hash = digest(cookie)

    case Map.get(state.sessions, session_hash) do
      %{person: person, key: key, revision: revision} = session ->
        case Diagnostics.result(
               :dashboard_http,
               :authentication,
               Relay.owner_revision(state.relay, person, key),
               :rejected
             ) do
          {:ok, ^revision} ->
            {:ok, session_hash, session, state}

          {:ok, _new_revision} ->
            invalidate_session(state, session_hash)

          {:error, :forbidden} ->
            invalidate_session(state, session_hash)

          _ ->
            {:error, :invalid_session, state}
        end

      _ when is_binary(session_hash) ->
        case Relay.browser_session(state.relay, session_hash) do
          {:ok, session} -> {:ok, session_hash, session, state}
          {:error, :storage_unavailable} -> {:error, :storage_unavailable, state}
          _ -> {:error, :invalid_session, state}
        end

      _ ->
        {:error, :invalid_session, state}
    end
  end

  defp invalidate_session(state, session_hash) do
    end_browser_session(state.relay, session_hash)
    {:error, :invalid_session, %{state | sessions: Map.delete(state.sessions, session_hash)}}
  end

  defp end_browser_session(relay, session_hash) do
    case Relay.end_browser_session(relay, session_hash) do
      :ok ->
        :ok

      {:error, reason} ->
        Diagnostics.failure(:dashboard_http, :session_end, reason, :unavailable)
        {:error, :storage_unavailable}

      other ->
        Diagnostics.failure(:dashboard_http, :session_end, other, :unavailable)
        {:error, :storage_unavailable}
    end
  catch
    :exit, reason ->
      Diagnostics.failure(:dashboard_http, :session_end, reason, :unavailable)
      {:error, :storage_unavailable}
  end

  defp consume_and_mint(state, token) do
    cookie = BrowserAuth.new_secret()

    result =
      if state.consume do
        state.consume.(token)
      else
        Relay.redeem_browser_fragment(state.relay, token, digest(cookie))
      end

    case Diagnostics.result(
           :dashboard_http,
           :request_dispatch,
           result,
           :rejected
         ) do
      {:ok, %{persistent: true}} ->
        {:reply, {:ok, cookie, :persistent}, state}

      {:ok,
       %{person_id: person, owner_key: key, owner_revision: revision, scope: :person_owner} =
           principal} ->
        case Diagnostics.result(
               :dashboard_http,
               :authentication,
               Relay.owner_revision(state.relay, person, key),
               :rejected
             ) do
          {:ok, ^revision} ->
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

  defp digest(value) when is_binary(value), do: :crypto.hash(:sha256, value)
  defp digest(_), do: :invalid

  defp browser_error(:invalid_session), do: :session_ended
  defp browser_error(reason), do: reason
end
