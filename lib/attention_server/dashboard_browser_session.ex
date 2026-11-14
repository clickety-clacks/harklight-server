defmodule AttentionServer.DashboardBrowserSession do
  @moduledoc """
  In-memory browser cookies after Relay consumes a durable one-use sign-in token.

  The injected consumer returns only a committed owner principal and issuer
  revision. This process never issues, stores, or reuses sign-in links. Every
  cookie read rechecks current owner authority; restart drops all cookies.
  """

  use GenServer

  alias AttentionServer.{Diagnostics, OriginWire, Relay}

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  def redeem(server, token), do: GenServer.call(server, {:redeem, token})
  def resolve(server, cookie), do: GenServer.call(server, {:resolve, cookie})
  def end_session(server, cookie), do: GenServer.call(server, {:end_session, cookie})

  def open_browser_connection(server, cookie, recipient, worker),
    do:
      GenServer.call(
        server,
        {:open_browser_connection, cookie, recipient, worker},
        :infinity
      )

  def browser_session_request_line(server, cookie, browser_session_id, session_id, raw),
    do:
      GenServer.call(
        server,
        {:browser_session_request_line, cookie, browser_session_id, session_id, raw},
        :infinity
      )

  def browser_session_drain(server, cookie, browser_session_id, session_id),
    do:
      GenServer.call(
        server,
        {:browser_session_drain, cookie, browser_session_id, session_id},
        :infinity
      )

  def close_browser_connection(server, cookie, session_id),
    do: GenServer.call(server, {:close_browser_connection, cookie, session_id}, :infinity)

  def browser_file_command(server, cookie, recipient, request) do
    case GenServer.call(server, {:browser_context, cookie}) do
      {:ok, %{relay: relay, browser_session_id: browser_session_id}} ->
        OriginWire.browser_command(relay, browser_session_id, recipient, request)

      {:error, reason} ->
        {:error, browser_error(reason)}
    end
  end

  def browser_file_limits(server, cookie, recipient),
    do: GenServer.call(server, {:browser_file_limits, cookie, recipient}, :infinity)

  def browser_upload_begin(server, cookie, recipient, attrs, size, worker),
    do:
      GenServer.call(
        server,
        {:browser_upload_begin, cookie, recipient, attrs, size, worker},
        :infinity
      )

  def browser_upload_file(server, cookie, recipient, attrs, path, size, sha256, lease) do
    case GenServer.call(server, {:browser_context, cookie}) do
      {:ok, %{relay: relay, browser_session_id: browser_session_id}} ->
        Relay.browser_upload_file(
          relay,
          browser_session_id,
          recipient,
          attrs,
          path,
          size,
          sha256,
          lease
        )

      {:error, reason} ->
        {:error, browser_error(reason)}
    end
  end

  def browser_upload_end(server, _cookie, _recipient, lease),
    do: GenServer.call(server, {:browser_upload_end, lease}, :infinity)

  def browser_fetch_file(server, cookie, upload_id) do
    case GenServer.call(server, {:browser_context, cookie}) do
      {:ok,
       %{
         relay: relay,
         person: person,
         key: key,
         revision: revision
       }} ->
        Relay.browser_fetch_file(relay, person, key, revision, upload_id)

      {:error, reason} ->
        {:error, browser_error(reason)}
    end
  end

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
    end_browser_session(state.relay, session_hash)
    {:reply, :ok, %{state | sessions: Map.delete(state.sessions, session_hash)}}
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

  def handle_call({:open_browser_connection, cookie, recipient, worker}, _from, state) do
    case checked_session(state, cookie) do
      {:ok, session_hash, session, next} ->
        case Relay.open_browser_recipient(
               state.relay,
               session.person,
               session.key,
               session.revision,
               session_hash,
               recipient,
               worker
             ) do
          {:ok, session_id} ->
            {:reply,
             {:ok,
              %{
                relay: state.relay,
                browser_session_id: session_hash,
                session_id: session_id
              }}, next}

          {:error, :forbidden} ->
            {:error, :invalid_session, invalidated} = invalidate_session(next, session_hash)
            {:reply, {:error, :invalid_session}, invalidated}

          other ->
            {:reply, other, next}
        end

      {:error, reason, next} ->
        {:reply, {:error, browser_error(reason)}, next}
    end
  end

  def handle_call(
        {:browser_session_request_line, cookie, browser_session_id, session_id, raw},
        _from,
        state
      ) do
    case checked_session(state, cookie) do
      {:ok, session_hash, _session, next} when session_hash == browser_session_id ->
        case Relay.browser_session_request_line(
               next.relay,
               session_hash,
               session_id,
               raw
             ) do
          {:error, :forbidden} ->
            {:error, :invalid_session, invalidated} = invalidate_session(next, session_hash)
            {:reply, {:error, :session_ended}, invalidated}

          result ->
            {:reply, result, next}
        end

      {:ok, _session_hash, _session, next} ->
        {:reply, {:error, :session_ended}, next}

      {:error, reason, next} ->
        {:reply, {:error, browser_error(reason)}, next}
    end
  end

  def handle_call(
        {:browser_session_drain, cookie, browser_session_id, session_id},
        _from,
        state
      ) do
    case checked_session(state, cookie) do
      {:ok, session_hash, _session, next} when session_hash == browser_session_id ->
        case Relay.browser_session_drain(next.relay, session_hash, session_id) do
          {:error, :forbidden} ->
            {:error, :invalid_session, invalidated} = invalidate_session(next, session_hash)
            {:reply, {:error, :session_ended}, invalidated}

          result ->
            {:reply, result, next}
        end

      {:ok, _session_hash, _session, next} ->
        {:reply, {:error, :session_ended}, next}

      {:error, reason, next} ->
        {:reply, {:error, browser_error(reason)}, next}
    end
  end

  def handle_call({:close_browser_connection, cookie, session_id}, _from, state) do
    :ok = Relay.close_browser_connection(state.relay, digest(cookie), session_id)
    {:reply, :ok, state}
  end

  def handle_call({:browser_file_limits, cookie, recipient}, _from, state) do
    case checked_session(state, cookie) do
      {:ok, session_hash, _session, next} ->
        {:reply, Relay.browser_file_limits(next.relay, session_hash, recipient), next}

      {:error, reason, next} ->
        {:reply, {:error, browser_error(reason)}, next}
    end
  end

  def handle_call({:browser_upload_begin, cookie, recipient, attrs, size, worker}, _from, state) do
    case checked_session(state, cookie) do
      {:ok, session_hash, _session, next} ->
        {:reply,
         Relay.browser_upload_begin(
           next.relay,
           session_hash,
           recipient,
           attrs,
           size,
           worker
         ), next}

      {:error, reason, next} ->
        {:reply, {:error, browser_error(reason)}, next}
    end
  end

  def handle_call({:browser_upload_end, lease}, _from, state) do
    {:reply, Relay.upload_end(state.relay, lease), state}
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
        :ok

      other ->
        Diagnostics.failure(:dashboard_http, :session_end, other, :unavailable)
        :ok
    end
  catch
    :exit, reason ->
      Diagnostics.failure(:dashboard_http, :session_end, reason, :unavailable)
      :ok
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
               Relay.owner_revision(state.relay, person, key),
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

  defp digest(value) when is_binary(value), do: :crypto.hash(:sha256, value)
  defp digest(_), do: :invalid

  defp browser_error(:invalid_session), do: :session_ended
  defp browser_error(reason), do: reason
end
