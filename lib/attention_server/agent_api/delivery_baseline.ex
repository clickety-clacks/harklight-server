defmodule AttentionServer.AgentAPI.DeliveryBaseline do
  @moduledoc false

  @behaviour AttentionServer.ClientAPI.Routes

  import Plug.Conn

  alias AttentionServer.{AgentChangeLog, Engine, JSON, RouteBIndex, Store}
  alias AttentionServer.AgentAPI.{Feed, FeedActivity, FeedStep}
  alias AttentionServer.ClientAPI.{AgentResources, Errors, Projection, Resources}
  alias AttentionServer.ClientAPI.Feed, as: PersonFeed

  @max_token 512

  @impl true
  def routes do
    [
      {"GET", ["agent", "delivery-baseline"], :baseline},
      {"PUT", ["agent", "messages", :item_id, "entries", :entry_id, "listener-received"],
       :listener_received}
    ]
  end

  def baseline(conn, %{principal: %{kind: :device}} = ctx) do
    conn = fetch_query_params(conn)
    page = conn.query_params["page"]

    with {:ok, session} <- Feed.session(conn.query_params["session"]),
         {:ok, ack2} <- Feed.capability(conn),
         {:ok, limit} <- PersonFeed.limit(conn.query_params["limit"]),
         {:ok, body} <- call(ctx, {:baseline, session, ack2, page, limit}) do
      ctx = %{ctx | params: Map.put(ctx.params, :session, session)}
      if page, do: FeedActivity.observe(ctx, :batch)

      conn
      |> Feed.echo_capability(ack2)
      |> put_resp_header("cache-control", "no-store")
      |> Errors.send_json(200, body)
    else
      {:error, field} when is_binary(field) ->
        Errors.send_error(conn, "invalid_field", field: field)

      {:error, :delivery_unavailable} ->
        PersonFeed.send_read_error(conn, :storage_unavailable)

      {:error, reason} ->
        PersonFeed.send_read_error(conn, reason)
    end
  end

  def baseline(conn, _ctx), do: Errors.send_error(conn, "forbidden")

  def listener_received(conn, %{principal: %{kind: :device}} = ctx) do
    with {:ok, raw, conn} <- read_body(conn, length: 1024),
         {:ok, body} when body == %{} <- JSON.decode_line(raw <> "\n"),
         :ok <- call(ctx, {:listener_received, ctx.params.item_id, ctx.params.entry_id}) do
      send_resp(conn, 204, "")
    else
      {:error, :not_found} -> Errors.send_error(conn, "not_found")
      {:error, :unsupported} -> Errors.send_error(conn, "unsupported")
      {:error, :account_paused} -> Errors.send_error(conn, "account_paused")
      {:error, :unauthorized} -> Errors.send_error(conn, "unauthorized")
      {:error, :storage_unavailable} -> PersonFeed.send_read_error(conn, :storage_unavailable)
      _ -> Errors.send_error(conn, "invalid_field", field: "body")
    end
  end

  def listener_received(conn, _ctx), do: Errors.send_error(conn, "forbidden")

  defp call(ctx, request),
    do: GenServer.call(ctx.relay, {:client_api, __MODULE__, {ctx.principal, request}}, :infinity)

  @doc false
  def handle_call({principal, request}, _from, state) do
    result =
      with {:ok, scope} <- FeedStep.authorized(state.store.engine, principal) do
        Projection.capture(fn -> apply_request(state, scope, request) end)
      end

    case result do
      {:ok, reply, store} -> {:reply, reply, %{state | store: store}}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  defp apply_request(%{store: store}, scope, {:baseline, session, ack2, nil, _limit}) do
    with {:ok, rev} <- AgentChangeLog.head(store, scope) do
      bound = get_in(store.engine.origin_feeds, [scope, :high]) || 0
      attempt = :crypto.strong_rand_bytes(16) |> Base.url_encode64(padding: false)
      page = %{attempt: attempt, bound: bound, position: 0, rev: rev}
      {:ok, {:ok, %{"rev" => rev, "next_page" => encode(page, scope, session, ack2)}}, store}
    end
  end

  defp apply_request(%{store: store} = state, scope, {:baseline, session, ack2, token, limit}) do
    with {:ok, page} <- decode(token, scope, session, ack2),
         stop = min(page.position + limit, page.bound),
         {:ok, changes, targets} <- page_changes(state, scope, session, page, stop),
         {:ok, store} <-
           FeedStep.commit_delivery(store, scope, targets, ack2: ack2, attempt: page.attempt) do
      body = %{"rev" => page.rev, "changes" => changes}

      body =
        if stop < page.bound,
          do: Map.put(body, "next_page", encode(%{page | position: stop}, scope, session, ack2)),
          else: body

      {:ok, {:ok, body}, store}
    end
  end

  defp apply_request(
         %{store: store} = state,
         {person, origin} = scope,
         {:listener_received, item_id, entry_id}
       ) do
    with {:ok, item} <- AgentResources.own_item_key(state, scope, item_id),
         {:ok, position} <- Resources.entry_seq(entry_id),
         {:ok, engine, seq} <-
           Engine.confirm_agent_listener_received(store.engine, person, origin, item, position) do
      audit = %{
        person: person,
        actor: origin,
        action: "agent_listener_received",
        conversation_entry_keys: [{person, seq}]
      }

      case Store.commit(store, engine, audit) do
        {:ok, next} -> {:ok, :ok, next}
        {:error, _reason, _store} -> {:error, :storage_unavailable}
      end
    else
      :error -> {:error, :not_found}
      {:error, _} = error -> error
    end
  end

  defp page_changes(%{store: %{engine: engine}} = state, scope, session, page, stop) do
    entries = get_in(engine.origin_feeds, [scope, :entries]) || %{}

    Enum.reduce_while((page.position + 1)..stop//1, {:ok, [], []}, fn seq,
                                                                      {:ok, changes, targets} ->
      with %{"author" => "person", "session_handle" => handle} = entry <- Map.get(entries, seq),
           true <- is_nil(session) or session == handle,
           true <- pending?(engine, scope, entry, page.attempt),
           {:ok, change, target} <- render(state, scope, entry) do
        {:cont, {:ok, [change | changes], [target | targets]}}
      else
        {:error, :not_found} -> {:cont, {:ok, changes, targets}}
        {:error, _} = error -> {:halt, error}
        _ -> {:cont, {:ok, changes, targets}}
      end
    end)
    |> case do
      {:ok, changes, targets} -> {:ok, Enum.reverse(changes), Enum.reverse(targets)}
      error -> error
    end
  end

  defp pending?(engine, {person, origin}, %{"feed_seq" => seq} = entry, attempt) do
    scope_key = {person, origin, seq}
    ack = Engine.delivery_ack_fact(engine, person, origin, entry, :ack1)
    # File reports can create a receipt record seeded from ACK1 alone.
    # That bookkeeping does not acknowledge application of the turn.
    receipt = Map.get(engine.origin_receipts, scope_key)

    settled =
      (not is_nil(receipt) and receipt[:implicit_ack] != true) or
        Map.has_key?(engine.origin_actions, scope_key)

    case ack do
      nil ->
        not settled

      %{"delivery_baseline_attempt" => ^attempt} ->
        not settled and
          is_nil(Engine.delivery_ack_fact(engine, person, origin, entry, :ack2)) and
          ack["listener_received"] != true

      _ ->
        false
    end
  end

  defp render(%{store: %{engine: engine}} = state, {person, _} = scope, %{
         "kind" => "discussion",
         "sender_id" => sender,
         "message_id" => id,
         "event_id" => turn
       }) do
    key = {person, sender, id}

    with position when is_integer(position) <-
           RouteBIndex.entry_position(engine.route_b_index, key, turn),
         {:ok, entry} <- AgentResources.entry(state, scope, key, position) do
      {:ok, change("entry", entry), {:entry, position}}
    end
  end

  defp render(%{store: %{engine: engine}} = state, {person, _} = scope, %{
         "kind" => "terminal",
         "sender_id" => sender,
         "message_id" => id,
         "terminal_seq" => terminal
       }) do
    key = {person, sender, id}

    with %{terminal: %{seq: ^terminal, state: "answered"}} <- Map.get(engine.messages, key),
         {:ok, message} <- AgentResources.message(state, scope, key) do
      {:ok, change("message", message), {:answer, message["item_id"]}}
    end
  end

  defp render(_state, _scope, _entry), do: :skip

  defp change(kind, %{"rev" => rev} = data), do: %{"kind" => kind, "rev" => rev, "data" => data}

  defp encode(page, scope, session, ack2) do
    [1, binding(scope, session, ack2), page.attempt, page.bound, page.position, page.rev]
    |> JSON.encode!()
    |> Base.url_encode64(padding: false)
  end

  defp decode(token, scope, session, ack2)
       when is_binary(token) and byte_size(token) <= @max_token do
    binding = binding(scope, session, ack2)

    with {:ok, raw} <- Base.url_decode64(token, padding: false),
         {:ok, [1, ^binding, attempt, bound, position, rev]} <- JSON.decode_line(raw <> "\n"),
         true <- is_binary(attempt) and byte_size(attempt) == 22,
         true <- is_integer(position) and position >= 0,
         true <- is_integer(bound) and bound >= position,
         true <- is_integer(rev) and rev >= 0 do
      {:ok, %{attempt: attempt, bound: bound, position: position, rev: rev}}
    else
      _ -> {:error, "page"}
    end
  end

  defp decode(_token, _scope, _session, _ack2), do: {:error, "page"}

  defp binding(scope, session, ack2) do
    :sha256
    |> :crypto.hash(:erlang.term_to_binary({scope, session, ack2}))
    |> Base.url_encode64(padding: false)
  end
end
