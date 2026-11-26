defmodule AttentionServer.AgentAPI.Feed do
  @moduledoc false

  @behaviour AttentionServer.ClientAPI.Routes

  import Plug.Conn

  alias AttentionServer.AgentAPI.{FeedActivity, FeedStep}
  alias AttentionServer.ClientAPI.{Errors, Feed, FeedStream, SharedCapability}
  alias AttentionServer.Message

  @impl true
  def routes, do: [{"GET", ["agent", "changes"], :changes, stream: true}]

  def changes(conn, %{principal: %{kind: :device}} = ctx) do
    conn = fetch_query_params(conn)

    with {:ok, shared} <- SharedCapability.parse(conn),
         {:ok, cursor} <- Feed.cursor(conn),
         {:ok, limit} <- Feed.limit(conn.query_params["limit"]),
         {:ok, session} <- session(conn.query_params["session"]),
         {:ok, ack2} <- capability(conn) do
      ctx = %{ctx | params: Map.merge(ctx.params, %{session: session, ack2: ack2, shared_capability: shared})}

      if Feed.event_stream?(conn) do
        stream_ctx = %{ctx | params: Map.put(ctx.params, :stream, true)}
        FeedStream.serve(conn, stream_ctx, cursor, FeedStep)
      else
        batch(conn, ctx, cursor, limit)
      end
    else
      {:error, "invalid_field", field} -> Errors.send_error(conn, "invalid_field", field: field)
      {:error, field} -> Errors.send_error(conn, "invalid_field", field: field)
    end
  end

  def changes(conn, _ctx), do: Errors.send_error(conn, "forbidden")

  defp batch(conn, ctx, cursor, limit) do
    case FeedStep.read(ctx, cursor, limit) do
      {:ok, changes, more, position} ->
        conn =
          conn |> echo_capability(ctx.params.ack2) |> Feed.send_batch(changes, more, position)

        FeedActivity.observe(ctx, :batch)

        conn

      {:error, reason} ->
        Feed.send_read_error(conn, reason)
    end
  end

  @doc false
  def session(nil), do: {:ok, nil}

  def session(value) do
    if Message.uuid_v4?(value), do: {:ok, value}, else: {:error, "session"}
  end

  @doc false
  def capability(conn) do
    case get_req_header(conn, "harklight-ack2") do
      [] -> {:ok, nil}
      ["true"] -> {:ok, true}
      ["false"] -> {:ok, false}
      _ -> {:error, "harklight_ack2"}
    end
  end

  @doc false
  def echo_capability(conn, nil), do: conn

  def echo_capability(conn, value),
    do: put_resp_header(conn, "harklight-ack2", to_string(value))
end

defmodule AttentionServer.AgentAPI.FeedStep do
  @moduledoc false

  alias AttentionServer.{AgentChangeLog, Diagnostics, Engine, Relay, SharedMessages, Store}
  alias AttentionServer.AgentAPI.{FeedActivity, FeedRenderer, Focus}
  alias AttentionServer.ClientAPI.{FeedStep, Projection, Reads, Resources, SharedContinuation}

  def scope(%{principal: %{person: person, key: key}}), do: {:agent, person, key}

  def read(
        %{relay: relay, principal: principal, params: %{session: session} = params},
        cursor,
        limit
      ),
      do:
        call(
          relay,
          {:read, principal, session, cursor, limit,
           Keyword.new(Map.take(params, [:ack2, :stream, :shared_capability]))}
        )

  def opened(ctx) do
    FeedActivity.observe(ctx, :open)
    attach_focus(ctx)
  end

  defp attach_focus(%{relay: relay, principal: %{key: key}}) do
    case Relay.focus_stream_attach(relay, key, self()) do
      {:ok, data} -> send(self(), {:focus_change, data})
      _ -> :ok
    end
  end

  def closed(ctx), do: FeedActivity.observe(ctx, :close)

  def focus(%{relay: relay, principal: principal}) do
    case Focus.get(relay, principal) do
      {:ok, projection} -> {:ok, %{"focus" => projection}}
      error -> error
    end
  end

  defp call(relay, request),
    do: GenServer.call(relay, {:client_api, __MODULE__, request}, :infinity)

  def handle_call(
        {:read, principal, session, cursor, limit, opts},
        _from,
        %{store: store} = state
      ) do
    with {:ok, agent, changes, more, position} <-
           read_rows(state, principal, session, cursor, limit, opts[:shared_capability]),
         targets = Enum.flat_map(changes, &delivery_target/1),
         {:ok, store} <- commit_delivery(store, agent, targets, opts) do
      {:reply, {:ok, changes, more, position}, %{state | store: store}}
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  @doc false
  def commit_delivery(store, _agent, [], _opts), do: {:ok, store}

  # As in the legacy origin fetch, the page's cursor and payload are selected
  # before ACK1, but neither is returned until the receipt commit succeeds.
  def commit_delivery(store, {person, key} = scope, targets, opts) do
    {shared, legacy} = Enum.split_with(targets, &match?({:shared_entry, _}, &1))

    with {:ok, store} <- commit_legacy_delivery(store, scope, legacy, opts) do
      ids = Enum.map(shared, fn {:shared_entry, id} -> id end)
      updated = SharedMessages.mark_served(store.engine, ids)

      case if(updated == store.engine, do: {:ok, store}, else: Store.commit(store, updated, %{person: person, actor: key, action: "shared_chat_served"})) do
        {:ok, committed} -> {:ok, committed}
        {:error, reason, _} -> delivery_failure(reason)
      end
    end
  end

  defp commit_legacy_delivery(store, _scope, [], _opts), do: {:ok, store}

  defp commit_legacy_delivery(store, {person, key}, targets, opts) do
    targets = resolve_answers(store.engine, person, targets)

    with {:ok, next} <- Engine.agent_feed_delivered(store.engine, person, key, targets, opts),
         audit = delivery_audit(store.engine, next, person, key),
         {:ok, store} <- Store.commit(store, next, audit) do
      {:ok, store}
    else
      {:error, reason, _store} -> delivery_failure(reason)
      {:error, reason} -> delivery_failure(reason)
    end
  end

  defp delivery_audit(previous, next, person, key) do
    # ACK1 only appends conversation facts for this person. Pass their
    # canonical keys to the shared commit without inspecting retained history.
    before = Map.get(previous.conversation_highs, person, 0)
    latest = Map.get(next.conversation_highs, person, 0)

    %{
      person: person,
      actor: key,
      action: "agent_feed_delivered",
      conversation_entry_keys: for(seq <- (before + 1)..latest//1, do: {person, seq})
    }
  end

  defp delivery_failure(reason) do
    Diagnostics.failure(:client_api, :state_commit, reason, :failed)
    {:error, :delivery_unavailable}
  end

  defp read_rows(%{store: store} = state, principal, session, cursor, limit, capability) do
    with {:ok, agent} <- authorized(store.engine, principal),
         :ok <- validate_capability(state, principal, session, cursor, capability),
         {:ok, rows, more, position} <-
           AgentChangeLog.read(store, agent, cursor, limit, session, match?(%{shared?: true}, capability)),
         {:ok, changes} <- render_changes(state, principal, rows) do
      {:ok, agent, changes, more, position}
    else
      {:error, %Projection{}} = error ->
        error

      {:error, reason}
      when reason in [:cursor_expired, :account_paused, :unauthorized, :disabled, :unsupported] ->
        {:error, reason}

      {:error, reason} ->
        Diagnostics.failure(:client_api, :stream_read, reason, :failed)
        {:error, :storage_unavailable}
    end
  rescue
    error ->
      Diagnostics.failure(:client_api, :stream_read, error, :failed)
      {:error, :storage_unavailable}
  end

  defp validate_capability(state, principal, session, cursor, %{shared?: true, continuation: token}) do
    if Reads.shared_readable?(state, principal.person) do
      SharedContinuation.validate(token, state.store.engine.agent_cursor_key, principal,
        :agent_changes, %{session: session}, cursor, state.clock.())
    else
      {:error, :unsupported}
    end
  end

  defp validate_capability(_state, _principal, _session, _cursor, %{continuation: token})
       when is_binary(token),
       do: {:error, :cursor_expired}

  defp validate_capability(_state, _principal, _session, _cursor, _capability), do: :ok

  defp render_changes(state, principal, rows) do
    Projection.capture(fn ->
      Projection.run(:feed, fn ->
        state |> FeedRenderer.render_many(principal, rows) |> FeedStep.changes()
      end)
    end)
  end

  @doc false
  def authorized(engine, %{kind: :device, person: person, key: key}) do
    case Engine.authorized_person(engine, key) do
      {:ok, ^person} ->
        if Map.get(engine.delivery_states, person) == "STOPPED",
          do: {:error, :account_paused},
          else: {:ok, {person, key}}

      _ ->
        {:error, :unauthorized}
    end
  end

  def authorized(_engine, _principal), do: {:error, :unauthorized}

  defp delivery_target(%{"data" => %{"shared_entry_id" => id, "author" => "person",
                           "delivery" => %{"delivered" => false}}}),
    do: [{:shared_entry, id}]

  defp delivery_target(%{"data" => %{"shared_entry_id" => _}}), do: []

  defp delivery_target(%{
         "kind" => "entry",
         "data" => %{
           "kind" => "reply",
           "author" => "person",
           "position" => position,
           "delivery" => %{"delivered" => false}
         }
       }),
       do: [{:entry, position}]

  defp delivery_target(%{
         "kind" => "message",
         "data" => %{
           "item_id" => id,
           "session" => _,
           "terminal" => %{"state" => "answered"} = terminal
         }
       }) do
    if get_in(terminal, ["delivery", "delivered"]) == true, do: [], else: [{:answer, id}]
  end

  defp delivery_target(_change), do: []

  defp resolve_answers(engine, person, targets) do
    Enum.flat_map(targets, fn
      {:answer, id} ->
        case Resources.item_key(engine, person, id) do
          {:ok, key} -> [{:answer, key}]
          _ -> []
        end

      {:entry, _} = target ->
        [target]
    end)
  end
end
