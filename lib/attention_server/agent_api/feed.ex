defmodule AttentionServer.AgentAPI.Feed do
  @moduledoc false

  @behaviour AttentionServer.ClientAPI.Routes

  import Plug.Conn

  alias AttentionServer.AgentAPI.{FeedActivity, FeedStep}
  alias AttentionServer.ClientAPI.{Errors, Feed, FeedStream}
  alias AttentionServer.Message

  @impl true
  def routes, do: [{"GET", ["agent", "changes"], :changes, stream: true}]

  def changes(conn, %{principal: %{kind: :device}} = ctx) do
    conn = fetch_query_params(conn)

    with {:ok, cursor} <- Feed.cursor(conn),
         {:ok, limit} <- Feed.limit(conn.query_params["limit"]),
         {:ok, session} <- session(conn.query_params["session"]) do
      ctx = %{ctx | params: Map.put(ctx.params, :session, session)}

      if Feed.event_stream?(conn),
        do: FeedStream.serve(conn, ctx, cursor, FeedStep),
        else: batch(conn, ctx, cursor, limit)
    else
      {:error, field} -> Errors.send_error(conn, "invalid_field", field: field)
    end
  end

  def changes(conn, _ctx), do: Errors.send_error(conn, "forbidden")

  defp batch(conn, ctx, cursor, limit) do
    case FeedStep.read(ctx, cursor, limit) do
      {:ok, changes, more, position} ->
        conn =
          Errors.send_json(conn, 200, %{"changes" => changes, "rev" => position, "more" => more})

        FeedActivity.observe(ctx, :batch)

        conn

      {:error, reason} ->
        Feed.send_read_error(conn, reason)
    end
  end

  defp session(nil), do: {:ok, nil}

  defp session(value) do
    if Message.uuid_v4?(value), do: {:ok, value}, else: {:error, "session"}
  end
end

defmodule AttentionServer.AgentAPI.FeedStep do
  @moduledoc false

  alias AttentionServer.{AgentChangeLog, Diagnostics, Engine, Relay, Store}
  alias AttentionServer.AgentAPI.{FeedActivity, FeedRenderer, Focus}
  alias AttentionServer.ClientAPI.{FeedStep, Resources}

  def scope(%{principal: %{person: person, key: key}}), do: {:agent, person, key}

  def read(%{relay: relay, principal: principal, params: %{session: session}}, cursor, limit),
    do: call(relay, {:read, principal, session, cursor, limit})

  def opened(ctx) do
    FeedActivity.observe(ctx, :open)
    focus_granted(ctx)
  end

  def focus_granted(%{relay: relay, principal: %{key: key}}) do
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

  def handle_call({:read, principal, session, cursor, limit}, _from, %{store: store} = state) do
    with {:ok, agent, changes, more, position} <-
           read_rows(state, principal, session, cursor, limit),
         targets = Enum.flat_map(changes, &delivery_target/1),
         {:ok, store} <- commit_delivery(store, agent, targets) do
      {:reply, {:ok, changes, more, position}, %{state | store: store}}
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  defp commit_delivery(store, _agent, []), do: {:ok, store}

  # As in the legacy origin fetch, the page's cursor and payload are selected
  # before ACK1, but neither is returned until the receipt commit succeeds.
  defp commit_delivery(store, {person, key}, targets) do
    targets = resolve_answers(store.engine, person, targets)

    with {:ok, next, transition} <- stage_delivery(store, person, key, targets),
         audit = delivery_audit(store.engine, next, person, key),
         {:ok, store} <- Store.commit(store, transition, audit) do
      {:ok, store}
    else
      {:error, reason, _store} -> delivery_failure(reason)
      {:error, reason} -> delivery_failure(reason)
    end
  end

  defp stage_delivery(%Store{mode: :keyed, engine: engine}, person, key, targets) do
    {next, result, touched} =
      Engine.with_touches(engine, fn current ->
        case Engine.agent_feed_delivered(current, person, key, targets) do
          {:ok, next} -> {next, :ok}
          error -> {current, error}
        end
      end)

    with :ok <- result, do: {:ok, next, {next, touched}}
  end

  defp stage_delivery(store, person, key, targets) do
    with {:ok, next} <- Engine.agent_feed_delivered(store.engine, person, key, targets),
         do: {:ok, next, next}
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

  defp read_rows(%{store: store} = state, principal, session, cursor, limit) do
    with {:ok, agent} <- authorized(store.engine, principal),
         {:ok, rows, more, position} <- AgentChangeLog.read(store, agent, cursor, limit, session),
         {:ok, changes} <-
           state |> FeedRenderer.render_many(principal, rows) |> FeedStep.changes() do
      {:ok, agent, changes, more, position}
    else
      {:error, reason}
      when reason in [:cursor_expired, :account_paused, :unauthorized, :disabled] ->
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

  defp authorized(engine, %{kind: :device, person: person, key: key}) do
    case Engine.authorized_person(engine, key) do
      {:ok, ^person} ->
        if Map.get(engine.delivery_states, person) == "STOPPED",
          do: {:error, :account_paused},
          else: {:ok, {person, key}}

      _ ->
        {:error, :unauthorized}
    end
  end

  defp authorized(_engine, _principal), do: {:error, :unauthorized}

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
