defmodule AttentionServer.ClientAPI.Feed do
  @moduledoc """
  `GET /v2/changes`: the person's change feed (docs/client-api-v2.md §6).

  - `Accept: text/event-stream` holds a Server-Sent Events stream: the backlog
    after the cursor, then live changes, with no gap. Each change is
    `id: <rev>`, `event: <kind>`, `data: <json>`. A `heartbeat` event with no
    `id` keeps the stream alive at least every 25 s, and the stream never
    ends for idleness. A server-initiated end sends a `closing` event with no
    `id` first: `revoked`, `account_paused` or `server_shutdown`.
  - Otherwise one JSON batch: `{changes, rev, more}`.

  The cursor is `Last-Event-ID`, which takes precedence, or `?after`. A cursor
  older than the retained log, or newer than the person's position, is
  `410 cursor_expired`.

  A batch's `rev` is the position it is complete through (contract round 12):
  the rev of the last log row read. That is its last change's `rev`, or later
  when trailing rows have no client form, such as a read-position fact, or
  `after` when nothing was read. So a batch of only such rows still advances
  and never loops.
  """

  @behaviour AttentionServer.ClientAPI.Routes

  import Plug.Conn

  require Logger

  alias AttentionServer.{ChangeLog, JSON}
  alias AttentionServer.ClientAPI.{Context, Errors, FeedStep}

  @max_limit 500
  @default_heartbeat_ms 20_000

  @impl true
  def routes, do: [{"GET", ["changes"], :changes, stream: true}]

  def changes(conn, %Context{} = ctx) do
    conn = fetch_query_params(conn)

    with {:ok, cursor} <- cursor(conn),
         {:ok, limit} <- limit(conn.query_params["limit"]) do
      if event_stream?(conn),
        do: stream(conn, ctx, cursor),
        else: batch(conn, ctx, cursor, limit)
    else
      {:error, field} -> Errors.send_error(conn, "invalid_field", field: field)
    end
  end

  # -- JSON batch ------------------------------------------------------------

  defp batch(conn, ctx, cursor, limit) do
    case FeedStep.read(ctx.relay, ctx.principal, cursor, limit) do
      {:ok, changes, more, position} ->
        Errors.send_json(conn, 200, %{"changes" => changes, "rev" => position, "more" => more})

      {:error, reason} ->
        send_read_error(conn, reason)
    end
  end

  # -- Server-Sent Events -----------------------------------------------------

  defp stream(conn, ctx, cursor) do
    person = ctx.principal.person

    # Subscribe before the first read, so nothing appended in between is lost.
    :ok = ChangeLog.subscribe(person)
    :ok = ChangeLog.subscribe_closing(ctx.principal)
    relay_ref = monitor_relay(ctx.relay)

    case FeedStep.read(ctx.relay, ctx.principal, cursor, @max_limit) do
      {:ok, changes, more, position} ->
        log(ctx, :open, nil)

        conn =
          conn
          |> put_resp_header("content-type", "text/event-stream; charset=utf-8")
          |> put_resp_header("cache-control", "no-cache")
          |> put_resp_header("x-accel-buffering", "no")
          |> send_chunked(200)

        state = %{ctx: ctx, relay_ref: relay_ref, cursor: position, timer: nil}

        case send_changes(conn, changes) do
          {:ok, conn} ->
            state = schedule_heartbeat(state)
            if more, do: pump(conn, state), else: wait(conn, state)

          {:error, _reason} ->
            finish(conn, state, "client_closed")
        end

      {:error, reason} ->
        ChangeLog.unsubscribe(person)
        send_read_error(conn, reason)
    end
  end

  defp pump(conn, state) do
    case FeedStep.read(state.ctx.relay, state.ctx.principal, state.cursor, @max_limit) do
      {:ok, changes, more, position} ->
        case send_changes(conn, changes) do
          {:ok, conn} ->
            state = %{state | cursor: position}
            if more, do: pump(conn, state), else: wait(conn, state)

          {:error, _reason} ->
            finish(conn, state, "client_closed")
        end

      # The cursor fell behind retention while streaming: end the stream; the
      # client's reconnect from its cursor then gets 410 and re-reads a snapshot.
      {:error, :cursor_expired} ->
        finish(conn, state, "cursor_expired")

      {:error, _reason} ->
        closing(conn, state, "server_shutdown")
    end
  catch
    :exit, _reason -> closing(conn, state, "server_shutdown")
  end

  defp wait(conn, state) do
    receive do
      {:change_log, _person, head} when head > state.cursor ->
        pump(conn, state)

      {:change_log, _person, _head} ->
        wait(conn, state)

      {:client_closing, _scope, reason} ->
        closing(conn, state, Atom.to_string(reason))

      {:DOWN, ref, :process, _pid, _reason} when ref == state.relay_ref ->
        closing(conn, state, "server_shutdown")

      {:feed_heartbeat, ref} when ref == state.timer ->
        case chunk(conn, "event: heartbeat\ndata: {}\n\n") do
          {:ok, conn} -> wait(conn, schedule_heartbeat(state))
          {:error, _reason} -> finish(conn, state, "client_closed")
        end
    end
  end

  defp send_changes(conn, []), do: {:ok, conn}

  defp send_changes(conn, changes) do
    chunk(conn, Enum.map(changes, &event/1))
  end

  defp event(%{"rev" => rev, "kind" => kind, "data" => data}) do
    ["id: ", Integer.to_string(rev), "\nevent: ", kind, "\ndata: ", JSON.encode!(data), "\n\n"]
  end

  defp closing(conn, state, reason) do
    _ = chunk(conn, ["event: closing\ndata: ", JSON.encode!(%{"reason" => reason}), "\n\n"])
    finish(conn, state, reason)
  end

  defp finish(conn, state, reason) do
    ChangeLog.unsubscribe(state.ctx.principal.person)
    log(state.ctx, :close, reason)
    conn
  end

  defp schedule_heartbeat(state) do
    ref = make_ref()
    Process.send_after(self(), {:feed_heartbeat, ref}, heartbeat_ms())
    %{state | timer: ref}
  end

  defp heartbeat_ms,
    do:
      Application.get_env(:attention_server, :client_api_feed_heartbeat_ms, @default_heartbeat_ms)

  defp monitor_relay(relay) do
    case GenServer.whereis(relay) do
      pid when is_pid(pid) -> Process.monitor(pid)
      _ -> nil
    end
  end

  # -- request parsing --------------------------------------------------------

  defp cursor(conn) do
    case get_req_header(conn, "last-event-id") do
      [value | _] -> non_negative(value, "last_event_id")
      [] -> non_negative(conn.query_params["after"] || "0", "after")
    end
  end

  defp limit(nil), do: {:ok, @max_limit}

  defp limit(value) do
    case Integer.parse(value) do
      {n, ""} when n > 0 -> {:ok, min(n, @max_limit)}
      _ -> {:error, "limit"}
    end
  end

  defp non_negative(value, field) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {n, ""} when n >= 0 -> {:ok, n}
      _ -> {:error, field}
    end
  end

  defp non_negative(_value, field), do: {:error, field}

  defp event_stream?(conn) do
    conn
    |> get_req_header("accept")
    |> Enum.any?(&String.contains?(&1, "text/event-stream"))
  end

  defp send_read_error(conn, :cursor_expired), do: Errors.send_error(conn, "cursor_expired")
  defp send_read_error(conn, :disabled), do: Errors.send_error(conn, "not_found")
  defp send_read_error(conn, _reason), do: Errors.send_error(conn, "storage_unavailable")

  defp log(ctx, phase, reason) do
    Logger.info(
      "event=harklight_client_feed request_id=#{ctx.request_id} phase=#{phase}" <>
        if(reason, do: " reason=#{reason}", else: "")
    )
  end
end

defmodule AttentionServer.ClientAPI.FeedStep do
  @moduledoc """
  The feed's read, in the Relay's serialized step: change-log rows are read
  and rendered from the same committed state, so each row shows its resource
  as of its rev (docs/client-api-v2.md §6.1).

  Rendering goes through `ClientAPI.FeedRenderer` (replaceable in tests through
  the `:client_api_feed_renderer` application setting): a module with
  `render(context, principal, change)` returning `{:ok, data}` or `:skip`.
  `context` is `%{engine, store, view}`, where `view` is the person's priority
  view.
  """

  require Logger

  alias AttentionServer.{ChangeLog, Diagnostics}

  @doc "Rows after `cursor`, rendered for `principal`."
  def read(relay, principal, cursor, limit),
    do: GenServer.call(relay, {:client_api, __MODULE__, {:read, principal, cursor, limit}})

  @doc false
  def handle_call({:read, principal, cursor, limit}, _from, state),
    do: {:reply, read_rows(state, principal, cursor, limit), state}

  # Only the read and render run in the Relay; the caller encodes and sends.
  # A failure here answers storage_unavailable and never takes down the Relay.
  defp read_rows(state, principal, cursor, limit) do
    person = principal.person

    with {:ok, rows, more, position} <- ChangeLog.read(state.store, person, cursor, limit) do
      context = %{
        engine: state.store.engine,
        store: state.store,
        view: Map.get(Map.get(state, :priority_views, %{}), person)
      }

      renderer = renderer()
      {:ok, Enum.flat_map(rows, &render(renderer, context, principal, &1)), more, position}
    end
  rescue
    error ->
      Diagnostics.failure(:client_api, :stream_read, error, :failed)
      {:error, :storage_unavailable}
  end

  # A renderer defect must not take down the Relay: the row is skipped and
  # logged.
  defp render(renderer, context, principal, %{rev: rev, kind: kind} = row) do
    case renderer.render(context, principal, row) do
      {:ok, data} when is_map(data) ->
        [%{"rev" => rev, "kind" => Atom.to_string(kind), "data" => data}]

      _ ->
        []
    end
  rescue
    error ->
      Diagnostics.failure(:client_api, :stream_read, error, :rejected)
      []
  end

  defp renderer,
    do:
      Application.get_env(
        :attention_server,
        :client_api_feed_renderer,
        AttentionServer.ClientAPI.FeedRenderer
      )
end
