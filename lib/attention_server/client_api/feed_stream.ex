defmodule AttentionServer.ClientAPI.FeedStream do
  @moduledoc """
  The Server-Sent Events form of `GET /v2/changes` (docs/client-api-v2.md §6).

  - The stream sends the backlog after the cursor, then live changes, with no
    gap. Each change is one event: `id: <rev>`, `event: <kind>`,
    `data: <data as one line of JSON>`.
  - It sends `event: heartbeat` with `data: {}` and no `id:` line at least
    every 25 s, and never ends for idleness.
  - Before a server-initiated end it sends `event: closing` with
    `data: {"reason": ...}` and no `id:` line, so the cursor does not move:
    - `revoked` when the device is revoked or removed, or the browser
      session ends;
    - `account_paused` when the account is paused;
    - `server_shutdown` when the server stops (`ClientAPI.FeedShutdown`) or
      the Relay exits;
    - `cursor_expired` when the position falls out of retention mid-stream;
    - `storage_unavailable` when a mid-stream read fails (round 17).
  - A device's feed also carries its held shows (round 11 r4 §3b): an
    `event: show` with the show's data and no `id:` line. A show comes from
    the Relay's memory-only focus state (`ClientAPI.Focus`), never from the
    change log, so it is never in a batch, never replayed on resume, and
    never moves the cursor. Its `held_ms` is its age when written.
  - The feed's ID is its `Request-Id`. It is logged at open, and at close
    with the reason, `client_closed` when the client went away.

  **Where the work runs.** The feed process subscribes to the person's change
  log before its first read, so a commit between the read and the wait still
  wakes it. Each read and render is one bounded step in the Relay
  (`ClientAPI.FeedStep`, at most `@page` rows). Encoding and sending happen
  here, in the request's own process.

  **A slow or dead client.** The feed holds at most one page in memory. Change
  wake-ups are coalesced, so a blocked feed's mailbox holds at most one.
  Notification never blocks the Relay, since it only sends a message.
  The HTTP server bounds a blocked send: HTTP/2 ends the stream when the
  peer's window stays closed, and HTTP/1.1 closes on its send timeout. The
  client then resumes from its `Last-Event-ID`.
  """

  import Plug.Conn

  require Logger

  alias AttentionServer.{ChangeLog, JSON}
  alias AttentionServer.ClientAPI.{Context, Feed, FeedShutdown, FeedStep, Focus}

  # Rows per Relay step. Smaller than a batch's 500, so a long backlog
  # interleaves with other work in the Relay.
  @page 100
  @default_heartbeat_ms 20_000

  @doc "Serves the stream from `cursor`. Errors before the stream opens are JSON."
  def serve(conn, %Context{} = ctx, cursor, source \\ FeedStep) do
    # Subscribe before the first read: a commit or a closing after the read
    # still reaches us. The watcher holds the change-log subscription.
    watcher = watch(ctx, source)
    :ok = ChangeLog.subscribe_closing(ctx.principal)
    :ok = FeedShutdown.subscribe()
    subscribe_shows(ctx.principal, source)

    try do
      open(conn, ctx, cursor, watcher, source)
    after
      send(watcher, {:feed_done, self()})
      ChangeLog.unsubscribe_closing(ctx.principal)
      FeedShutdown.unsubscribe()
      unsubscribe_shows(ctx.principal, source)
      flush_wakeups()
      flush_closings()
      flush_shows()
    end
  end

  defp open(conn, ctx, cursor, watcher, source) do
    case source.read(ctx, cursor, @page) do
      {:ok, changes, more, position} ->
        conn = start_stream(conn)

        send(watcher, {:feed_opened, self()})

        state =
          schedule_heartbeat(%{
            ctx: ctx,
            source: source,
            cursor: position,
            watcher: watcher,
            relay: monitor(ctx.relay),
            last_show_id: nil,
            timer: nil,
            timer_ref: nil
          })

        send_page(conn, state, changes, more)

      {:error, :delivery_unavailable} ->
        conn |> start_stream() |> send_closing("storage_unavailable")

      {:error, reason} ->
        Feed.send_read_error(conn, reason)
    end
  end

  defp start_stream(conn) do
    conn
    |> put_resp_header("content-type", "text/event-stream; charset=utf-8")
    |> put_resp_header("cache-control", "no-cache")
    |> put_resp_header("x-accel-buffering", "no")
    |> send_chunked(200)
  end

  # Reads and sends pages until the feed has caught up with the log.
  defp pump(conn, state) do
    case relay_step(fn -> state.source.read(state.ctx, state.cursor, @page) end) do
      {:ok, changes, more, position} ->
        state = %{state | cursor: position}

        send_page(conn, state, changes, more)

      # The principal stopped authenticating before its closing arrived.
      {:error, :unauthorized} ->
        closing(conn, state, "revoked")

      {:error, :account_paused} ->
        closing(conn, state, "account_paused")

      # The cursor fell behind retention while open: the client rebaselines.
      {:error, :cursor_expired} ->
        closing(conn, state, "cursor_expired")

      {:error, :server_shutdown} ->
        closing(conn, state, "server_shutdown")

      # Any other read failure is transient: the client reconnects later.
      {:error, _reason} ->
        closing(conn, state, "storage_unavailable")
    end
  end

  defp send_page(conn, state, changes, more) do
    case send_changes(conn, changes) do
      {:ok, conn} -> continue(conn, state, more)
      {:error, _reason} -> finish(conn, state, "client_closed")
    end
  end

  defp continue(conn, state, false), do: wait(conn, state)

  defp continue(conn, state, true) do
    # Ready control messages must be serviced between pages even when writes
    # keep the feed permanently behind the head.
    receive do
      {:client_closing, _scope, reason} ->
        closing(conn, state, Atom.to_string(reason))

      {:DOWN, ref, :process, _pid, _reason} when ref == state.relay ->
        closing(conn, state, "server_shutdown")

      {:feed_heartbeat, ref} when ref == state.timer ->
        heartbeat(conn, state, &pump/2)

      {:feed_focus, watcher} when watcher == state.watcher ->
        write_focus(conn, state, &pump/2)
    after
      0 -> pump(conn, state)
    end
  end

  defp wait(conn, state) do
    receive do
      # Acknowledged before the read, so a commit after it wakes us again.
      {:change_log, _person, head} when head > state.cursor ->
        send(state.watcher, {:feed_woken, self()})
        pump(conn, state)

      {:change_log, _person, _head} ->
        send(state.watcher, {:feed_woken, self()})
        wait(conn, state)

      {:client_closing, _scope, reason}
      when reason in [:revoked, :account_paused, :server_shutdown] ->
        closing(conn, state, Atom.to_string(reason))

      {:DOWN, ref, :process, _pid, _reason} when ref == state.relay ->
        closing(conn, state, "server_shutdown")

      {:client_api_show, device_key}
      when state.source == FeedStep and device_key == state.ctx.principal.key ->
        write_pending_show(conn, state)

      {:feed_heartbeat, ref} when ref == state.timer ->
        heartbeat(conn, state, &wait/2)

      {:feed_focus, watcher} when watcher == state.watcher ->
        write_focus(conn, state, &wait/2)
    end
  end

  defp heartbeat(conn, state, next) do
    case chunk_safely(conn, "event: heartbeat\ndata: {}\n\n") do
      {:ok, conn} -> next.(conn, schedule_heartbeat(state))
      {:error, _reason} -> finish(conn, state, "client_closed")
    end
  end

  defp write_focus(conn, state, next) do
    send(state.watcher, {:feed_focus_woken, self()})

    case relay_step(fn -> state.source.focus(state.ctx) end) do
      {:ok, data} ->
        case chunk_safely(conn, ["event: focus\ndata: ", JSON.encode!(data), "\n\n"]) do
          {:ok, conn} -> next.(conn, state)
          {:error, _reason} -> finish(conn, state, "client_closed")
        end

      {:error, :server_shutdown} ->
        closing(conn, state, "server_shutdown")

      _ ->
        next.(conn, state)
    end
  end

  defp relay_step(fun) do
    fun.()
  catch
    :exit, _reason -> {:error, :server_shutdown}
  end

  # A wake-up left over when the feed ends must not reach a later feed served
  # by this process.
  defp flush_wakeups do
    receive do
      {:change_log, _person, _head} -> flush_wakeups()
      {:feed_focus, _watcher} -> flush_wakeups()
    after
      0 -> :ok
    end
  end

  # A second closing for the same end (two of the principal's scopes) must not
  # end a later feed served by this process.
  defp flush_closings do
    receive do
      {:client_closing, _scope, _reason} -> flush_closings()
    after
      0 -> :ok
    end
  end

  # Shows are held per device; a browser has none.
  defp subscribe_shows(%{kind: :device, key: key}, FeedStep), do: Focus.subscribe_shows(key)
  defp subscribe_shows(_principal, _source), do: :ok

  defp unsubscribe_shows(%{kind: :device, key: key}, FeedStep), do: Focus.unsubscribe_shows(key)
  defp unsubscribe_shows(_principal, _source), do: :ok

  defp flush_shows do
    receive do
      {:client_api_show, _device_key} -> flush_shows()
    after
      0 -> :ok
    end
  end

  defp write_pending_show(conn, state) do
    case pending_show(state.ctx) do
      %{"show_id" => id} = data when id != state.last_show_id ->
        event = ["event: show\ndata: ", JSON.encode!(data), "\n\n"]

        case chunk_safely(conn, event) do
          {:ok, conn} -> wait(conn, %{state | last_show_id: id})
          {:error, _reason} -> finish(conn, state, "client_closed")
        end

      %{"show_id" => _id} ->
        wait(conn, state)

      {:error, :unauthorized} ->
        closing(conn, state, "revoked")

      {:error, :server_shutdown} ->
        closing(conn, state, "server_shutdown")

      nil ->
        wait(conn, state)
    end
  end

  defp pending_show(ctx) do
    Focus.pending_show(ctx.relay, ctx.principal)
  catch
    :exit, _reason -> {:error, :server_shutdown}
  end

  defp send_changes(conn, []), do: {:ok, conn}
  defp send_changes(conn, changes), do: chunk_safely(conn, Enum.map(changes, &event/1))

  @doc false
  def event(%{"rev" => rev, "kind" => kind, "data" => data}),
    do: [
      "id: ",
      Integer.to_string(rev),
      "\nevent: ",
      kind,
      "\ndata: ",
      JSON.encode!(data),
      "\n\n"
    ]

  # The HTTP server raises, rather than returning an error, when a send cannot
  # complete, for example when the peer has gone or its window stays closed.
  defp chunk_safely(conn, data) do
    chunk(conn, data)
  rescue
    error -> {:error, error}
  end

  defp closing(conn, state, reason) do
    conn |> send_closing(reason) |> finish(state, reason)
  end

  defp send_closing(conn, reason) do
    case chunk_safely(conn, [
           "event: closing\ndata: ",
           JSON.encode!(%{"reason" => reason}),
           "\n\n"
         ]) do
      {:ok, conn} -> conn
      {:error, _reason} -> conn
    end
  end

  defp monitor(relay) do
    case GenServer.whereis(relay) do
      pid when is_pid(pid) -> Process.monitor(pid)
      _ -> nil
    end
  end

  # A client that has gone is noticed at the next send, a heartbeat at the
  # latest: the HTTP server reports it then. The server's own messages are
  # left for it in the mailbox.
  defp finish(conn, state, reason) do
    cancel_heartbeat(state)
    if state.relay, do: Process.demonitor(state.relay, [:flush])
    send(state.watcher, {:feed_closed, self(), reason})
    conn
  end

  # The feed's watcher: a small unlinked process that
  #
  # - holds the change-log subscription and passes on at most one wake-up
  #   until the feed acknowledges it, so a feed blocked in a send never
  #   queues one wake-up per commit;
  # - logs the feed's open, and its close with the reason. When the HTTP
  #   server ends the feed's process because the client went away, the feed
  #   cannot log its own close, so the watcher logs `client_closed`.
  #
  # It subscribes before returning, so the feed's first read comes after it.
  defp watch(ctx, source) do
    feed = self()
    scope = source.scope(ctx)

    {pid, ref} =
      spawn_monitor(fn ->
        monitor = Process.monitor(feed)
        :ok = ChangeLog.subscribe(scope)
        send(feed, {:feed_watching, self()})

        watching(%{
          ctx: ctx,
          scope: scope,
          source: source,
          feed: feed,
          monitor: monitor,
          sent: false,
          head: nil,
          focus_sent: false,
          focus_pending: false,
          opened: false
        })
      end)

    receive do
      {:feed_watching, ^pid} ->
        Process.demonitor(ref, [:flush])
        pid

      {:DOWN, ^ref, :process, ^pid, reason} ->
        exit(reason)
    end
  end

  defp watching(%{feed: feed} = w) do
    receive do
      {:change_log, person, head} ->
        if w.sent do
          watching(%{w | head: head})
        else
          send(feed, {:change_log, person, head})
          watching(%{w | sent: true, head: nil})
        end

      {:feed_woken, ^feed} ->
        case w.head do
          nil ->
            watching(%{w | sent: false})

          head ->
            send(feed, {:change_log, w.scope, head})
            watching(%{w | sent: true, head: nil})
        end

      {:focus_change, _data} ->
        if w.focus_sent do
          watching(%{w | focus_pending: true})
        else
          send(feed, {:feed_focus, self()})
          watching(%{w | focus_sent: true})
        end

      :focus_forbidden ->
        watching(%{w | focus_pending: false})

      :agent_focus_granted ->
        if w.opened, do: w.source.focus_granted(w.ctx)
        watching(w)

      {:feed_focus_woken, ^feed} ->
        if w.focus_pending do
          send(feed, {:feed_focus, self()})
          watching(%{w | focus_pending: false})
        else
          watching(%{w | focus_sent: false})
        end

      {:feed_opened, ^feed} ->
        log(w.ctx, "open", nil)
        w.source.opened(w.ctx)

        watching(%{w | opened: true})

      {:feed_closed, ^feed, reason} ->
        log(w.ctx, "close", reason)
        if w.opened, do: w.source.closed(w.ctx)
        watching(%{w | opened: false})

      {:feed_done, ^feed} ->
        if w.opened, do: w.source.closed(w.ctx)
        :ok

      {:DOWN, ref, :process, ^feed, _} when ref == w.monitor ->
        if w.opened do
          log(w.ctx, "close", "client_closed")
          w.source.closed(w.ctx)
        end
    end
  end

  defp schedule_heartbeat(state) do
    cancel_heartbeat(state)
    ref = make_ref()
    timer = Process.send_after(self(), {:feed_heartbeat, ref}, heartbeat_ms())
    %{state | timer: ref, timer_ref: timer}
  end

  defp cancel_heartbeat(%{timer: ref, timer_ref: timer}) when is_reference(timer) do
    Process.cancel_timer(timer)

    receive do
      {:feed_heartbeat, ^ref} -> :ok
    after
      0 -> :ok
    end
  end

  defp cancel_heartbeat(_state), do: :ok

  defp heartbeat_ms,
    do:
      Application.get_env(:attention_server, :client_api_feed_heartbeat_ms, @default_heartbeat_ms)

  defp log(ctx, phase, reason) do
    Logger.info(
      "event=harklight_client_feed request_id=#{ctx.request_id} phase=#{phase}" <>
        if(reason, do: " reason=#{reason}", else: "")
    )
  end
end
