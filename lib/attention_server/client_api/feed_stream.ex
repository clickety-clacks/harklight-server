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
      the Relay exits.
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
  alias AttentionServer.ClientAPI.{Context, Feed, FeedShutdown, FeedStep}

  # Rows per Relay step. Smaller than a batch's 500, so a long backlog
  # interleaves with other work in the Relay.
  @page 100
  @default_heartbeat_ms 20_000

  @doc "Serves the stream from `cursor`. Errors before the stream opens are JSON."
  def serve(conn, %Context{} = ctx, cursor) do
    person = ctx.principal.person

    # Subscribe before the first read: a commit or a closing after the read
    # still reaches us.
    :ok = ChangeLog.subscribe(person)
    :ok = ChangeLog.subscribe_closing(ctx.principal)
    :ok = FeedShutdown.subscribe()

    try do
      open(conn, ctx, cursor)
    after
      ChangeLog.unsubscribe(person)
      ChangeLog.unsubscribe_closing(ctx.principal)
      FeedShutdown.unsubscribe()
      flush_wakeups()
      flush_closings()
    end
  end

  defp open(conn, ctx, cursor) do
    case FeedStep.read(ctx.relay, ctx.principal, cursor, @page) do
      {:ok, changes, more, position} ->
        watcher = watch(ctx)

        conn =
          conn
          |> put_resp_header("content-type", "text/event-stream; charset=utf-8")
          |> put_resp_header("cache-control", "no-cache")
          |> put_resp_header("x-accel-buffering", "no")
          |> send_chunked(200)

        state =
          schedule_heartbeat(%{
            ctx: ctx,
            cursor: position,
            watcher: watcher,
            relay: monitor(ctx.relay),
            timer: nil,
            timer_ref: nil
          })

        case send_changes(conn, changes) do
          {:ok, conn} -> if more, do: pump(conn, state), else: wait(conn, state)
          {:error, _reason} -> finish(conn, state, "client_closed")
        end

      {:error, reason} ->
        Feed.send_read_error(conn, reason)
    end
  end

  # Reads and sends pages until the feed has caught up with the log.
  defp pump(conn, state) do
    case FeedStep.read(state.ctx.relay, state.ctx.principal, state.cursor, @page) do
      {:ok, changes, more, position} ->
        state = %{state | cursor: position}

        case send_changes(conn, changes) do
          {:ok, conn} -> if more, do: pump(conn, state), else: wait(conn, state)
          {:error, _reason} -> finish(conn, state, "client_closed")
        end

      # The principal stopped authenticating before its closing arrived.
      {:error, :unauthorized} ->
        closing(conn, state, "revoked")

      # The cursor fell behind retention while open. Ending lets the client's
      # reconnect get 410 and re-read a snapshot.
      {:error, reason} ->
        finish(conn, state, Atom.to_string(reason))
    end
  catch
    :exit, _reason -> closing(conn, state, "server_shutdown")
  end

  defp wait(conn, state) do
    receive do
      {:change_log, _person, head} when head > state.cursor ->
        flush_wakeups()
        pump(conn, state)

      {:change_log, _person, _head} ->
        wait(conn, state)

      {:client_closing, _scope, reason}
      when reason in [:revoked, :account_paused, :server_shutdown] ->
        closing(conn, state, Atom.to_string(reason))

      {:DOWN, ref, :process, _pid, _reason} when ref == state.relay ->
        closing(conn, state, "server_shutdown")

      {:feed_heartbeat, ref} when ref == state.timer ->
        case chunk_safely(conn, "event: heartbeat\ndata: {}\n\n") do
          {:ok, conn} -> wait(conn, schedule_heartbeat(state))
          {:error, _reason} -> finish(conn, state, "client_closed")
        end
    end
  end

  # Later wake-ups are redundant: the next read covers everything up to now.
  defp flush_wakeups do
    receive do
      {:change_log, _person, _head} -> flush_wakeups()
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
    _ =
      chunk_safely(conn, ["event: closing\ndata: ", JSON.encode!(%{"reason" => reason}), "\n\n"])

    finish(conn, state, reason)
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

  # Logs the feed's open, and its close with the reason. When the HTTP server
  # ends the feed's process because the client went away, the feed cannot log
  # its own close, so a small unlinked watcher logs `client_closed`.
  defp watch(ctx) do
    feed = self()

    spawn(fn ->
      ref = Process.monitor(feed)
      log(ctx, "open", nil)

      receive do
        {:feed_closed, ^feed, reason} -> log(ctx, "close", reason)
        {:DOWN, ^ref, :process, ^feed, _} -> log(ctx, "close", "client_closed")
      end
    end)
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
