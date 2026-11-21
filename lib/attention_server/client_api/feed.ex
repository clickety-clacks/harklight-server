defmodule AttentionServer.ClientAPI.Feed do
  @moduledoc """
  `GET /v2/changes`: the person's change feed (docs/client-api-v2.md §6).

  - `Accept: text/event-stream` holds a Server-Sent Events stream
    (`ClientAPI.FeedStream`): the backlog after the cursor, then live changes.
  - Otherwise one JSON batch: `{changes, rev, more}`, up to `limit` changes
    (default and maximum 500).

  In both forms each change is `{rev, kind, data}`, in increasing `rev` with
  no gap. The cursor is `Last-Event-ID`, which takes precedence, or `?after`;
  no cursor means from the start. A cursor older than the retained log, or
  newer than the person's position, is `410 cursor_expired`.

  The batch's `rev` is the position the batch is complete through: the rev of
  the last log row read. That is the last change's `rev`, or `after` when no
  row was read. A row with no client form, such as a read-position fact, is
  read but not sent, so when such rows end a batch its `rev` is past its last
  change; the client continues from it, and never re-reads rows it cannot see.
  """

  @behaviour AttentionServer.ClientAPI.Routes

  import Plug.Conn

  alias AttentionServer.ClientAPI.{Context, Errors, FeedStep, FeedStream}

  @max_limit 500

  @impl true
  def routes, do: [{"GET", ["changes"], :changes, stream: true}]

  def changes(conn, %Context{} = ctx) do
    conn = fetch_query_params(conn)

    with {:ok, cursor} <- cursor(conn),
         {:ok, limit} <- limit(conn.query_params["limit"]) do
      if event_stream?(conn),
        do: FeedStream.serve(conn, ctx, cursor),
        else: batch(conn, ctx, cursor, limit)
    else
      {:error, field} -> Errors.send_error(conn, "invalid_field", field: field)
    end
  end

  defp batch(conn, ctx, cursor, limit) do
    case FeedStep.read(ctx.relay, ctx.principal, cursor, limit) do
      {:ok, changes, more, position} ->
        Errors.send_json(conn, 200, %{"changes" => changes, "rev" => position, "more" => more})

      {:error, reason} ->
        send_read_error(conn, reason)
    end
  end

  ## Request parsing

  @doc false
  # The resume cursor: `Last-Event-ID` takes precedence over `after` (§5).
  def cursor(conn) do
    case get_req_header(conn, "last-event-id") do
      [value | _] -> position(value, "last_event_id")
      [] -> position(conn.query_params["after"] || "0", "after")
    end
  end

  defp limit(nil), do: {:ok, @max_limit}

  defp limit(value) when is_binary(value) do
    case Integer.parse(value) do
      {n, ""} when n > 0 -> {:ok, min(n, @max_limit)}
      _ -> {:error, "limit"}
    end
  end

  defp limit(_value), do: {:error, "limit"}

  defp position(value, field) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {n, ""} when n >= 0 -> {:ok, n}
      _ -> {:error, field}
    end
  end

  defp position(_value, field), do: {:error, field}

  @doc false
  def event_stream?(conn) do
    conn
    |> get_req_header("accept")
    |> Enum.any?(&String.contains?(&1, "text/event-stream"))
  end

  @doc false
  def send_read_error(conn, :cursor_expired), do: Errors.send_error(conn, "cursor_expired")
  def send_read_error(conn, :unauthorized), do: Errors.send_error(conn, "unauthorized")
  def send_read_error(conn, :disabled), do: Errors.send_error(conn, "not_found")
  def send_read_error(conn, _reason), do: Errors.send_error(conn, "storage_unavailable")
end

defmodule AttentionServer.ClientAPI.FeedStep do
  @moduledoc """
  The feed's read, in the Relay's serialized step. Change-log rows are read
  and rendered from the same committed state, so each row shows its resource
  as of its rev (docs/client-api-v2.md §6.1). Only the read and render run in
  the Relay; the caller encodes and sends.

  Rows render through `ClientAPI.FeedRenderer`. A row the renderer skips, or
  one whose rendering fails, is not sent; a failure is reported to
  diagnostics and never takes down the Relay.
  """

  alias AttentionServer.{ChangeLog, Diagnostics}
  alias AttentionServer.ClientAPI.{FeedRenderer, Reads}

  @doc """
  Up to `limit` rows after `cursor`, rendered for `principal`:
  `{:ok, changes, more, position}`, or `{:error, :cursor_expired | :unauthorized
  | :disabled | :storage_unavailable}`.
  """
  def read(relay, principal, cursor, limit),
    do: GenServer.call(relay, {:client_api, __MODULE__, {:read, principal, cursor, limit}})

  @doc false
  def handle_call({:read, principal, cursor, limit}, _from, state) do
    {:reply, read_rows(state, principal, cursor, limit), state}
  end

  defp read_rows(state, principal, cursor, limit) do
    person = principal.person

    # A principal that no longer authenticates reads nothing, even on a
    # connection opened before its access ended.
    with {:ok, _} <- authorized(state, principal),
         {:ok, rows, more, position} <- ChangeLog.read(state.store, person, cursor, limit) do
      context = %{
        engine: state.store.engine,
        store: state.store,
        view: Map.get(Map.get(state, :priority_views, %{}), person)
      }

      {:ok, Enum.flat_map(rows, &render(context, principal, &1)), more, position}
    end
  rescue
    error ->
      Diagnostics.failure(:client_api, :stream_read, error, :failed)
      {:error, :storage_unavailable}
  end

  defp authorized(state, principal) do
    case Reads.client_principal(state.store.engine, principal) do
      {:ok, _} -> {:ok, principal}
      _ -> {:error, :unauthorized}
    end
  end

  defp render(context, principal, %{rev: rev, kind: kind} = row) do
    case FeedRenderer.render(context, principal, row) do
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
end
