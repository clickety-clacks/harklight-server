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

  A batch's `rev` is the position it is complete through (contract round 12):
  the rev of the last log row read. That is its last change's `rev`, or later
  when trailing rows have no client form, such as a read-position fact, or
  `after` when nothing was read. So a batch of only such rows still advances
  and never loops.
  """

  @behaviour AttentionServer.ClientAPI.Routes

  import Plug.Conn

  alias AttentionServer.JSON
  alias AttentionServer.ClientAPI.{Context, Errors, FeedStep, FeedStream, Projection, SharedCapability}

  @max_limit 500

  @impl true
  def routes, do: [{"GET", ["changes"], :changes, stream: true, granted_agent: true}]

  def changes(conn, %Context{} = ctx) do
    conn = fetch_query_params(conn)
    sse? = event_stream?(conn)
    mode = if sse?, do: :person_sse, else: :headers

    with {:ok, capability} <- SharedCapability.parse(conn, mode),
         {:ok, cursor} <- cursor(conn),
         {:ok, limit} <- limit(conn.query_params["limit"]) do
      ctx = %{ctx | principal: Map.put(ctx.principal, :shared_capability, capability)}

      if sse?,
        do: FeedStream.serve(conn, ctx, cursor),
        else: batch(conn, ctx, cursor, limit)
    else
      {:error, "invalid_field", field} -> Errors.send_error(conn, "invalid_field", field: field)
      {:error, field} -> Errors.send_error(conn, "invalid_field", field: field)
    end
  end

  defp batch(conn, ctx, cursor, limit) do
    case FeedStep.read(ctx.relay, ctx.principal, cursor, limit) do
      {:ok, changes, more, position} ->
        send_batch(conn, changes, more, position)

      {:error, reason} ->
        send_read_error(conn, reason)
    end
  end

  def send_batch(conn, changes, more, position) do
    encoded =
      Enum.map_intersperse(changes, ",", fn %{"data" => data} = change ->
        Projection.run(Projection.change_key(data), fn -> JSON.encode!(change) end)
      end)

    body = [
      "{\"changes\":[",
      encoded,
      "],\"more\":",
      JSON.encode!(more),
      ",\"rev\":",
      Integer.to_string(position),
      "}"
    ]

    conn |> put_resp_content_type("application/json", "utf-8") |> send_resp(200, body)
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

  def limit(nil), do: {:ok, @max_limit}

  def limit(value) when is_binary(value) do
    case Integer.parse(value) do
      {n, ""} when n > 0 -> {:ok, min(n, @max_limit)}
      _ -> {:error, "limit"}
    end
  end

  def limit(_value), do: {:error, "limit"}

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
  def send_read_error(conn, :unsupported), do: Errors.send_error(conn, "unsupported")
  def send_read_error(conn, :cursor_expired), do: Errors.send_error(conn, "cursor_expired")
  def send_read_error(conn, :account_paused), do: Errors.send_error(conn, "account_paused")
  def send_read_error(conn, :unauthorized), do: Errors.send_error(conn, "unauthorized")
  def send_read_error(conn, :disabled), do: Errors.send_error(conn, "not_found")
  def send_read_error(conn, %Projection{} = fault), do: Errors.projection_error(conn, fault)
  def send_read_error(conn, _reason), do: Errors.send_error(conn, "storage_unavailable")
end

defmodule AttentionServer.ClientAPI.FeedStep do
  @moduledoc """
  The feed's read, in the Relay's serialized step. Change-log rows are read
  and rendered from the same committed state. Catch-up emits only the newest
  row for each Entry, while the raw page still determines the continuation
  position (docs/client-api-v2.md §6.1). Only the read and render run in the
  Relay; the caller encodes and sends.

  Rows render through `ClientAPI.FeedRenderer` (replaceable in tests through
  the `:client_api_feed_renderer` application setting). A row with no wire
  form can be skipped. A projection failure stops the step at its input
  cursor and never takes down the Relay.
  """

  alias AttentionServer.{ChangeLog, Diagnostics}
  alias AttentionServer.ClientAPI.{FeedRenderer, Projection, Reads, SharedCapability, SharedContinuation}

  @doc false
  def scope(ctx), do: ctx.principal.person

  @doc false
  def read(ctx, cursor, limit), do: read(ctx.relay, ctx.principal, cursor, limit)

  @doc false
  def read_live(ctx, cursor, limit),
    do: read_step(ctx.relay, ctx.principal, cursor, limit, :live)

  @doc false
  def opened(_ctx), do: :ok

  @doc false
  def closed(_ctx), do: :ok

  @doc false
  def focus(_ctx), do: :skip

  @doc """
  Up to `limit` rows after `cursor`, rendered for `principal`:
  `{:ok, changes, more, position}`, or `{:error, :cursor_expired | :unauthorized
  | :disabled | :storage_unavailable}`.
  """
  def read(relay, principal, cursor, limit),
    do: read_step(relay, principal, cursor, limit, :catch_up)

  defp read_step(relay, principal, cursor, limit, mode),
    do:
      GenServer.call(
        relay,
        {:client_api, __MODULE__, {:read, principal, cursor, limit, mode}},
        # As the reads do: a busy Relay is waited for, not mistaken for a
        # stopped one. A Relay that exits still ends the call.
        :infinity
      )

  @doc false
  def handle_call({:read, principal, cursor, limit, mode}, _from, state) do
    {:reply, read_rows(state, principal, cursor, limit, mode), state}
  end

  defp read_rows(state, principal, cursor, limit, mode) do
    person = principal.person

    # A principal that no longer authenticates reads nothing, even on a
    # connection opened before its access ended.
    with {:ok, _} <- authorized(state, principal),
         :ok <- validate_capability(state, principal, cursor),
         :ok <- ChangeLog.ranks_available(state.store, person),
         {:ok, rows, more, position} <- ChangeLog.read(state.store, person, cursor, limit) do
      context = %{
        engine: state.store.engine,
        store: state.store,
        shared?: shared?(principal),
        capability: Map.get(principal, :shared_capability),
        compact_entries?: mode == :catch_up,
        view: Map.get(Map.get(state, :priority_views, %{}), person)
      }

      with {:ok, changes} <- Projection.capture(fn -> render_rows(context, principal, rows) end),
           do: {:ok, changes, more, position}
    else
      {:error, reason} when reason in [:cursor_expired, :unauthorized, :disabled, :unsupported] ->
        {:error, reason}

      # Any other failure of the store is a storage failure to the client.
      {:error, reason} ->
        Diagnostics.failure(:client_api, :stream_read, reason, :failed)
        {:error, :storage_unavailable}
    end
  rescue
    error ->
      Diagnostics.failure(:client_api, :stream_read, error, :failed)
      {:error, :storage_unavailable}
  end

  defp validate_capability(state, %{shared_capability: %{shared?: true, continuation: token} = capability} = principal, cursor) do
    engine = state.store.engine

    if SharedCapability.readable?(state, principal.person, capability) do
      scope = SharedCapability.scope(capability, %{})
      SharedContinuation.validate(token, engine.agent_cursor_key, principal, :person_changes, scope, cursor, state.clock.())
    else
      {:error, :unsupported}
    end
  end

  defp validate_capability(_state, %{shared_capability: %{continuation: token}}, _cursor)
       when is_binary(token),
       do: {:error, :cursor_expired}

  defp validate_capability(_state, _principal, _cursor), do: :ok

  defp shared?(%{shared_capability: %{shared?: true}}), do: true
  defp shared?(_principal), do: false

  defp authorized(state, principal) do
    case Reads.client_principal(state.store.engine, principal) do
      {:ok, _} -> {:ok, principal}
      _ -> {:error, :unauthorized}
    end
  end

  defp render_rows(context, principal, rows) do
    Projection.run(:feed, fn ->
      renderer = renderer()

      results =
        if renderer == FeedRenderer or
             (Code.ensure_loaded?(renderer) and function_exported?(renderer, :render_many, 3)) do
          renderer.render_many(context, principal, rows)
        else
          Enum.map(rows, fn row ->
            {row, renderer.render(context, principal, row)}
          end)
        end

      changes(results)
    end)
  end

  @doc false
  def changes(results) do
    # A row the renderer could not render because its store failed fails the
    # whole step: dropping it would leave a silent gap. A :skip row has no
    # client form and is skipped.
    if Enum.any?(results, &match?({_row, {:error, :storage_unavailable}}, &1)) do
      {:error, :storage_unavailable}
    else
      changes =
        for {%{rev: rev, kind: kind}, {:ok, data}} when is_map(data) <- results,
            do: %{"rev" => rev, "kind" => Atom.to_string(kind), "data" => data}

      {:ok, changes}
    end
  end

  defp renderer,
    do:
      Application.get_env(
        :attention_server,
        :client_api_feed_renderer,
        FeedRenderer
      )
end
