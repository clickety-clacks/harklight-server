defmodule AttentionServer.ClientAPI.Routes do
  @moduledoc """
  The route-registration interface of the client API router.

  A component exposes its endpoints from one module that implements this
  behaviour, and adds that module to `AttentionServer.ClientAPI.Router`'s
  `@route_modules`. Nothing else is registered anywhere.

      defmodule AttentionServer.ClientAPI.Reads do
        @behaviour AttentionServer.ClientAPI.Routes

        @impl true
        def routes do
          [
            {"GET", ["inbox"], :inbox},
            {"GET", ["items", :item_id], :item},
            {"GET", ["items", :item_id, "entries"], :entries}
          ]
        end

        def item(conn, %AttentionServer.ClientAPI.Context{} = ctx) do
          # ctx.principal, ctx.relay, ctx.params.item_id, ctx.request_id
          AttentionServer.ClientAPI.Errors.send_json(conn, 200, %{...})
        end
      end

  A route is `{method, segments, handler}` or `{method, segments, handler, opts}`:

  - `segments` is the path after `/v2`. A string matches literally. An atom
    captures that segment, percent-decoded, into `ctx.params` under that atom.
  - `handler` is a function of the module, called as `handler(conn, ctx)`. It
    must send the response, using `ClientAPI.Errors.send_json/3` or
    `send_error/3`, or for a stream `Plug.Conn.send_chunked/2`.
  - `opts`: `stream: true` exempts a long-lived response, such as the change
    feed, from the in-flight request limit. Every other request, matched or
    not, counts toward the limit from before authentication.

  Before a handler runs, the router has set `Request-Id`, authenticated the
  principal (401 otherwise), refused a paused account (403 `account_paused`,
  on every request except `GET /v2/me`), applied the browser write-origin check
  (403) and the load limit (503 with `Retry-After`). Unmatched requests get 404
  `not_found`. A handler that raises, or exits from a Relay call, answers 503
  `retry_later`. Request bodies are not read by the router.
  """

  @type segment :: String.t() | atom()
  @type route ::
          {String.t(), [segment()], atom()}
          | {String.t(), [segment()], atom(), keyword()}

  @callback routes() :: [route()]
end

defmodule AttentionServer.ClientAPI.Context do
  @moduledoc "What a client API handler receives besides the connection."

  @enforce_keys [:principal, :relay, :request_id]
  defstruct [:principal, :relay, :request_id, params: %{}]

  @type t :: %__MODULE__{
          principal: AttentionServer.ClientAPI.Principal.t(),
          relay: GenServer.server(),
          request_id: String.t(),
          params: %{optional(atom()) => String.t()}
        }
end

defmodule AttentionServer.ClientAPI.Router do
  @moduledoc """
  The one Plug router for `/v2`, mounted on the device listener (mutual TLS)
  and on the web origin (sign-in cookie). See `AttentionServer.ClientAPI.Routes`
  for how components add endpoints.

  Options:

  - `relay` (required)
  - `auth`: `:device`, or `{:browser, backend, expected_origin}`
  - `routes`: route modules, replacing `@route_modules` (tests)
  - `max_in_flight`: concurrent non-stream requests before `503 retry_later`
  """

  @behaviour Plug

  import Plug.Conn

  require Logger

  alias AttentionServer.ClientAPI.{Auth, Context, Errors}
  alias AttentionServer.{Diagnostics, Relay}

  # Components add their route module here: C3 reads, C4 writes, C5 the feed.
  @route_modules [
    AttentionServer.ClientAPI.Reads,
    AttentionServer.ClientAPI.Writes,
    AttentionServer.ClientAPI.Feed
  ]

  @default_max_in_flight 512

  @impl Plug
  def init(opts) do
    modules = Keyword.get(opts, :routes, @route_modules)

    %{
      relay: Keyword.fetch!(opts, :relay),
      auth: auth_option(Keyword.get(opts, :auth, :device)),
      routes: compile_routes(modules),
      max_in_flight: Keyword.get(opts, :max_in_flight, @default_max_in_flight),
      in_flight: :atomics.new(1, signed: true)
    }
  end

  @impl Plug
  def call(conn, opts) do
    started = System.monotonic_time()
    request_id = new_request_id()

    conn =
      conn
      |> assign(:client_api_request_id, request_id)
      |> put_resp_header("request-id", request_id)
      |> register_before_send(&log_response(&1, started))

    case conn.path_info do
      ["v2" | segments] ->
        route = match(opts.routes, conn.method, segments)

        # The load limit is counted before authentication, which calls the
        # Relay, so a saturated server sheds requests instead of queueing them.
        if stream?(route),
          do: authenticated(conn, opts, segments, route, request_id),
          else:
            limited(conn, opts, fn -> authenticated(conn, opts, segments, route, request_id) end)

      _ ->
        Errors.send_error(conn, "not_found")
    end
  end

  defp stream?({:ok, _module, _handler, _params, route_opts}),
    do: Keyword.get(route_opts, :stream, false)

  defp stream?(:nomatch), do: false

  defp authenticated(conn, opts, segments, route, request_id) do
    case authenticate(conn, opts) do
      {:ok, principal} ->
        paused = paused?(conn, opts, segments, principal)

        cond do
          paused == :unavailable ->
            Errors.send_error(conn, "retry_later")

          paused ->
            Errors.send_error(conn, "account_paused")

          not origin_ok?(conn, opts.auth) ->
            Errors.send_error(conn, "forbidden")

          true ->
            dispatch(conn, opts, route, %Context{
              principal: principal,
              relay: opts.relay,
              request_id: request_id
            })
        end

      {:error, :unauthorized} ->
        conn
        |> close_after_device_refusal(opts.auth)
        |> Errors.send_error("unauthorized")

      {:error, :unavailable} ->
        Errors.send_error(conn, "retry_later")
    end
  end

  defp authenticate(conn, %{auth: :device, relay: relay}) do
    Auth.device(conn, relay)
  catch
    :exit, _reason -> {:error, :unavailable}
  end

  defp authenticate(conn, %{auth: {:browser, backend, _origin}}) do
    Auth.browser(conn, backend)
  catch
    :exit, _reason -> {:error, :unavailable}
  end

  # A paused account may still read who it is: `GET /v2/me` reports the pause.
  defp paused?(%Plug.Conn{method: "GET"}, _opts, ["me"], _principal), do: false

  defp paused?(_conn, opts, _segments, principal) do
    Relay.account_paused?(opts.relay, principal.person) == true
  catch
    :exit, _reason -> :unavailable
  end

  defp origin_ok?(conn, {:browser, _backend, origin}), do: Auth.browser_origin_ok?(conn, origin)
  defp origin_ok?(_conn, :device), do: true

  # A device whose enrollment ended while its connection was open: refuse, and
  # end an HTTP/1.1 connection so the next attempt meets the TLS handshake.
  defp close_after_device_refusal(conn, :device) do
    if get_http_protocol(conn) == :"HTTP/1.1",
      do: put_resp_header(conn, "connection", "close"),
      else: conn
  end

  defp close_after_device_refusal(conn, _auth), do: conn

  defp dispatch(conn, opts, route, ctx) do
    case route do
      {:ok, module, handler, params, _route_opts} ->
        run(conn, module, handler, %{ctx | params: params}, opts)

      :nomatch ->
        Errors.send_error(conn, "not_found")
    end
  end

  defp limited(conn, %{in_flight: counter, max_in_flight: max}, fun) do
    if :atomics.add_get(counter, 1, 1) > max do
      :atomics.sub(counter, 1, 1)
      Errors.send_error(conn, "retry_later")
    else
      try do
        fun.()
      after
        :atomics.sub(counter, 1, 1)
      end
    end
  end

  defp run(conn, module, handler, ctx, opts) do
    case apply(module, handler, [conn, ctx]) do
      %Plug.Conn{state: state} = sent when state in [:sent, :chunked, :file] ->
        sent

      %Plug.Conn{} = unsent ->
        Diagnostics.failure(:client_api, :request_dispatch, :no_response, :outcome_unknown)
        Errors.send_error(unsent, "retry_later")
    end
  rescue
    error ->
      Diagnostics.failure(:client_api, :request_dispatch, error, :outcome_unknown)
      unless_sent(conn, &Errors.send_error(&1, "retry_later"))
  catch
    :exit, reason ->
      Diagnostics.failure(:client_api, :relay_boundary, reason, :outcome_unknown, nil, opts.relay)
      unless_sent(conn, &Errors.send_error(&1, "retry_later"))
  end

  defp unless_sent(%Plug.Conn{state: :unset} = conn, fun), do: fun.(conn)
  defp unless_sent(%Plug.Conn{state: :set} = conn, fun), do: fun.(conn)
  defp unless_sent(conn, _fun), do: conn

  @doc false
  def match(routes, method, segments) do
    Enum.find_value(routes, :nomatch, fn {route_method, pattern, module, handler, route_opts} ->
      with true <- route_method == method,
           {:ok, params} <- match_segments(pattern, segments, %{}) do
        {:ok, module, handler, params, route_opts}
      else
        _ -> nil
      end
    end)
  end

  defp match_segments([], [], params), do: {:ok, params}

  defp match_segments([literal | pattern], [literal | segments], params)
       when is_binary(literal),
       do: match_segments(pattern, segments, params)

  defp match_segments([capture | pattern], [segment | segments], params)
       when is_atom(capture) do
    case decode_segment(segment) do
      {:ok, value} -> match_segments(pattern, segments, Map.put(params, capture, value))
      :error -> :nomatch
    end
  end

  defp match_segments(_pattern, _segments, _params), do: :nomatch

  defp decode_segment(segment) do
    value = URI.decode(segment)
    if value != "" and String.valid?(value), do: {:ok, value}, else: :error
  rescue
    ArgumentError -> :error
  end

  defp compile_routes(modules) do
    Enum.flat_map(modules, fn module ->
      Enum.map(module.routes(), fn
        {method, pattern, handler} -> {method, pattern, module, handler, []}
        {method, pattern, handler, route_opts} -> {method, pattern, module, handler, route_opts}
      end)
    end)
  end

  defp auth_option(:device), do: :device

  defp auth_option({:browser, backend, origin}) when is_binary(origin) or is_nil(origin),
    do: {:browser, backend, origin}

  # Opaque, server-issued and free of identity: 128 random bits.
  defp new_request_id, do: Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)

  defp log_response(conn, started) do
    if conn.status >= 400 do
      duration_ms =
        System.convert_time_unit(System.monotonic_time() - started, :native, :millisecond)

      Logger.info(
        "event=harklight_client_api request_id=#{conn.assigns.client_api_request_id} " <>
          "status=#{conn.status} error=#{conn.assigns[:client_api_error] || "none"} " <>
          "duration_ms=#{duration_ms}"
      )
    end

    conn
  end
end

defmodule AttentionServer.ClientAPI.WebOrigin do
  @moduledoc """
  The web origin's plug: `/v2` goes to the client API router when
  `client_api_v2` is on, and answers 404 when it is off. Every other path goes
  to the dashboard unchanged.
  """

  @behaviour Plug

  alias AttentionServer.ClientAPI.Router

  @impl Plug
  def init(opts) do
    {dashboard, dashboard_opts} = Keyword.fetch!(opts, :dashboard)

    client_api =
      if Keyword.get(opts, :client_api_v2, false) == true do
        [
          relay: Keyword.fetch!(opts, :relay),
          auth: {:browser, Keyword.fetch!(opts, :backend), Keyword.get(opts, :expected_origin)}
        ]
        |> Keyword.merge(Keyword.take(opts, [:routes, :max_in_flight]))
        |> Router.init()
      end

    %{dashboard: {dashboard, dashboard.init(dashboard_opts)}, client_api: client_api}
  end

  @impl Plug
  def call(%Plug.Conn{path_info: ["v2" | _]} = conn, %{client_api: nil}),
    do: Plug.Conn.send_resp(conn, 404, "Not found")

  def call(%Plug.Conn{path_info: ["v2" | _]} = conn, %{client_api: client_api}),
    do: Router.call(conn, client_api)

  def call(conn, %{dashboard: {dashboard, dashboard_opts}}),
    do: dashboard.call(conn, dashboard_opts)
end
