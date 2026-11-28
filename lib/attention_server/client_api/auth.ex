defmodule AttentionServer.ClientAPI.Principal do
  @moduledoc """
  Who a client API request is from: one person and one client.

  - `kind: :device` — an enrolled device authenticated by certificate or
    native bearer. For a bearer, `session_id` is the lowercase hexadecimal
    session hash, and `pending` records whether certificate migration is
    awaiting activation.
  - `kind: :agent` — an owner-registered agent on the device listener.
    `key` is the enrolled agent key.
  - `kind: :browser` — a signed-in browser session on the web origin. `key` is
    the owner key the session was issued for, `session_id` the session's id
    (the raw session hash, as the Relay and the change log's closing notices
    name it; never sent to a client) and `owner_revision` the owner revision the
    session is pinned to.

  Authorization is by principal, with the same rules as the same operation
  today.
  """

  @enforce_keys [:person, :kind, :key]
  defstruct [:person, :kind, :key, :session_id, :owner_revision, :request_id, pending: false]

  @type t :: %__MODULE__{
          person: String.t(),
          kind: :device | :browser | :agent,
          key: String.t(),
          session_id: binary() | nil,
          owner_revision: term(),
          pending: boolean()
        }
end

defmodule AttentionServer.ClientAPI.Auth do
  @moduledoc """
  The two client API authenticators. Both resolve to a `ClientAPI.Principal`.

  - **Device listener**: enrolled device and owner-registered agent
    certificates. Each request checks enrollment again.
  - **Browser**: the existing sign-in cookie session on the web origin. A
    missing, ended or expired session is refused. Writes also require the exact
    web origin, as the existing browser routes do.
  """

  alias AttentionServer.{BrowserOrigin, DeviceIdentity, Relay}
  alias AttentionServer.ClientAPI.Principal
  alias AttentionServer.Repository.Postgres.NativeSessions

  @cookie "__Host-harklight"
  @write_methods ~w(POST PUT PATCH DELETE)

  @doc "The enrolled principal for a request on the device listener."
  @spec device(Plug.Conn.t(), GenServer.server()) ::
          {:ok, Principal.t()} | {:error, :unauthorized}
  def device(conn, relay) do
    with %{ssl_cert: der} when is_binary(der) <- Plug.Conn.get_peer_data(conn),
         {:ok, key} <- DeviceIdentity.from_certificate(der) do
      enrolled_principal(relay, key)
    else
      _ -> {:error, :unauthorized}
    end
  end

  defp enrolled_principal(relay, key) do
    case Relay.certificate_device_person(relay, key) do
      {:ok, person} when is_binary(person) ->
        {:ok, %Principal{person: person, kind: :device, key: key}}

      _ ->
        case Relay.agent_identity(relay, key) do
          {:ok, person, _id, _revision} ->
            {:ok, %Principal{person: person, kind: :agent, key: key}}

          _ ->
            {:error, :unauthorized}
        end
    end
  end

  @doc """
  The browser principal for a request on the web origin.

  `backend` is the web origin's dashboard backend (`{module, context}` or a
  module), which resolves the sign-in cookie.
  """
  @spec browser(Plug.Conn.t(), term()) :: {:ok, Principal.t()} | {:error, :unauthorized | :unavailable}
  def browser(conn, backend) do
    conn = Plug.Conn.fetch_cookies(conn)

    with cookie when is_binary(cookie) <- Map.get(conn.req_cookies, @cookie),
         {:ok, %{person: person, key: key, browser_session_id: session, revision: revision}}
         when is_binary(person) and is_binary(key) and is_binary(session) <-
           backend_call(backend, :browser_context, cookie) do
      {:ok,
       %Principal{
         person: person,
         kind: :browser,
         key: key,
         session_id: session,
         owner_revision: revision
       }}
    else
      {:error, :storage_unavailable} -> {:error, :unavailable}
      _ -> {:error, :unauthorized}
    end
  rescue
    _ -> {:error, :unauthorized}
  end

  def web(conn, backend, relay) do
    conn = Plug.Conn.fetch_cookies(conn)
    cookie = Map.get(conn.req_cookies, @cookie)

    case {cookie, Plug.Conn.get_req_header(conn, "authorization")} do
      {_, []} ->
        browser(conn, backend)

      {nil, ["Bearer " <> secret]} ->
        with {:ok, %{device_id: device, person_id: person, hash: hash, pending: pending}} <-
               native_session(relay, secret) do
          {:ok, %Principal{kind: :device, key: device, person: person, session_id: hash, pending: pending}}
        else
          {:error, :storage_unavailable} -> {:error, :unavailable}
          _ -> {:error, :unauthorized}
        end

      _ ->
        {:error, :unauthorized}
    end
  end

  @doc """
  Whether a browser request may proceed past the origin check. Reads pass.
  Writes need the exact web origin (`expected_origin`, or the request's own
  host and port when none is configured).
  """
  @spec browser_origin_ok?(Plug.Conn.t(), String.t() | nil) :: boolean()
  def browser_origin_ok?(%Plug.Conn{method: method} = conn, expected_origin)
      when method in @write_methods,
      do: BrowserOrigin.exact?(conn, %{expected_origin: expected_origin})

  def browser_origin_ok?(_conn, _expected_origin), do: true

  defp native_session(:postgres, secret), do: NativeSessions.resolve(secret)
  defp native_session(relay, secret), do: Relay.native_session(relay, secret)

  defp backend_call({module, context}, operation, value),
    do: apply(module, operation, [context, value])

  defp backend_call(module, operation, value) when is_atom(module),
    do: apply(module, operation, [value])
end
