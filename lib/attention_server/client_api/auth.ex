defmodule AttentionServer.ClientAPI.Principal do
  @moduledoc """
  Who a client API request is from: one person and one client.

  - `kind: :device` — an enrolled device on the device listener. `key` is the
    device key (the SHA-256 of its certificate's public key).
  - `kind: :browser` — a signed-in browser session on the web origin. `key` is
    the owner key the session was issued for, `session_id` the session's id
    (the raw session hash, as the Relay and the change log's closing notices
    name it; never sent to a client) and `owner_revision` the owner revision the
    session is pinned to.

  Authorization is by principal, with the same rules as the same operation
  today.
  """

  @enforce_keys [:person, :kind, :key]
  defstruct [:person, :kind, :key, :session_id, :owner_revision]

  @type t :: %__MODULE__{
          person: String.t(),
          kind: :device | :browser,
          key: String.t(),
          session_id: binary() | nil,
          owner_revision: term()
        }
end

defmodule AttentionServer.ClientAPI.Auth do
  @moduledoc """
  The two client API authenticators. Both resolve to a `ClientAPI.Principal`.

  - **Device**: the device listener's TLS handshake admits only a currently
    enrolled certificate (`EnrolledLeafVerify.device/1`). Each request checks the
    enrollment again, so a device revoked while its connection is open has its
    next request refused.
  - **Browser**: the existing sign-in cookie session on the web origin. A
    missing, ended or expired session is refused. Writes also require the exact
    web origin, as the existing browser routes do.
  """

  alias AttentionServer.{DashboardBrowserHTTP, DeviceIdentity, Relay}
  alias AttentionServer.ClientAPI.Principal

  @cookie "__Host-harklight"
  @write_methods ~w(POST PUT PATCH DELETE)

  @doc "The device principal for a request on the device listener."
  @spec device(Plug.Conn.t(), GenServer.server()) ::
          {:ok, Principal.t()} | {:error, :unauthorized}
  def device(conn, relay) do
    with %{ssl_cert: der} when is_binary(der) <- Plug.Conn.get_peer_data(conn),
         {:ok, key} <- DeviceIdentity.from_certificate(der),
         {:ok, person} when is_binary(person) <- Relay.device_person(relay, key) do
      {:ok, %Principal{person: person, kind: :device, key: key}}
    else
      _ -> {:error, :unauthorized}
    end
  end

  @doc """
  The browser principal for a request on the web origin.

  `backend` is the web origin's dashboard backend (`{module, context}` or a
  module), which resolves the sign-in cookie.
  """
  @spec browser(Plug.Conn.t(), term()) :: {:ok, Principal.t()} | {:error, :unauthorized}
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
      _ -> {:error, :unauthorized}
    end
  rescue
    _ -> {:error, :unauthorized}
  end

  @doc """
  Whether a browser request may proceed past the origin check. Reads pass.
  Writes need the exact web origin (`expected_origin`, or the request's own
  host and port when none is configured).
  """
  @spec browser_origin_ok?(Plug.Conn.t(), String.t() | nil) :: boolean()
  def browser_origin_ok?(%Plug.Conn{method: method} = conn, expected_origin)
      when method in @write_methods,
      do: DashboardBrowserHTTP.exact_origin?(conn, %{expected_origin: expected_origin})

  def browser_origin_ok?(_conn, _expected_origin), do: true

  defp backend_call({module, context}, operation, value),
    do: apply(module, operation, [context, value])

  defp backend_call(module, operation, value) when is_atom(module),
    do: apply(module, operation, [value])
end
