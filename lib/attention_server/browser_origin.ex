defmodule AttentionServer.BrowserOrigin do
  @moduledoc """
  The exact origin browser requests must come from: the configured public
  origin when the server sits behind a front end (`:expected_origin`),
  otherwise this listener's own `https` host and port. Used by the dashboard's
  form, join and sign-in checks and by the client API's write-origin check.
  """

  @spec expected(Plug.Conn.t(), map()) :: String.t()
  def expected(_conn, %{expected_origin: origin}) when is_binary(origin), do: origin

  def expected(conn, _opts) do
    if conn.port == 443,
      do: "https://" <> conn.host,
      else: "https://" <> conn.host <> ":" <> Integer.to_string(conn.port)
  end

  @doc "Whether the request carries exactly one `Origin` header equal to `expected/2`."
  @spec exact?(Plug.Conn.t(), map()) :: boolean()
  def exact?(conn, opts), do: Plug.Conn.get_req_header(conn, "origin") == [expected(conn, opts)]
end
