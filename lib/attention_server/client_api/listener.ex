defmodule AttentionServer.ClientAPI.Listener do
  @moduledoc """
  The device listener for client API v2: Bandit over TLS 1.3 with mutual TLS,
  HTTP/2 through ALPN and HTTP/1.1, serving `/v2` only.

  It is the only listener on the device port. It admits enrolled device
  certificates and owner-registered agent certificates.
  """

  alias AttentionServer.ClientAPI.Router
  alias AttentionServer.EnrolledLeafVerify

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      restart: :permanent,
      shutdown: 5_000
    }
  end

  @doc """
  Options: `relay`, `certfile`, `keyfile`, `cacertfile`, `port` (default 0),
  `ip` (default loopback), and `routes` / `max_in_flight` for the router.
  """
  def start_link(opts) do
    relay = Keyword.fetch!(opts, :relay)

    Bandit.start_link(
      plug:
        {Router, [relay: relay, auth: :device] ++ Keyword.take(opts, [:routes, :max_in_flight])},
      scheme: :https,
      ip: Keyword.get(opts, :ip, {127, 0, 0, 1}),
      port: Keyword.get(opts, :port, 0),
      certfile: Keyword.fetch!(opts, :certfile),
      keyfile: Keyword.fetch!(opts, :keyfile),
      startup_log: false,
      thousand_island_options: [
        transport_options: [
          versions: [:"tlsv1.3"],
          verify: :verify_peer,
          verify_fun: EnrolledLeafVerify.client_api(relay),
          fail_if_no_peer_cert: true,
          cacertfile: Keyword.fetch!(opts, :cacertfile),
          reuseaddr: true,
          session_tickets: :disabled,
          early_data: :disabled
        ]
      ]
    )
  end

  @doc "The bound port."
  def port(listener) do
    {:ok, {_ip, port}} = ThousandIsland.listener_info(listener)
    port
  end
end
