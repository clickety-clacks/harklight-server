defmodule AttentionServer.DashboardHTTPS do
  @moduledoc """
  Opt-in HTTPS host page for one-use owner sign-in and authorized device metadata.

  The operator pins its name, TLS certificate, account-owner person and port.
  The listener does not start unless the server receives dashboard configuration.

  In `saas` deployment mode no account-owner person is configured, so the
  account-wide view is unreachable; a configured public origin replaces the
  listener's own host and port in links and in the form-origin check.
  """

  use Supervisor

  alias AttentionServer.{
    DashboardBackend,
    DashboardBrowserSession,
    DashboardReadHTTP,
    Diagnostics,
    Relay
  }

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    relay = Keyword.fetch!(opts, :relay)
    hosted? = Keyword.get(opts, :deployment_mode, :self_hosted) == :saas
    owner_person = if hosted?, do: nil, else: Keyword.fetch!(opts, :owner_person)
    sessions_name = {:global, {__MODULE__, relay}}

    backend = %DashboardBackend{
      relay: relay,
      sessions: sessions_name,
      configured_owner_person: owner_person,
      origin: Relay.dashboard_origin(relay)
    }

    children = [
      Supervisor.child_spec(
        {DashboardBrowserSession,
         relay: relay,
         consume: fn token -> Relay.consume_browser_sign_in(relay, token) end,
         name: sessions_name},
        id: :dashboard_sessions
      ),
      Supervisor.child_spec(
        {Bandit,
         plug:
           {DashboardReadHTTP,
            backend: {DashboardBackend, backend},
            skill_path: Keyword.fetch!(opts, :skill_path),
            expected_origin: Keyword.get(opts, :public_origin),
            hosted: hosted?,
            hosted_addresses: Keyword.get(opts, :hosted_addresses, %{}),
            skill_path_saas: Keyword.get(opts, :skill_path_saas),
            skill_path_core: Keyword.get(opts, :skill_path_core)},
         scheme: :https,
         ip: Keyword.fetch!(opts, :ip),
         port: Keyword.fetch!(opts, :port),
         certfile: Keyword.fetch!(opts, :certfile),
         keyfile: Keyword.fetch!(opts, :keyfile),
         http_2_options: [enabled: false],
         startup_log: false},
        id: :dashboard_listener
      )
    ]

    Diagnostics.result(
      :dashboard_http,
      :listener_start,
      Supervisor.init(children, strategy: :rest_for_one),
      :unavailable
    )
  rescue
    error ->
      Diagnostics.failure(:dashboard_http, :configuration, error, :unavailable)
      reraise error, __STACKTRACE__
  catch
    kind, reason ->
      Diagnostics.failure(:dashboard_http, :configuration, {kind, reason}, :unavailable)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end
end
