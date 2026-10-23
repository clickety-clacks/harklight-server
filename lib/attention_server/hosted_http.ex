defmodule AttentionServer.HostedHTTP do
  @moduledoc """
  Loopback-only plain HTTP interface for the hosted account component.

  It runs only in `saas` deployment mode on a port the operator pins, and is
  meant to be reached by the hosted component on the same machine; it carries
  no credential of its own. Three bounded JSON posts exist: create or find the
  person for an account, issue a sign-in link for that person, and put the one
  designated test account into a named product state. Nothing here reads or
  changes another person's data: every call acts through the server-held owner
  key of the one hosted person it names.
  """

  use Supervisor

  alias AttentionServer.{Diagnostics, HostedRequests}

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts)

  @doc "The bound port, so an ephemeral port can be read back."
  def port(server) do
    case Enum.find(Supervisor.which_children(server), fn {id, _, _, _} ->
           id == :hosted_listener
         end) do
      {:hosted_listener, pid, _, _} when is_pid(pid) ->
        {:ok, {_ip, port}} = ThousandIsland.listener_info(pid)
        port

      _ ->
        raise "hosted listener is unavailable"
    end
  end

  @impl true
  def init(opts) do
    children = [
      Supervisor.child_spec(
        {Bandit,
         plug: {HostedRequests, relay: Keyword.fetch!(opts, :relay)},
         scheme: :http,
         ip: Keyword.fetch!(opts, :ip),
         port: Keyword.fetch!(opts, :port),
         http_2_options: [enabled: false],
         startup_log: false},
        id: :hosted_listener
      )
    ]

    Diagnostics.result(
      :hosted_http,
      :listener_start,
      Supervisor.init(children, strategy: :one_for_one),
      :unavailable
    )
  rescue
    error ->
      Diagnostics.failure(:hosted_http, :configuration, error, :unavailable)
      reraise error, __STACKTRACE__
  end
end
