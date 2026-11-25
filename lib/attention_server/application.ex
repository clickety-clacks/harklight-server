defmodule AttentionServer.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    # Change-log subscribers (client API v2 feeds) register here per person.
    registry = {Registry, keys: :duplicate, name: AttentionServer.ChangeLog.Registry}

    children =
      case Application.get_env(:attention_server, :server) do
        nil ->
          [registry]

        opts when is_list(opts) ->
          [registry, {AttentionServer.Server, opts}]
      end

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: AttentionServer.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
