defmodule AttentionServer.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children =
      case Application.get_env(:attention_server, :server) do
        nil -> []
        opts when is_list(opts) -> [{AttentionServer.Server, opts}]
      end

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: AttentionServer.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
