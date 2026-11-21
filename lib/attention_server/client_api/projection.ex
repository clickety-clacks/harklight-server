defmodule AttentionServer.ClientAPI.Projection do
  @moduledoc false

  def run(key, render) do
    inject_fault(key)
    render.()
  end

  if Mix.env() == :test do
    defp inject_fault(key) do
      case Application.get_env(:attention_server, :client_api_projection_fault) do
        nil -> :ok
        probe -> probe.(key)
      end
    end
  else
    defp inject_fault(_key), do: :ok
  end
end
