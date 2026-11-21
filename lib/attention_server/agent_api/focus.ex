defmodule AttentionServer.AgentAPI.Focus do
  @moduledoc false

  def notify_grant(person, key) do
    registry = AttentionServer.ChangeLog.Registry

    if Process.whereis(registry) do
      Registry.dispatch(registry, {:agent, person, key}, fn watchers ->
        for {pid, _} <- watchers, do: send(pid, :agent_focus_granted)
      end)
    end

    :ok
  end
end
