defmodule AttentionServer.RetiredSettings do
  @moduledoc """
  Server settings that 0.2.0 retired along with their listeners. The Server
  ignores them; at startup it names each one that is still set, so an upgraded
  installation can remove it. Values are not logged.
  """

  require Logger

  @retired ~w(
    ATTENTION_SERVER_AGENT_PORT
    ATTENTION_SERVER_AGENT_BIND
    ATTENTION_SERVER_ORIGIN_PORT
    ATTENTION_SERVER_ORIGIN_BIND
  )

  @doc "Logs one notice per retired setting present in `env` and returns their names."
  def notice(env \\ System.get_env()) do
    present = Enum.filter(@retired, &(Map.get(env, &1) not in [nil, ""]))

    for name <- present do
      Logger.warning(
        "event=harklight_config outcome=ignored setting=#{name} reason=retired_in_0.2.0"
      )
    end

    present
  end
end
