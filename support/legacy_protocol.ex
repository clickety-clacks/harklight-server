defmodule AttentionServer.LegacyProtocol do
  @moduledoc """
  Test opt-out for the retained protocol 0.1 device path.

  Server serves client API v2 by default. A test that still exercises the
  `/relay` device WebSocket passes `feature_flags_path: flags!(dir)` to start
  the Server with `client_api_v2` explicitly `false`. Remove this module and
  its callers with the old protocol path.
  """

  @doc "Writes a feature-flags file with `client_api_v2` false in `dir` and returns its path."
  def flags!(dir) do
    path = Path.join(dir, "legacy-protocol-flags.json")
    File.mkdir_p!(dir)
    File.write!(path, ~s({"client_api_v2":false}))
    path
  end
end
