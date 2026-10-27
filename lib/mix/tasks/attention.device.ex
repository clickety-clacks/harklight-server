defmodule Mix.Tasks.Attention.Device do
  @moduledoc """
  Foreground enrolled-device commands over the existing TLS WebSocket relay.

      mix attention.device send --directory ./sender --connect 127.0.0.1 --name relay.test --port 8445 --ca ca.pem --message message.json
      mix attention.device read --directory ./recipient --connect 127.0.0.1 --name relay.test --port 8445 --ca ca.pem
      mix attention.device respond --directory ./recipient --connect 127.0.0.1 --name relay.test --port 8445 --ca ca.pem --sender app:example --message-id request-1 --answer answer.json
      mix attention.device receipt --directory ./sender --connect 127.0.0.1 --name relay.test --port 8445 --ca ca.pem --sender app:example --message-id request-1

  The identity directory comes from `mix attention.enroll`. `send` and
  `respond` keep exact operations for safe retry; `read` saves replayed events
  before ACK. An answer file may contain any JSON value, including `null`.
  """

  use Mix.Task
  @shortdoc "Send, read, respond and retrieve an origin receipt as an enrolled device"

  @impl true
  def run(args) do
    Mix.Task.run("app.start")

    AttentionServer.CLI.with_io(Mix.shell(), Mix.Error, fn ->
      AttentionServer.CLI.Device.run(args)
    end)
  end
end
