defmodule Mix.Tasks.Attention.Device do
  @moduledoc """
  Foreground enrolled-device commands for reading and answering stored messages.

      mix attention.device read --directory ./recipient --connect 127.0.0.1 --name relay.test --port 8445 --ca ca.pem
      mix attention.device respond --directory ./recipient --connect 127.0.0.1 --name relay.test --port 8445 --ca ca.pem --sender app:example --message-id request-1 --answer answer.json

  The identity directory comes from `mix attention.enroll`. `read` saves
  replayed events before ACK, and `respond` keeps its exact operation for safe
  retry. Agents use the authenticated origin session route to send new messages.
  An answer file may contain any JSON value, including `null`.
  """

  use Mix.Task
  @shortdoc "Read and answer stored messages as an enrolled device"

  @impl true
  def run(args) do
    Mix.Task.run("app.start")

    AttentionServer.CLI.with_io(Mix.shell(), Mix.Error, fn ->
      AttentionServer.CLI.Device.run(args)
    end)
  end
end
