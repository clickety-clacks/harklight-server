defmodule Mix.Tasks.Attention.Device do
  @moduledoc """
  Foreground enrolled-device commands for reading and answering stored messages.

      mix attention.device read --directory ./recipient --connect 127.0.0.1 --name relay.test --port 8445 --ca ca.pem
      mix attention.device respond --directory ./recipient --connect 127.0.0.1 --name relay.test --port 8445 --ca ca.pem --sender app:example --message-id request-1 --answer answer.json

  The identity directory comes from `mix attention.enroll`; `--port` is the
  device port, which serves client API v2. `read` prints `{rev, items}`. The
  `respond` answer file is a JSON object with at most one of `choice` (an
  action `id`), `comment` or `result`, and optionally `request_revision`; `{}`
  answers with no comment. Its exact body and `Idempotency-Key` are saved
  before sending, so a retry of the same answer resends them unchanged.
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
