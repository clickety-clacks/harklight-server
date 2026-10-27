defmodule Mix.Tasks.Attention.Agent do
  @moduledoc """
  Locally generate a service identity and use its separately approved read or
  response grants over mutual-TLS HTTPS `POST /agent/v1`. Agent answers require an exact target, caller operation ID,
  intent reference, JSON answer file and an explicit local confirmation.

      mix attention.agent keygen --directory ./agent
      mix attention.agent search --directory ./agent --connect 127.0.0.1 --name relay.test --port 8446 --ca ca.pem --sender app:one
      mix attention.agent get --directory ./agent --connect 127.0.0.1 --name relay.test --port 8446 --ca ca.pem --sender app:one --message ITEM
      mix attention.agent respond --directory ./agent --connect 127.0.0.1 --name relay.test --port 8446 --ca ca.pem --sender app:one --message ITEM --acceptance SEQ --op UUID --intent-ref REF --answer-file answer.json

  The owner must first register the generated public certificate fingerprint
  and assign explicit read/respond scopes with `mix attention.owner`. A retry
  of an answer reuses the same caller-held operation UUID and JSON value.
  """

  use Mix.Task
  @shortdoc "Generate or use an owner-enrolled Attention agent key"

  @impl true
  def run(args) do
    Mix.Task.run("app.start")

    AttentionServer.CLI.with_io(Mix.shell(), Mix.Error, fn ->
      AttentionServer.CLI.Agent.run(args)
    end)
  end
end
