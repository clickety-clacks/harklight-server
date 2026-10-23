defmodule Mix.Tasks.Attention.Enroll do
  @moduledoc """
  Headless requesting-device enrollment with a locally generated key.

      mix attention.enroll start --directory ./device --connect 127.0.0.1 --name relay.test --port 8443 --ca ca.pem --label 'Laptop'
      mix attention.enroll retry --directory ./device --connect 127.0.0.1 --name relay.test --port 8443 --ca ca.pem --label 'Laptop'
      mix attention.enroll status --directory ./device --connect 127.0.0.1 --name relay.test --port 8443 --relay-port 8445 --ca ca.pem

  The displayed approval code is for the already authenticated owner. The
  status secret and private key remain in the local 0700 directory.
  """

  use Mix.Task
  @shortdoc "Create a local device key and request or check owner approval"

  @impl true
  def run(args) do
    Mix.Task.run("app.start")
    AttentionServer.CLI.with_io(Mix.shell(), Mix.Error, fn ->
      AttentionServer.CLI.Enroll.run(args)
    end)
  end
end
