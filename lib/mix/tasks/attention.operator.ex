defmodule Mix.Tasks.Attention.Operator do
  @moduledoc """
  Local operator provisioning of a person/owner credential.

      mix attention.operator bootstrap --store relay.sqlite3 --owner-cert owner.pem
      mix attention.operator recover --store relay.sqlite3 --person UUID --owner-cert new-owner.pem

  This command uses local database custody. It never uploads a private key.
  """

  use Mix.Task
  @shortdoc "Bootstrap or recover an Attention owner from a local public certificate"

  @impl true
  def run(args) do
    Mix.Task.run("app.start")
    AttentionServer.CLI.with_io(Mix.shell(), Mix.Error, fn ->
      AttentionServer.CLI.Operator.run(args)
    end)
  end
end
