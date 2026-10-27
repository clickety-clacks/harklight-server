defmodule AttentionServer.CLI.Operator do
  @moduledoc """
  Local operator provisioning of a person/owner credential.

      mix attention.operator bootstrap --store relay.sqlite3 --owner-cert owner.pem
      mix attention.operator recover --store relay.sqlite3 --person UUID --owner-cert new-owner.pem

  This command uses local database custody. It never uploads a private key.
  """

  alias AttentionServer.OperatorTool

  def run([command | args]) when command in ["bootstrap", "recover"] do
    {options, rest, invalid} =
      OptionParser.parse(args, strict: [store: :string, owner_cert: :string, person: :string])

    if rest != [] or invalid != [] do
      AttentionServer.CLI.error!("unexpected operator arguments")
    end

    store = Keyword.get(options, :store) || AttentionServer.CLI.error!("--store is required")

    certificate =
      Keyword.get(options, :owner_cert) || AttentionServer.CLI.error!("--owner-cert is required")

    AttentionServer.CLI.start_application!()

    result =
      case command do
        "bootstrap" ->
          if Keyword.has_key?(options, :person),
            do: AttentionServer.CLI.error!("bootstrap cannot select a person ID")

          OperatorTool.bootstrap(store, certificate)

        "recover" ->
          person =
            Keyword.get(options, :person) || AttentionServer.CLI.error!("--person is required")

          OperatorTool.recover(store, person, certificate)
      end

    case result do
      {:ok, person} -> AttentionServer.CLI.shell().info(person)
      :ok -> AttentionServer.CLI.shell().info("recovered")
      {:error, reason} -> AttentionServer.CLI.error!("operator action failed: #{inspect(reason)}")
    end
  end

  def run(_),
    do: AttentionServer.CLI.error!("use bootstrap or recover; see mix help attention.operator")
end
