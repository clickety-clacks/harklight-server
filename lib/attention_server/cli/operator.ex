defmodule AttentionServer.CLI.Operator do
  @moduledoc """
  Local operator provisioning of a person/owner credential.

      mix attention.operator bootstrap --store relay.sqlite3 --owner-cert owner.pem
      mix attention.operator recover --store relay.sqlite3 --person UUID --owner-cert new-owner.pem

  This command uses local database custody. It never uploads a private key.
  """

  alias AttentionServer.{BounceDecision, BouncePolicy, JSON, OperatorTool, Store}

  def run(["bounce-preview" | args]) do
    case OptionParser.parse(args, strict: [store: :string]) do
      {[store: path], [], []} -> bounce_preview(path)
      _ -> AttentionServer.CLI.error!("use harklight operator bounce-preview --store PATH")
    end
  end

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
      {:ok, person} ->
        AttentionServer.CLI.shell().info(person)

      :ok ->
        AttentionServer.CLI.shell().info("recovered")

      {:error, :store_open_failed} ->
        AttentionServer.CLI.error!(
          "Cannot open the local Harklight store. Check the --store path and permissions, then retry.",
          AttentionServer.CLI.CommandFailure
        )

      {:error, :store_initialization_failed} ->
        AttentionServer.CLI.error!(
          "The local Harklight store opened, but startup could not initialize it. Check the startup diagnostics before retrying.",
          AttentionServer.CLI.CommandFailure
        )

      {:error, reason} ->
        AttentionServer.CLI.error!("operator action failed: #{inspect(reason)}")
    end
  end

  def run(_),
    do: AttentionServer.CLI.error!("use bootstrap, recover or bounce-preview; see SELF_HOST.md")

  defp bounce_preview(path) do
    with {:ok, cutoff} <- BouncePolicy.cutoff(),
         {:ok, engine} <- Store.read_stopped_engine(path) do
      {_ignored, _keys, counts} = BounceDecision.startup(engine, System.system_time(:millisecond), cutoff)
      AttentionServer.CLI.shell().info(JSON.encode!(Map.take(counts, [:full, :soft])))
    else
      {:error, :invalid_bounce_cutoff} ->
        AttentionServer.CLI.error!("ATTENTION_SERVER_BOUNCE_NOT_BEFORE must be an RFC 3339 UTC timestamp")

      {:error, :store_not_checkpointed} ->
        AttentionServer.CLI.error!("Stop Harklight Server cleanly before previewing this store")

      {:error, reason} ->
        AttentionServer.CLI.error!("cannot preview the stopped FORMAT30 store: #{inspect(reason)}")
    end
  end

end
