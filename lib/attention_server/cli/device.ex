defmodule AttentionServer.CLI.Device do
  @moduledoc """
  Foreground enrolled-device commands for reading and answering stored messages.

      mix attention.device read --directory ./recipient --connect 127.0.0.1 --name relay.test --port 8445 --ca ca.pem
      mix attention.device respond --directory ./recipient --connect 127.0.0.1 --name relay.test --port 8445 --ca ca.pem --sender app:example --message-id request-1 --answer answer.json

  The identity directory comes from `mix attention.enroll`. `read` saves
  replayed events before ACK, and `respond` keeps its exact operation for safe
  retry. New agent messages use the authenticated origin session route. An
  answer file may contain any JSON value, including `null`.
  """

  alias AttentionServer.{HeadlessDevice, JSON, ToolEndpoint}

  def run([command | args]) when command in ["read", "respond"] do
    {options, rest, invalid} =
      OptionParser.parse(args,
        strict: [
          directory: :string,
          connect: :string,
          name: :string,
          port: :integer,
          ca: :string,
          sender: :string,
          message_id: :string,
          answer: :string
        ]
      )

    if rest != [] or invalid != [], do: AttentionServer.CLI.error!("unexpected device arguments")

    directory =
      Keyword.get(options, :directory) || AttentionServer.CLI.error!("--directory is required")

    endpoint =
      case ToolEndpoint.from_options(options) do
        {:ok, value} -> value
        _ -> AttentionServer.CLI.error!("--connect, --name, --port and --ca are required")
      end

    AttentionServer.CLI.start_application!()

    result =
      case command do
        "read" ->
          HeadlessDevice.read(directory, endpoint)

        "respond" ->
          {sender, id} = target!(options)

          answer =
            Keyword.get(options, :answer) || AttentionServer.CLI.error!("--answer is required")

          HeadlessDevice.respond(directory, endpoint, sender, id, answer)
      end

    case result do
      {:ok, value} ->
        AttentionServer.CLI.shell().info(JSON.encode!(value))

      {:error, {:server, response}} ->
        AttentionServer.CLI.error!(JSON.encode!(response))

      {:error, reason} ->
        AttentionServer.CLI.error!("device operation failed: #{inspect(reason)}")
    end
  end

  def run(_), do: AttentionServer.CLI.error!("use read or respond; see mix help attention.device")

  defp target!(options) do
    sender = Keyword.get(options, :sender) || AttentionServer.CLI.error!("--sender is required")

    id =
      Keyword.get(options, :message_id) || AttentionServer.CLI.error!("--message-id is required")

    {sender, id}
  end
end
