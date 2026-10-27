defmodule AttentionServer.CLI.Device do
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

  alias AttentionServer.{HeadlessDevice, JSON, ToolEndpoint}

  def run([command | args]) when command in ["send", "read", "respond", "receipt"] do
    {options, rest, invalid} =
      OptionParser.parse(args,
        strict: [
          directory: :string,
          connect: :string,
          name: :string,
          port: :integer,
          ca: :string,
          message: :string,
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
        "send" ->
          message =
            Keyword.get(options, :message) || AttentionServer.CLI.error!("--message is required")

          HeadlessDevice.send_message(directory, endpoint, message)

        "read" ->
          HeadlessDevice.read(directory, endpoint)

        "respond" ->
          {sender, id} = target!(options)

          answer =
            Keyword.get(options, :answer) || AttentionServer.CLI.error!("--answer is required")

          HeadlessDevice.respond(directory, endpoint, sender, id, answer)

        "receipt" ->
          {sender, id} = target!(options)
          HeadlessDevice.receipt(directory, endpoint, sender, id)
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

  def run(_),
    do:
      AttentionServer.CLI.error!(
        "use send, read, respond or receipt; see mix help attention.device"
      )

  defp target!(options) do
    sender = Keyword.get(options, :sender) || AttentionServer.CLI.error!("--sender is required")

    id =
      Keyword.get(options, :message_id) || AttentionServer.CLI.error!("--message-id is required")

    {sender, id}
  end
end
