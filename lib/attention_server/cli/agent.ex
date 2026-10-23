defmodule AttentionServer.CLI.Agent do
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


  alias AttentionServer.{HeadlessAgent, JSON, Message, ToolEndpoint}


  def run(["keygen" | args]) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: [directory: :string])
    if rest != [] or invalid != [], do: AttentionServer.CLI.error!("unexpected keygen arguments")
    directory = required!(opts, :directory)
    AttentionServer.CLI.start_application!()

    case HeadlessAgent.create_identity(directory) do
      {:ok, fingerprint} ->
        AttentionServer.CLI.shell().info(
          "agent key created at #{directory}; public-key fingerprint #{fingerprint}; ask the authenticated person owner to register this exact key"
        )

      error ->
        AttentionServer.CLI.error!("key generation failed: #{inspect(error)}")
    end
  end

  def run([command | args]) when command in ["search", "get", "respond", "answer"] do
    {opts, rest, invalid} =
      OptionParser.parse(args,
        strict: [
          directory: :string,
          connect: :string,
          name: :string,
          port: :integer,
          ca: :string,
          sender: :string,
          message: :string,
          state: :string,
          contains: :string,
          text: :string,
          limit: :integer,
          cursor: :string,
          acceptance: :integer,
          op: :string,
          intent_ref: :string,
          answer_file: :string
        ]
      )

    if rest != [] or invalid != [], do: AttentionServer.CLI.error!("unexpected agent arguments")
    directory = required!(opts, :directory)

    endpoint =
      case ToolEndpoint.from_options(opts) do
        {:ok, value} -> value
        _ -> AttentionServer.CLI.error!("--connect, --name, --port and --ca are required")
      end

    request = request!(command, opts)
    AttentionServer.CLI.start_application!()

    case HeadlessAgent.request(directory, endpoint, request) do
      {:error, {:application, reason, _response}} ->
        AttentionServer.CLI.error!("agent request denied: #{reason}")

      {:ok, %{"type" => "ok"} = response} ->
        AttentionServer.CLI.shell().info(JSON.encode!(response))

      error ->
        AttentionServer.CLI.error!("agent request failed: #{inspect(error)}")
    end
  end

  def run(_), do: AttentionServer.CLI.error!("use keygen, search, get or respond; see mix help attention.agent")

  defp request!("search", opts) do
    if Keyword.has_key?(opts, :text) and Keyword.has_key?(opts, :contains),
      do: AttentionServer.CLI.error!("use only one of --text and --contains")

    %{"type" => "search"}
    |> optional(opts, :sender, "sender_id")
    |> optional(opts, :message, "message_id")
    |> optional(opts, :state, "state")
    |> optional(opts, :text, "text")
    |> optional(opts, :contains, "text")
    |> optional(opts, :limit, "limit")
    |> optional(opts, :cursor, "cursor")
  end

  defp request!("get", opts),
    do: %{
      "type" => "get",
      "sender_id" => required!(opts, :sender),
      "message_id" => required!(opts, :message)
    }

  defp request!("answer", opts) do
    sender = required!(opts, :sender)
    message = required!(opts, :message)
    acceptance = required!(opts, :acceptance)
    op = required!(opts, :op)
    intent = required!(opts, :intent_ref)
    answer_file = required!(opts, :answer_file)

    unless Message.sender_id?(sender) and Message.uuid_v4?(op) and is_integer(acceptance) and
             acceptance > 0,
           do: AttentionServer.CLI.error!("invalid exact answer target or operation ID")

    answer =
      with {:ok, raw} <- File.read(answer_file),
           {:ok, value} <- JSON.decode_line(String.trim_trailing(raw, "\n") <> "\n") do
        value
      else
        _ -> AttentionServer.CLI.error!("answer file must contain one valid JSON value")
      end

    AttentionServer.CLI.shell().info(
      "Answer target #{visible(sender)} / #{visible(message)}, acceptance #{acceptance}, operation #{op}, intent #{visible(intent)}"
    )

    if String.trim(
         AttentionServer.CLI.shell().prompt("Type ANSWER to submit this exact deliberate response: ") || ""
       ) != "ANSWER",
       do: AttentionServer.CLI.error!("answer cancelled")

    %{
      "type" => "respond",
      "sender_id" => sender,
      "message_id" => message,
      "acceptance_seq" => acceptance,
      "op_id" => op,
      "intent_ref" => intent,
      "deliberate" => true,
      "answer" => answer
    }
  end

  defp request!("respond", opts), do: request!("answer", opts)

  defp optional(request, opts, option, field) do
    case Keyword.fetch(opts, option) do
      {:ok, value} -> Map.put(request, field, value)
      :error -> request
    end
  end

  defp required!(opts, option),
    do: Keyword.get(opts, option) || AttentionServer.CLI.error!("--#{option} is required")

  defp visible(value) when is_binary(value) do
    for <<byte <- value>>, into: "" do
      if byte in 0x20..0x7E and byte != ?\\,
        do: <<byte>>,
        else: "\\x" <> Base.encode16(<<byte>>)
    end
  end
end
