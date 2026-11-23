defmodule AttentionServer.CLI.Agent do
  @moduledoc """
  Locally generate a service identity and use its separately approved read or
  response grants over client API v2 on the device port, with mutual TLS.

      mix attention.agent keygen --directory ./agent
      mix attention.agent search --directory ./agent --connect 127.0.0.1 --name relay.test --port 8443 --ca ca.pem --sender app:one --q text
      mix attention.agent get --directory ./agent --connect 127.0.0.1 --name relay.test --port 8443 --ca ca.pem --item-id ITEM --entries
      mix attention.agent answer --directory ./agent --connect 127.0.0.1 --name relay.test --port 8443 --ca ca.pem --sender app:one --message MESSAGE --request-revision REVISION --answer-file answer.json

  `--port` is the device port. `search` is `GET /v2/search` (`--q`, `--sender`,
  `--message`, `--state`, `--limit`, `--page` from a previous `next_page`).
  `get` is `GET /v2/items/{item_id}` for `--item-id`, or for the item an exact
  `--sender` and `--message` name; `--entries` adds its entries page
  (`--before`, `--limit`). Each prints the server's JSON body.

  `answer` (or `respond`) is `POST /v2/items/{item_id}/answer`. The answer file
  is an AnswerBody object: at most one of `choice` (an action `id`), `comment`
  or `result`, or `{}` to answer with no comment. `--request-revision` is the
  item's `request_revision`. The answer needs typing ANSWER; after that and
  before anything is sent, the exact body and an `Idempotency-Key` (`--key
  UUID`, or a generated one) are saved under `operations/` in the agent
  directory. A cancelled answer saves nothing.
  Rerunning the same command resends the same body with the same key; the
  server replays a committed answer (`"replayed": true`) instead of writing a
  second one. Nothing is retried automatically, including after a
  non-retryable `internal_error`.

  The owner must first register the generated public certificate fingerprint
  and assign explicit read/respond scopes with `mix attention.owner`. Reading
  needs no read grant; answering needs both grants for the item's sender.
  """

  alias AttentionServer.{HeadlessAgent, HeadlessTLS, HeadlessV2Write, JSON, Message, ToolEndpoint}

  @removed ~w(--acceptance --op --intent-ref --text --contains --cursor)

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
    case Enum.find(args, &(&1 in @removed)) do
      nil ->
        :ok

      flag ->
        AttentionServer.CLI.error!("#{flag} was removed in Harklight 0.2; see SELF_HOST.md")
    end

    {opts, rest, invalid} =
      OptionParser.parse(args,
        strict: [
          directory: :string,
          connect: :string,
          name: :string,
          port: :integer,
          ca: :string,
          q: :string,
          sender: :string,
          message: :string,
          state: :string,
          limit: :integer,
          page: :string,
          item_id: :string,
          entries: :boolean,
          before: :string,
          request_revision: :integer,
          key: :string,
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

    identity = {Path.join(directory, "identity.pem"), Path.join(directory, "identity.key")}
    AttentionServer.CLI.start_application!()

    case command(command, opts, directory, endpoint, identity) do
      {:ok, result} ->
        AttentionServer.CLI.shell().info(JSON.encode!(result))

      {:error, {:server, %{"error" => token} = envelope}} ->
        AttentionServer.CLI.error!(
          "agent request refused: #{token}" <>
            if(is_binary(envelope["request_id"]),
              do: " request_id=#{envelope["request_id"]}",
              else: ""
            ) <>
            if(envelope["retryable"] == false,
              do: "; not retried (rerun the same command to recover an answer)",
              else: ""
            )
        )

      {:error, {:operation_conflict, path}} ->
        AttentionServer.CLI.error!(
          "operation_conflict: an answer for this item and revision is already saved in " <>
            "#{path} with a different body or key. Rerun with that saved answer to send or " <>
            "replay it. Remove the file only if that answer was never sent; the server " <>
            "keeps any answer it already committed."
        )

      {:error, reason} ->
        AttentionServer.CLI.error!("agent request failed: #{inspect(reason)}")
    end
  end

  def run(_),
    do: AttentionServer.CLI.error!("use keygen, search, get, answer or respond; see SELF_HOST.md")

  defp command("search", opts, _directory, endpoint, identity) do
    query =
      [q: :q, sender: :sender, message_id: :message, state: :state, limit: :limit, page: :page]
      |> Enum.flat_map(fn {param, option} ->
        case Keyword.fetch(opts, option) do
          {:ok, value} -> [{param, value}]
          :error -> []
        end
      end)

    get(endpoint, identity, "/v2/search" <> query_string(query))
  end

  defp command("get", opts, _directory, endpoint, identity) do
    with {:ok, item_id} <- target(opts, endpoint, identity),
         {:ok, item} <- get(endpoint, identity, "/v2/items/" <> encode(item_id)) do
      if Keyword.get(opts, :entries, false) do
        query =
          Enum.flat_map([before: :before, limit: :limit], fn {param, option} ->
            case Keyword.fetch(opts, option),
              do: (
                {:ok, v} -> [{param, v}]
                :error -> []
              )
          end)

        with {:ok, entries} <-
               get(
                 endpoint,
                 identity,
                 "/v2/items/#{encode(item_id)}/entries" <> query_string(query)
               ) do
          {:ok, %{"item" => item, "entries" => entries}}
        end
      else
        {:ok, item}
      end
    end
  end

  defp command("respond", opts, directory, endpoint, identity),
    do: command("answer", opts, directory, endpoint, identity)

  defp command("answer", opts, directory, endpoint, identity) do
    revision = required!(opts, :request_revision)
    answer_file = required!(opts, :answer_file)
    key = Keyword.get(opts, :key)

    unless is_integer(revision) and revision > 0,
      do: AttentionServer.CLI.error!("--request-revision must be a positive integer")

    unless is_nil(key) or Message.uuid_v4?(key),
      do: AttentionServer.CLI.error!("--key must be a UUIDv4")

    answer =
      case HeadlessV2Write.read_document(answer_file) do
        {:ok, value} -> value
        _ -> AttentionServer.CLI.error!("answer file must contain one valid JSON value")
      end

    unless HeadlessV2Write.answer_body?(answer) and
             Map.get(answer, "request_revision", revision) == revision,
           do:
             AttentionServer.CLI.error!(
               "answer file must be an AnswerBody object with at most one of choice, comment or result"
             )

    body = Map.put(answer, "request_revision", revision)
    request = %{"target" => target_request(opts), "request_revision" => revision}

    # Resolve and show the exact answer, confirm it, then journal it before
    # the first request byte. A cancelled answer leaves nothing behind.
    with :ok <- HeadlessV2Write.existing_private_directory(directory),
         {:ok, saved, state} <-
           HeadlessV2Write.prepare_answer(
             directory,
             request,
             body,
             fn ->
               with {:ok, item_id} <- target(opts, endpoint, identity), do: {:ok, item_id, body}
             end,
             key
           ) do
      AttentionServer.CLI.shell().info(
        "Answer item #{visible(saved["item_id"])}, revision #{revision}, key #{saved["idempotency_key"]}, body #{JSON.encode!(saved["body"])}"
      )

      # Anything but a typed ANSWER, including closed input, cancels.
      typed =
        AttentionServer.CLI.shell().prompt(
          "Type ANSWER to submit this exact deliberate response: "
        )

      unless is_binary(typed) and String.trim(typed) == "ANSWER",
        do: AttentionServer.CLI.error!("answer cancelled")

      with {:ok, saved} <-
             if(state == :new,
               do: HeadlessV2Write.save_answer(directory, saved),
               else: {:ok, saved}
             ) do
        HeadlessV2Write.post_answer(endpoint, identity, saved)
      end
    end
  end

  defp target_request(opts) do
    case Keyword.fetch(opts, :item_id) do
      {:ok, item_id} ->
        %{"item_id" => item_id}

      :error ->
        sender = required!(opts, :sender)
        message = required!(opts, :message)

        unless Message.sender_id?(sender),
          do: AttentionServer.CLI.error!("invalid --sender")

        %{"sender_id" => sender, "message_id" => message}
    end
  end

  # The item an exact sender and message ID name, through the exact search pair.
  defp target(opts, endpoint, identity) do
    case target_request(opts) do
      %{"item_id" => item_id} ->
        {:ok, item_id}

      %{"sender_id" => sender, "message_id" => message} ->
        path = "/v2/search" <> query_string(sender: sender, message_id: message, limit: 1)

        case get(endpoint, identity, path) do
          {:ok, %{"items" => [%{"item_id" => item_id} | _]}} when is_binary(item_id) ->
            {:ok, item_id}

          {:ok, _} ->
            {:error, {:server, %{"error" => "not_found"}}}

          error ->
            error
        end
    end
  end

  defp get(endpoint, identity, path) do
    case HeadlessTLS.v2_get(endpoint, identity, path) do
      {:ok, 200, body} when is_map(body) ->
        {:ok, body}

      {:ok, _status, %{"error" => token} = error} when is_binary(token) ->
        {:error, {:server, error}}

      {:ok, status, _body} ->
        {:error, {:http, status}}

      error ->
        error
    end
  end

  defp query_string([]), do: ""
  defp query_string(query), do: "?" <> URI.encode_query(query)

  defp encode(value), do: URI.encode(value, &URI.char_unreserved?/1)

  defp required!(opts, option),
    do:
      Keyword.get(opts, option) ||
        AttentionServer.CLI.error!(
          "--#{option |> Atom.to_string() |> String.replace("_", "-")} is required"
        )

  defp visible(value) when is_binary(value) do
    for <<byte <- value>>, into: "" do
      if byte in 0x20..0x7E and byte != ?\\,
        do: <<byte>>,
        else: "\\x" <> Base.encode16(<<byte>>)
    end
  end
end
