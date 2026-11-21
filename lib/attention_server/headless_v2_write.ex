defmodule AttentionServer.HeadlessV2Write do
  @moduledoc """
  Shared by the headless client API v2 tools: private local files, the
  contract's AnswerBody check, and an answer journaled before it is sent.

  An answer's exact body and `Idempotency-Key` are stored in
  `operations/v2-<sha256>.json` before the first request byte. Rerunning the
  same answer resends that body with that key, so an uncertain or failed
  response is recovered by the server's keyed replay rather than a second
  write.
  """

  import Bitwise

  alias AttentionServer.{HeadlessTLS, JSON}

  # Size cap on the small JSON documents the tools store (server metadata,
  # enrollment results and operation journals).
  @max_local_document 2 * 2 * 1024 * 1024 + 16_384

  @answer_members ~w(request_revision choice comment result)

  @doc "Whether `answer` is an AnswerBody: at most one of choice, comment and result."
  def answer_body?(answer) when is_map(answer) do
    Enum.all?(Map.keys(answer), &(&1 in @answer_members)) and
      Enum.count(~w(choice comment result), &Map.has_key?(answer, &1)) <= 1
  end

  def answer_body?(_answer), do: false

  @doc """
  The journal entry for `request`, created on first use.

  `request` names the answer (its digest is the journal file name). `body` is
  the exact body to send when it is already known locally, or `nil` when
  `resolve` supplies it. `resolve` is called only when no entry exists and
  returns `{:ok, item_id, body}`. `key` optionally fixes the Idempotency-Key.
  An existing entry whose body or key differs is an `:operation_conflict`.
  """
  def saved_answer(directory, request, body, resolve, key \\ nil) do
    digest = hex(:crypto.hash(:sha256, JSON.encode!(request)))
    path = Path.join([directory, "operations", "v2-" <> digest <> ".json"])

    with :ok <- private_directory(Path.dirname(path)) do
      case read_json(path) do
        {:ok,
         %{
           "request" => ^request,
           "item_id" => id,
           "body" => saved_body,
           "idempotency_key" => saved_key
         } =
             saved}
        when is_binary(id) and is_map(saved_body) and is_binary(saved_key) ->
          if (is_nil(body) or body == saved_body) and (is_nil(key) or key == saved_key),
            do: {:ok, saved},
            else: {:error, :operation_conflict}

        {:error, :enoent} ->
          with {:ok, item_id, resolved_body} <- resolve.() do
            saved = %{
              "request" => request,
              "item_id" => item_id,
              "body" => resolved_body,
              "idempotency_key" => key || uuid_v4()
            }

            case write_private(path, saved, :new) do
              :ok -> {:ok, saved}
              {:error, :eexist} -> saved_answer(directory, request, body, resolve, key)
              other -> other
            end
          end

        _ ->
          {:error, :operation_conflict}
      end
    end
  end

  @doc """
  Send a journaled answer once. The result carries `idempotency_key` and
  `replayed` (the server's `Idempotent-Replayed`). A server error envelope is
  `{:error, {:server, envelope}}`; nothing is retried automatically.
  """
  def post_answer(endpoint, identity, saved) do
    path = "/v2/items/" <> URI.encode(saved["item_id"], &URI.char_unreserved?/1) <> "/answer"

    headers = [
      {"accept", "application/json"},
      {"content-type", "application/json"},
      {"idempotency-key", saved["idempotency_key"]}
    ]

    case HeadlessTLS.v2_request(
           endpoint,
           identity,
           "POST",
           path,
           headers,
           JSON.encode!(saved["body"])
         ) do
      {:ok, status, response_headers, %{"rev" => rev} = result}
      when status in [200, 201] and is_integer(rev) ->
        {:ok,
         result
         |> Map.put("idempotency_key", saved["idempotency_key"])
         |> Map.put("replayed", {"idempotent-replayed", "true"} in response_headers)}

      {:ok, _status, _headers, %{"error" => token} = error} when is_binary(token) ->
        {:error, {:server, error}}

      {:ok, _status, _headers, _body} ->
        {:error, :invalid_v2_response}

      error ->
        error
    end
  end

  def uuid_v4 do
    <<a::32, b::16, c::16, d::16, e::48>> = :crypto.strong_rand_bytes(16)

    Enum.join(
      [
        integer_hex(a, 8),
        integer_hex(b, 4),
        integer_hex(0x4000 ||| (c &&& 0x0FFF), 4),
        integer_hex(0x8000 ||| (d &&& 0x3FFF), 4),
        integer_hex(e, 12)
      ],
      "-"
    )
  end

  def hex(bytes), do: Base.encode16(bytes, case: :lower)

  def private_directory(path) do
    case File.mkdir(path) do
      :ok -> File.chmod(path, 0o700)
      {:error, :eexist} -> existing_private_directory(path)
      error -> error
    end
  end

  def existing_private_directory(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :directory, mode: mode}} ->
        if (mode &&& 0o077) == 0, do: :ok, else: {:error, :unsafe_local_directory}

      _ ->
        {:error, :unsafe_local_directory}
    end
  end

  def write_private(path, value, mode \\ :replace) when mode in [:replace, :new] do
    # A random suffix: concurrent tool runs are separate VMs, so a VM-local
    # unique integer can repeat between them.
    temporary = path <> ".new-" <> hex(:crypto.strong_rand_bytes(8))

    with {:ok, io} <- File.open(temporary, [:write, :exclusive, :binary]) do
      result =
        with :ok <- File.chmod(temporary, 0o600),
             :ok <- IO.binwrite(io, JSON.encode!(value) <> "\n"),
             :ok <- :file.sync(io) do
          :ok
        end

      _ = File.close(io)

      case result do
        :ok ->
          installed =
            if mode == :new,
              do: File.ln(temporary, path),
              else: File.rename(temporary, path)

          _ = File.rm(temporary)

          case installed do
            :ok ->
              case System.cmd("sync", [Path.dirname(path)], stderr_to_stdout: true) do
                {_, 0} -> :ok
                _ -> {:error, :local_save_failed}
              end

            {:error, :eexist} ->
              {:error, :eexist}

            _ ->
              {:error, :local_save_failed}
          end

        _ ->
          _ = File.rm(temporary)
          {:error, :local_save_failed}
      end
    end
  end

  @doc "One JSON value from a file the operator supplied."
  def read_document(path) do
    with {:ok, contents} <- File.read(path),
         {:ok, value} <- JSON.decode_line(contents <> "\n", byte_size(contents) + 1) do
      {:ok, value}
    else
      _ -> {:error, :invalid_document}
    end
  end

  @doc "A stored document within the local size cap."
  def read_json(path) do
    with {:ok, contents} <- File.read(path),
         true <- byte_size(contents) <= @max_local_document do
      # Stored documents need no line terminator. The added LF is decoder
      # framing only; keep the limit on the original file bytes.
      JSON.decode_line(contents <> "\n", @max_local_document + 1)
    else
      false -> {:error, :bad_frame}
      error -> error
    end
  end

  defp integer_hex(value, width),
    do: value |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(width, "0")
end
