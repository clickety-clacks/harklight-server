defmodule AttentionServer.ConversationFiles do
  @moduledoc """
  Exact file bytes and durable upload references carried by one conversation
  entry. An invalid selection commits no part of an entry.
  """

  @inline_decoded_limit 262_144
  alias AttentionServer.Message

  def inline_limit, do: @inline_decoded_limit

  def new_upload_id do
    <<a::binary-size(6), b::8, c::8, d::8, e::binary-size(7)>> = :crypto.strong_rand_bytes(16)
    b = Bitwise.bor(Bitwise.band(b, 15), 64)
    d = Bitwise.bor(Bitwise.band(d, 63), 128)
    hex = Base.encode16(<<a::binary, b, c, d, e::binary>>, case: :lower)

    Enum.join(
      [
        String.slice(hex, 0, 8),
        String.slice(hex, 8, 4),
        String.slice(hex, 12, 4),
        String.slice(hex, 16, 4),
        String.slice(hex, 20, 12)
      ],
      "-"
    )
  end

  def normalize(frame) do
    case Map.fetch(frame, "files") do
      :error -> {:ok, []}
      {:ok, files} when is_list(files) -> normalize_files(files, [], 0, MapSet.new())
      _ -> {:error, "files"}
    end
  end

  def normalize(frame, engine, person, actor) do
    case Map.fetch(frame, "files") do
      :error ->
        {:ok, []}

      {:ok, files} when is_list(files) ->
        normalize_files(files, [], 0, MapSet.new(), {engine, person, actor, frame})

      _ ->
        {:error, "files"}
    end
  end

  def put_entry(entry, []), do: entry
  def put_entry(entry, files), do: Map.put(entry, "files", files)

  def valid_stored?(files) when is_list(files) do
    if Enum.all?(files, &(is_map(&1) and not is_struct(&1))) do
      input =
        Enum.map(files, fn file ->
          if file["tier"] == "upload",
            do: Map.take(file, ~w(name upload_id size sha256)),
            else: Map.take(file, ~w(name data_base64))
        end)

      normalize_stored(input, files)
    else
      false
    end
  end

  def valid_stored?(_), do: false

  def transfers_for(entry, reports) do
    names = Map.get(entry, "files", []) |> Enum.map(& &1["name"])

    cond do
      names == [] and reports in [nil, []] -> {:ok, %{}}
      not is_list(reports) -> {:error, :file_transfers}
      length(reports) != length(names) -> {:error, :file_transfers}
      true -> normalize_transfers(reports, MapSet.new(names), %{})
    end
  end

  def valid_transfer_state?(entry, transfers) when is_map(transfers) do
    names = Map.get(entry, "files", []) |> Enum.map(& &1["name"]) |> MapSet.new()

    MapSet.new(Map.keys(transfers)) == names and
      Enum.all?(transfers, fn {name, outcome} ->
        valid_transfer?(Map.put(outcome, "name", name))
      end)
  end

  def valid_transfer_state?(_, _), do: false

  def valid_use?(%{"status" => "used"} = report),
    do: Map.keys(report) |> Enum.sort() == ["status"]

  def valid_use?(%{"status" => "unusable", "reason" => reason} = report),
    do:
      Map.keys(report) |> Enum.sort() == ["reason", "status"] and is_binary(reason) and
        reason != "" and String.valid?(reason)

  def valid_use?(_), do: false

  defp normalize_transfers([], remaining, result) do
    if MapSet.size(remaining) == 0, do: {:ok, result}, else: {:error, :file_transfers}
  end

  defp normalize_transfers([%{"name" => name} = report | rest], remaining, result) do
    if MapSet.member?(remaining, name) and valid_transfer?(report) do
      normalize_transfers(
        rest,
        MapSet.delete(remaining, name),
        Map.put(result, name, Map.delete(report, "name"))
      )
    else
      {:error, :file_transfers}
    end
  end

  defp normalize_transfers(_, _, _), do: {:error, :file_transfers}

  defp valid_transfer?(%{"status" => "transferred"} = report),
    do: Enum.sort(Map.keys(report)) == ["name", "status"]

  defp valid_transfer?(%{"status" => "not_transferred", "reason" => reason} = report),
    do:
      Enum.sort(Map.keys(report)) == ["name", "reason", "status"] and
        is_binary(reason) and reason != "" and String.valid?(reason)

  defp valid_transfer?(_), do: false

  defp normalize_stored(input, files) do
    # Load validation cannot consult a TLS caller. The full upload edge is
    # checked against durable metadata by the store before accepting a state.
    case normalize_files(input, [], 0, MapSet.new(), :stored) do
      {:ok, ^files} -> true
      _ -> false
    end
  end

  defp normalize_files(files, reversed, total, names),
    do: normalize_files(files, reversed, total, names, :inline_only)

  defp normalize_files([], reversed, _total, _names, _context),
    do: {:ok, Enum.reverse(reversed)}

  defp normalize_files([%{"upload_id" => id} = file | rest], reversed, total, names, context) do
    name = file["name"]
    size = file["size"]
    hash = file["sha256"]

    valid? =
      Enum.sort(Map.keys(file)) == ~w(name sha256 size upload_id) and
        valid_name?(name) and not MapSet.member?(names, name) and
        Message.uuid_v4?(id) and is_integer(size) and size >= 0 and
        is_binary(hash) and Regex.match?(~r/\A[0-9a-f]{64}\z/, hash)

    authorized? =
      case context do
        :stored ->
          true

        {engine, person, actor, frame} ->
          case Map.get(engine.uploads, id) do
            %{
              person: ^person,
              uploader: ^actor,
              op_id: op,
              sender_id: sender,
              message_id: message,
              revision: revision,
              name: ^name,
              size: ^size,
              sha256: ^hash
            } ->
              op == frame["op_id"] and
                {sender, message, revision} == target(frame)

            _ ->
              false
          end

        _ ->
          false
      end

    if valid? and authorized? do
      normalized = %{
        "name" => name,
        "tier" => "upload",
        "upload_id" => id,
        "size" => size,
        "sha256" => hash
      }

      normalize_files(rest, [normalized | reversed], total, MapSet.put(names, name), context)
    else
      {:error, "files"}
    end
  end

  defp normalize_files([file | rest], reversed, total, names, context)
       when is_map(file) and not is_struct(file) do
    name = file["name"]
    data = file["data_base64"]

    with true <- Enum.sort(Map.keys(file)) == ["data_base64", "name"],
         true <-
           valid_name?(name),
         false <- MapSet.member?(names, name),
         true <- is_binary(data),
         {:ok, bytes} <- Base.decode64(data),
         true <- Base.encode64(bytes) == data,
         size when total + size <= @inline_decoded_limit <- byte_size(bytes) do
      normalized = %{
        "name" => name,
        "tier" => "inline",
        "size" => size,
        "sha256" => Base.encode16(:crypto.hash(:sha256, bytes), case: :lower),
        "data_base64" => data
      }

      normalize_files(
        rest,
        [normalized | reversed],
        total + size,
        MapSet.put(names, name),
        context
      )
    else
      _ -> {:error, "files"}
    end
  end

  defp normalize_files(_, _, _, _, _), do: {:error, "files"}

  defp target(%{"type" => "forward", "message" => message}) when is_map(message),
    do: {message["sender_id"], message["message_id"], 1}

  defp target(%{"type" => "conversation_revise", "base_revision" => revision} = frame)
       when is_integer(revision),
       do: {frame["sender_id"], frame["message_id"], revision + 1}

  defp target(frame),
    do: {frame["sender_id"], frame["message_id"], frame["request_revision"]}

  def valid_name?(name),
    do:
      is_binary(name) and name != "" and String.valid?(name) and
        Path.basename(name) == name and name not in [".", ".."] and
        not String.contains?(name, ["\\", <<0>>])
end
