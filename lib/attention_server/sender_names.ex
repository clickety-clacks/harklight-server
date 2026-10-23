defmodule AttentionServer.SenderNames do
  @moduledoc false

  alias AttentionServer.JSON

  @schema "harklight.sender-names/1"
  @document_keys ~w(schema senders)
  @entry_keys ~w(display_name origin person_id sender_id)

  def load!(nil), do: %{}

  def load!(path) when is_binary(path) do
    with {:ok, contents} <- File.read(path),
         {:ok, document} <- JSON.decode_line(contents <> "\n"),
         true <- is_map(document) and Enum.sort(Map.keys(document)) == @document_keys,
         true <- document["schema"] == @schema and is_list(document["senders"]),
         {:ok, names} <- entries(document["senders"]) do
      names
    else
      _ -> raise ArgumentError, "invalid ATTENTION_SERVER_SENDER_NAMES_FILE"
    end
  end

  def snapshot(names, person_id, sender_id, origin)
      when is_map(names) and is_binary(person_id) and is_binary(sender_id) and
             is_binary(origin) do
    case Map.get(names, {person_id, sender_id, origin}) do
      name when is_binary(name) ->
        if nonblank?(name) do
          %{"v" => 1, "sender_id" => sender_id, "origin" => origin, "name" => name}
        end

      _ ->
        nil
    end
  end

  def snapshot(_, _, _, _), do: nil

  def committed(record, sender_id) when is_map(record) and is_binary(sender_id) do
    case Map.get(record, "sender_display") do
      %{"v" => 1, "sender_id" => ^sender_id, "origin" => origin, "name" => name} = value
      when is_binary(name) ->
        if origin == Map.get(record, :origin) and nonblank?(name) and
             Enum.sort(Map.keys(value)) == ~w(name origin sender_id v),
           do: value,
           else: nil

      _ ->
        nil
    end
  end

  def committed(_, _), do: nil

  def nonblank?(value) when is_binary(value), do: Regex.match?(~r/\S/u, value)
  def nonblank?(_), do: false

  defp entries(values) do
    Enum.reduce_while(values, {:ok, %{}}, fn value, {:ok, names} ->
      case entry(value) do
        {:ok, key, name} when not is_map_key(names, key) ->
          {:cont, {:ok, Map.put(names, key, name)}}

        _ ->
          {:halt, :error}
      end
    end)
  end

  defp entry(value) when is_map(value) do
    valid_shape? = Enum.sort(Map.keys(value)) == @entry_keys

    person_id = value["person_id"]
    sender_id = value["sender_id"]
    origin = value["origin"]
    name = value["display_name"]

    if valid_shape? and Enum.all?([person_id, sender_id, origin, name], &nonempty_string?/1) and
         nonblank?(name) do
      {:ok, {person_id, sender_id, origin}, name}
    else
      :error
    end
  end

  defp entry(_), do: :error

  defp nonempty_string?(value), do: is_binary(value) and value != ""
end
