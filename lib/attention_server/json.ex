defmodule AttentionServer.JSON do
  @moduledoc """
  Bounded JSON wire codec that retains decimal tokens instead of converting
  them to IEEE-754 floats. Object key order and insignificant whitespace are
  not preserved; JSON values and array order are.
  """

  defmodule DecimalToken do
    @moduledoc false
    @enforce_keys [:raw]
    defstruct [:raw]
  end

  @max_frame 2 * 1024 * 1024
  @ui1_schema "attention-client.ui/1"
  @duplicate_keys_marker :__attention_server_duplicate_keys__
  @decimal_token ~r/\A-?(?:0|[1-9][0-9]*)(?:\.[0-9]+(?:[eE][+-]?[0-9]+)?|[eE][+-]?[0-9]+)\z/

  @spec decode_line(binary(), pos_integer()) :: {:ok, term()} | {:error, :bad_frame}
  def decode_line(raw, max_frame \\ @max_frame)

  def decode_line(raw, max_frame), do: decode_line_with(raw, max_frame, &put_unique/3)

  @doc false
  def decode_line_for_ui1_admission(raw, max_frame \\ @max_frame) do
    case decode_line(raw, max_frame) do
      {:ok, _value} = decoded ->
        decoded

      {:error, :bad_frame} ->
        case decode_line_with(raw, max_frame, &put_duplicate_marker/3) do
          {:ok, frame} when is_map(frame) and not is_struct(frame) ->
            if ui1_duplicate_request?(frame), do: {:ok, frame}, else: {:error, :bad_frame}

          _ ->
            {:error, :bad_frame}
        end
    end
  end

  @doc false
  def ui1_duplicate_request?(frame) when is_map(frame) and not is_struct(frame) do
    type = Map.get(frame, "type")
    message = Map.get(frame, "message")
    answer_ui = if is_map(message), do: Map.get(message, "answer_ui"), else: nil

    with true <- type in ["send", "revise", "forward", "conversation_revise"],
         %{"schema" => @ui1_schema} <- answer_ui,
         true <- has_marked_duplicates?(answer_ui) do
      without_answer_ui =
        frame
        |> Map.put("message", Map.put(message, "answer_ui", nil))

      not has_marked_duplicates?(without_answer_ui)
    else
      _ -> false
    end
  end

  def ui1_duplicate_request?(_), do: false

  @doc false
  def has_marked_duplicates?(value) when is_map(value) and not is_struct(value) do
    case Map.get(value, @duplicate_keys_marker) do
      [_ | _] ->
        true

      _ ->
        Enum.any?(value, fn {key, child} ->
          key != @duplicate_keys_marker and has_marked_duplicates?(child)
        end)
    end
  end

  def has_marked_duplicates?(value) when is_list(value),
    do: Enum.any?(value, &has_marked_duplicates?/1)

  def has_marked_duplicates?(_), do: false

  defp decode_line_with(raw, max_frame, object_push)
       when is_binary(raw) and is_integer(max_frame) and max_frame > 0 and
              byte_size(raw) <= max_frame do
    if String.ends_with?(raw, "\n") do
      json = binary_part(raw, 0, byte_size(raw) - 1)

      try do
        case JSON.decode(json, nil,
               float: fn token -> %DecimalToken{raw: token} end,
               object_start: fn _ -> %{} end,
               object_push: object_push,
               object_finish: fn object, old_acc -> {object, old_acc} end
             ) do
          {value, nil, rest} ->
            if String.trim(rest) == "", do: {:ok, value}, else: {:error, :bad_frame}

          _ ->
            {:error, :bad_frame}
        end
      rescue
        _ -> {:error, :bad_frame}
      catch
        :throw, :duplicate_key -> {:error, :bad_frame}
      end
    else
      {:error, :bad_frame}
    end
  end

  defp decode_line_with(_, _, _), do: {:error, :bad_frame}

  defp put_unique(key, value, object) do
    if Map.has_key?(object, key), do: throw(:duplicate_key)
    Map.put(object, key, value)
  end

  defp put_duplicate_marker(key, value, object) do
    if Map.has_key?(object, key) do
      Map.update(object, @duplicate_keys_marker, [key], &(&1 ++ [key]))
    else
      Map.put(object, key, value)
    end
  end

  @spec encode!(term()) :: binary()
  def encode!(%DecimalToken{raw: raw}) when is_binary(raw) do
    if Regex.match?(@decimal_token, raw),
      do: raw,
      else: raise(ArgumentError, "invalid decimal token")
  end

  def encode!(value)
      when is_nil(value) or is_boolean(value) or is_integer(value) or is_binary(value),
      do: JSON.encode!(value)

  # Server-computed scores must round-trip as JSON numbers without bucketing.
  def encode!(value) when is_float(value), do: :erlang.float_to_binary(value, [:short])

  def encode!(value) when is_list(value),
    do: "[" <> Enum.map_join(value, ",", &encode!/1) <> "]"

  def encode!(value) when is_map(value) do
    if not Enum.all?(Map.keys(value), &is_binary/1),
      do: raise(ArgumentError, "non-string JSON key")

    members =
      value
      |> Enum.sort_by(fn {key, _} -> key end)
      |> Enum.map_join(",", fn {key, item} -> JSON.encode!(key) <> ":" <> encode!(item) end)

    "{" <> members <> "}"
  end

  def encode!(_), do: raise(ArgumentError, "not a JSON value")

  @spec canonical(term()) :: binary()
  def canonical(value), do: encode!(value)
end
