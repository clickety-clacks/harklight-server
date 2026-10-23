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
  @decimal_token ~r/\A-?(?:0|[1-9][0-9]*)(?:\.[0-9]+(?:[eE][+-]?[0-9]+)?|[eE][+-]?[0-9]+)\z/

  @spec decode_line(binary(), pos_integer()) :: {:ok, term()} | {:error, :bad_frame}
  def decode_line(raw, max_frame \\ @max_frame)

  def decode_line(raw, max_frame)
      when is_binary(raw) and is_integer(max_frame) and max_frame > 0 and
             byte_size(raw) <= max_frame do
    if String.ends_with?(raw, "\n") do
      json = binary_part(raw, 0, byte_size(raw) - 1)

      try do
        case JSON.decode(json, nil,
               float: fn token -> %DecimalToken{raw: token} end,
               object_start: fn _ -> %{} end,
               object_push: &put_unique/3,
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

  def decode_line(_, _), do: {:error, :bad_frame}

  defp put_unique(key, value, object) do
    if Map.has_key?(object, key), do: throw(:duplicate_key)
    Map.put(object, key, value)
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
