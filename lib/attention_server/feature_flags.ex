defmodule AttentionServer.FeatureFlags do
  @moduledoc false
  require Logger
  alias AttentionServer.JSON

  def read(nil, warned), do: {%{}, warned}

  def read(path, warned) do
    with {:ok, contents} <- File.read(path),
         {:ok, flags} when is_map(flags) and not is_struct(flags) <-
           JSON.decode_line(contents <> "\n") do
      {Map.filter(flags, fn {_name, value} -> is_boolean(value) end), warned}
    else
      _ ->
        unless warned do
          Logger.warning(
            "event=harklight_feature_flags outcome=defaults reason=unavailable_or_invalid"
          )
        end

        {%{}, true}
    end
  end
end
