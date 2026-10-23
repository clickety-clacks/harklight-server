defmodule AttentionServer.ToolEndpoint do
  @moduledoc false

  def from_options(options) do
    with address when is_binary(address) <- Keyword.get(options, :connect),
         name when is_binary(name) and name != "" <- Keyword.get(options, :name),
         ca when is_binary(ca) <- Keyword.get(options, :ca),
         port when is_integer(port) and port in 1..65_535 <- Keyword.get(options, :port),
         true <- File.regular?(ca) do
      parsed =
        case :inet.parse_address(String.to_charlist(address)) do
          {:ok, ip} -> ip
          _ -> String.to_charlist(address)
        end

      {:ok, %{address: parsed, name: name, ca_file: ca, port: port}}
    else
      _ -> {:error, :invalid_endpoint}
    end
  end
end
