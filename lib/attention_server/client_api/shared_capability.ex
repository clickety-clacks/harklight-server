defmodule AttentionServer.ClientAPI.SharedCapability do
  @moduledoc false

  import Plug.Conn, only: [get_req_header: 2]

  def parse(conn, mode \\ :headers) do
    query = query_fields(conn, mode)

    with {:ok, feature} <- value(conn, query, "harklight-features", "features"),
         {:ok, continuation} <- value(conn, query, "harklight-continuation", "continuation"),
         true <- feature in [nil, "shared_messages"] do
      {:ok, %{shared?: feature == "shared_messages", continuation: continuation}}
    else
      false -> {:error, "invalid_field", "features"}
      error -> error
    end
  rescue
    ArgumentError -> {:error, "invalid_field", "features"}
  end

  defp query_fields(conn, :person_sse) do
    conn.query_string
    |> URI.query_decoder()
    |> Enum.filter(fn {key, _} -> key in ["features", "continuation"] end)
  end

  defp query_fields(_conn, _mode), do: []

  defp value(conn, query, header, field) do
    headers = get_req_header(conn, header)
    values = for {^field, value} <- query, do: value

    case {headers, values} do
      {[], []} -> {:ok, nil}
      {[value], []} -> {:ok, value}
      {[], [value]} -> {:ok, value}
      {[value], [value]} -> {:ok, value}
      _ -> {:error, "invalid_field", field}
    end
  end
end
