defmodule AttentionServer.ClientAPI.SharedCapability do
  @moduledoc false

  import Plug.Conn, only: [get_req_header: 2]

  def parse(conn, mode \\ :headers) do
    query = query_fields(conn, mode)

    with {:ok, features} <- value(conn, query, "harklight-features", "features", &flags/1),
         {:ok, continuation} <- value(conn, query, "harklight-continuation", "continuation") do
      {:ok, %{shared?: "shared_messages" in features, groups?: "groups" in features,
              continuation: continuation}}
    end
  rescue
    ArgumentError -> {:error, "invalid_field", "features"}
  end

  defp flags(value) do
    case value |> String.split(",") |> Enum.map(&String.trim/1) |> Enum.sort() do
      ["groups", "shared_messages"] = flags -> {:ok, flags}
      ["shared_messages"] = flags -> {:ok, flags}
      _ -> {:error, "invalid_field", "features"}
    end
  end

  def scope(%{groups?: true}, filters), do: Map.put(filters, :groups, true)
  def scope(_capability, filters), do: filters

  def visible?(%{group_id: _}, %{groups?: true}), do: true
  def visible?(%{group_id: _}, _capability), do: false
  def visible?(%{}, _capability), do: true
  def visible?(_record, _capability), do: false

  def readable?(state, person, capability) do
    AttentionServer.ClientAPI.Reads.shared_readable?(state, person) and
      (not match?(%{groups?: true}, capability) or Map.get(state, :groups, false))
  end

  defp query_fields(conn, :person_sse) do
    conn.query_string
    |> URI.query_decoder()
    |> Enum.filter(fn {key, _} -> key in ["features", "continuation"] end)
  end

  defp query_fields(_conn, _mode), do: []

  defp value(conn, query, header, field, normalize) do
    headers = get_req_header(conn, header)
    values = for {^field, value} <- query, do: value

    case {headers, values} do
      {[], []} -> {:ok, []}
      {[value], []} -> normalize.(value)
      {[], [value]} -> normalize.(value)
      {[header_value], [query_value]} ->
        with {:ok, normalized} <- normalize.(header_value),
             {:ok, ^normalized} <- normalize.(query_value) do
          {:ok, normalized}
        else
          _ -> {:error, "invalid_field", field}
        end

      _ -> {:error, "invalid_field", field}
    end
  end

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
