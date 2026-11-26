defmodule AttentionServer.ClientAPI.Devices do
  @moduledoc false

  @behaviour AttentionServer.ClientAPI.Routes

  import Plug.Conn

  alias AttentionServer.ClientAPI.Errors

  @impl true
  def routes do
    [
      {"GET", ["devices"], :list},
      {"GET", ["devices", "known"], :known},
      {"DELETE", ["devices", :device_id], :remove},
      {"POST", ["auth", "sign-out"], :sign_out}
    ]
  end

  def list(conn, ctx) do
    case request(ctx, :list) do
      {:ok, devices} -> Errors.send_json(conn, 200, %{"devices" => devices})
      error -> failure(conn, error)
    end
  end

  def known(conn, ctx) do
    conn = fetch_query_params(conn)

    case Map.get(conn.query_params, "device_id") do
      id when is_binary(id) and byte_size(id) in 1..256 ->
        case request(ctx, {:known, id}) do
          {:ok, known?} -> Errors.send_json(conn, 200, %{"known" => known?})
          error -> failure(conn, error)
        end

      _ ->
        Errors.send_error(conn, "invalid_field", field: "device_id")
    end
  end

  def remove(conn, ctx), do: removal(conn, ctx, {:remove, ctx.params.device_id})
  def sign_out(conn, ctx), do: removal(conn, ctx, :sign_out)

  defp removal(conn, ctx, operation) do
    case request(ctx, operation) do
      :ok -> send_resp(conn, 204, "")
      error -> failure(conn, error)
    end
  end

  defp request(ctx, operation),
    do: GenServer.call(ctx.relay, {:client_devices, ctx.principal, operation}, :infinity)

  defp failure(conn, {:error, :disabled}), do: Errors.send_error(conn, "not_found")
  defp failure(conn, {:error, :not_found}), do: Errors.send_error(conn, "not_found")
  defp failure(conn, {:error, :unauthorized}), do: Errors.send_error(conn, "unauthorized")
  defp failure(conn, {:error, :forbidden}), do: Errors.send_error(conn, "forbidden")
  defp failure(conn, _), do: Errors.send_error(conn, "storage_unavailable")
end
