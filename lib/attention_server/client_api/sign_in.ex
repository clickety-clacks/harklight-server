defmodule AttentionServer.ClientAPI.SignIn do
  @moduledoc false

  import Plug.Conn

  alias AttentionServer.{JSON, Relay}
  alias AttentionServer.ClientAPI.Errors

  @request_limit 4 * 1024

  def methods(conn, relay) do
    conn = put_resp_header(conn, "cache-control", "no-store")

    case Relay.native_sign_in_methods(relay) do
      {:ok, methods} -> Errors.send_json(conn, 200, %{"methods" => methods})
      error -> send_failure(conn, error)
    end
  catch
    :exit, _ -> Errors.send_error(conn, "retry_later")
  end

  def create_transaction(conn, relay) do
    handle_json(conn, fn
      conn, %{"verifier_hash" => hash} ->
        case Relay.create_native_transaction(relay, hash) do
          {:ok, reference} -> Errors.send_json(conn, 200, %{"transaction" => reference})
          {:error, :invalid_verifier_hash} -> Errors.send_error(conn, "invalid_field", field: "verifier_hash")
          error -> send_failure(conn, error)
        end

      conn, _ ->
        Errors.send_error(conn, "invalid_field", field: "verifier_hash")
    end)
  end

  def redeem(conn, relay) do
    handle_json(conn, fn conn, request ->
      case Relay.redeem_native_transaction(relay, request) do
        {:ok, result} -> Errors.send_json(conn, 200, result)
        error -> send_failure(conn, error)
      end
    end)
  end

  defp handle_json(conn, handler) do
    conn = put_resp_header(conn, "cache-control", "no-store")

    case read_body(conn, length: @request_limit, read_length: @request_limit) do
      {:ok, raw, conn} ->
        case JSON.decode_line(raw <> "\n") do
          {:ok, %{} = request} -> handler.(conn, request)
          _ -> Errors.send_error(conn, "invalid_field")
        end

      {:more, _raw, conn} ->
        Errors.send_error(conn, "payload_too_large")

      {:error, _reason} ->
        Errors.send_error(conn, "invalid_field")
    end
  catch
    :exit, _ -> Errors.send_error(conn, "retry_later")
  end

  defp send_failure(conn, {:error, :disabled}), do: Errors.send_error(conn, "not_found")
  defp send_failure(conn, {:error, :invalid_request}), do: Errors.send_error(conn, "invalid_field")
  defp send_failure(conn, {:error, :unauthorized}), do: Errors.send_error(conn, "unauthorized")
  defp send_failure(conn, _), do: Errors.send_error(conn, "storage_unavailable")
end
