defmodule AttentionServer.ClientAPI.SignIn do
  @moduledoc false

  import Plug.Conn

  alias AttentionServer.{JSON, Relay}
  alias AttentionServer.ClientAPI.Errors

  @request_limit 4 * 1024

  def create_transaction(conn, relay) do
    conn = put_resp_header(conn, "cache-control", "no-store")

    case read_body(conn, length: @request_limit, read_length: @request_limit) do
      {:ok, raw, conn} ->
        case JSON.decode_line(raw <> "\n") do
          {:ok, %{"verifier_hash" => hash}} -> begin(conn, relay, hash)
          _ -> Errors.send_error(conn, "invalid_field", field: "verifier_hash")
        end

      {:more, _raw, conn} ->
        Errors.send_error(conn, "payload_too_large")

      {:error, _reason} ->
        Errors.send_error(conn, "invalid_field")
    end
  end

  defp begin(conn, relay, hash) do
    case Relay.create_native_transaction(relay, hash) do
      {:ok, reference} -> Errors.send_json(conn, 200, %{"transaction" => reference})
      {:error, :invalid_verifier_hash} -> Errors.send_error(conn, "invalid_field", field: "verifier_hash")
      {:error, :disabled} -> Errors.send_error(conn, "not_found")
      _ -> Errors.send_error(conn, "storage_unavailable")
    end
  catch
    :exit, _ -> Errors.send_error(conn, "retry_later")
  end
end
