defmodule AttentionServer.DeviceConnection do
  @moduledoc """
  Identifiers, close reasons and log lines for device connections.

  Every device WebSocket is the device's change stream. It is admitted with a
  server-issued connection ID and closed by the server only with one of the
  close reasons below. Device reads and mutations may also arrive as
  sessionless HTTPS requests on `POST /client/v1`; each response carries a
  server-issued request ID. Both IDs are opaque and safe to log on both sides.
  """

  require Logger

  @close_codes %{
    superseded_by_new_stream: 4409,
    device_revoked: 4403,
    idle_timeout: 4408,
    server_shutdown: 1001,
    protocol_error: 1002
  }

  # Types served by `POST /client/v1`: snapshot reads and mutations. Every other
  # type is stream state and stays on the device WebSocket.
  @client_read_types ~w(inbox_list conversation_page view_get levels_get)
  @client_mutation_types ~w(
    message_open conversation_read discussion answer answer_no_comment lifecycle
    levels_set push_register push_remove push_status
  )

  def client_read_types, do: @client_read_types
  def client_types, do: @client_read_types ++ @client_mutation_types

  def new_connection_id, do: "c_" <> random_id()
  def new_request_id, do: "r_" <> random_id()

  defp random_id, do: Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)

  def close_reasons, do: Map.keys(@close_codes)
  def close_code(reason), do: Map.fetch!(@close_codes, reason)

  def log_connection(phase, session, detail \\ []) do
    log(
      "harklight_connection",
      [
        phase: phase,
        connection_id: Map.get(session, :connection_id),
        transport: Map.get(session, :transport)
      ] ++ detail
    )
  end

  def log_request(request_id, type, status, reason, duration_ms) do
    log("harklight_client_request",
      request_id: request_id,
      type: type || "none",
      status: status,
      reason: reason || "none",
      duration_ms: duration_ms
    )
  end

  defp log(event, fields) do
    Logger.info(
      "event=#{event} " <> Enum.map_join(fields, " ", fn {key, value} -> "#{key}=#{value}" end)
    )
  end
end
