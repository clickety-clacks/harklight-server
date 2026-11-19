defmodule AttentionServer.ConnectionRole do
  @moduledoc """
  Connection roles for the device WebSocket transport.

  A connection declares `stream` or `request` at admission. A connection that
  declares nothing is a legacy stream and keeps today's unrestricted frame set.
  Only an explicitly declared role is restricted:

    * `stream` may sync (`resync`, `inbox_*`, `conversation_open`), acknowledge
      and report presence. Reads and mutations are refused.
    * `request` may read and mutate. Sync, acknowledgements and presence are
      refused, so a request connection never enters paging mode and never holds
      a cursor.

  Refusals use the `wrong_connection_role` error reason.
  """

  require Logger

  @stream_frame_types ~w(
    resync ack conversation_open conversation_ack
    inbox_start inbox_page inbox_reconcile inbox_receipts inbox_checkpoint_ack inbox_attach
    focus_report
  )

  @close_codes %{
    superseded_by_new_stream: 4409,
    device_revoked: 4403,
    server_shutdown: 1001,
    idle_timeout: 4408,
    protocol_error: 1002
  }

  @type t :: :legacy | :stream | :request

  @doc "Frame types that belong to the stream role."
  def stream_frame_types, do: @stream_frame_types

  @doc "True when the client declared a role at admission."
  def explicit?(role), do: role in [:stream, :request]

  @doc "Returns `nil` when the role may send this frame type, else the refusal reason."
  def refusal(:stream, type) when is_binary(type),
    do: if(type in @stream_frame_types, do: nil, else: "wrong_connection_role")

  def refusal(:request, type) when is_binary(type),
    do: if(type in @stream_frame_types, do: "wrong_connection_role", else: nil)

  def refusal(_role, _type), do: nil

  @doc "Opaque server-issued connection identifier, safe to log on both sides."
  def new_connection_id,
    do: "c_" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)

  @doc "WebSocket close status for a close reason token."
  def close_code(reason), do: Map.fetch!(@close_codes, reason)

  def close_reasons, do: Map.keys(@close_codes)

  @doc "The first frame sent to a connection that declared a role."
  def admitted_frame(connection_id, role) do
    AttentionServer.JSON.encode_line(%{
      "v" => 1,
      "type" => "connection_admitted",
      "connection_id" => connection_id,
      "role" => Atom.to_string(role)
    })
  end

  def log(phase, session, detail \\ []) do
    fields =
      [
        phase: phase,
        connection_id: Map.get(session, :connection_id),
        role: Map.get(session, :role, :legacy),
        transport: Map.get(session, :transport)
      ] ++ detail

    Logger.info(
      "event=harklight_connection " <>
        Enum.map_join(fields, " ", fn {key, value} -> "#{key}=#{value}" end)
    )
  end
end
