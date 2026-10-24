defmodule AttentionServer.OriginWire do
  @moduledoc """
  JSON command mapping for the certificate-authenticated request origin.
  Caller identity comes only from TLS; a session handle routes, never authorizes.
  """

  alias AttentionServer.{Diagnostics, JSON, Message, Relay}

  @selector_fields ~w(person person_id origin origin_key device_id device_key agent_id agent_key)
  @operation_types ~w(send reply revise withdraw result file_use support attachment_update)

  def command(relay, key, request) when is_map(request) and not is_struct(request) do
    ref = Map.get(request, "ref")

    result =
      cond do
        request["v"] != 1 ->
          error("unsupported_version", nil, ref)

        Map.has_key?(request, "ref") and not valid_ref?(ref) ->
          error("invalid_field", "ref")

        selector = Enum.find(@selector_fields, &Map.has_key?(request, &1)) ->
          error("invalid_field", selector, ref)

        request["type"] in @operation_types and not Message.uuid_v4?(request["op_id"]) ->
          error("invalid_field", "op_id", ref)

        true ->
          run(relay, key, request, ref)
      end

    diagnose(result, request)
  end

  def command(_, _, request), do: diagnose(error("invalid_request"), request)

  defp diagnose(%{"type" => "error", "reason" => reason} = result, request) do
    cause = diagnostic_cause(reason)
    outcome = if cause == :storage_unavailable, do: :outcome_unknown, else: :rejected

    Diagnostics.failure(
      :origin_https,
      :request_dispatch,
      cause,
      outcome,
      Diagnostics.operation_id(request)
    )

    result
  end

  defp diagnose(result, _request), do: result

  defp diagnostic_cause("invalid_request"), do: :invalid_request
  defp diagnostic_cause("invalid_field"), do: :invalid_request
  defp diagnostic_cause("unsupported_version"), do: :invalid_request
  defp diagnostic_cause("forbidden"), do: :forbidden
  defp diagnostic_cause("not_found"), do: :not_found
  defp diagnostic_cause("storage_unavailable"), do: :storage_unavailable
  defp diagnostic_cause("clock_unavailable"), do: :clock_unavailable
  defp diagnostic_cause("expired"), do: :expired
  defp diagnostic_cause("used"), do: :used
  defp diagnostic_cause("request_conflict"), do: :request_conflict
  defp diagnostic_cause("upload_discarded"), do: :upload_discarded
  defp diagnostic_cause("invalid_upload"), do: :invalid_request
  defp diagnostic_cause(_), do: :unknown

  defp run(relay, key, %{"type" => "feed", "since" => since}, ref)
       when is_integer(since) and since >= 0 do
    case Relay.origin_feed(relay, key, since) do
      {:ok, feed} -> ok(feed, ref)
      {:error, reason} -> error(Atom.to_string(reason), nil, ref)
    end
  end

  defp run(relay, key, %{"type" => "ack", "event_id" => id} = request, ref) do
    with {:ok, seq} <- event_seq(id),
         {:ok, reply} <- Relay.origin_ack(relay, key, seq, request["file_transfers"]) do
      ok(reply, ref)
    else
      {:error, :invalid_event_id} -> error("invalid_field", "event_id", ref)
      {:error, reason} -> error(Atom.to_string(reason), nil, ref)
    end
  end

  defp run(relay, key, %{"type" => "file_use", "event_id" => id, "file" => name} = request, ref) do
    outcome = Map.take(request, ~w(status reason))

    with {:ok, seq} <- event_seq(id),
         {:ok, reply} <- Relay.origin_file_use(relay, key, seq, name, outcome, request["op_id"]) do
      ok(reply, ref)
    else
      {:error, :invalid_event_id} -> error("invalid_field", "event_id", ref)
      {:error, :file_use} -> error("invalid_field", "file_use", ref)
      {:error, :invalid_op_id} -> error("invalid_field", "op_id", ref)
      {:error, reason} -> error(Atom.to_string(reason), nil, ref)
    end
  end

  defp run(relay, key, %{"type" => "status", "session_handle" => handle}, ref)
       when is_binary(handle) do
    case Relay.origin_status(relay, key, handle) do
      {:ok, status} -> ok(status, ref)
      {:error, reason} -> error(Atom.to_string(reason), nil, ref)
    end
  end

  defp run(relay, key, %{"type" => "file_limits"}, ref) do
    case Relay.origin_file_limits(relay, key) do
      {:ok, limits} -> ok(limits, ref)
      {:error, reason} -> error(Atom.to_string(reason), nil, ref)
    end
  end

  defp run(relay, key, %{"type" => "discard_uploads"} = request, ref) do
    attributes = Map.take(request, ~w(op_id sender_id message_id revision))

    case Relay.discard_uploads(relay, key, attributes) do
      {:ok, result} -> ok(result, ref)
      {:error, reason} -> error(Atom.to_string(reason), nil, ref)
    end
  end

  defp run(relay, key, %{"type" => "support"} = request, ref) do
    case Relay.origin_support(relay, key, request) do
      {:ok, result} -> ok(result, ref)
      {:error, reason} -> error(Atom.to_string(reason), nil, ref)
    end
  end

  defp run(relay, key, %{"type" => "attachment_update"} = request, ref) do
    case Relay.origin_attachment_update(relay, key, request) do
      {:ok, result} -> ok(result, ref)
      {:error, reason} -> error(Atom.to_string(reason), nil, ref)
    end
  end

  defp run(
         relay,
         key,
         %{"type" => "session_end", "session_handle" => handle, "gone" => true},
         ref
       )
       when is_binary(handle) do
    case Relay.origin_session_gone(relay, key, handle) do
      :ok -> ok(%{"availability" => "gone"}, ref)
      {:error, reason} -> error(Atom.to_string(reason), nil, ref)
    end
  end

  defp run(relay, key, %{"type" => type} = request, ref)
       when type in ["send", "reply", "revise", "withdraw", "result"] do
    with {:ok, frame} <- operation_frame(request),
         {:ok, raw} <- Relay.request_line(relay, key, JSON.encode!(frame) <> "\n"),
         {:ok, reply} <- JSON.decode_line(raw) do
      maybe_ref(reply, ref)
    else
      {:error, field} when is_binary(field) -> error("invalid_field", field, ref)
      {:error, reason} when is_atom(reason) -> error(Atom.to_string(reason), nil, ref)
      _ -> error("storage_unavailable", nil, ref)
    end
  end

  defp run(_, _, _, ref), do: error("invalid_field", "type", ref)

  defp operation_frame(%{"type" => "send"} = request),
    do: {:ok, request |> Map.put("type", "forward") |> Map.drop(~w(ref))}

  defp operation_frame(%{"type" => type} = request)
       when type in ["reply", "revise", "withdraw", "result"] do
    mapped =
      case type do
        "reply" -> "conversation_reply"
        "revise" -> "conversation_revise"
        "withdraw" -> "conversation_withdraw"
        "result" -> "conversation_result"
      end

    frame = request |> Map.put("type", mapped) |> Map.drop(~w(ref))

    if Map.has_key?(frame, "in_reply_to") do
      case event_seq(frame["in_reply_to"]) do
        {:ok, seq} -> {:ok, Map.put(frame, "in_reply_to", seq)}
        _ -> {:error, "in_reply_to"}
      end
    else
      {:ok, frame}
    end
  end

  defp event_seq(id) when is_binary(id) do
    case Integer.parse(id) do
      {seq, ""} when seq > 0 ->
        if Integer.to_string(seq) == id, do: {:ok, seq}, else: {:error, :invalid_event_id}

      _ ->
        {:error, :invalid_event_id}
    end
  end

  defp event_seq(_), do: {:error, :invalid_event_id}

  defp valid_ref?(value),
    do: is_binary(value) and value != "" and byte_size(value) <= 256 and String.valid?(value)

  defp ok(value, ref), do: value |> Map.merge(%{"v" => 1, "type" => "ok"}) |> maybe_ref(ref)

  def error(reason, field \\ nil, ref \\ nil) do
    %{"v" => 1, "type" => "error", "reason" => reason}
    |> then(fn value -> if field, do: Map.put(value, "field", field), else: value end)
    |> maybe_ref(ref)
  end

  def http_response(status, value) do
    body = JSON.encode!(value)

    reason =
      case status do
        200 -> "OK"
        400 -> "Bad Request"
        403 -> "Forbidden"
        404 -> "Not Found"
        405 -> "Method Not Allowed"
        409 -> "Conflict"
        413 -> "Content Too Large"
        503 -> "Service Unavailable"
      end

    [
      "HTTP/1.1 #{status} #{reason}\r\n",
      "Content-Type: application/json; charset=utf-8\r\n",
      "Content-Length: #{byte_size(body)}\r\n",
      "Cache-Control: no-store\r\n",
      "Connection: close\r\n\r\n",
      body
    ]
  end

  defp maybe_ref(value, nil), do: value
  defp maybe_ref(value, ref), do: Map.put(value, "ref", ref)
end
