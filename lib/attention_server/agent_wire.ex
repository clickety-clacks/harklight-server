defmodule AttentionServer.AgentWire do
  @moduledoc """
  G6 HTTPS envelope and the narrow mapping to the serialized G1–G3 decisions.
  The authenticated TLS key, never a request field, selects the person and agent.
  """

  alias AttentionServer.JSON

  @selectors ~w(person person_id principal principal_id principal_kind agent_id device_id owner_id origin actor key_fingerprint)
  @search_fields ~w(sender_id message_id state limit cursor)
  @respond_fields ~w(sender_id message_id acceptance_seq deliberate intent_ref op_id answer)

  def normalize(request) when is_map(request) and not is_struct(request) do
    ref = Map.get(request, "ref")

    cond do
      Map.has_key?(request, "ref") and not is_binary(ref) ->
        {:error, "invalid_field", "ref", nil}

      request["v"] != 1 ->
        {:error, "invalid_field", "v", ref}

      selector = Enum.find(@selectors, &Map.has_key?(request, &1)) ->
        {:error, "invalid_field", selector, ref}

      request["type"] == "search" ->
        case invalid_search_type(request) do
          nil ->
            inner =
              Map.take(request, @search_fields) |> Map.merge(%{"v" => 1, "type" => "search"})

            inner =
              if Map.has_key?(request, "text"),
                do: Map.put(inner, "contains", request["text"]),
                else: inner

            {:ok, inner, ref}

          field ->
            {:error, "invalid_field", field, ref}
        end

      request["type"] == "get" ->
        {:ok,
         Map.take(request, ~w(sender_id message_id)) |> Map.merge(%{"v" => 1, "type" => "get"}),
         ref}

      request["type"] == "respond" ->
        {:ok, Map.take(request, @respond_fields) |> Map.merge(%{"v" => 1, "type" => "answer"}),
         ref}

      true ->
        {:error, "invalid_field", "type", ref}
    end
  end

  def normalize(_), do: {:error, "invalid_field", "request", nil}

  def application_reply(%{"error" => reason} = result, _request, ref) do
    %{"v" => 1, "type" => "error", "reason" => reason}
    |> maybe_put("field", result["field"])
    |> maybe_put("terminal", result["terminal"])
    |> maybe_put("ref", ref)
  end

  def application_reply(%{"type" => "answer_result", "seq" => seq} = result, request, ref) do
    %{
      "v" => 1,
      "type" => "ok",
      "terminal_seq" => seq,
      "operation_id" => request["op_id"],
      "relay_outcome" => "answered"
    }
    |> maybe_put("duplicate", result["duplicate"])
    |> maybe_put("ref", ref)
  end

  def application_reply(%{"type" => type} = result, _request, ref)
      when type in ["search_result", "get_result"] do
    result
    |> Map.drop(~w(type v))
    |> Map.merge(%{"v" => 1, "type" => "ok"})
    |> maybe_put("ref", ref)
  end

  def application_reply(_, _request, ref), do: error("invalid_request", nil, ref)

  def error(reason, field \\ nil, ref \\ nil) do
    %{"v" => 1, "type" => "error", "reason" => reason}
    |> maybe_put("field", field)
    |> maybe_put("ref", ref)
  end

  def http_response(status, value) do
    body = JSON.encode!(value)

    reason =
      case status do
        200 -> "OK"
        400 -> "Bad Request"
        404 -> "Not Found"
        405 -> "Method Not Allowed"
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

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp invalid_search_type(request) do
    Enum.find(~w(sender_id message_id state text cursor), fn field ->
      Map.has_key?(request, field) and not is_binary(request[field])
    end) ||
      if(Map.has_key?(request, "limit") and not is_integer(request["limit"]),
        do: "limit",
        else: nil
      )
  end
end
