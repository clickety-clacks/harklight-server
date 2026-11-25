defmodule AttentionServer.PushRelay do
  @moduledoc false

  @behaviour Plug

  import Plug.Conn
  require Logger

  alias AttentionServer.{APNS, Diagnostics, JSON, PushDispatcher}

  @max_body 4 * 1024
  @server_id ~r/\A[0-9a-f]{32}\z/
  @token ~r/\A(?:[0-9a-fA-F]{2})+\z/
  @fields ~w(token environment kind badge interruption_level sound)

  @impl Plug
  def init(dispatcher), do: %{dispatcher: dispatcher, secret: :crypto.strong_rand_bytes(32)}

  @impl Plug
  def call(%{method: "POST", path_info: ["push-relay", "v1", route]} = conn, state)
      when route in ["servers", "pushes"] and not is_nil(state) do
    case PushDispatcher.apple_config(state.dispatcher) do
      nil -> error(conn, 404, "not_found")
      apns -> dispatch(conn, route, state, apns)
    end
  end

  def call(conn, _state), do: error(conn, 404, "not_found")

  defp dispatch(conn, "servers", %{secret: secret}, _apns) do
    with_request(conn, fn conn, request ->
      case request do
        %{"server" => server} when is_binary(server) and map_size(request) == 1 ->
          if Regex.match?(@server_id, server) do
            Logger.info("event=harklight_push_relay_registration server=#{server}")
            json(conn, 201, %{"key" => key(secret, server)})
          else
            error(conn, 400, "invalid_request")
          end

        _ ->
          error(conn, 400, "invalid_request")
      end
    end)
  end

  defp dispatch(conn, "pushes", state, apns) do
    case verified_server(conn, state.secret) do
      {:ok, server} ->
        with_request(conn, fn conn, request ->
          case claim(request, server) do
            {:ok, claim} -> deliver(conn, claim, apns, state.dispatcher)
            {:error, reason} -> error(conn, 400, reason)
          end
        end)

      :error ->
        error(conn, 401, "unknown_key")
    end
  end

  defp verified_server(conn, secret) do
    with ["Bearer " <> value] <- get_req_header(conn, "authorization"),
         ["v1", server, _signature] <- :binary.split(value, ".", [:global]),
         true <- Regex.match?(@server_id, server),
         true <- Plug.Crypto.secure_compare(key(secret, server), value) do
      {:ok, server}
    else
      _ -> :error
    end
  end

  defp key(secret, server) do
    signature = :crypto.mac(:hmac, :sha256, secret, server)
    "v1." <> server <> "." <> Base.url_encode64(signature, padding: false)
  end

  defp claim(request, server) do
    case request do
      %{"token" => token, "environment" => environment, "kind" => kind}
      when is_binary(token) ->
        with true <- Regex.match?(@token, token),
             {:ok, profile} <- profile(environment),
             {:ok, presentation} <- presentation(kind),
             {:ok, badge} <- badge(request),
             {:ok, alert_options} <- alert_options(request, presentation),
             [] <- Map.keys(request) -- @fields do
          {:ok,
           %{
             relayed: true,
             server: server,
             address: token,
             profile_id: profile,
             presentation: presentation,
             badge: badge,
             alert_options: alert_options,
             environment: environment
           }}
        else
          false -> {:error, "invalid_request"}
          _ -> {:error, "unsupported"}
        end

      _ ->
        {:error, "invalid_request"}
    end
  end

  defp profile("production"), do: {:ok, "harklight-production"}
  defp profile("sandbox"), do: {:ok, "harklight-sandbox"}
  defp profile(_environment), do: :error

  defp presentation("alert"), do: {:ok, :alert}
  defp presentation("background"), do: {:ok, :background}
  defp presentation(_kind), do: :error

  defp badge(request) do
    case Map.fetch(request, "badge") do
      :error -> {:ok, nil}
      {:ok, badge} when is_integer(badge) and badge >= 0 -> {:ok, badge}
      _ -> :error
    end
  end

  defp alert_options(request, presentation) do
    case {presentation, Map.take(request, ["interruption_level", "sound"])} do
      {_, fields} when map_size(fields) == 0 ->
        {:ok, %{}}

      {:alert, %{"interruption_level" => "passive"} = fields} when map_size(fields) == 1 ->
        {:ok, %{"interruption-level" => "passive"}}

      {:alert, %{"sound" => "default"} = fields} when map_size(fields) == 1 ->
        {:ok, fields}

      _ ->
        :error
    end
  end

  defp deliver(conn, claim, apns, dispatcher) do
    started_at = System.monotonic_time(:millisecond)
    response = delivery_response(apns, claim, dispatcher)
    elapsed = System.monotonic_time(:millisecond) - started_at

    {status, reason} =
      case response do
        {:ok, reply} -> {reply.status, reply.reason}
        {:error, _cause} -> {nil, nil}
      end

    Logger.info(
      "event=harklight_push_relay server=#{claim.server} kind=#{claim.presentation} " <>
        "environment=#{claim.environment} apns_status=#{status || "-"} " <>
        "reason=#{reason || "-"} duration_ms=#{elapsed}"
    )

    case response do
      {:ok, reply} ->
        if reply.status != 200 do
          outcome =
            if reply.status == 429 or reply.status in 500..599, do: :retry, else: :rejected

          cause = reply.reason || Diagnostics.provider_cause(reply.status)
          Diagnostics.failure(:push_relay, :push_request, cause, outcome)
        end

        json(conn, 200, provider_reply(reply))

      {:error, cause} ->
        Diagnostics.failure(:push_relay, :push_request, cause, :retry)
        error(conn, 502, "unavailable")
    end
  end

  defp delivery_response(apns, claim, dispatcher) do
    token = fn profile, rejected -> PushDispatcher.apple_token(dispatcher, profile, rejected) end

    case APNS.deliver(apns, APNS.describe(claim), token) do
      {{:ok, reply, _report?}, _tokens} -> {:ok, reply}
      {{:error, cause}, _tokens} -> {:error, cause}
    end
  rescue
    error -> {:error, error}
  end

  defp provider_reply(reply) do
    %{
      "apns_status" => reply.status,
      "reason" => reply.reason,
      "timestamp" => reply.timestamp,
      "retry_after_ms" => if(reply.retry_after_ms > 0, do: reply.retry_after_ms)
    }
    |> Map.reject(fn {_field, value} -> is_nil(value) end)
  end

  defp with_request(conn, handler) do
    case read_body(conn, length: @max_body, read_length: @max_body) do
      {:ok, body, conn} when byte_size(body) <= @max_body ->
        case JSON.decode_line(body <> "\n", @max_body + 1) do
          {:ok, request} when is_map(request) -> handler.(conn, request)
          _ -> error(conn, 400, "invalid_request")
        end

      {result, _, conn} when result in [:ok, :more] ->
        error(conn, 413, "too_large")

      {:error, _reason} ->
        error(conn, 400, "invalid_request")
    end
  end

  defp error(conn, status, reason), do: json(conn, status, %{"error" => reason})

  defp json(conn, status, body) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, JSON.encode!(body))
  end
end
