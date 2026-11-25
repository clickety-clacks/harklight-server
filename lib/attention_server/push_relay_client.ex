defmodule AttentionServer.PushRelayClient do
  @moduledoc false

  alias AttentionServer.{APNS, Diagnostics, JSON, PushHTTP}

  @connect_timeout 5_000
  @response_timeout 15_000

  def prepare!(url, server, cacerts) do
    uri = URI.new!(url)

    unless uri.scheme in ["https", "http"] and is_binary(uri.host) and uri.host != "" and
             is_nil(uri.userinfo) and is_nil(uri.query) and is_nil(uri.fragment) do
      raise ArgumentError, "invalid push relay URL"
    end

    prefix = String.trim_trailing(uri.path || "", "/") <> "/push-relay/v1"
    %{uri: uri, prefix: prefix, server: server, cacerts: cacerts}
  end

  def register(config) do
    case post(config, "/servers", [], %{"server" => config.server}) do
      {:ok, 201, _headers, body} ->
        with {:ok, %{"key" => "v1." <> key = value}} <- JSON.decode_line(body <> "\n"),
             [server, signature] when byte_size(signature) > 0 <- :binary.split(key, "."),
             true <- server == config.server do
          {:ok, value}
        else
          _ -> {:error, :invalid_response, 0}
        end

      response ->
        transport_error(response)
    end
  end

  def send(config, claim, key) do
    request = request_fields(claim)
    {response, key} = push(config, request, key)

    result =
      case response do
        {:ok, 200, _headers, body} ->
          interpret(body, claim)

        {:ok, 400, _headers, body} ->
          cause =
            if JSON.decode_line(body <> "\n") == {:ok, %{"error" => "unsupported"}},
              do: :not_supported,
              else: :invalid_request

          Diagnostics.failure(:push_relay, :push_request, cause, :rejected, claim.claim_id)
          APNS.response_outcome(%{status: 400, reason: nil}, claim)

        response ->
          {:error, cause, delay} = transport_error(response)
          APNS.retry(cause, claim, :push_relay, delay)
      end

    {result, key}
  rescue
    error -> {APNS.retry(error, claim, :push_relay), key}
  end

  defp push(config, request, nil) do
    case register(config) do
      {:ok, key} -> push(config, request, key)
      {:error, _, _} = error -> {error, nil}
    end
  end

  defp push(config, request, key) do
    case post_push(config, request, key) do
      {:ok, 401, _headers, _body} -> register_and_push(config, request)
      response -> {response, key}
    end
  end

  defp register_and_push(config, request) do
    case register(config) do
      {:ok, key} ->
        case post_push(config, request, key) do
          {:ok, 401, _headers, _body} = response -> {response, nil}
          response -> {response, key}
        end

      {:error, _, _} = error ->
        {error, nil}
    end
  end

  defp post_push(config, request, key),
    do: post(config, "/pushes", [{"authorization", "Bearer " <> key}], request)

  defp request_fields(claim) do
    %{
      "token" => claim.address,
      "environment" => environment(claim.profile_id),
      "kind" => Atom.to_string(claim.presentation),
      "badge" => Map.get(claim, :badge)
    }
    |> Map.reject(fn {_field, value} -> is_nil(value) end)
    |> Map.merge(alert_fields(claim))
  end

  defp alert_fields(%{presentation: :alert, quiet: true}),
    do: %{"interruption_level" => "passive"}

  defp alert_fields(%{presentation: :alert}), do: %{"sound" => "default"}
  defp alert_fields(_claim), do: %{}

  defp environment("harklight-sandbox"), do: "sandbox"
  defp environment("harklight-production"), do: "production"

  defp interpret(body, claim) do
    case JSON.decode_line(body <> "\n") do
      {:ok, %{"apns_status" => status} = reply} when status in 100..599 ->
        reason = reply["reason"]

        response = %{
          status: status,
          reason: if(APNS.safe_reason?(reason), do: reason),
          timestamp: non_negative_integer(reply["timestamp"]),
          retry_after_ms: non_negative_integer(reply["retry_after_ms"]) || 0
        }

        response
        |> APNS.response_outcome(claim)
        |> APNS.report_response(claim, :push_relay)

      _ ->
        APNS.retry(:invalid_response, claim, :push_relay)
    end
  end

  defp non_negative_integer(value) when is_integer(value) and value >= 0, do: value
  defp non_negative_integer(_value), do: nil

  defp transport_error({:error, _, _} = error), do: error
  defp transport_error({:error, cause}), do: {:error, cause, 0}

  defp transport_error({:ok, status, headers, _body}),
    do: {:error, Diagnostics.provider_cause(status), APNS.retry_after_ms(headers)}

  defp post(config, path, headers, body) do
    %{uri: uri, prefix: prefix, cacerts: cacerts} = config
    scheme = if uri.scheme == "https", do: :https, else: :http

    options = [mode: :passive, timeout: @connect_timeout]

    options =
      if cacerts, do: Keyword.put(options, :transport_opts, cacerts: cacerts), else: options

    headers = [{"content-type", "application/json"} | headers]
    encoded = JSON.encode!(body)

    with {:ok, conn} <- Mint.HTTP.connect(scheme, uri.host, uri.port, options) do
      try do
        with {:ok, conn, ref} <- Mint.HTTP.request(conn, "POST", prefix <> path, headers, encoded),
             {:ok, _conn, status, headers, response} <-
               PushHTTP.receive_response(conn, ref, @response_timeout) do
          {:ok, status, headers, response}
        else
          {:error, _conn, cause} -> {:error, cause}
        end
      after
        _ = Mint.HTTP.close(conn)
      end
    end
  end
end
