defmodule AttentionServer.HostedRequests do
  @moduledoc """
  The requests the hosted account component may post to the loopback listener
  that `AttentionServer.HostedHTTP` runs. See that module for what each one
  may and may not reach.
  """

  @behaviour Plug

  import Plug.Conn

  alias AttentionServer.{Diagnostics, JSON, Relay}

  @max_body 4 * 1024

  @impl Plug
  def init(opts), do: %{relay: Keyword.fetch!(opts, :relay)}

  @impl Plug
  def call(conn, opts) do
    conn = put_resp_header(conn, "cache-control", "no-store")

    case {conn.method, conn.request_path} do
      {"POST", "/accounts"} -> with_request(conn, &create_account(&1, &2, opts))
      {"POST", "/sign-in-links"} -> with_request(conn, &create_sign_in_link(&1, &2, opts))
      {"POST", "/test-reset"} -> with_request(conn, &test_reset(&1, &2, opts))
      {"POST", _} -> send_resp(conn, 404, "")
      _ -> send_resp(conn, 405, "")
    end
  rescue
    error ->
      Diagnostics.failure(:hosted_http, :request_dispatch, error, :outcome_unknown)
      reraise error, __STACKTRACE__
  end

  defp create_account(conn, request, opts) do
    case Relay.hosted_account_create(opts.relay, Map.get(request, "account_ref")) do
      {:ok, person} -> json(conn, 200, %{"person_id" => person})
      {:error, :invalid_account_ref} -> json(conn, 400, %{"error" => "invalid_account_ref"})
      {:error, :storage_unavailable} -> json(conn, 503, %{"error" => "storage_unavailable"})
      {:error, reason} -> json(conn, 500, %{"error" => to_string(reason)})
    end
  end

  defp create_sign_in_link(conn, request, opts) do
    person = Map.get(request, "person_id")
    label = Map.get(request, "label")

    case Relay.hosted_sign_in_create(opts.relay, person, label) do
      {:ok, url} -> json(conn, 200, %{"sign_in_url" => url})
      {:error, :not_hosted} -> json(conn, 404, %{"error" => "unknown_person"})
      {:error, :invalid_label} -> json(conn, 400, %{"error" => "invalid_label"})
      {:error, :no_public_origin} -> json(conn, 503, %{"error" => "no_public_origin"})
      {:error, :storage_unavailable} -> json(conn, 503, %{"error" => "storage_unavailable"})
      {:error, reason} -> json(conn, 500, %{"error" => to_string(reason)})
    end
  end

  defp test_reset(conn, request, opts) do
    case Relay.hosted_test_reset(opts.relay, Map.get(request, "state")) do
      {:ok, result} ->
        body = %{"state" => result.state, "person_id" => result.person_id}
        body = if Map.has_key?(result, :code), do: Map.put(body, "code", result.code), else: body
        json(conn, 200, body)

      {:error, :unknown_state} ->
        json(conn, 400, %{"error" => "unknown_state"})

      {:error, reason} when reason in [:not_designated, :not_created, :not_test_account] ->
        json(conn, 404, %{"error" => to_string(reason)})

      {:error, :not_resettable} ->
        json(conn, 409, %{"error" => "not_resettable"})

      {:error, reason} when reason in [:storage_unavailable, :join_unavailable] ->
        json(conn, 503, %{"error" => to_string(reason)})

      {:error, reason} ->
        json(conn, 500, %{"error" => to_string(reason)})
    end
  end

  defp with_request(conn, handler) do
    case read_body(conn, length: @max_body, read_length: @max_body) do
      {:ok, body, conn} ->
        case JSON.decode_line(body <> "\n", @max_body + 1) do
          {:ok, request} when is_map(request) -> handler.(conn, request)
          _ -> json(conn, 400, %{"error" => "invalid_json"})
        end

      {:more, _, conn} ->
        json(conn, 413, %{"error" => "too_large"})

      {:error, _} ->
        send_resp(conn, 400, "")
    end
  end

  defp json(conn, status, body) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, JSON.encode!(body))
  end
end
