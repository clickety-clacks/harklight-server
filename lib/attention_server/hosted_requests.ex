defmodule AttentionServer.HostedRequests do
  @moduledoc """
  The requests the hosted account component may post to the loopback listener
  that `AttentionServer.HostedHTTP` runs. See that module for what each one
  may and may not reach.
  """

  @behaviour Plug

  import Plug.Conn

  alias AttentionServer.{BrowserAuth, Diagnostics, JSON, Relay}

  @max_body 4 * 1024

  @impl Plug
  def init(opts), do: %{relay: Keyword.fetch!(opts, :relay), groups: Keyword.get(opts, :groups, false) == true}

  @impl Plug
  def call(conn, opts) do
    conn = put_resp_header(conn, "cache-control", "no-store")

    case {conn.method, conn.request_path, conn.path_info} do
      {"PUT", _, ["organizations", ref, "members"]} ->
        with_request(conn, &replace_organization(&1, &2, opts, ref))

      {"DELETE", _, ["organizations", ref]} ->
        organization_response(conn, Relay.delete_hosted_organization(opts.relay, ref))

      {"PUT", _, ["organizations", business, "groups", group]} when opts.groups ->
        with_request(conn, &replace_group(&1, &2, opts.relay, {business, group}))

      {"DELETE", _, ["organizations", business, "groups", group]} when opts.groups ->
        group_response(conn, Relay.delete_hosted_group(opts.relay, {business, group}))

      {_, _, ["organizations", _, "groups" | _]} ->
        send_resp(conn, 404, "")

      {"POST", "/accounts", _} -> with_request(conn, &create_account(&1, &2, opts))
      {"POST", "/algorithm", _} -> with_request(conn, &set_algorithm(&1, &2, opts))
      {"POST", "/sign-in-links", _} -> with_request(conn, &create_sign_in_link(&1, &2, opts))
      {"POST", "/delivery-state", _} -> with_request(conn, &set_delivery_state(&1, &2, opts))
      {"POST", "/test-reset", _} -> with_request(conn, &test_reset(&1, &2, opts))
      {"POST", _, ["persons", person, "notices"]} ->
        with_request(conn, &create_notice(&1, &2, opts.relay, person))

      {"POST", "/session-grants", _} ->
        with_request(conn, &create_session_grant(&1, &2, opts))

      {"POST", "/session-grants/redeem", _} ->
        with_request(conn, &redeem_session_grant(&1, &2, opts))

      {"POST", _, _} -> send_resp(conn, 404, "")
      _ -> send_resp(conn, 405, "")
    end
  rescue
    error ->
      Diagnostics.failure(:hosted_http, :request_dispatch, error, :outcome_unknown)
      reraise error, __STACKTRACE__
  end

  defp create_notice(conn, request, relay, person) do
    case Relay.hosted_notice(relay, person, request) do
      {:ok, %{item_id: item_id, created: created}} ->
        json(conn, 200, %{"item_id" => item_id, "created" => created})

      {:error, :invalid_notice} ->
        json(conn, 400, %{"error" => "invalid_notice"})

      {:error, :unknown_person} ->
        json(conn, 404, %{"error" => "unknown_person"})

      {:error, :key_conflict} ->
        json(conn, 409, %{"error" => "key_conflict"})

      {:error, :storage_unavailable} ->
        json(conn, 503, %{"error" => "storage_unavailable"})
    end
  end

  defp replace_organization(conn, %{"person_ids" => people} = request, opts, ref)
       when map_size(request) == 1 do
    organization_response(conn, Relay.replace_hosted_organization(opts.relay, ref, people))
  end

  defp replace_organization(conn, _request, _opts, _ref),
    do: json(conn, 400, %{"error" => "invalid_members"})

  defp organization_response(conn, :ok), do: send_resp(conn, 204, "")

  defp organization_response(conn, {:error, reason})
       when reason in [:invalid_organization, :invalid_members],
       do: json(conn, 400, %{"error" => to_string(reason)})

  defp organization_response(conn, {:error, :storage_unavailable}),
    do: json(conn, 503, %{"error" => "storage_unavailable"})

  defp replace_group(conn, %{"name" => name, "person_ids" => people, "version" => version} = body, relay, ref)
       when map_size(body) == 3 do
    group_response(conn, Relay.replace_hosted_group(relay, ref, name, people, version))
  end

  defp replace_group(conn, _body, _relay, _ref),
    do: group_response(conn, {:error, :invalid_group})

  defp group_response(conn, {:ok, %{group_id: id, rev: rev}}),
    do: json(conn, 200, %{"group_id" => id, "rev" => rev})

  defp group_response(conn, :ok), do: send_resp(conn, 204, "")
  defp group_response(conn, {:error, :not_found}), do: send_resp(conn, 404, "")

  defp group_response(conn, {:error, reason}) do
    status =
      case reason do
        :group_deleted -> 410
        :not_affiliated -> 422
        error when error in [:stale_version, :version_conflict] -> 409
        :storage_unavailable -> 503
        _ -> 400
      end

    json(conn, status, %{"error" => to_string(reason)})
  end

  defp create_account(conn, request, opts) do
    case Relay.hosted_account_create(opts.relay, Map.get(request, "account_ref")) do
      {:ok, person} -> json(conn, 200, %{"person_id" => person})
      {:error, :invalid_account_ref} -> json(conn, 400, %{"error" => "invalid_account_ref"})
      {:error, :storage_unavailable} -> json(conn, 503, %{"error" => "storage_unavailable"})
      {:error, reason} -> json(conn, 500, %{"error" => to_string(reason)})
    end
  end

  defp create_session_grant(
         conn,
         %{"purpose" => "browser", "person_id" => person, "transaction" => transaction, "method" => method},
         opts
       ) do
    case Relay.hosted_browser_grant(opts.relay, person, transaction, method) do
      {:ok, grant} -> json(conn, 200, %{"grant" => grant})
      {:error, :invalid_request} -> json(conn, 400, %{"error" => "invalid_request"})
      {:error, :invalid_sign_in} -> json(conn, 401, %{"error" => "unauthorized"})
      {:error, :not_hosted} -> json(conn, 404, %{"error" => "unknown_person"})
      {:error, :disabled} -> send_resp(conn, 404, "")
      _ -> json(conn, 503, %{"error" => "storage_unavailable"})
    end
  end

  defp create_session_grant(
         conn,
         %{"purpose" => "app", "person_id" => person, "transaction" => transaction, "method" => method},
         opts
       ) do
    case Relay.authorize_hosted_native_transaction(opts.relay, person, transaction, method) do
      {:ok, reference} -> json(conn, 200, %{"transaction" => reference})
      {:error, :invalid_request} -> json(conn, 400, %{"error" => "invalid_request"})
      {:error, :invalid_sign_in} -> json(conn, 401, %{"error" => "unauthorized"})
      {:error, :not_hosted} -> json(conn, 404, %{"error" => "unknown_person"})
      {:error, :disabled} -> send_resp(conn, 404, "")
      _ -> json(conn, 503, %{"error" => "storage_unavailable"})
    end
  end

  defp create_session_grant(conn, _, _opts), do: json(conn, 400, %{"error" => "invalid_request"})

  defp redeem_session_grant(conn, %{"grant" => grant, "transaction" => transaction}, opts) do
    case Relay.redeem_hosted_browser_grant(opts.relay, grant, transaction) do
      {:ok, %{cookie: cookie, method: method}} ->
        conn |> BrowserAuth.put_cookie(cookie, true) |> json(200, %{"method" => method})

      {:error, :invalid_sign_in} ->
        json(conn, 401, %{"error" => "unauthorized"})

      {:error, :disabled} ->
        send_resp(conn, 404, "")

      _ ->
        json(conn, 503, %{"error" => "storage_unavailable"})
    end
  end

  defp redeem_session_grant(conn, _, _opts), do: json(conn, 400, %{"error" => "invalid_request"})

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

  defp set_algorithm(conn, request, opts) do
    person = Map.get(request, "person_id")
    algorithm = Map.get(request, "algorithm_id")

    case Relay.hosted_set_algorithm(opts.relay, person, algorithm) do
      {:ok, result} ->
        body = %{
          "person_id" => result.person_id,
          "algorithm_id" => result.algorithm_id
        }

        body =
          if Map.has_key?(result, :status), do: Map.put(body, "status", result.status), else: body

        json(conn, 200, body)

      {:error, :unknown_algorithm} ->
        json(conn, 400, %{"error" => "unknown_algorithm"})

      {:error, :algorithm_managed_by_server} ->
        json(conn, 409, %{"error" => "algorithm_managed_by_server"})

      {:error, :not_hosted} ->
        json(conn, 404, %{"error" => "unknown_person"})

      {:error, :storage_unavailable} ->
        json(conn, 503, %{"error" => "storage_unavailable"})

      {:error, _reason} ->
        json(conn, 503, %{"error" => "storage_unavailable"})
    end
  end

  defp set_delivery_state(conn, request, opts) do
    person = Map.get(request, "person_id")
    target_state = Map.get(request, "state")

    case Relay.hosted_delivery_state(opts.relay, person, target_state) do
      {:ok, result} ->
        json(conn, 200, %{"person_id" => result.person_id, "state" => result.state})

      {:error, :invalid_state} ->
        json(conn, 400, %{"error" => "invalid_state"})

      {:error, :unknown_person} ->
        json(conn, 404, %{"error" => "unknown_person"})

      {:error, :storage_unavailable} ->
        json(conn, 503, %{"error" => "storage_unavailable"})
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
