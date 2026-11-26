defmodule AttentionServer.DashboardReadHTTP do
  @moduledoc """
  Authenticated browser routes for the self-hosted device dashboard.

  A backend resolves an opaque browser session and rechecks owner authority for
  each invitation, exact pending request, and decision. A TLS listener supplies
  the pinned host; this plug never accepts a browser claim as owner authority.

  Options: `:expected_origin` fixes the origin that form posts, join checks and
  sign-in must come from (default: the request's own host and port, so a server
  behind a public front end names its public address here); `:hosted` selects
  the hosted wording, which never tells a person to ask an owner. Self-hosted
  mode serves the pinned web client with its local sign-in help;
  `:hosted_addresses` (hosted only) names where a browser goes without a usable
  Server session (`:sign_in`), after Sign out (`:sign_out`, defaulting to
  `:sign_in`) and for the Account link (`:account`).
  """

  @behaviour Plug

  import Plug.Conn

  alias AttentionServer.{BrowserAuth, DashboardBrand, DashboardView, DeviceProof, Diagnostics, JSON, LocalSignInPage, Message}

  @cookie "__Host-harklight"
  @invite_cookie "__Host-harklight-invite"
  @invite_cookie_age 30 * 24 * 60 * 60
  @dashboard_csp "default-src 'none'; script-src 'self'; connect-src 'self'; style-src 'unsafe-inline'; img-src data:; base-uri 'none'; frame-ancestors 'none'"
  # Where a self-hosted browser lands after signing out or losing its session.
  # The sign-in page shows that it is signed out instead of an invalid link.
  @signed_out_path "/sign-in?signed_out=1"
  @web_client_csp "default-src 'none'; script-src 'self'; connect-src 'self'; " <>
                    "style-src 'self' 'unsafe-inline'; font-src 'self'; img-src 'self' blob: data:; " <>
                    "base-uri 'none'; frame-ancestors 'none'"
  @web_content_types %{
    ".html" => "text/html",
    ".js" => "text/javascript",
    ".css" => "text/css",
    ".woff2" => "font/woff2",
    ".woff" => "font/woff",
    ".map" => "application/json"
  }

  @impl Plug
  def init(opts) do
    backend = Keyword.fetch!(opts, :backend)
    skill_path = Keyword.fetch!(opts, :skill_path)
    hosted = Keyword.get(opts, :hosted, false) == true

    skill_path_saas =
      Keyword.get(opts, :skill_path_saas) || sibling_skill_path(skill_path, "harklight-agent-saas")

    %{
      backend: backend,
      skill_path: skill_path,
      expected_origin: Keyword.get(opts, :expected_origin),
      hosted: hosted,
      invitations_configured: Keyword.get(opts, :invitations_configured, false),
      hosted_addresses:
        if(hosted, do: hosted_addresses(Keyword.get(opts, :hosted_addresses)), else: %{}),
      hosted_testflight_url:
        if(
          hosted,
          do: hosted_testflight_url(Keyword.get(opts, :hosted_testflight_url)),
          else: nil
        ),
      web_static:
        Plug.Static.init(
          at: "/app",
          from: {:attention_server, "priv/static/app"},
          only: ~w(index.html assets),
          content_types: web_asset_content_types(),
          cache_control_for_vsn_requests: "no-store",
          cache_control_for_etags: "no-store"
        ),
      skill_path_saas: skill_path_saas,
      skill_path_harklight: sibling_skill_path(skill_path, "harklight"),
      skill_path_core:
        Keyword.get(opts, :skill_path_core) ||
          Path.join(Path.dirname(Path.dirname(skill_path)), "core.md")
    }
  end

  defp sibling_skill_path(skill_path, name) do
    Path.join([Path.dirname(Path.dirname(skill_path)), name, "SKILL.md"])
  end

  defp hosted_addresses(addresses) when is_map(addresses) do
    sign_in = address(Map.get(addresses, :sign_in))

    %{
      sign_in: sign_in,
      sign_out: address(Map.get(addresses, :sign_out)) || sign_in,
      account: address(Map.get(addresses, :account))
    }
  end

  defp hosted_addresses(_), do: %{sign_in: nil, sign_out: nil, account: nil}

  defp hosted_testflight_url(url) when is_binary(url) do
    uri = URI.parse(url)

    if uri.scheme == "https" and uri.host == "testflight.apple.com" and
         uri.port in [nil, 443] and
         is_nil(uri.userinfo) and is_binary(uri.path) and
         String.starts_with?(uri.path, "/join/") and byte_size(uri.path) > byte_size("/join/"),
       do: url,
       else: nil
  rescue
    ArgumentError -> nil
  end

  defp hosted_testflight_url(_), do: nil

  defp address(url) when is_binary(url) and url != "", do: url
  defp address(_), do: nil

  @impl Plug
  def call(conn, opts) do
    conn =
      conn
      |> put_resp_header("cache-control", "no-store")
      |> put_resp_header("content-security-policy", content_security_policy(conn, opts))
      |> put_resp_header("referrer-policy", "no-referrer")
      |> put_resp_header("x-content-type-options", "nosniff")

    cond do
      conn.scheme != :https ->
        send_resp(conn, 403, "HTTPS required")

      conn.method == "GET" and conn.request_path == "/sign-in" ->
        sign_in_get(conn, opts)

      conn.method == "GET" and conn.request_path == "/sign-in/code.js" ->
        if local_code_available?(opts) do
          conn |> put_resp_content_type("text/javascript", "utf-8") |> send_resp(200, LocalSignInPage.script())
        else
          send_resp(conn, 404, "")
        end

      conn.method == "GET" and conn.request_path == "/sign-in/passkey.js" ->
        if local_code_available?(opts) do
          conn |> put_resp_content_type("text/javascript", "utf-8") |> send_resp(200, LocalSignInPage.passkey_script())
        else
          send_resp(conn, 404, "")
        end

      conn.method == "POST" and conn.request_path == "/sign-in/passkey/options" ->
        local_passkey_request(conn, opts, :assertion_options)

      conn.method == "POST" and conn.request_path == "/sign-in/passkey/finish" ->
        local_passkey_request(conn, opts, :assertion_finish)

      conn.method == "POST" and conn.request_path == "/devices/passkey/options" ->
        local_passkey_request(fetch_cookies(conn), opts, :registration_options)

      conn.method == "POST" and conn.request_path == "/devices/passkey/finish" ->
        local_passkey_request(fetch_cookies(conn), opts, :registration_finish)

      conn.method == "POST" and conn.request_path == "/sign-in/code" ->
        redeem_local_code(conn, opts)

      conn.method == "GET" and conn.request_path == "/sign-in.js" ->
        sign_in_script(conn, opts)

      conn.method == "POST" and conn.request_path == "/sign-in" ->
        redeem_sign_in(conn, opts)

      conn.method == "POST" and conn.request_path == "/sign-out" ->
        sign_out(fetch_cookies(conn), opts)

      conn.method == "GET" and conn.request_path == "/join" ->
        join_page(conn, opts)

      conn.method == "GET" and conn.request_path == "/join.js" ->
        join_script(conn, opts)

      conn.method == "POST" and conn.request_path == "/join/resolve" ->
        resolve_join(conn, opts)

      conn.method == "GET" and conn.request_path == "/agent-skills/SKILL.md" ->
        serve_skill_file(conn, opts.skill_path)

      conn.method == "GET" and
          conn.request_path == "/agent-skills/harklight-agent-saas/SKILL.md" ->
        serve_skill_file(conn, opts.skill_path_saas, fn skill ->
          String.replace(
            skill,
            "@HOSTED_APP_INSTALL@",
            hosted_app_install(opts.hosted_testflight_url)
          )
        end)

      conn.method == "GET" and conn.request_path == "/agent-skills/harklight/SKILL.md" ->
        serve_skill_file(conn, opts.skill_path_harklight)

      conn.method == "GET" and conn.request_path == "/agent-skills/core.md" ->
        serve_skill_file(conn, opts.skill_path_core)

      conn.method not in ["GET", "POST"] ->
        send_resp(conn, 405, "Method not allowed")

      true ->
        authenticated_request(fetch_cookies(conn), opts)
    end
  rescue
    error ->
      Diagnostics.failure(:dashboard_http, :request_dispatch, error, :outcome_unknown)
      reraise error, __STACKTRACE__
  catch
    kind, reason ->
      Diagnostics.failure(:dashboard_http, :request_dispatch, {kind, reason}, :outcome_unknown)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp authenticated_request(conn, opts) do
    case Map.get(conn.req_cookies, @cookie) do
      nil ->
        no_session(conn, opts)

      opaque ->
        case backend_call(opts.backend, :resolve_session, opaque) do
          {:ok, principal} -> route(conn, opts, principal)
          {:error, :storage_unavailable} -> send_resp(conn, 503, "Session unavailable")
          _ -> no_session(conn, opts)
        end
    end
  end

  defp sign_out(conn, opts) do
    session = Map.get(conn.req_cookies, @cookie)

    case form(conn, opts, ~w(confirm)) do
      {:ok, %{"confirm" => "sign_out"}, conn} ->
        result = if session, do: backend_call(opts.backend, :end_session, session), else: :ok
        complete_sign_out(conn, opts, result)

      {_, _, conn} ->
        send_resp(conn, 403, "Sign out refused")
    end
  end

  defp complete_sign_out(conn, opts, :ok) do
    location =
      if opts.hosted,
        do: opts.hosted_addresses.sign_out || "/devices",
        else: @signed_out_path

    conn
    |> delete_resp_cookie(@cookie, secure: true, http_only: true, same_site: "Lax", path: "/")
    |> put_resp_header("location", location)
    |> send_resp(303, "")
  end

  defp complete_sign_out(conn, _opts, _error), do: send_resp(conn, 503, "Sign out unavailable")

  # A hosted deployment sends a browser with no usable dashboard session to its
  # sign-in page for other requests.
  defp no_session(conn, %{hosted: true, hosted_addresses: %{sign_in: url}}) when is_binary(url) do
    conn
    |> delete_resp_cookie(@cookie, secure: true, http_only: true, same_site: "Lax", path: "/")
    |> put_resp_header("location", url)
    |> send_resp(303, "")
  end

  defp no_session(conn, %{hosted: false} = opts) do
    cond do
      conn.method == "GET" and self_hosted_page_request?(conn.request_path) and
          local_code_available?(opts) ->
        conn |> put_resp_header("location", "/sign-in") |> send_resp(303, "")

      self_hosted_page_request?(conn.request_path) ->
        conn
        |> put_resp_content_type("text/html", "utf-8")
        |> send_resp(401, self_hosted_sign_in_help_page())

      true ->
        send_resp(conn, 401, "Sign in to view your devices")
    end
  end

  defp no_session(conn, _opts), do: send_resp(conn, 401, "Sign in to view your devices")

  defp self_hosted_page_request?(path) do
    path in ["/devices", "/components", "/prioritization", "/inbox", "/settings"] or
      String.starts_with?(path, "/app/")
  end

  defp self_hosted_sign_in_help_page do
    ~s(<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Sign in · Harklight</title>#{DashboardBrand.favicon()}</head><body><main>#{DashboardBrand.lockup(decorative: true)}<h1>Sign in to Harklight</h1>#{self_hosted_sign_in_help_copy()}<p><a href="/sign-in">Open a sign-in link</a></p></main></body></html>)
  end

  defp self_hosted_sign_in_help_copy do
    ~s(<p>To get a new one-use sign-in link, ask the server owner to run <code>bin/harklight owner browser-sign-in</code>.</p>)
  end

  # The web client uses same-origin fetch and server-sent events only.
  defp content_security_policy(conn, %{hosted: false}) do
    if web_client_response?(conn.request_path),
      do: @web_client_csp,
      else: @dashboard_csp
  end

  defp content_security_policy(_conn, _opts), do: @dashboard_csp

  defp web_client_response?(path),
    do: path in ["/inbox", "/settings"] or String.starts_with?(path, "/app/")

  defp web_asset_content_types do
    case :code.priv_dir(:attention_server) do
      priv_dir when is_list(priv_dir) ->
        priv_dir
        |> List.to_string()
        |> Path.join("static/app/**/*")
        |> Path.wildcard()
        |> Enum.filter(&File.regular?/1)
        |> Map.new(fn path ->
          {Path.basename(path),
           Map.get(@web_content_types, Path.extname(path), MIME.from_path(path))}
        end)

      _ ->
        %{}
    end
  end

  # Hosted-only page chrome: the Account link and the Sign out control.
  defp chrome(%{hosted: true, hosted_addresses: addresses} = opts),
    do: [
      hosted: true,
      account_url: addresses.account,
      sign_out: true,
      agent_enrollment_only: "email_code" in sign_in_methods(opts),
      testflight_url: opts.hosted_testflight_url
    ]

  defp chrome(opts) do
    local_code? = local_code_available?(opts)
    [
      prioritization: true,
      invitations_configured: opts.invitations_configured,
      sign_out: local_code?,
      local_sign_in_code: local_code?,
      agent_enrollment_only: local_code?
    ]
  end

  defp route(
         %{method: "GET", request_path: "/api/profile"} = conn,
         _opts,
         %{display_name: display_name}
       )
       when is_binary(display_name) do
    conn
    |> put_resp_content_type("application/json", "utf-8")
    |> send_resp(200, JSON.encode!(%{"display_name" => display_name}))
  end

  defp route(
         %{method: "GET", request_path: "/api/profile"} = conn,
         %{hosted: false},
         _principal
       ),
       do:
         conn
         |> put_resp_content_type("application/json", "utf-8")
         |> send_resp(200, JSON.encode!(%{"display_name" => ""}))

  defp route(
         %{method: "GET", request_path: "/api/profile"} = conn,
         %{hosted: true},
         _principal
       ),
       do: send_resp(conn, 503, "Profile unavailable")

  defp route(
         %{method: "GET", request_path: "/app/config.json"} = conn,
         %{hosted: false},
         _principal
       ) do
    config = %{
      "accountUrl" => "",
      "sessionEndedPath" => @signed_out_path,
      "flags" => %{"webClient" => true, "attachments" => true, "answerForms" => true}
    }

    conn
    |> put_resp_content_type("application/json", "utf-8")
    |> send_resp(200, JSON.encode!(config))
  end

  defp route(
         %{method: "GET", request_path: path} = conn,
         %{hosted: false} = opts,
         _principal
       )
       when path in ["/inbox", "/settings"] do
    serve_web_index(conn, opts)
  end

  defp route(
         %{method: "GET", request_path: "/app/"} = conn,
         %{hosted: false} = opts,
         _principal
       ),
       do: serve_web_index(conn, opts)

  defp route(
         %{method: "GET", path_info: ["app" | _]} = conn,
         %{hosted: false} = opts,
         _principal
       ),
       do: serve_web_static(conn, opts)

  defp route(%{method: "GET", request_path: "/devices/passkey"} = conn, opts, _principal) do
    if local_code_available?(opts) do
      conn |> put_resp_content_type("text/html", "utf-8") |> send_resp(200, LocalSignInPage.setup_html())
    else
      send_resp(conn, 404, "")
    end
  end

  defp route(%{method: "POST", request_path: "/devices/sign-in-code"} = conn, opts, principal) do
    if local_code_available?(opts) do
      case form(conn, opts, ~w(confirm)) do
        {:ok, %{"confirm" => "create_code"}, conn} ->
          case backend_call(opts.backend, :issue_owner_code, principal) do
            {:ok, %{"code" => code}} ->
              conn
              |> put_resp_content_type("text/html", "utf-8")
              |> send_resp(200, DashboardView.one_time_code(display_name(principal), code, chrome(opts)))

            _ ->
              send_resp(conn, 503, "One-time code unavailable")
          end

        _ ->
          send_resp(conn, 403, "One-time code request refused")
      end
    else
      send_resp(conn, 404, "")
    end
  end

  defp route(%{method: "GET", request_path: "/devices"} = conn, opts, principal) do
    case backend_call(opts.backend, :list_devices, principal) do
      {:ok, %{person_name: name, devices: devices, waiting: waiting}}
      when is_list(devices) and is_list(waiting) ->
        new_form_request_id =
          case Map.get(conn.req_cookies, @invite_cookie) do
            request_id when is_binary(request_id) ->
              if Message.uuid_v4?(request_id), do: nil, else: new_request_id()

            _ ->
              new_request_id()
          end

        conn
        |> put_resp_content_type("text/html", "utf-8")
        |> send_resp(
          200,
          DashboardView.devices(name, devices, waiting, new_form_request_id, chrome(opts))
        )

      _ ->
        send_resp(conn, 403, "Devices unavailable")
    end
  end

  defp route(
         %{method: "GET", path_info: ["devices", "remove", device_key]} = conn,
         opts,
         principal
       ) do
    case removable_device(opts, principal, device_key) do
      {:ok, device} ->
        conn
        |> put_resp_content_type("text/html", "utf-8")
        |> send_resp(
          200,
          DashboardView.remove_device_confirmation(display_name(principal), device, chrome(opts))
        )

      :error ->
        send_resp(conn, 404, "Device unavailable")
    end
  end

  defp route(
         %{method: "POST", path_info: ["devices", "remove", device_key]} = conn,
         opts,
         principal
       ) do
    with {:ok, %{"confirm" => "remove"}, conn} <- form(conn, opts, ~w(confirm)),
         {:ok, _device} <- removable_device(opts, principal, device_key),
         :ok <- backend_call(opts.backend, :revoke_device, {principal, device_key}) do
      conn |> put_resp_header("location", "/devices") |> send_resp(303, "")
    else
      _ -> send_resp(conn, 409, "Device changed or unavailable; review it again")
    end
  end

  defp route(
         %{method: "GET", request_path: "/levels"} = conn,
         opts,
         principal
       ) do
    case backend_call(opts.backend, :levels, principal) do
      {:ok, result} ->
        send_levels_json(conn, result, 200)

      {:error, :storage_unavailable} ->
        send_resp(conn, 503, "Priority levels are unavailable.")

      _ ->
        send_resp(conn, 403, "Priority levels are unavailable.")
    end
  end

  defp route(
         %{method: "POST", request_path: "/levels"} = conn,
         opts,
         principal
       ) do
    case json_body(conn, opts, 1_048_576) do
      {:ok, %{"levels" => rows} = body, conn}
      when map_size(body) == 1 and is_list(rows) ->
        case backend_call(opts.backend, :save_levels, {principal, rows}) do
          {:ok, result} ->
            send_levels_json(conn, result, 200)

          {:error, :invalid_levels} ->
            send_resp(conn, 400, "The level table is invalid.")

          {:error, :storage_unavailable} ->
            send_resp(conn, 503, "The server could not save these levels.")

          {:error, :forbidden} ->
            send_resp(conn, 403, "Priority levels were refused.")

          _ ->
            send_resp(conn, 400, "The level table is invalid.")
        end

      # json_body refuses a foreign origin, content type or body as
      # {:error, conn}; any other body shape is {:ok, body, conn}.
      {:error, conn} ->
        send_resp(conn, 400, "The level table is invalid.")

      {:ok, _body, conn} ->
        send_resp(conn, 400, "The level table is invalid.")
    end
  end

  defp route(
         %{method: "GET", request_path: "/prioritization"} = conn,
         %{hosted: false} = opts,
         principal
       ) do
    case backend_call(opts.backend, :prioritization, principal) do
      {:ok, settings} -> send_prioritization_page(conn, principal, settings, nil, 200, opts)
      _ -> send_resp(conn, 403, "Prioritization settings unavailable")
    end
  end

  defp route(
         %{method: "POST", request_path: "/prioritization"} = conn,
         %{hosted: false} = opts,
         principal
       ) do
    case form(conn, opts, ~w(algorithm_id endpoint model), 8_192) do
      {:ok, %{"algorithm_id" => algorithm, "endpoint" => endpoint, "model" => model}, conn} ->
        case backend_call(
               opts.backend,
               :save_prioritization,
               {principal, algorithm, endpoint, model}
             ) do
          {:ok, _settings} ->
            conn |> put_resp_header("location", "/prioritization") |> send_resp(303, "")

          {:error, reason, settings} ->
            send_prioritization_page(
              conn,
              principal,
              settings,
              prioritization_error(reason),
              400,
              opts
            )

          {:error, reason} ->
            case backend_call(opts.backend, :prioritization, principal) do
              {:ok, settings} ->
                send_prioritization_page(
                  conn,
                  principal,
                  settings,
                  prioritization_error(reason),
                  if(reason == :storage_unavailable, do: 503, else: 400),
                  opts
                )

              _ ->
                send_resp(conn, 503, "Prioritization settings unavailable")
            end

          _ ->
            send_resp(conn, 503, "Prioritization settings unavailable")
        end

      {_, _, conn} ->
        send_resp(conn, 403, "Prioritization settings refused")
    end
  end

  defp route(%{method: "GET", request_path: "/devices/add"} = conn, _opts, _principal) do
    conn |> put_resp_header("location", "/devices") |> send_resp(303, "")
  end

  defp route(%{method: "GET", request_path: "/invitation-share.js"} = conn, _opts, _principal) do
    conn
    |> put_resp_content_type("text/javascript", "utf-8")
    |> send_resp(200, invitation_share_script())
  end

  defp route(%{method: "GET", request_path: "/components.js"} = conn, _opts, _principal) do
    conn
    |> put_resp_content_type("text/javascript", "utf-8")
    |> send_resp(200, components_script())
  end

  defp route(%{method: "POST", request_path: "/devices/add"} = conn, opts, principal) do
    case form(conn, opts, ~w(confirm)) do
      {:ok, %{"confirm" => "invite"}, conn} ->
        case Map.get(conn.req_cookies, @invite_cookie) do
          request_id when is_binary(request_id) ->
            if Message.uuid_v4?(request_id) do
              create_invitation(conn, opts, principal, request_id)
            else
              invitation_failure(
                conn,
                opts,
                principal,
                409,
                "This browser lost its invitation request identity.",
                :new
              )
            end

          _ ->
            invitation_failure(
              conn,
              opts,
              principal,
              403,
              "The invitation request was not accepted or its browser cookie is missing.",
              :new
            )
        end

      {_, _, conn} ->
        invitation_failure(
          conn,
          opts,
          principal,
          403,
          "The invitation request was not accepted or its browser cookie is missing.",
          :new
        )
    end
  end

  defp route(%{method: "POST", request_path: "/devices/invitation/new"} = conn, opts, principal) do
    case form(conn, opts, ~w(confirm request_id)) do
      {:ok, %{"confirm" => "new_invitation", "request_id" => request_id}, conn} ->
        if Message.uuid_v4?(request_id) do
          conn
          |> put_invite_cookie(request_id)
          |> create_invitation(opts, principal, request_id)
        else
          invitation_failure(
            conn,
            opts,
            principal,
            403,
            "A new invitation request was not accepted.",
            :new
          )
        end

      {_, _, conn} ->
        invitation_failure(
          conn,
          opts,
          principal,
          403,
          "This invitation form could not be verified.",
          :none
        )
    end
  end

  defp route(%{method: "GET", path_info: ["devices", "waiting", code]} = conn, opts, principal) do
    with true <- valid_code?(code),
         {:ok, request} <- backend_call(opts.backend, :review_pending, {principal, code}) do
      conn
      |> put_resp_content_type("text/html", "utf-8")
      |> send_resp(
        200,
        DashboardView.review_pending(display_name(principal), code, request, chrome(opts))
      )
    else
      _ -> send_resp(conn, 404, "Request unavailable")
    end
  end

  defp route(
         %{method: "POST", path_info: ["devices", "waiting", code, action]} = conn,
         opts,
         principal
       )
       when action in ["approve", "decline"] do
    if valid_code?(code) do
      case form(conn, opts, {:one_of, [~w(key deadline), ~w(key deadline senders)]}, 32_768) do
        {:ok, %{"key" => key, "deadline" => deadline} = fields, conn} ->
          with true <- Regex.match?(~r/\A[0-9a-f]{64}\z/, key),
               {:ok, deadline} <- parse_deadline(deadline),
               {:ok, %{key: ^key, expires_at: ^deadline}} <-
                 backend_call(opts.backend, :review_pending, {principal, code}),
               true <- not Map.has_key?(fields, "senders") or action == "approve",
               operation <- if(action == "approve", do: :approve_pending, else: :decline_pending),
               senders <-
                 fields
                 |> Map.get("senders", "")
                 |> String.split(",", trim: true)
                 |> Enum.map(&String.trim/1)
                 |> Enum.reject(&(&1 == "")),
               arguments <-
                 if(action == "approve",
                   do: {principal, code, key, deadline, senders},
                   else: {principal, code, key, deadline}
                 ),
               result <- backend_call(opts.backend, operation, arguments),
               true <- result == :ok or match?({:ok, _}, result) do
            conn |> put_resp_header("location", "/devices") |> send_resp(303, "")
          else
            _ -> send_resp(conn, 409, "Request changed or unavailable; review it again")
          end

        {_, _, conn} ->
          send_resp(conn, 409, "Request changed or unavailable; review it again")
      end
    else
      send_resp(conn, 409, "Request changed or unavailable; review it again")
    end
  end

  defp route(%{method: "GET", request_path: "/agent-skills"} = conn, _opts, _principal) do
    conn |> put_resp_header("location", "/components") |> send_resp(303, "")
  end

  defp route(%{method: "GET", request_path: "/components"} = conn, opts, principal) do
    html =
      DashboardView.components(
        display_name(principal),
        skill_version(opts.skill_path),
        skill_version(opts.skill_path_saas),
        skill_version(opts.skill_path_harklight),
        chrome(opts)
      )

    conn
    |> put_resp_content_type("text/html", "utf-8")
    |> send_resp(200, html)
  end

  defp route(conn, _opts, _principal), do: send_resp(conn, 404, "Not found")

  defp serve_web_index(conn, opts) do
    conn
    |> Map.put(:path_info, ["app", "index.html"])
    |> serve_web_static(opts)
  end

  defp serve_web_static(conn, opts) do
    case Plug.Static.call(conn, opts.web_static) do
      %{halted: true} = conn -> conn
      conn -> send_resp(conn, 404, "Not found")
    end
  end

  defp removable_device(opts, principal, device_key) do
    if Regex.match?(~r/\A[0-9a-f]{64}\z/, device_key) do
      case backend_call(opts.backend, :list_devices, principal) do
        {:ok, %{devices: devices}} when is_list(devices) ->
          case Enum.find(devices, fn device ->
                 Map.get(device, :device_id) == device_key and
                   Map.get(device, :can_revoke) == true
               end) do
            nil -> :error
            device -> {:ok, device}
          end

        _ ->
          :error
      end
    else
      :error
    end
  end

  defp send_prioritization_page(conn, principal, settings, message, status, opts) do
    conn
    |> put_resp_content_type("text/html", "utf-8")
    |> send_resp(
      status,
      DashboardView.prioritization(display_name(principal), settings, message, chrome(opts))
    )
  end

  defp prioritization_error(:invalid_endpoint), do: "Enter a valid HTTPS Jev endpoint."
  defp prioritization_error(:invalid_model), do: "Enter a model name."
  defp prioritization_error(:storage_unavailable), do: "The server could not save these settings."
  defp prioritization_error(_), do: "The server could not update prioritization settings."

  defp hosted_app_install(nil) do
    "Harklight for iPhone is in testing and is not yet open to everyone. " <>
      "Request an invite at [app.harklight.net/account/signup/](https://app.harklight.net/account/signup/); " <>
      "testers receive an invitation through Apple's TestFlight app. " <>
      "If you don't have TestFlight yet, install it from the App Store first."
  end

  defp hosted_app_install(url) do
    "Harklight for iPhone is available through Apple's TestFlight app. " <>
      "On your iPhone, open the [Harklight TestFlight invitation](<#{url}>) and follow TestFlight's prompts to install Harklight. " <>
      "If you don't have TestFlight yet, install it from the App Store first."
  end

  defp serve_skill_file(conn, skill_path, render \\ &Function.identity/1) do
    case Diagnostics.result(
           :dashboard_http,
           :request_read,
           File.read(skill_path),
           :unavailable
         ) do
      {:ok, skill} ->
        conn
        |> put_resp_content_type("text/plain", "utf-8")
        |> send_resp(200, render.(skill))

      _ ->
        send_resp(conn, 503, "Skill unavailable")
    end
  end

  defp skill_version(skill_path) do
    with {:ok, contents} <- File.read(skill_path),
         [_, version] <- Regex.run(~r/^version:\s*"([^"]+)"/m, contents) do
      {:ok, version}
    else
      _ -> :error
    end
  end

  # A waiting request created now has no deadline; the form carries "".
  defp parse_deadline(""), do: {:ok, nil}

  defp parse_deadline(value) do
    case Integer.parse(value) do
      {deadline, ""} -> {:ok, deadline}
      _ -> :error
    end
  end

  defp valid_code?(code), do: is_binary(code) and Regex.match?(~r/\A[A-Z2-7]{10}\z/, code)

  defp invitation_failure(conn, opts, principal, status, message, action \\ :retry) do
    conn
    |> put_resp_content_type("text/html", "utf-8")
    |> send_resp(
      status,
      DashboardView.invitation_failure(
        display_name(principal),
        message,
        action,
        new_request_id(),
        chrome(opts)
      )
    )
  end

  defp put_invite_cookie(conn, request_id) do
    put_resp_cookie(conn, @invite_cookie, request_id,
      secure: true,
      http_only: true,
      same_site: "Lax",
      path: "/",
      max_age: @invite_cookie_age
    )
  end

  defp create_invitation(conn, opts, principal, request_id) do
    case backend_call(opts.backend, :create_invitation, {principal, request_id}) do
      {:ok, invitation} ->
        conn
        |> put_resp_content_type("text/html", "utf-8")
        |> send_resp(
          200,
          DashboardView.invitation(
            display_name(principal),
            invitation,
            new_request_id(),
            chrome(opts)
          )
        )

      {:error, :forbidden} ->
        invitation_failure(
          conn,
          opts,
          principal,
          403,
          if(opts.hosted,
            do: "Your access changed.",
            else: "Your owner access changed. Sign in again with a new owner link."
          )
        )

      {:error, :capacity} ->
        invitation_failure(
          conn,
          opts,
          principal,
          503,
          "The invitation limit is full."
        )

      {:error, :used} ->
        invitation_failure(
          conn,
          opts,
          principal,
          409,
          "This invitation was used.",
          :new
        )

      {:error, :expired} ->
        invitation_failure(
          conn,
          opts,
          principal,
          409,
          "This invitation expired.",
          :new
        )

      {:error, :request_conflict} ->
        invitation_failure(
          conn,
          opts,
          principal,
          409,
          "This invitation request no longer matches the current owner or server settings.",
          :new
        )

      _ ->
        invitation_failure(
          conn,
          opts,
          principal,
          503,
          "The server could not create an invitation."
        )
    end
  end

  # The signed-in person is named by the readable label a hosted session carries,
  # or by the person identifier a self-hosted owner signed in as.
  defp display_name(principal) do
    case Map.get(principal, :display_name) do
      name when is_binary(name) -> name
      _ -> principal.person_id
    end
  end

  defp expected_origin(conn, opts), do: AttentionServer.BrowserOrigin.expected(conn, opts)

  defp new_request_id do
    <<a::binary-size(6), b::8, c::8, d::8, e::binary-size(7)>> = :crypto.strong_rand_bytes(16)

    bytes =
      <<a::binary, Bitwise.bor(Bitwise.band(b, 15), 64), c, Bitwise.bor(Bitwise.band(d, 63), 128),
        e::binary>>

    <<p1::binary-size(4), p2::binary-size(2), p3::binary-size(2), p4::binary-size(2),
      p5::binary-size(6)>> = bytes

    Enum.map_join([p1, p2, p3, p4, p5], "-", &Base.encode16(&1, case: :lower))
  end

  defp form(conn, opts, keys, limit \\ 512) do
    with true <- trusted_form_origin?(conn, expected_origin(conn, opts)),
         [content_type] <- get_req_header(conn, "content-type"),
         true <- form_content_type?(content_type) do
      case read_body(conn, length: limit, read_length: limit) do
        {:ok, body, conn} ->
          try do
            fields = URI.decode_query(body)

            accepted_keys =
              case keys do
                {:one_of, choices} -> choices
                keys -> [keys]
              end

            if Enum.any?(accepted_keys, &(Enum.sort(Map.keys(fields)) == Enum.sort(&1))),
              do: {:ok, fields, conn},
              else: {:error, :invalid_form, conn}
          rescue
            error ->
              Diagnostics.failure(:dashboard_http, :request_read, error, :rejected)
              {:error, :invalid_form, conn}
          end

        {:more, _, conn} ->
          {:error, :invalid_form, conn}

        {:error, reason} ->
          Diagnostics.failure(:dashboard_http, :request_read, reason, :rejected)
          {:error, :invalid_form, conn}

        other ->
          Diagnostics.failure(:dashboard_http, :request_read, other, :rejected)
          {:error, :invalid_form, conn}
      end
    else
      _ -> {:error, :invalid_form, conn}
    end
  rescue
    error ->
      Diagnostics.failure(:dashboard_http, :request_read, error, :rejected)
      {:error, :invalid_form, conn}
  end

  defp json_body(conn, opts, limit) do
    with true <- trusted_form_origin?(conn, expected_origin(conn, opts)),
         [content_type] <- get_req_header(conn, "content-type"),
         true <- json_content_type?(content_type) do
      case read_body(conn, length: limit, read_length: limit) do
        {:ok, body, conn} ->
          case JSON.decode_line(body <> "\n", limit + 1) do
            {:ok, value} when is_map(value) and not is_struct(value) -> {:ok, value, conn}
            _ -> {:error, conn}
          end

        {:more, _, conn} ->
          {:error, conn}

        {:error, _reason} ->
          {:error, conn}

        _ ->
          {:error, conn}
      end
    else
      _ -> {:error, conn}
    end
  rescue
    _ -> {:error, conn}
  end

  defp json_content_type?(value) when is_binary(value) do
    case String.split(value, ";", trim: true) |> Enum.map(&String.trim/1) do
      [type] ->
        String.downcase(type) == "application/json"

      [type, charset] ->
        String.downcase(type) == "application/json" and
          String.downcase(charset) == "charset=utf-8"

      _ ->
        false
    end
  end

  defp json_content_type?(_), do: false

  defp send_levels_json(conn, body, status) do
    conn
    |> put_resp_content_type("application/json", "utf-8")
    |> send_resp(status, JSON.encode!(body))
  end

  defp trusted_form_origin?(conn, expected_origin) do
    case get_req_header(conn, "origin") do
      [^expected_origin] ->
        true

      origin when origin in [["null"], []] ->
        # A no-referrer HTML form navigation can carry Origin: null. Fetch
        # metadata still binds that navigation to this exact origin; a
        # cross-site form cannot supply same-origin in a browser.
        get_req_header(conn, "sec-fetch-site") == ["same-origin"] and
          get_req_header(conn, "sec-fetch-mode") == ["navigate"]

      _ ->
        false
    end
  end

  defp form_content_type?(value) do
    case String.split(value, ";", trim: true) |> Enum.map(&String.trim/1) do
      [type] ->
        String.downcase(type) == "application/x-www-form-urlencoded"

      [type, charset] ->
        String.downcase(type) == "application/x-www-form-urlencoded" and
          String.downcase(charset) == "charset=utf-8"

      _ ->
        false
    end
  end

  defp sign_in_get(conn, %{hosted: true, hosted_addresses: %{sign_in: url}, backend: {module, _}} = opts)
       when is_binary(url) do
    conn = fetch_query_params(conn)

    with %{"return" => "app", "transaction" => transaction} <- conn.query_params,
         true <- DeviceProof.secret?(transaction),
         true <- Code.ensure_loaded?(module) and function_exported?(module, :local_methods, 2),
         {:ok, methods} <- backend_call(opts.backend, :local_methods, nil),
         true <- "email_code" in methods do
      uri = URI.parse(url)
      query = URI.decode_query(uri.query || "")
      query = Map.merge(query, %{"return" => "app", "transaction" => transaction})
      method = conn.query_params["method"]
      query = if method in methods, do: Map.put(query, "method", method), else: query

      location = URI.to_string(%{uri | query: URI.encode_query(query)})
      conn |> put_resp_header("location", location) |> send_resp(303, "")
    else
      _ -> sign_in_page(conn, opts)
    end
  end

  defp sign_in_get(conn, opts) do
    if local_code_available?(opts) do
      {:ok, methods} = backend_call(opts.backend, :local_methods, nil)
      conn = fetch_query_params(conn)
      signed_out? = conn.query_params["signed_out"] == "1"

      conn
      |> put_resp_content_type("text/html", "utf-8")
      |> send_resp(200, LocalSignInPage.html("passkey" in methods, signed_out?))
    else
      sign_in_page(conn, opts)
    end
  end

  # A hosted page with a sign-in address carries a hidden link back to it that
  # the script shows when the one-use link fails. Self-hosted output is unchanged.
  defp sign_in_page(conn, opts) do
    again =
      case opts do
        %{hosted: true, hosted_addresses: %{sign_in: url}} when is_binary(url) ->
          ~s(<p><a id="sign-in-again" href="#{Plug.HTML.html_escape(url)}" hidden>Sign in again</a></p>)

        _ ->
          ""
      end

    self_hosted_help = if opts.hosted, do: "", else: self_hosted_sign_in_help_copy()

    html =
      ~s(<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Sign in · Harklight</title>#{DashboardBrand.favicon()}</head><body><main>#{DashboardBrand.lockup(decorative: true)}<h1>Sign in to Harklight</h1><p id="status">Checking your one-use link…</p>#{again}#{self_hosted_help}</main><script src="/sign-in.js" defer></script></body></html>)

    conn |> put_resp_content_type("text/html", "utf-8") |> send_resp(200, html)
  end

  defp sign_in_script(conn, %{hosted: true}) do
    script =
      ~S|const status=document.getElementById("status");const again=document.getElementById("sign-in-again");const fail=message=>{status.textContent=message;if(again){again.hidden=false}};const token=location.hash.slice(1);history.replaceState(null,"",location.pathname);if(!/^hbs1_[0-9a-f]{64}$/.test(token)){fail("This sign-in link is invalid.")}else{fetch("/sign-in",{method:"POST",headers:{"content-type":"text/plain"},credentials:"same-origin",body:token}).then(r=>{if(r.status===204){location.replace("/inbox")}else{fail("This sign-in link is expired or already used.")}}).catch(()=>{fail("The server could not be reached.")})}|

    conn |> put_resp_content_type("text/javascript", "utf-8") |> send_resp(200, script)
  end

  defp sign_in_script(conn, _opts) do
    script =
      ~S|const status=document.getElementById("status");const token=location.hash.slice(1);const signedOut=new URLSearchParams(location.search).has("signed_out");history.replaceState(null,"",location.pathname+location.search);if(!/^hbs1_[0-9a-f]{64}$/.test(token)){status.textContent=signedOut&&token===""?"You are signed out.":"This sign-in link is invalid. Ask the owner for a new one."}else{fetch("/sign-in",{method:"POST",headers:{"content-type":"text/plain"},credentials:"same-origin",body:token}).then(r=>{if(r.status===204){location.replace("/inbox")}else{status.textContent="This sign-in link is expired or already used. Ask the owner for a new one."}}).catch(()=>{status.textContent="The server could not be reached. Ask the owner for a new link if this one was used."})}|

    conn |> put_resp_content_type("text/javascript", "utf-8") |> send_resp(200, script)
  end

  defp invitation_share_script do
    ~S"""
    const link = document.getElementById("tappable-link");
    const full = document.getElementById("join-link");
    const status = document.getElementById("share-status");
    const share = document.getElementById("share-invitation");
    const setup = document.getElementById("agent-setup-command");

    if (link && status && share) {
      if (!navigator.share || (navigator.canShare && !navigator.canShare({text: link.value}))) {
        share.hidden = true;
      }

      share.addEventListener("click", async () => {
        try {
          await navigator.share({text: link.value});
          status.textContent = "Share sheet finished.";
        } catch (error) {
          status.textContent = error && error.name === "AbortError"
            ? "Sharing cancelled."
            : "Sharing unavailable.";
        }
      });
    }

    const copyFields = [
      ["copy-invitation", link, "Tappable invitation copied.", "tappable link"],
      ["copy-full-invitation", full, "Full invitation copied.", "full link"],
      ["copy-agent-setup", setup, "Setup command copied.", "setup command"]
    ];

    for (const [id, field, copied, label] of copyFields) {
      const button = document.getElementById(id);
      if (!button || !field || !status) continue;

      button.addEventListener("click", async () => {
        try {
          await navigator.clipboard.writeText(field.value);
          status.textContent = copied;
        } catch (_error) {
          field.focus();
          field.select();
          status.textContent = `Automatic copy is unavailable. The ${label} is selected.`;
        }
      });
    }
    """
  end

  defp components_script do
    ~S"""
    const code = document.querySelector("#install-command code");
    const copy = document.getElementById("copy-install-command");
    const status = document.getElementById("install-copy-status");

    if (code && copy && status) {
      copy.addEventListener("click", async () => {
        const command = code.textContent.trim();

        try {
          await navigator.clipboard.writeText(command);
          status.textContent = "Command copied.";
        } catch (_error) {
          const range = document.createRange();
          range.selectNodeContents(code);
          const selection = window.getSelection();
          selection.removeAllRanges();
          selection.addRange(range);
          status.textContent = "Automatic copy is unavailable. The command is selected.";
        }
      });
    }
    """
  end

  defp join_page(conn, %{hosted: true}) do
    html =
      ~s|<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Join Harklight</title>#{DashboardBrand.favicon()}</head><body><main>#{DashboardBrand.lockup(decorative: true)}<h1>Join Harklight</h1><p id="join-status" role="status">Checking invitation…</p><noscript>This invitation needs JavaScript.</noscript><p id="join-details"></p><a id="open-harklight" hidden>Open in Harklight</a></main><script src="/join.js" defer></script></body></html>|

    conn |> put_resp_content_type("text/html", "utf-8") |> send_resp(200, html)
  end

  defp join_page(conn, _opts) do
    html =
      ~s|<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Join Harklight</title>#{DashboardBrand.favicon()}</head><body><main>#{DashboardBrand.lockup(decorative: true)}<h1>Join Harklight</h1><p id="join-status" role="status">Checking invitation…</p><noscript>This invitation needs JavaScript. Ask the account owner for the full invitation link.</noscript><p id="join-details"></p><a id="open-harklight" hidden>Open in Harklight</a><p>Opening Harklight shows the account and trust details before you choose Join. The account owner must separately approve your exact device.</p></main><script src="/join.js" defer></script></body></html>|

    conn |> put_resp_content_type("text/html", "utf-8") |> send_resp(200, html)
  end

  defp join_script(conn, %{hosted: true}) do
    script =
      ~S"""
      const status = document.getElementById("join-status");
      const details = document.getElementById("join-details");
      const open = document.getElementById("open-harklight");
      const token = location.hash.slice(1);
      history.replaceState(null, "", location.pathname);

      if (!/^[0-9a-f]{64}$/.test(token)) {
        status.textContent = "This invitation link is invalid.";
      } else {
        fetch("/join/resolve", {
          method: "POST",
          headers: {"content-type": "text/plain"},
          credentials: "omit",
          cache: "no-store",
          body: token
        }).then(async response => {
          if (response.status === 503) {
            status.textContent = "The server could not check this invitation.";
            return;
          }
          if (!response.ok) {
            const reason = response.status === 410 ? await response.text() : "";
            status.textContent = reason === "Invitation reissue required"
              ? "This invitation is from an older Harklight Server. Ask for a new invitation."
              : "This invitation is expired, used, or unavailable.";
            return;
          }
          const invitation = await response.json();
          if (typeof invitation.join_url !== "string" || !invitation.join_url.startsWith("harklight://join#")) {
            throw new Error("invalid invitation");
          }
          const account = typeof invitation.account_label === "string" ? invitation.account_label : "The account";
          const expiry = invitation.expires_at == null
            ? "No expiry"
            : "expires " + new Date(invitation.expires_at).toLocaleString();
          details.textContent = account + " on " + invitation.server_name + "; " + expiry + ".";
          open.href = invitation.join_url;
          open.hidden = false;
          status.textContent = "Invitation ready.";
        }).catch(() => {
          status.textContent = "The invitation could not be checked.";
        });
      }
      """

    conn |> put_resp_content_type("text/javascript", "utf-8") |> send_resp(200, script)
  end

  defp join_script(conn, _opts) do
    script =
      ~S"""
      const status = document.getElementById("join-status");
      const details = document.getElementById("join-details");
      const open = document.getElementById("open-harklight");
      const token = location.hash.slice(1);
      history.replaceState(null, "", location.pathname);

      if (!/^[0-9a-f]{64}$/.test(token)) {
        status.textContent = "This invitation link is invalid. Ask the account owner for a new one.";
      } else {
        fetch("/join/resolve", {
          method: "POST",
          headers: {"content-type": "text/plain"},
          credentials: "omit",
          cache: "no-store",
          body: token
        }).then(async response => {
          if (response.status === 503) {
            status.textContent = "The server could not check this invitation. Reopen the same link and try again.";
            return;
          }
          if (!response.ok) {
            const reason = response.status === 410 ? await response.text() : "";
            status.textContent = reason === "Invitation reissue required"
              ? "This invitation is from an older Harklight Server. Ask the account owner for a new invitation."
              : "This invitation is expired, used, or unavailable. Ask the account owner for a new one.";
            return;
          }
          const invitation = await response.json();
          if (typeof invitation.join_url !== "string" || !invitation.join_url.startsWith("harklight://join#")) {
            throw new Error("invalid invitation");
          }
          const expiry = invitation.expires_at == null
            ? "No expiry"
            : "expires " + new Date(invitation.expires_at).toLocaleString();
          details.textContent = "Account " + invitation.person_id + " on " + invitation.server_name + "; " + expiry + ".";
          open.href = invitation.join_url;
          open.hidden = false;
          status.textContent = "Invitation ready. Open it in Harklight, then review the account and trust details before joining.";
        }).catch(() => {
          status.textContent = "The invitation could not be checked. Reopen the original link or ask the owner for help.";
        });
      }
      """

    conn |> put_resp_content_type("text/javascript", "utf-8") |> send_resp(200, script)
  end

  defp resolve_join(conn, opts) do
    expected_origin = expected_origin(conn, opts)

    with [^expected_origin] <- get_req_header(conn, "origin"),
         ["text/plain"] <- get_req_header(conn, "content-type") do
      case read_body(conn, length: 128, read_length: 128) do
        {:ok, token, conn} ->
          if byte_size(token) == 64 and Regex.match?(~r/\A[0-9a-f]{64}\z/, token) do
            case backend_call(opts.backend, :resolve_invitation, token) do
              {:ok, invitation} ->
                conn
                |> put_resp_content_type("application/json")
                |> send_resp(200, JSON.encode!(invitation))

              {:error, reason} when reason in [:used, :expired] ->
                send_resp(conn, 410, "Invitation no longer available")

              {:error, :reissue_required} ->
                send_resp(conn, 410, "Invitation reissue required")

              {:error, :storage_unavailable} ->
                send_resp(conn, 503, "Invitation check unavailable; try the same link again")

              _ ->
                send_resp(conn, 404, "Invitation unavailable")
            end
          else
            send_resp(conn, 400, "Invalid invitation request")
          end

        {:more, _, conn} ->
          send_resp(conn, 400, "Invalid invitation request")

        _ ->
          send_resp(conn, 400, "Invalid invitation request")
      end
    else
      _ -> send_resp(conn, 400, "Invalid invitation request")
    end
  end

  defp local_code_available?(%{hosted: false} = opts), do: "owner_code" in sign_in_methods(opts)

  defp local_code_available?(_opts), do: false

  defp sign_in_methods(%{backend: {module, _} = backend}) do
    with true <- Code.ensure_loaded?(module) and function_exported?(module, :local_methods, 2),
         {:ok, methods} <- backend_call(backend, :local_methods, nil) do
      methods
    else
      _ -> []
    end
  end

  defp sign_in_methods(_opts), do: []

  defp local_passkey_request(conn, opts, operation) do
    if local_code_available?(opts) do
      with true <- get_req_header(conn, "origin") == [expected_origin(conn, opts)],
           [content_type] <- get_req_header(conn, "content-type"),
           true <- json_content_type?(content_type),
           {:ok, raw, conn} <- read_body(conn, length: 16_384, read_length: 16_384),
           {:ok, request} when is_map(request) <- JSON.decode_line(raw <> "\n") do
        payload =
          if operation in [:registration_options, :registration_finish],
            do: {Map.get(conn.req_cookies, @cookie), request},
            else: request

        case backend_call(opts.backend, :local_passkey, {operation, payload}) do
          {:ok, %{cookie: cookie, method: method}} ->
            conn |> BrowserAuth.put_cookie(cookie, true) |> send_levels_json(%{"method" => method}, 200)

          {:ok, %{transaction: reference}} ->
            send_levels_json(conn, %{"transaction" => reference}, 200)

          {:ok, result} ->
            send_levels_json(conn, result, 200)

          {:error, :invalid_passkey} ->
            send_levels_json(conn, %{"error" => "unauthorized"}, 401)

          _ ->
            send_levels_json(conn, %{"error" => "storage_unavailable"}, 503)
        end
      else
        _ -> send_levels_json(conn, %{"error" => "invalid_request"}, 400)
      end
    else
      send_resp(conn, 404, "")
    end
  end

  defp redeem_local_code(conn, opts) do
    if local_code_available?(opts) do
      with [content_type] <- get_req_header(conn, "content-type"),
           true <- json_content_type?(content_type),
           {:ok, raw, conn} <- read_body(conn, length: 1024, read_length: 1024),
           {:ok, %{"code" => code} = request} when is_binary(code) <- JSON.decode_line(raw <> "\n"),
           {:ok, transaction} <- local_code_context(conn, opts, request) do
        case backend_call(opts.backend, :redeem_owner_code, {code, transaction}) do
          {:ok, %{cookie: cookie, method: method}} ->
            conn |> BrowserAuth.put_cookie(cookie, true) |> send_levels_json(%{"method" => method}, 200)

          {:ok, %{transaction: reference}} ->
            send_levels_json(conn, %{"transaction" => reference}, 200)

          {:error, :invalid_sign_in} ->
            send_levels_json(conn, %{"error" => "unauthorized"}, 401)

          _ ->
            send_levels_json(conn, %{"error" => "storage_unavailable"}, 503)
        end
      else
        _ -> send_levels_json(conn, %{"error" => "invalid_request"}, 400)
      end
    else
      send_resp(conn, 404, "")
    end
  end

  defp local_code_context(conn, opts, %{"code" => _} = request) when map_size(request) == 1 do
    if get_req_header(conn, "origin") == [expected_origin(conn, opts)],
      do: {:ok, nil},
      else: {:error, :origin}
  end

  defp local_code_context(conn, opts, %{"transaction" => ref} = request)
       when is_binary(ref) and map_size(request) == 2 do
    case get_req_header(conn, "origin") do
      [] -> {:ok, ref}
      [origin] -> if origin == expected_origin(conn, opts), do: {:ok, ref}, else: {:error, :origin}
      _ -> {:error, :origin}
    end
  end

  defp local_code_context(_, _, _), do: {:error, :invalid_request}

  defp redeem_sign_in(conn, opts) do
    if get_req_header(conn, "origin") != [expected_origin(conn, opts)] or
         get_req_header(conn, "content-type") != ["text/plain"] do
      send_resp(conn, 403, "Sign-in refused")
    else
      case read_body(conn, length: 256, read_length: 256) do
        {:ok, token, conn} when byte_size(token) == 69 ->
          case backend_call(opts.backend, :redeem_sign_in, token) do
            {:ok, cookie, persistence} ->
              conn
              |> BrowserAuth.put_cookie(cookie, persistence == :persistent)
              |> send_resp(204, "")

            _ ->
              send_resp(conn, 401, "Sign-in link expired or used")
          end

        {:ok, _, conn} ->
          send_resp(conn, 400, "Invalid sign-in link")

        {:more, _, conn} ->
          send_resp(conn, 400, "Invalid sign-in link")

        _ ->
          send_resp(conn, 400, "Invalid sign-in link")
      end
    end
  end

  defp backend_call(backend, operation, value) do
    result =
      case backend do
        {module, context} -> apply(module, operation, [context, value])
        module -> apply(module, operation, [value])
      end

    Diagnostics.result(
      :dashboard_http,
      :request_dispatch,
      result,
      :automatic,
      backend_operation_id(operation, value)
    )
  end

  defp backend_operation_id(:create_invitation, {_principal, request_id}),
    do: if(Message.uuid_v4?(request_id), do: request_id)

  defp backend_operation_id(_operation, _value), do: nil
end
