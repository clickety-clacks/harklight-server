defmodule AttentionServer.DashboardReadHTTP do
  @moduledoc """
  Authenticated browser routes for the self-hosted device dashboard.

  A backend resolves an opaque browser session and rechecks owner authority for
  each invitation, exact pending request, and decision. A TLS listener supplies
  the pinned host; this plug never accepts a browser claim as owner authority.

  Options: `:expected_origin` fixes the origin that form posts, join checks and
  sign-in must come from (default: the request's own host and port, so a server
  behind a public front end names its public address here); `:hosted` selects
  the hosted wording, which never tells a person to ask an owner;
  `:hosted_addresses` (hosted only) names where a browser goes with no session
  or an expired one (`:sign_in`), after Sign out (`:sign_out`, defaulting to
  `:sign_in`) and for the Account link (`:account`). Self-hosted output is
  unchanged by every default.
  """

  @behaviour Plug

  import Plug.Conn

  alias AttentionServer.{DashboardView, Diagnostics, JSON, Message}

  @cookie "__Host-harklight"
  @invite_cookie "__Host-harklight-invite"
  @invite_cookie_age 30 * 24 * 60 * 60

  @impl Plug
  def init(opts) do
    backend = Keyword.fetch!(opts, :backend)
    skill_path = Keyword.fetch!(opts, :skill_path)
    hosted = Keyword.get(opts, :hosted, false) == true

    skill_path_send_listen =
      Keyword.get(opts, :skill_path_send_listen) || send_listen_skill_path(skill_path)

    %{
      backend: backend,
      skill_path: skill_path,
      expected_origin: Keyword.get(opts, :expected_origin),
      hosted: hosted,
      hosted_addresses:
        if(hosted, do: hosted_addresses(Keyword.get(opts, :hosted_addresses)), else: %{}),
      skill_path_send_listen: skill_path_send_listen
    }
  end

  defp send_listen_skill_path(skill_path) do
    Path.join([
      Path.dirname(Path.dirname(skill_path)),
      "harklight-agent-send-listen",
      "SKILL.md"
    ])
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

  defp address(url) when is_binary(url) and url != "", do: url
  defp address(_), do: nil

  @impl Plug
  def call(conn, opts) do
    conn =
      conn
      |> put_resp_header("cache-control", "no-store")
      |> put_resp_header(
        "content-security-policy",
        "default-src 'none'; script-src 'self'; connect-src 'self'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'"
      )
      |> put_resp_header("referrer-policy", "no-referrer")
      |> put_resp_header("x-content-type-options", "nosniff")

    cond do
      conn.scheme != :https ->
        send_resp(conn, 403, "HTTPS required")

      conn.method == "GET" and conn.request_path == "/sign-in" ->
        sign_in_page(conn, opts)

      conn.method == "GET" and conn.request_path == "/sign-in.js" ->
        sign_in_script(conn, opts)

      conn.method == "POST" and conn.request_path == "/sign-in" ->
        redeem_sign_in(conn, opts)

      conn.method == "GET" and conn.request_path == "/join" ->
        join_page(conn, opts)

      conn.method == "GET" and conn.request_path == "/join.js" ->
        join_script(conn, opts)

      conn.method == "POST" and conn.request_path == "/join/resolve" ->
        resolve_join(conn, opts)

      conn.method == "GET" and conn.request_path == "/agent-skills/SKILL.md" ->
        serve_skill_file(conn, opts.skill_path)

      conn.method == "GET" and
          conn.request_path == "/agent-skills/harklight-agent-send-listen/SKILL.md" ->
        serve_skill_file(conn, opts.skill_path_send_listen)

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
          _ -> no_session(conn, opts)
        end
    end
  end

  # A hosted deployment sends a browser with no usable dashboard session to its
  # sign-in page, so the hosted session ends with the dashboard one. Self-hosted
  # keeps the plain refusal.
  defp no_session(conn, %{hosted: true, hosted_addresses: %{sign_in: url}}) when is_binary(url) do
    conn
    |> delete_resp_cookie(@cookie, secure: true, http_only: true, same_site: "Lax", path: "/")
    |> put_resp_header("location", url)
    |> send_resp(303, "")
  end

  defp no_session(conn, _opts), do: send_resp(conn, 401, "Sign in to view your devices")

  # Hosted-only page chrome: the Account link and the Sign out control.
  defp chrome(%{hosted: true, hosted_addresses: addresses}),
    do: [account_url: addresses.account, sign_out: true]

  defp chrome(_opts), do: [prioritization: true]

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

          {:error, :jev_unconfigured, settings} ->
            send_prioritization_page(
              conn,
              principal,
              settings,
              "Jev needs a complete endpoint, model and readable key file before it can be selected.",
              409,
              opts
            )

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

  # Hosted only: end this browser's dashboard session and send it to the hosted
  # sign-out address. Self-hosted has no such route and falls through to 404.
  defp route(
         %{method: "POST", request_path: "/sign-out"} = conn,
         %{hosted: true} = opts,
         _principal
       ) do
    case form(conn, opts, ~w(confirm)) do
      {:ok, %{"confirm" => "sign_out"}, conn} ->
        backend_call(opts.backend, :end_session, Map.get(conn.req_cookies, @cookie))

        conn
        |> delete_resp_cookie(@cookie, secure: true, http_only: true, same_site: "Lax", path: "/")
        |> put_resp_header("location", opts.hosted_addresses.sign_out || "/devices")
        |> send_resp(303, "")

      {_, _, conn} ->
        send_resp(conn, 403, "Sign out refused")
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
               {deadline, ""} <- Integer.parse(deadline),
               {:ok, %{key: ^key, expires_at: ^deadline} = request} <-
                 backend_call(opts.backend, :review_pending, {principal, code}),
               true <-
                 not Map.has_key?(fields, "senders") or
                   (action == "approve" and request[:can_grant_senders] == true),
               operation <- if(action == "approve", do: :approve_pending, else: :decline_pending),
               senders <-
                 if(request[:can_grant_senders] == true,
                   do: String.split(Map.get(fields, "senders", ""), ",", trim: true),
                   else: []
                 ),
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
        principal.person_id,
        skill_version(opts.skill_path),
        skill_version(opts.skill_path_send_listen),
        chrome(opts)
      )

    conn
    |> put_resp_content_type("text/html", "utf-8")
    |> send_resp(200, html)
  end

  defp route(conn, _opts, _principal), do: send_resp(conn, 404, "Not found")

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

  defp serve_skill_file(conn, skill_path) do
    case Diagnostics.result(
           :dashboard_http,
           :request_read,
           File.read(skill_path),
           :unavailable
         ) do
      {:ok, skill} ->
        conn
        |> put_resp_content_type("text/plain", "utf-8")
        |> send_resp(200, skill)

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

  # The origin browser requests must come from: the configured public address
  # when the server sits behind a front end, otherwise this listener's own.
  defp expected_origin(_conn, %{expected_origin: origin}) when is_binary(origin), do: origin

  defp expected_origin(conn, _opts) do
    if conn.port == 443,
      do: "https://" <> conn.host,
      else: "https://" <> conn.host <> ":" <> Integer.to_string(conn.port)
  end

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

    html =
      ~s(<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Sign in · Harklight</title></head><body><main><h1>Sign in to Harklight</h1><p id="status">Checking your one-use link…</p>#{again}</main><script src="/sign-in.js" defer></script></body></html>)

    conn |> put_resp_content_type("text/html", "utf-8") |> send_resp(200, html)
  end

  defp sign_in_script(conn, %{hosted: true}) do
    script =
      ~S|const status=document.getElementById("status");const again=document.getElementById("sign-in-again");const fail=message=>{status.textContent=message;if(again){again.hidden=false}};const token=location.hash.slice(1);history.replaceState(null,"",location.pathname);if(!/^hbs1_[0-9a-f]{64}$/.test(token)){fail("This sign-in link is invalid.")}else{fetch("/sign-in",{method:"POST",headers:{"content-type":"text/plain"},credentials:"same-origin",body:token}).then(r=>{if(r.status===204){location.replace("/devices")}else{fail("This sign-in link is expired or already used.")}}).catch(()=>{fail("The server could not be reached.")})}|

    conn |> put_resp_content_type("text/javascript", "utf-8") |> send_resp(200, script)
  end

  defp sign_in_script(conn, _opts) do
    script =
      ~S|const status=document.getElementById("status");const token=location.hash.slice(1);history.replaceState(null,"",location.pathname);if(!/^hbs1_[0-9a-f]{64}$/.test(token)){status.textContent="This sign-in link is invalid. Ask the owner for a new one."}else{fetch("/sign-in",{method:"POST",headers:{"content-type":"text/plain"},credentials:"same-origin",body:token}).then(r=>{if(r.status===204){location.replace("/devices")}else{status.textContent="This sign-in link is expired or already used. Ask the owner for a new one."}}).catch(()=>{status.textContent="The server could not be reached. Ask the owner for a new link if this one was used."})}|

    conn |> put_resp_content_type("text/javascript", "utf-8") |> send_resp(200, script)
  end

  defp invitation_share_script do
    ~S"""
    const link = document.getElementById("tappable-link");
    const full = document.getElementById("join-link");
    const status = document.getElementById("share-status");
    const share = document.getElementById("share-invitation");
    const copy = document.getElementById("copy-invitation");
    const copyFull = document.getElementById("copy-full-invitation");

    if (link && full && status && share && copy && copyFull) {
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

      copy.addEventListener("click", async () => {
        try {
          await navigator.clipboard.writeText(link.value);
          status.textContent = "Tappable invitation copied.";
        } catch (_error) {
          link.focus();
          link.select();
          status.textContent = "Automatic copy is unavailable. The tappable link is selected.";
        }
      });

      copyFull.addEventListener("click", async () => {
        try {
          await navigator.clipboard.writeText(full.value);
          status.textContent = "Full invitation copied.";
        } catch (_error) {
          full.focus();
          full.select();
          status.textContent = "Automatic copy is unavailable. The full link is selected.";
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
      ~S|<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Join Harklight</title></head><body><main><h1>Join Harklight</h1><p id="join-status" role="status">Checking invitation…</p><noscript>This invitation needs JavaScript.</noscript><p id="join-details"></p><a id="open-harklight" hidden>Open in Harklight</a></main><script src="/join.js" defer></script></body></html>|

    conn |> put_resp_content_type("text/html", "utf-8") |> send_resp(200, html)
  end

  defp join_page(conn, _opts) do
    html =
      ~S|<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Join Harklight</title></head><body><main><h1>Join Harklight</h1><p id="join-status" role="status">Checking invitation…</p><noscript>This invitation needs JavaScript. Ask the account owner for the full invitation link.</noscript><p id="join-details"></p><a id="open-harklight" hidden>Open in Harklight</a><p>Opening Harklight shows the account and trust details before you choose Join. The account owner must separately approve your exact device.</p></main><script src="/join.js" defer></script></body></html>|

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
            status.textContent = "This invitation is expired, used, or unavailable.";
            return;
          }
          const invitation = await response.json();
          if (typeof invitation.join_url !== "string" || !invitation.join_url.startsWith("harklight://join#")) {
            throw new Error("invalid invitation");
          }
          const account = typeof invitation.account_label === "string" ? invitation.account_label : "The account";
          details.textContent = `${account} on ${invitation.server_name}; expires ${new Date(invitation.expires_at).toLocaleString()}.`;
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
            status.textContent = "This invitation is expired, used, or unavailable. Ask the account owner for a new one.";
            return;
          }
          const invitation = await response.json();
          if (typeof invitation.join_url !== "string" || !invitation.join_url.startsWith("harklight://join#")) {
            throw new Error("invalid invitation");
          }
          details.textContent = `Account ${invitation.person_id} on ${invitation.server_name}; expires ${new Date(invitation.expires_at).toLocaleString()}.`;
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

  defp redeem_sign_in(conn, opts) do
    if get_req_header(conn, "origin") != [expected_origin(conn, opts)] or
         get_req_header(conn, "content-type") != ["text/plain"] do
      send_resp(conn, 403, "Sign-in refused")
    else
      case read_body(conn, length: 256, read_length: 256) do
        {:ok, token, conn} when byte_size(token) == 69 ->
          case backend_call(opts.backend, :redeem_sign_in, token) do
            {:ok, cookie, _expires_at} ->
              conn
              |> put_resp_cookie(@cookie, cookie,
                secure: true,
                http_only: true,
                same_site: "Lax",
                path: "/",
                max_age: 12 * 60 * 60
              )
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
