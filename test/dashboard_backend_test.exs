defmodule AttentionServer.DashboardBackendTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    DashboardBackend,
    DashboardBrowserSession,
    DashboardReadHTTP,
    GuidedJoin
  }

  alias AttentionServer.{DeviceIdentity, Relay, TLSFixtures}

  test "configured self-host owner sees device metadata across persons; another owner sees only their own" do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    {:ok, relay} = Relay.start_link()

    owner_a = TLSFixtures.certificate_der!(certs, "a1")
    owner_b = TLSFixtures.certificate_der!(certs, "b1")
    {:ok, person_a} = Relay.operator_bootstrap(relay, owner_a)
    {:ok, person_b} = Relay.operator_bootstrap(relay, owner_b)
    {:ok, key_a} = DeviceIdentity.from_certificate(owner_a)
    {:ok, key_b} = DeviceIdentity.from_certificate(owner_b)
    device_a = String.duplicate("c", 64)
    device_b = String.duplicate("d", 64)
    :ok = Relay.setup_device(relay, person_a, device_a)
    :ok = Relay.setup_device(relay, person_b, device_b)
    {:ok, revision_a} = Relay.owner_revision(relay, person_a, key_a)
    {:ok, revision_b} = Relay.owner_revision(relay, person_b, key_b)

    principals = %{
      "owner-a" => %{
        person_id: person_a,
        owner_key: key_a,
        owner_revision: revision_a,
        scope: :person_owner
      },
      "owner-b" => %{
        person_id: person_b,
        owner_key: key_b,
        owner_revision: revision_b,
        scope: :person_owner
      }
    }

    {:ok, sessions} =
      DashboardBrowserSession.start_link(relay: relay, consume: &Map.fetch(principals, &1))

    {:ok, cookie_a, _} = DashboardBrowserSession.redeem(sessions, "owner-a")
    {:ok, cookie_b, _} = DashboardBrowserSession.redeem(sessions, "owner-b")

    backend = %DashboardBackend{
      relay: relay,
      sessions: sessions,
      configured_owner_person: person_a
    }

    skill = Path.join(System.tmp_dir!(), "dashboard-skill-#{System.unique_integer([:positive])}")
    File.write!(skill, "# Harklight")
    on_exit(fn -> File.rm(skill) end)

    opts =
      DashboardReadHTTP.init(
        backend: {DashboardBackend, backend},
        skill_path: skill,
        skill_path_send_listen: skill
      )

    owner = request(cookie_a) |> DashboardReadHTTP.call(opts)
    other = request(cookie_b) |> DashboardReadHTTP.call(opts)
    assert owner.status == 200
    assert owner.resp_body =~ person_a
    assert owner.resp_body =~ person_b
    assert other.status == 200
    assert other.resp_body =~ person_b
    refute other.resp_body =~ person_a
    assert owner.resp_body =~ "Unknown"

    replacement = TLSFixtures.certificate_der!(certs, "a2")
    :ok = Relay.operator_recover(relay, person_a, replacement)
    assert (request(cookie_a) |> DashboardReadHTTP.call(opts)).status == 401
  end

  test "one-use Relay owner link becomes a secure browser cookie and cannot be replayed" do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    {:ok, relay} = Relay.start_link()
    owner_cert = TLSFixtures.certificate_der!(certs, "a1")
    {:ok, person} = Relay.operator_bootstrap(relay, owner_cert)
    {:ok, key} = DeviceIdentity.from_certificate(owner_cert)
    :ok = Relay.setup_device(relay, person, String.duplicate("c", 64))

    {:ok, sessions} =
      DashboardBrowserSession.start_link(
        relay: relay,
        consume: fn token -> Relay.consume_browser_sign_in(relay, token) end
      )

    backend = %DashboardBackend{relay: relay, sessions: sessions, configured_owner_person: person}
    skill = Path.join(System.tmp_dir!(), "dashboard-skill-#{System.unique_integer([:positive])}")
    File.write!(skill, "# Harklight")
    on_exit(fn -> File.rm(skill) end)

    opts =
      DashboardReadHTTP.init(
        backend: {DashboardBackend, backend},
        skill_path: skill,
        skill_path_send_listen: skill
      )

    {:ok, credential} = Relay.owner_browser_sign_in_create(relay, person, key)
    token = credential["sign_in_token"]

    assert (request_post(token, "https://other.example") |> DashboardReadHTTP.call(opts)).status ==
             403

    response = request_post(token, "https://www.example.com") |> DashboardReadHTTP.call(opts)
    assert response.status == 204
    [set_cookie] = Plug.Conn.get_resp_header(response, "set-cookie")
    assert set_cookie =~ "__Host-harklight="
    assert set_cookie =~ "secure"
    assert set_cookie =~ "HttpOnly"
    assert set_cookie =~ "SameSite=Lax"
    [_, cookie] = Regex.run(~r/__Host-harklight=([^;]+)/, set_cookie)
    assert (request(cookie) |> DashboardReadHTTP.call(opts)).status == 200

    assert (request_post(token, "https://www.example.com") |> DashboardReadHTTP.call(opts)).status ==
             401
  end

  test "signed-in owner approves invitation and command-line enrollments without sender rights" do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)

    {:ok, join} =
      GuidedJoin.configuration("relay.test", 18444, 18443, File.read!(Path.join(certs, "ca.pem")))

    {:ok, relay} = Relay.start_link(join_configuration: join)
    owner_cert = TLSFixtures.certificate_der!(certs, "a1")
    device_cert = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, person} = Relay.operator_bootstrap(relay, owner_cert)
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner_cert)
    {:ok, device_key} = DeviceIdentity.from_certificate(device_cert)

    {:ok, sessions} =
      DashboardBrowserSession.start_link(
        relay: relay,
        consume: fn token -> Relay.consume_browser_sign_in(relay, token) end
      )

    backend = %DashboardBackend{
      relay: relay,
      sessions: sessions,
      configured_owner_person: person,
      origin: "https://relay.test:18447"
    }

    opts =
      DashboardReadHTTP.init(
        backend: {DashboardBackend, backend},
        skill_path: "/unused",
        skill_path_send_listen: "/unused"
      )

    {:ok, credential} = Relay.owner_browser_sign_in_create(relay, person, owner_key)

    sign_in =
      request_post(credential["sign_in_token"], "https://www.example.com")
      |> DashboardReadHTTP.call(opts)

    [set_cookie] = Plug.Conn.get_resp_header(sign_in, "set-cookie")
    [_, cookie] = Regex.run(~r/__Host-harklight=([^;]+)/, set_cookie)

    devices = request(cookie) |> DashboardReadHTTP.call(opts)
    assert Plug.Conn.get_resp_header(devices, "set-cookie") == []
    [_, invite_request_id] = Regex.run(~r/name="request_id" value="([^"]+)"/, devices.resp_body)

    new_body =
      URI.encode_query(%{"confirm" => "new_invitation", "request_id" => invite_request_id})

    missing_request =
      owner_post("/devices/add", "confirm=invite", cookie)
      |> DashboardReadHTTP.call(opts)

    assert missing_request.status == 403
    assert missing_request.resp_body =~ "browser cookie is missing"
    assert map_size(:sys.get_state(relay).store.engine.join_invitations) == 0

    bad_origin =
      owner_post(
        "/devices/invitation/new",
        new_body,
        cookie,
        "https://other.example"
      )
      |> DashboardReadHTTP.call(opts)

    assert bad_origin.status == 403
    assert bad_origin.resp_body =~ "Invitation unavailable"
    assert bad_origin.resp_body =~ "This invitation form could not be verified."
    assert map_size(:sys.get_state(relay).store.engine.join_invitations) == 0

    old_add_page =
      %{request(cookie) | request_path: "/devices/add", path_info: ["devices", "add"]}
      |> DashboardReadHTTP.call(opts)

    assert old_add_page.status == 303
    assert Plug.Conn.get_resp_header(old_add_page, "location") == ["/devices"]

    invitation_page =
      owner_post(
        "/devices/invitation/new",
        new_body,
        cookie
      )
      |> DashboardReadHTTP.call(opts)

    assert invitation_page.status == 200
    assert [invite_cookie] = Plug.Conn.get_resp_header(invitation_page, "set-cookie")
    assert invite_cookie =~ "__Host-harklight-invite=#{invite_request_id}"
    [_, link] = Regex.run(~r/value="(harklight:\/\/join#[^"]+)"/, invitation_page.resp_body)
    "harklight://join#" <> encoded = link

    {:ok, payload} =
      encoded
      |> Base.url_decode64!(padding: false)
      |> Kernel.<>("\n")
      |> AttentionServer.JSON.decode_line()

    assert payload["person_id"] == person
    assert payload["server_name"] == "relay.test"
    assert invitation_page.resp_body =~ "https://relay.test:18447/join##{payload["invite_token"]}"

    secret = Base.encode16(:crypto.strong_rand_bytes(32), case: :lower)

    {:ok, pending} =
      Relay.invited_enroll(relay, payload["invite_token"], device_cert, "Ansible", secret)

    code = pending["approval_code"]
    deadline = pending["expires_at"]
    review = request(cookie) |> Map.put(:request_path, "/devices/waiting/#{code}")
    review = %{review | path_info: ["devices", "waiting", code]} |> DashboardReadHTTP.call(opts)
    assert review.status == 200
    assert review.resp_body =~ "Ansible"
    assert review.resp_body =~ device_key

    wrong =
      URI.encode_query(%{"key" => device_key, "deadline" => Integer.to_string(deadline + 1)})

    assert (owner_post("/devices/waiting/#{code}/approve", wrong, cookie)
            |> DashboardReadHTTP.call(opts)).status == 409

    assert :sys.get_state(relay).store.engine.devices[device_key] == nil

    exact = URI.encode_query(%{"key" => device_key, "deadline" => Integer.to_string(deadline)})

    approved =
      owner_post("/devices/waiting/#{code}/approve", exact, cookie, "null")
      |> Plug.Conn.put_req_header("sec-fetch-site", "same-origin")
      |> Plug.Conn.put_req_header("sec-fetch-mode", "navigate")
      |> DashboardReadHTTP.call(opts)

    assert approved.status == 303
    engine = :sys.get_state(relay).store.engine
    assert engine.devices[device_key].person == person
    assert Map.get(engine.sender_grants, {person, device_key}, MapSet.new()) == MapSet.new()

    cli_cert = TLSFixtures.certificate_der!(certs, "unknown")
    {:ok, cli_key} = DeviceIdentity.from_certificate(cli_cert)
    {:ok, cli_request} = Relay.pending_enroll(relay, cli_cert, "CLI workstation")
    cli_code = cli_request.approval_code
    cli_deadline = cli_request.expires_at

    cli_queue = request(cookie) |> DashboardReadHTTP.call(opts)
    assert cli_queue.status == 200
    assert cli_queue.resp_body =~ "CLI workstation · Command-line enrollment"
    assert cli_queue.resp_body =~ cli_code
    assert cli_queue.resp_body =~ "/devices/waiting/#{cli_code}"

    cli_review =
      request(cookie)
      |> Map.put(:request_path, "/devices/waiting/#{cli_code}")
      |> Map.put(:path_info, ["devices", "waiting", cli_code])
      |> DashboardReadHTTP.call(opts)

    assert cli_review.status == 200
    assert cli_review.resp_body =~ "CLI workstation"
    assert cli_review.resp_body =~ "Approve this exact device"

    cli_exact =
      URI.encode_query(%{
        "key" => cli_key,
        "deadline" => Integer.to_string(cli_deadline)
      })

    cli_approved =
      owner_post("/devices/waiting/#{cli_code}/approve", cli_exact, cookie, "null")
      |> Plug.Conn.put_req_header("sec-fetch-site", "same-origin")
      |> Plug.Conn.put_req_header("sec-fetch-mode", "navigate")
      |> DashboardReadHTTP.call(opts)

    assert cli_approved.status == 303

    after_approval = request(cookie) |> DashboardReadHTTP.call(opts)
    assert after_approval.resp_body =~ "CLI workstation</td>"
    refute after_approval.resp_body =~ cli_code

    engine = :sys.get_state(relay).store.engine
    assert engine.devices[cli_key].person == person
    assert Map.get(engine.sender_grants, {person, cli_key}, MapSet.new()) == MapSet.new()
  end

  defp request(cookie) do
    %{Plug.Test.conn(:get, "/devices") | scheme: :https}
    |> Plug.Conn.put_req_header("cookie", "__Host-harklight=#{cookie}")
  end

  defp request_post(token, origin) do
    %{Plug.Test.conn(:post, "/sign-in", token) | scheme: :https, port: 443}
    |> Plug.Conn.put_req_header("origin", origin)
    |> Plug.Conn.put_req_header("content-type", "text/plain")
  end

  defp owner_post(path, body, cookie, origin \\ "https://www.example.com", request_id \\ nil) do
    cookies =
      "__Host-harklight=#{cookie}" <>
        if(request_id, do: "; __Host-harklight-invite=#{request_id}", else: "")

    %{Plug.Test.conn(:post, path, body) | scheme: :https, port: 443}
    |> Plug.Conn.put_req_header("cookie", cookies)
    |> Plug.Conn.put_req_header("origin", origin)
    |> Plug.Conn.put_req_header("content-type", "application/x-www-form-urlencoded")
  end
end
