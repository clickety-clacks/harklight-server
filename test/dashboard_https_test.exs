defmodule AttentionServer.DashboardHTTPSTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    DashboardHTTPS,
    DeviceIdentity,
    GuidedJoin,
    HeadlessOwner,
    Relay,
    Server,
    TLSFixtures
  }

  test "supervised HTTPS serves an owner-issued one-use link and current-owner device view" do
    certs = TLSFixtures.create!()

    dir =
      Path.join(
        System.tmp_dir!(),
        "harklight-dashboard-tls-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)

    on_exit(fn ->
      File.rm_rf!(certs)
      File.rm_rf!(dir)
    end)

    skill_path = Path.join(dir, "skills/harklight-agent/SKILL.md")
    File.mkdir_p!(Path.dirname(skill_path))

    File.write!(
      skill_path,
      "---\nname: harklight-agent\nversion: \"0.1.0\"\n---\n\n# Harklight agent"
    )

    skill_path_saas =
      Path.join(dir, "skills/harklight-agent-saas/SKILL.md")

    File.mkdir_p!(Path.dirname(skill_path_saas))

    File.write!(
      skill_path_saas,
      "---\nname: harklight-agent-saas\nversion: \"0.2.0\"\n---\n\n# Harklight for hosted agents"
    )

    File.write!(Path.join(dir, "skills/core.md"), "# Shared core")

    port = available_port()
    store_path = Path.join(dir, "relay.sqlite3")

    {:ok, join} =
      GuidedJoin.configuration("relay.test", 18444, 18443, File.read!(Path.join(certs, "ca.pem")))

    {:ok, relay} =
      Relay.start_link(
        store_path: store_path,
        join_configuration: join,
        dashboard_origin: "https://relay.test:#{port}"
      )

    owner_cert = TLSFixtures.certificate_der!(certs, "a1")
    {:ok, person} = Relay.operator_bootstrap(relay, owner_cert)
    {:ok, key} = DeviceIdentity.from_certificate(owner_cert)
    :ok = Relay.setup_device(relay, person, String.duplicate("c", 64))

    {:ok, listener} =
      DashboardHTTPS.start_link(
        relay: relay,
        owner_person: person,
        port: port,
        ip: {127, 0, 0, 1},
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        skill_path: skill_path
      )

    Process.unlink(listener)
    on_exit(fn -> if Process.alive?(listener), do: Supervisor.stop(listener) end)

    {protocol, page} = send_https_with_alpn(certs, port)
    assert protocol == {:ok, <<"http/1.1">>}
    assert page =~ "HTTP/1.1 200"

    {:ok, credential} = Relay.owner_browser_sign_in_create(relay, person, key)
    token = credential["sign_in_token"]
    assert credential["sign_in_url"] == "https://relay.test:#{port}/sign-in##{token}"

    page =
      send_https(
        certs,
        port,
        "GET /sign-in HTTP/1.1\r\nHost: relay.test:#{port}\r\nConnection: close\r\n\r\n"
      )

    assert page =~ "HTTP/1.1 200"
    refute page =~ token

    body = token

    invalid_sign_in =
      send_https(certs, port, [
        "POST /sign-in HTTP/1.1\r\nHost: relay.test:#{port}\r\n",
        "Origin: https://relay.test:#{port}\r\nContent-Type: text/plain\r\n",
        "Content-Length: 5\r\nConnection: close\r\n\r\nshort"
      ])

    assert invalid_sign_in =~ "HTTP/1.1 400"

    exchange =
      send_https(certs, port, [
        "POST /sign-in HTTP/1.1\r\n",
        "Host: relay.test:#{port}\r\n",
        "Origin: https://relay.test:#{port}\r\n",
        "Content-Type: text/plain\r\n",
        "Content-Length: #{byte_size(body)}\r\n",
        "Connection: close\r\n\r\n",
        body
      ])

    assert exchange =~ "HTTP/1.1 204"
    [_, cookie] = Regex.run(~r/__Host-harklight=([^;\r\n]+)/, exchange)
    assert exchange =~ "secure"
    assert exchange =~ "HttpOnly"

    view =
      send_https(certs, port, [
        "GET /devices HTTP/1.1\r\n",
        "Host: relay.test:#{port}\r\n",
        "Cookie: __Host-harklight=#{cookie}\r\n",
        "Connection: close\r\n\r\n"
      ])

    assert view =~ "HTTP/1.1 200"
    assert view =~ "referrer-policy: no-referrer"
    assert view =~ person
    assert view =~ "Unknown"
    assert view =~ ~s(<form action="/devices/invitation/new" method="post">)
    refute view =~ ~s(<form action="/devices/add" method="post">)
    assert view =~ "Create a new invitation"
    refute view =~ "Set-Cookie: __Host-harklight-invite="

    components =
      send_https(certs, port, [
        "GET /components HTTP/1.1\r\n",
        "Host: relay.test:#{port}\r\n",
        "Cookie: __Host-harklight=#{cookie}\r\n",
        "Connection: close\r\n\r\n"
      ])

    assert components =~ "HTTP/1.1 200"
    assert components =~ "Agent skill: harklight-agent-saas"
    assert components =~ "v0.2.0"

    saas_skill =
      send_https(certs, port, [
        "GET /agent-skills/harklight-agent-saas/SKILL.md HTTP/1.1\r\n",
        "Host: relay.test:#{port}\r\n",
        "Connection: close\r\n\r\n"
      ])

    assert saas_skill =~ "HTTP/1.1 200"
    assert saas_skill =~ "# Harklight for hosted agents"

    core =
      send_https(certs, port, [
        "GET /agent-skills/core.md HTTP/1.1\r\n",
        "Host: relay.test:#{port}\r\n",
        "Connection: close\r\n\r\n"
      ])

    assert core =~ "HTTP/1.1 200"
    assert core =~ "# Shared core"
    [_, invite_request_id] = Regex.run(~r/name="request_id" value="([^"]+)"/, view)
    assert AttentionServer.Message.uuid_v4?(invite_request_id)

    invite_body = "confirm=invite"
    new_body = "confirm=new_invitation&request_id=#{invite_request_id}"

    malformed_form = "confirm=new_invitation"

    refused_form =
      send_https(certs, port, [
        "POST /devices/invitation/new HTTP/1.1\r\nHost: relay.test:#{port}\r\n",
        "Origin: https://relay.test:#{port}\r\n",
        "Content-Type: application/x-www-form-urlencoded\r\n",
        "Cookie: __Host-harklight=#{cookie}\r\n",
        "Content-Length: #{byte_size(malformed_form)}\r\nConnection: close\r\n\r\n",
        malformed_form
      ])

    assert refused_form =~ "HTTP/1.1 403"
    assert map_size(:sys.get_state(relay).store.engine.join_invitations) == 0

    # An absolute-form request target must not replace the pinned dashboard
    # authority and turn a matching attacker-supplied Origin into an invitation.
    poisoned_authority =
      send_https(certs, port, [
        "POST https://relay.test@evil.test:#{port}/devices/invitation/new HTTP/1.1\r\n",
        "Host: relay.test:#{port}\r\n",
        "Origin: https://relay.test@evil.test:#{port}\r\n",
        "Content-Type: application/x-www-form-urlencoded\r\n",
        "Cookie: __Host-harklight=#{cookie}\r\n",
        "Content-Length: #{byte_size(new_body)}\r\nConnection: close\r\n\r\n",
        new_body
      ])

    assert poisoned_authority =~ "400 Bad Request"
    assert map_size(:sys.get_state(relay).store.engine.join_invitations) == 0

    wrong_origin =
      send_https(certs, port, [
        "POST /devices/invitation/new HTTP/1.1\r\nHost: relay.test:#{port}\r\n",
        "Origin: https://elsewhere.test\r\n",
        "Sec-Fetch-Site: same-origin\r\nSec-Fetch-Mode: navigate\r\n",
        "Content-Type: application/x-www-form-urlencoded\r\n",
        "Cookie: __Host-harklight=#{cookie}\r\n",
        "Content-Length: #{byte_size(new_body)}\r\nConnection: close\r\n\r\n",
        new_body
      ])

    assert wrong_origin =~ "HTTP/1.1 403"
    assert wrong_origin =~ "Invitation unavailable"
    assert map_size(:sys.get_state(relay).store.engine.join_invitations) == 0

    null_cross_site =
      send_https(certs, port, [
        "POST /devices/invitation/new HTTP/1.1\r\nHost: relay.test:#{port}\r\n",
        "Origin: null\r\nSec-Fetch-Site: cross-site\r\nSec-Fetch-Mode: navigate\r\n",
        "Content-Type: application/x-www-form-urlencoded\r\n",
        "Cookie: __Host-harklight=#{cookie}\r\n",
        "Content-Length: #{byte_size(new_body)}\r\nConnection: close\r\n\r\n",
        new_body
      ])

    assert null_cross_site =~ "HTTP/1.1 403"
    assert map_size(:sys.get_state(relay).store.engine.join_invitations) == 0

    null_without_site_proof =
      send_https(certs, port, [
        "POST /devices/invitation/new HTTP/1.1\r\nHost: relay.test:#{port}\r\n",
        "Origin: null\r\nContent-Type: application/x-www-form-urlencoded\r\n",
        "Cookie: __Host-harklight=#{cookie}\r\n",
        "Content-Length: #{byte_size(new_body)}\r\nConnection: close\r\n\r\n",
        new_body
      ])

    assert null_without_site_proof =~ "HTTP/1.1 403"
    assert null_without_site_proof =~ "This invitation form could not be verified."
    assert map_size(:sys.get_state(relay).store.engine.join_invitations) == 0

    invitation =
      send_https(certs, port, [
        "POST /devices/invitation/new HTTP/1.1\r\nHost: relay.test:#{port}\r\n",
        "Origin: null\r\nSec-Fetch-Site: same-origin\r\nSec-Fetch-Mode: navigate\r\n",
        "Content-Type: application/x-www-form-urlencoded; charset=UTF-8\r\n",
        "Cookie: __Host-harklight=#{cookie}\r\n",
        "Content-Length: #{byte_size(new_body)}\r\nConnection: close\r\n\r\n",
        new_body
      ])

    assert invitation =~ "HTTP/1.1 200"
    assert invitation =~ "Invitation ready"
    assert invitation =~ "Destination: <strong>relay.test</strong>"
    assert invitation =~ "Account: <strong>#{person}</strong>"
    assert invitation =~ "Expires: <strong>"

    assert invitation =~
             ~s(<button class="button" id="share-invitation" type="button">Share invitation</button>)

    assert invitation =~
             ~s(<button class="button" id="copy-invitation" type="button">Copy tappable link</button>)

    assert invitation =~
             ~s(<button class="button" id="copy-full-invitation" type="button">Copy full link</button>)

    assert invitation =~ ~s(<script src="/invitation-share.js" defer></script>)
    assert invitation =~ "__Host-harklight-invite=#{invite_request_id}"
    assert invitation =~ "SameSite=Lax"
    assert invitation =~ ~s(<form action="/devices/invitation/new" method="post">)
    refute invitation =~ ~s(<form action="/devices/add" method="post">)
    assert [_, link] = Regex.run(~r/value="(harklight:\/\/join#[^"]+)"/, invitation)
    "harklight://join#" <> encoded = link

    {:ok, payload} =
      AttentionServer.JSON.decode_line(Base.url_decode64!(encoded, padding: false) <> "\n")

    assert payload["person_id"] == person
    assert payload["server_name"] == "relay.test"
    assert invitation =~ "https://relay.test:#{port}/join##{payload["invite_token"]}"
    assert byte_size("https://relay.test:#{port}/join##{payload["invite_token"]}") < 512
    assert invitation =~ payload["enrollment_url"]

    assert invitation =~
             DateTime.to_iso8601(DateTime.from_unix!(payload["expires_at"], :millisecond))

    assert map_size(:sys.get_state(relay).store.engine.join_invitations) == 1
    assert :sys.get_state(relay).store.engine.sender_grants == %{}

    recipient_page =
      send_https(
        certs,
        port,
        "GET /join HTTP/1.1\r\nHost: relay.test:#{port}\r\nConnection: close\r\n\r\n"
      )

    assert recipient_page =~ "HTTP/1.1 200"
    assert recipient_page =~ "Open in Harklight"
    refute recipient_page =~ payload["invite_token"]

    recipient_script =
      send_https(
        certs,
        port,
        "GET /join.js HTTP/1.1\r\nHost: relay.test:#{port}\r\nConnection: close\r\n\r\n"
      )

    assert recipient_script =~ "HTTP/1.1 200"
    assert recipient_script =~ "history.replaceState(null, \"\", location.pathname)"
    assert recipient_script =~ "open.href = invitation.join_url"
    refute recipient_script =~ payload["invite_token"]

    version = :sys.get_state(relay).store.version
    assert {200, resolved} = resolve_join(certs, port, payload["invite_token"])
    assert resolved == Map.put(payload, "join_url", link)
    assert :sys.get_state(relay).store.version == version
    assert map_size(:sys.get_state(relay).store.engine.join_invitations) == 1
    assert {400, _} = resolve_join(certs, port, "invalid")
    assert :sys.get_state(relay).store.version == version

    refused_origin =
      send_https(certs, port, [
        "POST /join/resolve HTTP/1.1\r\nHost: relay.test:#{port}\r\n",
        "Origin: https://elsewhere.test\r\nContent-Type: text/plain\r\n",
        "Content-Length: 64\r\nConnection: close\r\n\r\n",
        payload["invite_token"]
      ])

    assert refused_origin =~ "HTTP/1.1 400"
    assert :sys.get_state(relay).store.version == version

    share_script =
      send_https(certs, port, [
        "GET /invitation-share.js HTTP/1.1\r\nHost: relay.test:#{port}\r\n",
        "Cookie: __Host-harklight=#{cookie}\r\nConnection: close\r\n\r\n"
      ])

    assert share_script =~ "HTTP/1.1 200"
    refute share_script =~ payload["invite_token"]

    unsigned_script =
      send_https(
        certs,
        port,
        "GET /invitation-share.js HTTP/1.1\r\nHost: relay.test:#{port}\r\nConnection: close\r\n\r\n"
      )

    assert unsigned_script =~ "HTTP/1.1 401"

    components_script =
      send_https(certs, port, [
        "GET /components.js HTTP/1.1\r\nHost: relay.test:#{port}\r\n",
        "Cookie: __Host-harklight=#{cookie}\r\nConnection: close\r\n\r\n"
      ])

    assert components_script =~ "HTTP/1.1 200"
    assert components_script =~ "navigator.clipboard.writeText(command)"
    assert components_script =~ "Command copied."

    unsigned_components_script =
      send_https(
        certs,
        port,
        "GET /components.js HTTP/1.1\r\nHost: relay.test:#{port}\r\nConnection: close\r\n\r\n"
      )

    assert unsigned_components_script =~ "HTTP/1.1 401"

    # The Create response and its request cookie can both be lost. A later
    # Devices visit must not silently prepare another ordinary Create request.
    without_request_cookie =
      send_https(certs, port, [
        "GET /devices HTTP/1.1\r\nHost: relay.test:#{port}\r\n",
        "Cookie: __Host-harklight=#{cookie}\r\nConnection: close\r\n\r\n"
      ])

    assert without_request_cookie =~ "HTTP/1.1 200"
    assert without_request_cookie =~ "cannot recover an earlier invitation"
    assert without_request_cookie =~ ~s(<form action="/devices/invitation/new" method="post">)
    refute without_request_cookie =~ ~s(<form action="/devices/add" method="post">)
    refute without_request_cookie =~ "Set-Cookie: __Host-harklight-invite="

    refused_ordinary_create =
      send_https(certs, port, [
        "POST /devices/add HTTP/1.1\r\nHost: relay.test:#{port}\r\n",
        "Origin: https://relay.test:#{port}\r\n",
        "Content-Type: application/x-www-form-urlencoded\r\n",
        "Cookie: __Host-harklight=#{cookie}\r\n",
        "Content-Length: #{byte_size(invite_body)}\r\nConnection: close\r\n\r\n",
        invite_body
      ])

    assert refused_ordinary_create =~ "HTTP/1.1 403"

    assert refused_ordinary_create =~
             "The invitation request was not accepted or its browser cookie is missing."

    assert map_size(:sys.get_state(relay).store.engine.join_invitations) == 1

    # A double submit of the original visible form keeps its original ID even
    # when its cookie was not retained.
    recovered_form =
      send_https(certs, port, [
        "POST /devices/invitation/new HTTP/1.1\r\nHost: relay.test:#{port}\r\n",
        "Origin: null\r\nSec-Fetch-Site: same-origin\r\nSec-Fetch-Mode: navigate\r\n",
        "Content-Type: application/x-www-form-urlencoded\r\n",
        "Cookie: __Host-harklight=#{cookie}\r\n",
        "Content-Length: #{byte_size(new_body)}\r\nConnection: close\r\n\r\n",
        new_body
      ])

    assert recovered_form =~ "HTTP/1.1 200"
    assert recovered_form =~ ~s(value="#{link}")
    assert map_size(:sys.get_state(relay).store.engine.join_invitations) == 1

    # Repeating after an ignored or lost response must return the original
    # shareable link, with no second live invitation.
    recovered =
      send_https(certs, port, [
        "POST /devices/add HTTP/1.1\r\nHost: relay.test:#{port}\r\n",
        "Origin: null\r\nSec-Fetch-Site: same-origin\r\nSec-Fetch-Mode: navigate\r\n",
        "Content-Type: application/x-www-form-urlencoded\r\n",
        "Cookie: __Host-harklight=#{cookie}; __Host-harklight-invite=#{invite_request_id}\r\n",
        "Content-Length: #{byte_size(invite_body)}\r\nConnection: close\r\n\r\n",
        invite_body
      ])

    assert recovered =~ "HTTP/1.1 200"
    assert recovered =~ ~s(value="#{link}")
    assert map_size(:sys.get_state(relay).store.engine.join_invitations) == 1

    device_cert = TLSFixtures.certificate_der!(certs, "a2")
    secret = Base.encode16(:crypto.strong_rand_bytes(32), case: :lower)

    assert {:ok, pending_reply} =
             Relay.invited_enroll(
               relay,
               payload["invite_token"],
               device_cert,
               "Another device",
               secret
             )

    approval_code = pending_reply["approval_code"]
    assert {:ok, pending} = Relay.owner_pending_lookup(relay, person, key, approval_code)
    stale_approval = "key=#{pending.key}&deadline=#{pending.expires_at - 1}"

    refused_approval =
      send_https(certs, port, [
        "POST /devices/waiting/#{approval_code}/approve HTTP/1.1\r\n",
        "Host: relay.test:#{port}\r\nOrigin: https://relay.test:#{port}\r\n",
        "Content-Type: application/x-www-form-urlencoded\r\n",
        "Cookie: __Host-harklight=#{cookie}\r\n",
        "Content-Length: #{byte_size(stale_approval)}\r\nConnection: close\r\n\r\n",
        stale_approval
      ])

    assert refused_approval =~ "HTTP/1.1 409"
    assert {:ok, ^pending} = Relay.owner_pending_lookup(relay, person, key, approval_code)

    assert {410, _} = resolve_join(certs, port, payload["invite_token"])

    used =
      send_https(certs, port, [
        "POST /devices/add HTTP/1.1\r\nHost: relay.test:#{port}\r\n",
        "Origin: https://relay.test:#{port}\r\n",
        "Content-Type: application/x-www-form-urlencoded\r\n",
        "Cookie: __Host-harklight=#{cookie}; __Host-harklight-invite=#{invite_request_id}\r\n",
        "Content-Length: #{byte_size(invite_body)}\r\nConnection: close\r\n\r\n",
        invite_body
      ])

    assert used =~ "HTTP/1.1 409"
    assert used =~ "This invitation was used"
    assert used =~ ~s(<form action="/devices/invitation/new" method="post">)
    refute used =~ ~s(<form action="/devices/add" method="post">)
    refute used =~ link

    [_, next_id] = Regex.run(~r/name="request_id" value="([^"]+)"/, used)
    assert AttentionServer.Message.uuid_v4?(next_id)
    refute next_id == invite_request_id
    second_body = "confirm=new_invitation&request_id=#{next_id}"

    second =
      send_https(certs, port, [
        "POST /devices/invitation/new HTTP/1.1\r\nHost: relay.test:#{port}\r\n",
        "Origin: https://relay.test:#{port}\r\n",
        "Content-Type: application/x-www-form-urlencoded\r\n",
        "Cookie: __Host-harklight=#{cookie}; __Host-harklight-invite=#{invite_request_id}\r\n",
        "Content-Length: #{byte_size(second_body)}\r\nConnection: close\r\n\r\n",
        second_body
      ])

    assert second =~ "HTTP/1.1 200"
    assert second =~ "__Host-harklight-invite=#{next_id}"
    assert [_, next_link] = Regex.run(~r/value="(harklight:\/\/join#[^"]+)"/, second)
    refute next_link == link
    "harklight://join#" <> next_encoded = next_link

    {:ok, next_payload} =
      AttentionServer.JSON.decode_line(Base.url_decode64!(next_encoded, padding: false) <> "\n")

    assert map_size(:sys.get_state(relay).store.engine.join_invitations) == 2

    assert send_https(certs, port, [
             "POST /sign-in HTTP/1.1\r\nHost: relay.test:#{port}\r\n",
             "Origin: https://relay.test:#{port}\r\nContent-Type: text/plain\r\n",
             "Content-Length: #{byte_size(body)}\r\nConnection: close\r\n\r\n",
             body
           ]) =~ "HTTP/1.1 401"

    Supervisor.stop(listener)
    GenServer.stop(relay)
    restart_port = available_port()

    {:ok, restarted} =
      Relay.start_link(
        store_path: store_path,
        join_configuration: join,
        dashboard_origin: "https://relay.test:#{restart_port}"
      )

    assert map_size(:sys.get_state(restarted).store.engine.join_invitations) == 2

    {:ok, restarted_listener} =
      DashboardHTTPS.start_link(
        relay: restarted,
        owner_person: person,
        port: restart_port,
        ip: {127, 0, 0, 1},
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        skill_path: skill_path,
        skill_path_saas: skill_path
      )

    Process.unlink(restarted_listener)

    on_exit(fn ->
      if Process.alive?(restarted_listener), do: Supervisor.stop(restarted_listener)
    end)

    {:ok, new_credential} = Relay.owner_browser_sign_in_create(restarted, person, key)
    new_token = new_credential["sign_in_token"]

    new_session =
      send_https(certs, restart_port, [
        "POST /sign-in HTTP/1.1\r\nHost: relay.test:#{restart_port}\r\n",
        "Origin: https://relay.test:#{restart_port}\r\nContent-Type: text/plain\r\n",
        "Content-Length: #{byte_size(new_token)}\r\nConnection: close\r\n\r\n",
        new_token
      ])

    assert new_session =~ "HTTP/1.1 204"
    [_, new_cookie] = Regex.run(~r/__Host-harklight=([^;\r\n]+)/, new_session)

    resumed =
      send_https(certs, restart_port, [
        "POST /devices/add HTTP/1.1\r\nHost: relay.test:#{restart_port}\r\n",
        "Origin: https://relay.test:#{restart_port}\r\n",
        "Content-Type: application/x-www-form-urlencoded\r\n",
        "Cookie: __Host-harklight=#{new_cookie}; __Host-harklight-invite=#{next_id}\r\n",
        "Content-Length: #{byte_size(invite_body)}\r\nConnection: close\r\n\r\n",
        invite_body
      ])

    assert resumed =~ "HTTP/1.1 200"
    assert resumed =~ ~s(value="#{next_link}")

    assert {200, %{"join_url" => ^next_link}} =
             resolve_join(certs, restart_port, next_payload["invite_token"])

    assert map_size(:sys.get_state(restarted).store.engine.join_invitations) == 2
  end

  test "opt-in server exposes an owner-issued link at its pinned HTTPS dashboard" do
    certs = TLSFixtures.create!()

    dir =
      Path.join(
        System.tmp_dir!(),
        "harklight-dashboard-server-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)

    on_exit(fn ->
      File.rm_rf!(certs)
      File.rm_rf!(dir)
    end)

    skill = Path.join(dir, "SKILL.md")
    File.write!(skill, "# Harklight agent")
    dashboard_port = available_port()
    relay_name = {:global, {__MODULE__, make_ref()}}
    store_path = Path.join(dir, "relay.sqlite3")
    owner_cert = TLSFixtures.certificate_der!(certs, "a1")
    {:ok, initial_relay} = Relay.start_link(store_path: store_path)
    {:ok, person} = Relay.operator_bootstrap(initial_relay, owner_cert)
    GenServer.stop(initial_relay)

    {:ok, server} =
      Server.start_link(
        store_path: store_path,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem"),
        relay_name: relay_name,
        port: available_port(),
        enroll_port: available_port(),
        owner_port: 0,
        agent_port: 0,
        join_name: "relay.test",
        dashboard: [
          port: dashboard_port,
          ip: {127, 0, 0, 1},
          owner_person: person,
          certfile: Path.join(certs, "server.pem"),
          keyfile: Path.join(certs, "server.key"),
          skill_path: skill,
          skill_path_saas: skill
        ]
      )

    Process.unlink(server)
    on_exit(fn -> if Process.alive?(server), do: Supervisor.stop(server) end)
    relay = Server.relay(server)
    {:ok, key} = DeviceIdentity.from_certificate(owner_cert)
    assert {:ok, _} = Relay.owner_revision(relay, person, key)

    endpoint = %{
      address: {127, 0, 0, 1},
      name: "relay.test",
      ca_file: Path.join(certs, "ca.pem"),
      port: Server.owner_port(server)
    }

    assert {:ok, %{"browser_sign_in" => credential}} =
             HeadlessOwner.browser_sign_in(
               endpoint,
               Path.join(certs, "a1.pem"),
               Path.join(certs, "a1.key"),
               person
             )

    assert credential["sign_in_url"] ==
             "https://relay.test:#{dashboard_port}/sign-in##{credential["sign_in_token"]}"

    assert send_https(
             certs,
             dashboard_port,
             "GET /sign-in HTTP/1.1\r\nHost: relay.test:#{dashboard_port}\r\nConnection: close\r\n\r\n"
           ) =~
             "HTTP/1.1 200"

    output = Path.join(dir, "owner-sign-in-link.txt")

    AttentionServer.CLI.Owner.run([
      "browser-sign-in",
      "--connect",
      "127.0.0.1",
      "--name",
      "relay.test",
      "--port",
      Integer.to_string(Server.owner_port(server)),
      "--ca",
      Path.join(certs, "ca.pem"),
      "--cert",
      Path.join(certs, "a1.pem"),
      "--key",
      Path.join(certs, "a1.key"),
      "--person",
      person,
      "--output",
      output
    ])

    assert String.trim(File.read!(output)) =~
             ~r|\Ahttps://relay\.test:\d+/sign-in#hbs1_[0-9a-f]{64}\z|

    assert {:ok, %{mode: mode}} = File.stat(output)
    assert Bitwise.band(mode, 0o077) == 0
  end

  defp send_https(certs, port, request) do
    {:ok, socket} =
      :ssl.connect({127, 0, 0, 1}, port,
        versions: [:"tlsv1.3"],
        verify: :verify_peer,
        cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
        server_name_indication: ~c"relay.test",
        active: false,
        mode: :binary,
        packet: :raw
      )

    :ok = :ssl.send(socket, request)
    response = read_all(socket, <<>>)
    :ssl.close(socket)
    response
  end

  defp resolve_join(certs, port, token) do
    response =
      send_https(certs, port, [
        "POST /join/resolve HTTP/1.1\r\nHost: relay.test:#{port}\r\n",
        "Origin: https://relay.test:#{port}\r\nContent-Type: text/plain\r\n",
        "Content-Length: #{byte_size(token)}\r\nConnection: close\r\n\r\n",
        token
      ])

    [headers, body] = String.split(response, "\r\n\r\n", parts: 2)
    [_, status] = Regex.run(~r/HTTP\/1\.1 (\d+)/, headers)

    if status == "200" do
      {:ok, invitation} = AttentionServer.JSON.decode_line(body <> "\n")
      {200, invitation}
    else
      {String.to_integer(status), body}
    end
  end

  defp send_https_with_alpn(certs, port) do
    {:ok, socket} =
      :ssl.connect({127, 0, 0, 1}, port,
        versions: [:"tlsv1.3"],
        verify: :verify_peer,
        cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
        server_name_indication: ~c"relay.test",
        alpn_advertised_protocols: [<<"h2">>, <<"http/1.1">>],
        active: false,
        mode: :binary,
        packet: :raw
      )

    protocol = :ssl.negotiated_protocol(socket)

    :ok =
      :ssl.send(
        socket,
        "GET /sign-in HTTP/1.1\r\nHost: relay.test:#{port}\r\nConnection: close\r\n\r\n"
      )

    response = read_all(socket, <<>>)
    :ssl.close(socket)
    {protocol, response}
  end

  defp read_all(socket, acc) do
    case :ssl.recv(socket, 0, 5_000) do
      {:ok, bytes} -> read_all(socket, acc <> bytes)
      {:error, :closed} -> acc
    end
  end

  defp available_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_, port}} = :inet.sockname(socket)
    :gen_tcp.close(socket)
    port
  end
end
