defmodule AttentionServer.DashboardReadHTTPTest do
  use ExUnit.Case, async: true

  alias AttentionServer.DashboardReadHTTP

  defmodule Backend do
    def resolve_session("member-session"), do: {:ok, %{person_id: "mike"}}
    def resolve_session(_), do: {:error, :invalid_session}

    def list_devices(%{person_id: "mike"}) do
      {:ok,
       %{
         person_name: "Mike",
         devices: [%{label: "Mike's iPad", person_name: "Mike", revoked: false}],
         waiting: []
       }}
    end

    def create_invitation({%{person_id: "mike"}, request_id}) do
      if AttentionServer.Message.uuid_v4?(request_id), do: {:error, :unavailable}
    end

    def prioritization(%{person_id: "mike"}) do
      {:ok,
       %{
         person_id: "mike",
         algorithm_id: "jev-set",
         endpoint: "https://api.typesafe.ai/v1/systemone",
         model: "jev-latest",
         key_status: "not readable",
         basis: "fallback"
       }}
    end

    def save_prioritization({%{person_id: "mike"}, "baseline", _endpoint, "jev-latest"}),
      do: {:ok, %{}}

    def save_prioritization({%{person_id: "mike"}, "jev-set", "", ""}),
      do: {:ok, %{}}
  end

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "harklight-dashboard-skill-#{System.unique_integer([:positive])}.md"
      )

    File.write!(
      path,
      "---\nname: harklight-agent\nversion: \"0.1.0\"\n---\n\n# Operate Harklight\n"
    )

    on_exit(fn -> File.rm(path) end)

    path_send_listen =
      Path.join(
        System.tmp_dir!(),
        "harklight-dashboard-skill-send-listen-#{System.unique_integer([:positive])}.md"
      )

    File.write!(
      path_send_listen,
      "---\nname: harklight-agent-send-listen\nversion: \"0.2.0\"\n---\n\n# Send and listen\n"
    )

    on_exit(fn -> File.rm(path_send_listen) end)

    %{
      opts:
        DashboardReadHTTP.init(
          backend: Backend,
          skill_path: path,
          skill_path_send_listen: path_send_listen
        )
    }
  end

  test "device records are unavailable before sign-in", %{opts: opts} do
    for path <- ["/devices", "/components", "/agent-skills"] do
      response = path |> secure_get() |> DashboardReadHTTP.call(opts)
      assert response.status == 401
      refute response.resp_body =~ "Mike's iPad"
      assert Plug.Conn.get_resp_header(response, "cache-control") == ["no-store"]
    end
  end

  test "both skill files are served without a dashboard session", %{opts: opts} do
    skill = "/agent-skills/SKILL.md" |> secure_get() |> DashboardReadHTTP.call(opts)

    assert skill.status == 200

    assert skill.resp_body ==
             "---\nname: harklight-agent\nversion: \"0.1.0\"\n---\n\n# Operate Harklight\n"

    skill_send_listen =
      "/agent-skills/harklight-agent-send-listen/SKILL.md"
      |> secure_get()
      |> DashboardReadHTTP.call(opts)

    assert skill_send_listen.status == 200

    assert skill_send_listen.resp_body ==
             "---\nname: harklight-agent-send-listen\nversion: \"0.2.0\"\n---\n\n# Send and listen\n"
  end

  test "a resolved person sees only backend-authorized device records", %{opts: opts} do
    response = "/devices" |> secure_get("member-session") |> DashboardReadHTTP.call(opts)

    assert response.status == 200
    assert response.resp_body =~ "Mike&#39;s iPad"
    assert response.resp_body =~ "Enrolled</td><td>Unknown"
    refute response.resp_body =~ "Connected now"
  end

  test "the self-hosted prioritization page shows connection status without key material", %{
    opts: opts
  } do
    page = "/prioritization" |> secure_get("member-session") |> DashboardReadHTTP.call(opts)

    assert page.status == 200
    assert page.resp_body =~ "Prioritization"
    assert page.resp_body =~ "Most urgent first, then soonest deadline, then newest"
    assert page.resp_body =~ "Jev re-ranking"
    assert page.resp_body =~ "not readable"

    assert length(
             Regex.scan(
               ~r/Jev setup is incomplete; waiting items are in basic order\./,
               page.resp_body
             )
           ) == 1

    assert page.resp_body =~ ~s(name="endpoint")
    assert page.resp_body =~ ~s(name="model")
    refute page.resp_body =~ "ATTENTION_SERVER_JEV_KEY_PATH"
    refute page.resp_body =~ ~s(name="key")
    refute page.resp_body =~ ~s(name="key_path")
    refute page.resp_body =~ "/protected/key/location"
    refute page.resp_body =~ "Choose how waiting items are ordered on this server."
    refute page.resp_body =~ "The key is read from the server's configured key file."
    refute page.resp_body =~ ">Baseline</option>"
    assert page.resp_body =~ ~s(href="/prioritization")
  end

  test "SaaS dashboard does not expose the prioritization page", %{opts: opts} do
    hosted =
      DashboardReadHTTP.init(
        backend: Backend,
        skill_path: opts.skill_path,
        skill_path_send_listen: opts.skill_path_send_listen,
        hosted: true
      )

    response = "/prioritization" |> secure_get("member-session") |> DashboardReadHTTP.call(hosted)
    assert response.status == 404
  end

  test "the prioritization form accepts a full configured endpoint", %{opts: opts} do
    endpoint = "https://example.test/" <> String.duplicate("a", 1_020)

    body =
      URI.encode_query(%{
        "algorithm_id" => "baseline",
        "endpoint" => endpoint,
        "model" => "jev-latest"
      })

    conn = %{Plug.Test.conn(:post, "/prioritization", body) | scheme: :https, port: 443}

    response =
      conn
      |> Plug.Conn.put_req_header("cookie", "__Host-harklight=member-session")
      |> Plug.Conn.put_req_header("origin", "https://www.example.com")
      |> Plug.Conn.put_req_header("content-type", "application/x-www-form-urlencoded")
      |> DashboardReadHTTP.call(opts)

    assert response.status == 303
    assert Plug.Conn.get_resp_header(response, "location") == ["/prioritization"]
  end

  test "the prioritization form accepts a saved choice before connection setup", %{opts: opts} do
    body = URI.encode_query(%{"algorithm_id" => "jev-set", "endpoint" => "", "model" => ""})
    conn = %{Plug.Test.conn(:post, "/prioritization", body) | scheme: :https, port: 443}

    response =
      conn
      |> Plug.Conn.put_req_header("cookie", "__Host-harklight=member-session")
      |> Plug.Conn.put_req_header("origin", "https://www.example.com")
      |> Plug.Conn.put_req_header("content-type", "application/x-www-form-urlencoded")
      |> DashboardReadHTTP.call(opts)

    assert response.status == 303
    assert Plug.Conn.get_resp_header(response, "location") == ["/prioritization"]
  end

  test "the signed-in components page serves the packaged skill without granting access", %{
    opts: opts
  } do
    page = "/components" |> secure_get("member-session") |> DashboardReadHTTP.call(opts)

    skill =
      "/agent-skills/SKILL.md" |> secure_get("member-session") |> DashboardReadHTTP.call(opts)

    assert page.status == 200
    assert page.resp_body =~ "v0.1.0"
    assert skill.status == 200

    assert skill.resp_body ==
             "---\nname: harklight-agent\nversion: \"0.1.0\"\n---\n\n# Operate Harklight\n"
  end

  test "the components page tolerates a skill file with no version field", %{opts: opts} do
    path =
      Path.join(
        System.tmp_dir!(),
        "harklight-dashboard-skill-noversion-#{System.unique_integer([:positive])}.md"
      )

    File.write!(path, "# Operate Harklight\n")
    on_exit(fn -> File.rm(path) end)
    opts = %{opts | skill_path: path}

    page = "/components" |> secure_get("member-session") |> DashboardReadHTTP.call(opts)

    assert page.status == 200
    refute page.resp_body =~ "v0.1.0"
    assert page.resp_body =~ "v0.2.0"
  end

  test "the agent skills page redirects to the components page for a signed-in session", %{
    opts: opts
  } do
    response = "/agent-skills" |> secure_get("member-session") |> DashboardReadHTTP.call(opts)

    assert response.status == 303
    assert Plug.Conn.get_resp_header(response, "location") == ["/components"]
  end

  test "the components page requires sign-in and discloses no skill content", %{opts: opts} do
    response = "/components" |> secure_get("unknown") |> DashboardReadHTTP.call(opts)

    assert response.status == 401
    refute response.resp_body =~ "0.1.0"
  end

  test "an invitation failure is an actionable page and never invents a link", %{opts: opts} do
    devices = "/devices" |> secure_get("member-session") |> DashboardReadHTTP.call(opts)
    assert Plug.Conn.get_resp_header(devices, "set-cookie") == []
    [_, request_id] = Regex.run(~r/name="request_id" value="([^"]+)"/, devices.resp_body)

    body = URI.encode_query(%{"confirm" => "new_invitation", "request_id" => request_id})
    conn = %{Plug.Test.conn(:post, "/devices/invitation/new", body) | scheme: :https, port: 443}

    response =
      conn
      |> Plug.Conn.put_req_header(
        "cookie",
        "__Host-harklight=member-session"
      )
      |> Plug.Conn.put_req_header("origin", "https://www.example.com")
      |> Plug.Conn.put_req_header("content-type", "application/x-www-form-urlencoded")
      |> DashboardReadHTTP.call(opts)

    assert response.status == 503
    assert response.resp_body =~ "Invitation unavailable"
    assert response.resp_body =~ "server could not create an invitation"
    assert response.resp_body =~ "Try again"
    assert [invite_cookie] = Plug.Conn.get_resp_header(response, "set-cookie")
    assert invite_cookie =~ "__Host-harklight-invite=#{request_id}"
    refute response.resp_body =~ "harklight://join#"
  end

  test "plain HTTP and an invalid browser session disclose no device list", %{opts: opts} do
    plain = Plug.Test.conn(:get, "/devices")
    assert DashboardReadHTTP.call(plain, opts).status == 403

    invalid = "/devices" |> secure_get("unknown") |> DashboardReadHTTP.call(opts)
    assert invalid.status == 401
    refute invalid.resp_body =~ "Mike's iPad"
  end

  defp secure_get(path, session \\ nil) do
    conn = %{Plug.Test.conn(:get, path) | scheme: :https}

    if session do
      Plug.Conn.put_req_header(conn, "cookie", "__Host-harklight=#{session}")
    else
      conn
    end
  end
end
