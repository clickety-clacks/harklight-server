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
  end

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "harklight-dashboard-skill-#{System.unique_integer([:positive])}.md"
      )

    File.write!(path, "---\nname: harklight-agent\nversion: \"0.1.0\"\n---\n\n# Operate Harklight\n")
    on_exit(fn -> File.rm(path) end)
    %{opts: DashboardReadHTTP.init(backend: Backend, skill_path: path)}
  end

  test "device records and the skill are unavailable before sign-in", %{opts: opts} do
    for path <- ["/devices", "/agent-skills", "/agent-skills/SKILL.md"] do
      response = path |> secure_get() |> DashboardReadHTTP.call(opts)
      assert response.status == 401
      refute response.resp_body =~ "Mike's iPad"
      assert Plug.Conn.get_resp_header(response, "cache-control") == ["no-store"]
    end
  end

  test "a resolved person sees only backend-authorized device records", %{opts: opts} do
    response = "/devices" |> secure_get("member-session") |> DashboardReadHTTP.call(opts)

    assert response.status == 200
    assert response.resp_body =~ "Mike&#39;s iPad"
    assert response.resp_body =~ "Enrolled</td><td>Unknown"
    refute response.resp_body =~ "Connected now"
  end

  test "the signed-in agent skills page serves the packaged skill without granting access", %{
    opts: opts
  } do
    page = "/agent-skills" |> secure_get("member-session") |> DashboardReadHTTP.call(opts)

    skill =
      "/agent-skills/SKILL.md" |> secure_get("member-session") |> DashboardReadHTTP.call(opts)

    assert page.status == 200
    assert page.resp_body =~ "Installing the skill alone grants no access"
    assert page.resp_body =~ "Packaged skill version: 0.1.0"
    assert skill.status == 200
    assert skill.resp_body == "---\nname: harklight-agent\nversion: \"0.1.0\"\n---\n\n# Operate Harklight\n"
  end

  test "the agent skills page tolerates a skill file with no version field", %{opts: opts} do
    path =
      Path.join(
        System.tmp_dir!(),
        "harklight-dashboard-skill-noversion-#{System.unique_integer([:positive])}.md"
      )

    File.write!(path, "# Operate Harklight\n")
    on_exit(fn -> File.rm(path) end)
    opts = %{opts | skill_path: path}

    page = "/agent-skills" |> secure_get("member-session") |> DashboardReadHTTP.call(opts)

    assert page.status == 200
    refute page.resp_body =~ "Packaged skill version"
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
