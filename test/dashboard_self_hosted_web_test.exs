defmodule AttentionServer.DashboardSelfHostedWebTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{DashboardReadHTTP, JSON}

  @origin "https://server.example.test:9443"
  @web_commit "fad1f0e83c810605d83efe92d62182cc17382d2e"
  @web_archive "harklight-web-fad1f0e83c810605d83efe92d62182cc17382d2e.tar.gz"
  @web_archive_sha256 "f26cfe66ea18d1dbb704b2993eb0e070cd2d64e1220e8aed02ffb0367ce28e3f"
  @dashboard_csp "default-src 'none'; script-src 'self'; connect-src 'self'; style-src 'unsafe-inline'; img-src data:; base-uri 'none'; frame-ancestors 'none'"

  @web_client_csp "default-src 'none'; script-src 'self'; connect-src 'self'; style-src 'self' 'unsafe-inline'; font-src 'self'; img-src 'self' blob: data:; base-uri 'none'; frame-ancestors 'none'"

  defmodule Backend do
    def resolve_session(state, "active") do
      Agent.get(state, fn _ ->
        {:ok, %{person_id: "fixture-owner", display_name: "Local owner"}}
      end)
    end

    def resolve_session(_state, _cookie), do: {:error, :invalid_session}

    def end_session(state, cookie) do
      Agent.update(state, &Map.update!(&1, :ended, fn ended -> [cookie | ended] end))
      :ok
    end

    def list_devices(_state, _principal),
      do: {:ok, %{person_name: "Local owner", devices: [], waiting: []}}
  end

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "harklight-self-hosted-web-#{System.unique_integer([:positive])}"
      )

    {:ok, state} =
      Agent.start_link(fn ->
        %{ended: [], file_command: nil, uploaded: nil, upload_ended: 0, fetch_path: path}
      end)

    opts =
      DashboardReadHTTP.init(
        backend: {Backend, state},
        skill_path: __ENV__.file,
        expected_origin: @origin
      )

    on_exit(fn ->
      File.rm(path)
      if Process.alive?(state), do: Agent.stop(state)
    end)

    %{opts: opts, state: state}
  end

  test "the self-hosted bundle serves exact pinned archive files and MIME types", %{opts: opts} do
    priv = :code.priv_dir(:attention_server) |> List.to_string()
    archive_path = Path.join([priv, "harklight-web", @web_archive])
    archive = File.read!(archive_path)

    assert sha256(archive) == @web_archive_sha256

    # The package carries only the pinned bundle and its manifest.
    assert Enum.sort(File.ls!(Path.join(priv, "harklight-web"))) ==
             [@web_archive, "manifest.json"]

    manifest =
      archive_path
      |> Path.dirname()
      |> Path.join("manifest.json")
      |> File.read!()
      |> then(&JSON.decode_line(&1 <> "\n"))

    assert {:ok,
            %{
              "repository" => "clickety-clacks/harklight-web",
              "source_commit" => @web_commit,
              "archive" => @web_archive,
              "archive_sha256" => @web_archive_sha256
            }} = manifest

    assert {:ok, entries} = :erl_tar.extract({:binary, archive}, [:compressed, :memory])

    entries
    |> Enum.filter(fn {path, bytes} ->
      is_binary(bytes) and Path.extname(archive_path(path)) != ""
    end)
    |> Enum.each(fn {path, bytes} ->
      relative_path = archive_path(path) |> String.trim_leading("./")
      response = get("/app/" <> relative_path, "active", opts)

      assert response.status == 200
      assert response.resp_body == bytes
      assert Plug.Conn.get_resp_header(response, "content-security-policy") == [@web_client_csp]
      assert Plug.Conn.get_resp_header(response, "content-type") == [content_type(relative_path)]
      assert Plug.Conn.get_resp_header(response, "cache-control") == ["no-store"]
    end)
  end

  test "inbox, settings and app root serve the bundled client with the scoped CSP", %{opts: opts} do
    index = File.read!(priv_file("static/app/index.html"))

    for path <- ["/app/", "/app/index.html", "/inbox", "/settings"] do
      response = get(path, "active", opts)

      assert response.status == 200
      assert response.resp_body == index

      assert Plug.Conn.get_resp_header(response, "content-security-policy") == [
               @web_client_csp
             ]
    end

    devices = get("/devices", "active", opts)
    assert devices.status == 200
    assert Plug.Conn.get_resp_header(devices, "content-security-policy") == [@dashboard_csp]
  end

  test "the web client CSP allows only same-origin connections with no configured origin",
       %{state: state} do
    opts = DashboardReadHTTP.init(backend: {Backend, state}, skill_path: __ENV__.file)

    for path <- ["/app/", "/inbox", "/settings", "/app/config.json"] do
      response = get(path, "active", opts)
      assert response.status == 200

      assert Plug.Conn.get_resp_header(response, "content-security-policy") == [
               @web_client_csp
             ]
    end
  end

  test "self-hosted app configuration enables only shipped client features", %{opts: opts} do
    response = get("/app/config.json", "active", opts)

    assert response.status == 200

    assert Plug.Conn.get_resp_header(response, "content-type") == [
             "application/json; charset=utf-8"
           ]

    assert Plug.Conn.get_resp_header(response, "cache-control") == ["no-store"]

    assert Plug.Conn.get_resp_header(response, "content-security-policy") == [
             "default-src 'none'; script-src 'self'; connect-src 'self'; style-src 'self' 'unsafe-inline'; font-src 'self'; img-src 'self' blob: data:; base-uri 'none'; frame-ancestors 'none'"
           ]

    assert JSON.decode_line(response.resp_body <> "\n") ==
             {:ok,
              %{
                "accountUrl" => "",
                "sessionEndedPath" => "/sign-in?signed_out=1",
                "flags" => %{"webClient" => true, "attachments" => true, "answerForms" => true}
              }}
  end

  test "signed-out pages explain how to get a new link", %{opts: opts} do
    for path <- ["/inbox", "/settings", "/app/", "/devices"] do
      response = get(path, nil, opts)

      assert response.status == 401
      assert response.resp_body =~ "bin/harklight owner browser-sign-in"
      assert Plug.Conn.get_resp_header(response, "content-type") == ["text/html; charset=utf-8"]
    end

    sign_in = get("/sign-in", nil, opts)
    assert sign_in.status == 200
    assert sign_in.resp_body =~ "bin/harklight owner browser-sign-in"

    script = get("/sign-in.js", nil, opts)
    assert script.status == 200
    assert script.resp_body =~ ~s|location.replace("/inbox")|
    refute script.resp_body =~ ~s|location.replace("/devices")|

    # After a sign-out the landing says so; a bad or missing link without that
    # marker still reads as invalid.
    assert script.resp_body =~ ~s|new URLSearchParams(location.search).has("signed_out")|
    assert script.resp_body =~ ~s|signedOut&&token===""?"You are signed out."|
    assert script.resp_body =~ "This sign-in link is invalid. Ask the owner for a new one."
    # The link fragment is cleared but the marker is kept, so a reload still
    # shows the signed-out message.
    assert script.resp_body =~ ~s|history.replaceState(null,"",location.pathname+location.search)|

    landing = get("/sign-in?signed_out=1", nil, opts)
    assert landing.status == 200
    assert landing.resp_body =~ "bin/harklight owner browser-sign-in"
  end

  test "self-hosted sign-out validates the form, ends its session and returns to sign-in help", %{
    opts: opts,
    state: state
  } do
    response = sign_out("active", @origin, opts)

    assert response.status == 303
    assert Plug.Conn.get_resp_header(response, "location") == ["/sign-in?signed_out=1"]
    assert [cookie] = Plug.Conn.get_resp_header(response, "set-cookie")
    assert cookie =~ "__Host-harklight="
    assert cookie =~ "max-age=0"
    assert Agent.get(state, & &1.ended) == ["active"]

    no_session = sign_out(nil, @origin, opts)
    assert no_session.status == 303
    assert Plug.Conn.get_resp_header(no_session, "location") == ["/sign-in?signed_out=1"]
    assert Agent.get(state, & &1.ended) == ["active"]

    refused = sign_out("active", "https://elsewhere.example", opts)
    assert refused.status == 403
    assert Agent.get(state, & &1.ended) == ["active"]
  end

  test "self-hosted profile names the signed-in person", %{opts: opts} do
    profile = get("/api/profile", "active", opts)
    assert profile.status == 200
    assert {:ok, %{"display_name" => "Local owner"}} = JSON.decode_line(profile.resp_body <> "\n")
  end

  defp get(path, cookie, opts) do
    conn =
      Plug.Test.conn(:get, path)
      |> Map.put(:scheme, :https)
      |> Map.put(:port, 9443)

    conn =
      if cookie,
        do: Plug.Conn.put_req_header(conn, "cookie", "__Host-harklight=#{cookie}"),
        else: conn

    DashboardReadHTTP.call(conn, opts)
  end

  defp sign_out(cookie, origin, opts) do
    conn =
      Plug.Test.conn(:post, "/sign-out", "confirm=sign_out")
      |> Plug.Conn.put_req_header("origin", origin)
      |> Plug.Conn.put_req_header("content-type", "application/x-www-form-urlencoded")
      |> Map.put(:scheme, :https)
      |> Map.put(:port, 9443)

    conn =
      if cookie,
        do: Plug.Conn.put_req_header(conn, "cookie", "__Host-harklight=#{cookie}"),
        else: conn

    DashboardReadHTTP.call(conn, opts)
  end

  defp sha256(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

  defp archive_path(path) when is_list(path), do: List.to_string(path)
  defp archive_path(path) when is_binary(path), do: path

  defp content_type(path) do
    case Path.extname(path) do
      ".html" -> "text/html"
      ".js" -> "text/javascript"
      ".css" -> "text/css"
      ".woff2" -> "font/woff2"
      ".woff" -> "font/woff"
      ".map" -> "application/json"
      ".svg" -> "image/svg+xml"
      ".ico" -> "image/vnd.microsoft.icon"
      ".png" -> "image/png"
    end
  end

  defp priv_file(path) do
    :code.priv_dir(:attention_server)
    |> List.to_string()
    |> Path.join(path)
  end
end
