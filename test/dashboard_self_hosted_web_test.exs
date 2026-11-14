defmodule AttentionServer.DashboardSelfHostedWebTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{DashboardBrowserWebSocket, DashboardReadHTTP, JSON}

  @origin "https://server.example.test:9443"
  @recipient "00000000-0000-4000-8000-000000000001"
  @upload_id "00000000-0000-4000-8000-000000000002"
  @web_commit "9278117f13fd87d74ff4c851bd2ed09b6086aff5"
  @web_archive "harklight-web-9278117f13fd87d74ff4c851bd2ed09b6086aff5.tar.gz"
  @web_archive_sha256 "1caad243e2f39c43ed82690a2d2d00237135e30527b234410bbaa6110493c8b7"
  @dashboard_csp "default-src 'none'; script-src 'self'; connect-src 'self'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'"

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

    def browser_file_command(state, {cookie, recipient, request}) do
      Agent.update(state, &Map.put(&1, :file_command, {cookie, recipient, request}))

      if request["type"] == "discard_uploads" do
        {:error, :session_ended}
      else
        {:ok,
         %{
           "v" => 1,
           "type" => "ok",
           "inline_decoded_bytes_per_entry" => 262_144,
           "upload_cap_bytes_per_file" => 1_000_000
         }}
      end
    end

    def browser_file_limits(_state, {_cookie, _recipient}),
      do: {:ok, %{"upload_cap_bytes_per_file" => 1_000_000}}

    def browser_upload_begin(_state, {_cookie, _recipient, _attrs, _size, _worker}),
      do: {:ok, :lease}

    def browser_upload_file(state, {_cookie, _recipient, attrs, path, size, sha256, :lease}) do
      bytes = File.read!(path)
      Agent.update(state, &Map.put(&1, :uploaded, {attrs, bytes, size, sha256}))

      {:ok,
       %{
         "upload_id" => "00000000-0000-4000-8000-000000000002",
         "name" => attrs["name"],
         "size" => size,
         "sha256" => sha256,
         "duplicate" => false
       }}
    end

    def browser_upload_end(state, {_cookie, _recipient, :lease}) do
      Agent.update(state, &Map.update!(&1, :upload_ended, fn count -> count + 1 end))
    end

    def browser_fetch_file(state, {_cookie, upload_id}) do
      if upload_id == "00000000-0000-4000-8000-000000000002" do
        Agent.get(state, fn %{uploaded: {_attrs, bytes, size, sha256}, fetch_path: path} ->
          File.write!(path, bytes)
          {:ok, %{size: size, sha256: sha256, name: "large.txt"}, path}
        end)
      else
        {:error, :not_found}
      end
    end
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
               "default-src 'none'; script-src 'self'; connect-src 'self' wss://server.example.test:9443; style-src 'self' 'unsafe-inline'; font-src 'self'; img-src 'self' blob: data:; base-uri 'none'; frame-ancestors 'none'"
             ]
    end

    devices = get("/devices", "active", opts)
    assert devices.status == 200
    assert Plug.Conn.get_resp_header(devices, "content-security-policy") == [@dashboard_csp]
  end

  test "self-hosted app configuration enables only shipped client features", %{opts: opts} do
    response = get("/app/config.json", "active", opts)

    assert response.status == 200

    assert Plug.Conn.get_resp_header(response, "content-type") == [
             "application/json; charset=utf-8"
           ]

    assert Plug.Conn.get_resp_header(response, "cache-control") == ["no-store"]

    assert Plug.Conn.get_resp_header(response, "content-security-policy") == [
             "default-src 'none'; script-src 'self'; connect-src 'self' wss://server.example.test:9443; style-src 'self' 'unsafe-inline'; font-src 'self'; img-src 'self' blob: data:; base-uri 'none'; frame-ancestors 'none'"
           ]

    assert {:ok,
            %{
              "accountUrl" => "",
              "sessionEndedPath" => "/sign-in",
              "flags" => %{"webClient" => true, "attachments" => true, "answerForms" => true}
            } = config} = JSON.decode_line(response.resp_body <> "\n")

    assert map_size(config) == 3
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
  end

  test "self-hosted sign-out validates the form, ends its session and returns to sign-in help", %{
    opts: opts,
    state: state
  } do
    response = sign_out("active", @origin, opts)

    assert response.status == 303
    assert Plug.Conn.get_resp_header(response, "location") == ["/sign-in"]
    assert [cookie] = Plug.Conn.get_resp_header(response, "set-cookie")
    assert cookie =~ "__Host-harklight="
    assert cookie =~ "max-age=0"
    assert Agent.get(state, & &1.ended) == ["active"]

    no_session = sign_out(nil, @origin, opts)
    assert no_session.status == 303
    assert Plug.Conn.get_resp_header(no_session, "location") == ["/sign-in"]
    assert Agent.get(state, & &1.ended) == ["active"]

    refused = sign_out("active", "https://elsewhere.example", opts)
    assert refused.status == 403
    assert Agent.get(state, & &1.ended) == ["active"]
  end

  test "self-hosted profile, relay upgrade, commands, uploads and file reads use the browser handlers",
       %{
         opts: opts,
         state: state
       } do
    profile = get("/api/profile", "active", opts)
    assert profile.status == 200
    assert {:ok, %{"display_name" => "Local owner"}} = JSON.decode_line(profile.resp_body <> "\n")

    command =
      Plug.Test.conn(
        :post,
        "/browser/v1?recipient=" <> @recipient,
        JSON.encode!(%{"v" => 1, "type" => "file_limits"})
      )
      |> Plug.Conn.put_req_header("origin", @origin)
      |> Plug.Conn.put_req_header("cookie", "__Host-harklight=active")
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> Map.put(:scheme, :https)
      |> Map.put(:port, 9443)
      |> DashboardReadHTTP.call(opts)

    assert command.status == 200

    assert {:ok, %{"type" => "ok", "upload_cap_bytes_per_file" => 1_000_000}} =
             JSON.decode_line(command.resp_body <> "\n")

    assert Agent.get(state, & &1.file_command) == {
             "active",
             @recipient,
             %{"v" => 1, "type" => "file_limits"}
           }

    ended_command =
      Plug.Test.conn(
        :post,
        "/browser/v1?recipient=" <> @recipient,
        JSON.encode!(%{"v" => 1, "type" => "discard_uploads"})
      )
      |> Plug.Conn.put_req_header("origin", @origin)
      |> Plug.Conn.put_req_header("cookie", "__Host-harklight=active")
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> Map.put(:scheme, :https)
      |> Map.put(:port, 9443)
      |> DashboardReadHTTP.call(opts)

    assert ended_command.status == 303
    assert Plug.Conn.get_resp_header(ended_command, "location") == ["/sign-in"]

    payload = String.duplicate("a", 262_145)

    upload =
      Plug.Test.conn(:post, "/browser/v1/upload?recipient=" <> @recipient, payload)
      |> Plug.Conn.put_req_header("origin", @origin)
      |> Plug.Conn.put_req_header("cookie", "__Host-harklight=active")
      |> Plug.Conn.put_req_header("content-type", "application/octet-stream")
      |> Plug.Conn.put_req_header("content-length", Integer.to_string(byte_size(payload)))
      |> Plug.Conn.put_req_header(
        "x-harklight-file-name",
        Base.url_encode64("large.txt", padding: false)
      )
      |> Plug.Conn.put_req_header("x-harklight-op-id", "00000000-0000-4000-8000-000000000003")
      |> Plug.Conn.put_req_header("x-harklight-sender-id", "browser:fixture")
      |> Plug.Conn.put_req_header("x-harklight-message-id", "message-fixture")
      |> Plug.Conn.put_req_header("x-harklight-request-revision", "2")
      |> Map.put(:scheme, :https)
      |> Map.put(:port, 9443)
      |> DashboardReadHTTP.call(opts)

    assert upload.status == 200

    assert {:ok,
            %{
              "v" => 1,
              "type" => "ok",
              "upload_id" => @upload_id,
              "name" => "large.txt",
              "size" => 262_145,
              "sha256" => hash,
              "duplicate" => false
            }} = JSON.decode_line(upload.resp_body <> "\n")

    assert hash == sha256(payload)
    assert Agent.get(state, & &1.upload_ended) == 1

    fetched = get("/browser/v1/files/" <> @upload_id, "active", opts)
    assert fetched.status == 200
    assert fetched.resp_body == payload

    for subprotocol <- [nil, "attention-relay/1"] do
      conn = Plug.Test.conn(:get, "/browser/relay?recipient=" <> @recipient)

      conn =
        conn
        |> Plug.Conn.put_req_header("origin", @origin)
        |> Plug.Conn.put_req_header("cookie", "__Host-harklight=active")
        |> Map.put(:scheme, :https)
        |> Map.put(:port, 9443)

      conn =
        if subprotocol,
          do: Plug.Conn.put_req_header(conn, "sec-websocket-protocol", subprotocol),
          else: conn

      upgraded = DashboardReadHTTP.call(conn, opts)
      assert upgraded.status == 101
      assert upgraded.state == :upgraded

      assert_receive {_ref, :upgrade,
                      {:websocket, {DashboardBrowserWebSocket, _handler_opts, _connection_opts}}}
    end
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
    end
  end

  defp priv_file(path) do
    :code.priv_dir(:attention_server)
    |> List.to_string()
    |> Path.join(path)
  end
end
