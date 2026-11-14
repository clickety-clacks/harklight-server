defmodule AttentionServer.DashboardBrowserHTTPTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DashboardReadHTTP, JSON}

  @origin "https://browser.example.test"
  @recipient "00000000-0000-4000-8000-000000000001"

  defmodule Backend do
    def resolve_session(_state, "signed-in"),
      do: {:ok, %{person_id: "account-person", display_name: "Test account"}}

    def resolve_session(_state, "ends-after-auth"),
      do: {:ok, %{person_id: "account-person", display_name: "Test account"}}

    def resolve_session(_state, _), do: {:error, :invalid_session}

    def browser_file_command(state, {cookie, recipient, request}) do
      Agent.update(state, fn current ->
        Map.update!(current, :commands, &[{cookie, recipient, request} | &1])
      end)

      if cookie == "ends-after-auth" do
        {:error, :session_ended}
      else
        case request["type"] do
          "discard_uploads" ->
            {:ok, %{"v" => 1, "type" => "error", "reason" => "upload_discarded"}}

          _ ->
            {:ok,
             %{
               "v" => 1,
               "type" => "ok",
               "inline_decoded_bytes_per_entry" => 1_048_576,
               "upload_cap_bytes_per_file" => 8_388_608
             }}
        end
      end
    end

    def browser_file_limits(_state, {_cookie, _recipient}),
      do: {:ok, %{"upload_cap_bytes_per_file" => 100}}

    def browser_upload_begin(_state, {_cookie, _recipient, _attrs, _size, _worker}),
      do: {:ok, :lease}

    def browser_upload_file(state, {_cookie, recipient, attrs, path, size, hash, :lease}) do
      {:ok, bytes} = File.read(path)

      Agent.update(state, fn current ->
        Map.put(current, :upload, %{
          recipient: recipient,
          attrs: attrs,
          bytes: bytes,
          size: size,
          hash: hash
        })
      end)

      {:ok,
       %{
         "v" => 1,
         "type" => "ok",
         "upload_id" => "00000000-0000-4000-8000-000000000002",
         "name" => attrs["name"],
         "size" => size,
         "sha256" => hash,
         "duplicate" => false
       }}
    end

    def browser_upload_end(state, {_cookie, _recipient, :lease}),
      do: Agent.update(state, &Map.update!(&1, :upload_ended, fn count -> count + 1 end))

    def browser_fetch_file(state, {_cookie, upload_id}) do
      Agent.get(state, fn
        %{fetch_path: path} when upload_id == "00000000-0000-4000-8000-000000000005" ->
          bytes = File.read!(path)

          {:ok,
           %{
             size: byte_size(bytes),
             sha256: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower),
             name: "sample.html"
           }, path}

        _ ->
          {:error, :not_found}
      end)
    end
  end

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "harklight-browser-http-#{System.unique_integer([:positive])}"
      )

    File.write!(path, "<html>download only</html>")

    {:ok, state} =
      Agent.start_link(fn -> %{commands: [], upload: nil, upload_ended: 0, fetch_path: path} end)

    opts =
      DashboardReadHTTP.init(
        backend: {Backend, state},
        skill_path: __ENV__.file,
        hosted: true,
        expected_origin: @origin
      )

    on_exit(fn ->
      _ = File.rm(path)
      if Process.alive?(state), do: Agent.stop(state)
    end)

    %{state: state, path: path, opts: opts}
  end

  test "file command requires the exact Origin and session recipient", %{state: state, opts: opts} do
    body = JSON.encode!(%{"v" => 1, "type" => "file_limits"})

    refused =
      post("/browser/v1?recipient=" <> @recipient, body, "signed-in", "https://elsewhere.test")
      |> DashboardReadHTTP.call(opts)

    assert refused.status == 403
    assert Agent.get(state, & &1.commands) == []

    missing_origin =
      Plug.Test.conn(:post, "/browser/v1?recipient=" <> @recipient, body)
      |> Plug.Conn.put_req_header("cookie", "__Host-harklight=signed-in")
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> Map.put(:scheme, :https)
      |> DashboardReadHTTP.call(opts)

    assert missing_origin.status == 403
    assert Agent.get(state, & &1.commands) == []

    accepted =
      post("/browser/v1?recipient=" <> @recipient, body, "signed-in", @origin)
      |> DashboardReadHTTP.call(opts)

    assert accepted.status == 200

    assert {:ok, %{"type" => "ok", "upload_cap_bytes_per_file" => 8_388_608}} =
             JSON.decode_line(accepted.resp_body <> "\n")

    assert [{"signed-in", @recipient, %{"type" => "file_limits"}}] =
             Agent.get(state, & &1.commands)
  end

  test "file command replies preserve the device route status and error body", %{opts: opts} do
    body = JSON.encode!(%{"v" => 1, "type" => "discard_uploads"})

    response =
      post("/browser/v1?recipient=" <> @recipient, body, "signed-in", @origin)
      |> DashboardReadHTTP.call(opts)

    assert response.status == 200

    assert {:ok, %{"v" => 1, "type" => "error", "reason" => "upload_discarded"}} =
             JSON.decode_line(response.resp_body <> "\n")
  end

  test "file command route rejects relay commands and malformed JSON", %{
    state: state,
    opts: opts
  } do
    relay_command =
      post(
        "/browser/v1?recipient=" <> @recipient,
        JSON.encode!(%{"v" => 1, "type" => "answer"}),
        "signed-in",
        @origin
      )
      |> DashboardReadHTTP.call(opts)

    assert relay_command.status == 200

    assert {:ok, %{"type" => "error", "reason" => "invalid_field", "field" => "type"}} =
             JSON.decode_line(relay_command.resp_body <> "\n")

    malformed =
      post("/browser/v1?recipient=" <> @recipient, "{", "signed-in", @origin)
      |> DashboardReadHTTP.call(opts)

    assert malformed.status == 400
    assert Agent.get(state, & &1.commands) == []
  end

  test "a session ending after initial authentication follows the hosted sign-in route", %{
    opts: opts
  } do
    hosted =
      DashboardReadHTTP.init(
        backend: opts.backend,
        skill_path: opts.skill_path,
        hosted: true,
        expected_origin: @origin,
        hosted_addresses: %{sign_in: "https://account.example/sign-in"}
      )

    response =
      post(
        "/browser/v1?recipient=" <> @recipient,
        JSON.encode!(%{"v" => 1, "type" => "file_limits"}),
        "ends-after-auth",
        @origin
      )
      |> DashboardReadHTTP.call(hosted)

    assert response.status == 303

    assert Plug.Conn.get_resp_header(response, "location") == [
             "https://account.example/sign-in"
           ]
  end

  test "browser upload streams the device headers and bytes to the selected recipient", %{
    state: state,
    opts: opts
  } do
    op_id = "00000000-0000-4000-8000-000000000003"
    payload = "browser attachment bytes"

    response =
      Plug.Test.conn(:post, "/browser/v1/upload?recipient=" <> @recipient, payload)
      |> Plug.Conn.put_req_header("origin", @origin)
      |> Plug.Conn.put_req_header("cookie", "__Host-harklight=signed-in")
      |> Plug.Conn.put_req_header("content-type", "application/octet-stream")
      |> Plug.Conn.put_req_header("content-length", Integer.to_string(byte_size(payload)))
      |> Plug.Conn.put_req_header(
        "x-harklight-file-name",
        Base.url_encode64("report.txt", padding: false)
      )
      |> Plug.Conn.put_req_header("x-harklight-op-id", op_id)
      |> Plug.Conn.put_req_header("x-harklight-sender-id", "browser:sender-1")
      |> Plug.Conn.put_req_header("x-harklight-message-id", "message-1")
      |> Plug.Conn.put_req_header("x-harklight-request-revision", "2")
      |> Map.put(:scheme, :https)
      |> DashboardReadHTTP.call(opts)

    assert response.status == 200

    assert {:ok, %{"type" => "ok", "name" => "report.txt", "size" => size, "sha256" => hash}} =
             JSON.decode_line(response.resp_body <> "\n")

    assert size == byte_size(payload)
    assert hash == Base.encode16(:crypto.hash(:sha256, payload), case: :lower)

    assert %{recipient: @recipient, attrs: %{"op_id" => ^op_id, "revision" => 2}, bytes: ^payload} =
             Agent.get(state, & &1.upload)

    assert Agent.get(state, & &1.upload_ended) == 1
  end

  test "hosted upload rejects an oversized declared body with the server cap", %{opts: opts} do
    payload = String.duplicate("a", 101)

    response =
      Plug.Test.conn(:post, "/browser/v1/upload?recipient=" <> @recipient, payload)
      |> Plug.Conn.put_req_header("origin", @origin)
      |> Plug.Conn.put_req_header("cookie", "__Host-harklight=signed-in")
      |> Plug.Conn.put_req_header("content-type", "application/octet-stream")
      |> Plug.Conn.put_req_header("content-length", Integer.to_string(byte_size(payload)))
      |> Plug.Conn.put_req_header(
        "x-harklight-file-name",
        Base.url_encode64("report.txt", padding: false)
      )
      |> Plug.Conn.put_req_header("x-harklight-op-id", "00000000-0000-4000-8000-000000000004")
      |> Plug.Conn.put_req_header("x-harklight-sender-id", "browser:sender-2")
      |> Plug.Conn.put_req_header("x-harklight-message-id", "message-2")
      |> Plug.Conn.put_req_header("x-harklight-request-revision", "2")
      |> Map.put(:scheme, :https)
      |> DashboardReadHTTP.call(opts)

    assert response.status == 413

    assert {:ok,
            %{
              "type" => "error",
              "reason" => "upload_cap_exceeded",
              "upload_cap_bytes_per_file" => 100
            }} =
             JSON.decode_line(response.resp_body <> "\n")
  end

  test "fetch serves files as attachment bytes with nosniff and no recipient parameter", %{
    opts: opts
  } do
    response =
      Plug.Test.conn(:get, "/browser/v1/files/00000000-0000-4000-8000-000000000005")
      |> Plug.Conn.put_req_header("cookie", "__Host-harklight=signed-in")
      |> Map.put(:scheme, :https)
      |> DashboardReadHTTP.call(opts)

    assert response.status == 200
    assert response.resp_body == "<html>download only</html>"

    assert Plug.Conn.get_resp_header(response, "content-type") == [
             "application/octet-stream"
           ]

    assert Plug.Conn.get_resp_header(response, "content-length") == ["26"]

    assert Plug.Conn.get_resp_header(response, "content-disposition") == [
             "attachment; filename*=UTF-8''sample.html"
           ]

    assert Plug.Conn.get_resp_header(response, "x-content-type-options") == ["nosniff"]
    assert Plug.Conn.get_resp_header(response, "cache-control") == ["no-store"]
    assert Plug.Conn.get_resp_header(response, "x-harklight-sha256") != []
    assert Plug.Conn.get_resp_header(response, "transfer-encoding") == []
  end

  defp post(path, body, cookie, origin) do
    Plug.Test.conn(:post, path, body)
    |> Plug.Conn.put_req_header("origin", origin)
    |> Plug.Conn.put_req_header("cookie", "__Host-harklight=#{cookie}")
    |> Plug.Conn.put_req_header("content-type", "application/json")
    |> Map.put(:scheme, :https)
  end
end
