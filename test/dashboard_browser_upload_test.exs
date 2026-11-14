defmodule AttentionServer.DashboardBrowserUploadTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    DashboardBackend,
    DashboardBrowserSession,
    DashboardReadHTTP,
    DeviceIdentity,
    JSON,
    Relay,
    TLSFixtures
  }

  @origin "https://browser.example.test"
  @recipient "00000000-0000-4000-8000-000000000001"

  defp op(n),
    do: "00000000-0000-4000-8000-" <> String.pad_leading(Integer.to_string(n), 12, "0")

  setup do
    certs = TLSFixtures.create!()

    dir =
      Path.join(
        System.tmp_dir!(),
        "harklight-browser-upload-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    {:ok, relay} = Relay.start_link(store_path: Path.join(dir, "state.sqlite3"))
    owner_cert = TLSFixtures.certificate_der!(certs, "a1")
    {:ok, person} = Relay.operator_bootstrap(relay, owner_cert)
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner_cert)
    {:ok, owner_revision} = Relay.owner_revision(relay, person, owner_key)

    consume = fn
      "approved" ->
        {:ok,
         %{
           person_id: person,
           owner_key: owner_key,
           owner_revision: owner_revision,
           scope: :person_owner
         }}

      _ ->
        {:error, :invalid_link}
    end

    {:ok, sessions} = DashboardBrowserSession.start_link(relay: relay, consume: consume)
    {:ok, cookie, nil} = DashboardBrowserSession.redeem(sessions, "approved")

    backend = %DashboardBackend{relay: relay, sessions: sessions, configured_owner_person: person}

    opts =
      DashboardReadHTTP.init(
        backend: {DashboardBackend, backend},
        skill_path: __ENV__.file,
        hosted: true,
        expected_origin: @origin
      )

    on_exit(fn ->
      if Process.alive?(sessions), do: GenServer.stop(sessions)
      if Process.alive?(relay), do: GenServer.stop(relay)
      File.rm_rf!(dir)
      File.rm_rf!(certs)
    end)

    %{relay: relay, person: person, sessions: sessions, cookie: cookie, opts: opts}
  end

  test "an over-inline browser upload reply is accepted and its file can be attached", c do
    %{relay: relay, person: person, sessions: sessions, cookie: cookie, opts: opts} = c

    :ok = Relay.setup_device(relay, person, "origin")
    :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert {:ok, forward_line} =
             Relay.request_line(
               relay,
               "origin",
               JSON.encode!(%{
                 "v" => 1,
                 "type" => "forward",
                 "op_id" => op(1),
                 "session_handle" => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                 "message" => %{
                   "sender_id" => "app:test",
                   "message_id" => "over-inline-upload",
                   "created_at" => 1,
                   "title" => "Review this file"
                 }
               }) <> "\n"
             )

    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(forward_line)

    assert {:ok, connection} =
             DashboardBrowserSession.open_browser_connection(sessions, cookie, @recipient, self())

    payload = :binary.copy(<<0xA5>>, 262_145)
    op_id = op(2)
    name = "large-attachment.bin"

    response =
      Plug.Test.conn(:post, "/browser/v1/upload?recipient=" <> @recipient, payload)
      |> Plug.Conn.put_req_header("origin", @origin)
      |> Plug.Conn.put_req_header("cookie", "__Host-harklight=" <> cookie)
      |> Plug.Conn.put_req_header("content-type", "application/octet-stream")
      |> Plug.Conn.put_req_header("content-length", Integer.to_string(byte_size(payload)))
      |> Plug.Conn.put_req_header(
        "x-harklight-file-name",
        Base.url_encode64(name, padding: false)
      )
      |> Plug.Conn.put_req_header("x-harklight-op-id", op_id)
      |> Plug.Conn.put_req_header("x-harklight-sender-id", "app:test")
      |> Plug.Conn.put_req_header("x-harklight-message-id", "over-inline-upload")
      |> Plug.Conn.put_req_header("x-harklight-request-revision", "1")
      |> Map.put(:scheme, :https)
      |> DashboardReadHTTP.call(opts)

    assert response.status == 200, response.resp_body

    assert {:ok,
            %{
              "v" => 1,
              "type" => "ok",
              "upload_id" => upload_id,
              "name" => ^name,
              "size" => size,
              "sha256" => sha256,
              "duplicate" => false
            } = upload_reply} = JSON.decode_line(response.resp_body <> "\n")

    assert size == byte_size(payload)
    assert sha256 == Base.encode16(:crypto.hash(:sha256, payload), case: :lower)

    file_reference = Map.take(upload_reply, ["upload_id", "name", "size", "sha256"])

    discussion = %{
      "v" => 1,
      "type" => "discussion",
      "op_id" => op_id,
      "sender_id" => "app:test",
      "message_id" => "over-inline-upload",
      "request_revision" => 1,
      "text" => "Please see the attached file",
      "files" => [file_reference]
    }

    assert {:ok, [discussion_reply]} =
             DashboardBrowserSession.browser_session_request_line(
               sessions,
               cookie,
               connection.browser_session_id,
               connection.session_id,
               JSON.encode!(discussion) <> "\n"
             )

    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(discussion_reply)

    assert {:ok, %{size: ^size, sha256: ^sha256, name: ^name}, path} =
             DashboardBrowserSession.browser_fetch_file(sessions, cookie, upload_id)

    assert File.read!(path) == payload
  end
end
