defmodule AttentionServer.ConversationNativeFixture do
  use ExUnit.Case, async: false

  alias AttentionServer.{ConversationFiles, JSON, Message, Relay, Server, TLSFixtures}

  @moduletag timeout: 700_000
  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @sender "app:interop"
  @message "native-interop"

  test "an enrolled native reader replays and commits exact conversation files over TLS" do
    root = System.fetch_env!("HARKLIGHT_INTEROP_DIR")
    commit = System.fetch_env!("HARKLIGHT_INTEROP_SERVER_COMMIT")
    mode = System.get_env("HARKLIGHT_INTEROP_MODE", "seed_only")
    assert Regex.match?(~r/\A[0-9a-f]{40}\z/, commit)
    assert mode in ["seed_only", "native"]

    fixture_id = ConversationFiles.new_upload_id()
    dir = Path.join(root, fixture_id)
    File.mkdir_p!(dir)
    ready = Path.join(root, "ready.json")
    marker = Path.join(root, "completed.json")
    File.rm(marker)
    certs = TLSFixtures.create!(server_san: "DNS:relay.test,DNS:localhost")

    on_exit(fn ->
      File.rm(ready)
      File.rm(marker)
      File.rm_rf!(dir)
      File.rm_rf!(certs)
    end)

    {:ok, server} =
      Server.start_link(
        store_path: Path.join(dir, "state.sqlite3"),
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem"),
        port: 0,
        origin_port: 0,
        relay_name: :"native_interop_#{System.unique_integer([:positive])}"
      )

    Process.unlink(server)
    on_exit(fn -> if Process.alive?(server), do: Supervisor.stop(server) end)
    relay = Server.relay(server)

    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))

    {:ok, origin} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, reader} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "b1"))

    :ok = Relay.setup_sender(relay, origin, @sender)

    incoming = :binary.copy(<<0, 1, 255, 42>>, 530_000)
    incoming_path = Path.join(dir, "seed-large.bin")
    File.write!(incoming_path, incoming)
    incoming_hash = sha256(incoming)

    {:ok, %{"upload_id" => incoming_id}} =
      Relay.upload_file(
        relay,
        origin,
        %{
          "op_id" => op(1),
          "sender_id" => @sender,
          "message_id" => @message,
          "revision" => 1,
          "name" => "seed-large.bin"
        },
        incoming_path,
        byte_size(incoming),
        incoming_hash
      )

    assert %{"type" => "ok", "seq" => 1} =
             request!(relay, origin, %{
               "type" => "forward",
               "op_id" => op(1),
               "session_handle" => @handle,
               "route" => "claude-code",
               "cli_version" => "interop-fixture",
               "unsupported_reason" => nil,
               "message" => %{
                 "sender_id" => @sender,
                 "message_id" => @message,
                 "created_at" => 1,
                 "title" => "Please review the attached file"
               },
               "files" => [
                 %{
                   "name" => "seed-large.bin",
                   "upload_id" => incoming_id,
                   "size" => byte_size(incoming),
                   "sha256" => incoming_hash
                 }
               ]
             })

    assert {:ok, [initial], 1} = Relay.events(relay, reader, 0)

    assert %{
             "type" => "delivery",
             "event_id" => initial_id,
             "listening_support" => %{"route" => "claude-code"} = support,
             "files" => [%{"upload_id" => ^incoming_id}]
           } = initial

    assert %{"type" => "ok", "event_id" => discussion_id} =
             request!(relay, reader, %{
               "type" => "discussion",
               "op_id" => op(2),
               "sender_id" => @sender,
               "message_id" => @message,
               "request_revision" => 1,
               "text" => "What should I check first?"
             })

    assert %{"type" => "ok", "event_id" => reply_id} =
             request!(relay, origin, %{
               "type" => "conversation_reply",
               "op_id" => op(3),
               "sender_id" => @sender,
               "message_id" => @message,
               "request_revision" => 1,
               "in_reply_to" => String.to_integer(discussion_id),
               "text" => "Check the exact bytes."
             })

    assert {:ok, conversations, high} = Relay.conversation_events(relay, reader, 0)
    assert Enum.any?(conversations, &(&1["event_id"] == reply_id))
    assert high >= 2
    assert {:ok, _upload, fetched_path} = Relay.fetch_file(relay, reader, incoming_id)
    assert File.read!(fetched_path) == incoming

    native_inline = :binary.copy(<<7>>, 200_000)
    native_upload = :binary.copy(<<0, 255, 13>>, 100_000)
    inline_path = Path.join(dir, "native-inline.bin")
    upload_path = Path.join(dir, "native-upload.bin")
    File.write!(inline_path, native_inline)
    File.write!(upload_path, native_upload)

    manifest = %{
      "fixture_version" => 1,
      "fixture_id" => fixture_id,
      "server_commit" => commit,
      "host" => "localhost",
      "relay_port" => Server.port(server),
      "origin_port" => Server.origin_port(server),
      "ca_path" => Path.join(certs, "ca.pem"),
      "reader_cert_path" => Path.join(certs, "b1.pem"),
      "reader_key_path" => Path.join(certs, "b1.key"),
      "person_id" => person,
      "reader_device_id" => reader,
      "sender_id" => @sender,
      "message_id" => @message,
      "session_handle" => @handle,
      "request_revision" => 1,
      "initial_event_id" => initial_id,
      "listening_support" => support,
      "seed_conversation_high" => high,
      "incoming_file" => %{
        "name" => "seed-large.bin",
        "upload_id" => incoming_id,
        "size" => byte_size(incoming),
        "sha256" => incoming_hash
      },
      "native_submission" => %{
        "text" => "Native mixed-file reply",
        "inline" => %{
          "path" => inline_path,
          "name" => "native-inline.bin",
          "size" => byte_size(native_inline),
          "sha256" => sha256(native_inline)
        },
        "upload" => %{
          "path" => upload_path,
          "name" => "native-upload.bin",
          "size" => byte_size(native_upload),
          "sha256" => sha256(native_upload)
        }
      }
    }

    write_json!(ready, manifest)

    if mode == "native" do
      completion = await_marker!(marker, fixture_id)
      assert completion["wrapper_exit_status"] == 0
      assert completion["status"] == "passed"
      assert {:ok, 1} = Relay.acknowledged_cursor(relay, reader)
      assert {:ok, _events, final_high} = Relay.conversation_events(relay, reader, 0)
      assert final_high > high
      assert {:ok, ^final_high} = Relay.conversation_cursor(relay, reader)

      assert {:ok, %{"entries" => entries}} = Relay.origin_feed(relay, origin, 0)

      assert [submitted] =
               Enum.filter(entries, fn entry ->
                 entry["kind"] == "discussion" and
                   entry["text"] == "Native mixed-file reply" and
                   Enum.map(Map.get(entry, "files", []), & &1["name"]) ==
                     ["native-inline.bin", "native-upload.bin"]
               end)

      assert Message.uuid_v4?(submitted["op_id"])
      refute submitted["op_id"] in [op(1), op(2), op(3)]

      assert %{
               "kind" => "discussion",
               "text" => "Native mixed-file reply",
               "files" => [inline_file, upload_file]
             } = submitted

      assert inline_file["name"] == "native-inline.bin"
      assert inline_file["data_base64"] == Base.encode64(native_inline)
      assert upload_file["name"] == "native-upload.bin"
      assert upload_file["size"] == byte_size(native_upload)
      assert upload_file["sha256"] == sha256(native_upload)
      assert {:ok, _upload, path} = Relay.fetch_file(relay, origin, upload_file["upload_id"])
      assert File.read!(path) == native_upload
    end
  end

  defp request!(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, reply} = JSON.decode_line(line)
    reply
  end

  defp write_json!(path, value) do
    temp = path <> ".tmp"
    File.write!(temp, JSON.encode!(value) <> "\n")
    File.rename!(temp, path)
  end

  defp await_marker!(path, fixture_id) do
    deadline = System.monotonic_time(:millisecond) + 600_000
    await_marker!(path, fixture_id, deadline)
  end

  defp await_marker!(path, fixture_id, deadline) do
    case File.read(path) do
      {:ok, bytes} ->
        assert {:ok, %{"fixture_id" => ^fixture_id} = marker} = JSON.decode_line(bytes)
        marker

      {:error, :enoent} ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("native wrapper did not complete before the fixture deadline")
        else
          Process.sleep(200)
          await_marker!(path, fixture_id, deadline)
        end
    end
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"
end
