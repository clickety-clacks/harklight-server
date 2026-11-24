defmodule AttentionServer.RevokedOriginGoneTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DeviceIdentity, JSON, Relay, RelayReaders, TLSFixtures}

  @handle_a "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @handle_b "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
  @unsupported_handle "cccccccc-cccc-4ccc-8ccc-cccccccccccc"

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir =
      Path.join(
        System.tmp_dir!(),
        "harklight-revoked-origin-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    previous_trap_exit = Process.flag(:trap_exit, true)

    {:ok, relay} = Relay.start_link(store_path: Path.join(dir, "state.sqlite3"))
    owner_certificate = TLSFixtures.certificate_der!(certs, "a1")
    {:ok, person} = Relay.operator_bootstrap(relay, owner_certificate)
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner_certificate)

    assert :ok = Relay.setup_device(relay, person, "origin")
    assert :ok = Relay.setup_device(relay, person, "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    on_exit(fn ->
      if Process.alive?(relay), do: GenServer.stop(relay)
      Process.flag(:trap_exit, previous_trap_exit)
      File.rm_rf!(dir)
    end)

    %{relay: relay, person: person, owner_key: owner_key}
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp request(relay, key, frame) do
    raw = JSON.encode!(Map.put(frame, "v", 1)) <> "\n"

    assert {:ok, line} = Relay.request_line(relay, key, raw)
    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp forward(relay, handle, message_id, operation, unsupported_reason \\ nil) do
    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(operation),
               "session_handle" => handle,
               "route" => "claude-code",
               "cli_version" => "test-only",
               "unsupported_reason" => unsupported_reason,
               "origin_listener" => false,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => message_id,
                 "created_at" => 1,
                 "title" => "Question"
               }
             })
  end

  test "owner revocation retains one gone availability per open origin session and chat", %{
    relay: relay,
    person: person,
    owner_key: owner_key
  } do
    forward(relay, @handle_a, "revoked-chat-a", 1)
    forward(relay, @handle_b, "revoked-chat-b", 2)
    forward(relay, @unsupported_handle, "unsupported-chat", 4, "Legacy listening disabled")

    assert {:ok, _before, cursor} = RelayReaders.conversation_events(relay, "reader", 0)
    assert :ok = Relay.owner_revoke_device(relay, person, owner_key, "origin")

    assert {:ok, changes, next_cursor} = RelayReaders.conversation_events(relay, "reader", cursor)
    gone = Enum.filter(changes, &(&1["kind"] == "availability"))

    assert Enum.sort(Enum.map(gone, & &1["session_handle"])) ==
             Enum.sort([@handle_a, @handle_b])

    assert Enum.all?(gone, &(&1["availability"] == "gone"))
    assert length(gone) == 2

    assert :ok = Relay.owner_revoke_device(relay, person, owner_key, "origin")

    assert {:ok, [], ^next_cursor} =
             RelayReaders.conversation_events(relay, "reader", next_cursor)

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(3),
               "sender_id" => "app:test",
               "message_id" => "revoked-chat-a",
               "request_revision" => 1,
               "text" => "Can we discuss this?"
             })

    assert {:ok, chat_events, _} = RelayReaders.conversation_events(relay, "reader", next_cursor)
    assert Enum.any?(chat_events, &(&1["kind"] == "discussion"))
    refute Enum.any?(chat_events, &(&1["kind"] == "availability"))
  end
end
