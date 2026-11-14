defmodule AttentionServer.BrowserRelayRecipientTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DeviceIdentity, JSON, OriginWire, Relay, TLSFixtures}

  @recipient_one "00000000-0000-4000-8000-000000000001"
  @recipient_two "00000000-0000-4000-8000-000000000002"
  @browser_session String.duplicate("s", 43)

  defp op(n),
    do: "00000000-0000-4000-8000-" <> String.pad_leading(Integer.to_string(n), 12, "0")

  setup do
    certs = TLSFixtures.create!()
    owner_cert = TLSFixtures.certificate_der!(certs, "a1")
    {:ok, relay} = Relay.start_link()
    {:ok, person} = Relay.operator_bootstrap(relay, owner_cert)
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner_cert)

    on_exit(fn ->
      if Process.alive?(relay), do: GenServer.stop(relay)
      File.rm_rf!(certs)
    end)

    %{relay: relay, person: person, owner_key: owner_key}
  end

  test "each tab has its own recipient and replacement closes only the old connection", c do
    assert {:ok, first} =
             Relay.open_browser_recipient(
               c.relay,
               c.person,
               c.owner_key,
               1,
               @browser_session,
               @recipient_one,
               self()
             )

    assert {:ok, other_tab} =
             Relay.open_browser_recipient(
               c.relay,
               c.person,
               c.owner_key,
               1,
               @browser_session,
               @recipient_two,
               self()
             )

    assert {:ok, replacement} =
             Relay.open_browser_recipient(
               c.relay,
               c.person,
               c.owner_key,
               1,
               @browser_session,
               @recipient_one,
               self()
             )

    assert_receive {:browser_connection_replaced, ^first}
    state = :sys.get_state(c.relay)
    refute Map.has_key?(state.sessions, first)
    assert Map.has_key?(state.sessions, other_tab)
    assert Map.has_key?(state.sessions, replacement)
    refute Map.has_key?(state.store.engine.devices, state.sessions[replacement].key)
    assert map_size(state.focus_clients) == 0
    assert map_size(state.browser_recipients) == 2
  end

  test "browser frame allowlist serves view_get and refuses device-only frame types", c do
    assert {:ok, session} =
             Relay.open_browser_recipient(
               c.relay,
               c.person,
               c.owner_key,
               1,
               @browser_session,
               @recipient_one,
               self()
             )

    assert {:ok, [view_line]} =
             Relay.session_request_line(
               c.relay,
               session,
               JSON.encode!(%{"v" => 1, "type" => "view_get", "ref" => "browser-view"}) <>
                 "\n"
             )

    assert {:ok, %{"type" => "ok", "ref" => "browser-view", "list_items" => items}} =
             JSON.decode_line(view_line)

    assert is_list(items)

    assert {:ok, [push_line]} =
             Relay.session_request_line(
               c.relay,
               session,
               JSON.encode!(%{"v" => 1, "type" => "push_register"}) <> "\n"
             )

    assert {:ok, %{"type" => "error", "reason" => "unknown_type"}} =
             JSON.decode_line(push_line)

    assert map_size(:sys.get_state(c.relay).store.engine.devices) == 0
  end

  test "browser file commands reuse the recipient's file scope", c do
    assert {:ok, _session} =
             Relay.open_browser_recipient(
               c.relay,
               c.person,
               c.owner_key,
               1,
               @browser_session,
               @recipient_one,
               self()
             )

    assert {:ok,
            %{
              "type" => "ok",
              "ref" => "limits",
              "inline_decoded_bytes_per_entry" => inline_limit,
              "upload_cap_bytes_per_file" => "none"
            }} =
             OriginWire.browser_command(c.relay, @browser_session, @recipient_one, %{
               "v" => 1,
               "type" => "file_limits",
               "ref" => "limits"
             })

    assert inline_limit > 0

    assert {:ok,
            %{
              "type" => "ok",
              "ref" => "discard",
              "state" => "discarded",
              "discarded" => 0
            }} =
             OriginWire.browser_command(c.relay, @browser_session, @recipient_one, %{
               "v" => 1,
               "type" => "discard_uploads",
               "ref" => "discard",
               "op_id" => op(90),
               "sender_id" => "app:test",
               "message_id" => "browser-file",
               "revision" => 1
             })

    assert {:error, :forbidden} =
             OriginWire.browser_command(c.relay, @browser_session, @recipient_two, %{
               "v" => 1,
               "type" => "file_limits"
             })
  end

  test "browser actors can open and answer a message with browser identity", c do
    assert :ok = Relay.setup_device(c.relay, c.person, "origin")
    assert :ok = Relay.setup_sender(c.relay, "origin", "app:test")

    assert {:ok, forward_line} =
             Relay.request_line(
               c.relay,
               "origin",
               JSON.encode!(%{
                 "v" => 1,
                 "type" => "forward",
                 "op_id" => op(1),
                 "session_handle" => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                 "message" => %{
                   "sender_id" => "app:test",
                   "message_id" => "browser-action",
                   "created_at" => 1,
                   "title" => "A browser action"
                 }
               }) <> "\n"
             )

    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(forward_line)

    assert {:ok, revision} = Relay.owner_revision(c.relay, c.person, c.owner_key)

    assert {:ok, session_id} =
             Relay.open_browser_recipient(
               c.relay,
               c.person,
               c.owner_key,
               revision,
               @browser_session,
               @recipient_one,
               self()
             )

    assert {:ok, [opened_line]} =
             Relay.session_request_line(
               c.relay,
               session_id,
               JSON.encode!(%{
                 "v" => 1,
                 "type" => "message_open",
                 "op_id" => op(2),
                 "sender_id" => "app:test",
                 "message_id" => "browser-action",
                 "request_revision" => 1
               }) <> "\n"
             )

    assert {:ok, %{"type" => "ok", "opened" => true}} = JSON.decode_line(opened_line)

    assert {:ok, [answered_line]} =
             Relay.session_request_line(
               c.relay,
               session_id,
               JSON.encode!(%{
                 "type" => "answer_no_comment",
                 "request_id" => "browser-action",
                 "request_revision" => 1
               }) <> "\n"
             )

    assert {:ok, %{"type" => "ok", "terminal_kind" => "no_comment"}} =
             JSON.decode_line(answered_line)

    terminal =
      :sys.get_state(c.relay).store.engine.messages[{c.person, "app:test", "browser-action"}].terminal

    assert terminal.actor_kind == :browser
  end

  test "ending a server session closes its recipients and removes their state", c do
    assert {:ok, session} =
             Relay.open_browser_recipient(
               c.relay,
               c.person,
               c.owner_key,
               1,
               @browser_session,
               @recipient_one,
               self()
             )

    assert :ok = Relay.end_browser_session(c.relay, @browser_session)

    assert_receive {:attention_fenced, ^session}
    state = :sys.get_state(c.relay)
    assert state.sessions == %{}
    assert state.browser_recipients == %{}
    assert state.store.engine.devices == %{}
  end

  test "browser file lookup checks the current owner revision without a tab recipient", c do
    assert {:ok, revision} = Relay.owner_revision(c.relay, c.person, c.owner_key)

    assert {:error, :not_found} =
             Relay.browser_fetch_file(
               c.relay,
               c.person,
               c.owner_key,
               revision,
               "00000000-0000-4000-8000-000000000003"
             )

    assert {:error, :session_ended} =
             Relay.browser_fetch_file(
               c.relay,
               c.person,
               c.owner_key,
               revision + 1,
               "00000000-0000-4000-8000-000000000003"
             )
  end
end
