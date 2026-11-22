defmodule AttentionServer.BrowserRelayRecipientTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{BrowserRecipient, DeviceIdentity, JSON, Relay, TLSFixtures}
  alias AttentionServer.ClientAPI.{Reads, Writes}

  @browser_session String.duplicate("s", 43)

  defp op(n),
    do: "00000000-0000-4000-8000-" <> String.pad_leading(Integer.to_string(n), 12, "0")

  @moduletag :tmp_dir

  setup %{tmp_dir: dir} do
    certs = TLSFixtures.create!()
    owner_cert = TLSFixtures.certificate_der!(certs, "a1")
    path = Path.join(dir, "state.sqlite3")
    {:ok, relay} = Relay.start_link(store_path: path)
    {:ok, person} = Relay.operator_bootstrap(relay, owner_cert)
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner_cert)

    on_exit(fn ->
      if Process.alive?(relay), do: GenServer.stop(relay)
      File.rm_rf!(certs)
    end)

    %{relay: relay, path: path, person: person, owner_key: owner_key}
  end

  # The principal that the web origin's sign-in cookie resolves to
  # (`ClientAPI.Auth.browser/2`) for a browser session.
  defp browser(c, relay) do
    assert {:ok, revision} = Relay.owner_revision(relay, c.person, c.owner_key)

    %{
      kind: :browser,
      person: c.person,
      key: c.owner_key,
      session_id: @browser_session,
      owner_revision: revision
    }
  end

  # The browser recipient a client API v2 write from `session` acts as: one
  # fixed recipient per session, derived from the session id.
  defp browser_actor(person, session) do
    <<a::48, _::4, b::12, _::2, c::62, _::binary>> =
      :crypto.hash(:sha256, "client_api_v2:" <> session)

    <<p1::binary-8, p2::binary-4, p3::binary-4, p4::binary-4, p5::binary-12>> =
      Base.encode16(<<a::48, 4::4, b::12, 2::2, c::62>>, case: :lower)

    BrowserRecipient.key(person, session, Enum.join([p1, p2, p3, p4, p5], "-"))
  end

  defp forward(c) do
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
    browser = browser(c, c.relay)
    assert {:ok, %{"items" => [%{"item_id" => item}]}} = Reads.inbox(c.relay, browser)
    {browser, item}
  end

  defp terminal(relay, person),
    do:
      :sys.get_state(relay).store.engine.messages[{person, "app:test", "browser-action"}].terminal

  test "a browser opens and answers as its session's derived browser actor, durably", c do
    {browser, item} = forward(c)
    actor = browser_actor(c.person, @browser_session)
    assert BrowserRecipient.valid_for?(actor, c.person)

    assert {:ok, 200, %{"item" => %{"seen" => true}}, false} =
             Writes.seen(c.relay, browser, item, op(2))

    answer_key = op(3)

    assert {:ok, 200, answered, false} =
             Writes.answer(c.relay, browser, item, 1, :no_comment, answer_key)

    terminal = terminal(c.relay, c.person)
    assert terminal.actor_kind == :browser
    assert terminal.actor == actor
    refute Map.has_key?(:sys.get_state(c.relay).store.engine.devices, actor)

    # The same session retrying with the same key replays the committed answer.
    assert {:ok, 200, ^answered, true} =
             Writes.answer(c.relay, browser, item, 1, :no_comment, answer_key)

    assert terminal(c.relay, c.person) == terminal

    GenServer.stop(c.relay)
    {:ok, restarted} = Relay.start_link(store_path: c.path)
    on_exit(fn -> if Process.alive?(restarted), do: GenServer.stop(restarted) end)

    assert terminal(restarted, c.person) == terminal
    browser = browser(c, restarted)

    assert {:ok, 200, ^answered, true} =
             Writes.answer(restarted, browser, item, 1, :no_comment, answer_key)

    assert terminal(restarted, c.person) == terminal
  end

  test "ending a browser session holds no Relay state for it; stored actor state goes at restart",
       c do
    {browser, item} = forward(c)
    actor = browser_actor(c.person, @browser_session)

    # Seen stores an operation receipt under the browser actor.
    assert {:ok, 200, _, false} = Writes.seen(c.relay, browser, item, op(2))
    assert {:ok, 200, _, false} = Writes.answer(c.relay, browser, item, 1, :no_comment, op(3))

    assert Enum.any?(:sys.get_state(c.relay).store.engine.receipts, fn
             {{_person, ^actor, _op}, _} -> true
             _ -> false
           end)

    assert :ok = Relay.end_browser_session(c.relay, @browser_session)
    state = :sys.get_state(c.relay)
    assert state.sessions == %{}
    refute Map.has_key?(state.store.engine.devices, actor)

    GenServer.stop(c.relay)
    {:ok, restarted} = Relay.start_link(store_path: c.path)
    on_exit(fn -> if Process.alive?(restarted), do: GenServer.stop(restarted) end)
    engine = :sys.get_state(restarted).store.engine

    refute Enum.any?(engine.receipts, fn {{_person, key, _op}, _} -> key == actor end)
    assert terminal(restarted, c.person).actor == actor
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
