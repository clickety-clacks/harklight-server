defmodule AttentionServer.OriginNoopDeclarationsTest do
  # Agents re-declare the same listening support and attachment many times a
  # minute. A declaration that would change nothing is answered from current
  # state: no snapshot commit and no new operation receipt. A changed
  # declaration commits exactly as before, and retries stay coherent.
  use ExUnit.Case, async: false

  alias AttentionServer.{JSON, OriginWire, Relay}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(to_string(n), 12, "0")}"

  setup do
    dir = Path.join(System.tmp_dir!(), "noop-declarations-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "state.sqlite3")
    relay = start_supervised!({Relay, store_path: path}, id: make_ref(), restart: :temporary)
    :ok = Relay.setup_person(relay, "person")
    :ok = Relay.setup_device(relay, "person", "origin")
    :ok = Relay.setup_sender(relay, "origin", "app:test")

    {:ok, line} =
      Relay.request_line(
        relay,
        "origin",
        JSON.encode!(%{
          "v" => 1,
          "type" => "forward",
          "op_id" => op(1),
          "session_handle" => @handle,
          "message" => %{
            "sender_id" => "app:test",
            "message_id" => "question",
            "created_at" => 1,
            "title" => "Check this"
          }
        }) <> "\n"
      )

    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(line)
    %{relay: relay, path: path}
  end

  defp support(relay, n, ack2) do
    OriginWire.command(relay, "origin", %{
      "v" => 1,
      "type" => "support",
      "op_id" => op(n),
      "session_handle" => @handle,
      "route" => "codex",
      "cli_version" => "fixture",
      "unsupported_reason" => nil,
      "ack2" => ack2
    })
  end

  defp attachment(relay, n, vendor, support \\ %{}) do
    OriginWire.command(
      relay,
      "origin",
      Map.merge(
        %{
          "v" => 1,
          "type" => "attachment_update",
          "op_id" => op(n),
          "session_handle" => @handle,
          "vendor_session_id" => vendor
        },
        support
      )
    )
  end

  defp store(relay), do: :sys.get_state(relay).store
  defp receipts(relay), do: map_size(store(relay).engine.receipts)

  test "an unchanged support declaration is answered from state without a commit or receipt",
       %{relay: relay} do
    assert %{"type" => "ok", "listening_support" => first} = support(relay, 10, true)
    version = store(relay).version
    receipt_count = receipts(relay)
    engine = store(relay).engine

    # Same declaration, new operation id: nothing would change.
    assert %{"type" => "ok", "listening_support" => ^first} = same = support(relay, 11, true)
    assert store(relay).version == version
    assert receipts(relay) == receipt_count
    assert store(relay).engine.origin_sessions == engine.origin_sessions

    # A retry of that operation id gets the same answer, still without a commit.
    assert support(relay, 11, true) == same
    assert store(relay).version == version

    # A changed declaration commits with its receipt, as before.
    assert %{"type" => "ok", "listening_support" => %{"ack2" => false}} =
             support(relay, 12, false)

    assert store(relay).version == version + 1
    assert receipts(relay) == receipt_count + 1

    # A retry of an operation that minted a receipt is still a recorded duplicate.
    assert %{"type" => "ok", "duplicate" => true, "listening_support" => ^first} =
             support(relay, 10, true)

    assert store(relay).version == version + 1

    # A retry of the unchanged operation after a changed one is applied as the
    # declaration it carries (last arrival wins): a normal answer, never a refusal.
    assert %{"type" => "ok", "listening_support" => %{"ack2" => true}} = support(relay, 11, true)
    assert store(relay).version == version + 2
  end

  test "an unchanged attachment update is answered from state without a commit; a change commits",
       %{relay: relay} do
    assert %{"type" => "ok", "vendor_session_id" => "vendor-1"} =
             attachment(relay, 20, "vendor-1")

    version = store(relay).version
    receipt_count = receipts(relay)

    assert %{"type" => "ok", "vendor_session_id" => "vendor-1"} =
             attachment(relay, 21, "vendor-1")

    assert store(relay).version == version
    assert receipts(relay) == receipt_count

    declared = %{"route" => "codex", "cli_version" => "fixture", "unsupported_reason" => nil}

    assert %{"type" => "ok", "listening_support" => record} =
             attachment(relay, 22, "vendor-1", declared)

    assert store(relay).version == version + 1

    # The same attachment and the same support: nothing would change.
    assert %{"type" => "ok", "listening_support" => ^record} =
             attachment(relay, 23, "vendor-1", declared)

    assert store(relay).version == version + 1

    assert %{"type" => "ok", "vendor_session_id" => "vendor-2"} =
             attachment(relay, 24, "vendor-2")

    assert store(relay).version == version + 2
  end

  test "no-op answers survive a restart of the Relay", %{relay: relay, path: path} do
    assert %{"type" => "ok", "listening_support" => first} = support(relay, 30, true)
    assert %{"type" => "ok"} = support(relay, 31, true)
    engine = store(relay).engine
    GenServer.stop(relay)

    restarted =
      start_supervised!({Relay, store_path: path}, id: make_ref(), restart: :temporary)

    assert :sys.get_state(restarted).store.engine.origin_sessions == engine.origin_sessions
    assert %{"type" => "ok", "listening_support" => ^first} = support(restarted, 31, true)
  end
end
