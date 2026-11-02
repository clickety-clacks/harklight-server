defmodule AttentionServer.PagingTerminalIdentityTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{Engine, JSON, Relay, Store}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  setup do
    dir = Path.join(System.tmp_dir!(), "paging-terminal-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "state.sqlite3")
    {:ok, relay} = Relay.start_link(store_path: path)
    on_exit(fn -> if Process.alive?(relay), do: GenServer.stop(relay) end)
    :ok = Relay.setup_person(relay, "person")
    :ok = Relay.setup_person(relay, "other-person")

    for device <- ~w(origin reader second-reader) do
      :ok = Relay.setup_device(relay, "person", device)
    end

    :ok = Relay.setup_device(relay, "other-person", "outsider")
    :ok = Relay.setup_sender(relay, "origin", "app:test")
    %{relay: relay, path: path}
  end

  for kind <- ~w(answer answer_no_comment lifecycle) do
    test "paged #{kind} terminal keeps the turn referenced by a real origin receipt", ctx do
      kind = unquote(kind)
      send = send_frame("terminal")
      assert %{"type" => "ok", "seq" => 1} = request(ctx.relay, "origin", send)
      assert {:error, :not_found} = Relay.origin_ack(ctx.relay, "origin", 1)

      assert %{"type" => "ok", "revision" => 2} =
               request(ctx.relay, "origin", %{
                 "type" => "conversation_revise",
                 "op_id" => op(2),
                 "sender_id" => "app:test",
                 "message_id" => "terminal",
                 "base_revision" => 1,
                 "message" => %{send["message"] | "title" => "Updated request"}
               })

      terminal_operation = terminal_operation(kind)

      assert %{"type" => "ok", "seq" => 2} =
               request(ctx.relay, "reader", terminal_operation)

      assert {:ok, %{"entries" => [initial, revised, terminal]}} =
               Relay.origin_feed(ctx.relay, "origin", 0)

      assert initial["kind"] == "revision"
      assert revised["request_revision"] == 2
      assert terminal["kind"] == "terminal"
      assert terminal["request_revision"] == 2
      assert terminal["author"] == "person"
      refute initial["event_id"] == terminal["event_id"]
      assert {:error, :not_found} = Relay.origin_ack(ctx.relay, "outsider", 3)
      assert {:ok, %{"status" => "delivered"}} = Relay.origin_ack(ctx.relay, "origin", 3)

      for device <- ~w(origin reader second-reader) do
        assert {:ok, [revision, receipt], 2} =
                 Relay.conversation_events(ctx.relay, device, 0)

        assert revision["kind"] == "revision"
        assert revision["request_revision"] == 2
        assert receipt["kind"] == "receipt"
        assert receipt["event_id"] == terminal["event_id"]
        assert receipt["request_revision"] == 2
        assert receipt["session_handle"] == @handle
        assert receipt["sender_id"] == "app:test"
        assert receipt["message_id"] == "terminal"
        assert receipt["origin"] == "origin"
        assert receipt["delivered"] == true
        refute Map.has_key?(receipt, "answer")
      end

      assert {:ok, [reader_terminal], 2} = Relay.events(ctx.relay, "reader", 1)
      assert {:ok, [origin_terminal], 2} = Relay.events(ctx.relay, "origin", 1)
      assert reader_terminal["event_id"] == terminal["event_id"]
      refute Map.has_key?(reader_terminal, "answer")

      engine = stored_engine(ctx)
      assert {:ok, frozen} = Engine.inbox_item(engine, "reader", {"app:test", "terminal"}, 1)
      refute Map.has_key?(frozen, "terminal")

      for device <- ~w(origin reader second-reader) do
        assert {:ok, page} = Engine.inbox_item(engine, device, {"app:test", "terminal"}, 2)
        assert page["acceptance"]["event_id"] == initial["event_id"]
        assert page["acceptance"]["request_revision"] == 1
        expected = if device == "origin", do: origin_terminal, else: reader_terminal
        assert page["terminal"] == expected
      end

      assert {:error, :snapshot_unavailable} =
               Engine.inbox_item(engine, "outsider", {"app:test", "terminal"}, 2)

      assert {:ok, ^origin_terminal} = Engine.inbox_receipt(engine, "origin", 2)
      assert {:error, :snapshot_unavailable} = Engine.inbox_receipt(engine, "reader", 2)

      assert {:ok, %{"terminal" => ^origin_terminal}} =
               Engine.reconcile_operation(engine, "origin", send, 2, true)

      assert {:ok, frozen_send} = Engine.reconcile_operation(engine, "origin", send, 1, true)
      refute Map.has_key?(frozen_send, "terminal")

      if kind == "answer_no_comment" do
        # The direct operation has no wire op_id. Paging has its own correlation.
        refute Map.has_key?(terminal_operation, "op_id")
        reconcile = Map.put(terminal_operation, "op_id", op(4))

        assert {:ok, _, %{"status" => "committed", "terminal" => ^reader_terminal}} =
                 Engine.reconcile_no_comment(engine, "reader", reconcile)
      else
        assert {:ok, %{"status" => "committed", "terminal" => ^reader_terminal}} =
                 Engine.reconcile_operation(engine, "reader", terminal_operation, 2, true)

        assert {:error, :snapshot_unavailable} =
                 Engine.reconcile_operation(engine, "reader", terminal_operation, 1, true)
      end
    end
  end

  test "discussion history precedes its receipt for origin and other devices", ctx do
    assert %{"type" => "ok"} = request(ctx.relay, "origin", send_frame("discussion"))

    assert %{"type" => "ok"} =
             request(ctx.relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(2),
               "sender_id" => "app:test",
               "message_id" => "discussion",
               "request_revision" => 1,
               "text" => "Which option?"
             })

    assert {:ok, %{"status" => "delivered"}} = Relay.origin_ack(ctx.relay, "origin", 2)

    for device <- ~w(origin reader second-reader) do
      assert {:ok, [discussion, receipt], 2} = Relay.conversation_events(ctx.relay, device, 0)
      assert discussion["kind"] == "discussion"
      assert receipt["kind"] == "receipt"
      assert discussion["conversation_seq"] < receipt["conversation_seq"]

      for field <- ~w(event_id request_revision session_handle sender_id message_id origin) do
        assert discussion[field] == receipt[field]
      end

      if device == "origin" do
        refute Map.has_key?(discussion, "text")
      else
        assert discussion["text"] == "Which option?"
      end

      # A later cursor intentionally omits its prior target: paging acceptance
      # alone does not reconstruct discussion history already acknowledged.
      assert {:ok, [^receipt], 2} = Relay.conversation_events(ctx.relay, device, 1)
    end

    assert {:ok, [], 0} = Relay.conversation_events(ctx.relay, "outsider", 0)
    engine = stored_engine(ctx)
    assert {:ok, page} = Engine.inbox_item(engine, "second-reader", {"app:test", "discussion"}, 1)
    assert page["acceptance"]["event_id"] == "1"
    refute Map.has_key?(page, "terminal")
  end

  test "unbound terminal paging does not invent conversation identity", ctx do
    send = Map.delete(send_frame("legacy"), "session_handle")
    assert %{"type" => "ok"} = request(ctx.relay, "origin", send)

    assert %{"type" => "ok"} =
             request(ctx.relay, "reader", %{
               "type" => "answer",
               "op_id" => op(2),
               "sender_id" => "app:test",
               "message_id" => "legacy",
               "answer" => nil
             })

    assert {:ok, [terminal], 2} = Relay.events(ctx.relay, "reader", 1)
    engine = stored_engine(ctx)

    assert {:ok, %{"terminal" => ^terminal}} =
             Engine.inbox_item(engine, "reader", {"app:test", "legacy"}, 2)

    refute Map.has_key?(terminal, "event_id")
    refute Map.has_key?(terminal, "request_revision")
  end

  defp stored_engine(ctx) do
    GenServer.stop(ctx.relay)
    assert {:ok, store} = Store.open(ctx.path)
    engine = store.engine
    assert :ok = Store.close(store)
    engine
  end

  defp send_frame(id) do
    %{
      "type" => "forward",
      "op_id" => op(1),
      "session_handle" => @handle,
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => id,
        "created_at" => 1,
        "title" => "Request"
      }
    }
  end

  defp terminal_operation("answer_no_comment") do
    %{"type" => "answer_no_comment", "request_id" => "terminal", "request_revision" => 2}
  end

  defp terminal_operation(kind) do
    base = %{
      "type" => kind,
      "op_id" => op(3),
      "sender_id" => "app:test",
      "message_id" => "terminal"
    }

    if kind == "answer",
      do: Map.merge(base, %{"request_revision" => 2, "answer" => "Confirmed"}),
      else: Map.merge(base, %{"state" => "cleared", "at" => 2})
  end

  defp request(relay, key, frame) do
    wire = if frame["type"] == "answer_no_comment", do: frame, else: Map.put(frame, "v", 1)
    assert {:ok, line} = Relay.request_line(relay, key, JSON.encode!(wire) <> "\n")
    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"
end
