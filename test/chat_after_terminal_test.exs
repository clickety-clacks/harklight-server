defmodule AttentionServer.ChatAfterTerminalTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{JSON, Relay}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  setup do
    dir = Path.join(System.tmp_dir!(), "harklight-chat-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    assert {:ok, relay} = Relay.start_link(store_path: Path.join(dir, "state.sqlite3"))
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")
    on_exit(fn -> if Process.alive?(relay), do: GenServer.stop(relay) end)
    %{relay: relay}
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp request(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp send_request(relay, id, n) do
    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(n),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => id,
                 "created_at" => n,
                 "title" => "Question #{id}"
               }
             })
  end

  defp discussion(relay, id, n, text \\ "A question") do
    request(relay, "reader", %{
      "type" => "discussion",
      "op_id" => op(n),
      "sender_id" => "app:test",
      "message_id" => id,
      "request_revision" => 1,
      "text" => text
    })
  end

  defp reply(relay, id, n, to, text) do
    request(relay, "origin", %{
      "type" => "conversation_reply",
      "op_id" => op(n),
      "sender_id" => "app:test",
      "message_id" => id,
      "in_reply_to" => to,
      "request_revision" => 1,
      "text" => text
    })
  end

  defp terminal_state(relay, id) do
    {:ok, %{"entries" => entries}} = Relay.origin_feed(relay, "origin", 0)

    entries
    |> Enum.filter(&(&1["kind"] == "terminal" and &1["message_id"] == id))
    |> Enum.map(& &1["terminal_state"])
  end

  test "chat stays open both ways after answered, cleared, expired, retracted and superseded", %{
    relay: relay
  } do
    for {id, n} <- [{"answered", 10}, {"cleared", 20}, {"retracted", 30}, {"old", 40}, {"expired", 60}] do
      send_request(relay, id, n)
      assert %{"type" => "ok", "event_id" => event} = discussion(relay, id, n + 1, "Before")
      assert %{"type" => "ok"} = reply(relay, id, n + 2, String.to_integer(event), "Before reply")
    end

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(100),
               "sender_id" => "app:test",
               "message_id" => "answered",
               "request_revision" => 1,
               "answer" => "Yes"
             })

    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "lifecycle",
               "op_id" => op(101),
               "sender_id" => "app:test",
               "message_id" => "cleared",
               "state" => "cleared",
               "at" => 5
             })

    assert %{"type" => "ok", "state" => "retracted"} =
             request(relay, "origin", %{
               "type" => "conversation_withdraw",
               "op_id" => op(102),
               "sender_id" => "app:test",
               "message_id" => "retracted"
             })

    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "lifecycle",
               "op_id" => op(104),
               "sender_id" => "app:test",
               "message_id" => "expired",
               "state" => "expired",
               "at" => 6
             })

    # A newer message from the same sender supersedes the old one.
    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(103),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "new",
                 "created_at" => 50,
                 "title" => "Replacement",
                 "supersedes" => ["old"]
               }
             })

    for {id, n} <- [{"answered", 110}, {"cleared", 120}, {"retracted", 130}, {"old", 140}, {"expired", 160}] do
      before = terminal_state(relay, id)
      assert before != []

      assert %{"type" => "ok", "event_id" => event} = discussion(relay, id, n, "After the end")
      assert %{"type" => "ok"} = reply(relay, id, n + 1, String.to_integer(event), "Still here")

      # The outcome is not touched by chat.
      assert terminal_state(relay, id) == before
    end

    # A second answer is still refused.
    assert %{"type" => "error", "reason" => "closed"} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(150),
               "sender_id" => "app:test",
               "message_id" => "answered",
               "request_revision" => 1,
               "answer" => "No"
             })
  end

  test "an agent may reply several times to one chat message; the same operation is deduped",
       %{relay: relay} do
    send_request(relay, "m", 10)
    assert %{"type" => "ok", "event_id" => event} = discussion(relay, "m", 11)
    to = String.to_integer(event)

    assert %{"type" => "ok", "event_id" => first} = reply(relay, "m", 12, to, "One")
    assert %{"type" => "ok", "event_id" => second} = reply(relay, "m", 13, to, "Two")
    assert %{"type" => "ok", "event_id" => third} = reply(relay, "m", 14, to, "Three")
    assert length(Enum.uniq([first, second, third])) == 3

    assert %{"type" => "ok", "duplicate" => true, "event_id" => ^second} =
             reply(relay, "m", 13, to, "Two")

    assert %{"type" => "error", "reason" => "operation_conflict"} =
             reply(relay, "m", 13, to, "Different text")

    assert {:ok, %{"entries" => entries}} = Relay.origin_feed(relay, "origin", 0)

    assert entries
           |> Enum.filter(&(&1["kind"] == "reply"))
           |> Enum.map(& &1["text"]) == ["One", "Two", "Three"]

    # The person event is recorded as acted on once.
    assert {:ok, events, _high} = Relay.conversation_events(relay, "reader", 0)
    assert Enum.count(events, &(&1["kind"] == "acted")) == 1
  end

  test "chat to a sender that is not listening is accepted and held", %{relay: relay} do
    send_request(relay, "m", 10)

    assert {:ok, %{"availability" => availability}} = Relay.origin_status(relay, "origin", @handle)
    assert availability in ["not_listening", "unsupported"]

    assert %{"type" => "ok", "event_id" => event_id} = discussion(relay, "m", 11, "Anyone there?")

    assert {:ok, %{"entries" => entries}} = Relay.origin_feed(relay, "origin", 0)
    held = Enum.find(entries, &(&1["event_id"] == event_id))
    assert held["kind"] == "discussion"
    assert held["delivered"] == false
  end
end
