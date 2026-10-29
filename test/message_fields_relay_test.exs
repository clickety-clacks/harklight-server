defmodule AttentionServer.MessageFieldsRelayTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{JSON, Placement, Relay}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  setup do
    dir =
      Path.join(
        System.tmp_dir!(),
        "harklight-message-fields-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{path: Path.join(dir, "relay.sqlite3")}
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp start_relay(path, opts \\ []) do
    assert {:ok, relay} = Relay.start_link(Keyword.merge([store_path: path], opts))
    Process.unlink(relay)
    on_exit(fn -> if Process.alive?(relay), do: GenServer.stop(relay) end)
    relay
  end

  defp request(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, reply} = JSON.decode_line(line)
    reply
  end

  defp setup_devices(relay) do
    assert :ok = Relay.setup_person(relay, "person-a")
    assert :ok = Relay.setup_device(relay, "person-a", "origin")
    assert :ok = Relay.setup_device(relay, "person-a", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")
  end

  defp await_terminal(relay, device, remaining \\ 100)

  defp await_terminal(_relay, _device, 0), do: flunk("default terminal did not arrive")

  defp await_terminal(relay, device, remaining) do
    case Relay.origin_feed(relay, device, 0) do
      {:ok, %{"entries" => entries}} ->
        case Enum.find(entries, &(&1["kind"] == "terminal")) do
          terminal when is_map(terminal) ->
            terminal

          nil ->
            Process.sleep(50)
            await_terminal(relay, device, remaining - 1)
        end

      other ->
        flunk("unexpected origin feed result: #{inspect(other)}")
    end
  end

  test "raw sends keep invalid optional bytes while the style hold strips only styles", %{
    path: path
  } do
    relay = start_relay(path)
    setup_devices(relay)

    original = %{
      "sender_id" => "app:test",
      "message_id" => "raw-fields",
      "created_at" => 1,
      "title" => "A request",
      "urgency" => "urgent",
      "by_when" => "tomorrow",
      "thread" => "",
      "choice_styles" => nil,
      "stake" => 7,
      "future_member" => %{"kept" => true}
    }

    assert %{
             "type" => "ok",
             "field_warnings" => [
               %{"field" => "urgency", "reason" => "invalid"},
               %{"field" => "by_when", "reason" => "invalid"},
               %{"field" => "thread", "reason" => "invalid"},
               %{"field" => "choice_styles", "reason" => "held"},
               %{"field" => "stake", "reason" => "invalid"}
             ]
           } =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(1),
               "message" => original
             })

    assert {:ok, [delivery], 1} = Relay.events(relay, "reader", 0)
    assert delivery["type"] == "delivery"
    assert delivery["message"] == Map.delete(original, "choice_styles")
    GenServer.stop(relay)
  end

  test "revisions tolerate invalid optional fields and keep the original message group", %{
    path: path
  } do
    relay = start_relay(path)
    setup_devices(relay)

    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(4),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "revised-fields",
                 "created_at" => 1,
                 "title" => "Original",
                 "thread" => "original-thread",
                 "urgency" => "low"
               }
             })

    revised_message = %{
      "sender_id" => "app:test",
      "message_id" => "revised-fields",
      "created_at" => 2,
      "title" => "Revised",
      "urgency" => "urgent",
      "by_when" => "tomorrow",
      "thread" => "new-thread",
      "choice_styles" => %{"unknown-action" => "primary"},
      "stake" => 7,
      "future_member" => %{"kept" => true}
    }

    assert %{
             "type" => "ok",
             "revision" => 2,
             "field_warnings" => [
               %{"field" => "urgency", "reason" => "invalid"},
               %{"field" => "by_when", "reason" => "invalid"},
               %{"field" => "choice_styles", "reason" => "held"},
               %{"field" => "stake", "reason" => "invalid"}
             ]
           } =
             request(relay, "origin", %{
               "type" => "conversation_revise",
               "op_id" => op(5),
               "sender_id" => "app:test",
               "message_id" => "revised-fields",
               "base_revision" => 1,
               "message" => revised_message
             })

    assert {:ok, %{"entries" => [_, revision]}} = Relay.origin_feed(relay, "origin", 0)
    assert revision["kind"] == "revision"
    assert revision["message"] == Map.delete(revised_message, "choice_styles")

    [view_entry] =
      relay
      |> :sys.get_state()
      |> then(&Placement.baseline(&1.store.engine, "person-a"))

    assert view_entry["thread"] == "original-thread"
    assert view_entry["priority"] == "normal"
    refute Map.has_key?(view_entry, "due_at")

    GenServer.stop(relay)
  end

  test "a due default wins on restart and follows the normal terminal and receipt paths", %{
    path: path
  } do
    {:ok, clock} = Agent.start_link(fn -> 1_000 end)
    on_exit(fn -> if Process.alive?(clock), do: Agent.stop(clock) end)
    now_ms = fn -> Agent.get(clock, & &1) end

    relay = start_relay(path, clock: now_ms)
    setup_devices(relay)

    default_at = 2_000

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(2),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "defaulted",
                 "created_at" => 1,
                 "title" => "Choose",
                 "default" => %{"value" => "ship", "at" => default_at},
                 "answer_ui" => %{
                   "schema" => "attention-client.actions/1",
                   "actions" => [
                     %{"id" => "hold", "label" => "Later", "kind" => "value", "value" => "hold"},
                     %{"id" => "ship", "label" => "Later", "kind" => "value", "value" => "ship"},
                     %{"id" => "reply", "label" => "Reply", "kind" => "text"}
                   ]
                 }
               }
             })

    GenServer.stop(relay)
    Agent.update(clock, fn _ -> 3_000 end)

    restarted = start_relay(path, clock: now_ms)

    assert %{"type" => "ok"} =
             request(restarted, "origin", %{
               "type" => "forward",
               "op_id" => op(3),
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "unrelated",
                 "created_at" => 2,
                 "title" => "Still available"
               }
             })

    terminal = await_terminal(restarted, "origin")
    assert terminal["author"] == "server"
    assert terminal["by"] == "default"
    assert terminal["answer"] == "ship"
    assert terminal["at"] >= default_at

    assert {:ok, events, 3} = Relay.events(restarted, "reader", 1)
    assert length(events) == 2
    unrelated_delivery = Enum.find(events, &(&1["type"] == "delivery"))
    lifecycle = Enum.find(events, &(&1["type"] == "lifecycle"))
    assert unrelated_delivery["type"] == "delivery"
    assert unrelated_delivery["message"]["message_id"] == "unrelated"
    assert lifecycle["type"] == "lifecycle"
    assert lifecycle["author"] == "server"
    assert lifecycle["state"] == "answered"
    assert lifecycle["by"] == "default"

    assert lifecycle["answer_outcome"] == %{
             "kind" => "default",
             "value" => "ship",
             "text" => "Later"
           }

    assert %{
             "type" => "ok",
             "receipt" => %{
               "state" => "answered",
               "by" => "default",
               "answer" => "ship",
               "actor" => %{"kind" => "server"}
             }
           } =
             request(restarted, "origin", %{
               "type" => "answer_receipt",
               "receipt_version" => 1,
               "sender_id" => "app:test",
               "message_id" => "defaulted",
               "terminal_seq" => terminal["terminal_seq"]
             })

    GenServer.stop(restarted)

    second_restart = start_relay(path)
    assert {:ok, [], 3} = Relay.events(second_restart, "reader", 3)
    GenServer.stop(second_restart)
  end
end
