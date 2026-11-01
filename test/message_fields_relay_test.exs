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

  test "typed sends are stored and delivered, with coded refusals and raw kind warnings", %{
    path: path
  } do
    relay = start_relay(path)
    setup_devices(relay)

    answer_ui = %{
      "schema" => "attention-client.actions/1",
      "actions" => [
        %{"id" => "hold", "label" => "Hold", "kind" => "value", "value" => "hold"},
        %{"id" => "reply", "label" => "Reply", "kind" => "text"}
      ]
    }

    typed = %{
      "sender_id" => "app:test",
      "message_id" => "typed-request",
      "created_at" => 1,
      "title" => "Choose",
      "answer_ui" => answer_ui,
      "message_type" => "request",
      "message_kind" => "blocker"
    }

    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(20),
               "session_handle" => @handle,
               "message" => typed
             })

    assert {:ok, [delivery], 1} = Relay.events(relay, "reader", 0)
    assert delivery["message"] == typed

    assert {:ok, %{"entries" => [feed_entry]}} = Relay.origin_feed(relay, "origin", 0)
    assert feed_entry["message"] == typed

    invalid_type = Map.put(%{typed | "message_id" => "bad-type"}, "message_type", "notice")

    assert %{
             "type" => "error",
             "code" => "message_type_invalid",
             "reason" => ~s(message_type must be "fyi" or "request"; got "notice")
           } =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(21),
               "message" => invalid_type
             })

    invalid_controls =
      typed
      |> Map.put("message_id", "bad-controls")
      |> Map.put("message_type", "fyi")

    assert %{
             "type" => "error",
             "code" => "fyi_has_answer_controls",
             "reason" => reason
           } =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(22),
               "message" => invalid_controls
             })

    assert reason =~ "Seen descriptor"

    invalid_kind =
      typed
      |> Map.put("message_id", "bad-kind")
      |> Map.put("message_kind", "update")

    assert %{
             "type" => "ok",
             "field_warnings" => [%{"field" => "message_kind", "reason" => "invalid"}]
           } =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(23),
               "session_handle" => @handle,
               "message" => invalid_kind
             })

    assert {:ok, events, 2} = Relay.events(relay, "reader", 1)
    dropped_kind_delivery = Enum.find(events, &(&1["message"]["message_id"] == "bad-kind"))
    refute Map.has_key?(dropped_kind_delivery["message"], "message_kind")

    assert {:ok, %{"entries" => entries}} = Relay.origin_feed(relay, "origin", 0)

    assert Enum.map(entries, &get_in(&1, ["message", "message_id"])) == [
             "typed-request",
             "bad-kind"
           ]

    GenServer.stop(relay)
  end

  test "typed revision carries omitted type and controls and refuses an invalid replacement atomically",
       %{
         path: path
       } do
    relay = start_relay(path)
    setup_devices(relay)

    answer_ui = %{
      "schema" => "attention-client.actions/1",
      "actions" => [%{"id" => "reply", "label" => "Reply", "kind" => "text"}]
    }

    original = %{
      "sender_id" => "app:test",
      "message_id" => "typed-revision",
      "created_at" => 1,
      "title" => "Original request",
      "answer_ui" => answer_ui,
      "message_type" => "request"
    }

    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(24),
               "session_handle" => @handle,
               "message" => original
             })

    revision = %{
      "sender_id" => "app:test",
      "message_id" => "typed-revision",
      "created_at" => 2,
      "title" => "Revised request"
    }

    assert %{"type" => "ok", "revision" => 2} =
             request(relay, "origin", %{
               "type" => "conversation_revise",
               "op_id" => op(25),
               "sender_id" => "app:test",
               "message_id" => "typed-revision",
               "base_revision" => 1,
               "message" => revision
             })

    assert %{
             "type" => "error",
             "code" => "fyi_has_answer_controls",
             "reason" => reason
           } =
             request(relay, "origin", %{
               "type" => "conversation_revise",
               "op_id" => op(26),
               "sender_id" => "app:test",
               "message_id" => "typed-revision",
               "base_revision" => 2,
               "message" =>
                 revision
                 |> Map.put("created_at", 3)
                 |> Map.put("title", "Invalid FYI")
                 |> Map.put("message_type", "fyi")
                 |> Map.put("answer_ui", answer_ui)
             })

    assert reason =~ "Seen descriptor"

    assert %{
             "type" => "error",
             "code" => "message_type_invalid",
             "reason" => ~s(message_type must be "fyi" or "request"; got "notice")
           } =
             request(relay, "origin", %{
               "type" => "conversation_revise",
               "op_id" => op(27),
               "sender_id" => "app:test",
               "message_id" => "typed-revision",
               "base_revision" => 2,
               "message" => revision |> Map.put("message_type", "notice")
             })

    assert {:ok, %{"entries" => [_original_entry, revision_entry]}} =
             Relay.origin_feed(relay, "origin", 0)

    assert revision_entry["message"]["message_type"] == "request"
    assert revision_entry["message"]["answer_ui"] == answer_ui
    assert revision_entry["message"]["title"] == "Revised request"

    GenServer.stop(relay)
  end

  test "an invalid type on a revise leaves a stored untyped message unchanged", %{path: path} do
    relay = start_relay(path)
    setup_devices(relay)

    original = %{
      "sender_id" => "app:test",
      "message_id" => "untyped-revision",
      "created_at" => 1,
      "title" => "Legacy request",
      "answer_ui" => %{"kind" => "choice", "items" => ["yes", "no"]}
    }

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(28),
               "session_handle" => @handle,
               "message" => original
             })

    assert %{
             "type" => "error",
             "code" => "message_type_invalid",
             "reason" => ~s(message_type must be "fyi" or "request"; got "notice")
           } =
             request(relay, "origin", %{
               "type" => "conversation_revise",
               "op_id" => op(29),
               "sender_id" => "app:test",
               "message_id" => "untyped-revision",
               "base_revision" => 1,
               "message" => Map.put(original, "message_type", "notice")
             })

    assert {:ok, %{"entries" => [entry]}} = Relay.origin_feed(relay, "origin", 0)
    assert entry["message"] == original
    refute Map.has_key?(entry["message"], "message_type")

    assert {:ok, [delivery], 1} = Relay.events(relay, "reader", 0)
    assert delivery["message"] == original

    GenServer.stop(relay)
  end

  test "a typed revision can change the effective message type with matching controls", %{
    path: path
  } do
    relay = start_relay(path)
    setup_devices(relay)

    seen = %{
      "schema" => "attention-client.actions/1",
      "actions" => [
        %{
          "id" => "seen",
          "label" => "Seen",
          "kind" => "value",
          "value" => %{"acknowledgment" => "seen"}
        }
      ]
    }

    original = %{
      "sender_id" => "app:test",
      "message_id" => "typed-type-change",
      "created_at" => 1,
      "title" => "Notice",
      "answer_ui" => seen,
      "message_type" => "fyi"
    }

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(30),
               "session_handle" => @handle,
               "message" => original
             })

    revised =
      original
      |> Map.put("created_at", 2)
      |> Map.put("title", "Question")
      |> Map.put("message_type", "request")
      |> Map.put("answer_ui", %{
        "schema" => "attention-client.actions/1",
        "actions" => [%{"id" => "reply", "label" => "Reply", "kind" => "text"}]
      })

    assert %{"type" => "ok", "revision" => 2} =
             request(relay, "origin", %{
               "type" => "conversation_revise",
               "op_id" => op(31),
               "sender_id" => "app:test",
               "message_id" => "typed-type-change",
               "base_revision" => 1,
               "message" => revised
             })

    assert {:ok, %{"entries" => [_initial, revised_entry]}} =
             Relay.origin_feed(relay, "origin", 0)

    assert revised_entry["message"]["message_type"] == "request"
    assert revised_entry["message"]["answer_ui"] == revised["answer_ui"]

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
      "message_kind" => "update",
      "future_member" => %{"kept" => true}
    }

    assert %{
             "type" => "ok",
             "revision" => 2,
             "field_warnings" => [
               %{"field" => "urgency", "reason" => "invalid"},
               %{"field" => "by_when", "reason" => "invalid"},
               %{"field" => "choice_styles", "reason" => "held"},
               %{"field" => "stake", "reason" => "invalid"},
               %{"field" => "message_kind", "reason" => "invalid"}
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

    assert revision["message"] ==
             revised_message |> Map.delete("choice_styles") |> Map.delete("message_kind")

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

    assert %{"type" => "error", "reason" => "unknown_type"} =
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
