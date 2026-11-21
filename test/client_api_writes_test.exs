defmodule AttentionServer.ClientAPIWritesTest do
  # Client API v2 writes and the Idempotency-Key store (docs/client-api-v2.md
  # §2 and §5, spec §4 and §10), behind the client_api_v2 flag.
  use ExUnit.Case, async: false

  alias AttentionServer.{ChangeLog, JSON, Relay}
  alias AttentionServer.ClientAPI.{Context, FeedRenderer, Principal, Reads, Writes}
  alias AttentionServer.ClientAPISchema, as: Schema

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @contract Path.expand("../docs/client-api-v2.md", __DIR__)
  @owner_key String.duplicate("ab", 32)

  setup do
    dir =
      Path.join(
        System.tmp_dir!(),
        "harklight-client-writes-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    flags = Path.join(dir, "flags.json")
    File.write!(flags, ~s({"client_api_v2":true}))
    %{path: Path.join(dir, "state.sqlite3"), flags: flags}
  end

  defp start(path, flags, extra \\ []) do
    opts = [store_path: path, agent_fixture_mode: true] ++ extra
    opts = if flags, do: Keyword.put(opts, :feature_flags_path, flags), else: opts
    assert {:ok, relay} = Relay.start_link(opts)
    relay
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp key do
    <<a::48, _::4, b::12, _::2, c::62>> = :crypto.strong_rand_bytes(16)
    hex = Base.encode16(<<a::48, 4::4, b::12, 2::2, c::62>>, case: :lower)
    <<p1::binary-8, p2::binary-4, p3::binary-4, p4::binary-4, p5::binary-12>> = hex
    Enum.join([p1, p2, p3, p4, p5], "-")
  end

  defp request(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp forward(relay, id, operation) do
    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(operation),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => id,
                 "created_at" => operation,
                 "title" => "Question #{id}"
               }
             })
  end

  defp scripted_relay(path, flags, extra \\ []) do
    relay = start(path, flags, extra)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "B", "stranger")
    for {id, n} <- [{"one", 2}, {"two", 3}], do: forward(relay, id, n)
    relay
  end

  defp with_owner(relay) do
    :sys.replace_state(relay, fn state ->
      engine = state.store.engine
      owners = Map.put(engine.owners, "A", %{key: @owner_key, revision: 4})
      %{state | store: %{state.store | engine: %{engine | owners: owners}}}
    end)

    %{kind: :browser, person: "A", key: @owner_key, owner_revision: 4, session_id: "s1"}
  end

  defp item_id(relay, message_id) do
    {:ok, %{"items" => items}} = Reads.inbox(relay, "reader")
    Enum.find_value(items, &(&1["message"]["message_id"] == message_id && &1["item_id"]))
  end

  defp head(relay), do: ChangeLog.head(:sys.get_state(relay).store, "A")

  defp changes_after(relay, position) do
    {:ok, changes, _more, _position} =
      ChangeLog.read(:sys.get_state(relay).store, "A", position, 500)

    changes
  end

  defp item_resource(message_id), do: ChangeLog.item_resource({"A", "app:test", message_id})

  describe "POST /v2/items/{item_id}/seen" do
    test "marks the item Seen and returns it at its new rev, with its change row",
         %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      one = item_id(relay, "one")
      assert {:ok, %{"item" => %{"seen" => false}}} = Reads.item(relay, "reader", one)
      {:ok, before} = head(relay)

      assert {:ok, 200, body, false} = Writes.seen(relay, "reader", one, key())
      assert Schema.errors(Schema.contract_defs(@contract), "WriteResult", body) == []
      assert %{"rev" => position, "item" => %{"item_id" => ^one, "seen" => true} = item} = body
      assert position > before
      assert item["rev"] > before and item["rev"] <= position

      # The write's change is on the one write path: a row for the item at its rev.
      changes = changes_after(relay, before)

      assert %{kind: :item, rev: rev} =
               Enum.find(changes, &(&1.resource_id == item_resource("one")))

      assert rev == item["rev"]

      # The write's result is what a read now returns.
      assert {:ok, %{"item" => ^item}} = Reads.item(relay, "reader", one)
      GenServer.stop(relay)
    end

    test "an unknown or another person's item is not_found", %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      one = item_id(relay, "one")
      assert {:error, "not_found", nil, false} = Writes.seen(relay, "reader", "i999", key())
      assert {:error, "not_found", nil, false} = Writes.seen(relay, "stranger", one, key())
      assert {:error, "unauthorized", nil, false} = Writes.seen(relay, "nobody", one, key())
      GenServer.stop(relay)
    end

    test "with client_api_v2 off there is nothing to write", %{path: path} do
      relay = scripted_relay(path, nil)
      engine = :sys.get_state(relay).store.engine
      record = engine.messages[{"A", "app:test", "one"}]
      one = AttentionServer.ClientAPI.Resources.item_id(record)

      assert {:error, "not_found", nil, false} = Writes.seen(relay, "reader", one, key())
      assert :sys.get_state(relay).store.engine == engine
      GenServer.stop(relay)
    end
  end

  describe "Idempotency-Key" do
    test "a repeat with the same key and body returns the stored response and applies nothing",
         %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      one = item_id(relay, "one")
      k = key()
      assert {:ok, 200, first, false} = Writes.seen(relay, "reader", one, k)
      {:ok, after_first} = head(relay)

      # The receipt is compact: what the write changed, not the rendered body.
      assert {:ok, %{status: 200, body: %{"item_id" => ^one}}} =
               AttentionServer.ClientAPI.Idempotency.lookup(:sys.get_state(relay).store, "A", k)

      assert {:ok, 200, ^first, true} = Writes.seen(relay, "reader", one, k)

      # Another change moves the item on; a replay renders it now and applies nothing.
      discuss(relay, "one", 30, "A later note")
      {:ok, moved} = head(relay)
      assert moved > after_first

      assert {:ok, 200, %{"rev" => ^moved, "item" => %{"item_id" => ^one, "seen" => true}}, true} =
               Writes.seen(relay, "reader", one, k)

      assert head(relay) == {:ok, moved}
      assert changes_after(relay, moved) == []
      GenServer.stop(relay)
    end

    test "the same key with a different request is idempotency_conflict, and nothing is applied",
         %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      one = item_id(relay, "one")
      two = item_id(relay, "two")
      k = key()
      assert {:ok, 200, _, false} = Writes.seen(relay, "reader", one, k)
      {:ok, position} = head(relay)

      assert {:error, "idempotency_conflict", nil, false} = Writes.seen(relay, "reader", two, k)
      assert head(relay) == {:ok, position}
      assert {:ok, %{"item" => %{"seen" => false}}} = Reads.item(relay, "reader", two)
      GenServer.stop(relay)
    end

    test "keys are scoped per person: every client of the person shares them, others do not",
         %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      browser = with_owner(relay)
      one = item_id(relay, "one")
      two = item_id(relay, "two")
      k = key()

      # A device's key, resent by the same person's browser: the stored response.
      assert {:ok, 200, first, false} = Writes.seen(relay, "reader", one, k)
      assert {:ok, 200, replayed, true} = Writes.seen(relay, browser, one, k)
      # Rendered for the browser that asked, which sees knocks.
      assert Map.delete(replayed["item"], "as") == Map.delete(first["item"], "as")
      assert {:error, "idempotency_conflict", nil, false} = Writes.seen(relay, browser, two, k)

      # The same key is new for another person.
      assert {:error, "not_found", nil, false} = Writes.seen(relay, "stranger", one, k)
      GenServer.stop(relay)
    end

    test "a write by a browser and one by a device of the same person see the same resources",
         %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      browser = with_owner(relay)
      one = item_id(relay, "one")
      two = item_id(relay, "two")

      assert {:ok, 200, %{"item" => from_browser}, false} =
               Writes.seen(relay, browser, one, key())

      assert {:ok, %{"item" => from_device}} = Reads.item(relay, "reader", one)
      assert Map.delete(from_browser, "as") == Map.delete(from_device, "as")
      assert from_device["seen"]

      assert {:ok, 200, %{"item" => by_device}, false} = Writes.seen(relay, "reader", two, key())
      assert {:ok, %{"item" => seen_by_browser}} = Reads.item(relay, browser, two)
      assert Map.delete(by_device, "as") == Map.delete(seen_by_browser, "as")

      # A browser whose owner revision moved on may not write.
      assert {:error, "unauthorized", nil, false} =
               Writes.seen(relay, %{browser | owner_revision: 3}, one, key())

      GenServer.stop(relay)
    end

    test "receipts survive a restart", %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      one = item_id(relay, "one")
      k = key()
      assert {:ok, 200, first, false} = Writes.seen(relay, "reader", one, k)
      GenServer.stop(relay)

      relay = start(path, flags)
      assert {:ok, 200, ^first, true} = Writes.seen(relay, "reader", one, k)
      GenServer.stop(relay)
    end
  end

  defp agent_reply(relay, id, operation, to, text) do
    assert %{"type" => "ok", "seq" => seq} =
             request(relay, "origin", %{
               "type" => "conversation_reply",
               "op_id" => op(operation),
               "sender_id" => "app:test",
               "message_id" => id,
               "request_revision" => 1,
               "in_reply_to" => to,
               "text" => text
             })

    AttentionServer.ClientAPI.Resources.entry_id(seq)
  end

  defp person_note(relay, id, operation, text) do
    assert %{"type" => "ok", "event_id" => event, "seq" => seq} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(operation),
               "sender_id" => "app:test",
               "message_id" => id,
               "request_revision" => 1,
               "text" => text
             })

    {String.to_integer(event), AttentionServer.ClientAPI.Resources.entry_id(seq)}
  end

  describe "POST /v2/items/{item_id}/replies" do
    test "a reply returns the item and the new entry, and emits the entry's change row",
         %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      one = item_id(relay, "one")
      {:ok, before} = head(relay)

      assert {:ok, 200, body, false} =
               Writes.reply(relay, "reader", one, "Can you say more?", key())

      assert Schema.errors(Schema.contract_defs(@contract), "WriteResult", body) == []
      assert %{"rev" => position, "item" => %{"item_id" => ^one}, "entry" => entry} = body

      assert %{"item_id" => ^one, "kind" => "reply", "author" => "person"} = entry
      assert entry["text"] == "Can you say more?"
      assert entry["rev"] > before and entry["rev"] <= position

      {:ok, seq} = AttentionServer.ClientAPI.Resources.entry_seq(entry["entry_id"])
      changes = changes_after(relay, before)
      assert Enum.any?(changes, &(&1.resource_id == ChangeLog.entry_resource(seq)))

      assert {:ok, %{"entries" => entries}} = Reads.entries(relay, "reader", one)
      assert List.last(entries) == entry
      GenServer.stop(relay)
    end

    test "replies are accepted in every item state, to an agent that is not listening",
         %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)

      for {id, n} <- [{"answered", 40}, {"cleared", 41}, {"retracted", 42}],
          do: forward(relay, id, n)

      assert %{"type" => "ok"} =
               request(relay, "reader", %{
                 "type" => "answer",
                 "op_id" => op(50),
                 "sender_id" => "app:test",
                 "message_id" => "answered",
                 "request_revision" => 1,
                 "answer" => nil
               })

      assert %{"type" => "ok"} =
               request(relay, "origin", %{
                 "type" => "lifecycle",
                 "op_id" => op(51),
                 "sender_id" => "app:test",
                 "message_id" => "cleared",
                 "state" => "cleared",
                 "at" => 5
               })

      assert %{"type" => "ok", "state" => "retracted"} =
               request(relay, "origin", %{
                 "type" => "conversation_withdraw",
                 "op_id" => op(52),
                 "sender_id" => "app:test",
                 "message_id" => "retracted"
               })

      # The agent stops listening; chat stays open and the reply is held for it.
      assert :ok = Relay.origin_session_gone(relay, "origin", @handle)

      for id <- ["one", "answered", "cleared", "retracted"] do
        item = item_id(relay, id)
        assert {:ok, %{"item" => before}} = Reads.item(relay, "reader", item)
        assert before["availability"]["state"] in ["not_listening", "gone", "unsupported"]

        assert {:ok, 200, %{"entry" => %{"text" => "Still there?"}}, false} =
                 Writes.reply(relay, "reader", item, "Still there?", key())
      end

      GenServer.stop(relay)
    end

    test "a repeated reply is replayed, not posted twice; files wait for uploads",
         %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      one = item_id(relay, "one")
      k = key()
      assert {:ok, 200, first, false} = Writes.reply(relay, "reader", one, "Once", k)
      assert {:ok, 200, ^first, true} = Writes.reply(relay, "reader", one, "Once", k)

      assert {:error, "idempotency_conflict", nil, false} =
               Writes.reply(relay, "reader", one, "Twice", k)

      assert {:ok, %{"entries" => entries}} = Reads.entries(relay, "reader", one)
      assert Enum.count(entries, &(&1["text"] == "Once")) == 1

      assert {:error, "invalid_field", "file_ids", false} =
               Writes.reply(relay, "reader", one, "See file", ["fs-unknown"], key())

      assert {:error, "invalid_field", "text", false} =
               Writes.reply(relay, "reader", one, "", key())

      # An item sent with no session has no conversation: unsupported, not
      # not_found (contract rulings round 13), and nothing is stored.
      assert %{"type" => "ok"} =
               request(relay, "origin", %{
                 "type" => "forward",
                 "op_id" => op(80),
                 "message" => %{
                   "sender_id" => "app:test",
                   "message_id" => "no-chat",
                   "created_at" => 80,
                   "title" => "No chat"
                 }
               })

      no_chat = item_id(relay, "no-chat")

      assert {:ok, %{"item" => %{"chat" => %{"available" => false}}}} =
               Reads.item(relay, "reader", no_chat)

      assert {:error, "unsupported", nil, false} =
               Writes.reply(relay, "reader", no_chat, "Hi", key())

      # Today's refusals keep their tokens: the item's own origin is not its reader.
      assert {:error, "forbidden", nil, false} = Writes.reply(relay, "origin", one, "Me", key())

      GenServer.stop(relay)
    end
  end

  describe "PUT /v2/items/{item_id}/read-position" do
    test "max-wins by position, through the newest agent reply at or before the entry",
         %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      one = item_id(relay, "one")
      {note, note_entry} = person_note(relay, "one", 60, "Question")
      first = agent_reply(relay, "one", 61, note, "First answer")
      second = agent_reply(relay, "one", 62, note, "Second answer")
      {_, later_note} = person_note(relay, "one", 63, "Thanks")

      assert {:ok, %{"item" => %{"chat" => chat}}} = Reads.item(relay, "reader", one)
      assert chat["read_position"] == nil

      assert {:ok, 200, %{"item" => %{"chat" => %{"read_position" => ^first}}} = body, false} =
               Writes.read_position(relay, "reader", one, first)

      assert body["rev"] >= body["item"]["rev"]

      # At or below the current position: 200 with the unchanged item, nothing written.
      {:ok, position} = head(relay)

      for entry <- [first, note_entry] do
        assert {:ok, 200, %{"rev" => ^position, "item" => item}, false} =
                 Writes.read_position(relay, "reader", one, entry)

        assert item["chat"]["read_position"] == first
      end

      assert head(relay) == {:ok, position}

      # A later entry of the person's own reads through the newest agent reply before it.
      assert {:ok, 200, %{"item" => %{"chat" => %{"read_position" => ^second}}}, false} =
               Writes.read_position(relay, "reader", one, later_note, key())

      assert {:ok, %{"item" => %{"chat" => %{"read_position" => ^second}}}} =
               Reads.item(relay, "reader", one)

      GenServer.stop(relay)
    end

    test "an entry of another item, or no entry, is invalid_field", %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      one = item_id(relay, "one")
      two = item_id(relay, "two")
      {note, _} = person_note(relay, "one", 70, "Question")
      reply = agent_reply(relay, "one", 71, note, "Answer")

      assert {:error, "invalid_field", "entry_id", false} =
               Writes.read_position(relay, "reader", two, reply)

      for bad <- ["e999", "x1", "e0"] do
        assert {:error, "invalid_field", "entry_id", false} =
                 Writes.read_position(relay, "reader", one, bad)
      end

      GenServer.stop(relay)
    end
  end

  defp forward_message(relay, operation, message, files \\ nil) do
    frame = %{
      "type" => "forward",
      "op_id" => op(operation),
      "session_handle" => @handle,
      "message" =>
        Map.merge(
          %{"sender_id" => "app:test", "created_at" => operation, "title" => "Q"},
          message
        )
    }

    frame = if files, do: Map.put(frame, "files", files), else: frame
    assert %{"type" => "ok"} = request(relay, "origin", frame)
  end

  @choices %{
    "schema" => "attention-client.actions/1",
    "actions" => [
      %{"id" => "yes", "label" => "Yes", "kind" => "value", "value" => %{"pick" => "yes"}},
      %{"id" => "reply", "label" => "Reply", "kind" => "text"}
    ]
  }

  @seen %{
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

  # The known-valid UI/1 screen of message_fields_relay_test, with its image.
  @form %{
    "schema" => "attention-client.ui/1",
    "requires" => ["scene/1", "result/1", "illustration/1", "response/1", "image/1"],
    "illustrations" => [
      %{
        "id" => "cover",
        "label" => "Cover",
        "width" => 100,
        "height" => 100,
        "primitives" => [
          %{
            "kind" => "image",
            "x" => 0,
            "y" => 0,
            "width" => 100,
            "height" => 100,
            "file" => "cover.png"
          }
        ]
      }
    ],
    "inputs" => [
      %{
        "kind" => "response",
        "id" => "cover-choice",
        "label" => "Choose a cover",
        "actions" => [
          %{
            "id" => "blue",
            "label" => "Blue cover",
            "kind" => "value",
            "value" => "blue",
            "illustration" => "cover"
          }
        ]
      }
    ],
    "open" => %{"kind" => "input", "id" => "cover-choice", "layout" => "cards"},
    "resolved" => %{"kind" => "text", "text" => "Recorded", "style" => "body"},
    "submission" => %{
      "label" => "Send answer",
      "result" => %{"kind" => "input", "id" => "cover-choice"}
    }
  }

  defp stored_terminal(relay, id),
    do: :sys.get_state(relay).store.engine.messages[{"A", "app:test", id}].terminal

  describe "POST /v2/items/{item_id}/answer" do
    test "a choice stores its action's value; the first answer settles the request",
         %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)

      forward_message(relay, 40, %{
        "message_id" => "pick",
        "message_type" => "request",
        "answer_ui" => @choices
      })

      pick = item_id(relay, "pick")
      {:ok, before} = head(relay)
      k = key()

      assert {:ok, 200, body, false} =
               Writes.answer(relay, "reader", pick, 1, {:choice, "yes"}, k)

      assert Schema.errors(Schema.contract_defs(@contract), "WriteResult", body) == []

      assert %{"rev" => position, "item" => %{"terminal" => %{"state" => "answered"}} = item} =
               body

      assert item["rev"] > before and item["rev"] <= position
      assert stored_terminal(relay, "pick").answer == %{"pick" => "yes"}
      assert stored_terminal(relay, "pick").answer_path == "choice"
      assert Enum.any?(changes_after(relay, before), &(&1.resource_id == item_resource("pick")))

      # The same request again is the stored response; another answer is refused.
      assert {:ok, 200, ^body, true} =
               Writes.answer(relay, "reader", pick, 1, {:choice, "yes"}, k)

      assert {:error, "already_answered", nil, false} =
               Writes.answer(relay, "reader", pick, 1, {:comment, "Actually no"}, key())

      assert {:error, "already_answered", nil, false} =
               Writes.answer(relay, "reader", pick, 1, :no_comment, key())

      # Chat stays open after the answer.
      assert {:ok, 200, _, false} = Writes.reply(relay, "reader", pick, "One more thing", key())
      GenServer.stop(relay)
    end

    test "a comment is a plain-text answer on any answerable item", %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)

      forward_message(relay, 40, %{
        "message_id" => "pick",
        "message_type" => "request",
        "answer_ui" => @choices
      })

      for id <- ["one", "pick"] do
        assert {:ok, 200, %{"item" => item}, false} =
                 Writes.answer(
                   relay,
                   "reader",
                   item_id(relay, id),
                   1,
                   {:comment, "Sounds good"},
                   key()
                 )

        assert item["terminal"]["answer"] == %{"kind" => "reply", "text" => "Sounds good"}
        assert stored_terminal(relay, id).answer_path == "comment"
      end

      # A plain-text answer on a UI/1 screen stays distinguishable from a string result.
      for {id, n, value, path} <- [
            {"form-text", 44, {:comment, "blue"}, "comment"},
            {"form-string", 45, {:result, "blue"}, "result"}
          ] do
        forward_message(
          relay,
          n,
          %{"message_id" => id, "message_type" => "request", "answer_ui" => @form},
          [%{"name" => "cover.png", "data_base64" => Base.encode64("not image bytes")}]
        )

        assert {:ok, 200, _, false} =
                 Writes.answer(relay, "reader", item_id(relay, id), 1, value, key())

        assert %{answer: "blue", answer_path: ^path} = stored_terminal(relay, id)
      end

      # Today's answer frame records no path: its terminal keeps today's shape.
      assert %{"type" => "ok"} =
               request(relay, "reader", %{
                 "type" => "answer",
                 "op_id" => op(46),
                 "sender_id" => "app:test",
                 "message_id" => "two",
                 "answer" => "Yes"
               })

      refute Map.has_key?(stored_terminal(relay, "two"), :answer_path)

      GenServer.stop(relay)
    end

    test "no member is no comment", %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      one = item_id(relay, "one")

      assert {:ok, 200, %{"item" => item}, false} =
               Writes.answer(relay, "reader", one, 1, :no_comment, key())

      assert item["terminal"]["answer"] == %{"kind" => "no_comment"}
      GenServer.stop(relay)
    end

    test "a refused request does not consume its key: the same request is evaluated again",
         %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      one = item_id(relay, "one")
      k = key()

      assert {:error, "stale_request", nil, false} =
               Writes.answer(relay, "reader", one, 2, {:comment, "Yes"}, k)

      assert {:ok, nil} =
               AttentionServer.ClientAPI.Idempotency.lookup(:sys.get_state(relay).store, "A", k)

      # The request moves to revision 2; the same request under the same key now applies.
      message = :sys.get_state(relay).store.engine.messages[{"A", "app:test", "one"}].message

      assert %{"type" => "ok"} =
               request(relay, "origin", %{
                 "type" => "conversation_revise",
                 "op_id" => op(45),
                 "sender_id" => "app:test",
                 "message_id" => "one",
                 "base_revision" => 1,
                 "message" => Map.put(message, "title", "Revised")
               })

      assert {:ok, 200, %{"item" => %{"terminal" => %{"state" => "answered"}}} = body, false} =
               Writes.answer(relay, "reader", one, 2, {:comment, "Yes"}, k)

      assert {:ok, 200, ^body, true} =
               Writes.answer(relay, "reader", one, 2, {:comment, "Yes"}, k)

      GenServer.stop(relay)
    end

    test "a stale request_revision is stale_request, and nothing is applied",
         %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      one = item_id(relay, "one")
      {:ok, position} = head(relay)

      for value <- [{:comment, "Yes"}, :no_comment, {:choice, "yes"}] do
        assert {:error, "stale_request", nil, false} =
                 Writes.answer(relay, "reader", one, 2, value, key())
      end

      assert head(relay) == {:ok, position}
      assert stored_terminal(relay, "one") == nil
      GenServer.stop(relay)
    end

    test "choice and result must match the item's answer screen", %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)

      forward_message(relay, 40, %{
        "message_id" => "pick",
        "message_type" => "request",
        "answer_ui" => @choices
      })

      pick = item_id(relay, "pick")
      one = item_id(relay, "one")

      for {item, value, field} <- [
            {pick, {:choice, "nope"}, "choice"},
            {pick, {:choice, "reply"}, "choice"},
            {one, {:choice, "yes"}, "choice"},
            {pick, {:result, %{"a" => 1}}, "result"},
            {one, {:result, %{"a" => 1}}, "result"}
          ] do
        assert {:error, "invalid_field", ^field, false} =
                 Writes.answer(relay, "reader", item, 1, value, key())
      end

      # A UI/1 screen takes a result, stored as today's answer value.
      forward_message(
        relay,
        41,
        %{
          "message_id" => "form",
          "message_type" => "request",
          "answer_ui" => @form
        },
        [%{"name" => "cover.png", "data_base64" => Base.encode64("not image bytes")}]
      )

      form = item_id(relay, "form")

      assert {:ok, %{"item" => %{"message" => %{"answer_ui" => %{"schema" => ui}}}}} =
               Reads.item(relay, "reader", form)

      assert ui == "attention-client.ui/1"

      assert {:ok, 200, %{"item" => %{"terminal" => %{"state" => "answered"}}}, false} =
               Writes.answer(relay, "reader", form, 1, {:result, %{"choice" => "blue"}}, key())

      assert stored_terminal(relay, "form").answer == %{"choice" => "blue"}
      assert stored_terminal(relay, "form").answer_path == "result"

      # Any JSON value is a result, null included, stored unchanged (round 16).
      for {id, n, result} <- [{"form-null", 42, nil}, {"form-list", 43, ["blue", 2, true]}] do
        forward_message(
          relay,
          n,
          %{"message_id" => id, "message_type" => "request", "answer_ui" => @form},
          [%{"name" => "cover.png", "data_base64" => Base.encode64("not image bytes")}]
        )

        assert {:ok, 200, %{"item" => %{"terminal" => %{"state" => "answered"}}}, false} =
                 Writes.answer(relay, "reader", item_id(relay, id), 1, {:result, result}, key())

        assert stored_terminal(relay, id).answer == result
      end

      GenServer.stop(relay)
    end

    test "an FYI's Seen is its answer, as today; a retracted item is already_retracted",
         %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)

      forward_message(relay, 40, %{
        "message_id" => "note",
        "message_type" => "fyi",
        "answer_ui" => @seen
      })

      note = item_id(relay, "note")

      assert {:ok, 200, %{"item" => %{"terminal" => %{"state" => "answered"}}}, false} =
               Writes.answer(relay, "reader", note, 1, {:choice, "seen"}, key())

      assert stored_terminal(relay, "note").answer == %{"acknowledgment" => "seen"}

      for value <- [{:choice, "seen"}, {:comment, "ok"}, :no_comment] do
        assert {:error, "already_answered", nil, false} =
                 Writes.answer(relay, "reader", note, 1, value, key())
      end

      assert %{"type" => "ok", "state" => "retracted"} =
               request(relay, "origin", %{
                 "type" => "conversation_withdraw",
                 "op_id" => op(41),
                 "sender_id" => "app:test",
                 "message_id" => "two"
               })

      assert {:error, "already_retracted", nil, false} =
               Writes.answer(relay, "reader", item_id(relay, "two"), 1, {:comment, "Hi"}, key())

      GenServer.stop(relay)
    end
  end

  @levels [
    %{"name" => "Low", "lower_bound" => 0, "bounce_after_days" => nil},
    %{"name" => "High", "lower_bound" => 0.5, "bounce_after_days" => 2}
  ]

  defp settings_etag(relay, who) do
    {:ok, %{"settings" => settings}} = Reads.settings(relay, who)
    Reads.etag(settings)
  end

  describe "PUT /v2/settings/priority-levels" do
    test "If-Match with the Settings ETag replaces the levels; a stale one is 412",
         %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      held = settings_etag(relay, "reader")
      {:ok, before} = head(relay)

      assert {:ok, 200, body, false} = Writes.priority_levels(relay, "reader", @levels, held)

      # Checked in its wire form: stored decimals encode exactly as sent.
      wire = Elixir.JSON.decode!(JSON.encode!(body))
      assert Schema.errors(Schema.contract_defs(@contract), "WriteResult", wire) == []
      assert Enum.map(wire["settings"]["priority_levels"], & &1["lower_bound"]) == [0, 0.5]
      assert %{"rev" => position, "settings" => settings} = body
      assert Enum.map(settings["priority_levels"], & &1["name"]) == ["Low", "High"]
      assert settings["rev"] > before and settings["rev"] <= position
      assert Enum.any?(changes_after(relay, before), &(&1.resource_id == "settings"))

      # The write's result is what GET /v2/settings now reads, with the same ETag.
      assert {:ok, %{"settings" => ^settings}} = Reads.settings(relay, "reader")
      assert settings_etag(relay, "reader") == Reads.etag(settings)

      # The ETag the client held is stale now: nothing is applied.
      {:ok, position} = head(relay)

      assert {:error, "precondition_failed", nil, false} =
               Writes.priority_levels(relay, "reader", Enum.take(@levels, 1), held)

      assert head(relay) == {:ok, position}

      assert {:ok, 200, _, false} =
               Writes.priority_levels(relay, "reader", Enum.take(@levels, 1), "*")

      GenServer.stop(relay)
    end

    test "invalid levels are invalid_field", %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      tag = settings_etag(relay, "reader")

      for levels <- [[], [%{"name" => "x"}], Enum.reverse(@levels)] do
        assert {:error, "invalid_field", "levels", false} =
                 Writes.priority_levels(relay, "reader", levels, tag)
      end

      GenServer.stop(relay)
    end
  end

  @token String.duplicate("ab", 32)

  describe "/v2/devices/me/push" do
    test "PUT registers the device's binding, DELETE removes it; a stale revision is 412",
         %{path: path, flags: flags} do
      relay = scripted_relay(path, flags, push_enabled: true)

      assert {:ok, 200, body, false} = Writes.push(relay, "reader", @token, "production", 1)
      assert Schema.errors(Schema.contract_defs(@contract), "WriteResult", body) == []
      assert %{"revision" => 1, "environment" => "production"} = body["settings"]["push"]

      assert {:error, "precondition_failed", nil, false} =
               Writes.push(relay, "reader", String.duplicate("cd", 32), "sandbox", 1)

      assert {:ok, 200, %{"settings" => %{"push" => push}}, false} =
               Writes.remove_push(relay, "reader")

      assert %{"state" => "canceled", "revision" => 2} = push
      refute Map.has_key?(push, "environment")

      # Another device's binding is its own.
      assert {:ok, %{"settings" => %{"push" => %{"state" => "unconfigured"}}}} =
               Reads.settings(relay, "origin")

      GenServer.stop(relay)
    end

    test "bad fields keep the client API's names", %{path: path, flags: flags} do
      relay = scripted_relay(path, flags, push_enabled: true)

      assert {:error, "invalid_field", "token", false} =
               Writes.push(relay, "reader", "not-a-token", "production", 1)

      GenServer.stop(relay)
    end

    test "browsers are forbidden, and push disabled is unsupported", %{path: path, flags: flags} do
      relay = scripted_relay(path, flags, push_enabled: true)
      browser = with_owner(relay)

      assert {:error, "forbidden", nil, false} =
               Writes.push(relay, browser, @token, "production", 1)

      assert {:error, "forbidden", nil, false} = Writes.remove_push(relay, browser)
      GenServer.stop(relay)

      relay = start(path, flags)

      assert {:error, "unsupported", nil, false} =
               Writes.push(relay, "reader", @token, "production", 1)

      GenServer.stop(relay)
    end
  end

  describe "the Content-Type sent with POST /v2/files" do
    test "is kept on the reply's upload and served back, across a restart",
         %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      one = item_id(relay, "one")

      assert {201, %{"file_id" => file_id}, _} =
               send_file(relay, "png bytes", [{"idempotency-key", key()}])

      assert {:ok, 200, %{"entry" => %{"files" => [%{"file_id" => served}]}}, false} =
               Writes.reply(relay, "reader", one, "A picture", [file_id], key())

      assert {:ok, %{content_type: "image/png"}} = Reads.file(relay, "reader", served)
      GenServer.stop(relay)

      # The store reopens and validates the record with the extra key.
      relay = start(path, flags)
      assert {:ok, %{content_type: "image/png"}} = Reads.file(relay, "reader", served)
      GenServer.stop(relay)
    end

    test "today's upload paths keep their record shape and serve octet-stream",
         %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      bytes = "legacy bytes"
      hash = Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
      source = Path.join(Path.dirname(path), "legacy.bin")
      File.write!(source, bytes)

      # Today's attributes are decoded JSON: a client-sent "content_type" is not kept.
      attrs = %{
        "op_id" => op(90),
        "sender_id" => "app:test",
        "message_id" => "one",
        "revision" => 1,
        "name" => "legacy.bin",
        "content_type" => "text/html"
      }

      assert {:ok, lease} = Relay.upload_begin(relay, "reader", attrs, byte_size(bytes))

      assert {:ok, %{"upload_id" => upload_id}} =
               Relay.upload_file(relay, "reader", attrs, source, byte_size(bytes), hash, lease)

      assert :ok = Relay.upload_end(relay, lease)

      record = :sys.get_state(relay).store.engine.uploads[upload_id]
      refute Map.has_key?(record, :content_type)

      assert Enum.sort(Map.keys(record)) ==
               Enum.sort(
                 ~w(id person uploader uploader_kind op_id sender_id message_id revision name size sha256 last_activity_ms)a
               )

      # Referenced by today's discussion frame, it serves with no stored type.
      assert %{"type" => "ok"} =
               request(relay, "reader", %{
                 "type" => "discussion",
                 "op_id" => op(90),
                 "sender_id" => "app:test",
                 "message_id" => "one",
                 "request_revision" => 1,
                 "text" => "Legacy file",
                 "files" => [
                   %{
                     "name" => "legacy.bin",
                     "upload_id" => upload_id,
                     "size" => byte_size(bytes),
                     "sha256" => hash
                   }
                 ]
               })

      assert {:ok, %{content_type: "application/octet-stream"}} =
               Reads.file(relay, "reader", "fu" <> upload_id)

      GenServer.stop(relay)
    end
  end

  # Every write's change is on the one write path: replaying the rendered log
  # from empty, with the version rule, reaches what the snapshot reads show.
  test "after every kind of write, replaying the log from empty equals the snapshot",
       %{path: path, flags: flags} do
    relay = scripted_relay(path, flags, push_enabled: true)

    forward_message(relay, 40, %{
      "message_id" => "pick",
      "message_type" => "request",
      "answer_ui" => @choices
    })

    one = item_id(relay, "one")
    two = item_id(relay, "two")
    pick = item_id(relay, "pick")
    {note, _} = person_note(relay, "one", 41, "Question")
    agent = agent_reply(relay, "one", 42, note, "Answer")

    assert {201, %{"file_id" => file_id}, _} =
             send_file(relay, "bytes", [{"idempotency-key", key()}])

    assert {:ok, 200, _, false} = Writes.seen(relay, "reader", two, key())
    # The read position is moved by another device of the person (round 12).
    assert :ok = Relay.setup_device(relay, "A", "second")
    assert {:ok, 200, _, false} = Writes.read_position(relay, "second", one, agent)

    assert {:ok, 200, _, false} =
             Writes.reply(relay, "reader", one, "With a file", [file_id], key())

    assert {:ok, 200, _, false} = Writes.answer(relay, "reader", pick, 1, {:choice, "yes"}, key())
    assert {:ok, 200, _, false} = Writes.answer(relay, "reader", two, 1, :no_comment, key())

    assert {:ok, 200, _, false} =
             Writes.priority_levels(relay, "reader", @levels, settings_etag(relay, "reader"))

    assert {:ok, 200, _, false} = Writes.push(relay, "reader", @token, "production", 1)

    state = :sys.get_state(relay)
    view = Map.get(state.priority_views, "A")
    context = %{engine: state.store.engine, store: state.store, view: view}
    assert {:ok, rows, false, head} = ChangeLog.read(state.store, "A", 0, 1000)

    applied =
      Enum.reduce(rows, %{}, fn row, acc ->
        case FeedRenderer.render(context, "reader", row) do
          {:ok, data} ->
            case row.kind do
              :item -> Map.put(acc, {:item, data["item_id"]}, data)
              :entry -> Map.put(acc, {:entry, data["entry_id"]}, data)
              kind when kind in [:counts, :settings] -> Map.put(acc, kind, data)
              _ -> acc
            end

          :skip ->
            acc
        end
      end)

    assert {:ok, inbox} = Reads.inbox(relay, "reader")
    assert inbox["rev"] == head
    assert applied[:counts] == inbox["counts"]
    assert applied[{:item, one}]["chat"]["read_position"] == agent

    for item <- inbox["items"] do
      assert applied[{:item, item["item_id"]}] == item
      assert {:ok, %{"entries" => entries}} = Reads.entries(relay, "reader", item["item_id"])
      for entry <- entries, do: assert(applied[{:entry, entry["entry_id"]}] == entry)
    end

    assert {:ok, %{"settings" => settings}} = Reads.settings(relay, "reader")
    assert applied[:settings] == settings
    GenServer.stop(relay)
  end

  describe "HTTP" do
    defp post(relay, path_info, body, headers, method \\ "POST", principal \\ nil) do
      conn =
        Plug.Test.conn(method, "/v2/" <> Enum.join(path_info, "/"), body)
        |> Plug.Conn.assign(:client_api_request_id, "req-1")

      conn = Enum.reduce(headers, conn, fn {k, v}, c -> Plug.Conn.put_req_header(c, k, v) end)

      ctx = %Context{
        principal: principal || %Principal{person: "A", kind: :device, key: "reader"},
        relay: relay,
        request_id: "req-1",
        params: %{item_id: Enum.at(path_info, 1)}
      }

      handler =
        case {method, List.last(path_info)} do
          {_, "seen"} -> :post_seen
          {_, "answer"} -> :post_answer
          {_, "replies"} -> :post_replies
          {_, "read-position"} -> :put_read_position
          {_, "priority-levels"} -> :put_priority_levels
          {"PUT", "push"} -> :put_push
          {"DELETE", "push"} -> :delete_push
          {_, "files"} -> :post_files
        end

      conn = apply(Writes, handler, [conn, ctx])
      {conn.status, JSON.decode_line(conn.resp_body <> "\n") |> elem(1), conn}
    end

    test "seen needs an Idempotency-Key, refuses unknown members and marks replays",
         %{path: path, flags: flags} do
      relay = scripted_relay(path, flags)
      one = item_id(relay, "one")
      route = ["items", one, "seen"]
      k = key()

      assert {400, %{"error" => "invalid_field", "field" => "Idempotency-Key"}, _} =
               post(relay, route, "{}", [])

      assert {400, %{"error" => "invalid_field", "field" => "Idempotency-Key"}, _} =
               post(relay, route, "{}", [{"idempotency-key", "not-a-uuid"}])

      assert {400, %{"error" => "invalid_field", "field" => "extra"}, _} =
               post(relay, route, ~s({"extra":1}), [{"idempotency-key", k}])

      assert {400, %{"error" => "invalid_field"}, _} =
               post(relay, route, "[]", [{"idempotency-key", k}])

      assert {200, first, conn} = post(relay, route, "{}", [{"idempotency-key", k}])
      assert Plug.Conn.get_resp_header(conn, "idempotent-replayed") == []
      assert first["item"]["seen"]

      # An empty body is the same request as {}.
      assert {200, ^first, conn} = post(relay, route, "", [{"idempotency-key", k}])
      assert Plug.Conn.get_resp_header(conn, "idempotent-replayed") == ["true"]

      assert {409, %{"error" => "idempotency_conflict", "retryable" => false}, _} =
               post(relay, ["items", item_id(relay, "two"), "seen"], "{}", [
                 {"idempotency-key", k}
               ])

      GenServer.stop(relay)
    end
  end

  test "priority levels need If-Match; push bodies are checked; browsers get 403",
       %{path: path, flags: flags} do
    relay = scripted_relay(path, flags, push_enabled: true)
    levels = JSON.encode!(%{"levels" => @levels})

    assert {400, %{"error" => "invalid_field", "field" => "If-Match"}, _} =
             post(relay, ["settings", "priority-levels"], levels, [], "PUT")

    assert {412, %{"error" => "precondition_failed"}, _} =
             post(relay, ["settings", "priority-levels"], levels, [{"if-match", ~s("x")}], "PUT")

    tag = settings_etag(relay, "reader")

    assert {200, %{"settings" => _}, _} =
             post(relay, ["settings", "priority-levels"], levels, [{"if-match", tag}], "PUT")

    for {body, field} <- [
          {~s({"token":"#{@token}","environment":"staging","revision":1}), "environment"},
          {~s({"token":"#{@token}","environment":"production","revision":0}), "revision"},
          {~s({"environment":"production","revision":1}), "token"}
        ] do
      assert {400, %{"error" => "invalid_field", "field" => ^field}, _} =
               post(relay, ["devices", "me", "push"], body, [], "PUT")
    end

    assert {200, %{"settings" => %{"push" => %{"revision" => 1}}}, _} =
             post(
               relay,
               ["devices", "me", "push"],
               ~s({"token":"#{@token}","environment":"sandbox","revision":1}),
               [],
               "PUT"
             )

    assert {200, %{"settings" => %{"push" => %{"state" => "canceled"}}}, _} =
             post(relay, ["devices", "me", "push"], "", [], "DELETE")

    GenServer.stop(relay)
  end

  defp send_file(relay, bytes, headers, principal \\ nil) do
    headers =
      [
        {"content-type", "image/png"},
        {"content-length", Integer.to_string(byte_size(bytes))},
        {"content-disposition", "attachment; filename*=UTF-8''caf%C3%A9%20plan.png"}
      ]
      |> Enum.reject(fn {name, _} -> List.keymember?(headers, name, 0) end)
      |> Kernel.++(headers)
      |> Enum.reject(fn {_, value} -> is_nil(value) end)

    post(relay, ["files"], bytes, headers, "POST", principal)
  end

  defp staged_dir(relay) do
    Path.join(:sys.get_state(relay).store.upload_dir, "client-api-v2-staged")
  end

  test "POST /v2/files stages the bytes under their name and replays by key",
       %{path: path, flags: flags} do
    relay = scripted_relay(path, flags)
    bytes = :binary.copy(<<1, 2, 3, 250>>, 1000)
    k = key()

    assert {201, created, conn} = send_file(relay, bytes, [{"idempotency-key", k}])
    assert Schema.errors(Schema.contract_defs(@contract), "FileCreated", created) == []
    assert created["name"] == "café plan.png"
    assert created["size"] == byte_size(bytes)
    assert created["sha256"] == Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
    assert Plug.Conn.get_resp_header(conn, "idempotent-replayed") == []

    assert {201, ^created, conn} = send_file(relay, bytes, [{"idempotency-key", k}])
    assert Plug.Conn.get_resp_header(conn, "idempotent-replayed") == ["true"]

    assert {409, %{"error" => "idempotency_conflict"}, _} =
             send_file(relay, "other", [{"idempotency-key", k}])

    # One staged copy, and no temporary files left behind.
    assert File.ls!(staged_dir(relay)) == [created["file_id"]]
    GenServer.stop(relay)
  end

  test "POST /v2/files needs a name, a length and room under the cap", %{path: path, flags: flags} do
    relay = scripted_relay(path, flags, upload_cap: 10)

    for {headers, field} <- [
          {[{"content-disposition", nil}], "name"},
          {[{"content-disposition", "attachment; filename=plain.png"}], "name"},
          {[{"content-disposition", "attachment; filename*=UTF-8''"}], "name"},
          {[{"content-length", nil}], "Content-Length"},
          {[{"content-length", "3"}], "Content-Length"}
        ] do
      assert {400, %{"error" => "invalid_field", "field" => ^field}, _} =
               send_file(relay, "abcd", [{"idempotency-key", key()} | headers])
    end

    assert {413, %{"error" => "payload_too_large"}, _} =
             send_file(relay, String.duplicate("x", 11), [{"idempotency-key", key()}])

    assert {400, %{"error" => "invalid_field", "field" => "Idempotency-Key"}, _} =
             send_file(relay, "abcd", [])

    assert File.ls!(staged_dir(relay)) == []
    GenServer.stop(relay)
  end

  test "a reply names staged files; they arrive as the entry's files, readable by GET",
       %{path: path, flags: flags} do
    relay = scripted_relay(path, flags)
    browser = with_owner(relay)
    one = item_id(relay, "one")
    bytes = "the plan, in bytes"

    assert {201, %{"file_id" => file_id}, _} =
             send_file(relay, bytes, [{"idempotency-key", key()}])

    k = key()

    assert {:ok, 200, %{"entry" => entry} = body, false} =
             Writes.reply(relay, "reader", one, "Here it is", [file_id], k)

    assert Schema.errors(Schema.contract_defs(@contract), "WriteResult", body) == []
    assert [%{"name" => "café plan.png", "file_id" => entry_file}] = entry["files"]
    assert {:ok, %{size: size}} = Reads.file(relay, "reader", entry_file)
    assert size == byte_size(bytes)

    assert {:ok, 200, ^body, true} =
             Writes.reply(relay, "reader", one, "Here it is", [file_id], k)

    # A browser's staged file is its person's, and only theirs.
    browser_principal = %Principal{
      person: "A",
      kind: :browser,
      key: browser.key,
      owner_revision: 4,
      session_id: "s1"
    }

    assert {201, %{"file_id" => from_browser}, _} =
             send_file(relay, "browser bytes", [{"idempotency-key", key()}], browser_principal)

    assert {:ok, 200, %{"entry" => %{"files" => [_]}}, false} =
             Writes.reply(relay, browser, one, "From the web", [from_browser], key())

    # Another person's staged file cannot be named.
    stranger = %Principal{person: "B", kind: :device, key: "stranger"}

    assert {201, %{"file_id" => theirs}, _} =
             send_file(relay, "not yours", [{"idempotency-key", key()}], stranger)

    assert {:error, "invalid_field", "file_ids", false} =
             Writes.reply(relay, "reader", one, "x", [theirs], key())

    GenServer.stop(relay)
  end

  test "answer bodies carry request_revision and at most one of choice, comment and result",
       %{path: path, flags: flags} do
    relay = scripted_relay(path, flags)
    route = ["items", item_id(relay, "one"), "answer"]

    for {body, field} <- [
          {~s({}), "request_revision"},
          {~s({"request_revision":0}), "request_revision"},
          {~s({"request_revision":1,"choice":"a","comment":"b"}), nil},
          {~s({"request_revision":1,"comment":"b","result":{}}), nil},
          {~s({"request_revision":1,"choice":""}), "choice"},
          {~s({"request_revision":1,"comment":5}), "comment"},
          {~s({"request_revision":1,"result":[]}), "result"},
          {~s({"request_revision":1,"result":null}), "result"},
          {~s({"request_revision":1,"choice":"a","result":null}), nil},
          {~s({"request_revision":1,"other":1}), "other"}
        ] do
      assert {400, error, _} = post(relay, route, body, [{"idempotency-key", key()}])
      assert error["error"] == "invalid_field"
      assert error["field"] == field
    end

    assert {200, %{"item" => %{"terminal" => %{"state" => "answered"}}}, _} =
             post(relay, route, ~s({"request_revision":1,"comment":"Yes"}), [
               {"idempotency-key", key()}
             ])

    # On a UI/1 screen a present result is the answer, whatever JSON it is (round 16).
    for {id, n, body} <- [
          {"form-null", 50, ~s({"request_revision":1,"result":null})},
          {"form-list", 51, ~s({"request_revision":1,"result":["blue",2]})}
        ] do
      forward_message(
        relay,
        n,
        %{"message_id" => id, "message_type" => "request", "answer_ui" => @form},
        [%{"name" => "cover.png", "data_base64" => Base.encode64("not image bytes")}]
      )

      assert {200, %{"item" => %{"terminal" => %{"state" => "answered"}}}, _} =
               post(relay, ["items", item_id(relay, id), "answer"], body, [
                 {"idempotency-key", key()}
               ])
    end

    GenServer.stop(relay)
  end

  defp discuss(relay, id, operation, text) do
    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(operation),
               "sender_id" => "app:test",
               "message_id" => id,
               "request_revision" => 1,
               "text" => text
             })
  end
end
