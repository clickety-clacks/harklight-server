defmodule AttentionServer.ClientAPIWritesTest do
  # Client API v2 writes and the Idempotency-Key store (docs/client-api-v2.md
  # §2 and §5, spec §4 and §10), behind the client_api_v2 flag.
  use ExUnit.Case, async: false

  alias AttentionServer.{ChangeLog, JSON, Relay}
  alias AttentionServer.ClientAPI.{Context, Principal, Reads, Writes}
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

  defp start(path, flags) do
    opts = [store_path: path, agent_fixture_mode: true]
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

  defp scripted_relay(path, flags) do
    relay = start(path, flags)
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

      # Another change moves the item on; the replay still answers the original.
      discuss(relay, "one", 30, "A later note")
      {:ok, moved} = head(relay)
      assert moved > after_first

      assert {:ok, 200, ^first, true} = Writes.seen(relay, "reader", one, k)
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
      assert {:ok, 200, ^first, true} = Writes.seen(relay, browser, one, k)
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

      assert {:error, "unsupported", "file_ids", false} =
               Writes.reply(relay, "reader", one, "See file", ["fu1"], key())

      assert {:error, "invalid_field", "text", false} =
               Writes.reply(relay, "reader", one, "", key())

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

  describe "HTTP" do
    defp post(relay, path_info, body, headers) do
      conn =
        Plug.Test.conn("POST", "/v2/" <> Enum.join(path_info, "/"), body)
        |> Plug.Conn.assign(:client_api_request_id, "req-1")

      conn = Enum.reduce(headers, conn, fn {k, v}, c -> Plug.Conn.put_req_header(c, k, v) end)

      ctx = %Context{
        principal: %Principal{person: "A", kind: :device, key: "reader"},
        relay: relay,
        request_id: "req-1",
        params: %{item_id: Enum.at(path_info, 1)}
      }

      conn = Writes.post_seen(conn, ctx)
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
