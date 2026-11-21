defmodule AttentionServer.ClientAPIResourcesTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{Engine, JSON, Placement, Relay}
  alias AttentionServer.ClientAPI.Resources
  alias AttentionServer.ClientAPISchema, as: Schema

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @contract Path.expand("../docs/client-api-v2.md", __DIR__)

  setup do
    dir =
      Path.join(System.tmp_dir!(), "harklight-client-api-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{path: Path.join(dir, "state.sqlite3")}
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp request(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  # Some person frames (answer_no_comment) carry neither "v" nor "op_id".
  defp raw_request(relay, key, frame) do
    assert {:ok, line} = Relay.request_line(relay, key, JSON.encode!(frame) <> "\n")
    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp forward(relay, id, operation, extra \\ %{}) do
    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(operation),
               "session_handle" => @handle,
               "message" =>
                 Map.merge(
                   %{
                     "sender_id" => "app:test",
                     "message_id" => id,
                     "created_at" => operation,
                     "title" => "Question #{id}",
                     "body" => "Body #{id}"
                   },
                   extra
                 )
             })
  end

  # One person with an origin device, a subscribed reader, a knocked reader and
  # a mix of open, answered, no-comment, revised, retracted and chatted items.
  defp scripted_relay(path) do
    assert {:ok, relay} = Relay.start_link(store_path: path, agent_fixture_mode: true)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_device(relay, "A", "other")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "subscribe_remote",
               "op_id" => op(1),
               "revision" => 1,
               "sender_ids" => ["app:test"]
             })

    for {id, n} <- [{"old", 2}, {"answered", 3}, {"silent", 4}, {"chat", 5}, {"revised", 6}],
        do: forward(relay, id, n)

    forward(relay, "retracted", 7)

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(10),
               "sender_id" => "app:test",
               "message_id" => "answered",
               "answer" => nil
             })

    assert %{"type" => "ok"} =
             raw_request(relay, "reader", %{
               "type" => "answer_no_comment",
               "request_id" => "silent",
               "request_revision" => 1
             })

    assert %{"type" => "ok", "event_id" => discussion_id} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(12),
               "sender_id" => "app:test",
               "message_id" => "chat",
               "request_revision" => 1,
               "text" => "Could you clarify?"
             })

    assert %{"type" => "ok", "seq" => reply_seq} =
             request(relay, "origin", %{
               "type" => "conversation_reply",
               "op_id" => op(13),
               "sender_id" => "app:test",
               "message_id" => "chat",
               "request_revision" => 1,
               "in_reply_to" => String.to_integer(discussion_id),
               "text" => "Here is more detail"
             })

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "conversation_read",
               "op_id" => op(14),
               "sender_id" => "app:test",
               "message_id" => "chat",
               "read_through_conversation_seq" => reply_seq
             })

    assert %{"type" => "ok", "opened" => true} =
             request(relay, "reader", %{
               "type" => "message_open",
               "op_id" => op(15),
               "sender_id" => "app:test",
               "message_id" => "chat",
               "request_revision" => 1
             })

    assert %{"type" => "ok", "revision" => 2} =
             request(relay, "origin", %{
               "type" => "conversation_revise",
               "op_id" => op(16),
               "sender_id" => "app:test",
               "message_id" => "revised",
               "base_revision" => 1,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "revised",
                 "created_at" => 6,
                 "title" => "Question revised, again",
                 "body" => "Body revised"
               }
             })

    assert %{"type" => "ok", "state" => "retracted"} =
             request(relay, "origin", %{
               "type" => "conversation_withdraw",
               "op_id" => op(17),
               "sender_id" => "app:test",
               "message_id" => "retracted"
             })

    relay
  end

  defp snapshot(relay) do
    state = :sys.get_state(relay)
    {state.store.engine, Map.get(state.priority_views, "A")}
  end

  defp key(id), do: {"A", "app:test", id}

  defp rendered_items(engine, view, principal_key) do
    {:ok, principal} = Resources.principal(engine, principal_key)
    ranks = Resources.ranks(engine, "A", view)

    engine
    |> Resources.item_keys("A")
    |> Enum.map(fn key ->
      {:ok, item_id} = Resources.item_id(engine, key)

      {key,
       engine
       |> Resources.item(key, Map.fetch!(ranks, item_id))
       |> Resources.stamp(1)
       |> then(&Resources.render(:item, &1, principal))}
    end)
    |> Map.new()
  end

  describe "schema conformance with docs/client-api-v2.md" do
    test "every rendered Item, Settings and Counts matches the contract exactly",
         %{path: path} do
      defs = Schema.contract_defs(@contract)
      relay = scripted_relay(path)
      {engine, view} = snapshot(relay)

      for principal_key <- ["reader", "other", "origin"] do
        {:ok, principal} = Resources.principal(engine, principal_key)
        items = rendered_items(engine, view, principal_key)
        assert map_size(items) == 6

        for {_key, item} <- items do
          assert {item["item_id"], Schema.errors(defs, "Item", item)} == {item["item_id"], []}
        end

        settings =
          engine
          |> Resources.settings("A")
          |> Resources.stamp(1)
          |> then(&Resources.render(:settings, &1, principal))

        assert Schema.errors(defs, "Settings", settings) == []
        assert Schema.errors(defs, "Counts", Resources.counts(engine, "A")) == []
      end

      GenServer.stop(relay)
    end

    test "every rendered Entry matches the contract exactly", %{path: path} do
      defs = Schema.contract_defs(@contract)
      relay = scripted_relay(path)
      {engine, _view} = snapshot(relay)
      {:ok, principal} = Resources.principal(engine, "reader")

      entries =
        for key <- Resources.item_keys(engine, "A"),
            entry <- Resources.entries(engine, key),
            do: entry |> Resources.stamp(1) |> then(&Resources.render(:entry, &1, principal))

      assert Enum.map(entries, & &1["kind"]) |> Enum.uniq() |> Enum.sort() ==
               ~w(lifecycle reply revision)

      for entry <- entries do
        assert {entry["entry_id"], Schema.errors(defs, "Entry", entry)} == {entry["entry_id"], []}
      end

      GenServer.stop(relay)
    end

    test "the validator rejects undeclared and missing members" do
      defs = Schema.contract_defs(@contract)

      assert ["Counts: undeclared extra"] ==
               Schema.errors(defs, "Counts", %{"unresolved" => 0, "extra" => 1})

      assert ["Counts: missing unresolved"] == Schema.errors(defs, "Counts", %{})
    end
  end

  describe "items" do
    test "carry today's inbox row content under the contract's names", %{path: path} do
      relay = scripted_relay(path)
      {engine, view} = snapshot(relay)
      reader = rendered_items(engine, view, "reader")
      other = rendered_items(engine, view, "other")
      origin = rendered_items(engine, view, "origin")

      assert reader[key("chat")]["as"] == "message"
      assert other[key("chat")]["as"] == "knock"
      refute Map.has_key?(origin[key("chat")], "as")
      for item <- Map.values(reader), do: refute(Map.has_key?(item, "_viewer"))

      chat = reader[key("chat")]
      assert chat["seen"]
      assert chat["chat"]["available"]
      assert chat["chat"]["started"]
      assert chat["chat"]["last_origin_reply"] == chat["chat"]["read_position"]
      assert is_binary(chat["chat"]["read_position"])
      assert chat["message"]["title"] == "Question chat"
      assert chat["sender"] == "app:test"

      assert reader[key("revised")]["request_revision"] == 2
      assert reader[key("revised")]["message"]["title"] == "Question revised, again"

      assert %{"state" => "answered", "by_this_person" => true} =
               reader[key("answered")]["terminal"]

      assert reader[key("answered")]["terminal"]["answer"] == %{"kind" => "seen"}

      assert %{"state" => "answered", "answer" => %{"kind" => "no_comment"}} =
               reader[key("silent")]["terminal"]

      refute Map.has_key?(reader[key("silent")]["terminal"], "no_comment")

      assert %{"state" => "retracted", "by_this_person" => false} =
               reader[key("retracted")]["terminal"]

      assert reader[key("old")]["terminal"] == nil
      assert Resources.counts(engine, "A") == %{"unresolved" => 3}
      GenServer.stop(relay)
    end

    test "agree with today's Route B rows for the same reader", %{path: path} do
      relay = scripted_relay(path)
      {engine, view} = snapshot(relay)
      items = rendered_items(engine, view, "reader")

      assert {:ok, %{"rows" => rows}} =
               Engine.route_b_inbox_page(
                 engine,
                 "reader",
                 view.message_order,
                 view.message_order_suffixes,
                 view.message_entries_by_identity,
                 100,
                 nil
               )

      assert length(rows) == map_size(items)

      for row <- rows do
        item = Map.fetch!(items, key(row["message_id"]))
        assert item["seen"] == row["opened"]
        assert item["request_revision"] == row["request_revision"]
        assert item["message"] == row["message"]
        assert item["as"] == row["as"]
        assert item["chat"]["started"] == row["chat_started"]
        assert item["chat"]["available"] == Map.has_key?(row, "session_handle")
        assert item["availability"]["state"] == get_in(row, ["availability", "availability"])

        case row["terminal"] do
          nil -> assert item["terminal"] == nil
          %{"terminal_reason" => "retracted"} -> assert item["terminal"]["state"] == "retracted"
          terminal -> assert item["terminal"]["state"] == terminal["state"]
        end
      end

      # Rank keys sort into today's list order (Placement.list_items), and
      # sections follow its free-order state.
      by_key = items |> Map.values() |> Enum.sort_by(& &1["rank"]["key"])

      listed =
        view.list_items
        |> Enum.filter(&(&1["kind"] == "message"))
        |> Enum.map(&Map.fetch!(items, key(&1["message_id"])))

      assert Enum.map(by_key, & &1["item_id"]) == Enum.map(listed, & &1["item_id"])

      for row <- view.list_items, row["kind"] == "message" do
        rank = Map.fetch!(items, key(row["message_id"]))["rank"]

        case row["free_section"] do
          %{"state" => "active", "group" => group} ->
            assert rank["section"] == "current" and rank["group"] == group

          %{"state" => "history"} ->
            assert rank["section"] == "resolved" and not Map.has_key?(rank, "group")
        end
      end

      GenServer.stop(relay)
    end

    test "item_id and entry_id resolve back, and foreign or malformed ids do not",
         %{path: path} do
      relay = scripted_relay(path)
      {engine, _view} = snapshot(relay)

      for key <- Resources.item_keys(engine, "A") do
        assert {:ok, item_id} = Resources.item_id(engine, key)
        assert byte_size(item_id) <= 128
        assert {:ok, ^key} = Resources.item_key(engine, "A", item_id)
        assert {:error, :not_found} = Resources.item_key(engine, "B", item_id)
      end

      for bad <- ["", "i", "i0", "i01", "i-1", "x1", "i99999", "i1.0", 7] do
        assert {:error, :not_found} = Resources.item_key(engine, "A", bad)
      end

      for entry <- Resources.entries(engine, key("chat")) do
        assert {:ok, entry["position"]} == Resources.entry_seq(entry["entry_id"])
      end

      assert {:error, :not_found} = Resources.entry_seq("e0")
      assert {:error, :not_found} = Resources.file_source(engine, "A", "fi1.1.0")
      assert {:error, :not_found} = Resources.file_source(engine, "A", "fu-missing")
      GenServer.stop(relay)
    end
  end

  describe "entries" do
    test "map today's conversation facts and fold receipts into delivery", %{path: path} do
      relay = scripted_relay(path)
      {engine, _view} = snapshot(relay)
      {:ok, chat_id} = Resources.item_id(engine, key("chat"))

      assert [question, answer] =
               engine |> Resources.entries(key("chat")) |> Enum.filter(&(&1["kind"] == "reply"))

      assert %{"author" => "person", "text" => "Could you clarify?", "item_id" => ^chat_id} =
               question

      # The origin's reply to the question records that it acted on it.
      assert question["delivery"] == %{"delivered" => false, "acted" => true}
      assert %{"author" => "origin", "text" => "Here is more detail"} = answer
      assert answer["in_reply_to"] == question["entry_id"]
      assert question["position"] < answer["position"]

      positions = engine |> Resources.entries(key("chat")) |> Enum.map(& &1["position"])
      assert positions == Enum.sort(positions)

      assert [%{"kind" => "revision", "request_revision" => 2}] =
               engine
               |> Resources.entries(key("revised"))
               |> Enum.filter(&(&1["kind"] == "revision"))

      assert [%{"kind" => "lifecycle", "state" => "retracted", "at_ms" => at_ms}] =
               engine
               |> Resources.entries(key("retracted"))
               |> Enum.filter(&(&1["kind"] == "lifecycle"))

      assert is_integer(at_ms) and at_ms > 0

      [%{"feed_seq" => feed_seq}] =
        engine.origin_feeds[{"A", "origin"}].entries
        |> Map.values()
        |> Enum.filter(&(&1["kind"] == "discussion"))

      assert {:ok, _} = Relay.origin_ack(relay, "origin", feed_seq)
      {engine, _view} = snapshot(relay)

      assert [%{"delivery" => %{"delivered" => true, "acted" => true}}] =
               engine
               |> Resources.entries(key("chat"))
               |> Enum.filter(&(&1["author"] == "person"))

      GenServer.stop(relay)
    end
  end

  describe "settings" do
    test "show priority levels and only this device's push status", %{path: path} do
      relay = scripted_relay(path)
      {engine, _view} = snapshot(relay)
      settings = Resources.settings(engine, "A")

      for key <- ["reader", "origin"] do
        {:ok, principal} = Resources.principal(engine, key)
        rendered = Resources.render(:settings, settings, principal)
        assert rendered["push"] == %{"state" => "unconfigured", "revision" => 0}
        assert [%{"name" => _, "lower_bound" => 0} | _] = rendered["priority_levels"]
      end

      binding = %{
        revision: 3,
        state: "ready",
        active: true,
        profile_id: "harklight-sandbox",
        last_attempt_at: 1_000,
        next_at: nil
      }

      engine = %{engine | push_bindings: %{{"A", "reader"} => binding}}
      settings = Resources.settings(engine, "A")
      {:ok, reader} = Resources.principal(engine, "reader")
      {:ok, origin} = Resources.principal(engine, "origin")

      assert Resources.render(:settings, settings, reader)["push"] == %{
               "state" => "ready",
               "revision" => 3,
               "environment" => "sandbox",
               "last_attempt_at_ms" => 1_000
             }

      assert Resources.render(:settings, settings, origin)["push"]["state"] == "unconfigured"

      assert Resources.render(:settings, settings, %{reader | kind: :browser})["push"]["state"] ==
               "unconfigured"

      GenServer.stop(relay)
    end
  end

  describe "round 7 and 8" do
    test "each origin result is a result entry, and the item shows the latest", %{path: path} do
      relay = scripted_relay(path)
      {engine, _} = snapshot(relay)

      %{"feed_seq" => terminal_seq} =
        engine.origin_feeds[{"A", "origin"}].entries
        |> Map.values()
        |> Enum.find(&(&1["kind"] == "terminal" and &1["message_id"] == "answered"))

      assert {:ok, _} = Relay.origin_ack(relay, "origin", terminal_seq)

      for {status, n} <- [{"received", 50}, {"applied", 51}] do
        assert %{"type" => "ok"} =
                 request(relay, "origin", %{
                   "type" => "conversation_result",
                   "op_id" => op(n),
                   "sender_id" => "app:test",
                   "message_id" => "answered",
                   "in_reply_to" => terminal_seq,
                   "status" => status
                 })
      end

      {engine, view} = snapshot(relay)

      assert [
               %{"kind" => "result", "author" => "origin", "result" => first},
               %{"kind" => "result", "result" => second}
             ] =
               engine
               |> Resources.entries(key("answered"), view)
               |> Enum.filter(&(&1["kind"] == "result"))

      assert first == %{"status" => "received", "revision" => 1, "result_ref" => op(50)}
      assert second == %{"status" => "applied", "revision" => 2, "result_ref" => op(51)}
      item = rendered_items(engine, view, "reader")[key("answered")]
      assert item["result"] == %{"status" => "applied", "revision" => 2, "result_ref" => op(51)}

      defs = Schema.contract_defs(@contract)

      for entry <- Resources.entries(engine, key("answered"), view) do
        rendered = entry |> Resources.stamp(1) |> then(&Resources.render(:entry, &1, nil))

        assert {entry["entry_id"], Schema.errors(defs, "Entry", rendered)} ==
                 {entry["entry_id"], []}
      end

      GenServer.stop(relay)
    end

    test "reply fields stay with every reply read, with today's priority; items carry arrival",
         %{path: path} do
      relay = scripted_relay(path)
      {engine, view} = snapshot(relay)
      items = rendered_items(engine, view, "reader")
      chat = items[key("chat")]["chat"]

      # The scripted reader read through the agent's only reply.
      assert chat["unread_replies"] == 0
      assert is_binary(chat["reply_rank_key"])
      assert Map.has_key?(chat, "reply_group")

      assert chat["reply_priority"] == %{"level" => "normal"} or
               match?(%{"level" => "normal", "score" => _}, chat["reply_priority"])

      for {k, item} <- items,
          k != key("chat"),
          do: refute(Map.has_key?(item["chat"], "reply_rank_key"))

      assert [%{"author" => "origin", "priority" => %{"level" => "normal"}}] =
               engine
               |> Resources.entries(key("chat"), view)
               |> Enum.filter(&(&1["kind"] == "reply" and &1["author"] == "origin"))

      for item <- Map.values(items) do
        assert is_integer(item["arrival"]) and item["arrival"] > 0
        assert is_integer(item["created_at_ms"])
      end

      arrivals = items |> Map.values() |> Enum.map(& &1["arrival"])
      assert arrivals == Enum.uniq(arrivals)

      undated = update_in(engine.messages[key("old")], &Map.drop(&1, [:accepted_at, :free_order]))
      {:ok, old_id} = Resources.item_id(undated, key("old"))
      rank = Map.fetch!(Resources.ranks(undated, "A", view), old_id)
      old = Resources.item(undated, key("old"), rank)
      refute Map.has_key?(old, "created_at_ms") or Map.has_key?(old, "arrival")
      GenServer.stop(relay)
    end
  end

  describe "inbox order against today's" do
    test "rank order equals today's Route B inbox paging in free order, with threads and history",
         %{path: path} do
      relay = scripted_relay(path)

      for {id, thread, n} <- [
            {"t-a", "plan", 60},
            {"solo", nil, 61},
            {"t-b", "plan", 62},
            {"t-c", "other", 63},
            {"solo-2", nil, 65},
            {"t-d", "plan", 66}
          ] do
        extra = if thread, do: %{"thread" => thread}, else: %{}
        forward(relay, id, n, extra)
      end

      assert %{"type" => "ok"} =
               request(relay, "reader", %{
                 "type" => "answer",
                 "op_id" => op(64),
                 "sender_id" => "app:test",
                 "message_id" => "t-a",
                 "answer" => nil
               })

      {engine, view} = snapshot(relay)
      assert Placement.choice(engine, "A") == "baseline"
      items = rendered_items(engine, view, "reader")

      route_b =
        Stream.unfold(nil, fn
          :done ->
            nil

          cursor ->
            {:ok, page} =
              Engine.route_b_inbox_page(
                engine,
                "reader",
                view.message_order,
                view.message_order_suffixes,
                view.message_entries_by_identity,
                3,
                cursor
              )

            next =
              case page["next_cursor"] do
                nil ->
                  :done

                encoded ->
                  encoded
                  |> Base.url_decode64!(padding: false)
                  |> Kernel.<>("\n")
                  |> JSON.decode_line()
                  |> then(fn {:ok, %{"sender_id" => s, "message_id" => m}} -> {s, m} end)
              end

            {page["rows"], next}
        end)
        |> Enum.flat_map(& &1)
        |> Enum.map(& &1["message_id"])

      by_rank =
        items
        |> Enum.sort_by(fn {_, item} -> item["rank"]["key"] end)
        |> Enum.map(fn {{_, _, id}, _} -> id end)

      listed = for %{"kind" => "message", "message_id" => id} <- view.list_items, do: id

      assert by_rank == listed

      # Round 9: open members of one (sender, thread) group are adjacent, and
      # carry the thread; resolved and unthreaded items carry none.
      threads = Map.new(items, fn {{_, _, id}, item} -> {id, item["thread"]} end)
      assert threads["t-b"] == "plan" and threads["t-d"] == "plan" and threads["t-c"] == "other"
      assert threads["solo"] == nil and threads["t-a"] == nil
      b = Enum.find_index(by_rank, &(&1 == "t-b"))
      d = Enum.find_index(by_rank, &(&1 == "t-d"))
      assert abs(b - d) == 1
      # Today's two orders agree on the current section; report where they differ.
      current =
        for {{_, _, id}, item} <- Enum.sort_by(items, fn {_, i} -> i["rank"]["key"] end),
            item["rank"]["section"] == "current",
            do: id

      assert Enum.take(route_b, length(current)) == current
      send(self(), {:orders, route_b, by_rank})
      assert route_b == by_rank
      GenServer.stop(relay)
    end
  end

  describe "answer outcomes" do
    test "an FYI answered with its Seen action shows answer kind seen", %{path: path} do
      relay = scripted_relay(path)

      forward(relay, "fyi", 70, %{
        "message_type" => "fyi",
        "answer_ui" => %{
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
      })

      assert %{"type" => "ok"} =
               request(relay, "reader", %{
                 "type" => "answer",
                 "op_id" => op(71),
                 "sender_id" => "app:test",
                 "message_id" => "fyi",
                 "answer" => %{"acknowledgment" => "seen"}
               })

      {engine, view} = snapshot(relay)
      item = rendered_items(engine, view, "reader")[key("fyi")]
      assert %{"state" => "answered", "answer" => %{"kind" => "seen"}} = item["terminal"]
      GenServer.stop(relay)
    end

    test "an answer through another value action shows answer kind choice with its id and label",
         %{path: path} do
      relay = scripted_relay(path)

      forward(relay, "deploy", 72, %{
        "answer_ui" => %{
          "schema" => "attention-client.actions/1",
          "actions" => [
            %{
              "id" => "go",
              "label" => "Deploy",
              "kind" => "value",
              "value" => %{"deploy" => true}
            },
            %{
              "id" => "stop",
              "label" => "Hold",
              "kind" => "value",
              "value" => %{"deploy" => false}
            }
          ]
        }
      })

      assert %{"type" => "ok"} =
               request(relay, "reader", %{
                 "type" => "answer",
                 "op_id" => op(73),
                 "sender_id" => "app:test",
                 "message_id" => "deploy",
                 "answer" => %{"deploy" => false}
               })

      {engine, view} = snapshot(relay)
      item = rendered_items(engine, view, "reader")[key("deploy")]

      assert item["terminal"]["answer"] == %{
               "kind" => "choice",
               "id" => "stop",
               "label" => "Hold"
             }

      defs = Schema.contract_defs(@contract)
      assert Schema.errors(defs, "Item", item) == []
      GenServer.stop(relay)
    end
  end

  describe "structured answers" do
    test "a UI/1 item's answer follows its recorded path; unmarked answers keep today's outcome",
         %{path: path} do
      relay = scripted_relay(path)
      {engine, view} = snapshot(relay)
      defs = Schema.contract_defs(@contract)

      outcome = fn answer, path_marker ->
        engine =
          update_in(engine.messages[key("answered")], fn record ->
            terminal =
              record.terminal
              |> Map.merge(%{answer: answer, answer_present: true, by: "answer"})
              |> then(fn t ->
                if path_marker, do: Map.put(t, :answer_path, path_marker), else: t
              end)

            %{
              record
              | message:
                  Map.put(record.message, "answer_ui", %{"schema" => "attention-client.ui/1"}),
                terminal: terminal
            }
          end)

        {:ok, id} = Resources.item_id(engine, key("answered"))

        item =
          Resources.item(
            engine,
            key("answered"),
            Map.fetch!(Resources.ranks(engine, "A", view), id)
          )

        rendered =
          item
          |> Resources.stamp(1)
          |> then(&Resources.render(:item, &1, %{key: "reader", senders: MapSet.new()}))

        assert Schema.errors(defs, "Item", rendered) == []
        item["terminal"]["answer"]
      end

      # A /v2 answer records its path.
      for value <- [nil, %{"x" => 1}, [1, 2], 3, true, "ok"] do
        assert outcome.(value, "result") == %{"kind" => "result", "value" => value}
      end

      assert outcome.("on my way", "comment") == %{"kind" => "reply", "text" => "on my way"}

      # Before 0.2 there is no path: today's outcome, exactly (option C).
      assert outcome.(nil, nil) == %{"kind" => "seen"}
      assert outcome.("typed", nil) == %{"kind" => "reply", "text" => "typed"}
      for value <- [%{"x" => 1}, [1, 2], 3, true], do: assert(outcome.(value, nil) == nil)
      GenServer.stop(relay)
    end
  end

  describe "rank level" do
    test "Rank carries today's display level wherever the view entry has one", %{path: path} do
      relay = scripted_relay(path)
      forward(relay, "urgent", 74, %{"urgency" => "now"})
      {engine, view} = snapshot(relay)
      items = rendered_items(engine, view, "reader")
      assert items[key("urgent")]["rank"]["level"] == "now"

      for {{_, _, id}, item} <- items do
        case view.message_entries_by_identity[{"app:test", id}] do
          %{"priority" => level} -> assert item["rank"]["level"] == level
          _ -> refute Map.has_key?(item["rank"], "level")
        end
      end

      GenServer.stop(relay)
    end
  end

  describe "file types" do
    test "an upload's stored type is FileRef.content_type; older files have none", %{path: path} do
      relay = scripted_relay(path)
      {engine, view} = snapshot(relay)
      sha = String.duplicate("b", 64)

      upload = %{
        "tier" => "upload",
        "upload_id" => "u-typed",
        "name" => "photo.png",
        "size" => 3,
        "sha256" => sha
      }

      untyped = %{upload | "upload_id" => "u-plain", "name" => "old.bin"}

      engine =
        engine
        |> Map.update!(:uploads, fn uploads ->
          uploads
          |> Map.put("u-typed", %{
            person: "A",
            name: "photo.png",
            size: 3,
            sha256: sha,
            content_type: "image/png"
          })
          |> Map.put("u-plain", %{person: "A", name: "old.bin", size: 3, sha256: sha})
        end)
        |> update_in([Access.key(:messages), key("old")], fn record ->
          %{record | files_by_revision: %{1 => [upload, untyped]}}
        end)

      {:ok, id} = Resources.item_id(engine, key("old"))

      item =
        Resources.item(engine, key("old"), Map.fetch!(Resources.ranks(engine, "A", view), id))

      assert [%{"file_id" => "fuu-typed", "content_type" => "image/png"}, plain] = item["files"]
      refute Map.has_key?(plain, "content_type")
      GenServer.stop(relay)
    end
  end

  describe "availability entries" do
    test "an availability change is a schema-conformant server entry on each open item",
         %{path: path} do
      relay = scripted_relay(path)
      {engine, view} = snapshot(relay)
      defs = Schema.contract_defs(@contract)

      engine = Engine.origin_availability(engine, "A", "origin", @handle, "not_listening", 1_234)

      entries =
        for key <- Resources.item_keys(engine, "A"),
            entry <- Resources.entries(engine, key, view),
            entry["kind"] == "availability",
            do: entry

      open =
        for key <- Resources.item_keys(engine, "A"),
            is_nil(engine.messages[key].terminal),
            do: key

      assert length(entries) == length(open)

      for entry <- entries do
        assert %{
                 "author" => "server",
                 "availability" => %{"state" => "not_listening", "last_detached_at_ms" => 1_234}
               } = entry

        rendered = entry |> Resources.stamp(1) |> then(&Resources.render(:entry, &1, nil))
        assert Schema.errors(defs, "Entry", rendered) == []
        assert Schema.errors(defs, "Entry", %{rendered | "author" => "system"}) != []
      end

      GenServer.stop(relay)
    end
  end

  describe "revisions" do
    test "every request revision reads, including the first", %{path: path} do
      relay = scripted_relay(path)
      {engine, _view} = snapshot(relay)

      assert {:ok, %{"request_revision" => 1, "message" => first, "files" => []}} =
               Resources.revision(engine, key("revised"), 1)

      assert first["title"] == "Question revised"

      assert {:ok,
              %{"request_revision" => 2, "message" => %{"title" => "Question revised, again"}}} =
               Resources.revision(engine, key("revised"), 2)

      for n <- [0, 3, -1],
          do: assert({:error, :not_found} = Resources.revision(engine, key("revised"), n))

      GenServer.stop(relay)
    end
  end

  describe "entry sources" do
    test "each conversation fact shows up in at most one entry", %{path: path} do
      relay = scripted_relay(path)
      {engine, _view} = snapshot(relay)
      entries = Resources.entries(engine, key("chat"))
      [question] = Enum.filter(entries, &(&1["author"] == "person"))
      assert length(Resources.entry_sources(question)) == 2

      for {{"A", seq}, %{key: {"A", _, "chat"}}} <- engine.conversation_events do
        case Resources.entry_for_fact(engine, "A", seq) do
          nil -> :ok
          entry -> assert seq in Resources.entry_sources(entry)
        end
      end

      assert Resources.entry_for_fact(engine, "A", 999_999) == nil
      GenServer.stop(relay)
    end
  end

  describe "rank keys" do
    defp ordered?(keys), do: keys == Enum.sort(keys) and keys == Enum.uniq(keys)

    defp in_order(map, ids), do: Enum.map(ids, &Map.fetch!(map, &1))

    test "fresh keys sort in the given order and stay short" do
      ids = Enum.map(1..2_000, &"i#{&1}")
      keys = Resources.rank_keys(%{}, ids)
      assert ordered?(in_order(keys, ids))
      assert keys |> Map.values() |> Enum.map(&byte_size/1) |> Enum.max() <= 4
      assert Enum.all?(Map.values(keys), &String.starts_with?(&1, "1"))
    end

    test "previous keys in order are kept, so one move rewrites one key" do
      ids = Enum.map(1..50, &"i#{&1}")
      first = Resources.rank_keys(%{}, ids)
      assert Resources.rank_keys(first, ids) == first

      moved = List.delete(ids, "i40") |> List.insert_at(3, "i40")
      second = Resources.rank_keys(first, moved)
      assert ordered?(in_order(second, moved))
      assert Map.keys(second) |> Enum.reject(&(second[&1] == first[&1])) == ["i40"]

      inserted = List.insert_at(moved, 10, "i51") ++ ["i52"]
      third = Resources.rank_keys(second, inserted)
      assert ordered?(in_order(third, inserted))

      assert Map.keys(third) |> Enum.reject(&(third[&1] == second[&1])) |> Enum.sort() ==
               ["i51", "i52"]
    end

    test "any reordering of previous keys yields an ordered assignment" do
      for seed <- 1..25 do
        :rand.seed(:exsss, {seed, seed, seed})
        ids = Enum.map(1..40, &"i#{&1}")
        previous = Resources.rank_keys(%{}, Enum.shuffle(ids))
        order = Enum.shuffle(ids ++ ["new-a", "new-b"]) |> Enum.drop(2)
        assert ordered?(in_order(Resources.rank_keys(previous, order), order))
      end
    end

    test "midpoint is strictly between its bounds and has no trailing zero" do
      pairs = [
        {"", nil},
        {"", "1"},
        {"0001", "0002"},
        {"V", "W"},
        {"z", nil},
        {"Vz", "W"},
        {"", "0001"}
      ]

      for {low, high} <- pairs do
        middle = Resources.midpoint(low, high)
        assert low < middle
        if high, do: assert(middle < high)
        refute String.ends_with?(middle, "0")
      end
    end

    test "without a view, open items come first and resolved ones last, newest first",
         %{path: path} do
      relay = scripted_relay(path)
      {engine, _view} = snapshot(relay)
      ranks = Resources.ranks(engine, "A", nil)

      ordered =
        engine
        |> Resources.item_keys("A")
        |> Enum.map(fn key ->
          {:ok, id} = Resources.item_id(engine, key)
          record = engine.messages[key]
          {ranks[id]["key"], is_nil(record.terminal), record.accepted_seq, ranks[id]["section"]}
        end)
        |> Enum.sort()

      assert Enum.map(ordered, &elem(&1, 1)) == [true, true, true, false, false, false]

      for {_, open, _, section} <- ordered,
          do: assert(section == if(open, do: "current", else: "resolved"))

      resolved = for {_, false, seq, _} <- ordered, do: seq
      assert resolved == Enum.sort(resolved, :desc)
      GenServer.stop(relay)
    end

    test "an unread agent reply gets today's reply row: its key and group on Chat",
         %{path: path} do
      relay = scripted_relay(path)
      {engine, _} = snapshot(relay)

      [question] =
        engine |> Resources.entries(key("chat")) |> Enum.filter(&(&1["author"] == "person"))

      [%{"event_id" => event_id}] =
        engine.origin_feeds[{"A", "origin"}].entries
        |> Map.values()
        |> Enum.filter(&(&1["kind"] == "discussion"))

      refute question == nil

      assert %{"type" => "ok"} =
               request(relay, "origin", %{
                 "type" => "conversation_reply",
                 "op_id" => op(40),
                 "sender_id" => "app:test",
                 "message_id" => "chat",
                 "request_revision" => 1,
                 "in_reply_to" => String.to_integer(event_id),
                 "text" => "One more thing"
               })

      {engine, view} = snapshot(relay)
      items = rendered_items(engine, view, "reader")
      chat = items[key("chat")]["chat"]
      assert chat["unread_replies"] == 1
      assert is_binary(chat["reply_rank_key"])
      assert chat["reply_group"] in [nil, "earlier_chat_replies", "earlier_chat_replies_unknown"]
      assert Map.has_key?(chat, "reply_group")

      for {k, item} <- items, k != key("chat") do
        assert item["chat"]["unread_replies"] == 0
        refute Map.has_key?(item["chat"], "reply_rank_key")
      end

      # The reply row sorts among item rows where today's list puts it.
      listed =
        Enum.flat_map(view.list_items, fn
          %{"kind" => "message", "message_id" => id} ->
            [{:item, id}]

          %{"kind" => "reply", "message_id" => id} ->
            [{:reply, id}]

          _ ->
            []
        end)

      by_key =
        items
        |> Enum.flat_map(fn {{_, _, id}, item} ->
          [{item["rank"]["key"], {:item, id}}] ++
            if(item["chat"]["reply_rank_key"],
              do: [{item["chat"]["reply_rank_key"], {:reply, id}}],
              else: []
            )
        end)
        |> Enum.sort()
        |> Enum.map(&elem(&1, 1))

      assert by_key == listed

      # The change log keeps keys by rank unit, and maps each unit to its item.
      {:ok, chat_id} = Resources.item_id(engine, key("chat"))
      unit_keys = Resources.rank_unit_keys(Resources.ranks(engine, "A", view))
      assert unit_keys["r" <> chat_id] == chat["reply_rank_key"]
      assert unit_keys[chat_id] == items[key("chat")]["rank"]["key"]
      assert map_size(unit_keys) == map_size(items) + 1
      assert {:ok, key("chat")} == Resources.rank_unit_item(engine, "A", "r" <> chat_id)
      assert {:ok, key("chat")} == Resources.rank_unit_item(engine, "A", chat_id)

      assert Resources.ranks(engine, "A", view, unit_keys) |> Resources.rank_unit_keys() ==
               unit_keys

      GenServer.stop(relay)
    end

    test "items carry when they arrived, and earlier revisions' files their screen names",
         %{path: path} do
      relay = scripted_relay(path)
      {engine, view} = snapshot(relay)
      items = rendered_items(engine, view, "reader")

      assert Enum.all?(
               Map.values(items),
               &(is_integer(&1["created_at_ms"]) and &1["created_at_ms"] > 0)
             )

      file = %{
        "name" => "plan.png",
        "tier" => "inline",
        "size" => 3,
        "sha256" => String.duplicate("a", 64),
        "data_base64" => "AAAA"
      }

      other = %{file | "name" => "unused.png"}

      engine =
        update_in(engine.messages[key("revised")], fn record ->
          %{
            record
            | message:
                Map.put(record.message, "answer_ui", %{
                  "schema" => "attention-client.ui/1",
                  "body" => [%{"kind" => "image", "file" => "plan.png"}]
                }),
              files_by_revision: %{1 => [other, file], 2 => []}
          }
        end)

      {:ok, id} = Resources.item_id(engine, key("revised"))
      rank = Map.fetch!(Resources.ranks(engine, "A", view), id)

      assert [%{"name" => "plan.png", "file_id" => file_id}] =
               Resources.item(engine, key("revised"), rank)["files"]

      assert {:ok, {:inline, %{"name" => "plan.png"}}} =
               Resources.file_source(engine, "A", file_id)

      GenServer.stop(relay)
    end
  end
end
