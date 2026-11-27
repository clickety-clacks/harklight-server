defmodule AttentionServer.ClientAPIReadsTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    AgentAPIInProcess,
    BounceDecision,
    ChangeLog,
    Engine,
    JSON,
    PreChangeLogStore,
    Relay,
    Store
  }

  alias AttentionServer.ClientAPI.{FeedStep, Reads}
  alias AttentionServer.ClientAPISchema, as: Schema
  alias Exqlite.Sqlite3

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @contract Path.expand("../docs/client-api-v2.md", __DIR__)

  setup do
    dir =
      Path.join(System.tmp_dir!(), "harklight-client-reads-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{path: Path.join(dir, "state.sqlite3")}
  end

  defp start(path) do
    :ok = PreChangeLogStore.convert_fixture(path)
    assert {:ok, relay} = Relay.start_link(store_path: path, agent_fixture_mode: true)
    relay
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp request(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp forward(relay, id, operation, files \\ nil) do
    frame = forward_frame(id, operation)
    frame = if files, do: Map.put(frame, "files", files), else: frame
    assert %{"type" => "ok"} = request(relay, "origin", frame)
  end

  defp forward_frame(id, operation) do
    %{
      "type" => "forward",
      "op_id" => op(operation),
      "session_handle" => @handle,
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => id,
        "created_at" => operation,
        "title" => "Question #{id}"
      }
    }
  end

  defp discuss(relay, id, operation, text),
    do: assert(%{"type" => "ok"} = request(relay, "reader", discuss_frame(id, operation, text)))

  defp discuss_frame(id, operation, text) do
    %{
      "type" => "discussion",
      "op_id" => op(operation),
      "sender_id" => "app:test",
      "message_id" => id,
      "request_revision" => 1,
      "text" => text
    }
  end

  defp answer_frame(id, operation) do
    %{
      "type" => "answer",
      "op_id" => op(operation),
      "sender_id" => "app:test",
      "message_id" => id,
      "answer" => nil
    }
  end

  @bytes :binary.copy(<<0, 255, 42, 10>>, 100)

  defp scripted_relay(path) do
    relay = start(path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "B", "stranger")

    for {id, n} <- [{"one", 2}, {"two", 3}, {"three", 4}, {"four", 5}], do: forward(relay, id, n)
    forward(relay, "file", 6, [%{"name" => "data.bin", "data_base64" => Base.encode64(@bytes)}])

    for n <- 1..5, do: discuss(relay, "one", 10 + n, "Note #{n}")

    assert %{"type" => "ok"} = request(relay, "reader", answer_frame("two", 20))
    relay
  end

  test "stored rank and pagination place an older Alarm before ordinary messages after restart", %{path: path} do
    relay = scripted_relay(path)
    before = all_pages(relay, "reader", 1) |> Enum.flat_map(& &1["items"])

    alarm = put_in(forward_frame("alarm", 101), ["message", "alarm"], true)
    assert %{"type" => "ok"} = request(relay, "origin", alarm)
    forward(relay, "ordinary-newer", 102)
    GenServer.stop(relay)
    relay = start(path)

    pages = all_pages(relay, "reader", 1)
    assert [%{"message" => %{"alarm" => true}} | ordinary] = Enum.flat_map(pages, & &1["items"])
    assert Enum.map(tl(ordinary), & &1["item_id"]) == Enum.map(before, & &1["item_id"])
  end

  defp all_pages(relay, key, limit, page \\ nil, acc \\ []) do
    params = %{"limit" => Integer.to_string(limit)}
    params = if page, do: Map.put(params, "page", page), else: params
    assert {:ok, body} = Reads.inbox(relay, key, params)
    acc = acc ++ [body]

    case body["next_page"] do
      nil -> acc
      next -> all_pages(relay, key, limit, next, acc)
    end
  end

  defp item_id(relay, message_id) do
    {:ok, %{"items" => items}} = Reads.inbox(relay, "reader")
    Enum.find_value(items, &(&1["message"]["message_id"] == message_id && &1["item_id"]))
  end

  test "the inbox pages through every item in rank order", %{path: path} do
    relay = scripted_relay(path)
    defs = Schema.contract_defs(@contract)
    assert {:ok, whole} = Reads.inbox(relay, "reader")
    assert Schema.errors(defs, "InboxPage", whole) == []
    refute Map.has_key?(whole, "next_page")
    assert length(whole["items"]) == 5
    assert whole["counts"] == %{"unresolved" => 4}
    assert whole["rev"] > 0
    assert Enum.all?(whole["items"], &(&1["rev"] >= 1 and &1["rev"] <= whole["rev"]))

    keys = Enum.map(whole["items"], & &1["rank"]["key"])
    assert keys == Enum.sort(keys)
    assert List.last(whole["items"])["terminal"]["state"] == "answered"

    pages = all_pages(relay, "reader", 2)
    assert Enum.map(pages, &length(&1["items"])) == [2, 2, 1]
    assert Enum.flat_map(pages, & &1["items"]) == whole["items"]
    for page <- pages, do: assert(Schema.errors(defs, "InboxPage", page) == [])

    assert {:ok, %{"items" => capped}} = Reads.inbox(relay, "reader", %{"limit" => "500"})
    assert length(capped) == 5
    GenServer.stop(relay)
  end

  test "bad parameters and keys are refused with contract tokens", %{path: path} do
    relay = scripted_relay(path)

    for limit <- ["0", "-1", "x", "1.5", ""] do
      assert {:error, :invalid_field, "limit"} = Reads.inbox(relay, "reader", %{"limit" => limit})
    end

    for page <- ["", "p", "p!!", "q" <> Base.url_encode64("1")] do
      assert {:error, :invalid_field, "page"} = Reads.inbox(relay, "reader", %{"page" => page})
    end

    assert {:error, :unauthorized} = Reads.inbox(relay, "nobody")
    assert {:error, :unauthorized} = Reads.settings(relay, "nobody")

    one = item_id(relay, "one")
    assert {:error, :not_found} = Reads.item(relay, "reader", "i999")
    assert {:error, :not_found} = Reads.item(relay, "stranger", one)
    assert {:error, :not_found} = Reads.entries(relay, "stranger", one)

    assert {:error, :invalid_field, "before"} =
             Reads.entries(relay, "reader", one, %{"before" => "e999"})

    assert {:error, :invalid_field, "limit"} =
             Reads.entries(relay, "reader", one, %{"limit" => "0"})

    GenServer.stop(relay)
  end

  test "one item reads alone, as it appears in the inbox", %{path: path} do
    relay = scripted_relay(path)
    defs = Schema.contract_defs(@contract)
    assert {:ok, %{"items" => items}} = Reads.inbox(relay, "reader")

    for item <- items do
      assert {:ok, response} = Reads.item(relay, "reader", item["item_id"])
      assert {:ok, %{"rev" => position}} = Reads.inbox(relay, "reader")
      assert response == %{"rev" => position, "item" => item}
      assert Schema.errors(defs, "ItemResponse", response) == []
    end

    GenServer.stop(relay)
  end

  test "entries page backwards by position with before and limit", %{path: path} do
    relay = scripted_relay(path)
    one = item_id(relay, "one")

    assert {:ok, %{"entries" => newest, "older" => true}} =
             Reads.entries(relay, "reader", one, %{"limit" => "2"})

    assert Enum.map(newest, & &1["text"]) == ["Note 4", "Note 5"]

    assert {:ok, %{"entries" => middle, "older" => true}} =
             Reads.entries(relay, "reader", one, %{
               "limit" => "2",
               "before" => hd(newest)["entry_id"]
             })

    assert Enum.map(middle, & &1["text"]) == ["Note 2", "Note 3"]

    assert {:ok, %{"entries" => oldest, "older" => false}} =
             Reads.entries(relay, "reader", one, %{
               "limit" => "2",
               "before" => hd(middle)["entry_id"]
             })

    assert Enum.map(oldest, & &1["text"]) == ["Note 1"]

    assert {:ok, %{"entries" => all, "older" => false}} = Reads.entries(relay, "reader", one)
    assert all == oldest ++ middle ++ newest
    assert Enum.all?(all, &(&1["item_id"] == one and &1["rev"] >= 1))
    GenServer.stop(relay)
  end

  test "settings carry levels and an ETag over the resource alone", %{path: path} do
    relay = scripted_relay(path)
    defs = Schema.contract_defs(@contract)
    assert {:ok, body} = Reads.settings(relay, "reader")
    assert Schema.errors(defs, "SettingsResponse", body) == []
    assert body["settings"]["alerts"] == "important"
    assert body["settings"]["push"] == %{"state" => "unconfigured", "revision" => 0}

    etag = Reads.etag(body["settings"])
    assert etag =~ ~r/^"[A-Za-z0-9_-]{43}"$/
    assert {:ok, again} = Reads.settings(relay, "reader")
    assert Reads.etag(again["settings"]) == etag
    refute Reads.etag(body) == etag
    GenServer.stop(relay)
  end

  test "an inline file reads by its file_id with its sha256", %{path: path} do
    relay = scripted_relay(path)
    file_item = item_id(relay, "file")
    assert {:ok, %{"item" => %{"files" => [ref]}}} = Reads.item(relay, "reader", file_item)
    sha256 = Base.encode16(:crypto.hash(:sha256, @bytes), case: :lower)
    assert %{"name" => "data.bin", "size" => 400, "sha256" => ^sha256} = ref

    assert {:ok, file} = Reads.file(relay, "reader", ref["file_id"])
    assert file.data == @bytes
    assert file.size == 400
    assert file.content_type == "application/octet-stream"
    assert Reads.file_etag(file) == ~s("#{sha256}")

    assert {:error, :not_found} = Reads.file(relay, "stranger", ref["file_id"])

    assert {:error, :not_found} =
             Reads.file(relay, "reader", "fu" <> "00000000-0000-4000-8000-000000000000")

    assert {:error, :unauthorized} = Reads.file(relay, "nobody", ref["file_id"])
    GenServer.stop(relay)
  end

  test "data written before the client API reads through it, seeded at rev 1",
       %{path: path} do
    # The same data, committed as a Server before 0.2.0 did, with the log off.
    # The file item carries no file here, because the Relay stages those.
    PreChangeLogStore.write(path, fn store ->
      store =
        store
        |> PreChangeLogStore.setup_person("A")
        |> PreChangeLogStore.setup_device("A", "origin")
        |> PreChangeLogStore.setup_device("A", "reader")
        |> PreChangeLogStore.setup_sender("origin", "app:test")
        |> PreChangeLogStore.setup_person("B")
        |> PreChangeLogStore.setup_device("B", "stranger")

      items = [{"one", 2}, {"two", 3}, {"three", 4}, {"four", 5}, {"file", 6}]
      forwards = for {id, n} <- items, do: {"origin", forward_frame(id, n)}
      notes = for n <- 1..5, do: {"reader", discuss_frame("one", 10 + n, "Note #{n}")}
      frames = forwards ++ notes ++ [{"reader", answer_frame("two", 20)}]

      Enum.reduce(frames, store, fn {key, frame}, store ->
        assert {%{"type" => "ok"}, store} = PreChangeLogStore.request(store, key, frame)
        store
      end)
    end)

    relay = start(path)
    defs = Schema.contract_defs(@contract)
    assert {:ok, inbox} = Reads.inbox(relay, "reader")
    assert Schema.errors(defs, "InboxPage", inbox) == []
    assert inbox["rev"] == 1
    assert Enum.all?(inbox["items"], &(&1["rev"] == 1))
    assert length(inbox["items"]) == 5

    one = item_id(relay, "one")
    assert {:ok, %{"rev" => 1, "entries" => entries}} = Reads.entries(relay, "reader", one)
    assert length(entries) == 5
    assert Enum.all?(entries, &(&1["rev"] == 1 and not Map.has_key?(&1, "at_ms")))
    assert {:ok, %{"rev" => 1, "settings" => %{"rev" => 1}}} = Reads.settings(relay, "reader")

    # The first change after the seed moves the position and that item's rev only.
    discuss(relay, "three", 40, "After cut-over")
    assert {:ok, after_change} = Reads.inbox(relay, "reader")
    assert after_change["rev"] > 1
    three = item_id(relay, "three")

    for item <- after_change["items"] do
      if item["item_id"] == three,
        do: assert(item["rev"] > 1 and item["rev"] <= after_change["rev"]),
        else: assert(item["rev"] == 1)
    end

    GenServer.stop(relay)
  end

  test "an older resolved message without an explicit revision survives inbox and feed catch-up",
       %{path: path} do
    key = {"A", "app:test", "legacy"}

    PreChangeLogStore.write(path, fn store ->
      store =
        store
        |> PreChangeLogStore.setup_person("A")
        |> PreChangeLogStore.setup_device("A", "origin")
        |> PreChangeLogStore.setup_device("A", "reader")
        |> PreChangeLogStore.setup_sender("origin", "app:test")

      frame = Map.delete(forward_frame("legacy", 1), "session_handle")
      assert {%{"type" => "ok"}, store} = PreChangeLogStore.request(store, "origin", frame)

      assert {%{"type" => "ok"}, store} =
               PreChangeLogStore.request(store, "reader", answer_frame("legacy", 2))

      record = Map.drop(store.engine.messages[key], [:revision, :revisions])

      engine =
        store.engine
        |> Engine.put_row(:messages, key, record)
        |> Engine.delete_row(:bounce_revisions, key)
        |> BounceDecision.rebuild_indexes()

      audit = %{person: "A", actor: "origin", action: "legacy_fixture"}
      assert {:ok, store} = Store.commit(store, engine, audit)
      store
    end)

    relay = start(path)
    stored = :sys.get_state(relay).store.engine.messages[key]
    refute Map.has_key?(stored, :revision)
    principal = %{person: "A", kind: :device, key: "reader"}

    # Record a change after the cutover seed so catch-up renders the saved item.
    :sys.replace_state(relay, fn %{store: store} = state ->
      assert {:ok, 1} = ChangeLog.head(store, "A")
      pending = %{"A" => [{:item, ChangeLog.item_resource(key)}]}
      assert :ok = Sqlite3.execute(store.db, "BEGIN IMMEDIATE")

      assert {:ok, %{"A" => 2}} =
               ChangeLog.append(store.db, pending, %{"A" => true}, store.version)

      assert :ok = Sqlite3.execute(store.db, "COMMIT")
      state
    end)

    assert {:ok, changes, false, 2} = FeedStep.read(relay, principal, 1, 100)

    assert Enum.any?(changes, fn change ->
             match?(
               %{
                 "kind" => "item",
                 "data" => %{
                   "request_revision" => 1,
                   "message" => %{"message_id" => "legacy", "title" => "Question legacy"}
                 }
               },
               change
             )
           end)

    assert {:ok, %{"items" => [%{"request_revision" => 1, "message" => message}]}} =
             Reads.inbox(relay, "reader")

    assert message["message_id"] == "legacy"
    assert :sys.get_state(relay).store.engine.messages[key] == stored
    GenServer.stop(relay)
  end

  test "positions and revs follow the change log, and a folded receipt raises its entry's rev",
       %{path: path} do
    relay = scripted_relay(path)
    one = item_id(relay, "one")
    assert {:ok, %{"rev" => position, "entries" => entries}} = Reads.entries(relay, "reader", one)
    assert position > 0
    assert Enum.all?(entries, &(&1["rev"] >= 1 and &1["rev"] <= position))
    assert entries |> Enum.map(& &1["rev"]) |> Enum.uniq() |> length() == length(entries)
    # Entries created with the change log on carry their first-seen time.
    assert Enum.all?(entries, &(is_integer(&1["at_ms"]) and &1["at_ms"] > 0))

    first = hd(entries)

    # The origin's agent feed delivers the turn (ACK1).
    assert {:ok, _, _, _} = AgentAPIInProcess.changes(relay, "A", "origin")

    assert {:ok, %{"rev" => later, "entries" => [marked | _]}} =
             Reads.entries(relay, "reader", one)

    assert marked["entry_id"] == first["entry_id"]
    assert marked["delivery"]["delivered"]
    assert marked["rev"] > first["rev"] and marked["rev"] <= later
    assert later > position
    GenServer.stop(relay)
  end

  test "any request revision reads, including the first", %{path: path} do
    relay = scripted_relay(path)
    defs = Schema.contract_defs(@contract)
    file_item = item_id(relay, "file")
    assert {:ok, body} = Reads.revision(relay, "reader", file_item, "1")
    assert Schema.errors(defs, "RevisionResponse", body) == []
    assert %{"request_revision" => 1, "files" => [%{"name" => "data.bin"}]} = body["revision"]

    for n <- ["0", "2", "x", "01x"],
        do: assert({:error, :not_found} = Reads.revision(relay, "reader", file_item, n))

    assert {:error, :not_found} = Reads.revision(relay, "stranger", file_item, "1")
    GenServer.stop(relay)
  end

  test "a browser session reads the same resources, as knocks, while its owner revision holds",
       %{path: path} do
    relay = scripted_relay(path)

    :sys.replace_state(relay, fn state ->
      engine = state.store.engine
      owners = Map.put(engine.owners, "A", %{key: "owner-a", revision: 4})
      %{state | store: %{state.store | engine: %{engine | owners: owners}}}
    end)

    browser = %{kind: :browser, person: "A", key: "owner-a", owner_revision: 4, session_id: "s1"}
    assert {:ok, from_browser} = Reads.inbox(relay, browser)
    assert {:ok, from_device} = Reads.inbox(relay, "reader")
    assert Enum.all?(from_browser["items"], &(&1["as"] == "knock"))

    assert Enum.map(from_browser["items"], &Map.delete(&1, "as")) ==
             Enum.map(from_device["items"], &Map.delete(&1, "as"))

    assert {:ok, %{"settings" => %{"push" => %{"state" => "unconfigured"}}}} =
             Reads.settings(relay, browser)

    file_item = item_id(relay, "file")
    assert {:ok, %{"item" => %{"files" => [ref]}}} = Reads.item(relay, browser, file_item)
    assert {:ok, %{data: @bytes}} = Reads.file(relay, browser, ref["file_id"])

    for stale <- [
          %{browser | owner_revision: 3},
          %{browser | key: "other"},
          %{browser | person: "B"}
        ] do
      assert {:error, :unauthorized} = Reads.inbox(relay, stale)
    end

    assert {:ok, _} = Reads.inbox(relay, %{kind: :device, key: "reader"})
    assert {:error, :unauthorized} = Reads.inbox(relay, %{kind: :device, key: "owner-a"})
    GenServer.stop(relay)
  end

  test "an inline file whose stored data is gone answers storage_unavailable, and the relay lives",
       %{path: path} do
    relay = scripted_relay(path)
    file_item = item_id(relay, "file")
    assert {:ok, %{"item" => %{"files" => [ref]}}} = Reads.item(relay, "reader", file_item)

    :sys.replace_state(relay, fn state ->
      engine = state.store.engine
      key = {"A", "app:test", "file"}

      gone = %AttentionServer.LargeData.Ref{
        id: "gone",
        size: 10,
        sha256: String.duplicate("0", 64),
        path: Path.join(System.tmp_dir!(), "no-such-large-data")
      }

      engine =
        update_in(engine.messages[key].files_by_revision[1], fn [file] ->
          [%{file | "data_base64" => gone}]
        end)

      %{state | store: %{state.store | engine: engine}}
    end)

    assert {:error, :storage_unavailable} = Reads.file(relay, "reader", ref["file_id"])
    assert Process.alive?(relay)
    assert {:ok, _} = Reads.inbox(relay, "reader")
    GenServer.stop(relay)
  end

  test "a person's items are found from their own sequence, not every person's", %{
    path: path
  } do
    relay = scripted_relay(path)
    engine = :sys.get_state(relay).store.engine
    scanned = for {{"A", _, _} = key, _} <- engine.messages, do: key

    assert Enum.sort(AttentionServer.ClientAPI.Resources.item_keys(engine, "A")) ==
             Enum.sort(scanned)

    assert AttentionServer.ClientAPI.Resources.item_keys(engine, "B") == []
    assert AttentionServer.ClientAPI.Resources.item_keys(engine, "nobody") == []
    GenServer.stop(relay)
  end

  # Review finding F3: with ranks stored once per change, keys of items that
  # did not move stay put, so paging across a change neither repeats nor skips
  # them, and anything that did move has a change after the first page.
  test "paging across a rank change neither repeats nor skips an unmoved item",
       %{path: path} do
    relay = scripted_relay(path)
    for n <- 1..4, do: forward(relay, "more-#{n}", 30 + n)

    assert {:ok, %{"items" => first, "next_page" => page, "rev" => first_rev}} =
             Reads.inbox(relay, "reader", %{"limit" => "3"})

    keys_before = Map.new(first, &{&1["item_id"], &1["rank"]["key"]})

    # Two arrivals rank ahead of everything; nothing else moves.
    forward(relay, "late-1", 40)
    forward(relay, "late-2", 41)

    rest = all_pages(relay, "reader", 3, page) |> Enum.flat_map(& &1["items"])
    shown = Enum.map(first ++ rest, & &1["item_id"])
    assert shown == Enum.uniq(shown), "an item repeated across pages"

    {:ok, %{"items" => now}} = Reads.inbox(relay, "reader", %{"limit" => "200"})
    late = for item <- now, item["message"]["message_id"] in ["late-1", "late-2"], do: item
    assert length(late) == 2

    for item <- now, item not in late do
      assert item["item_id"] in shown, "an unmoved item was skipped"
    end

    # Items on the first page kept their keys, and each arrival has a change
    # after the first page's position, so the feed delivers what paging missed.
    for item <- now,
        Map.has_key?(keys_before, item["item_id"]),
        do: assert(item["rank"]["key"] == keys_before[item["item_id"]])

    for item <- late, do: assert(item["rev"] > first_rev)
    GenServer.stop(relay)
  end

  test "a failing rank store answers storage_unavailable, not a recomputation", %{
    path: path
  } do
    relay = scripted_relay(path)
    one = item_id(relay, "one")
    assert {:ok, _} = Reads.item(relay, "reader", one)

    :sys.replace_state(relay, fn state -> %{state | store: %{state.store | db: :unusable}} end)

    state = :sys.get_state(relay)
    context = %{engine: state.store.engine, store: state.store, view: nil}
    assert {:error, :storage_unavailable} = Reads.item_rank(context, "A", one)

    principal = %AttentionServer.ClientAPI.Principal{person: "A", kind: :device, key: "reader"}
    {:ok, key} = AttentionServer.ClientAPI.Resources.item_key(state.store.engine, "A", one)

    row = %{
      rev: 99,
      kind: :item,
      resource_id: AttentionServer.ChangeLog.item_resource(key)
    }

    assert AttentionServer.ClientAPI.FeedRenderer.render(context, principal, row) ==
             {:error, :storage_unavailable}
  end
end
