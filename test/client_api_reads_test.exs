defmodule AttentionServer.ClientAPIReadsTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{JSON, Relay}
  alias AttentionServer.ClientAPI.Reads
  alias AttentionServer.ClientAPISchema, as: Schema

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @contract Path.expand("../docs/client-api-v2.md", __DIR__)

  setup do
    dir =
      Path.join(System.tmp_dir!(), "harklight-client-reads-#{System.unique_integer([:positive])}")

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

  defp request(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp forward(relay, id, operation, files \\ nil) do
    frame = %{
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

    frame = if files, do: Map.put(frame, "files", files), else: frame
    assert %{"type" => "ok"} = request(relay, "origin", frame)
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

  @bytes :binary.copy(<<0, 255, 42, 10>>, 100)

  defp scripted_relay(path, flags) do
    relay = start(path, flags)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "B", "stranger")

    for {id, n} <- [{"one", 2}, {"two", 3}, {"three", 4}, {"four", 5}], do: forward(relay, id, n)
    forward(relay, "file", 6, [%{"name" => "data.bin", "data_base64" => Base.encode64(@bytes)}])

    for n <- 1..5, do: discuss(relay, "one", 10 + n, "Note #{n}")

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(20),
               "sender_id" => "app:test",
               "message_id" => "two",
               "answer" => nil
             })

    relay
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

  test "the inbox pages through every item in rank order", %{path: path, flags: flags} do
    relay = scripted_relay(path, flags)
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

  test "bad parameters and keys are refused with contract tokens", %{path: path, flags: flags} do
    relay = scripted_relay(path, flags)

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

  test "one item reads alone, as it appears in the inbox", %{path: path, flags: flags} do
    relay = scripted_relay(path, flags)
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

  test "entries page backwards by position with before and limit", %{path: path, flags: flags} do
    relay = scripted_relay(path, flags)
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

  test "settings carry levels and an ETag over the resource alone", %{path: path, flags: flags} do
    relay = scripted_relay(path, flags)
    defs = Schema.contract_defs(@contract)
    assert {:ok, body} = Reads.settings(relay, "reader")
    assert Schema.errors(defs, "SettingsResponse", body) == []
    assert body["settings"]["push"] == %{"state" => "unconfigured", "revision" => 0}

    etag = Reads.etag(body["settings"])
    assert etag =~ ~r/^"[A-Za-z0-9_-]{43}"$/
    assert {:ok, again} = Reads.settings(relay, "reader")
    assert Reads.etag(again["settings"]) == etag
    refute Reads.etag(body) == etag
    GenServer.stop(relay)
  end

  test "an inline file reads by its file_id with its sha256", %{path: path, flags: flags} do
    relay = scripted_relay(path, flags)
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
       %{path: path, flags: flags} do
    relay = scripted_relay(path, nil)
    assert {:error, :not_found} = Reads.inbox(relay, "reader")
    GenServer.stop(relay)

    relay = start(path, flags)
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

  test "positions and revs follow the change log, and a folded receipt raises its entry's rev",
       %{path: path, flags: flags} do
    relay = scripted_relay(path, flags)
    one = item_id(relay, "one")
    assert {:ok, %{"rev" => position, "entries" => entries}} = Reads.entries(relay, "reader", one)
    assert position > 0
    assert Enum.all?(entries, &(&1["rev"] >= 1 and &1["rev"] <= position))
    assert entries |> Enum.map(& &1["rev"]) |> Enum.uniq() |> length() == length(entries)
    # Entries created with the change log on carry their first-seen time.
    assert Enum.all?(entries, &(is_integer(&1["at_ms"]) and &1["at_ms"] > 0))

    first = hd(entries)

    %{"feed_seq" => feed_seq} =
      :sys.get_state(relay).store.engine.origin_feeds[{"A", "origin"}].entries
      |> Map.values()
      |> Enum.find(&(&1["kind"] == "discussion" and &1["text"] == "Note 1"))

    assert {:ok, _} = Relay.origin_ack(relay, "origin", feed_seq)

    assert {:ok, %{"rev" => later, "entries" => [marked | _]}} =
             Reads.entries(relay, "reader", one)

    assert marked["entry_id"] == first["entry_id"]
    assert marked["delivery"]["delivered"]
    assert marked["rev"] > first["rev"] and marked["rev"] <= later
    assert later > position
    GenServer.stop(relay)
  end

  test "any request revision reads, including the first", %{path: path, flags: flags} do
    relay = scripted_relay(path, flags)
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
       %{path: path, flags: flags} do
    relay = scripted_relay(path, flags)

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
       %{path: path, flags: flags} do
    relay = scripted_relay(path, flags)
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
    path: path,
    flags: flags
  } do
    relay = scripted_relay(path, flags)
    engine = :sys.get_state(relay).store.engine
    scanned = for {{"A", _, _} = key, _} <- engine.messages, do: key

    assert Enum.sort(AttentionServer.ClientAPI.Resources.item_keys(engine, "A")) ==
             Enum.sort(scanned)

    assert AttentionServer.ClientAPI.Resources.item_keys(engine, "B") == []
    assert AttentionServer.ClientAPI.Resources.item_keys(engine, "nobody") == []
    GenServer.stop(relay)
  end
end
