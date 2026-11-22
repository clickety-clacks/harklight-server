defmodule AttentionServer.SupersedesTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{AgentAPIInProcess, JSON, Relay, RelayReaders}
  alias AttentionServer.ClientAPI.Reads

  @first_session "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @second_session "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

  setup do
    dir =
      Path.join(System.tmp_dir!(), "harklight-supersedes-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{path: Path.join(dir, "state.sqlite3")}
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp request(relay, device, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, device, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp send_request(relay, device, session, id, operation, extra \\ %{}) do
    request(relay, device, %{
      "type" => "forward",
      "op_id" => op(operation),
      "session_handle" => session,
      "message" =>
        Map.merge(
          %{
            "sender_id" => "app:test",
            "message_id" => id,
            "created_at" => operation,
            "title" => "Question #{id}"
          },
          extra
        )
    })
  end

  # The origin's v2 agent feed: one changes batch from `cursor`.
  defp agent_changes(relay, person, key, cursor) do
    assert {:ok, changes, false, position} =
             AgentAPIInProcess.changes(relay, person, key, cursor)

    {changes, position}
  end

  defp agent_terminal(relay, person, key, id) do
    item = AgentAPIInProcess.item_id(relay, person, "app:test", id)
    assert {:ok, %{"message" => message}} = AgentAPIInProcess.message(relay, person, key, item)
    {item, message["terminal"]}
  end

  test "one send supersedes several requests, warns on odd ids, and reaches phones and origin feeds",
       %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "A", "first")
    assert :ok = Relay.setup_device(relay, "A", "second")
    assert :ok = Relay.setup_device(relay, "A", "phone")
    assert :ok = Relay.setup_device(relay, "B", "other")
    assert :ok = Relay.setup_sender(relay, "first", "app:test")
    assert :ok = Relay.setup_sender(relay, "second", "app:test")
    assert :ok = Relay.setup_sender(relay, "other", "app:test")

    assert %{"seq" => 1} = send_request(relay, "first", @first_session, "m1", 1)
    assert %{"seq" => 2} = send_request(relay, "second", @second_session, "m2", 2)
    assert %{"seq" => 3} = send_request(relay, "first", @first_session, "answered", 3)

    assert %{"seq" => 4} =
             request(relay, "phone", %{
               "type" => "answer",
               "op_id" => op(4),
               "sender_id" => "app:test",
               "message_id" => "answered",
               "request_revision" => 1,
               "answer" => nil
             })

    assert %{"seq" => 1} = send_request(relay, "other", @first_session, "m1", 5)
    {_, first_feed_position} = agent_changes(relay, "A", "first", 0)

    # The phone's replay reaches seq 4 before the superseding send.
    assert {:ok, _initial_replay, 4} = RelayReaders.events(relay, "phone", 0)

    listed = ["m1", "m2", "answered", "missing", 7, "m1", "new"]

    expected_warnings = [
      %{"message_id" => "answered", "reason" => "closed"},
      %{"message_id" => "missing", "reason" => "unknown"},
      %{"message_id" => 7, "reason" => "invalid"},
      %{"message_id" => "new", "reason" => "invalid"}
    ]

    assert %{"seq" => 5, "supersedes_warnings" => ^expected_warnings} =
             send_request(relay, "second", @second_session, "new", 6, %{
               "supersedes" => listed
             })

    # The phone's live events after seq 4.
    assert {:ok, live, 7} = RelayReaders.events(relay, "phone", 4)

    assert Enum.map(live, & &1["seq"]) == [5, 6, 7]
    assert hd(live)["message"]["supersedes"] == listed

    assert Enum.map(tl(live), &{&1["message_id"], &1["state"]}) ==
             [{"m1", "superseded"}, {"m2", "superseded"}]

    assert {:ok, %{"items" => items}} = Reads.inbox(relay, "phone")

    for id <- ["m1", "m2"] do
      item = Enum.find(items, &(&1["message"]["message_id"] == id))
      assert item["terminal"]["state"] == "superseded"
    end

    # The superseding send reaches the superseded request's origin feed once.
    {m1_item, _} = agent_terminal(relay, "A", "first", "m1")
    {superseded, _} = agent_changes(relay, "A", "first", first_feed_position)

    assert [%{"kind" => "message", "data" => %{"terminal" => %{"state" => "superseded"}}}] =
             Enum.filter(superseded, &(&1["data"]["item_id"] == m1_item))

    assert %{"seq" => 5, "supersedes_warnings" => ^expected_warnings} =
             send_request(relay, "second", @second_session, "new", 6, %{
               "supersedes" => listed
             })

    assert {:ok, events, 7} = RelayReaders.events(relay, "phone", 0)
    assert Enum.at(events, 4)["message"]["supersedes"] == listed

    assert Enum.map(Enum.drop(events, 5), &{&1["message_id"], &1["state"]}) ==
             [{"m1", "superseded"}, {"m2", "superseded"}]

    refute Enum.any?(Enum.drop(events, 5), &Map.has_key?(&1, "terminal_reason"))

    assert {_, %{"state" => "superseded"}} = agent_terminal(relay, "A", "first", "m1")

    assert %{"type" => "error", "reason" => "closed"} =
             request(relay, "phone", %{
               "type" => "answer",
               "op_id" => op(7),
               "sender_id" => "app:test",
               "message_id" => "m1",
               "request_revision" => 1,
               "answer" => "too late"
             })

    # A superseded request still takes chat.
    assert %{"type" => "ok"} =
             request(relay, "phone", %{
               "type" => "discussion",
               "op_id" => op(8),
               "sender_id" => "app:test",
               "message_id" => "m2",
               "request_revision" => 1,
               "text" => "too late"
             })

    assert %{"seq" => 8} =
             request(relay, "phone", %{
               "type" => "answer",
               "op_id" => op(9),
               "sender_id" => "app:test",
               "message_id" => "new",
               "request_revision" => 1,
               "answer" => nil
             })

    assert %{"seq" => 9, "supersedes_warnings" => [%{"reason" => "invalid"}]} =
             send_request(relay, "second", @second_session, "malformed", 10, %{
               "supersedes" => "m1"
             })

    assert {:ok, [_only_other_account_delivery], 1} = RelayReaders.events(relay, "other", 0)
    GenServer.stop(relay)

    assert {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, replayed, 9} = RelayReaders.events(restarted, "phone", 0)
    assert Enum.at(replayed, 4)["message"]["supersedes"] == listed

    assert Enum.map(Enum.slice(replayed, 5, 2), &{&1["message_id"], &1["state"]}) ==
             [{"m1", "superseded"}, {"m2", "superseded"}]

    # Catch-up from seq 4 after the restart.
    assert {:ok, catchup, 9} = RelayReaders.events(restarted, "phone", 4)
    assert Enum.map(catchup, & &1["seq"]) == [5, 6, 7, 8, 9]

    assert Enum.map(Enum.slice(catchup, 1, 2), &{&1["message_id"], &1["state"]}) ==
             [{"m1", "superseded"}, {"m2", "superseded"}]

    assert {:ok, [_only_other_account_delivery], 1} = RelayReaders.events(restarted, "other", 0)
    GenServer.stop(restarted)
  end

  test "a different session on the same origin receives a superseded terminal", %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "shared")
    assert :ok = Relay.setup_sender(relay, "shared", "app:test")

    assert %{"seq" => 1} = send_request(relay, "shared", @first_session, "m1", 1)
    {_, before} = agent_changes(relay, "A", "shared", 0)

    assert %{"seq" => 2} =
             send_request(relay, "shared", @second_session, "m3", 2, %{
               "supersedes" => ["m1"]
             })

    # The agent feed of the first session receives the superseded terminal.
    {m1_item, terminal} = agent_terminal(relay, "A", "shared", "m1")
    assert %{"state" => "superseded"} = terminal
    {changes, after_position} = agent_changes(relay, "A", "shared", before)
    assert after_position > before

    assert [%{"kind" => "message", "data" => %{"session" => @first_session} = m1}] =
             Enum.filter(changes, &(&1["data"]["item_id"] == m1_item))

    assert m1["terminal"] == terminal

    {first_only, _} =
      AgentAPIInProcess.changes(relay, "A", "shared", before, session: @first_session)
      |> then(fn {:ok, rows, false, position} -> {rows, position} end)

    assert Enum.any?(first_only, &(&1["data"]["item_id"] == m1_item))

    # The stored origin feed keeps the server-authored terminal, undelivered.
    feed = :sys.get_state(relay).store.engine.origin_feeds[{"A", "shared"}]

    assert [
             %{
               "author" => "server",
               "message_id" => "m1",
               "session_handle" => @first_session,
               "terminal_state" => "superseded"
             }
           ] =
             for({_, %{"kind" => "terminal"} = entry} <- feed.entries, do: entry)

    GenServer.stop(relay)
  end
end
