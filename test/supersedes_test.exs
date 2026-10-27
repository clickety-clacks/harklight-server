defmodule AttentionServer.SupersedesTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{JSON, Relay}

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
    assert {:ok, %{"cursor" => first_feed_high}} = Relay.origin_feed(relay, "first", 0)
    assert :ok = Relay.origin_stream_attach(relay, "first", @first_session, self())

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

    assert_receive {:origin_state_changed, next_feed_high}
    assert next_feed_high == first_feed_high + 1

    assert %{"seq" => 5, "supersedes_warnings" => ^expected_warnings} =
             send_request(relay, "second", @second_session, "new", 6, %{
               "supersedes" => listed
             })

    assert {:ok, events, 7} = Relay.events(relay, "phone", 0)
    assert Enum.at(events, 4)["message"]["supersedes"] == listed

    assert Enum.map(Enum.drop(events, 5), &{&1["message_id"], &1["state"]}) ==
             [{"m1", "superseded"}, {"m2", "superseded"}]

    refute Enum.any?(Enum.drop(events, 5), &Map.has_key?(&1, "terminal_reason"))

    assert {:ok, %{"entries" => first_feed}} = Relay.origin_feed(relay, "first", 0)
    assert List.last(first_feed)["terminal_state"] == "superseded"
    assert List.last(first_feed)["message_id"] == "m1"

    assert %{"type" => "error", "reason" => "closed"} =
             request(relay, "phone", %{
               "type" => "answer",
               "op_id" => op(7),
               "sender_id" => "app:test",
               "message_id" => "m1",
               "request_revision" => 1,
               "answer" => "too late"
             })

    assert %{"type" => "error", "reason" => "closed"} =
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

    assert {:ok, [_only_other_account_delivery], 1} = Relay.events(relay, "other", 0)
    GenServer.stop(relay)

    assert {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, replayed, 9} = Relay.events(restarted, "phone", 0)
    assert Enum.at(replayed, 4)["message"]["supersedes"] == listed

    assert Enum.map(Enum.slice(replayed, 5, 2), &{&1["message_id"], &1["state"]}) ==
             [{"m1", "superseded"}, {"m2", "superseded"}]

    assert {:ok, [_only_other_account_delivery], 1} = Relay.events(restarted, "other", 0)
    GenServer.stop(restarted)
  end
end
