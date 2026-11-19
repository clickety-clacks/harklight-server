defmodule AttentionServer.ContradictionsRelayTest do
  use ExUnit.Case, async: true
  alias AttentionServer.{Engine, JSON, Relay}

  defp op(n), do: "00000000-0000-4000-8000-" <> String.pad_leading(Integer.to_string(n), 12, "0")

  defp start(opts) do
    path =
      Path.join(System.tmp_dir!(), "contradictions-#{System.unique_integer([:positive])}.sqlite3")

    on_exit(fn ->
      File.rm_rf!(path)
      File.rm_rf!(path <> "-wal")
      File.rm_rf!(path <> "-shm")
    end)

    relay = start_supervised!({Relay, Keyword.put(opts, :store_path, path)})
    :ok = Relay.setup_person(relay, "person")
    :ok = Relay.setup_person(relay, "other")

    for {person, key} <- [{"person", "origin"}, {"person", "reader"}, {"other", "other-origin"}] do
      :ok = Relay.setup_device(relay, person, key)
      :ok = Relay.setup_sender(relay, key, "app:test")
    end

    {relay, path}
  end

  defp jev(relay) do
    :sys.replace_state(relay, fn state ->
      {:ok, engine} =
        Engine.set_jev_connection(
          state.store.engine,
          "https://example.com/v1/systemone",
          "jev-latest"
        )

      {:ok, engine} = Engine.set_algorithm_choice(engine, "person", "jev-set")
      %{state | store: %{state.store | engine: engine}}
    end)
  end

  defp frame(n, extra \\ %{}) do
    message =
      Map.merge(
        %{
          "sender_id" => "app:test",
          "message_id" => "m#{n}",
          "title" => "Title #{n}",
          "body" => "Full text #{n}",
          "created_at" => 1,
          "open" => %{"argv" => ["unused"], "terminal" => false}
        },
        extra
      )

    %{"v" => 1, "type" => "forward", "op_id" => op(n), "message" => message}
  end

  defp request(relay, frame, key \\ "origin") do
    {:ok, line} = Relay.request_line(relay, key, JSON.encode_line(frame))
    {:ok, result} = JSON.decode_line(line)
    result
  end

  test "baseline and empty checked sets preserve the existing reply" do
    {relay, _} =
      start(
        contradiction_batch_size: 1,
        contradiction_evaluator: fn _ -> flunk("unexpected check") end
      )

    refute Map.has_key?(request(relay, frame(1)), "possible_contradictions")
    jev(relay)
    # Other person's first message is still baseline and does not join this set.
    refute Map.has_key?(request(relay, frame(2), "other-origin"), "possible_contradictions")
  end

  test "checked result survives same-op replay and restart; new-op duplicate does not check" do
    parent = self()

    evaluator = fn request ->
      send(parent, {:evaluated, request.candidates})
      {:ok, Map.new(request.candidates, &{&1, 1})}
    end

    {relay, path} = start(contradiction_batch_size: 1, contradiction_evaluator: evaluator)
    request(relay, frame(1, %{"display_name" => "First sender"}))
    jev(relay)
    result = request(relay, frame(2))
    check = result["possible_contradictions"]
    assert check["status"] == "checked"
    assert hd(check["messages"])["message_id"] == "m1"
    assert hd(check["messages"])["sender_name"] == "First sender"
    assert_receive {:evaluated, [{"app:test", "m1"}]}, 2_000
    assert request(relay, frame(2))["possible_contradictions"] == check

    refute Map.has_key?(
             request(relay, Map.put(frame(2), "op_id", op(3))),
             "possible_contradictions"
           )

    refute_receive {:evaluated, _}
    stop_supervised!(Relay)

    {:ok, restarted} =
      Relay.start_link(
        store_path: path,
        contradiction_batch_size: 1,
        contradiction_evaluator: evaluator
      )

    on_exit(fn -> if Process.alive?(restarted), do: GenServer.stop(restarted) end)
    assert request(restarted, frame(2))["possible_contradictions"] == check
    refute_receive {:evaluated, _}
  end

  test "delivery, reads, other sends, and in-flight replay do not wait for the check" do
    parent = self()

    evaluator = fn req ->
      send(parent, {:checking, self(), req.candidates})

      receive do
        :finish -> {:ok, Map.new(req.candidates, &{&1, 1})}
      end
    end

    {relay, _} = start(contradiction_batch_size: 8, contradiction_evaluator: evaluator)
    :ok = Relay.enable_push(relay)

    assert request(
             relay,
             %{
               "v" => 1,
               "type" => "push_register",
               "op_id" => op(99),
               "revision" => 1,
               "profile_id" => "harklight-sandbox",
               "address_kind" => "apns_token",
               "address" => String.duplicate("ab", 32)
             },
             "reader"
           )["type"] == "ok"

    request(relay, frame(1))
    jev(relay)
    sending = Task.async(fn -> request(relay, frame(2)) end)
    assert_receive {:checking, worker, [{"app:test", "m1"}]}, 2_000
    assert {:ok, events, _} = Relay.events(relay, "reader", 0)
    assert Enum.any?(events, &(&1["message_id"] == "m2"))
    assert {:ok, claim} = Relay.claim_push(relay, System.system_time(:millisecond) + 300_001)

    assert claim.high ==
             :sys.get_state(relay).store.engine.messages[{"person", "app:test", "m2"}].accepted_seq

    replay = Task.async(fn -> request(relay, frame(2)) end)
    next = Task.async(fn -> request(relay, frame(3)) end)
    assert_receive {:checking, second, _}, 2_000
    assert {:ok, events, _} = Relay.events(relay, "reader", 0)
    assert Enum.any?(events, &(&1["message_id"] == "m3"))
    assert Task.yield(sending, 0) == nil
    send(worker, :finish)
    send(second, :finish)
    first = Task.await(sending)
    assert Task.await(replay)["possible_contradictions"] == first["possible_contradictions"]
    assert Task.await(next)["type"] == "ok"
  end

  test "unfixed size reports unavailable without a comparison call" do
    {relay, _} = start(contradiction_evaluator: fn _ -> flunk("N not measured") end)
    request(relay, frame(1))
    jev(relay)

    assert request(relay, frame(2))["possible_contradictions"] == %{
             "status" => "incomplete",
             "reason" => "unavailable",
             "unchecked" => 1
           }
  end

  test "superseded messages and other people are excluded, own open messages are included" do
    parent = self()

    {relay, _} =
      start(
        contradiction_batch_size: 8,
        contradiction_evaluator: fn req ->
          send(parent, {:evaluated, req.candidates})
          {:ok, Map.new(req.candidates, &{&1, 0})}
        end
      )

    request(relay, frame(1))
    request(relay, frame(2))
    request(relay, frame(5), "other-origin")
    jev(relay)

    refute Map.has_key?(
             request(relay, frame(3, %{"supersedes" => ["m1"]})),
             "possible_contradictions"
           )

    assert_receive {:evaluated, [{"app:test", "m2"}]}, 2_000
  end

  test "a check that never answers waits past the old bound and ends as a recorded failure" do
    parent = self()

    {relay, _} =
      start(
        contradiction_batch_size: 8,
        contradiction_evaluator: fn _ ->
          send(parent, {:waiting, self()})

          # Stands in for the Jev transport: it answers only when the
          # connection fails, the same way JevClient.post/3 ends.
          receive do
            :connection_lost -> {:error, :unavailable}
          end
        end
      )

    request(relay, frame(1))
    jev(relay)
    sending = Task.async(fn -> request(relay, frame(2)) end)
    assert_receive {:waiting, call}, 2_000
    assert {:ok, events, _} = Relay.events(relay, "reader", 0)
    assert Enum.any?(events, &(&1["message_id"] == "m2"))

    # The removed five-second bound no longer ends the wait.
    assert Task.yield(sending, 5_500) == nil
    send(call, :connection_lost)

    failed = %{"status" => "incomplete", "reason" => "unavailable", "unchecked" => 1}
    assert Task.await(sending)["possible_contradictions"] == failed
    # The failure is recorded on the receipt, so the same-op replay copies it.
    assert request(relay, frame(2))["possible_contradictions"] == failed
  end

  test "empty Jev checked set, revision, and unavailable checks preserve delivery" do
    parent = self()

    {relay, _} =
      start(
        contradiction_batch_size: 8,
        contradiction_evaluator: fn req ->
          send(parent, {:evaluated, req.candidates})
          {:error, :unavailable}
        end
      )

    jev(relay)
    original = Map.put(frame(1), "session_handle", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    refute Map.has_key?(request(relay, original), "possible_contradictions")

    revised = %{
      "v" => 1,
      "type" => "conversation_revise",
      "op_id" => op(30),
      "sender_id" => "app:test",
      "message_id" => "m1",
      "base_revision" => 1,
      "message" => Map.put(original["message"], "title", "Revised title")
    }

    assert request(relay, revised)["type"] == "ok"
    refute_receive {:evaluated, _}
    check = request(relay, frame(2))["possible_contradictions"]
    assert check == %{"status" => "incomplete", "reason" => "unavailable", "unchecked" => 1}
    assert_receive {:evaluated, [{"app:test", "m1"}]}, 2_000
    assert {:ok, events, _} = Relay.events(relay, "reader", 0)
    assert Enum.any?(events, &(&1["message_id"] == "m2"))
  end

  test "size exclusion is incomplete, while fitting comparisons still produce matches" do
    parent = self()

    {relay, _} =
      start(
        contradiction_batch_size: 8,
        contradiction_evaluator: fn req ->
          send(parent, {:evaluated, req.candidates})
          {:ok, Map.new(req.candidates, &{&1, 1})}
        end
      )

    request(relay, frame(1, %{"body" => String.duplicate("x", 25_000)}))
    request(relay, frame(2))
    jev(relay)
    check = request(relay, frame(3))["possible_contradictions"]
    assert check["reason"] == "unavailable"
    assert check["unchecked"] == 1
    assert hd(check["messages"])["message_id"] == "m2"
    assert_receive {:evaluated, [{"app:test", "m2"}]}, 2_000

    check =
      request(relay, frame(4, %{"body" => String.duplicate("x", 25_000)}))[
        "possible_contradictions"
      ]

    assert check == %{"status" => "incomplete", "reason" => "unavailable", "unchecked" => 3}
    refute_receive {:evaluated, _}
  end

  test "origin send mapping carries the advisory object unchanged" do
    {relay, _} =
      start(
        contradiction_batch_size: 8,
        contradiction_evaluator: fn req ->
          {:ok, Map.new(req.candidates, &{&1, 1})}
        end
      )

    request(relay, frame(1))
    jev(relay)
    posted = frame(2) |> Map.put("type", "send") |> Map.put("ref", "caller-ref")
    result = AttentionServer.OriginWire.command(relay, "origin", posted)
    assert result["type"] == "ok"
    assert result["ref"] == "caller-ref"
    assert result["possible_contradictions"]["status"] == "checked"
    assert hd(result["possible_contradictions"]["messages"])["message_id"] == "m1"

    assert AttentionServer.OriginWire.command(relay, "origin", posted)["possible_contradictions"] ==
             result["possible_contradictions"]
  end
end
