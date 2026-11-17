defmodule AttentionServer.ContradictionsTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Contradictions, JSON}

  @endpoint "https://example.com/v1/systemone"
  @model "jev-latest"
  @posted %{"title" => "Hold release", "body" => "Wait until the audit completes."}
  @warning "these possibly contradictory messages are still active, please talk to its poster to reconcile them"

  defp item(n, body \\ "Ship Friday.") do
    %{
      identity: {"app:sender", "message-#{n}"},
      accepted_seq: n,
      sender_name: "Example sender",
      message: %{"title" => "Release #{n}", "body" => body}
    }
  end

  test "complete matches are oldest first and expose only identifying fields" do
    first = item(1)
    second = item(2)

    check =
      Contradictions.result(
        [second, first],
        %{first.identity => 1, second.identity => 0.75},
        false
      )

    assert check["status"] == "checked"
    assert check["warning"] == @warning
    assert Enum.map(check["messages"], & &1["message_id"]) == ["message-1", "message-2"]

    assert Enum.all?(
             check["messages"],
             &(Enum.sort(Map.keys(&1)) == ~w(message_id sender_id sender_name title))
           )

    refute Map.has_key?(check, "reason")
    refute Map.has_key?(check, "unchecked")
  end

  test "complete negative comparisons add no field" do
    candidate = item(1)
    assert Contradictions.result([candidate], %{candidate.identity => 0}, false) == nil
  end

  test "missing and invalid answers stay unchecked while usable matches survive" do
    [a, b, c] = Enum.map(1..3, &item/1)

    for scores <- [
          %{a.identity => 1, c.identity => "invalid"},
          %{a.identity => 1, c.identity => 4}
        ] do
      check = Contradictions.result([a, b, c], scores, false)
      assert check["unchecked"] == 2
      assert check["reason"] == "unavailable"
      assert check["status"] == "incomplete"
      assert check["warning"] == @warning
      assert length(check["messages"]) == 1
    end

    assert Contradictions.result([a, b], %{}, false) == %{
             "status" => "incomplete",
             "reason" => "unavailable",
             "unchecked" => 2
           }
  end

  test "full title and body are supplied and oversize comparisons are split or excluded" do
    oversized = item(1, String.duplicate("x", 24_000))
    normal = item(2)

    assert {:error, :request_too_large} =
             Contradictions.compose(@posted, [oversized], @endpoint, @model)

    [request] = Contradictions.batches(@posted, [oversized, normal], @endpoint, @model, 8)
    assert request.candidates == [normal.identity]
    assert {:ok, body} = JSON.decode_line(request.body <> "\n")
    assert body["state"]["posted_message"] == @posted
    assert body["state"]["open_messages"] == [normal.message]

    assert Contradictions.batches(
             %{@posted | "body" => oversized.message["body"]},
             [normal],
             @endpoint,
             @model,
             8
           ) == []
  end

  test "one failed batch does not cancel an eligible comparison" do
    [a, b] = Enum.map(1..2, &item/1)

    evaluator = fn request ->
      if request.candidates == [a.identity],
        do: {:error, :unavailable},
        else: {:ok, %{b.identity => 1}}
    end

    check = Contradictions.run(@posted, [a, b], @endpoint, @model, 1, evaluator: evaluator)
    assert check["reason"] == "unavailable"
    assert check["unchecked"] == 1
    assert hd(check["messages"])["message_id"] == "message-2"
  end

  test "one shared deadline retains ready matches and discards late batches" do
    [a, b] = Enum.map(1..2, &item/1)
    parent = self()

    evaluator = fn request ->
      if request.candidates == [a.identity] do
        {:ok, %{a.identity => 1}}
      else
        send(parent, {:pending, self()})

        receive do
          :finish -> {:ok, %{b.identity => 1}}
        end
      end
    end

    check =
      Contradictions.run(@posted, [a, b], @endpoint, @model, 1,
        evaluator: evaluator,
        deadline: System.monotonic_time(:millisecond) + 100
      )

    assert check["reason"] == "timeout"
    assert check["unchecked"] == 1
    assert hd(check["messages"])["message_id"] == "message-1"
    assert_receive {:pending, worker}
    refute Process.alive?(worker)
    refute_receive {_, {:ok, _}}
  end

  test "unconfigured Jev reports incomplete without calling the evaluator" do
    check =
      Contradictions.run(@posted, [item(1), item(2)], "", "", 1,
        evaluator: fn _ -> flunk("unconfigured comparison must not run") end
      )

    assert check == %{"status" => "incomplete", "reason" => "unavailable", "unchecked" => 2}
  end

  test "unexpected worker death does not lose other batch results" do
    [a, b] = Enum.map(1..2, &item/1)

    evaluator = fn request ->
      if request.candidates == [a.identity] do
        Process.exit(self(), :kill)
      else
        {:ok, %{b.identity => 1}}
      end
    end

    check = Contradictions.run(@posted, [a, b], @endpoint, @model, 1, evaluator: evaluator)
    assert check["unchecked"] == 1
    assert check["reason"] == "unavailable"
    assert hd(check["messages"])["message_id"] == "message-2"
  end

  test "timeout takes precedence over a failed comparison" do
    [a, b] = Enum.map(1..2, &item/1)

    evaluator = fn request ->
      if request.candidates == [a.identity] do
        {:error, :unavailable}
      else
        receive do
          :finish -> {:ok, %{b.identity => 0}}
        end
      end
    end

    check =
      Contradictions.run(@posted, [a, b], @endpoint, @model, 1,
        evaluator: evaluator,
        deadline: System.monotonic_time(:millisecond) + 100
      )

    assert check == %{"status" => "incomplete", "reason" => "timeout", "unchecked" => 2}
  end
end
