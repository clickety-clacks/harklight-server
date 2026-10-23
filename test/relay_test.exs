defmodule AttentionServer.RelayTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{JSON, Relay}
  alias AttentionServer.JSON.DecimalToken

  setup do
    relay = start_supervised!({Relay, []})
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "A", "a1")
    assert :ok = Relay.setup_device(relay, "A", "a2")
    assert :ok = Relay.setup_device(relay, "B", "b1")
    assert :ok = Relay.setup_sender(relay, "a1", "app:test")
    assert :ok = Relay.setup_sender(relay, "b1", "app:test")
    %{relay: relay}
  end

  defp op(n),
    do:
      "00000000-0000-4000-8000-" <>
        (n |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(12, "0"))

  defp message(id \\ "m") do
    %{
      "sender_id" => "app:test",
      "message_id" => id,
      "created_at" => 1,
      "title" => "first",
      "unknown" => %{
        "big" => 9_007_199_254_740_993,
        "decimal" => %DecimalToken{raw: "0.12345678901234567890123"}
      },
      "open" => %{"argv" => ["never-run"], "terminal" => false}
    }
  end

  defp request(relay, key, frame) do
    raw = JSON.encode!(Map.put_new(frame, "v", 1)) <> "\n"

    case Relay.request_line(relay, key, raw) do
      {:ok, line} ->
        assert {:ok, reply} = JSON.decode_line(line)
        reply

      other ->
        other
    end
  end

  test "tenant-scoped first copy and origin privacy survive forged envelope identity", %{
    relay: relay
  } do
    original = message("same")

    assert %{"type" => "ok", "seq" => 1, "ref" => "r1"} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(1),
               "message" => original,
               "person_id" => "B",
               "device_id" => "b1",
               "origin" => "b1",
               "ref" => "r1"
             })

    assert {:ok, [origin], 1} = Relay.events(relay, "a1", 0)
    assert Map.keys(origin) |> Enum.sort() == ~w(message_id origin sender_id seq type v)
    assert origin["origin"] == "a1"
    refute String.contains?(JSON.encode!(origin), "first")

    assert {:ok, [remote], 1} = Relay.events(relay, "a2", 0)
    assert remote["message"] == original
    assert remote["as"] == "knock"
    assert {:ok, [], 0} = Relay.events(relay, "b1", 0)

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "b1", %{
               "type" => "forward",
               "op_id" => op(1),
               "message" => %{original | "title" => "B secret"}
             })

    assert {:ok, [b_origin], 1} = Relay.events(relay, "b1", 0)
    assert b_origin["origin"] == "b1"
    assert {:ok, [a_remote], 1} = Relay.events(relay, "a2", 0)
    assert a_remote["message"]["title"] == "first"
  end

  test "sender authority is per device and exact or literal prefix, not an envelope claim", %{
    relay: relay
  } do
    assert %{"reason" => "sender_not_authorized"} =
             request(relay, "a2", %{"type" => "forward", "op_id" => op(3), "message" => message()})

    assert %{"reason" => "sender_not_authorized"} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(4),
               "message" => %{message() | "sender_id" => "app:testing"}
             })

    assert :ok = Relay.setup_sender(relay, "a1", "agentd://host/")

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(5),
               "message" => %{message() | "sender_id" => "agentd://host/app"}
             })

    assert %{"reason" => "sender_not_authorized"} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(6),
               "message" => %{message("other") | "sender_id" => "agentd://hostile/app"}
             })

    assert {:ok, [_], 1} = Relay.events(relay, "a1", 0)

    # The earlier rejected operation was not reserved; granting this device
    # permits the same stable operation ID to succeed once.
    assert :ok = Relay.setup_sender(relay, "a2", "app:test")

    assert %{"type" => "ok", "seq" => 2} =
             request(relay, "a2", %{
               "type" => "forward",
               "op_id" => op(3),
               "message" => message("after-grant")
             })
  end

  test "duplicate forward and retry receipts never replace first content or reopen", %{
    relay: relay
  } do
    original = message()

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "a1", %{"type" => "forward", "op_id" => op(7), "message" => original})

    assert %{"type" => "ok", "seq" => 1, "duplicate" => true} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(7),
               "message" => original,
               "ref" => "later"
             })

    assert %{"reason" => "operation_conflict"} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(7),
               "message" => %{original | "title" => "changed"}
             })

    assert %{"type" => "ok", "seq" => 1, "duplicate" => true} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(8),
               "message" => %{original | "title" => "changed"}
             })

    assert {:ok, [remote], 1} = Relay.events(relay, "a2", 0)
    assert remote["message"] == original
  end

  test "answer null, origin-only value, stable retry and truthful closed result", %{relay: relay} do
    assert %{"seq" => 1} =
             request(relay, "a1", %{"type" => "forward", "op_id" => op(9), "message" => message()})

    assert %{"reason" => "unknown_message"} =
             request(relay, "b1", %{
               "type" => "answer",
               "op_id" => op(10),
               "sender_id" => "app:test",
               "message_id" => "m",
               "answer" => "cross-person"
             })

    assert %{"type" => "ok", "seq" => 2} =
             request(relay, "a2", %{
               "type" => "answer",
               "op_id" => op(11),
               "sender_id" => "app:test",
               "message_id" => "m",
               "answer" => nil
             })

    assert {:ok, [_, origin_terminal], 2} = Relay.events(relay, "a1", 0)
    assert Map.has_key?(origin_terminal, "answer")
    assert is_nil(origin_terminal["answer"])
    assert origin_terminal["by"] == "answer"
    refute Map.has_key?(origin_terminal, "terminal_actor")
    assert {:ok, [_, remote_terminal], 2} = Relay.events(relay, "a2", 0)
    refute Map.has_key?(remote_terminal, "answer")

    assert %{"type" => "ok", "seq" => 2, "duplicate" => true} =
             request(relay, "a2", %{
               "type" => "answer",
               "op_id" => op(11),
               "sender_id" => "app:test",
               "message_id" => "m",
               "answer" => nil
             })

    assert %{"reason" => "operation_conflict"} =
             request(relay, "a2", %{
               "type" => "answer",
               "op_id" => op(11),
               "sender_id" => "app:test",
               "message_id" => "m",
               "answer" => false
             })

    assert %{"reason" => "invalid_field", "field" => "answer"} =
             request(relay, "a2", %{
               "type" => "answer",
               "op_id" => op(22),
               "sender_id" => "app:test",
               "message_id" => "m"
             })

    assert %{"reason" => "closed", "terminal" => %{"seq" => 2, "answer" => nil}} =
             request(relay, "a1", %{
               "type" => "lifecycle",
               "op_id" => op(12),
               "sender_id" => "app:test",
               "message_id" => "m",
               "state" => "cleared",
               "at" => 0
             })

    assert %{"reason" => "closed", "terminal" => losing_view} =
             request(relay, "a2", %{
               "type" => "answer",
               "op_id" => op(13),
               "sender_id" => "app:test",
               "message_id" => "m",
               "answer" => "late"
             })

    refute Map.has_key?(losing_view, "answer")

    assert %{"type" => "ok", "seq" => 1, "duplicate" => true} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(23),
               "message" => %{message() | "title" => "cannot reopen"}
             })

    assert {:ok, [_, _], 2} = Relay.events(relay, "a1", 0)

    assert :ok = Relay.revoke_device(relay, "a2")

    assert {:error, :forbidden} =
             request(relay, "a2", %{
               "type" => "answer",
               "op_id" => op(11),
               "sender_id" => "app:test",
               "message_id" => "m",
               "answer" => nil
             })
  end

  test "concurrent contenders serialize to one terminal event", %{relay: relay} do
    assert %{"seq" => 1} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(14),
               "message" => message("race")
             })

    contenders = [
      {"a1", op(15), "X"},
      {"a2", op(16), "Y"}
    ]

    results =
      contenders
      |> Enum.map(fn {key, operation, value} ->
        Task.async(fn ->
          request(relay, key, %{
            "type" => "answer",
            "op_id" => operation,
            "sender_id" => "app:test",
            "message_id" => "race",
            "answer" => value
          })
        end)
      end)
      |> Enum.map(&Task.await/1)

    assert Enum.sort(Enum.map(results, & &1["type"])) == ["error", "ok"]
    assert Enum.any?(results, &match?(%{"reason" => "closed"}, &1))
    assert {:ok, [_, terminal], 2} = Relay.events(relay, "a1", 0)
    assert terminal["answer"] in ["X", "Y"]
  end

  test "value-less answered lifecycle is not an answer and origin authority is enforced", %{
    relay: relay
  } do
    assert %{"seq" => 1} =
             request(relay, "a1", %{
               "type" => "forward",
               "op_id" => op(17),
               "message" => message("local")
             })

    assert %{"reason" => "not_origin"} =
             request(relay, "a2", %{
               "type" => "lifecycle",
               "op_id" => op(18),
               "sender_id" => "app:test",
               "message_id" => "local",
               "state" => "expired",
               "at" => 0
             })

    assert %{"type" => "ok", "seq" => 2} =
             request(relay, "a1", %{
               "type" => "lifecycle",
               "op_id" => op(19),
               "sender_id" => "app:test",
               "message_id" => "local",
               "state" => "answered",
               "at" => 1,
               "by" => "self-reported"
             })

    assert {:ok, [_, result], 2} = Relay.events(relay, "a1", 0)
    assert result["state"] == "answered"
    refute Map.has_key?(result, "answer")
  end

  test "revocation, invalid frames and unsupported requests cannot mutate tenant state", %{
    relay: relay
  } do
    assert {:error, :forbidden} = Relay.request_line(relay, "unknown", "{oops}\n")
    assert :ok = Relay.revoke_device(relay, "a2")
    assert {:error, :forbidden} = Relay.events(relay, "a2", 0)

    for raw <- ["{oops}\n", "{}", "1.5\n", "{\"v\":1,\"v\":1}\n"] do
      assert {:ok, line} = Relay.request_line(relay, "a1", raw)
      assert {:ok, %{"reason" => "bad_frame"}} = JSON.decode_line(line)
    end

    assert %{"reason" => "invalid_field", "field" => "ref"} =
             request(relay, "a1", %{
               "type" => "forward",
               "ref" => 2,
               "op_id" => op(20),
               "message" => message()
             })

    assert %{"reason" => "unsupported_version"} =
             request(relay, "a1", %{
               "v" => %DecimalToken{raw: "1.0"},
               "type" => "forward",
               "op_id" => op(21),
               "message" => message()
             })

    assert %{"reason" => "not_supported"} =
             request(relay, "a1", %{"type" => "resync", "since" => 0})

    assert %{"reason" => "unknown_type"} = request(relay, "a1", %{"type" => "invented"})
    assert {:ok, [], 0} = Relay.events(relay, "a1", 0)
  end
end
