defmodule AttentionServer.JSONTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{JSON, Message}
  alias AttentionServer.JSON.DecimalToken

  test "large integers, precise decimals, unknown values and array order survive JSON roundtrip" do
    raw =
      ~s|{"future":{"n":9007199254740993,"d":0.123456789012345678901234567890,"array":[null,false,"é",17]},"created_at":-1}| <>
        "\n"

    assert {:ok, value} = JSON.decode_line(raw)
    assert value["future"]["n"] == 9_007_199_254_740_993
    assert value["future"]["d"] == %DecimalToken{raw: "0.123456789012345678901234567890"}
    assert value["future"]["array"] == [nil, false, "é", 17]

    assert {:ok, ^value} = JSON.decode_line(JSON.encode!(value) <> "\n")
    assert String.contains?(JSON.encode!(value), "0.123456789012345678901234567890")
  end

  test "malformed, duplicate-key, trailing, non-newline and over-bound frames fail closed" do
    assert {:error, :bad_frame} = JSON.decode_line("{oops}\n")
    assert {:error, :bad_frame} = JSON.decode_line("{\"a\":1,\"a\":2}\n")
    assert {:error, :bad_frame} = JSON.decode_line("{\"nested\":{\"a\":1,\"a\":2}}\n")
    assert {:error, :bad_frame} = JSON.decode_line("{} true\n")
    assert {:error, :bad_frame} = JSON.decode_line("{}")
    assert {:error, :bad_frame} = JSON.decode_line("{}\n" <> :binary.copy("x", 2_097_152))
    assert {:error, :bad_frame} = JSON.decode_line(<<0xFF, ?\n>>)
    assert_raise ArgumentError, fn -> JSON.encode!(%DecimalToken{raw: "1,\"unsafe\":2"}) end
  end

  test "a wrapped local-size message fits while the input bound is explicit" do
    value = %{"v" => 1, "type" => "forward", "message" => %{"body" => :binary.copy("x", 65_536)}}
    assert {:ok, ^value} = JSON.decode_line(JSON.encode!(value) <> "\n")
  end

  test "sender identity and message schema retain valid unknown fields but reject bad authority identifiers" do
    assert Message.sender_id?("agentd://Host/app")
    refute Message.sender_id?("Agentd://host/app")
    refute Message.sender_id?("app:test wrong")
    refute Message.sender_id?("app:test\nwrong")
    refute Message.sender_id?("app:")

    message = %{
      "sender_id" => "app:MixedCase",
      "message_id" => "m",
      "created_at" => 1,
      "title" => "T",
      "open" => %{"argv" => ["never-run"], "terminal" => false, "future" => 1},
      "callback" => %{"anything" => [nil, %DecimalToken{raw: "1.234567890123456789"}]},
      "future" => true
    }

    assert :ok = Message.validate(message)
    assert {:error, "created_at"} = Message.validate(%{message | "created_at" => true})

    assert {:error, "open"} =
             Message.validate(%{message | "open" => %{"argv" => [], "terminal" => false}})

    assert {:error, "callback"} =
             Message.validate(%{message | "callback" => %DecimalToken{raw: "1.5"}})

    assert {:error, "answer_ui"} =
             Message.validate(Map.put(message, "answer_ui", %DecimalToken{raw: "1.5"}))

    assert {:error, "message"} = Message.validate(nil)
  end
end
