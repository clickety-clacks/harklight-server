defmodule AttentionServer.BouncePolicyTest do
  use ExUnit.Case, async: true

  alias AttentionServer.BouncePolicy
  alias AttentionServer.JSON.DecimalToken
  alias AttentionServer.LargeData.Ref

  test "configured ranges form an ordered inclusive partition from zero through one" do
    assert {:ok, levels} =
             BouncePolicy.normalize_levels([
               %{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil},
               %{
                 "name" => "Follow up",
                 "lower_bound" => %DecimalToken{raw: "0.4"},
                 "bounce_after_days" => %DecimalToken{raw: "1.5"}
               },
               %{"name" => "Urgent", "lower_bound" => 0.8, "bounce_after_days" => 2}
             ])

    assert {:ok, first} = BouncePolicy.select_level(levels, 0)
    assert first["name"] == "Routine"
    assert {:ok, second} = BouncePolicy.select_level(levels, %DecimalToken{raw: "0.4"})
    assert second["name"] == "Follow up"
    assert second["interval_ms"] == 129_600_000
    assert {:ok, last} = BouncePolicy.select_level(levels, 1)
    assert last["name"] == "Urgent"
  end

  test "approved defaults preserve the continuous score ranges and bounce delays" do
    levels = BouncePolicy.default_levels()

    assert BouncePolicy.public_levels(levels) == [
             %{"name" => "Low", "lower_bound" => 0, "bounce_after_days" => 7},
             %{"name" => "Normal", "lower_bound" => 0.3, "bounce_after_days" => 3},
             %{"name" => "High", "lower_bound" => 0.6, "bounce_after_days" => 1},
             %{"name" => "Urgent", "lower_bound" => 0.8, "bounce_after_days" => 0.5}
           ]

    assert {:ok, low} = BouncePolicy.select_level(levels, %DecimalToken{raw: "0.299"})
    assert low["interval_ms"] == 604_800_000

    assert {:ok, normal} = BouncePolicy.select_level(levels, %DecimalToken{raw: "0.3"})
    assert normal["interval_ms"] == 259_200_000

    assert {:ok, high} = BouncePolicy.select_level(levels, %DecimalToken{raw: "0.6"})
    assert high["interval_ms"] == 86_400_000

    assert {:ok, urgent} = BouncePolicy.select_level(levels, %DecimalToken{raw: "0.8"})
    assert urgent["interval_ms"] == 43_200_000
  end

  test "empty, incomplete, repeated, unordered and out-of-range tables are rejected" do
    base = [
      %{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil},
      %{"name" => "High", "lower_bound" => 0.6, "bounce_after_days" => 2}
    ]

    assert {:error, :invalid_levels} = BouncePolicy.normalize_levels([])
    assert {:error, :invalid_levels} = BouncePolicy.normalize_levels(Enum.drop(base, 1))

    assert {:error, :invalid_levels} =
             BouncePolicy.normalize_levels([hd(base), %{List.last(base) | "lower_bound" => 0}])

    assert {:error, :invalid_levels} =
             BouncePolicy.normalize_levels([hd(base), %{List.last(base) | "lower_bound" => 1.1}])

    assert {:error, :invalid_levels} =
             BouncePolicy.normalize_levels([
               hd(base),
               Map.delete(List.last(base), "bounce_after_days")
             ])

    assert {:error, :invalid_levels} =
             BouncePolicy.normalize_levels([
               hd(base),
               %{List.last(base) | "bounce_after_days" => 0}
             ])
  end

  test "disabled intervals remain explicit and positive fractional days round up exactly" do
    assert {:ok, nil} = BouncePolicy.interval_ms(nil)
    assert {:error, :invalid_interval} = BouncePolicy.interval_ms(0)
    assert {:error, :invalid_interval} = BouncePolicy.interval_ms(-1)
    assert {:ok, 1} = BouncePolicy.interval_ms(%DecimalToken{raw: "0.0000000000000000001"})
    assert {:ok, 2} = BouncePolicy.interval_ms(%DecimalToken{raw: "0.000000012731481481481482"})
  end

  test "identical numeric values survive representation changes without changing the schedule" do
    assert {:ok, from_json} =
             BouncePolicy.normalize_levels([
               %{
                 "name" => "Routine",
                 "lower_bound" => %DecimalToken{raw: "0.0"},
                 "bounce_after_days" => nil
               },
               %{
                 "name" => "High",
                 "lower_bound" => %DecimalToken{raw: "0.60"},
                 "bounce_after_days" => %DecimalToken{raw: "1.500"}
               }
             ])

    assert {:ok, from_numbers} =
             BouncePolicy.normalize_levels([
               %{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil},
               %{"name" => "High", "lower_bound" => 0.6, "bounce_after_days" => 1.5}
             ])

    assert BouncePolicy.equivalent_levels?(from_json, from_numbers)
    assert BouncePolicy.valid_normalized_levels?(from_json)

    changed = List.update_at(from_numbers, 1, &Map.put(&1, "bounce_after_days", 1))
    refute BouncePolicy.equivalent_levels?(from_json, changed)
  end

  test "substantive comparisons include authored content and attachment bytes, not upload ids" do
    message = %{"title" => "Check", "urgency" => "normal", "message_type" => "fyi"}
    file = %{"name" => "notes.txt", "size" => 3, "sha256" => "abc", "upload_id" => "old"}
    replayed_file = %{file | "upload_id" => "new"}

    assert BouncePolicy.same_substance?(message, [file], message, [replayed_file])

    refute BouncePolicy.same_substance?(message, [file], Map.put(message, "urgency", "high"), [
             file
           ])

    refute BouncePolicy.same_substance?(message, [file], message, [%{file | "sha256" => "def"}])
  end

  test "large message data compares by content, wherever its bytes are held" do
    hash = String.duplicate("a", 64)
    durable = %Ref{id: hash, sha256: hash, size: 3_000_000, path: "/state/uploads/" <> hash}
    transient = %Ref{durable | path: "/tmp/attention-json-body-1"}
    other = String.duplicate("b", 64)
    message = %{"title" => "Large", "message_type" => "fyi", "body" => durable}

    assert BouncePolicy.same_substance?(message, [], %{message | "body" => transient}, [])

    refute BouncePolicy.same_substance?(
             message,
             [],
             %{message | "body" => %Ref{id: other, sha256: other, size: 3_000_000, path: nil}},
             []
           )

    refute BouncePolicy.same_substance?(message, [], %{message | "body" => "small"}, [])
  end

  test "inline and referenced forms of the same large content are the same substance" do
    bytes = :binary.copy("x", 2 * 1024 * 1024 + 1)
    hash = Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
    ref = %Ref{id: hash, sha256: hash, size: byte_size(bytes), path: "/state/uploads/" <> hash}
    inline = %{"title" => "Large", "message_type" => "fyi", "body" => bytes}
    stored = %{inline | "body" => ref}

    assert BouncePolicy.same_substance?(stored, [], inline, [])
    assert BouncePolicy.same_substance?(inline, [], stored, [])

    changed = :binary.copy("y", byte_size(bytes))
    refute BouncePolicy.same_substance?(stored, [], %{inline | "body" => changed}, [])
    refute BouncePolicy.same_substance?(%{inline | "body" => changed}, [], stored, [])
  end
end
