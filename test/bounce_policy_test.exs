defmodule AttentionServer.BouncePolicyTest do
  use ExUnit.Case, async: true

  alias AttentionServer.BouncePolicy
  alias AttentionServer.JSON.DecimalToken

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
end
