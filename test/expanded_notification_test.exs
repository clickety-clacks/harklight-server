defmodule AttentionServer.ExpandedNotificationTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{APNS.Expansion, JSON, LargeData}

  @server "0123456789abcdef0123456789abcdef"

  defp legacy do
    %{
      "aps" => %{
        "alert" => %{"title" => "3 new requests", "body" => "Synthetic sender"},
        "content-available" => 1,
        "badge" => 9
      },
      "attention" => %{"v" => 1, "type" => "sync", "server" => @server}
    }
  end

  defp batch(titles),
    do: %{presentation: :alert, expanded: %{titles: titles, total_count: length(titles)}}

  defp without_expansion(payload) do
    payload
    |> update_in(["aps"], &Map.delete(&1, "category"))
    |> update_in(["attention"], &Map.delete(&1, "expanded"))
  end

  defp expanded(payload), do: payload["attention"]["expanded"]

  test "single and chat use only the delivered preview and preserve their locator" do
    locator = %{
      "v" => 1,
      "binding_id" => "00000000-0000-4000-8000-000000000001",
      "person_id" => "synthetic-person",
      "device_id" => "synthetic-device",
      "sender_id" => "app:synthetic",
      "message_id" => "synthetic-message",
      "acceptance_seq" => 7
    }

    for target <- [locator, Map.put(locator, "target", "chat")] do
      base =
        legacy()
        |> put_in(["aps", "alert"], %{"title" => "Sender", "body" => "Final fitted…"})
        |> put_in(["attention", "locator"], target)

      actual = Expansion.add(base, %{presentation: :alert, expanded: :single})
      assert actual["aps"]["category"] == "harklight.expanded.single.v1"
      assert without_expansion(actual) == base
      refute Map.has_key?(actual["attention"], "expanded")

      if target["target"] == "chat", do: assert_fixture(actual, "chat")
    end
  end

  test "whole titles keep order, duplicates and UTF-8/JSON escapes; count is not badge" do
    titles = ["Same title", "Same title", "Quote \"slash\\\n雪 👩‍👩‍👧‍👦"]
    actual = Expansion.add(legacy(), batch(titles))
    assert expanded(actual) == %{"v" => 1, "titles" => titles, "total_count" => 3}
    assert actual["aps"]["badge"] == 9
    assert actual["aps"]["category"] == "harklight.expanded.batch.v1"
    assert without_expansion(actual) == legacy()
    refute Map.has_key?(actual["aps"], "sound")
    assert {:ok, ^actual} = JSON.decode_line(JSON.encode!(actual) <> "\n")
    assert_fixture(actual, "batch")
  end

  test "the exact 4096 byte boundary includes server and does not reserve it twice" do
    empty = Expansion.add(legacy(), batch([String.duplicate("x", 5_000)]))
    available = 4_096 - byte_size(JSON.encode!(empty)) - 2
    title = String.duplicate("x", available)
    actual = Expansion.add(legacy(), batch([title]))
    assert expanded(actual)["titles"] == [title]
    assert byte_size(JSON.encode!(actual)) == 4_096
    assert without_expansion(actual) == legacy()

    over = Expansion.add(legacy(), batch([title <> "x"]))
    assert expanded(over) == %{"v" => 1, "titles" => [], "total_count" => 1}
  end

  test "encoded escaping and multibyte content decide fit rather than character count" do
    for title <- [String.duplicate("\"", 2_000), String.duplicate("雪", 1_400)] do
      actual = Expansion.add(legacy(), batch([title, "later title"]))
      assert expanded(actual) == %{"v" => 1, "titles" => [], "total_count" => 2}
      assert without_expansion(actual) == legacy()
      assert byte_size(JSON.encode!(actual)) <= 4_096
    end
  end

  test "a non-fitting entry ends the prefix instead of skipping ahead or truncating it" do
    actual = Expansion.add(legacy(), batch(["first", String.duplicate("x", 4_096), "last"]))
    assert expanded(actual) == %{"v" => 1, "titles" => ["first"], "total_count" => 3}
    assert without_expansion(actual) == legacy()
  end

  test "when even the envelope or category cannot fit the exact legacy bytes survive" do
    base = put_in(legacy(), ["aps", "alert", "body"], "")

    base =
      put_in(
        base,
        ["aps", "alert", "body"],
        String.duplicate("x", 4_096 - byte_size(JSON.encode!(base)))
      )

    assert byte_size(JSON.encode!(base)) == 4_096

    for claim <- [batch(["one", "two"]), %{presentation: :alert, expanded: :single}] do
      actual = Expansion.add(base, claim)
      assert actual == base
      assert JSON.encode!(actual) == JSON.encode!(base)
      refute Map.has_key?(actual["aps"], "category")
      refute Map.has_key?(actual["attention"], "expanded")
    end
  end

  test "background and older claims are untouched even if expansion data is present" do
    base = update_in(legacy(), ["aps"], &Map.delete(&1, "alert"))
    assert Expansion.add(base, %{batch(["one"]) | presentation: :background}) == base
    assert Expansion.add(legacy(), %{presentation: :alert}) == legacy()
  end

  test "an expansion encoding failure returns the complete legacy payload" do
    actual = Expansion.add(legacy(), batch(["valid first title", <<255>>]))
    assert actual == legacy()
    assert JSON.encode!(actual) == JSON.encode!(legacy())
    refute Map.has_key?(actual["aps"], "category")
    refute Map.has_key?(actual["attention"], "expanded")
  end

  test "an oversized reference remains undisplayed without substituting a preview" do
    ref = %LargeData.Ref{id: "synthetic", sha256: "synthetic", size: 3_000_000, path: nil}
    actual = Expansion.add(legacy(), batch([ref, "later title"]))
    assert expanded(actual) == %{"v" => 1, "titles" => [], "total_count" => 2}
  end

  @tag :tmp_dir
  test "a fitting reference provides the whole verified title from existing local content", %{
    tmp_dir: dir
  } do
    title = "  Whole \"title\"\n雪  "
    ref = reference(dir, title)
    actual = Expansion.add(legacy(), batch([ref, title]))
    assert expanded(actual)["titles"] == [title, title]
    assert without_expansion(actual) == legacy()
  end

  @tag :tmp_dir
  test "missing, changed or invalid reference content leaves a truthful remainder", %{
    tmp_dir: dir
  } do
    ref = reference(dir, "expected")

    for invalid <- [
          %{ref | path: Path.join(dir, "absent")},
          %{ref | size: 1},
          %{ref | sha256: String.duplicate("0", 64)}
        ] do
      actual = Expansion.add(legacy(), batch(["first", invalid, "last"]))
      assert expanded(actual) == %{"v" => 1, "titles" => ["first"], "total_count" => 3}
      assert without_expansion(actual) == legacy()
    end
  end

  @tag :tmp_dir
  test "a small reference with oversized file content is not rendered", %{tmp_dir: dir} do
    ref = reference(dir, String.duplicate("x", 2_100_000))
    actual = Expansion.add(legacy(), batch([%{ref | size: 1}]))
    assert expanded(actual)["titles"] == []
  end

  defp reference(dir, text) do
    path = Path.join(dir, "title")
    File.write!(path, text)
    hash = Base.encode16(:crypto.hash(:sha256, text), case: :lower)
    %LargeData.Ref{id: hash, sha256: hash, size: byte_size(text), path: path}
  end

  defp assert_fixture(payload, kind) do
    path = Path.join(__DIR__, "fixtures/expanded-notification-#{kind}-v1.json")
    assert File.read!(path) == JSON.encode!(payload) <> "\n"
  end
end
