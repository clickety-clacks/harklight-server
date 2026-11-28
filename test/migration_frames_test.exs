defmodule AttentionServer.Migration.FramesTest do
  use ExUnit.Case, async: true
  alias AttentionServer.Migration.Frames

  setup do
    root = Path.join(System.tmp_dir!(), "migration-frames-#{System.unique_integer([:positive])}")
    File.mkdir!(root)
    File.chmod!(root, 0o700)
    on_exit(fn -> File.rm_rf!(root) end)
    %{path: Path.join(root, "records.frames")}
  end

  test "round trips typed facts and refuses overwrite", %{path: path} do
    facts = [
      {{:field, :key}, <<0, 255>>},
      {{:row, :receipts, {"p", "a", 3}}, %{set: MapSet.new([1, 2]), exact: "1.00"}}
    ]

    assert {:ok, summary} = Frames.write(path, facts)
    assert summary.count == 2

    assert {:ok, ^facts} =
             Frames.fold(path, summary, [], fn row, rows -> {:ok, rows ++ [row]} end)

    assert {:error, :eexist} = Frames.write(path, [:replacement])
    assert Bitwise.band(File.stat!(path).mode, 0o777) == 0o600
  end

  test "checks truncation and corruption before consumer writes", %{path: path} do
    {:ok, summary} = Frames.write(path, [{:a, 1}, {:b, 2}])
    original = File.read!(path)
    consume = fn _, _ -> flunk("consumer must not run on an invalid bundle") end
    File.write!(path, binary_part(original, 0, byte_size(original) - 1))
    assert {:error, :truncated_frame_file} = Frames.fold(path, summary, nil, consume)
    <<prefix::binary-size(byte_size(original) - 1), last>> = original
    File.write!(path, prefix <> <<Bitwise.bxor(last, 1)>>)
    assert {:error, :invalid_frame_checksum} = Frames.fold(path, summary, nil, consume)
  end

  test "a well-formed shorter export cannot match the expected manifest", %{path: path} do
    {:ok, summary} = Frames.write(path, [1, 2])
    File.rm!(path)
    {:ok, _} = Frames.write(path, [1])

    assert {:error, :frame_manifest_mismatch} =
             Frames.fold(path, summary, nil, fn _, _ -> flunk("must verify before consuming") end)
  end
end
