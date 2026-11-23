defmodule AttentionServer.CLIHelpTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO
  alias AttentionServer.CLI

  test "top-level help aliases show the packaged usage" do
    for flag <- ["help", "--help", "-h"] do
      assert capture_io(fn -> assert :ok = CLI.run([flag]) end) ==
               "harklight operator|enroll|owner|device|agent ...; see SELF_HOST.md\n"
    end
  end

  test "subcommand usage errors point to the packaged guide" do
    for command <- ~w(operator enroll owner device agent) do
      assert_raise RuntimeError, ~r/see SELF_HOST\.md$/, fn ->
        CLI.run([command, "--help"])
      end
    end
  end

  test "an obsolete agent flag points to the packaged guide" do
    assert_raise RuntimeError, ~r/was removed in Harklight 0\.2; see SELF_HOST\.md$/, fn ->
      CLI.run(["agent", "search", "--cursor", "18448"])
    end
  end
end
