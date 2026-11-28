defmodule AttentionServer.Migration.CLIErrorTest do
  use ExUnit.Case, async: true
  alias AttentionServer.Migration.CLI

  test "retryable validation has a distinct process contract; other failures stay redacted" do
    assert {75, "Migration retryable: validation_retry_required; retry the same command with the same bundle and receipt."} =
      CLI.failure(:validation_retry_required)
    assert {2, "Migration refused: invalid_arguments"} = CLI.failure(:invalid_arguments)
    assert {2, "Migration refused: validation_comparison_failed"} =
      CLI.failure({:validation_comparison_failed, "private source detail"})
    assert {2, "Migration refused: migration_failed"} =
      CLI.failure(%Postgrex.Error{message: "private database detail"})
  end
end
