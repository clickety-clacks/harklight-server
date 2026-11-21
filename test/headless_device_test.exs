defmodule AttentionServer.HeadlessDeviceTest do
  # The device CLI's command surface. Behavior over client API v2 is covered in
  # headless_device_v2_test.exs.
  use ExUnit.Case, async: false

  alias AttentionServer.HeadlessDevice

  test "device CLI no longer exposes the agent send or receipt commands" do
    Code.ensure_loaded!(HeadlessDevice)

    assert_raise Mix.Error, ~r/use read or respond/, fn ->
      AttentionServer.CLI.with_io(Mix.Shell.Process, Mix.Error, fn ->
        AttentionServer.CLI.Device.run(["send"])
      end)
    end

    assert_raise Mix.Error, ~r/use read or respond/, fn ->
      AttentionServer.CLI.with_io(Mix.Shell.Process, Mix.Error, fn ->
        AttentionServer.CLI.Device.run(["receipt"])
      end)
    end

    refute function_exported?(HeadlessDevice, :send_message, 3)
    refute function_exported?(HeadlessDevice, :receipt, 4)
  end
end
