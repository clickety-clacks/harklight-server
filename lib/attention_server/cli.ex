defmodule AttentionServer.CLI do
  @moduledoc """
  Release-compatible entry point for the existing operator, owner, device
  enrollment and authorized-agent tools. Transport and approval rules are shared
  with the historical Mix commands.
  """

  def main do
    args =
      System.fetch_env!("HARKLIGHT_CLI_ARGS")
      |> Base.decode64!()
      |> String.split(<<0>>, trim: false)
      |> Enum.drop(-1)

    try do
      run(args)
    rescue
      error in RuntimeError ->
        IO.puts(:stderr, Exception.message(error))
        System.halt(2)
    end
  end

  def run(["operator" | args]), do: AttentionServer.CLI.Operator.run(args)
  def run(["enroll" | args]), do: AttentionServer.CLI.Enroll.run(args)
  def run(["owner" | args]), do: AttentionServer.CLI.Owner.run(args)
  def run(["device" | args]), do: AttentionServer.CLI.Device.run(args)
  def run(["agent" | args]), do: AttentionServer.CLI.Agent.run(args)

  def run(["help"]),
    do: IO.puts("harklight operator|enroll|owner|device|agent ...; see SELF_HOST.md")

  def run(_), do: error!("use harklight operator|enroll|owner|device|agent; see SELF_HOST.md")

  def start_application! do
    case Application.ensure_all_started(:attention_server) do
      {:ok, _} -> :ok
      {:error, reason} -> error!("cannot start local tool dependencies: #{inspect(reason)}")
    end
  end

  def shell, do: Process.get(:harklight_cli_shell, AttentionServer.CLI.Shell)
  def error!(message), do: raise(Process.get(:harklight_cli_error, RuntimeError), message)

  def with_io(shell, error, fun) do
    old_shell = Process.get(:harklight_cli_shell)
    old_error = Process.get(:harklight_cli_error)
    Process.put(:harklight_cli_shell, shell)
    Process.put(:harklight_cli_error, error)

    try do
      fun.()
    after
      restore(:harklight_cli_shell, old_shell)
      restore(:harklight_cli_error, old_error)
    end
  end

  defp restore(key, nil), do: Process.delete(key)
  defp restore(key, value), do: Process.put(key, value)
end

defmodule AttentionServer.CLI.Shell do
  @moduledoc false
  def info(message), do: IO.puts(message)
  def prompt(message), do: IO.gets(message <> " ")
end
