defmodule AttentionServer.CLI.StoreConversion do
  @moduledoc false

  alias AttentionServer.{CLI, Store}
  alias AttentionServer.Store.ConversionCapacity

  def run(["convert-stopped", path]), do: convert(path, nil)

  def run(["convert-stopped", path, "--from-release", release]) do
    convert(path, source_format(release))
  end

  def run(_), do: CLI.error!("use harklight store convert-stopped STORE_PATH")

  defp convert(path, expected_format) do
    :ok = CLI.start_application!()

    with true <- File.regular?(path),
         {:ok, budget} <- ConversionCapacity.read(Path.expand(path)),
         {:ok, result} <- Store.convert_stopped(path, budget, expected_format) do
      case result do
        :converted -> IO.puts("Converted stopped Harklight store to FORMAT31.")
        :already_converted -> IO.puts("The stopped Harklight store already uses FORMAT31.")
      end
    else
      false -> CLI.error!("The stopped Harklight store file is missing.")
      {:error, :fit_not_established} ->
        CLI.error!("Conversion fit not established. Keep the stopped-store backup and use the documented off-host conversion procedure.")
      {:error, :source_format_mismatch} ->
        CLI.error!("The store format differs from the verified previous release. The store was left unchanged.")
      {:error, :unsupported_snapshot} ->
        CLI.error!("The store has an unsupported or unreadable snapshot header. The store was left unchanged.")
      {:error, _reason} ->
        CLI.error!("Could not convert the stopped Harklight store. Keep the stopped-store backup and check the storage diagnostics.")
    end
  end

  defp source_format("v0.2." <> minor) do
    case Integer.parse(minor) do
      {n, ""} when n in 16..25 -> 30
      _ -> :unsupported
    end
  end

  defp source_format(_), do: :unsupported
end
