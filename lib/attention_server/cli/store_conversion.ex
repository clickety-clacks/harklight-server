defmodule AttentionServer.CLI.StoreConversion do
  @moduledoc false

  alias AttentionServer.{CLI, Store}
  alias AttentionServer.Store.ConversionCapacity

  @capacity_labels %{
    prefix_memory: "memory for the snapshot header probe",
    prefix_disk: "free disk for the snapshot header probe",
    memory: "conversion memory",
    disk: "extra conversion disk space"
  }

  @shape_labels %{
    expanded_snapshot: {"expanded snapshot size", "bytes"},
    upload_bytes: {"allocated upload directory size", "bytes"},
    client_resource_revs: {"client resource revisions", "rows"},
    agent_changes: {"agent changes", "rows"},
    large_data_references: {"large-data references", "rows"}
  }

  @measurement_labels %{
    host_memory: "available host memory",
    cgroup_memory: "available cgroup memory",
    disk: "available disk space"
  }

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
      {:error, {:fit_not_established, _, _, _} = failure} ->
        CLI.error!(fit_failure(failure))
      {:error, {:fit_not_established, :measurement_unavailable, field}} ->
        label = @measurement_labels[field] || elem(Map.fetch!(@shape_labels, field), 0)
        CLI.error!("Conversion fit not established. Could not measure #{label}. Keep the stopped-store backup.")
      {:error, :source_format_mismatch} ->
        CLI.error!("The store format differs from the verified previous release. The store was left unchanged.")
      {:error, :unsupported_snapshot} ->
        CLI.error!("The store has an unsupported or unreadable snapshot header. The store was left unchanged.")
      {:error, _reason} ->
        CLI.error!("Could not convert the stopped Harklight store. Keep the stopped-store backup and check the storage diagnostics.")
    end
  end

  defp fit_failure({:fit_not_established, field, needed, available}) when is_map_key(@capacity_labels, field) do
    label = Map.fetch!(@capacity_labels, field)

    "Conversion fit not established. Required #{label} is #{needed} bytes, with #{available} bytes available. " <>
      "Keep the stopped-store backup and use the documented off-host conversion procedure."
  end

  defp fit_failure({:fit_not_established, field, actual, limit}) do
    {label, unit} = Map.fetch!(@shape_labels, field)

    "Conversion fit not established. The #{label} is #{actual} #{unit}, above the calibrated limit of #{limit} #{unit}. " <>
      "Keep the stopped-store backup and report the failed limit before converting."
  end

  defp source_format("v0.2." <> minor) do
    case Integer.parse(minor) do
      {n, ""} when n in 16..25 -> 30
      _ -> :unsupported
    end
  end

  defp source_format(_), do: :unsupported
end
