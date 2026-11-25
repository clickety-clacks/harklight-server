defmodule AttentionServer.Store.ConversionCapacity do
  @moduledoc false

  def read(path) do
    with {:ok, host} <- host_memory(),
         {:ok, memory} <- cgroup_memory(host),
         {:ok, disk} <- disk_available(Path.dirname(path)),
         {:ok, uploads} <- upload_bytes(path <> ".uploads") do
      {:ok, %{memory: memory, disk: disk, uploads: uploads}}
    else
      _ -> {:error, :fit_not_established}
    end
  end

  defp host_memory do
    with {:ok, contents} <- File.read("/proc/meminfo"),
         [_, kib] <- Regex.run(~r/^MemAvailable:\s+(\d+) kB$/m, contents) do
      {:ok, String.to_integer(kib) * 1024}
    end
  end

  defp cgroup_memory(host) do
    with {:ok, contents} <- File.read("/proc/self/cgroup") do
      case Regex.run(~r/^0::(\/.*)$/m, contents) do
        [_, group] -> group_memory("/sys/fs/cgroup", group, host)
        nil -> legacy_cgroup_memory(contents, host)
      end
    end
  end

  defp legacy_cgroup_memory(contents, host) do
    case Regex.run(~r/^\d+:(?:[^:\n]*,)?memory(?:,[^:\n]*)?:(\/.*)$/m, contents) do
      [_, group] -> group_memory("/sys/fs/cgroup/memory", group, host, :legacy)
      nil -> {:ok, host}
    end
  end

  defp group_memory(root, group, host, mode \\ :unified) do
    {limit_file, usage_file} =
      case mode do
        :unified -> {"memory.max", "memory.current"}
        :legacy -> {"memory.limit_in_bytes", "memory.usage_in_bytes"}
      end

    # A delegated container can mount the controller at its own cgroup root.
    # A host mount exposes the process's cgroup and all limiting ancestors.
    dir = Path.expand("." <> group, root)

    if dir == root or String.starts_with?(dir, root <> "/") do
      cond do
        File.dir?(dir) -> remaining_memory(root, dir, host, limit_file, usage_file)
        File.regular?(Path.join(root, limit_file)) -> remaining_memory(root, root, host, limit_file, usage_file)
        true -> {:error, :unknown_memory_limit}
      end
    end
  end

  defp remaining_memory(root, dir, available, limit_file, usage_file) do
    with {:ok, limit} <- group_limit(dir, limit_file, usage_file) do
      available = if limit == :unlimited, do: available, else: min(available, max(0, limit))

      if dir == root,
        do: {:ok, available},
        else: remaining_memory(root, Path.dirname(dir), available, limit_file, usage_file)
    end
  end

  defp group_limit(dir, limit_file, usage_file) do
    case File.read(Path.join(dir, limit_file)) do
      {:ok, cap} ->
        case String.trim(cap) do
          "max" -> {:ok, :unlimited}
          cap ->
            with {limit, ""} <- Integer.parse(cap),
                 {:ok, usage} <- File.read(Path.join(dir, usage_file)),
                 {used, ""} <- Integer.parse(String.trim(usage)) do
              {:ok, limit - used}
            end
        end

      {:error, :enoent} -> {:ok, :unlimited}
      error -> error
    end
  end

  defp disk_available(directory) do
    with {output, 0} <- System.cmd("df", ["-Pk", "--", directory], env: [{"LC_ALL", "C"}]),
         [_, _, _, kib | _] <- output |> String.trim_trailing() |> String.split("\n") |> List.last() |> String.split(),
         {kib, ""} <- Integer.parse(kib) do
      {:ok, kib * 1024}
    end
  end

  defp upload_bytes(directory) do
    case File.stat(directory) do
      {:error, :enoent} -> {:ok, 0}
      {:ok, %{type: :directory}} ->
        with {output, 0} <- System.cmd("du", ["-sk", "--", directory], env: [{"LC_ALL", "C"}]),
             {kib, _} <- Integer.parse(output) do
          {:ok, kib * 1024}
        end
      error -> error
    end
  end
end
