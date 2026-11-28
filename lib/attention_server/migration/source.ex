defmodule AttentionServer.Migration.Source do
  @moduledoc false
  alias AttentionServer.Migration.{Frames, Provenance}

  # The observer seam is for isolated tests. The CLI never accepts an observer.
  # No function in this module opens SQLite, checkpoints it, or probes its locks.
  def capture(path, destination, opts, observer \\ &__MODULE__.observe/2) do
    source = Path.expand(path)

    with {:ok, before} <- observations(source, opts, observer),
         :ok <- admissible(before),
         :ok <- File.mkdir(destination) do
      with :ok <- File.chmod(destination, 0o700),
           :ok <- File.cp(source, Path.join(destination, "source.sqlite3")),
           :ok <- File.chmod(Path.join(destination, "source.sqlite3"), 0o600),
           {:ok, copied} <- file_observation(Path.join(destination, "source.sqlite3")),
           {:ok, after_state} <- observations(source, opts, observer),
           :ok <- admissible(after_state),
           receipt = receipt(before, after_state),
           :ok <- Provenance.validate(receipt, copied.sha256, copied.size),
           {:ok, receipt_file} <-
             Frames.write(Path.join(destination, "provenance.frames"), [receipt]) do
        {:ok,
         %{
           copy: Path.join(destination, "source.sqlite3"),
           provenance: receipt,
           provenance_file: receipt_file
         }}
      end

      # A failed capture leaves a private, incomplete directory for diagnosis.
      # No provenance file is published until every observation agrees.
    end
  end

  def confirm(path, receipt, opts, observer \\ &__MODULE__.observe/2) do
    before = %{
      supervisor: receipt.supervisor_before,
      marker: receipt.process_marker_before,
      open_files: receipt.open_files_before,
      files: receipt.files_before
    }

    with {:ok, current} <- observations(Path.expand(path), opts, observer),
         :ok <- admissible(current),
         updated = receipt(before, current),
         :ok <-
           Provenance.validate(
             updated,
             receipt.files_before.main.sha256,
             receipt.files_before.main.size
           ) do
      {:ok, updated}
    end
  end

  defp observations(path, opts, observer) do
    with {:ok, supervisor} <- observer.(:supervisor, Keyword.get(opts, :supervisor)),
         {:ok, marker} <- observer.(:pidfile, Keyword.get(opts, :pidfile)),
         {:ok, handles} <- observer.(:open_files, [path, path <> "-wal", path <> "-shm"]),
         {:ok, files} <- files(path) do
      {:ok, %{supervisor: supervisor, marker: marker, open_files: handles, files: files}}
    end
  end

  defp admissible(state) do
    with true <- state.open_files.status in [:clear, :unavailable],
         :ok <-
           Provenance.validate(
             receipt(state, state),
             state.files.main.sha256,
             state.files.main.size
           ) do
      :ok
    else
      false -> {:error, :source_files_open}
      error -> error
    end
  end

  defp receipt(before, after_state) do
    %{
      version: 1,
      producer: :stopped_export,
      supervisor_before: before.supervisor,
      supervisor_after: after_state.supervisor,
      process_marker_before: before.marker,
      process_marker_after: after_state.marker,
      open_files_before: before.open_files,
      open_files_after: after_state.open_files,
      files_before: before.files,
      files_after: after_state.files
    }
  end

  def observe(:supervisor, {:systemd, unit}) when is_binary(unit) and byte_size(unit) > 0 do
    case System.cmd(
           "systemctl",
           ["--user", "show", "--property=LoadState,ActiveState,MainPID", "--", unit],
           stderr_to_stdout: true
         ) do
      {text, 0} ->
        fields =
          text
          |> String.split("\n", trim: true)
          |> Enum.map(&String.split(&1, "=", parts: 2))
          |> Enum.filter(&(length(&1) == 2))
          |> Map.new(fn [key, value] -> {key, value} end)

        case fields do
          %{"LoadState" => "loaded", "ActiveState" => state, "MainPID" => "0"}
          when state in ["inactive", "failed"] ->
            {:ok, %{unit: unit, active_state: state, main_pid: 0}}

          _ ->
            {:error, :supervisor_not_stopped}
        end

      _ ->
        {:error, :supervisor_observation_failed}
    end
  rescue
    ErlangError -> {:error, :supervisor_observation_failed}
  end

  def observe(:supervisor, {:operator, name, evidence})
      when is_binary(name) and byte_size(name) > 0 and is_binary(evidence) and
             byte_size(evidence) > 0,
      do: {:ok, %{kind: :operator, supervisor: name, stopped_evidence: evidence, stopped: true}}

  def observe(:supervisor, _), do: {:error, :supervisor_required}

  # With no configured pidfile, the supervisor's zero MainPID (or the explicit
  # operator evidence) is the absent-process marker. Never invent a pidfile.
  def observe(:pidfile, nil), do: {:ok, %{state: :absent, source: :supervisor}}

  def observe(:pidfile, path) when is_binary(path) do
    case File.read(path) do
      {:error, :enoent} ->
        {:ok, %{state: :absent, source: :configured_pidfile}}

      {:ok, text} ->
        case Integer.parse(String.trim(text)) do
          {pid, ""} when pid > 0 -> pid_state(pid)
          _ -> {:error, :invalid_pidfile}
        end

      _ ->
        {:error, :pidfile_observation_failed}
    end
  end

  def observe(:open_files, paths) do
    paths = Enum.filter(paths, &File.exists?/1)

    cond do
      executable = System.find_executable("fuser") ->
        check_handles(executable, paths, "fuser")

      executable = System.find_executable("lsof") ->
        check_handles(executable, ["-t" | paths], "lsof")

      true ->
        {:ok, %{tool: nil, status: :unavailable}}
    end
  end

  defp check_handles(executable, args, tool) do
    case System.cmd(executable, args, stderr_to_stdout: true) do
      {_, 0} ->
        {:ok, %{tool: tool, status: :open}}

      {text, 1} ->
        if String.trim(text) == "",
          do: {:ok, %{tool: tool, status: :clear}},
          else: {:error, :open_files_observation_failed}

      _ ->
        {:error, :open_files_observation_failed}
    end
  rescue
    ErlangError -> {:error, :open_files_observation_failed}
  end

  defp pid_state(pid) do
    if File.dir?("/proc/self") do
      case File.stat("/proc/#{pid}") do
        {:error, :enoent} -> {:ok, %{state: :stale, pid: pid, alive: false}}
        {:ok, _} -> {:error, :source_process_running}
        _ -> {:error, :pidfile_observation_failed}
      end
    else
      {:error, :pidfile_observation_failed}
    end
  end

  defp files(path) do
    Enum.reduce_while(
      [main: "", wal: "-wal", shm: "-shm", journal: "-journal"],
      {:ok, %{}},
      fn {key, suffix}, {:ok, files} ->
        case file_observation(path <> suffix) do
          {:ok, file} -> {:cont, {:ok, Map.put(files, key, file)}}
          {:error, :enoent} when key != :main -> {:cont, {:ok, Map.put(files, key, :absent)}}
          _ -> {:halt, {:error, :source_file_observation_failed}}
        end
      end
    )
  end

  def file_observation(path) do
    with {:ok, %{type: :regular} = before} <- File.lstat(path, time: :posix) do
      {hash, bytes} =
        path
        |> File.stream!([], 1024 * 1024)
        |> Enum.reduce({:crypto.hash_init(:sha256), 0}, fn chunk, {hash, bytes} ->
          {:crypto.hash_update(hash, chunk), bytes + byte_size(chunk)}
        end)

      with {:ok, after_state} <- File.lstat(path, time: :posix),
           true <-
             before.size == bytes and before.size == after_state.size and
               before.mtime == after_state.mtime do
        {:ok,
         %{
           size: bytes,
           mtime: before.mtime,
           sha256: Base.encode16(:crypto.hash_final(hash), case: :lower)
         }}
      else
        _ -> {:error, :source_changed}
      end
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_source_file}
    end
  rescue
    File.Error -> {:error, :source_file_observation_failed}
  end
end
