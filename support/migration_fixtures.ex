defmodule AttentionServer.MigrationFixtures do
  @moduledoc false

  # Synthetic producer receipt for copy-consumer tests only. This does not
  # inspect a service and must never be used as a source admission command.
  def provenance(path) do
    files = Map.new([main: "", wal: "-wal", shm: "-shm", journal: "-journal"], fn {key, suffix} ->
      entry = case File.stat(path <> suffix, time: :posix) do
        {:ok, stat} -> %{size: stat.size, mtime: stat.mtime,
          sha256: Base.encode16(:crypto.hash(:sha256, File.read!(path <> suffix)), case: :lower)}
        {:error, :enoent} -> :absent
      end
      {key, entry}
    end)
    supervisor = %{unit: "synthetic-fixture.service", active_state: "inactive", main_pid: 0}
    %{version: 1, producer: :stopped_export,
      supervisor_before: supervisor, supervisor_after: supervisor,
      process_marker_before: %{state: :absent}, process_marker_after: %{state: :absent},
      files_before: files, files_after: files}
  end
end
