defmodule AttentionServer.Migration.Provenance do
  @moduledoc false

  # This is a trusted export receipt, not a claim made by the inventory caller.
  # The source-facing exporter owns collection of supervisor/process evidence
  # and source file observations. This consumer binds that receipt to the copy.
  def validate(%{version: 1, producer: :stopped_export} = record, hash, size) do
    with %{
           supervisor_before: before,
           supervisor_after: after_state,
           process_marker_before: marker_before,
           process_marker_after: marker_after,
           files_before: files,
           files_after: same_files
         } <- record,
         true <- inactive?(before) and before == after_state,
         true <- stopped_marker?(marker_before) and stopped_marker?(marker_after),
         true <- files == same_files and valid_files?(files),
         %{size: ^size, sha256: ^hash} <- files.main do
      :ok
    else
      _ -> {:error, :invalid_stopped_provenance}
    end
  end

  def validate(nil, _, _), do: {:error, :stopped_provenance_required}
  def validate(_, _, _), do: {:error, :invalid_stopped_provenance}

  defp inactive?(%{unit: unit, active_state: state, main_pid: 0})
       when state in ["inactive", "failed"],
       do: is_binary(unit) and byte_size(unit) > 0

  defp inactive?(%{kind: :operator, supervisor: name, stopped_evidence: evidence, stopped: true}),
    do:
      is_binary(name) and byte_size(name) > 0 and is_binary(evidence) and byte_size(evidence) > 0

  defp inactive?(_), do: false

  defp stopped_marker?(%{state: :absent}), do: true

  defp stopped_marker?(%{state: :stale, pid: pid, alive: false}),
    do: is_integer(pid) and pid > 0

  defp stopped_marker?(_), do: false

  defp valid_files?(%{main: main, wal: wal, shm: shm, journal: journal} = files) do
    map_size(files) == 4 and file?(main) and sidecar?(shm) and
      empty?(wal) and empty?(journal)
  end

  defp valid_files?(_), do: false

  defp file?(%{size: size, mtime: mtime, sha256: hash}),
    do:
      is_integer(size) and size >= 0 and is_integer(mtime) and
        is_binary(hash) and byte_size(hash) == 64 and String.match?(hash, ~r/\A[0-9a-f]{64}\z/)

  defp file?(_), do: false
  defp sidecar?(:absent), do: true
  defp sidecar?(value), do: file?(value)
  defp empty?(:absent), do: true
  defp empty?(%{size: 0} = value), do: file?(value)
  defp empty?(_), do: false
end
