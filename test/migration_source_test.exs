defmodule AttentionServer.Migration.SourceTest do
  use ExUnit.Case, async: true
  alias AttentionServer.Migration.{Frames, Provenance, Source}

  setup do
    root = Path.join(System.tmp_dir!(), "stopped-source-#{System.unique_integer([:positive])}")
    File.mkdir!(root)
    File.chmod!(root, 0o700)
    path = Path.join(root, "source.sqlite3")
    # Deliberately not SQLite: collecting stop/copy evidence must not open it.
    File.write!(path, "synthetic bytes before FORMAT32 inspection")
    on_exit(fn -> File.rm_rf!(root) end)
    %{path: path, dest: Path.join(root, "capture"), root: root}
  end

  defp observer(:supervisor, _),
    do: {:ok, %{unit: "synthetic.service", active_state: "inactive", main_pid: 0}}

  defp observer(:open_files, _), do: {:ok, %{tool: nil, status: :unavailable}}
  defp observer(op, arg), do: Source.observe(op, arg)

  test "captures an unchanged private copy with a durable receipt", %{path: path, dest: dest} do
    before = File.read!(path)
    assert {:ok, captured} = Source.capture(path, dest, [], &observer/2)
    assert File.read!(captured.copy) == before
    assert File.read!(path) == before
    assert captured.provenance.open_files_before.status == :unavailable
    assert Bitwise.band(File.stat!(dest).mode, 0o777) == 0o700
    assert Bitwise.band(File.stat!(captured.copy).mode, 0o777) == 0o600

    assert :ok =
             Provenance.validate(
               captured.provenance,
               captured.provenance.files_before.main.sha256,
               byte_size(before)
             )

    assert {:ok, [receipt]} =
             Frames.fold(
               Path.join(dest, "provenance.frames"),
               captured.provenance_file,
               [],
               fn receipt, rows -> {:ok, [receipt | rows]} end
             )

    assert receipt == captured.provenance
    assert {:error, :eexist} = Source.capture(path, dest, [], &observer/2)
  end

  test "refuses active supervisor, open files and uncheckpointed journal", %{
    path: path,
    dest: dest
  } do
    active = fn
      :supervisor, _ -> {:ok, %{unit: "synthetic.service", active_state: "active", main_pid: 7}}
      op, arg -> observer(op, arg)
    end

    assert {:error, :invalid_stopped_provenance} = Source.capture(path, dest, [], active)
    refute File.exists?(dest)

    held = fn
      :open_files, _ -> {:ok, %{tool: "synthetic", status: :open}}
      op, arg -> observer(op, arg)
    end

    assert {:error, :source_files_open} = Source.capture(path, dest, [], held)
    File.write!(path <> "-wal", "pending")
    assert {:error, :invalid_stopped_provenance} = Source.capture(path, dest, [], &observer/2)
    refute File.exists?(dest)
  end

  test "changed source or SHM never gets a published receipt", %{path: path, dest: dest} do
    Process.put(:source_observations, 0)

    changing = fn op, arg ->
      if op == :supervisor do
        n = Process.get(:source_observations)
        Process.put(:source_observations, n + 1)
        if n == 1, do: File.write!(path <> "-shm", "changed during copy")
      end

      observer(op, arg)
    end

    assert {:error, :invalid_stopped_provenance} = Source.capture(path, dest, [], changing)
    refute File.exists?(Path.join(dest, "provenance.frames"))
  end

  test "operator-supervised capture is explicit and names the evidence" do
    assert {:error, :supervisor_required} = Source.observe(:supervisor, nil)

    assert {:error, :supervisor_required} =
             Source.observe(:supervisor, {:operator, "", "stopped"})

    assert {:ok, %{kind: :operator, stopped_evidence: "synthetic container exited"}} =
             Source.observe(
               :supervisor,
               {:operator, "synthetic provisioner", "synthetic container exited"}
             )
  end

  test "configured pidfile distinguishes live, absent and stale processes", %{root: root} do
    path = Path.join(root, "external.pid")
    assert {:ok, %{state: :absent}} = Source.observe(:pidfile, path)
    File.write!(path, System.pid())
    assert {:error, :source_process_running} = Source.observe(:pidfile, path)
    File.write!(path, "999999999\n")
    assert {:ok, %{state: :stale, alive: false}} = Source.observe(:pidfile, path)
  end

  test "available open-file tool detects a real held synthetic file", %{path: path} do
    {:ok, file} = File.open(path, [:read, :binary])

    try do
      case Source.observe(:open_files, [path]) do
        {:ok, %{status: :unavailable}} -> :ok
        result -> assert {:ok, %{status: :open}} = result
      end
    after
      File.close(file)
    end
  end
end
