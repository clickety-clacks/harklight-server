Code.require_file("../scripts/compatibility/scenarios.exs", __DIR__)

defmodule AttentionServer.Migration.BundleTest do
  use ExUnit.Case, async: false
  alias AttentionServer.{CompatibilityScenarios, JSON}
  alias AttentionServer.Migration.{Bundle, CLI, Frames}

  test "command requires explicit source, supervision and material inventory" do
    assert {:error, :invalid_arguments} = CLI.run(["export"])
    assert {:error, :invalid_arguments} = CLI.run(["import", "anything"])

    assert {:error, :supervisor_required} =
             CLI.run([
               "export",
               "--source",
               "/synthetic/source",
               "--output",
               "/synthetic/output",
               "--material-map",
               "/synthetic/material.json"
             ])
  end

  @tag timeout: 60_000
  test "exports typed history, private material and verified uploads without changing source history" do
    root = Path.join(System.tmp_dir!(), "migration-bundle-#{System.unique_integer([:positive])}")
    File.mkdir!(root)
    File.chmod!(root, 0o700)
    fixture = CompatibilityScenarios.fixture(Path.join(root, "history"))
    on_exit(fn -> File.rm_rf!(root) end)
    {:ok, relay} = CompatibilityScenarios.start(fixture)

    history =
      try do
        CompatibilityScenarios.run(relay, fixture)
      after
        GenServer.stop(relay)
      end

    secret = Path.join(root, "synthetic-identity")
    File.write!(secret, "synthetic identity bytes, not real credentials")
    path = Path.join(fixture.root, "relay.sqlite3")
    before = File.read!(path)
    destination = Path.join(root, "export")

    opts = [
      supervisor:
        {:operator, "synthetic test supervisor", "GenServer.stop returned; fixture stopped"},
      material: %{"owner/identity" => secret}
    ]

    assert {:ok, summary} = Bundle.export(path, destination, opts)
    {:ok, marker} = JSON.decode_line(File.read!(Path.join(destination, "COMPLETE.json")) <> "\n")
    assert marker["sha256"] == summary.sha256

    assert {:ok, [manifest]} =
             Frames.fold(Path.join(destination, "manifest.frames"), summary, [], fn record,
                                                                                    records ->
               {:ok, [record | records]}
             end)

    assert {:ok, ^manifest, ^summary} = Bundle.verify(destination)
    assert {:ok, %{"contract_version" => 1, "command" => "verify", "result" => "verified_export"}} = CLI.run(["verify", destination])
    assert manifest.data.records.count == manifest.source.records.count
    assert map_size(manifest.data.files) > 0
    assert File.read!(Path.join(destination, "material/owner/identity")) == File.read!(secret)

    identity = %{id: "synthetic-material-import", source: Base.decode16!(summary.sha256, case: :lower),
      schema: AttentionServer.Repository.Postgres.Schema.digest()}
    staging = Path.join(root, "private-stage")
    assert {:ok, material} = AttentionServer.Migration.ImportMaterial.run(destination, identity, staging)
    assert material["root"] == Path.join(staging, "files")
    staged = Path.join(material["root"], "owner/identity")
    assert File.read!(staged) == File.read!(secret)
    assert Bitwise.band(File.stat!(staged).mode, 0o777) == 0o600
    assert Bitwise.band(File.stat!(Path.dirname(staged)).mode, 0o777) == 0o700
    assert {:ok, ^material} = AttentionServer.Migration.ImportMaterial.run(destination, identity, staging)
    for {path, private, drifted} <- [
      {staging, 0o700, 0o755}, {material["root"], 0o700, 0o755},
      {Path.dirname(staged), 0o700, 0o755}, {staged, 0o600, 0o644},
      {Path.join(staging, "OWNER.json"), 0o600, 0o644},
      {Path.join(staging, "COMPLETE.json"), 0o600, 0o644}
    ] do
      File.chmod!(path, drifted)
      assert {:error, :material_permissions_mismatch} = AttentionServer.Migration.ImportMaterial.run(destination, identity, staging)
      assert Bitwise.band(File.stat!(path).mode, 0o777) == drifted
      File.chmod!(path, private)
      assert {:ok, ^material} = AttentionServer.Migration.ImportMaterial.run(destination, identity, staging)
    end
    assert {:error, :import_source_mismatch} = AttentionServer.Migration.ImportMaterial.run(destination,
      %{identity | source: :crypto.hash(:sha256, "other")}, staging)
    assert {:error, :material_identity_mismatch} = AttentionServer.Migration.ImportMaterial.run(destination,
      %{identity | id: "different-import"}, staging)
    original_material = File.read!(staged)
    File.write!(staged, "corrupt staged content")
    assert {:error, :material_content_mismatch} = AttentionServer.Migration.ImportMaterial.run(destination, identity, staging)
    File.rm!(staged)
    File.write!(staged <> ".partial", "interrupted copy")
    File.rm!(Path.join(staging, "COMPLETE.json"))
    assert {:ok, ^material} = AttentionServer.Migration.ImportMaterial.run(destination, identity, staging)
    assert File.read!(staged) == original_material
    refute File.exists?(staged <> ".partial")
    assert File.read!(secret) == original_material
    refute String.contains?(AttentionServer.JSON.encode!(material), original_material)

    for {relative, expected} <- manifest.data.files do
      bytes = File.read!(Path.join(destination, relative))
      assert byte_size(bytes) == expected.size
      assert Base.encode16(:crypto.hash(:sha256, bytes), case: :lower) == expected.sha256
    end

    frames = Path.join(destination, "records.frames")
    saved = File.read!(frames)
    File.write!(frames, binary_part(saved, 0, byte_size(saved) - 1))
    assert {:error, _} = Bundle.verify(destination)
    File.write!(frames, saved)
    marker_path = Path.join(destination, "COMPLETE.json")
    marker_bytes = File.read!(marker_path)
    File.rm!(marker_path)
    assert {:error, :enoent} = Bundle.verify(destination)
    File.write!(marker_path, marker_bytes)
    assert File.read!(path) == before
    assert {:error, :eexist} = Bundle.export(path, destination, opts)

    # A corrupt referenced upload must leave the new export unpublished.
    {relative, _} = Enum.at(manifest.data.files, 0)
    suffix = String.replace_prefix(relative, "files/uploads/", "")
    original_file = Path.join(path <> ".uploads", suffix)
    content = File.read!(original_file)
    File.write!(original_file, "damaged")
    assert {:error, :export_file_mismatch} = Bundle.export(path, Path.join(root, "broken"), opts)
    refute File.exists?(Path.join(root, "broken/COMPLETE.json"))
    File.write!(original_file, content)

    {:ok, reopened} = CompatibilityScenarios.start(fixture)

    try do
      assert :ok = CompatibilityScenarios.verify(reopened, fixture, history)
    after
      GenServer.stop(reopened)
      Agent.stop(fixture.clock)
    end
  end
end
