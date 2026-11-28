Code.require_file("../scripts/compatibility/scenarios.exs", __DIR__)

defmodule AttentionServer.Migration.BundleTest do
  use ExUnit.Case, async: false
  alias AttentionServer.{CompatibilityScenarios, JSON}
  alias AttentionServer.Migration.{Bundle, CLI, Frames}

  defp verify_staged_material(bundle, identity, staging, expected) do
    alias AttentionServer.Migration.VerifyMaterial
    files = ["OWNER.json", "COMPLETE.json" | Enum.map(expected["files"], &("files/" <> &1["path"]))]
    snapshot = fn -> Map.new(files, fn relative ->
      path = Path.join(staging, relative)
      stat = File.stat!(path)
      {relative, {:crypto.hash(:sha256, File.read!(path)), stat.size, stat.mtime, stat.mode}}
    end) end
    before = snapshot.()
    verify_material_owner_drift(bundle, identity, staging, expected)
    assert {:ok, ^expected} = VerifyMaterial.run(bundle, identity, staging)
    assert snapshot.() == before
    for relative <- ["OWNER.json", "COMPLETE.json", "files/person-id"] do
      path = Path.join(staging, relative)
      bytes = File.read!(path)
      File.write!(path, "changed synthetic material")
      assert {:error, _} = VerifyMaterial.run(bundle, identity, staging)
      assert File.read!(path) == "changed synthetic material"
      File.write!(path, bytes)
      assert {:ok, ^expected} = VerifyMaterial.run(bundle, identity, staging)
      File.rename!(path, path <> ".partial")
      assert {:error, _} = VerifyMaterial.run(bundle, identity, staging)
      refute File.exists?(path)
      assert File.read!(path <> ".partial") == bytes
      File.rename!(path <> ".partial", path)
      assert {:ok, ^expected} = VerifyMaterial.run(bundle, identity, staging)
    end
    for {relative, private, public} <- [{"", 0o700, 0o755}, {"files/owner", 0o700, 0o755},
      {"files/owner/identity.key", 0o600, 0o644}, {"COMPLETE.json", 0o600, 0o644}] do
      path = Path.join(staging, relative)
      File.chmod!(path, public)
      assert {:error, :material_permissions_mismatch} = VerifyMaterial.run(bundle, identity, staging)
      assert Bitwise.band(File.stat!(path).mode, 0o777) == public
      File.chmod!(path, private)
      assert {:ok, ^expected} = VerifyMaterial.run(bundle, identity, staging)
    end
    for relative <- ["unexpected", "files/owner/extra.key"] do
      path = Path.join(staging, relative)
      File.write!(path, "synthetic extra")
      File.chmod!(path, 0o600)
      assert {:error, :material_source_or_inventory_mismatch} = VerifyMaterial.run(bundle, identity, staging)
      assert File.read!(path) == "synthetic extra"
      File.rm!(path)
      assert {:ok, ^expected} = VerifyMaterial.run(bundle, identity, staging)
    end
    extra_directory = Path.join(staging, "files/unexpected-directory")
    File.mkdir!(extra_directory)
    File.chmod!(extra_directory, 0o700)
    assert {:error, :material_source_or_inventory_mismatch} = VerifyMaterial.run(bundle, identity, staging)
    assert File.dir?(extra_directory)
    File.rmdir!(extra_directory)
    assert {:ok, ^expected} = VerifyMaterial.run(bundle, identity, staging)
    assert {:error, :material_identity_mismatch} = VerifyMaterial.run(bundle, %{identity | id: "another-import"}, staging)
    assert {:error, :material_identity_mismatch} = VerifyMaterial.run(bundle,
      %{identity | schema: :crypto.hash(:sha256, "another-schema")}, staging)
    missing = staging <> "-missing"
    assert {:error, _} = VerifyMaterial.run(bundle, identity, missing)
    refute File.exists?(missing)
  end

  defp verify_material_owner_drift(bundle, identity, staging, expected) do
    # The disposable PostgreSQL CI job runs on a fresh Linux runner with sudo.
    # Never require chown privileges for ordinary local unit-test runs.
    if System.get_env("HARKLIGHT_MIGRATION_TEST_URL") do
      alias AttentionServer.Migration.VerifyMaterial
      original = File.stat!(staging)
      for relative <- ["", "files", "files/owner", "OWNER.json", "COMPLETE.json", "files/owner/identity.key"],
          {uid, gid} <- [{original.uid + 10_000, original.gid}, {original.uid, original.gid + 10_000}] do
        path = Path.join(staging, relative)
        args = ["#{uid}:#{gid}", path]
        {_, 0} = System.cmd("sudo", ["-n", "chown" | args], stderr_to_stdout: true)
        try do
          assert {:error, :material_ownership_mismatch} = VerifyMaterial.run(bundle, identity, staging)
          stat = File.lstat!(path)
          assert {stat.uid, stat.gid} == {uid, gid}
        after
          {_, 0} = System.cmd("sudo", ["-n", "chown", "#{original.uid}:#{original.gid}", path], stderr_to_stdout: true)
        end
        assert {:ok, ^expected} = VerifyMaterial.run(bundle, identity, staging)
      end
    end
  end

  test "command requires explicit source, supervision and material inventory" do
    assert {:ok, %{"contract_version" => 1, "command" => "capabilities",
      "commands" => ["export", "verify", "status"], "migration_ready" => false}} = CLI.run(["capabilities"])
    assert {:error, :receipt_missing} = CLI.run(["status", "--receipt", "/synthetic/nonexistent-receipt"])
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

    secret = Path.join(fixture.root, "certs/expired.key")
    person_file = Path.join(root, "person-id")
    File.write!(person_file, hd(history["people"]) <> "\n")
    env = Path.join(root, "server.env")
    certs = Path.join(fixture.root, "certs")
    File.write!(env, Enum.map_join([
      {"CERTFILE", "server.pem"}, {"KEYFILE", "server.key"}, {"CACERTFILE", "ca.pem"}
    ], "\n", fn {key, file} -> "export ATTENTION_SERVER_#{key}='#{Path.join(certs, file)}'" end))
    path = Path.join(fixture.root, "relay.sqlite3")
    before = File.read!(path)
    destination = Path.join(root, "export")

    opts = [
      supervisor:
        {:operator, "synthetic test supervisor", "GenServer.stop returned; fixture stopped"},
      material: %{"owner/identity.key" => secret,
        "owner/identity.pem" => Path.join(fixture.root, "certs/expired.pem"),
        "person-id" => person_file, "server.env" => env,
        "tls/server.pem" => Path.join(certs, "server.pem"),
        "tls/server.key" => Path.join(certs, "server.key"),
        "tls/ca.pem" => Path.join(certs, "ca.pem"),
        "tls/ca.key" => Path.join(certs, "ca.key")}
    ]

    auxiliary = Map.new(["tls/server.csr", "tls/server.ext", "tls/ca.srl", "owner/sign-in-link.txt"], fn name ->
      source = Path.join(root, name)
      File.mkdir_p!(Path.dirname(source))
      File.write!(source, "synthetic installer auxiliary #{name}")
      {name, source}
    end)
    opts = Keyword.update!(opts, :material, &Map.merge(&1, auxiliary))

    provider = root <> "-provider.key"
    File.write!(provider, "synthetic opaque provider key bytes")
    on_exit(fn -> File.rm!(provider) end)
    File.write!(env, "\nATTENTION_SERVER_JEV_KEY_PATH='#{provider}'\n", [:append])
    opts = Keyword.update!(opts, :material, &Map.put(&1, "external" <> provider, provider))

    material_map = Path.join(root, "material-map.json")
    File.write!(material_map, JSON.encode!(opts[:material]))
    assert {:ok, %{"command" => "export", "bundle_version" => 2}} = CLI.run([
      "export", "--source", path, "--output", destination,
      "--material-map", material_map, "--supervisor", "synthetic test supervisor",
      "--stopped-evidence", "GenServer.stop returned; fixture stopped"
    ])
    assert {:ok, _, summary} = Bundle.verify(destination)
    {:ok, marker} = JSON.decode_line(File.read!(Path.join(destination, "COMPLETE.json")) <> "\n")
    assert marker["sha256"] == summary.sha256

    assert {:ok, [manifest]} =
             Frames.fold(Path.join(destination, "manifest.frames"), summary, [], fn record,
                                                                                    records ->
               {:ok, [record | records]}
             end)

    assert {:ok, ^manifest, ^summary} = Bundle.verify(destination)
    assert manifest.version == 2 and marker["version"] == 2
    for suffix <- ["-wal", "-journal"] do
      sidecar = Path.join(destination, "source.sqlite3" <> suffix)
      File.write!(sidecar, "synthetic pending source write")
      assert {:error, :bundle_source_pending_writes} = Bundle.verify(destination)
      assert File.read!(sidecar) == "synthetic pending source write"
      File.write!(sidecar, "")
      assert {:ok, ^manifest, ^summary} = Bundle.verify(destination)
      assert File.read!(sidecar) == ""
      File.rm!(sidecar)
      assert {:ok, ^manifest, ^summary} = Bundle.verify(destination)
    end

    assert {:error, :legacy_bundle_requires_reexport} = AttentionServer.Migration.BundleValidation.validate(destination,
      manifest |> Map.put(:version, 1) |> Map.delete(:material_provenance))
    marker_path = Path.join(destination, "COMPLETE.json")
    File.write!(marker_path, JSON.encode!(%{marker | "version" => 1}))
    assert {:error, :legacy_bundle_requires_reexport} = Bundle.verify(destination)
    assert {:error, :legacy_bundle_requires_reexport} = CLI.run(["verify", destination])
    File.write!(marker_path, JSON.encode!(marker))
    assert {:ok, ^manifest, ^summary} = Bundle.verify(destination)

    assert {:ok, %{"contract_version" => 1, "command" => "verify", "result" => "verified_export", "bundle_version" => 2}} = CLI.run(["verify", destination])
    for {name, source} <- auxiliary do
      assert File.read!(Path.join(destination, "material/" <> name)) == File.read!(source)
    end
    wrong_auxiliary = put_in(manifest, [:material_provenance, "files", "material/tls/server.csr", "path"], "/different/source/server.csr")
    assert {:error, :material_auxiliary_source_mismatch} = AttentionServer.Migration.MaterialIdentity.validate(destination, wrong_auxiliary)
    assert manifest.material_provenance["anchor"] == "stopped_source_configuration"
    assert Enum.all?(manifest.material_provenance["files"], fn {_, entry} -> entry["before"] == entry["after"] end)
    assert manifest.data.records.count == manifest.source.records.count
    assert map_size(manifest.data.files) > 0
    assert File.read!(Path.join(destination, "material/owner/identity.key")) == File.read!(secret)

    identity = %{id: "synthetic-material-import", source: Base.decode16!(summary.sha256, case: :lower),
      schema: AttentionServer.Repository.Postgres.Schema.digest()}
    receipt = Path.join(root, "resume-receipt.json")
    assert {:ok, fresh} = AttentionServer.Migration.ImportReceipt.open(receipt, destination)
    assert fresh.source == identity.source
    assert AttentionServer.Message.uuid_v4?(fresh.id)
    assert Bitwise.band(File.stat!(receipt).mode, 0o777) == 0o600
    assert {:ok, ^fresh} = AttentionServer.Migration.ImportReceipt.open(receipt, destination)
    pinned = %{fresh | schema: :crypto.hash(:sha256, File.read!("priv/postgres/001_initial.sql"))}
    File.write!(receipt, JSON.encode!(AttentionServer.Migration.ImportReceipt.document(pinned)))
    assert {:ok, ^pinned} = AttentionServer.Migration.ImportReceipt.open(receipt, destination)
    File.rename!(receipt, receipt <> ".partial")
    assert {:ok, ^pinned} = AttentionServer.Migration.ImportReceipt.open(receipt, destination)
    File.write!(receipt, JSON.encode!(AttentionServer.Migration.ImportReceipt.document(%{pinned | source: :crypto.hash(:sha256, "wrong")})))
    assert {:error, :import_source_mismatch} = AttentionServer.Migration.ImportReceipt.open(receipt, destination)
    File.write!(receipt, "invalid receipt")
    assert {:error, :invalid_import_receipt} = AttentionServer.Migration.ImportReceipt.load(receipt)
    staging = Path.join(root, "private-stage")
    assert {:ok, material} = AttentionServer.Migration.ImportMaterial.run(destination, identity, staging)
    assert material["root"] == Path.join(staging, "files")
    provider_stage = Path.join(material["root"], "external" <> provider)
    assert File.read!(provider_stage) == File.read!(provider)
    assert Bitwise.band(File.stat!(provider_stage).mode, 0o777) == 0o600

    staged = Path.join(material["root"], "owner/identity.key")
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
    verify_staged_material(destination, identity, staging, material)
    refute String.contains?(AttentionServer.JSON.encode!(material), original_material)

    foreign_map = opts[:material]
      |> Map.put("owner/identity.key", Path.join(fixture.root, "certs/b1.key"))
      |> Map.put("owner/identity.pem", Path.join(fixture.root, "certs/b1.pem"))
    foreign_bundle = Path.join(root, "foreign-owner-export")
    assert {:ok, foreign_summary} = Bundle.export(path, foreign_bundle, Keyword.put(opts, :material, foreign_map))
    foreign_identity = %{identity | source: Base.decode16!(foreign_summary.sha256, case: :lower)}
    assert {:error, :material_identity_source_mismatch} = AttentionServer.Migration.ImportMaterial.run(
      foreign_bundle, foreign_identity, Path.join(root, "foreign-stage"))
    refute File.exists?(Path.join(root, "foreign-stage"))
    mismatched_bundle = Path.join(root, "mismatched-owner-export")
    mismatched = Map.put(opts[:material], "owner/identity.key", Path.join(fixture.root, "certs/b1.key"))
    assert {:ok, mismatched_summary} = Bundle.export(path, mismatched_bundle, Keyword.put(opts, :material, mismatched))
    mismatched_identity = %{identity | source: Base.decode16!(mismatched_summary.sha256, case: :lower)}
    assert {:error, :material_identity_source_mismatch} = AttentionServer.Migration.ImportMaterial.run(
      mismatched_bundle, mismatched_identity, Path.join(root, "mismatched-stage"))

    # Extend a separate stopped synthetic source, keeping the M1 golden source unchanged.
    invitation_path = Path.join(root, "invitation.sqlite3")
    File.cp!(path, invitation_path)
    File.cp_r!(path <> ".uploads", invitation_path <> ".uploads")
    # A retained real guided-join invitation pins the same captured CA.
    {:ok, store} = AttentionServer.Store.open_relay(invitation_path)
    person = hd(history["people"])
    owner = Map.fetch!(store.engine.owners, person).key
    {:ok, configuration} = AttentionServer.GuidedJoin.configuration("fixture.test", 4080, 4040,
      File.read!(Path.join(certs, "ca.pem")))
    {engine, :ok, touched} = AttentionServer.Engine.with_touches(store.engine, fn engine ->
      {:ok, engine, _} = AttentionServer.GuidedJoin.create(engine, person, owner,
        :crypto.hash(:sha256, "migration invitation"), configuration)
      {engine, :ok}
    end)
    {:ok, store} = AttentionServer.Store.commit(store, {engine, touched},
      %{person: person, actor: "fixture", action: "migration_invitation"})
    :ok = AttentionServer.Store.close(store)
    invitation_bundle = Path.join(root, "invitation-export")
    assert {:ok, invitation_summary} = Bundle.export(invitation_path, invitation_bundle, opts)
    invitation_identity = %{identity | source: Base.decode16!(invitation_summary.sha256, case: :lower)}
    assert {:ok, _} = AttentionServer.Migration.ImportMaterial.run(invitation_bundle,
      invitation_identity, Path.join(root, "invitation-stage"))

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
