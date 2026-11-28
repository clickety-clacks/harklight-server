defmodule AttentionServer.Migration.MaterialIdentityTest do
  use ExUnit.Case, async: false
  alias AttentionServer.{DeviceIdentity, GuidedJoin, TLSFixtures}
  alias AttentionServer.Migration.{Frames, MaterialIdentity, MaterialProvenance, Source}

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  test "TLS uses captured configuration with no stored CA pin and checks all retained pins", %{certs: certs} do
    {root, manifest} = bundle(certs, [])
    assert :ok = MaterialIdentity.validate(root, manifest)
    without_config = %{manifest | material: Map.delete(manifest.material, "material/server.env")}
    assert {:error, :unsupported_or_missing_identity_material} = MaterialIdentity.validate(root, without_config)
    {:ok, configuration} = GuidedJoin.configuration("fixture.test", 4080, 4040, File.read!(Path.join(certs, "ca.pem")))
    {root, manifest} = bundle(certs, [configuration, configuration])
    assert :ok = MaterialIdentity.validate(root, manifest)
    foreign = %{configuration | "ca_sha256" => String.duplicate("0", 64)}
    {root, manifest} = bundle(certs, [configuration, foreign])
    assert {:error, :material_invitation_ca_mismatch} = MaterialIdentity.validate(root, manifest)
  end

  test "private keys and chain must match the captured certificates", %{certs: certs} do
    for changes <- [
      %{"tls/server.key" => "b1.key"},
      %{"tls/ca.key" => "untrusted-ca.key"},
      %{"tls/ca.pem" => "untrusted-ca.pem", "tls/ca.key" => "untrusted-ca.key"}
    ] do
      {root, manifest} = bundle(certs, [], changes)
      assert {:error, :material_tls_identity_mismatch} = MaterialIdentity.validate(root, manifest)
    end
  end

  test "configured paths bind to copied source files and observations refuse drift", %{certs: certs} do
    {root, manifest} = bundle(certs, [])
    receipt = manifest.material_provenance
    [name | _] = Map.keys(receipt["files"])
    drift = put_in(receipt, ["files", name, "after", :size], -1)
    assert {:error, :material_provenance_mismatch} = MaterialProvenance.verify(root, manifest.material, drift)
    assert {:error, :material_provenance_required} = MaterialProvenance.verify(root, manifest.material, nil)
    source = Path.join(root, "source-env")
    File.write!(source, "first source bytes")
    assert {:ok, before} = MaterialProvenance.capture(%{"test" => source})
    File.write!(source, "changed source bytes")
    assert {:error, :source_material_changed} = MaterialProvenance.confirm(before, root, %{})
    receipt = put_in(receipt, ["files", "material/tls/server.pem", "path"], "/other/server.pem")
    assert {:error, :configured_tls_material_missing} = MaterialProvenance.verify(root, manifest.material, receipt)
  end

  test "configuration parses literal installer quoting and refuses expansion" do
    assert {:ok, values} = MaterialProvenance.configuration("export ATTENTION_SERVER_CERTFILE='/private/a b.pem'\nATTENTION_SERVER_KEYFILE=/private/a\\ b.key\nATTENTION_SERVER_CACERTFILE=\"/private/ca.pem\"")
    assert values["ATTENTION_SERVER_CERTFILE"] == "/private/a b.pem"
    assert values["ATTENTION_SERVER_KEYFILE"] == "/private/a b.key"
    for value <- ["$ROOT/server.pem", "$(command)", "`command`", "/private/a;command", "'unclosed"] do
      assert {:error, :unsupported_tls_configuration} = MaterialProvenance.configuration("ATTENTION_SERVER_CERTFILE=" <> value)
    end
  end

  test "configured provider and feature files preserve opaque bytes with exact source paths", %{certs: certs} do
    configured = %{
      "ATTENTION_SERVER_JEV_KEY_PATH" => {"jev.key", "synthetic opaque provider bytes"},
      "ATTENTION_SERVER_APNS_KEY_PATH" => {"apns.key", <<0, 1, 2, 255>>},
      "ATTENTION_SERVER_APNS_PRODUCTION_KEY_PATH" => {"apns-production.key", "synthetic production key"},
      "ATTENTION_SERVER_FEATURE_FLAGS_PATH" => {"flags.json", "{}"}
    }
    {root, manifest} = bundle(certs, [], %{}, configured)
    assert :ok = MaterialProvenance.verify(root, manifest.material, manifest.material_provenance)
    assert :ok = MaterialIdentity.validate(root, manifest)
    assert {:ok, paths} = MaterialProvenance.configured_paths(root, manifest.material, manifest.material_provenance["files"])
    for {variable, {_, bytes}} <- configured do
      name = Map.fetch!(paths, variable)
      assert File.read!(Path.join(root, name)) == bytes
      assert name == "material/external" <> manifest.material_provenance["files"][name]["path"]
    end
    name = paths["ATTENTION_SERVER_JEV_KEY_PATH"]
    changed = put_in(manifest.material_provenance, ["files", name, "path"], "/different/configured.key")
    assert {:error, :configured_material_missing} = MaterialProvenance.verify(root, manifest.material, changed)
    assert {:error, :configured_material_missing} = MaterialProvenance.configured_paths(root, manifest.material,
      Map.delete(manifest.material_provenance["files"], name))
    alias_name = "material/external/wrong-restore-path"
    files = manifest.material_provenance["files"] |> Map.put(alias_name, manifest.material_provenance["files"][name]) |> Map.delete(name)
    material = manifest.material |> Map.put(alias_name, manifest.material[name]) |> Map.delete(name)
    assert {:error, :configured_material_path_mismatch} = MaterialProvenance.configured_paths(root, material, files)
    assert :ok = MaterialIdentity.validate(root, manifest)
  end

  test "dashboard pairing is local and unknown external or score output is refused", %{certs: certs} do
    configured = %{
      "ATTENTION_SERVER_DASHBOARD_CERTFILE" => {"dashboard.pem", File.read!(Path.join(certs, "server.pem"))},
      "ATTENTION_SERVER_DASHBOARD_KEYFILE" => {"dashboard.key", File.read!(Path.join(certs, "server.key"))}
    }
    {root, manifest} = bundle(certs, [], %{}, configured)
    assert :ok = MaterialIdentity.validate(root, manifest)
    foreign = Map.put(configured, "ATTENTION_SERVER_DASHBOARD_KEYFILE", {"dashboard.key", File.read!(Path.join(certs, "b1.key"))})
    {root, manifest} = bundle(certs, [], %{}, foreign)
    assert {:error, :material_dashboard_identity_mismatch} = MaterialIdentity.validate(root, manifest)
    for {variable, wrong_file} <- [{"ATTENTION_SERVER_DASHBOARD_CERTFILE", "b1.pem"},
                                  {"ATTENTION_SERVER_DASHBOARD_KEYFILE", "b1.key"}] do
      # Match runtime's independent fallback to the captured Server component.
      {root, manifest} = bundle(certs, [], %{}, Map.take(configured, [variable]))
      assert :ok = MaterialIdentity.validate(root, manifest)
      {filename, _} = configured[variable]
      wrong = %{variable => {filename, File.read!(Path.join(certs, wrong_file))}}
      {root, manifest} = bundle(certs, [], %{}, wrong)
      assert {:error, :material_dashboard_identity_mismatch} = MaterialIdentity.validate(root, manifest)
    end
    for variable <- ["UNCONFIGURED_FILE", "ATTENTION_SERVER_JEV_SCORE_RECORD_PATH"] do
      {root, manifest} = bundle(certs, [], %{}, %{variable => {"not-imported", "synthetic output"}})
      assert {:error, :unsupported_or_missing_identity_material} = MaterialIdentity.validate(root, manifest)
    end
  end

  test "optional configured files may be disabled but never evaluated" do
    assert {:ok, values} = MaterialProvenance.configuration("ATTENTION_SERVER_APNS_KEY_PATH=''\nATTENTION_SERVER_FEATURE_FLAGS_PATH=\"\"")
    assert values["ATTENTION_SERVER_APNS_KEY_PATH"] == ""
    for variable <- ["ATTENTION_SERVER_JEV_KEY_PATH", "ATTENTION_SERVER_DASHBOARD_KEYFILE", "ATTENTION_SERVER_FEATURE_FLAGS_PATH"] do
      assert {:error, :unsupported_tls_configuration} = MaterialProvenance.configuration(variable <> "=$UNEXPANDED/key")
      assert {:error, :unsupported_tls_configuration} = MaterialProvenance.configuration(variable <> "=/a\n" <> variable <> "=/b")
    end
  end

  defp bundle(certs, invitations, changes \\ %{}, configured \\ %{}) do
    root = Path.join(System.tmp_dir!(), "material-identity-#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(root, "material"))
    on_exit(fn -> File.rm_rf!(root) end)
    files = Map.merge(%{"owner/identity.pem" => "expired.pem", "owner/identity.key" => "expired.key",
      "tls/server.pem" => "server.pem", "tls/server.key" => "server.key", "tls/ca.pem" => "ca.pem",
      "tls/ca.key" => "ca.key"}, changes)
    material = Map.new(files, fn {name, file} -> {name, Path.join(certs, file)} end)
    person = Path.join(root, "source-person")
    File.write!(person, "synthetic-person\n")
    env = Path.join(root, "source-env")
    File.write!(env, Enum.map_join([{"CERTFILE", "tls/server.pem"}, {"KEYFILE", "tls/server.key"},
      {"CACERTFILE", "tls/ca.pem"}], "\n", fn {key, name} ->
        "export ATTENTION_SERVER_#{key}='#{material[name]}'"
      end))
    external_root = root <> "-configured"
    File.mkdir_p!(external_root)
    on_exit(fn -> File.rm_rf!(external_root) end)
    material = Enum.reduce(configured, material, fn {variable, {filename, bytes}}, acc ->
      path = Path.join(external_root, filename)
      File.write!(path, bytes)
      File.write!(env, "\n#{variable}='#{path}'\n", [:append])
      Map.put(acc, "external" <> path, path)
    end)
    material = material |> Map.put("person-id", person) |> Map.put("server.env", env)
    {:ok, before} = MaterialProvenance.capture(material)
    facts = Map.new(material, fn {name, source} ->
      relative = "material/" <> name
      target = Path.join(root, relative)
      File.mkdir_p!(Path.dirname(target))
      File.cp!(source, target)
      {:ok, observed} = Source.file_observation(target)
      {relative, Map.take(observed, [:size, :sha256])}
    end)
    {:ok, provenance} = MaterialProvenance.confirm(before, root, facts)
    [{:Certificate, der, :not_encrypted}] = :public_key.pem_decode(File.read!(Path.join(certs, "expired.pem")))
    {:ok, owner} = DeviceIdentity.from_certificate(der)
    records = [{{:row, :owners, "synthetic-person"}, %{key: owner}}] ++
      Enum.with_index(invitations, fn configuration, index ->
        {{:row, :join_invitations, index}, %{configuration: configuration}}
      end)
    {:ok, summary} = Frames.write(Path.join(root, "records.frames"), records)
    {root, %{material: facts, material_provenance: provenance, data: %{records: summary}}}
  end
end
