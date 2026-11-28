if System.get_env("HARKLIGHT_MIGRATION_TEST_URL") do
  Code.require_file("../scripts/compatibility/scenarios.exs", __DIR__)

  defmodule AttentionServer.Migration.CLIPipelineTest do
    use ExUnit.Case, async: false
    alias AttentionServer.{CompatibilityScenarios, JSON}
    alias AttentionServer.Migration.{CLI, ImportPipeline}

    @tag timeout: 600_000
    test "command path prepares an empty owned database, resumes and validates without activation" do
      root = Path.join(System.tmp_dir!(), "migration-cli-#{System.unique_integer([:positive])}")
      File.mkdir!(root)
      File.chmod!(root, 0o700)
      on_exit(fn -> File.rm_rf!(root) end)
      fixture = CompatibilityScenarios.fixture(Path.join(root, "source"))
      {:ok, relay} = CompatibilityScenarios.start(fixture)
      history = try do
        CompatibilityScenarios.run(relay, fixture)
      after
        GenServer.stop(relay)
      end
      certs = Path.join(fixture.root, "certs")
      env_file = Path.join(root, "server.env")
      person = Path.join(root, "person-id")
      File.write!(person, hd(history["people"]) <> "\n")
      File.write!(env_file, Enum.map_join([
        {"CERTFILE", "server.pem"}, {"KEYFILE", "server.key"}, {"CACERTFILE", "ca.pem"}
      ], "\n", fn {key, name} -> "ATTENTION_SERVER_#{key}='#{Path.join(certs, name)}'" end))
      material = %{"server.env" => env_file, "person-id" => person,
        "owner/identity.pem" => Path.join(certs, "expired.pem"), "owner/identity.key" => Path.join(certs, "expired.key"),
        "tls/server.pem" => Path.join(certs, "server.pem"), "tls/server.key" => Path.join(certs, "server.key"),
        "tls/ca.pem" => Path.join(certs, "ca.pem"), "tls/ca.key" => Path.join(certs, "ca.key")}
      map = Path.join(root, "material-map.json")
      File.write!(map, JSON.encode!(material))
      source = Path.join(fixture.root, "relay.sqlite3")
      original = File.read!(source)
      bundle = Path.join(root, "bundle")
      receipt = Path.join(root, "receipt.json")
      assert {:ok, %{"bundle_version" => 2}} = CLI.run(["export", "--source", source, "--output", bundle,
        "--supervisor", "synthetic fixture", "--stopped-evidence", "fixture Relay stopped", "--material-map", map])
      assert {:ok, _} = CLI.run(["verify", bundle])
      assert {:error, :receipt_missing} = CLI.run(["validate", "--bundle", bundle, "--receipt", receipt])
      assert {:error, :invalid_arguments} = CLI.run(["import", "--bundle", bundle, "--bundle", bundle])
      uri = URI.parse(System.fetch_env!("HARKLIGHT_MIGRATION_TEST_URL"))
      assert uri.host in ["localhost", "127.0.0.1"]
      password = Path.join(root, "db-password")
      File.write!(password, "synthetic-cli-password")
      File.chmod!(password, 0o600)
      env = %{"HARKLIGHT_MIGRATOR_URL" => "postgres://harklight_migrator@127.0.0.1:#{uri.port}/migration_cli",
        "HARKLIGHT_MIGRATOR_PASSWORD_FILE" => password}
      previous = Map.new(Map.keys(env), &{&1, System.get_env(&1)})
      try do
        System.put_env(env)
        args = ["--bundle", bundle, "--receipt", receipt]
        assert {:ok, imported} = CLI.run(["import" | args])
        assert imported["command"] == "import" and imported["phase"] == "loading"
        assert imported["validation_sha256"] == nil
        assert {:ok, ^imported} = CLI.run(["import" | args])
        assert {:ok, %{"phase" => "loading"}} = CLI.run(["status", "--receipt", receipt])
        assert Bitwise.band(File.stat!(receipt).mode, 0o777) == 0o600
        staged = Path.join([ImportPipeline.material_root(receipt), "files", "owner", "identity.key"])
        bytes = File.read!(staged)
        File.write!(staged, "synthetic corruption")
        assert {:error, :material_content_mismatch} = CLI.run(["validate" | args])
        assert {:ok, %{"phase" => "loading", "validation_sha256" => nil}} = CLI.run(["status", "--receipt", receipt])
        File.write!(staged, bytes)
        assert {:ok, validated} = CLI.run(["validate" | args])
        assert validated["command"] == "validate" and validated["phase"] == "validated"
        assert byte_size(validated["validation_sha256"]) == 64
        assert validated["import_id"] == imported["import_id"]
        assert validated["material"] == imported["material"]
        assert {:ok, ^validated} = CLI.run(["validate" | args])
        assert {:ok, %{"phase" => "validated"}} = CLI.run(["status", "--receipt", receipt])
        assert {:ok, resumed} = CLI.run(["import" | args])
        assert resumed["phase"] == "validated" and resumed["validation_sha256"] == validated["validation_sha256"]
        assert resumed["material"] == validated["material"]
        assert {:ok, %{"migration_ready" => false}} = CLI.run(["capabilities"])
        assert {:error, :invalid_arguments} = CLI.run(["activate", "--receipt", receipt])
      after
        Enum.each(previous, fn {key, value} ->
          if is_nil(value), do: System.delete_env(key), else: System.put_env(key, value)
        end)
      end
      assert File.read!(source) == original
      assert {:ok, _} = CLI.run(["verify", bundle])
    end
  end
end
