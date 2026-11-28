if System.get_env("HARKLIGHT_MIGRATION_TEST_URL") do
  defmodule AttentionServer.Migration.PrepareSchemaTest do
    use ExUnit.Case, async: false
    alias AttentionServer.Migration.{ImportSession, PrepareSchema}
    alias AttentionServer.Repository.Postgres.Schema

    defp options(database) do
      uri = URI.parse(System.fetch_env!("HARKLIGHT_MIGRATION_TEST_URL"))
      assert uri.host in ["localhost", "127.0.0.1"]
      [user, password] = String.split(uri.userinfo, ":", parts: 2)
      [hostname: uri.host, port: uri.port, username: URI.decode(user),
       password: URI.decode(password), database: database, log: false]
    end
    defp migration_options(opts), do: Keyword.merge(opts, username: "fixture_migrator", password: "synthetic-migrator-password")
    defp roles, do: %{migrator: "fixture_migrator", runtime: "fixture_runtime"}
    defp prepare(opts, id), do: PrepareSchema.run(migration_options(opts), id, roles())

    defp identity do
      %{id: "synthetic-schema-import", source: :crypto.hash(:sha256, "synthetic bundle"), schema: Schema.digest()}
    end

    test "fresh explicit setup is serialized, repeatable and respects loading custody" do
      opts = options("migration_schema_fresh")
      db = start_supervised!({Postgrex, opts})
      id = identity()
      assert {:ok, _} = ImportSession.query(db, "SELECT pg_advisory_lock(1212240455,1)", [])
      assert {:error, :schema_preparation_refused} = prepare(opts, id)
      assert {:ok, _} = ImportSession.query(db, "SELECT pg_advisory_unlock(1212240455,1)", [])
      version = Schema.version()
      runtime_opts = Keyword.merge(opts, username: "fixture_runtime", password: "synthetic-runtime-password")
      assert {:error, :migration_role_boundary_required} = PrepareSchema.run(runtime_opts, id, roles())
      assert {:ok, _} = ImportSession.query(db, "GRANT CREATE ON DATABASE migration_schema_fresh TO fixture_runtime", [])
      assert {:error, :migration_role_boundary_required} = PrepareSchema.run(runtime_opts, id, roles())
      assert {:error, :migration_role_boundary_required} = prepare(opts, id)
      assert {:ok, %{rows: [[nil]]}} = ImportSession.query(db, "SELECT to_regnamespace('harklight')::text", [])
      assert {:ok, _} = ImportSession.query(db, "REVOKE CREATE ON DATABASE migration_schema_fresh FROM fixture_runtime", [])
      assert {:ok, _} = ImportSession.query(db, "ALTER DATABASE migration_schema_fresh OWNER TO fixture", [])
      assert {:error, :migration_role_boundary_required} = prepare(opts, id)
      assert {:ok, _} = ImportSession.query(db, "ALTER DATABASE migration_schema_fresh OWNER TO fixture_migrator", [])
      assert {:ok, %{schema_version: ^version}} = prepare(opts, id)
      assert {:ok, %{schema_version: ^version}} = prepare(opts, id)
      assert {:error, :migration_role_boundary_required} = PrepareSchema.run(runtime_opts, id, roles())
      assert {:ok, :claimed} = ImportSession.claim(db, id)
      assert {:error, :import_identity_or_phase_mismatch} = prepare(opts, %{id | id: "different"})
      assert {:ok, %{schema_version: ^version}} = prepare(opts, id)
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.import_state SET phase='validated',manifest_sha256=$1", [:crypto.hash(:sha256, "synthetic validation")])
      assert {:error, :import_identity_or_phase_mismatch} = prepare(opts, id)
    end

    test "a claimed earlier schema advances without changing its pinned identity or data" do
      opts = options("migration_schema_partial")
      db = start_supervised!({Postgrex, opts})
      digest = :crypto.hash(:sha256, File.read!(Application.app_dir(:attention_server, "priv/postgres/001_initial.sql")))
      id = %{identity() | schema: digest}
      assert {:ok, _} = ImportSession.query(db,
        "INSERT INTO harklight.import_state(import_id,source_sha256,schema_sha256,phase) VALUES($1,$2,$3,'loading')", [id.id, id.source, digest])
      assert {:ok, _} = ImportSession.query(db,
        "INSERT INTO harklight.server_identity(instance_id,cursor_signing_key,cursor_epoch) VALUES('synthetic-instance',$1,17)", [:crypto.hash(:sha256, "synthetic cursor")])
      version = Schema.version()
      assert {:ok, %{schema_version: ^version}} = prepare(opts, id)
      assert {:ok, %{rows: [[^digest, "loading"]]}} = ImportSession.query(db,
        "SELECT schema_sha256,phase FROM harklight.import_state", [])
      assert {:ok, %{rows: [[17]]}} = ImportSession.query(db, "SELECT cursor_epoch FROM harklight.server_identity", [])
      assert {:ok, :resumed} = ImportSession.claim(db, id)
      assert {:ok, _} = ImportSession.query(db, "INSERT INTO harklight.schema_migrations(version) VALUES($1)", [version + 1])
      assert {:error, :unsupported_schema} = prepare(opts, id)
    end

    test "an existing foreign database is unchanged and never bootstrapped" do
      opts = options("migration_schema_foreign")
      db = start_supervised!({Postgrex, opts})
      assert {:ok, _} = ImportSession.query(db, "CREATE TABLE public.synthetic_existing(value integer)", [])
      assert {:ok, _} = ImportSession.query(db, "INSERT INTO public.synthetic_existing VALUES(17)", [])
      assert {:error, :destination_not_empty} = prepare(opts, identity())
      assert {:ok, %{rows: [[17]]}} = ImportSession.query(db, "SELECT value FROM public.synthetic_existing", [])
      assert {:ok, %{rows: [[nil]]}} = ImportSession.query(db, "SELECT to_regnamespace('harklight')::text", [])
    end
  end
end
