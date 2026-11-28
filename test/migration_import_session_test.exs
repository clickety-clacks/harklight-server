if System.get_env("HARKLIGHT_MIGRATION_TEST_URL") do
  Code.require_file("../scripts/compatibility/scenarios.exs", __DIR__)

  defmodule AttentionServer.Migration.ImportSessionTest do
    use ExUnit.Case, async: false
    alias AttentionServer.{CompatibilityScenarios, JSON, Relay}
    alias AttentionServer.Migration.{Bundle, Frames, ImportFacts, ImportOrganizations, ImportSession, ImportTables, SourceTables}
    alias AttentionServer.Repository.Postgres.Attributes

    for epoch <- [1, 17] do
      @tag timeout: 120_000, epoch: epoch
      test "interrupted migration and authority readback with epoch #{epoch}", %{epoch: epoch} do
        uri = URI.parse(System.fetch_env!("HARKLIGHT_MIGRATION_TEST_URL"))
        assert uri.host in ["localhost", "127.0.0.1"]
        [user, password] = String.split(uri.userinfo, ":", parts: 2)

        config = [
          hostname: uri.host,
          port: uri.port,
          username: URI.decode(user),
          password: URI.decode(password),
          database:
            if(epoch == 17,
              do: "migration_explicit_epoch",
              else: String.trim_leading(uri.path, "/")
            )
        ]

        db = start_supervised!({Postgrex, config})

        root =
          Path.join(System.tmp_dir!(), "import-history-#{System.unique_integer([:positive])}")

        File.mkdir!(root)
        File.chmod!(root, 0o700)
        on_exit(fn -> File.rm_rf!(root) end)
        fixture = CompatibilityScenarios.fixture(Path.join(root, "history"))
        {:ok, relay} = CompatibilityScenarios.start(fixture)

        try do
          CompatibilityScenarios.run(relay, fixture)
          {person, %{key: owner}} = Enum.at(:sys.get_state(relay).store.engine.owners, 0)
          device = Base.encode16(:crypto.hash(:sha256, "synthetic revoked device"), case: :lower)
          agent = Base.encode16(:crypto.hash(:sha256, "synthetic revoked agent"), case: :lower)
          assert :ok = Relay.setup_device(relay, person, device)
          assert :ok = Relay.setup_sender(relay, device, "app:device-scope")
          assert :ok = Relay.owner_revoke_device(relay, person, owner, device)

          assert :ok =
                   Relay.owner_register_agent(
                     relay,
                     person,
                     owner,
                     "30000000-0000-4000-8000-000000000001",
                     agent
                   )

          assert :ok =
                   Relay.owner_replace_agent_grants(
                     relay,
                     person,
                     owner,
                     agent,
                     1,
                     ["app:read"],
                     ["app:respond"]
                   )

          assert :ok =
                   Relay.owner_replace_agent_grants(
                     relay,
                     person,
                     owner,
                     agent,
                     2,
                     ["app:read", "app:read-more"],
                     ["app:respond"]
                   )

          assert :ok = Relay.owner_revoke_agent(relay, person, owner, agent)
        after
          GenServer.stop(relay)
        end

        # Synthetic setup: allocation high deliberately exceeds retained history.
        source_path = Path.join(fixture.root, "relay.sqlite3")
        expected_organizations = seed_organizations(source_path, epoch)
        {:ok, sqlite} = Exqlite.Sqlite3.open(source_path)

        {:ok, rows} =
          AttentionServer.Store.RecordTable.select(
            sqlite,
            "SELECT key,payload,checksum FROM relay_records"
          )

        {session_person, session_owner} = Enum.find_value(rows, fn [key, payload, hash] ->
          case AttentionServer.Store.RecordCodec.decode_row({key, payload, hash}) do
            {:ok, {:row, :owners, person}, owner} -> {person, owner}
            _ -> nil
          end
        end)
        {:ok, _} = AttentionServer.Store.AuthSchema.initialize(sqlite)
        :ok = AttentionServer.BrowserAuth.add_session(sqlite,
          :crypto.hash(:sha256, "synthetic migration browser session"),
          session_person, session_owner.key, session_owner.revision, "fixture")

        {address, _} =
          Enum.find_value(rows, fn [key, payload, hash] ->
            case AttentionServer.Store.RecordCodec.decode_row({key, payload, hash}) do
              {:ok, {:row, :persons, _} = address, value} -> {address, value}
              _ -> nil
            end
          end)

        {key, payload, hash} = AttentionServer.Store.RecordCodec.encode_row(address, 1_000_000)

        :ok =
          AttentionServer.Store.RecordTable.run(
            sqlite,
            "UPDATE relay_records SET payload=?,checksum=? WHERE key=?",
            [{:blob, payload}, {:blob, hash}, {:blob, key}]
          )

        for number <- 1..300 do
          {key, payload, hash} =
            AttentionServer.Store.RecordCodec.encode_row(
              {:row, :persons, "synthetic-extra-#{number}"},
              10_000 + number
            )

          :ok =
            AttentionServer.Store.RecordTable.run(
              sqlite,
              "INSERT INTO relay_records(key,payload,checksum,account) VALUES (?,?,?,1)",
              [{:blob, key}, {:blob, payload}, {:blob, hash}]
            )
        end

        if epoch == 17 do
          :ok =
            AttentionServer.Store.RecordTable.run(
              sqlite,
              "INSERT INTO client_change_log_epoch(id,epoch) VALUES(1,17) ON CONFLICT(id) DO UPDATE SET epoch=17",
              []
            )
        end

        :ok = Exqlite.Sqlite3.close(sqlite)
        bundle = Path.join(root, "export")

        assert {:ok, summary} =
                 Bundle.export(Path.join(fixture.root, "relay.sqlite3"), bundle,
                   supervisor: {:operator, "synthetic fixture", "seed Relay stopped"},
                   material: %{}
                 )

        {:ok, manifest, _} = Bundle.verify(bundle)

        identity = %{
          id: "synthetic-import",
          source: Base.decode16!(summary.sha256, case: :lower),
          schema: :crypto.hash(:sha256, File.read!("priv/postgres/001_initial.sql"))
        }

        # Refuse an already populated target rather than claiming it.
        {:ok, _} =
          ImportSession.query(db, "INSERT INTO harklight.people(id) VALUES ('occupied')", [])

        assert {:error, :destination_not_empty} = ImportSession.claim(db, identity)
        {:ok, _} = ImportSession.query(db, "DELETE FROM harklight.people WHERE id='occupied'", [])
        assert {:ok, :claimed} = ImportSession.claim(db, identity)
        assert {:ok, :resumed} = ImportSession.claim(db, identity)

        assert {:error, :import_identity_mismatch} =
                 ImportSession.claim(db, %{identity | id: "other"})

        {:ok, zero} = ImportSession.checkpoint(db, identity, "interruption")
        parent = self()

        interrupted =
          Task.async(fn ->
            {:ok, own_db} = Postgrex.start_link(config)

            ImportSession.batch(own_db, identity, "interruption", zero, [:before_failure], fn tx,
                                                                                              _ ->
              {:ok, _} =
                ImportSession.query(tx, "INSERT INTO harklight.people(id) VALUES ('partial')", [])

              send(parent, :uncommitted_rows_written)

              receive do
                :never -> :ok
              end
            end)
          end)

        assert_receive :uncommitted_rows_written, 5_000
        Task.shutdown(interrupted, :brutal_kill)
        assert {:ok, ^zero} = ImportSession.checkpoint(db, identity, "interruption")

        assert {:ok, %{rows: [[0]]}} =
                 ImportSession.query(db, "SELECT count(*) FROM harklight.people", [])

        assert {:ok, one} =
                 ImportSession.batch(db, identity, "interruption", zero, [:committed], fn tx, _ ->
                   case ImportSession.query(
                          tx,
                          "INSERT INTO harklight.people(id) VALUES ('committed')",
                          []
                        ) do
                     {:ok, _} -> :ok
                     error -> error
                   end
                 end)

        assert one.ordinal == 1

        assert {:error, :checkpoint_changed} =
                 ImportSession.batch(db, identity, "interruption", zero, [:stale], fn _, _ ->
                   flunk("stale checkpoint must not write")
                 end)

        # Fail the second batch after the first checkpoint has committed. This
        # trigger exists only in this dedicated throwaway database.
        {:ok, _} =
          ImportSession.query(
            db,
            """
            CREATE FUNCTION harklight.synthetic_interrupt() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
              IF (SELECT count(*) FROM harklight.compatibility_facts) >= 256 THEN
                RAISE EXCEPTION 'synthetic interruption after committed batch';
              END IF;
              RETURN NEW;
            END $$
            """,
            []
          )

        {:ok, _} =
          ImportSession.query(
            db,
            """
            CREATE TRIGGER synthetic_interrupt BEFORE INSERT ON harklight.compatibility_facts
            FOR EACH ROW EXECUTE FUNCTION harklight.synthetic_interrupt()
            """,
            []
          )

        assert {:error, _} = ImportFacts.run(db, identity, bundle)

        assert {:ok, %{ordinal: 256} = boundary} =
                 ImportSession.checkpoint(db, identity, "records")

        assert {:ok, %{rows: [[256]]}} =
                 ImportSession.query(db, "SELECT count(*) FROM harklight.compatibility_facts", [])

        {:ok, _} =
          ImportSession.query(
            db,
            "DROP TRIGGER synthetic_interrupt ON harklight.compatibility_facts",
            []
          )

        {:ok, _} =
          ImportSession.query(
            db,
            "UPDATE harklight.import_checkpoints SET cumulative_sha256=$1 WHERE family='records'",
            [:binary.copy(<<0>>, 32)]
          )

        assert {:error, :checkpoint_source_mismatch} = ImportFacts.run(db, identity, bundle)

        assert {:ok, %{rows: [[256]]}} =
                 ImportSession.query(db, "SELECT count(*) FROM harklight.compatibility_facts", [])

        {:ok, _} =
          ImportSession.query(
            db,
            "UPDATE harklight.import_checkpoints SET cumulative_sha256=$1 WHERE family='records'",
            [boundary.digest]
          )

        assert {:ok, complete} = ImportFacts.run(db, identity, bundle)
        assert complete.ordinal == manifest.data.records.count
        assert {:ok, ^complete} = ImportFacts.run(db, identity, bundle)
        assert {:ok, ^complete} = ImportSession.checkpoint(db, identity, "records")

        assert {:ok, %{rows: [[count]]}} =
                 ImportSession.query(db, "SELECT count(*) FROM harklight.compatibility_facts", [])

        assert count == complete.ordinal

        assert {:ok, :ok} =
                 Frames.fold(
                   Path.join(bundle, "records.frames"),
                   manifest.data.records,
                   :ok,
                   fn {address, expected}, :ok ->
                     {:ok, %{rows: [[text]]}} =
                       ImportSession.query(
                         db,
                         "SELECT body::text FROM harklight.compatibility_facts WHERE address = $1",
                         [Attributes.key(address)]
                       )

                     {:ok, envelope} = JSON.decode_line(text <> "\n")
                     assert {:ok, ^expected} = Attributes.decode(envelope)
                     {:ok, :ok}
                   end
                 )

        assert {:ok, %{rows: [["loading", nil]]}} =
                 ImportSession.query(
                   db,
                   "SELECT phase, manifest_sha256 FROM harklight.import_state",
                   []
                 )

        {:ok, _} =
          ImportSession.query(db, "DELETE FROM harklight.people WHERE id='committed'", [])

        assert :ok = ImportOrganizations.run(db, identity, bundle)
        assert :ok = ImportOrganizations.run(db, identity, bundle)
        verify_organizations(db, expected_organizations)
        assert :ok = ImportTables.run(db, identity, bundle)
        assert :ok = ImportTables.run(db, identity, bundle)
        verify_tables(db, bundle, manifest)

        assert {:ok, :ok} =
                 Frames.fold(
                   Path.join(bundle, "records.frames"),
                   manifest.data.records,
                   :ok,
                   fn entry, :ok ->
                     verify_account(db, entry)
                     {:ok, :ok}
                   end
                 )

        assert {:ok, %{rows: [["loading"]]}} =
                 ImportSession.query(db, "SELECT phase FROM harklight.import_state", [])

        assert {:ok, %{rows: [[^epoch]]}} =
                 ImportSession.query(db, "SELECT cursor_epoch FROM harklight.server_identity", [])

        assert {:ok, %{rows: [[2]]}} =
                 ImportSession.query(
                   db,
                   "SELECT count(*) FROM harklight.principals WHERE revoked=true AND revision=2",
                   []
                 )

        assert {:ok, %{rows: [[text]]}} =
                 ImportSession.query(
                   db,
                   "SELECT attributes::text FROM harklight.principals WHERE kind='agent' AND revoked=true",
                   []
                 )

        assert {:ok, envelope} = JSON.decode_line(text <> "\n")
        assert {:ok, agent} = Attributes.decode(envelope)
        assert agent.grant_revision == 4
        assert agent.read_grants == MapSet.new(["app:read", "app:read-more"])
        assert agent.respond_grants == MapSet.new(["app:respond"])
        assert length(agent.grant_history) == 4
        Agent.stop(fixture.clock)
      end
    end

    @tag timeout: 120_000
    test "legacy source without local roster retains initialization membership" do
      uri = URI.parse(System.fetch_env!("HARKLIGHT_MIGRATION_TEST_URL"))
      assert uri.host in ["localhost", "127.0.0.1"]
      [user, password] = String.split(uri.userinfo, ":", parts: 2)
      db = start_supervised!({Postgrex, [
        hostname: uri.host, port: uri.port,
        username: URI.decode(user), password: URI.decode(password),
        database: "migration_implicit_roster"
      ]})
      root = Path.join(System.tmp_dir!(), "import-legacy-#{System.unique_integer([:positive])}")
      File.mkdir!(root)
      File.chmod!(root, 0o700)
      on_exit(fn -> File.rm_rf!(root) end)
      fixture = CompatibilityScenarios.fixture(Path.join(root, "history"))
      {:ok, relay} = CompatibilityScenarios.start(fixture, shared_messages: false, groups: false)
      expected = try do
        %{person: person, other: other} = CompatibilityScenarios.provision(relay, fixture)
        {:ok, hosted} = Relay.hosted_account_create(relay, "synthetic-legacy-hosted")
        engine = :sys.get_state(relay).store.engine
        %{instance: engine.relay_instance_id, members: [person, other], hosted: hosted}
      after
        GenServer.stop(relay)
      end
      # Modern provisioning initializes the roster even with features off.
      # Reconstruct the valid legacy state after stopping, with no group facts.
      alias AttentionServer.{Engine, Store}
      {:ok, legacy} = Store.open_relay(Path.join(fixture.root, "relay.sqlite3"))
      assert legacy.engine.groups == %{}
      {engine, :ok, touched} = Engine.with_touches(legacy.engine, fn engine ->
        {Engine.delete_row(engine, :local_organizations, :default), :ok}
      end)
      {:ok, legacy} = Store.commit(legacy, {engine, touched}, %{person: "fixture", actor: "fixture", action: "legacy_roster"})
      refute Map.has_key?(legacy.engine.local_organizations, :default)
      :ok = Store.close(legacy)
      bundle = Path.join(root, "export")
      assert {:ok, summary} = Bundle.export(Path.join(fixture.root, "relay.sqlite3"), bundle,
        supervisor: {:operator, "synthetic fixture", "seed Relay stopped"}, material: %{})
      identity = %{id: "synthetic-legacy", source: Base.decode16!(summary.sha256, case: :lower),
        schema: :crypto.hash(:sha256, File.read!("priv/postgres/001_initial.sql"))}
      assert :ok = ImportOrganizations.run(db, identity, bundle)
      assert :ok = ImportOrganizations.run(db, identity, bundle)
      assert_members(db, expected.instance, expected.members)
      assert {:ok, %{rows: [[0]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.organization_members WHERE person_id=$1", [expected.hosted])
      assert {:ok, %{rows: [[text]]}} = ImportSession.query(db,
        "SELECT attributes::text FROM harklight.organizations WHERE id=$1", [expected.instance])
      assert_typed(text, nil)
      Agent.stop(fixture.clock)
    end

    defp verify_tables(db, bundle, manifest) do
      for {table, source} <- manifest.data.tables do
        family = table
        assert {:ok, %{rows: [[count]]}} = ImportSession.query(db,
          "SELECT count(*) FROM harklight.compatibility_facts WHERE family=$1", [family])
        case SourceTables.disposition(table) do
          {:durable, _} ->
            assert count == source.summary.count
            assert {:ok, :ok} = Frames.fold(Path.join(bundle, source.file), source.summary, :ok,
              fn values, :ok ->
                {:ok, address, _row} = SourceTables.fact(table, values)
                assert {:ok, %{rows: [[text]]}} = ImportSession.query(db,
                  "SELECT body::text FROM harklight.compatibility_facts WHERE family=$1 AND address=$2",
                  [family, Attributes.key(address)])
                assert_typed(text, values)
                {:ok, :ok}
              end)
          excluded when excluded in [:derived, :source_only] -> assert count == 0
        end
      end
      assert {:ok, %{rows: [[1]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.compatibility_facts WHERE family='harklight_browser_sessions'", [])
      assert {:ok, %{rows: [[text]]}} = ImportSession.query(db,
        "SELECT body::text FROM harklight.compatibility_facts WHERE family='harklight_auth_schema' AND address=$1", [Attributes.key(1)])
      {:ok, envelope} = JSON.decode_line(text <> "\n")
      assert {:ok, [1, 1, 0]} = Attributes.decode(envelope)
    end

    defp seed_organizations(path, epoch) do
      alias AttentionServer.{Engine, Groups, Store}
      {:ok, store} = Store.open_relay(path)
      {engine, :ok, touched} = Engine.with_touches(store.engine, fn engine ->
        [manager, invitee | _] = Enum.sort(Map.keys(engine.owners))
        extra = "50000000-0000-4000-8000-000000000001"
        hosted = "50000000-0000-4000-8000-000000000002"
        organization = "60000000-0000-4000-8000-000000000001"
        local_group = "70000000-0000-4000-8000-000000000001"
        hosted_group = "70000000-0000-4000-8000-000000000002"
        deleted_group = "70000000-0000-4000-8000-000000000003"
        invitation = "80000000-0000-4000-8000-000000000001"
        {:ok, engine} = Engine.operator_bootstrap(engine, extra, String.duplicate("e", 64))
        {:ok, engine, _} = Engine.hosted_account_create(engine, "synthetic-hosted", true, hosted, String.duplicate("f", 64))
        {:ok, engine} = Engine.replace_hosted_organization(engine, "synthetic-business", [hosted], organization)
        {:ok, engine, _} = Groups.replace_hosted(engine, {"synthetic-business", "team"}, hosted_group, "Hosted team", [hosted], 3)
        {:ok, engine, _} = Groups.mute(engine, hosted_group, hosted, true)
        {:ok, engine, _} = Groups.replace_hosted(engine, {"synthetic-business", "deleted"}, deleted_group, "Deleted team", [hosted], 1)
        {:ok, engine} = Groups.delete_hosted(engine, {"synthetic-business", "deleted"})
        {:ok, engine, _} = Groups.create(engine, local_group, manager, "Pending invitations")
        {:ok, engine, _, ^invitation} = Groups.invite(engine, local_group, manager, invitee, invitation)
        {:ok, engine, _} = Groups.mute(engine, local_group, manager, true)
        engine = if epoch == 1 do
          {:ok, explicit} = Engine.set_local_organization_member(engine, extra, true)
          explicit
        else
          {:ok, explicit} = Engine.set_local_organization_member(engine, extra, false)
          explicit
        end
        {engine, :ok}
      end)
      {:ok, store} = Store.commit(store, {engine, touched}, %{person: "fixture", actor: "fixture", action: "groups"})
      :ok = Store.close(store)
      %{
        instance: engine.relay_instance_id,
        local: Map.get(engine.local_organizations, :default),
        owners: Map.keys(engine.owners),
        accounts: engine.hosted_accounts,
        organizations: engine.hosted_organizations,
        groups: engine.groups
      }
    end

    defp verify_organizations(db, expected) do
      assert {:ok, %{rows: [[organization_count]]}} =
        ImportSession.query(db, "SELECT count(*) FROM harklight.organizations", [])
      assert organization_count == map_size(expected.organizations) + 1
      assert {:ok, %{rows: [[group_count]]}} =
        ImportSession.query(db, "SELECT count(*) FROM harklight.groups", [])
      assert group_count == map_size(expected.groups)
      assert {:ok, %{rows: [[account_count]]}} =
        ImportSession.query(db, "SELECT count(*) FROM harklight.hosted_accounts", [])
      assert account_count == map_size(expected.accounts)
      hosted_people = MapSet.new(Map.values(expected.accounts), & &1.person)
      local_members = expected.local || MapSet.difference(MapSet.new(expected.owners), hosted_people)
      assert_members(db, expected.instance, local_members)
      assert {:ok, %{rows: [[nil, text]]}} =
        ImportSession.query(db, "SELECT hosted_ref,attributes::text FROM harklight.organizations WHERE id=$1", [expected.instance])
      assert_typed(text, expected.local)
      for {ref, value} <- expected.accounts do
        assert {:ok, %{rows: [[person, text]]}} =
          ImportSession.query(db, "SELECT person_id,attributes::text FROM harklight.hosted_accounts WHERE external_ref=$1", [ref])
        assert person == value.person
        assert_typed(text, value)
      end
      for {ref, value} <- expected.organizations do
        assert_members(db, value.id, value.members)
        assert {:ok, %{rows: [[^ref, text]]}} =
          ImportSession.query(db, "SELECT hosted_ref,attributes::text FROM harklight.organizations WHERE id=$1", [value.id])
        assert_typed(text, value)
      end
      for {id, group} <- expected.groups do
        assert {:ok, %{rows: [[org, manager, name, hosted_ref, text]]}} =
          ImportSession.query(db, "SELECT organization_id,manager_id,name,hosted_ref,attributes::text FROM harklight.groups WHERE id=$1", [id])
        assert org == group.organization_id and manager == group.manager and name == group.name
        assert hosted_ref == if(group.hosted_ref, do: elem(group.hosted_ref, 1), else: nil)
        assert_typed(text, group)
        assert {:ok, %{rows: members}} =
          ImportSession.query(db, "SELECT person_id,muted FROM harklight.group_members WHERE group_id=$1 ORDER BY person_id", [id])
        assert members == Enum.map(Enum.sort(group.members), fn {person, muted} -> [person, muted] end)
        assert {:ok, %{rows: invitations}} =
          ImportSession.query(db, "SELECT person_id,id,state FROM harklight.group_invitations WHERE group_id=$1 ORDER BY person_id", [id])
        assert invitations == Enum.map(Enum.sort(group.invitations), fn {person, invitation} -> [person, invitation, "pending"] end)
      end
      # Concrete nonempty fixtures ensure each normalized branch ran.
      assert {:ok, %{rows: [[1]]}} =
        ImportSession.query(db, "SELECT count(*) FROM harklight.groups WHERE manager_id IS NULL", [])
      assert {:ok, %{rows: [[2]]}} =
        ImportSession.query(db, "SELECT count(*) FROM harklight.group_members WHERE muted", [])
      assert {:ok, %{rows: [[1]]}} =
        ImportSession.query(db, "SELECT count(*) FROM harklight.group_invitations WHERE state='pending'", [])
    end

    defp assert_members(db, organization, expected) do
      assert {:ok, %{rows: members}} =
        ImportSession.query(db, "SELECT person_id FROM harklight.organization_members WHERE organization_id=$1 ORDER BY person_id", [organization])
      assert List.flatten(members) == Enum.sort(expected)
    end

    defp verify_account(db, {{:row, field, person}, high})
         when field in [:persons, :free_order_highs, :conversation_highs] do
      kind =
        %{persons: "accepted", free_order_highs: "free_order", conversation_highs: "conversation"}[
          field
        ]

      assert {:ok, %{rows: [[^high]]}} =
               ImportSession.query(
                 db,
                 "SELECT high FROM harklight.allocation_highs WHERE person_id=$1 AND origin_key='' AND session_handle='' AND kind=$2",
                 [person, kind]
               )
    end

    defp verify_account(db, {{:row, :owners, person}, value}) do
      assert {:ok, %{rows: [[^person, "owner", revision, false, text]]}} =
               ImportSession.query(
                 db,
                 "SELECT person_id,kind,revision,revoked,attributes::text FROM harklight.principals WHERE key=$1",
                 [value.key]
               )

      assert revision == value.revision
      assert_typed(text, value)
    end

    defp verify_account(db, {{:row, field, key}, value}) when field in [:devices, :agents] do
      person = value.person
      kind = if field == :devices, do: "device", else: "agent"

      assert {:ok, %{rows: [[^person, ^kind, revision, revoked, text]]}} =
               ImportSession.query(
                 db,
                 "SELECT person_id,kind,revision,revoked,attributes::text FROM harklight.principals WHERE key=$1",
                 [key]
               )

      assert revision == value.enrollment_revision and revoked == value.revoked
      assert_typed(text, value)
    end

    defp verify_account(db, {{:row, :sender_grants, {_, key}}, patterns}) do
      assert {:ok, %{rows: rows}} =
               ImportSession.query(
                 db,
                 "SELECT pattern FROM harklight.sender_grants WHERE principal_key=$1 ORDER BY pattern",
                 [key]
               )

      assert List.flatten(rows) == Enum.sort(patterns)
    end

    defp verify_account(db, {{:row, :origin_sessions, {person, origin, handle}}, value}) do
      assert {:ok, %{rows: [[text]]}} =
               ImportSession.query(
                 db,
                 "SELECT attributes::text FROM harklight.origin_sessions WHERE person_id=$1 AND origin_key=$2 AND handle=$3",
                 [person, origin, handle]
               )

      assert_typed(text, value)
    end

    defp verify_account(db, {{:field, :relay_instance_id}, value}) do
      assert {:ok, %{rows: [[^value]]}} =
               ImportSession.query(db, "SELECT instance_id FROM harklight.server_identity", [])
    end

    defp verify_account(db, {{:field, :agent_cursor_key}, value}) do
      assert {:ok, %{rows: [[^value]]}} =
               ImportSession.query(
                 db,
                 "SELECT cursor_signing_key FROM harklight.server_identity",
                 []
               )
    end

    defp verify_account(db, {{:row, :people_profiles, person}, value}) do
      assert {:ok, %{rows: [[text]]}} =
               ImportSession.query(db, "SELECT profile::text FROM harklight.people WHERE id=$1", [
                 person
               ])

      assert_typed(text, value)
    end

    defp verify_account(db, {{:row, :delivery_states, person}, value}) do
      assert {:ok, %{rows: [[^value]]}} =
               ImportSession.query(
                 db,
                 "SELECT delivery_state FROM harklight.people WHERE id=$1",
                 [person]
               )
    end

    defp verify_account(db, {{:row, field, person}, value})
         when field in [:algorithm_choices, :alert_choices, :bounce_levels] do
      assert {:ok, %{rows: [[text]]}} =
               ImportSession.query(
                 db,
                 "SELECT settings::text FROM harklight.people WHERE id=$1",
                 [person]
               )

      assert {:ok, envelope} = JSON.decode_line(text <> "\n")
      assert {:ok, settings} = Attributes.decode(envelope)
      assert Map.fetch!(settings, field) == value
    end

    defp verify_account(_, _), do: :ok

    defp assert_typed(text, expected) do
      assert {:ok, envelope} = JSON.decode_line(text <> "\n")
      assert {:ok, ^expected} = Attributes.decode(envelope)
    end
  end
end
