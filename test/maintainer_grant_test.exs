defmodule AttentionServer.MaintainerGrantTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DeviceIdentity, HeadlessOwner, HeadlessTLS, OwnerTLS, Relay, TLSFixtures}
  alias Exqlite.Sqlite3

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  test "format-five snapshot gains empty maintainer foundation with one audit migration" do
    dir =
      Path.join(
        System.tmp_dir!(),
        "attention-maintainer-migration-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")
    {:ok, relay} = Relay.start_link(store_path: path)
    before = :sys.get_state(relay).store
    GenServer.stop(relay)

    payload =
      :erlang.term_to_binary({:attention_relay_state, 5, before.engine}, [{:compressed, 6}])

    assert {:ok, db} = Sqlite3.open(path)
    assert {:ok, stmt} = Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ?")
    assert :ok = Sqlite3.bind(stmt, [{:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}])
    assert :done = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)

    assert {:ok, migrated} = AttentionServer.Store.open(path)
    assert migrated.version == before.version + 1
    assert migrated.engine.maintainer_grants == %{}
    assert :ok = AttentionServer.Store.close(migrated)

    assert {:ok, again} = AttentionServer.Store.open(path)
    assert again.version == migrated.version
    assert :ok = AttentionServer.Store.close(again)
    assert {:ok, db} = Sqlite3.open(path)

    assert {:ok, stmt} =
             Sqlite3.prepare(
               db,
               "SELECT actor_id, action FROM relay_audit ORDER BY id DESC LIMIT 1"
             )

    assert {:row, ["schema_migration", "maintainer_state_init"]} = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)
  end

  test "owner creates and revokes durable inert grant; cross-person access and exercise stay denied",
       %{certs: certs} do
    dir =
      Path.join(System.tmp_dir!(), "attention-maintainer-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")
    {:ok, relay} = Relay.start_link(store_path: path)

    {:ok, listener} =
      OwnerTLS.start_link(
        relay: relay,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem")
      )

    on_exit(fn ->
      if Process.alive?(listener), do: GenServer.stop(listener)
      if Process.alive?(relay), do: GenServer.stop(relay)
    end)

    owner_a = TLSFixtures.certificate_der!(certs, "a1")
    owner_b = TLSFixtures.certificate_der!(certs, "b1")
    replacement = TLSFixtures.certificate_der!(certs, "unknown")
    {:ok, owner_a_key} = DeviceIdentity.from_certificate(owner_a)
    {:ok, owner_b_key} = DeviceIdentity.from_certificate(owner_b)
    {:ok, replacement_key} = DeviceIdentity.from_certificate(replacement)
    assert {:ok, person_a} = Relay.operator_bootstrap(relay, owner_a)
    assert {:ok, person_b} = Relay.operator_bootstrap(relay, owner_b)
    grant_id = "00000000-0000-4000-8000-000000000077"

    endpoint = %{
      address: {127, 0, 0, 1},
      name: "relay.test",
      port: OwnerTLS.port(listener),
      ca_file: Path.join(certs, "ca.pem")
    }

    cert_a = Path.join(certs, "a1.pem")
    key_a = Path.join(certs, "a1.key")
    cert_b = Path.join(certs, "b1.pem")
    key_b = Path.join(certs, "b1.key")

    assert {:error, "not_found"} =
             HeadlessOwner.grant_maintainer(
               endpoint,
               cert_b,
               key_b,
               person_a,
               grant_id,
               person_b,
               ["devices", "senders"]
             )

    assert {:ok, %{"ok" => true}} =
             HeadlessOwner.grant_maintainer(
               endpoint,
               cert_a,
               key_a,
               person_a,
               grant_id,
               person_b,
               ["devices", "senders"]
             )

    assert {:ok, %{"ok" => true}} =
             HeadlessOwner.grant_maintainer(
               endpoint,
               cert_a,
               key_a,
               person_a,
               grant_id,
               person_b,
               ["devices", "senders"]
             )

    grant = :sys.get_state(relay).store.engine.maintainer_grants[grant_id]
    assert grant.target_person == person_a
    assert grant.grantee_person == person_b
    assert grant.scopes == MapSet.new(["devices", "senders"])
    assert grant.granted_by == owner_a_key
    assert is_integer(grant.granted_at)
    assert is_nil(grant.revoked_at)
    assert {:error, :forbidden} = Relay.owner_revision(relay, person_a, owner_b_key)

    assert {:ok, %{"error" => "not_supported"}} =
             HeadlessTLS.owner_request(endpoint, cert_b, key_b, %{
               "v" => 1,
               "type" => "maintainer_use",
               "person_id" => person_b,
               "grant_id" => grant_id,
               "scope" => "devices"
             })

    assert :ok = Relay.operator_recover(relay, person_a, replacement)

    assert {:error, _} =
             HeadlessOwner.revoke_maintainer(endpoint, cert_a, key_a, person_a, grant_id)

    assert {:ok, %{"ok" => true}} =
             HeadlessOwner.revoke_maintainer(
               endpoint,
               Path.join(certs, "unknown.pem"),
               Path.join(certs, "unknown.key"),
               person_a,
               grant_id
             )

    assert {:ok, %{"ok" => true}} =
             HeadlessOwner.revoke_maintainer(
               endpoint,
               Path.join(certs, "unknown.pem"),
               Path.join(certs, "unknown.key"),
               person_a,
               grant_id
             )

    revoked = :sys.get_state(relay).store.engine.maintainer_grants[grant_id]
    assert revoked.revoked_by == replacement_key
    assert revoked.revoked_at >= revoked.granted_at

    assert {:error, "not_found"} =
             HeadlessOwner.grant_maintainer(
               endpoint,
               Path.join(certs, "unknown.pem"),
               Path.join(certs, "unknown.key"),
               person_a,
               grant_id,
               person_b,
               ["devices", "senders"]
             )

    GenServer.stop(listener)
    GenServer.stop(relay)
    {:ok, restarted} = Relay.start_link(store_path: path)
    on_exit(fn -> if Process.alive?(restarted), do: GenServer.stop(restarted) end)
    assert :sys.get_state(restarted).store.engine.maintainer_grants[grant_id] == revoked
  end
end
