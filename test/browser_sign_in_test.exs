defmodule AttentionServer.BrowserSignInTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    BrowserSignIn,
    DeviceIdentity,
    Engine,
    HeadlessOwner,
    HeadlessTLS,
    OwnerTLS,
    Relay,
    Store,
    TLSFixtures
  }

  @person "00000000-0000-4000-8000-000000000001"
  @other "00000000-0000-4000-8000-000000000002"
  @owner String.duplicate("1", 64)
  @other_owner String.duplicate("2", 64)
  @device String.duplicate("3", 64)

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup do
    {:ok, engine} = Engine.operator_bootstrap(Engine.new(), @person, @owner)
    {:ok, engine} = Engine.operator_bootstrap(engine, @other, @other_owner)
    {:ok, hash} = BrowserSignIn.token_hash("hbs1_" <> String.duplicate("a", 64))
    %{engine: engine, hash: hash}
  end

  test "only current person owner issues a ten-minute login, without device or sender grants",
       c do
    {:ok, engine} = Engine.setup_device(c.engine, @person, @device)
    assert {:error, :forbidden} = BrowserSignIn.issue(engine, @person, @device, c.hash, 100)
    assert {:error, :forbidden} = BrowserSignIn.issue(engine, @other, @owner, c.hash, 100)
    assert {:ok, issued, record} = BrowserSignIn.issue(engine, @person, @owner, c.hash, 100)
    assert record.person == @person
    assert record.owner_key == @owner
    assert record.expires_at == 600_100
    assert issued.devices == engine.devices
    assert issued.sender_grants == engine.sender_grants
    assert issued.join_invitations == engine.join_invitations
    assert issued.pending_enrollments == engine.pending_enrollments
    assert {:error, :collision} = BrowserSignIn.issue(issued, @person, @owner, c.hash, 101)
  end

  test "consume authenticates exactly the stored person-owner once with no caller-selected target",
       c do
    {:ok, issued, _} = BrowserSignIn.issue(c.engine, @person, @owner, c.hash, 100)
    assert {:ok, consumed, principal} = BrowserSignIn.consume(issued, c.hash, 101)

    assert principal == %{
             person_id: @person,
             owner_key: @owner,
             owner_revision: 1,
             scope: :person_owner
           }

    assert {:error, :used} = BrowserSignIn.consume(consumed, c.hash, 102)

    assert {:error, :invalid_sign_in} =
             BrowserSignIn.consume(consumed, :crypto.hash(:sha256, "unknown"), 102)

    assert consumed.devices == %{}
    assert consumed.sender_grants == %{}
  end

  test "login credentials cannot be device invites or opaque enrollment status handles", c do
    raw = String.duplicate("a", 64)
    assert {:error, :invalid_sign_in} = BrowserSignIn.token_hash(raw)
    assert {:error, :invalid_sign_in} = BrowserSignIn.token_hash("hbs1_" <> String.upcase(raw))
    assert {:error, :invalid_sign_in} = BrowserSignIn.token_hash(%{"person_id" => @person})
    refute c.hash == :crypto.hash(:sha256, Base.decode16!(raw, case: :lower))
    {:ok, issued, _} = BrowserSignIn.issue(c.engine, @person, @owner, c.hash, 100)
    assert {:error, :not_found} = Engine.pending_status(issued, c.hash, 101)
    assert issued.join_invitations == %{}
  end

  test "expiry and observed clock rollback cannot revive a login or alter enrollment time", c do
    {:ok, issued, record} = BrowserSignIn.issue(c.engine, @person, @owner, c.hash, 100)
    assert {:error, :expired} = BrowserSignIn.consume(issued, c.hash, record.expires_at)
    observed = BrowserSignIn.observe_time(issued, c.hash, record.expires_at)
    assert {:error, :expired} = BrowserSignIn.consume(observed, c.hash, 101)
    assert observed.pending_clock_high_water == c.engine.pending_clock_high_water
    other_hash = :crypto.hash(:sha256, "fresh-login")

    assert {:error, :clock_unavailable} =
             BrowserSignIn.issue(observed, @person, @owner, other_hash, 101)

    assert {:ok, cleaned, _} =
             BrowserSignIn.issue(observed, @person, @owner, other_hash, record.retain_until)

    refute Map.has_key?(cleaned.browser_sign_ins, c.hash)
  end

  test "owner recovery invalidates an old issuer without delegating browser authority", c do
    {:ok, issued, _} = BrowserSignIn.issue(c.engine, @person, @owner, c.hash, 100)
    {:ok, recovered} = Engine.operator_recover(issued, @person, @device)
    assert {:error, :invalid_sign_in} = BrowserSignIn.consume(recovered, c.hash, 101)
    assert {:error, :forbidden} = BrowserSignIn.issue(recovered, @person, @owner, c.hash, 101)
    new_hash = :crypto.hash(:sha256, "new-current-owner")
    {:ok, fresh, _} = BrowserSignIn.issue(recovered, @person, @device, new_hash, 101)

    assert {:ok, _, %{owner_key: @device, owner_revision: 2, person_id: @person}} =
             BrowserSignIn.consume(fresh, new_hash, 102)
  end

  test "concurrent consumption has one durable winner and raw token never enters the snapshot",
       c do
    path = database_path()
    relay = start_supervised!({Relay, store_path: path})
    owner_cert = TLSFixtures.certificate_der!(c.certs, "a1")
    {:ok, person} = Relay.operator_bootstrap(relay, owner_cert)
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner_cert)
    {:ok, credential} = Relay.owner_browser_sign_in_create(relay, person, owner_key)
    token = credential["sign_in_token"]
    serialized = :erlang.term_to_binary(:sys.get_state(relay).store.engine)
    refute String.contains?(serialized, token)
    raw = token |> String.replace_prefix("hbs1_", "") |> Base.decode16!(case: :lower)
    refute String.contains?(serialized, raw)
    results = for _ <- 1..4, do: Task.async(fn -> Relay.consume_browser_sign_in(relay, token) end)
    replies = Enum.map(results, &Task.await/1)

    assert Enum.count(replies, &match?({:ok, %{person_id: ^person, scope: :person_owner}}, &1)) ==
             1

    assert Enum.count(replies, &(&1 == {:error, :used})) == 3
    {:ok, _fresh} = Relay.owner_browser_sign_in_create(relay, person, owner_key)
    assert {:error, :invalid_sign_in} = Relay.consume_browser_sign_in(relay, token)
    stop_supervised(Relay)
    restarted = start_supervised!({Relay, store_path: path})
    assert {:error, :invalid_sign_in} = Relay.consume_browser_sign_in(restarted, token)
    assert :sys.get_state(restarted).store.engine.devices == c.engine.devices
    assert :sys.get_state(restarted).store.engine.sender_grants == %{}
  end

  test "unconsumed login survives restart and stale writer cannot return an authenticated principal",
       c do
    path = database_path()
    relay = start_supervised!({Relay, store_path: path})
    owner_cert = TLSFixtures.certificate_der!(c.certs, "a1")
    {:ok, person} = Relay.operator_bootstrap(relay, owner_cert)
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner_cert)
    {:ok, credential} = Relay.owner_browser_sign_in_create(relay, person, owner_key)
    token = credential["sign_in_token"]
    stop_supervised(Relay)
    restarted = start_supervised!({Relay, store_path: path})
    {:ok, stale} = Relay.start_link(store_path: path)
    on_exit(fn -> if Process.alive?(stale), do: GenServer.stop(stale) end)
    assert {:ok, %{person_id: ^person}} = Relay.consume_browser_sign_in(restarted, token)
    assert {:error, :storage_unavailable} = Relay.consume_browser_sign_in(stale, token)
    assert {:error, :used} = Relay.consume_browser_sign_in(restarted, token)

    assert {:error, :invalid_sign_in} =
             Relay.consume_browser_sign_in(restarted, String.duplicate("a", 64))
  end

  test "fresh login survives a full store of expired, spent or retired-issuer credentials",
       c do
    full =
      Enum.reduce(1..1024, c.engine, fn index, engine ->
        hash = :crypto.hash(:sha256, "retired-issuer-#{index}")
        {:ok, next, _} = BrowserSignIn.issue(engine, @person, @owner, hash, 100)
        next
      end)

    assert {:error, :capacity} = BrowserSignIn.issue(full, @person, @owner, c.hash, 101)
    assert {:ok, _, _} = BrowserSignIn.issue(full, @person, @owner, c.hash, 600_100)

    spent =
      Enum.reduce(Map.keys(full.browser_sign_ins), full, fn hash, engine ->
        {:ok, next, _} = BrowserSignIn.consume(engine, hash, 101)
        next
      end)

    assert {:ok, fresh_after_spent, _} = BrowserSignIn.issue(spent, @person, @owner, c.hash, 102)
    assert {:ok, _, %{person_id: @person}} = BrowserSignIn.consume(fresh_after_spent, c.hash, 103)
    {:ok, recovered} = Engine.operator_recover(full, @person, @device)
    assert {:ok, fresh, _} = BrowserSignIn.issue(recovered, @person, @device, c.hash, 101)

    assert {:ok, _, %{person_id: @person, owner_key: @device, owner_revision: 2}} =
             BrowserSignIn.consume(fresh, c.hash, 102)

    other_hash = :crypto.hash(:sha256, "other-person-login")
    assert {:ok, other, _} = BrowserSignIn.issue(recovered, @other, @other_owner, other_hash, 101)

    assert {:ok, _, %{person_id: @other, owner_key: @other_owner, owner_revision: 1}} =
             BrowserSignIn.consume(other, other_hash, 102)

    assert fresh.devices == recovered.devices
    assert other.sender_grants == recovered.sender_grants
  end

  test "owner TLS issues only a same-person credential and refuses caller-supplied identity or lifetime",
       c do
    relay = start_supervised!({Relay, store_path: database_path()})
    owner_cert = TLSFixtures.certificate_der!(c.certs, "a1")
    {:ok, person} = Relay.operator_bootstrap(relay, owner_cert)
    {:ok, other} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(c.certs, "b1"))

    listener =
      start_supervised!(
        {OwnerTLS,
         relay: relay,
         certfile: Path.join(c.certs, "server.pem"),
         keyfile: Path.join(c.certs, "server.key"),
         cacertfile: Path.join(c.certs, "ca.pem")}
      )

    endpoint = %{
      address: {127, 0, 0, 1},
      name: "relay.test",
      ca_file: Path.join(c.certs, "ca.pem"),
      port: OwnerTLS.port(listener)
    }

    cert = Path.join(c.certs, "a1.pem")
    key = Path.join(c.certs, "a1.key")
    assert {:error, "not_found"} = HeadlessOwner.browser_sign_in(endpoint, cert, key, other)

    assert {:ok, %{"error" => "invalid_request"}} =
             HeadlessTLS.owner_request(endpoint, cert, key, %{
               "v" => 1,
               "type" => "browser_sign_in_create",
               "person_id" => person,
               "expires_at" => 99_999_999_999_999
             })

    assert {:ok, %{"browser_sign_in" => credential}} =
             HeadlessOwner.browser_sign_in(endpoint, cert, key, person)

    assert credential["person_id"] == person
    assert {:ok, principal} = Relay.consume_browser_sign_in(relay, credential["sign_in_token"])
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner_cert)

    assert principal == %{
             person_id: person,
             owner_key: owner_key,
             owner_revision: 1,
             scope: :person_owner
           }

    assert :sys.get_state(relay).store.engine.sender_grants == %{}
  end

  test "older snapshots gain no login credential while retaining owner/device/invitation state",
       c do
    path = database_path()
    {:ok, store} = Store.open(path)
    Store.close(store)
    {:ok, device_engine} = Engine.setup_device(c.engine, @person, @device)
    legacy = Map.drop(device_engine, [:browser_sign_ins, :browser_sign_in_clock_high_water])
    payload = :erlang.term_to_binary({:attention_relay_state, 13, legacy}, [{:compressed, 6}])
    {:ok, db} = Exqlite.Sqlite3.open(path)

    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ? WHERE id = 1")

    :ok = Exqlite.Sqlite3.bind(stmt, [{:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}])
    :done = Exqlite.Sqlite3.step(db, stmt)
    :ok = Exqlite.Sqlite3.release(db, stmt)
    :ok = Exqlite.Sqlite3.close(db)
    {:ok, migrated} = Store.open(path)
    assert migrated.engine.browser_sign_ins == %{}
    assert migrated.engine.browser_sign_in_clock_high_water == 0

    assert Map.drop(migrated.engine, [:browser_sign_ins, :browser_sign_in_clock_high_water]) ==
             legacy

    Store.close(migrated)
  end

  defp database_path do
    dir = Path.join(System.tmp_dir!(), "browser-login-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    Path.join(dir, "relay.sqlite3")
  end
end
