defmodule AttentionServer.GuidedJoinTest do
  use ExUnit.Case, async: false
  alias AttentionServer.{DeviceIdentity, Engine, GuidedJoin, Relay, Store, TLSFixtures}

  @person "00000000-0000-4000-8000-000000000001"
  @other "00000000-0000-4000-8000-000000000002"
  @code "ABCDEFGH23"
  @other_code "JKLMNPQR45"

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    owner = TLSFixtures.certificate_der!(certs, "a1")
    other_owner = TLSFixtures.certificate_der!(certs, "b1")
    device = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner)
    {:ok, other_key} = DeviceIdentity.from_certificate(other_owner)
    {:ok, device_key} = DeviceIdentity.from_certificate(device)

    {:ok, config} =
      GuidedJoin.configuration("relay.test", 18444, 18443, File.read!(Path.join(certs, "ca.pem")))

    {:ok, engine} = Engine.operator_bootstrap(Engine.new(), @person, owner_key)
    {:ok, engine} = Engine.operator_bootstrap(engine, @other, other_key)

    %{
      engine: engine,
      owner: owner,
      owner_key: owner_key,
      other_key: other_key,
      device: device,
      device_key: device_key,
      config: config,
      token_hash: :crypto.hash(:sha256, "invite"),
      secret_hash: :crypto.hash(:sha256, "status")
    }
  end

  test "only current owner creates a person-bound configured-trust invitation", c do
    assert {:error, :forbidden} =
             GuidedJoin.create(c.engine, @person, c.other_key, c.token_hash, c.config, 0)

    assert {:ok, engine, invite} = create(c)
    assert invite.person == @person
    assert invite.expires_at == 1_800_000
    assert invite.configuration == c.config
    assert engine.devices == %{}
    assert engine.sender_grants == %{}
    assert {:error, :refused} = redeem(c, engine, %{c.config | "server_name" => "other.test"}, 1)
    {:ok, recovered} = Engine.operator_recover(engine, @person, c.device_key)
    assert {:error, :refused} = redeem(c, recovered, c.config, 1)
    assert {:error, :expired} = redeem(c, engine, c.config, 1_800_000)
    observed = GuidedJoin.observe_time(engine, c.token_hash, 1_800_000)
    assert {:error, _} = redeem(c, observed, c.config, 1)
  end

  test "redemption binds one exact key and retry does not replace or prolong pending request",
       c do
    {:ok, engine, _} = create(c)
    assert {:ok, pending, reply} = redeem(c, engine, c.config, 1)
    assert reply["state"] == "pending"
    assert reply["approval_code"] == @code
    assert reply["expires_at"] == 86_400_001
    assert pending.pending_enrollments[@code].person == @person
    assert pending.pending_enrollments[@code].key == c.device_key

    assert {:ok, ^pending, ^reply} =
             GuidedJoin.redeem(
               pending,
               c.token_hash,
               c.device,
               "changed retry label",
               c.secret_hash,
               @other_code,
               c.config,
               2
             )

    assert {:error, :used} =
             GuidedJoin.redeem(
               pending,
               c.token_hash,
               c.device,
               "same",
               :crypto.hash(:sha256, "other secret"),
               @other_code,
               c.config,
               2
             )

    assert {:error, :used} =
             GuidedJoin.redeem(
               pending,
               c.token_hash,
               c.owner,
               "same",
               c.secret_hash,
               @other_code,
               c.config,
               2
             )

    assert {:error, :expired} = redeem(c, pending, c.config, 86_400_001)
    assert map_size(pending.pending_enrollments) == 1
    assert pending.devices == %{}
  end

  test "late redemption retains its own fixed 24-hour approval/retry window", c do
    {:ok, engine, _} = create(c)
    {:ok, pending, reply} = redeem(c, engine, c.config, 1_740_000)
    assert reply["expires_at"] == 88_140_000
    assert {:ok, ^pending, ^reply} = redeem(c, pending, c.config, 1_800_001)
    assert {:error, :expired} = redeem(c, pending, c.config, 88_140_000)
  end

  test "approval requires current intended owner, exact key/deadline and empty sender grants",
       c do
    {:ok, engine, _} = create(c)
    {:ok, pending, reply} = redeem(c, engine, c.config, 1)
    deadline = reply["expires_at"]
    assert {:error, :not_found} = Engine.pending_lookup(pending, @other, c.other_key, @code, 2)

    assert {:error, :not_found} =
             Engine.pending_approve(
               pending,
               @other,
               c.other_key,
               @code,
               c.device_key,
               deadline,
               [],
               2
             )

    assert {:error, :not_found} =
             Engine.pending_approve(
               pending,
               @person,
               c.owner_key,
               @code,
               c.owner_key,
               deadline,
               [],
               2
             )

    assert {:error, :not_found} =
             Engine.pending_approve(
               pending,
               @person,
               c.owner_key,
               @code,
               c.device_key,
               deadline + 1,
               [],
               2
             )

    assert {:error, :invalid_senders} =
             Engine.pending_approve(
               pending,
               @person,
               c.owner_key,
               @code,
               c.device_key,
               deadline,
               ["app:unrequested"],
               2
             )

    assert {:ok, approved, key} =
             Engine.pending_approve(
               pending,
               @person,
               c.owner_key,
               @code,
               c.device_key,
               deadline,
               [],
               2
             )

    assert key == c.device_key
    assert approved.devices[key].person == @person
    assert MapSet.size(Map.get(approved.sender_grants, {@person, key}, MapSet.new())) == 0
    assert {:ok, %{"state" => "approved"}} = Engine.pending_status(approved, c.secret_hash, 3)
  end

  test "exact owner decline is terminal and truthfully visible without enrolling a device", c do
    {:ok, engine, _} = create(c)
    {:ok, pending, reply} = redeem(c, engine, c.config, 1)
    deadline = reply["expires_at"]

    assert {:error, :not_found} =
             Engine.pending_decline(
               pending,
               @other,
               c.other_key,
               @code,
               c.device_key,
               deadline,
               2
             )

    assert {:ok, declined} =
             Engine.pending_decline(
               pending,
               @person,
               c.owner_key,
               @code,
               c.device_key,
               deadline,
               2
             )

    assert {:ok, %{"state" => "declined"}} = Engine.pending_status(declined, c.secret_hash, 3)
    assert {:ok, ^declined, %{"state" => "declined"}} = redeem(c, declined, c.config, 3)

    assert {:error, :declined} =
             Engine.pending_approve(
               declined,
               @person,
               c.owner_key,
               @code,
               c.device_key,
               deadline,
               [],
               3
             )

    assert declined.devices == %{}
    assert declined.sender_grants == %{}
  end

  test "concurrent same-binding replay and lost reply survive atomic SQLite restart", c do
    path = database_path()
    opts = [store_path: path, join_configuration: c.config]
    relay = start_supervised!({Relay, opts})
    {:ok, person} = Relay.operator_bootstrap(relay, c.owner)
    {:ok, invitation} = Relay.owner_invite_create(relay, person, c.owner_key)
    token = invitation["invite_token"]
    secret = Base.encode16(:crypto.strong_rand_bytes(32), case: :lower)

    tasks =
      for _ <- 1..4,
          do: Task.async(fn -> Relay.invited_enroll(relay, token, c.device, "app", secret) end)

    replies = Enum.map(tasks, &Task.await/1)
    assert Enum.uniq(replies) |> length() == 1
    [{:ok, reply} | _] = replies
    engine = :sys.get_state(relay).store.engine
    assert map_size(engine.pending_enrollments) == 1
    assert map_size(engine.join_invitations) == 1
    snapshot = :erlang.term_to_binary(engine)
    refute String.contains?(snapshot, token)
    refute String.contains?(snapshot, secret)
    refute String.contains?(snapshot, Base.decode16!(token, case: :lower))
    refute String.contains?(snapshot, Base.decode16!(secret, case: :lower))
    stop_supervised(Relay)
    restarted = start_supervised!({Relay, opts})
    assert {:ok, ^reply} = Relay.invited_enroll(restarted, token, c.device, "retry", secret)
    code = reply["approval_code"]

    assert {:ok, key} =
             Relay.owner_pending_approve(
               restarted,
               person,
               c.owner_key,
               code,
               c.device_key,
               reply["expires_at"],
               []
             )

    assert key == c.device_key
    stop_supervised(Relay)
    restarted = start_supervised!({Relay, opts})
    assert {:ok, %{"state" => "approved"}} = Relay.pending_status(restarted, secret)

    assert MapSet.size(
             Map.get(
               :sys.get_state(restarted).store.engine.sender_grants,
               {person, key},
               MapSet.new()
             )
           ) == 0
  end

  test "stale SQLite writer cannot reply success with another redemption", c do
    path = database_path()
    {:ok, relay} = Relay.start_link(store_path: path, join_configuration: c.config)
    {:ok, person} = Relay.operator_bootstrap(relay, c.owner)
    {:ok, invitation} = Relay.owner_invite_create(relay, person, c.owner_key)
    {:ok, stale} = Relay.start_link(store_path: path, join_configuration: c.config)

    on_exit(fn ->
      if Process.alive?(relay), do: GenServer.stop(relay)
      if Process.alive?(stale), do: GenServer.stop(stale)
    end)

    token = invitation["invite_token"]
    secret = String.duplicate("a", 64)
    assert {:ok, _} = Relay.invited_enroll(relay, token, c.device, "first", secret)

    assert {:error, :storage_unavailable} =
             Relay.invited_enroll(stale, token, c.device, "stale", String.duplicate("b", 64))

    {:ok, stored} = Store.open(path)
    assert map_size(stored.engine.pending_enrollments) == 1
    assert map_size(stored.engine.join_invitations) == 1
    Store.close(stored)
  end

  test "existing owner, sender and manual request survive migration without gaining invite authority",
       c do
    path = database_path()
    {:ok, relay} = Relay.start_link(store_path: path)
    {:ok, person} = Relay.operator_bootstrap(relay, c.owner)
    {:ok, pending} = Relay.pending_enroll(relay, c.device, "existing sender")

    {:ok, _} =
      Relay.owner_pending_approve(
        relay,
        person,
        c.owner_key,
        pending.approval_code,
        c.device_key,
        pending.expires_at,
        ["app:keep"]
      )

    {:ok, manual} =
      Relay.pending_enroll(relay, TLSFixtures.certificate_der!(c.certs, "unknown"), "manual")

    before = :sys.get_state(relay).store.engine
    GenServer.stop(relay)
    old = Map.delete(before, :join_invitations)
    payload = :erlang.term_to_binary({:attention_relay_state, 11, old}, [{:compressed, 6}])
    {:ok, db} = Exqlite.Sqlite3.open(path)

    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ? WHERE id = 1")

    :ok = Exqlite.Sqlite3.bind(stmt, [{:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}])
    :done = Exqlite.Sqlite3.step(db, stmt)
    :ok = Exqlite.Sqlite3.release(db, stmt)
    :ok = Exqlite.Sqlite3.close(db)
    {:ok, migrated} = Store.open(path)
    assert migrated.engine.owners == before.owners
    assert migrated.engine.devices == before.devices
    assert migrated.engine.sender_grants == before.sender_grants

    assert migrated.engine.pending_enrollments ==
             Map.new(before.pending_enrollments, fn {code, item} ->
               {code, Map.put_new(item, :joined, false)}
             end)

    assert migrated.engine.pending_history == before.pending_history
    assert migrated.engine.join_invitations == %{}

    assert {:ok, %{"state" => "pending"}} =
             Engine.pending_status(
               migrated.engine,
               :crypto.hash(:sha256, Base.decode16!(manual.status_secret, case: :lower)),
               System.system_time(:millisecond)
             )

    assert {:ok, _, _} =
             GuidedJoin.create(
               migrated.engine,
               person,
               c.owner_key,
               c.token_hash,
               c.config,
               System.system_time(:millisecond)
             )

    Store.close(migrated)
  end

  test "approved device label and enrollment time outlive receipt cleanup, retry and revocation",
       c do
    {:ok, engine, _} = create(c)
    {:ok, pending, reply} = redeem(c, engine, c.config, 1)

    {:ok, approved, key} =
      Engine.pending_approve(
        pending,
        @person,
        c.owner_key,
        @code,
        c.device_key,
        reply["expires_at"],
        [],
        123
      )

    assert approved.devices[key].label == "app"
    assert approved.devices[key].enrolled_at == 123

    assert {:ok, ^approved, ^key} =
             Engine.pending_approve(
               approved,
               @person,
               c.owner_key,
               @code,
               key,
               reply["expires_at"],
               [],
               456
             )

    {:ok, cleaned} =
      Engine.pending_enroll(
        approved,
        TLSFixtures.certificate_der!(c.certs, "unknown"),
        "another device",
        @other_code,
        :crypto.hash(:sha256, "fresh-status"),
        reply["status_expires_at"]
      )

    assert cleaned.pending_history[@code].joined

    assert {:ok, ^cleaned, %{"state" => "approved", "device_id" => ^key}} =
             Engine.pending_resume(
               cleaned,
               c.secret_hash,
               c.device,
               @other_code,
               reply["status_expires_at"]
             )

    assert cleaned.join_invitations == %{}
    {:ok, removed, _} = Engine.device_remove(cleaned, key)
    assert removed.devices[key].label == "app"
    assert removed.devices[key].enrolled_at == 123
    path = database_path()
    {:ok, store} = Store.open(path)

    {:ok, committed} =
      Store.commit(store, removed, %{person: @person, actor: c.owner_key, action: "metadata"})

    Store.close(committed)
    {:ok, reloaded} = Store.open(path)
    assert reloaded.engine.devices[key] == removed.devices[key]
    Store.close(reloaded)
  end

  test "a full retained-request set still renews the existing joined attempt and permits owner decline",
       c do
    {:ok, engine, _} = create(c)
    {:ok, pending, reply} = redeem(c, engine, c.config, 1)

    retained =
      Map.new(1..1_023, fn index ->
        code = "ZZ" <> Base.encode32(<<index::40>>, padding: false)

        {code,
         %{
           state: :expired,
           key: c.other_key,
           secret_hash: :crypto.hash(:sha256, <<index::32>>),
           expires_at: reply["expires_at"] + 1,
           status_expires_at: reply["expires_at"] + 86_400_001
         }}
      end)

    full = %{pending | pending_history: retained}
    assert map_size(full.pending_enrollments) + map_size(full.pending_history) == 1_024

    assert {:ok, renewed, %{"request_generation" => 2, "approval_code" => new_code}} =
             Engine.pending_resume(
               full,
               c.secret_hash,
               c.device,
               @other_code,
               reply["expires_at"]
             )

    assert map_size(renewed.pending_enrollments) + map_size(renewed.pending_history) == 1_024

    assert {:error, :capacity} =
             Engine.pending_enroll(
               renewed,
               TLSFixtures.certificate_der!(c.certs, "unknown"),
               "new device",
               "ABCDEFGH24",
               :crypto.hash(:sha256, "new request"),
               reply["expires_at"] + 1
             )

    assert {:ok, declined} =
             Engine.pending_decline(
               renewed,
               @person,
               c.owner_key,
               new_code,
               c.device_key,
               reply["expires_at"] + 86_400_000,
               reply["expires_at"] + 1
             )

    assert {:ok, ^declined, %{"state" => "declined"}} =
             Engine.pending_resume(
               declined,
               c.secret_hash,
               c.device,
               "ABCDEFGH24",
               reply["expires_at"] + 2
             )
  end

  test "legacy devices migrate to unknown metadata without inventing or changing authority", c do
    path = database_path()
    {:ok, engine} = Engine.setup_device(c.engine, @person, c.device_key)
    old_record = Map.drop(engine.devices[c.device_key], [:label, :enrolled_at])
    legacy = %{engine | devices: %{c.device_key => old_record}}
    {:ok, store} = Store.open(path)
    Store.close(store)
    payload = :erlang.term_to_binary({:attention_relay_state, 12, legacy}, [{:compressed, 6}])
    {:ok, db} = Exqlite.Sqlite3.open(path)

    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ? WHERE id = 1")

    :ok = Exqlite.Sqlite3.bind(stmt, [{:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}])
    :done = Exqlite.Sqlite3.step(db, stmt)
    :ok = Exqlite.Sqlite3.release(db, stmt)
    :ok = Exqlite.Sqlite3.close(db)
    {:ok, migrated} = Store.open(path)
    record = migrated.engine.devices[c.device_key]
    assert record.label == nil
    assert record.enrolled_at == nil
    assert Map.drop(record, [:label, :enrolled_at]) == old_record
    assert migrated.engine.owners == legacy.owners
    assert migrated.engine.sender_grants == legacy.sender_grants
    Store.close(migrated)
    {:ok, restarted} = Store.open(path)
    assert restarted.engine.devices[c.device_key] == record
    Store.close(restarted)
  end

  test "device label is bounded on submission and on durable snapshot acceptance", c do
    {:ok, engine, _} = create(c)
    label = String.duplicate("x", 256)

    {:ok, pending, reply} =
      GuidedJoin.redeem(engine, c.token_hash, c.device, label, c.secret_hash, @code, c.config, 1)

    {:ok, approved, key} =
      Engine.pending_approve(
        pending,
        @person,
        c.owner_key,
        @code,
        c.device_key,
        reply["expires_at"],
        [],
        2
      )

    assert approved.devices[key].label == label

    assert {:error, :invalid_request} =
             GuidedJoin.redeem(
               engine,
               c.token_hash,
               c.device,
               label <> "x",
               c.secret_hash,
               @code,
               c.config,
               1
             )

    path = database_path()
    {:ok, store} = Store.open(path)

    {:ok, store} =
      Store.commit(store, approved, %{
        person: @person,
        actor: c.owner_key,
        action: "bounded_label"
      })

    for bad <- [%{label: label <> "x"}, %{enrolled_at: -1}, %{enrolled_at: "invented"}] do
      record = Map.merge(approved.devices[key], bad)
      invalid = %{approved | devices: Map.put(approved.devices, key, record)}

      assert {:error, :storage_unavailable, ^store} =
               Store.commit(store, invalid, %{
                 person: @person,
                 actor: c.owner_key,
                 action: "invalid_metadata"
               })
    end

    Store.close(store)
  end

  test "requester cancellation is exact and whichever approval/cancellation commits first wins",
       c do
    {:ok, engine, _} = create(c)
    {:ok, pending, reply} = redeem(c, engine, c.config, 1)
    deadline = reply["expires_at"]

    for {secret, key, time} <- [
          {c.token_hash, c.device_key, deadline},
          {c.secret_hash, c.owner_key, deadline}
        ] do
      assert {:error, :not_found} = Engine.pending_cancel(pending, secret, @code, key, time, 2)
    end

    assert {:error, :stale_request, %{"approval_code" => @code, "expires_at" => ^deadline}} =
             Engine.pending_cancel(pending, c.secret_hash, @code, c.device_key, deadline + 1, 2)

    {:ok, cancelled, status} =
      Engine.pending_cancel(pending, c.secret_hash, @code, c.device_key, deadline, 2)

    assert status == %{"state" => "cancelled"}
    assert cancelled.devices == %{}
    assert cancelled.sender_grants == %{}

    assert {:ok, ^cancelled, ^status} =
             Engine.pending_cancel(cancelled, c.secret_hash, @code, c.device_key, deadline, 3)

    assert {:error, :cancelled} =
             Engine.pending_approve(
               cancelled,
               @person,
               c.owner_key,
               @code,
               c.device_key,
               deadline,
               [],
               3
             )

    assert {:ok, ^cancelled, %{"state" => "cancelled"}} = redeem(c, cancelled, c.config, 3)

    {:ok, approved, key} =
      Engine.pending_approve(pending, @person, c.owner_key, @code, c.device_key, deadline, [], 2)

    assert {:ok, ^approved, %{"state" => "approved"}} =
             Engine.pending_cancel(approved, c.secret_hash, @code, key, deadline, 3)

    assert {:ok, expired_cancelled, %{"state" => "cancelled"}} =
             Engine.pending_cancel(pending, c.secret_hash, @code, key, deadline, deadline)

    assert {:ok, ^expired_cancelled, %{"state" => "cancelled"}} =
             Engine.pending_resume(
               expired_cancelled,
               c.secret_hash,
               c.device,
               @other_code,
               deadline + 1
             )
  end

  test "fragment invitation carries the exact v1 payload and no authority in path or query", c do
    invite =
      Map.merge(c.config, %{
        "v" => 1,
        "person_id" => @person,
        "invite_token" => String.duplicate("a", 64),
        "expires_at" => 1_800_000,
        "person_label" => "display only",
        "inviter_label" => "display only"
      })

    link = GuidedJoin.link(invite)
    uri = URI.parse(link)
    assert uri.scheme == "harklight"
    assert uri.host == "join"
    assert uri.query == nil
    assert uri.path in [nil, ""]
    refute String.contains?(uri.fragment, "=")
    {:ok, payload} = Base.url_decode64(uri.fragment, padding: false)
    assert {:ok, ^invite} = AttentionServer.JSON.decode_line(payload <> "\n")
  end

  test "short recipient link resolves only the current unused person invitation", c do
    token = String.duplicate("a", 64)
    raw = Base.decode16!(token, case: :lower)
    hash = :crypto.hash(:sha256, raw)
    assert {:ok, engine, _} = GuidedJoin.create(c.engine, @person, c.owner_key, hash, c.config, 0)

    assert GuidedJoin.bridge_link("https://relay.test:18447", token) ==
             "https://relay.test:18447/join##{token}"

    assert {:ok, invitation} = GuidedJoin.resolve(engine, token, c.config, 1)
    assert invitation["person_id"] == @person
    assert invitation["invite_token"] == token
    assert invitation["join_url"] == GuidedJoin.link(Map.delete(invitation, "join_url"))
    assert engine.sender_grants == %{}

    assert {:error, :unavailable} =
             GuidedJoin.resolve(engine, String.duplicate("b", 64), c.config, 1)

    assert {:error, :unavailable} =
             GuidedJoin.resolve(engine, token, %{c.config | "server_name" => "wrong.test"}, 1)

    {:ok, recovered} = Engine.operator_recover(engine, @person, c.device_key)
    assert {:error, :unavailable} = GuidedJoin.resolve(recovered, token, c.config, 1)
    assert {:error, :expired} = GuidedJoin.resolve(engine, token, c.config, 1_800_000)
    observed = GuidedJoin.observe_time(engine, hash, 1_800_000)
    assert {:error, :unavailable} = GuidedJoin.resolve(observed, token, c.config, 1)

    assert {:ok, consumed, _} =
             GuidedJoin.redeem(engine, hash, c.device, "phone", c.secret_hash, @code, c.config, 1)

    assert {:error, :used} = GuidedJoin.resolve(consumed, token, c.config, 2)
  end

  defp create(c), do: GuidedJoin.create(c.engine, @person, c.owner_key, c.token_hash, c.config, 0)

  defp redeem(c, engine, config, now),
    do:
      GuidedJoin.redeem(engine, c.token_hash, c.device, "app", c.secret_hash, @code, config, now)

  defp database_path do
    dir = Path.join(System.tmp_dir!(), "guided-join-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    Path.join(dir, "relay.sqlite3")
  end
end
