defmodule AttentionServer.PendingEnrollmentTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DeviceIdentity, Engine, Relay, TLSFixtures}

  @person_a "00000000-0000-4000-8000-000000000001"
  @person_b "00000000-0000-4000-8000-000000000002"
  @code_a "ABCDEFGH23"
  @code_b "JKLMNPQR45"

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  test "pending identity is immutable; secret and owner are separate authorities", %{
    certs: certs
  } do
    owner_a = TLSFixtures.certificate_der!(certs, "a1")
    owner_b = TLSFixtures.certificate_der!(certs, "b1")
    device = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, owner_a_key} = DeviceIdentity.from_certificate(owner_a)
    {:ok, owner_b_key} = DeviceIdentity.from_certificate(owner_b)
    {:ok, device_key} = DeviceIdentity.from_certificate(device)
    secret = :crypto.hash(:sha256, "secret-A")
    other_secret = :crypto.hash(:sha256, "secret-B")

    engine = Engine.new()
    assert {:ok, engine} = Engine.operator_bootstrap(engine, @person_a, owner_a_key)
    assert {:ok, engine} = Engine.operator_bootstrap(engine, @person_b, owner_b_key)

    assert {:ok, engine} = Engine.pending_enroll(engine, device, "A second device", @code_a, secret)

    assert {:error, :key_pending} =
             Engine.pending_enroll(engine, device, "again", @code_b, other_secret)

    assert {:ok, %{"state" => "pending"}} = Engine.pending_status(engine, secret)
    assert {:error, :not_found} = Engine.pending_status(engine, other_secret)

    assert {:error, :not_found} = Engine.pending_lookup(engine, @person_b, owner_a_key, @code_a)

    assert {:ok, %{label: "A second device", key: ^device_key, expires_at: nil}} =
             Engine.pending_lookup(engine, @person_a, owner_a_key, @code_a)

    assert {:error, :forbidden} = engine_approve(engine, @person_b, owner_a_key, @code_a, [], 1)

    assert {:ok, approved, ^device_key} =
             engine_approve(engine, @person_a, owner_a_key, @code_a, ["app:test"], 2)

    assert approved.devices[device_key].person == @person_a
    assert MapSet.member?(approved.sender_grants[{@person_a, device_key}], "app:test")

    assert {:ok, %{"state" => "approved", "person_id" => @person_a, "device_id" => ^device_key}} =
             Engine.pending_status(approved, secret)

    assert {:ok, ^approved, ^device_key} =
             engine_approve(approved, @person_a, owner_a_key, @code_a, [], 3)

    assert {:error, :not_found} = engine_approve(approved, @person_b, owner_b_key, @code_a, [], 3)

    assert {:error, :key_in_use} =
             Engine.pending_enroll(
               approved,
               device,
               "reuse",
               "WXYZ234567",
               :crypto.hash(:sha256, "C")
             )

    assert {:error, :key_in_use} = Engine.operator_recover(approved, @person_b, device_key)
    assert {:error, :key_in_use} = Engine.operator_recover(approved, @person_a, owner_b_key)
  end

  test "a waiting request never expires and approves with no deadline binding", %{certs: certs} do
    owner = TLSFixtures.certificate_der!(certs, "a1")
    device = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner)
    {:ok, device_key} = DeviceIdentity.from_certificate(device)
    secret = :crypto.hash(:sha256, "secret-A")

    {:ok, engine} = Engine.operator_bootstrap(Engine.new(), @person_a, owner_key)
    {:ok, engine} = Engine.pending_enroll(engine, device, "waiting", @code_a, secret)

    item = engine.pending_enrollments[@code_a]
    assert item.expires_at == nil and item.status_expires_at == nil
    assert {:ok, %{"state" => "pending"}} = Engine.pending_status(engine, secret)

    # Approval a very long time later, with the owner binding the null deadline.
    far_future = System.system_time(:millisecond) + 10 * 365 * 86_400_000

    assert {:ok, approved, ^device_key} =
             Engine.pending_approve(
               engine,
               @person_a,
               owner_key,
               @code_a,
               device_key,
               nil,
               [],
               far_future
             )

    assert approved.devices[device_key].person == @person_a
  end

  test "starting again with the same key returns the same request and code with a new status secret",
       %{certs: certs} do
    owner = TLSFixtures.certificate_der!(certs, "a1")
    device = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner)
    first_secret = :crypto.hash(:sha256, "secret-A")
    second_secret = :crypto.hash(:sha256, "secret-B")

    {:ok, engine} = Engine.operator_bootstrap(Engine.new(), @person_a, owner_key)
    {:ok, engine} = Engine.pending_enroll(engine, device, "first label", @code_a, first_secret)

    assert {:ok, restarted, @code_a} =
             Engine.pending_restart(engine, device, "second label", second_secret)

    assert map_size(restarted.pending_enrollments) == 1
    assert restarted.pending_enrollments[@code_a].label == "second label"
    assert {:ok, %{"state" => "pending"}} = Engine.pending_status(restarted, second_secret)
    assert {:error, :not_found} = Engine.pending_status(restarted, first_secret)

    other_device = TLSFixtures.certificate_der!(certs, "unknown")
    assert :none = Engine.pending_restart(restarted, other_device, "other", first_secret)

    # A secret already held by any request is never reused.
    assert :none = Engine.pending_restart(restarted, device, "third label", second_secret)

    # A request bound to a person by an invitation and an approved request
    # are not restarted.
    {:ok, key} = DeviceIdentity.from_certificate(device)
    joined = put_in(restarted.pending_enrollments[@code_a].person, @person_a)
    assert :none = Engine.pending_restart(joined, device, "joined", first_secret)

    approved =
      update_in(restarted.pending_enrollments[@code_a], &Map.put(&1, :state, :approved))

    assert key == approved.pending_enrollments[@code_a].key
    assert :none = Engine.pending_restart(approved, device, "approved", first_secret)
  end

  test "a repeated enroll start through the relay returns the same approval code", %{
    certs: certs
  } do
    owner = TLSFixtures.certificate_der!(certs, "a1")
    device = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, relay} = Relay.start_link([])
    assert {:ok, _person} = Relay.operator_bootstrap(relay, owner)

    assert {:ok, first} = Relay.pending_enroll(relay, device, "waiting")
    assert {:ok, second} = Relay.pending_enroll(relay, device, "waiting again")

    assert second.approval_code == first.approval_code
    assert second.status_secret != first.status_secret
    assert second.expires_at == nil
    assert {:ok, %{"state" => "pending"}} = Relay.pending_status(relay, second.status_secret)
    assert {:error, :not_found} = Relay.pending_status(relay, first.status_secret)
    GenServer.stop(relay)
  end

  test "pending status and approval survive SQLite restart without exposing code as status authority",
       %{
         certs: certs
       } do
    dir = Path.join(System.tmp_dir!(), "attention-pending-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")
    owner = TLSFixtures.certificate_der!(certs, "a1")
    device = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner)
    {:ok, device_key} = DeviceIdentity.from_certificate(device)
    {:ok, relay} = Relay.start_link(store_path: path)
    assert {:ok, person} = Relay.operator_bootstrap(relay, owner)
    assert {:ok, request} = Relay.pending_enroll(relay, device, "waiting")
    assert byte_size(request.approval_code) == 10
    assert Regex.match?(~r/\A[A-Z2-7]{10}\z/, request.approval_code)
    assert byte_size(request.status_secret) == 64
    assert {:ok, %{"state" => "pending"}} = Relay.pending_status(relay, request.status_secret)
    assert {:error, :not_found} = Relay.pending_status(relay, request.approval_code)
    assert {:error, :not_found} = Relay.pending_status(relay, nil)
    assert Process.alive?(relay)
    GenServer.stop(relay)

    {:ok, restarted} = Relay.start_link(store_path: path)

    assert {:ok, %{label: "waiting", key: ^device_key}} =
             Relay.owner_pending_lookup(restarted, person, owner_key, request.approval_code)

    assert {:ok, ^device_key} =
             relay_approve(restarted, person, owner_key, request.approval_code, [])

    assert {:ok, %{"state" => "approved", "person_id" => ^person, "device_id" => ^device_key}} =
             Relay.pending_status(restarted, request.status_secret)

    assert {:ok, ^device_key} =
             relay_approve(restarted, person, owner_key, request.approval_code, [])

    GenServer.stop(restarted)
    {:ok, after_restart} = Relay.start_link(store_path: path)

    assert {:ok, %{"state" => "approved"}} =
             Relay.pending_status(after_restart, request.status_secret)

    assert {:ok, ^device_key} =
             relay_approve(
               after_restart,
               person,
               owner_key,
               request.approval_code,
               []
             )

    GenServer.stop(after_restart)
  end

  test "owner mutations are person-scoped, durable, and fenced by recovery and device revocation",
       %{
         certs: certs
       } do
    dir =
      Path.join(
        System.tmp_dir!(),
        "attention-owner-actions-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")

    owner = TLSFixtures.certificate_der!(certs, "a1")
    replacement = TLSFixtures.certificate_der!(certs, "unknown")
    other_owner = TLSFixtures.certificate_der!(certs, "b1")
    device = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner)
    {:ok, replacement_key} = DeviceIdentity.from_certificate(replacement)
    {:ok, other_key} = DeviceIdentity.from_certificate(other_owner)
    {:ok, device_key} = DeviceIdentity.from_certificate(device)
    grant_id = "00000000-0000-4000-8000-000000000099"
    target = %{"kind" => "device", "id" => device_key, "key_fingerprint" => device_key}

    {:ok, relay} = Relay.start_link(store_path: path)
    assert {:ok, person} = Relay.operator_bootstrap(relay, owner)
    assert {:ok, other_person} = Relay.operator_bootstrap(relay, other_owner)
    assert {:ok, request} = Relay.pending_enroll(relay, device, "device for A")

    assert {:error, :not_found} =
             Relay.owner_pending_lookup(
               relay,
               other_person,
               other_key,
               request.approval_code <> "X"
             )

    assert {:error, :forbidden} =
             relay_approve(relay, person, other_key, request.approval_code, [])

    assert {:ok, ^device_key} =
             relay_approve(relay, person, owner_key, request.approval_code, [])

    assert {:error, :not_found} =
             Relay.owner_grant_sender(relay, other_person, other_key, device_key, "app:test")

    assert :ok = Relay.owner_grant_sender(relay, person, owner_key, device_key, "app:test")
    assert :ok = Relay.owner_grant_sender(relay, person, owner_key, device_key, "app:test")

    assert {:error, :not_found} =
             Relay.owner_grant_enrollment_read(
               relay,
               other_person,
               other_key,
               device_key,
               grant_id,
               "source-A",
               target
             )

    assert :ok =
             Relay.owner_grant_enrollment_read(
               relay,
               person,
               owner_key,
               device_key,
               grant_id,
               "source-A",
               target
             )

    assert :ok = Relay.operator_recover(relay, person, replacement)

    assert {:error, :forbidden} =
             Relay.owner_revoke_sender(relay, person, owner_key, device_key, "app:test")

    assert {:error, :forbidden} =
             Relay.owner_revoke_device(relay, person, owner_key, device_key)

    assert :ok = Relay.owner_revoke_sender(relay, person, replacement_key, device_key, "app:test")

    assert {:error, :not_found} =
             Relay.owner_revoke_enrollment_read(relay, other_person, other_key, grant_id)

    assert :ok = Relay.owner_revoke_enrollment_read(relay, person, replacement_key, grant_id)
    assert :ok = Relay.owner_revoke_device(relay, person, replacement_key, device_key)
    assert {:error, :forbidden} = Relay.acknowledged_cursor(relay, device_key)

    assert {:error, :not_found} =
             Relay.owner_grant_sender(relay, person, replacement_key, device_key, "app:test")

    GenServer.stop(relay)
    {:ok, restarted} = Relay.start_link(store_path: path)
    engine = :sys.get_state(restarted).store.engine
    assert engine.owners[person].key == replacement_key
    assert engine.devices[device_key].revoked
    refute MapSet.member?(engine.sender_grants[{person, device_key}], "app:test")
    assert engine.enrollment_read_grants[grant_id].revoked
    assert {:error, :forbidden} = Relay.owner_revision(restarted, person, owner_key)
    assert {:ok, 2} = Relay.owner_revision(restarted, person, replacement_key)
    GenServer.stop(restarted)
  end

  defp engine_approve(engine, person, owner, code, senders, now_ms) do
    item = Map.get(engine.pending_enrollments, code) || Map.get(engine.pending_history, code)

    Engine.pending_approve(
      engine,
      person,
      owner,
      code,
      item.key,
      item.expires_at,
      senders,
      now_ms
    )
  end

  defp relay_approve(relay, person, owner, code, senders) do
    engine = :sys.get_state(relay).store.engine
    item = Map.get(engine.pending_enrollments, code) || Map.get(engine.pending_history, code)
    Relay.owner_pending_approve(relay, person, owner, code, item.key, item.expires_at, senders)
  end
end
