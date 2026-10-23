defmodule AttentionServer.PendingEnrollmentDeadlinesTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DeviceIdentity, Engine, Store, TLSFixtures}

  @person "00000000-0000-4000-8000-000000000001"
  @code "ABCDEFGH23"
  @approval_deadline 86_400_000
  @status_deadline 172_800_000

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  test "approval, historical status and exact expected binding obey both immutable deadlines", %{
    certs: certs
  } do
    owner = TLSFixtures.certificate_der!(certs, "a1")
    device = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner)
    {:ok, device_key} = DeviceIdentity.from_certificate(device)
    secret = :crypto.hash(:sha256, "status-A")

    {:ok, engine} = Engine.operator_bootstrap(Engine.new(), @person, owner_key)
    {:ok, pending} = Engine.pending_enroll(engine, device, "laptop", @code, secret, 0)
    assert pending.pending_enrollments[@code].expires_at == @approval_deadline
    assert pending.pending_enrollments[@code].status_expires_at == @status_deadline

    assert {:ok, %{"state" => "pending"}} =
             Engine.pending_status(pending, secret, @approval_deadline - 1)

    assert {:ok, %{"state" => "expired"}} =
             Engine.pending_status(pending, secret, @approval_deadline)

    assert {:ok, %{"state" => "expired"}} =
             Engine.pending_status(pending, secret, @status_deadline - 1)

    assert {:error, :not_found} = Engine.pending_status(pending, secret, @status_deadline)

    assert {:error, :not_found} =
             Engine.pending_approve(
               pending,
               @person,
               owner_key,
               @code,
               String.duplicate("f", 64),
               @approval_deadline,
               [],
               599_999
             )

    assert {:error, :not_found} =
             Engine.pending_approve(
               pending,
               @person,
               owner_key,
               @code,
               device_key,
               @approval_deadline + 1,
               [],
               599_999
             )

    assert {:error, :invalid_expected_binding} =
             Engine.pending_approve(pending, @person, owner_key, @code, nil, nil, [], 599_999)

    assert {:error, :expired} =
             Engine.pending_approve(
               pending,
               @person,
               owner_key,
               @code,
               device_key,
               @approval_deadline,
               [],
               @approval_deadline
             )

    assert {:ok, approved, ^device_key} =
             Engine.pending_approve(
               pending,
               @person,
               owner_key,
               @code,
               device_key,
               @approval_deadline,
               ["app:test"],
               599_999
             )

    assert {:ok, %{"state" => "approved", "device_id" => ^device_key}} =
             Engine.pending_status(approved, secret, @status_deadline - 1)

    assert {:error, :not_found} = Engine.pending_status(approved, secret, @status_deadline)

    assert {:ok, approved} =
             Engine.owner_revoke_sender(approved, @person, owner_key, device_key, "app:test")

    assert {:ok, approved} = Engine.owner_revoke_device(approved, @person, owner_key, device_key)

    assert {:error, :revoked} =
             Engine.pending_approve(
               approved,
               @person,
               owner_key,
               @code,
               device_key,
               @approval_deadline,
               ["app:other"],
               @status_deadline - 1
             )

    assert approved.devices[device_key].revoked
    refute MapSet.member?(approved.sender_grants[{@person, device_key}], "app:test")
    refute MapSet.member?(approved.sender_grants[{@person, device_key}], "app:other")

    assert {:error, :not_found} =
             Engine.pending_approve(
               approved,
               @person,
               owner_key,
               @code,
               device_key,
               @approval_deadline,
               [],
               @status_deadline
             )
  end

  test "mixed retained capacity survives restart and first admission prunes without erasing authority",
       %{certs: certs} do
    old = TLSFixtures.certificate_der!(certs, "a2")
    fresh = TLSFixtures.certificate_der!(certs, "b1")
    owner = TLSFixtures.certificate_der!(certs, "a1")
    {:ok, old_key} = DeviceIdentity.from_certificate(old)
    {:ok, fresh_key} = DeviceIdentity.from_certificate(fresh)
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner)
    {:ok, engine} = Engine.operator_bootstrap(Engine.new(), @person, owner_key)
    {:ok, engine} = Engine.setup_device(engine, @person, old_key)

    history =
      Map.new(0..1_023, fn index ->
        {code(index),
         %{
           state: :expired,
           secret_hash: :crypto.hash(:sha256, <<index::32>>),
           key: old_key,
           expires_at: @approval_deadline,
           status_expires_at: @status_deadline
         }}
      end)

    full = %{engine | pending_history: history}
    path = temp_path("capacity")
    {:ok, store} = Store.open(path)

    assert {:ok, stored} =
             Store.commit(store, full, %{
               person: "unassigned",
               actor: "test",
               action: "retained_capacity"
             })

    assert :ok = Store.close(stored)
    assert {:ok, restarted} = Store.open(path)

    assert {:error, :capacity} =
             Engine.pending_enroll(
               restarted.engine,
               fresh,
               "new",
               "ZZZZZZZZZZ",
               :crypto.hash(:sha256, "new"),
               @status_deadline - 1
             )

    assert {:ok, %{"state" => "expired"}} =
             Engine.pending_status(
               restarted.engine,
               history[code(0)].secret_hash,
               @status_deadline - 1
             )

    assert {:ok, revoked} =
             Engine.owner_revoke_device(restarted.engine, @person, owner_key, old_key)

    assert revoked.devices[old_key].revoked

    assert {:ok, next} =
             Engine.pending_enroll(
               revoked,
               fresh,
               "new",
               code(0),
               :crypto.hash(:sha256, "new"),
               @status_deadline
             )

    assert map_size(next.pending_history) == 0
    assert map_size(next.pending_enrollments) == 1
    assert next.pending_enrollments[code(0)].key == fresh_key
    assert next.devices[old_key].revoked

    assert {:error, :clock_unavailable} =
             Engine.pending_enroll(
               next,
               old,
               "rolled back clock",
               code(0),
               :crypto.hash(:sha256, "rolled-back"),
               0
             )

    assert {:error, :not_found} =
             Engine.pending_status(next, history[code(0)].secret_hash, @status_deadline)

    assert {:error, :not_found} =
             Engine.pending_approve(
               next,
               @person,
               owner_key,
               code(0),
               old_key,
               @approval_deadline,
               [],
               @status_deadline + 1
             )

    assert {:ok, saved} =
             Store.commit(restarted, next, %{
               person: "unassigned",
               actor: "pending_submitter",
               action: "pending_enrollment"
             })

    assert :ok = Store.close(saved)
    assert {:ok, reloaded} = Store.open(path)
    assert map_size(reloaded.engine.pending_enrollments) == 1
    assert map_size(reloaded.engine.pending_history) == 0

    assert {:error, :not_found} =
             Engine.pending_status(
               reloaded.engine,
               history[code(0)].secret_hash,
               @status_deadline + 1
             )

    assert :ok = Store.close(reloaded)
  end

  test "observed expiry and readback deadlines stay crossed after clock rollback and restart", %{
    certs: certs
  } do
    owner = TLSFixtures.certificate_der!(certs, "a1")
    device = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner)
    {:ok, device_key} = DeviceIdentity.from_certificate(device)
    secret = :crypto.hash(:sha256, "rollback-secret")
    {:ok, engine} = Engine.operator_bootstrap(Engine.new(), @person, owner_key)
    {:ok, pending} = Engine.pending_enroll(engine, device, "rollback", @code, secret, 0)

    path = temp_path("rollback")
    {:ok, store} = Store.open(path)

    assert {:ok, stored} =
             Store.commit(store, pending, %{
               person: "unassigned",
               actor: "pending_submitter",
               action: "pending_enrollment"
             })

    crossed = Engine.pending_observe_time(stored.engine, {:secret, secret}, @approval_deadline)
    assert crossed.pending_clock_high_water == @approval_deadline

    assert {:ok, stored} =
             Store.commit(stored, crossed, %{
               person: "system",
               actor: "pending_deadline",
               action: "pending_clock_fence"
             })

    assert :ok = Store.close(stored)
    assert {:ok, reopened} = Store.open(path)
    assert {:ok, %{"state" => "expired"}} = Engine.pending_status(reopened.engine, secret, 1)

    assert {:error, :expired} =
             Engine.pending_approve(
               reopened.engine,
               @person,
               owner_key,
               @code,
               device_key,
               @approval_deadline,
               [],
               1
             )

    crossed =
      Engine.pending_observe_time(reopened.engine, {:code, @code}, @status_deadline)

    assert {:ok, stored} =
             Store.commit(reopened, crossed, %{
               person: "system",
               actor: "pending_deadline",
               action: "pending_clock_fence"
             })

    assert :ok = Store.close(stored)
    assert {:ok, reopened} = Store.open(path)
    assert {:error, :not_found} = Engine.pending_status(reopened.engine, secret, 1)
    assert :ok = Store.close(reopened)
  end

  defp code(index), do: Base.encode32(<<index::40>>, padding: false) <> "AA"

  defp temp_path(label) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "attention-pending-#{label}-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    Path.join(dir, "relay.sqlite3")
  end
end
