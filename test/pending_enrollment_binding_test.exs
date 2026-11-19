defmodule AttentionServer.PendingEnrollmentBindingTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DeviceIdentity, Engine, Store, TLSFixtures}

  @person "00000000-0000-4000-8000-000000000001"
  @code "ABCDEFGH23"

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  test "approval binds the exact device key and revoked devices cannot be re-approved", %{
    certs: certs
  } do
    owner = TLSFixtures.certificate_der!(certs, "a1")
    device = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner)
    {:ok, device_key} = DeviceIdentity.from_certificate(device)
    secret = :crypto.hash(:sha256, "status-A")

    {:ok, engine} = Engine.operator_bootstrap(Engine.new(), @person, owner_key)
    {:ok, pending} = Engine.pending_enroll(engine, device, "laptop", @code, secret)

    assert {:error, :not_found} =
             Engine.pending_approve(
               pending,
               @person,
               owner_key,
               @code,
               String.duplicate("f", 64),
               nil,
               [],
               1
             )

    assert {:error, :not_found} =
             Engine.pending_approve(pending, @person, owner_key, @code, device_key, 12_345, [], 1)

    assert {:error, :invalid_expected_binding} =
             Engine.pending_approve(pending, @person, owner_key, @code, nil, nil, [], 1)

    assert {:ok, approved, ^device_key} =
             Engine.pending_approve(
               pending,
               @person,
               owner_key,
               @code,
               device_key,
               nil,
               ["app:test"],
               1
             )

    assert {:ok, %{"state" => "approved", "device_id" => ^device_key}} =
             Engine.pending_status(approved, secret)

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
               nil,
               ["app:other"],
               2
             )

    assert {:ok, %{"state" => "revoked"}} = Engine.pending_status(approved, secret)
    assert approved.devices[device_key].revoked
    refute MapSet.member?(approved.sender_grants[{@person, device_key}], "app:test")
    refute MapSet.member?(approved.sender_grants[{@person, device_key}], "app:other")
  end

  test "waiting requests and approved receipts without deadlines persist across restart", %{
    certs: certs
  } do
    owner = TLSFixtures.certificate_der!(certs, "a1")
    waiting_device = TLSFixtures.certificate_der!(certs, "a2")
    approved_device = TLSFixtures.certificate_der!(certs, "b1")
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner)
    {:ok, approved_key} = DeviceIdentity.from_certificate(approved_device)
    waiting_secret = :crypto.hash(:sha256, "waiting")
    approved_secret = :crypto.hash(:sha256, "approved")

    {:ok, engine} = Engine.operator_bootstrap(Engine.new(), @person, owner_key)
    {:ok, engine} = Engine.pending_enroll(engine, waiting_device, "waiting", @code, waiting_secret)

    {:ok, engine} =
      Engine.pending_enroll(engine, approved_device, "approved", "JKLMNPQR45", approved_secret)

    {:ok, engine, ^approved_key} =
      Engine.pending_approve(engine, @person, owner_key, "JKLMNPQR45", approved_key, nil, [], 1)

    path = temp_path()
    {:ok, store} = Store.open(path)

    assert {:ok, stored} =
             Store.commit(store, engine, %{
               person: "unassigned",
               actor: "test",
               action: "pending_enrollment"
             })

    assert :ok = Store.close(stored)
    assert {:ok, reopened} = Store.open(path)

    assert {:ok, %{"state" => "pending"}} = Engine.pending_status(reopened.engine, waiting_secret)

    assert {:ok, %{"state" => "approved", "device_id" => ^approved_key}} =
             Engine.pending_status(reopened.engine, approved_secret)

    assert :ok = Store.close(reopened)
  end

  test "a request stored with the earlier deadline fields still loads and no longer expires", %{
    certs: certs
  } do
    owner = TLSFixtures.certificate_der!(certs, "a1")
    device = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, owner_key} = DeviceIdentity.from_certificate(owner)
    {:ok, device_key} = DeviceIdentity.from_certificate(device)
    secret = :crypto.hash(:sha256, "legacy")

    {:ok, engine} = Engine.operator_bootstrap(Engine.new(), @person, owner_key)
    {:ok, engine} = Engine.pending_enroll(engine, device, "legacy", @code, secret)

    legacy_item = %{
      engine.pending_enrollments[@code]
      | expires_at: 1_000,
        status_expires_at: 1_000 + 86_400_000
    }

    engine = %{engine | pending_enrollments: %{@code => legacy_item}}
    path = temp_path()
    {:ok, store} = Store.open(path)

    assert {:ok, stored} =
             Store.commit(store, engine, %{
               person: "unassigned",
               actor: "test",
               action: "pending_enrollment"
             })

    assert :ok = Store.close(stored)
    assert {:ok, reopened} = Store.open(path)
    assert {:ok, %{"state" => "pending"}} = Engine.pending_status(reopened.engine, secret)

    # The stored deadline is only part of the exact binding the owner confirms.
    assert {:ok, _approved, ^device_key} =
             Engine.pending_approve(
               reopened.engine,
               @person,
               owner_key,
               @code,
               device_key,
               1_000,
               [],
               System.system_time(:millisecond)
             )

    assert :ok = Store.close(reopened)
  end

  defp temp_path do
    dir =
      Path.join(System.tmp_dir!(), "attention-pending-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    Path.join(dir, "relay.sqlite3")
  end
end
