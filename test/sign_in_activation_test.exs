defmodule AttentionServer.SignInActivationTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Engine, Relay, Store}
  alias AttentionServer.Store.AuthSchema
  alias Exqlite.Sqlite3

  @moduletag :tmp_dir
  @person "00000000-0000-4000-8000-000000000001"
  @owner String.duplicate("a", 64)
  @overlap String.duplicate("b", 64)
  @other String.duplicate("c", 64)
  @origin "https://local.example.test"

  test "enabling S1 retires local grants and seeds only the designated devices", %{tmp_dir: dir} do
    path = Path.join(dir, "relay.sqlite3")
    {:ok, store} = Store.open_relay(path)
    store = Store.track_relay_writes(store)
    {:ok, engine} = Engine.operator_bootstrap(store.engine, @person, @owner)
    {:ok, engine} = Engine.setup_device(engine, @person, @overlap)
    {:ok, engine} = Engine.setup_device(engine, @person, @other)
    {:ok, store} = Store.commit(store, engine, %{person: @person, actor: @owner, action: "fixture"})
    :ok = Store.close(store)

    options = [store_path: path, dashboard_origin: @origin]
    {:ok, relay} = Relay.start_link(options)
    assert {:ok, %{"sign_in_token" => old_grant}} = Relay.owner_browser_sign_in_create(relay, @person, @owner)
    assert {:ok, @person} = Relay.certificate_device_person(relay, @other)
    :ok = GenServer.stop(relay)

    failed = options ++ [sign_in_s1: true, auth_migration_devices: ["unknown"]]
    assert {:error, _} = GenServer.start(Relay, failed)

    {:ok, store} = Store.open_relay(path)
    assert {:ok, nil} = AuthSchema.read(store.db)
    assert map_size(store.engine.browser_sign_ins) == 1
    :ok = Store.close(store)

    enabled = options ++ [sign_in_s1: true, auth_migration_devices: [@overlap]]
    {:ok, relay} = Relay.start_link(enabled)
    assert {:ok, ["owner_code"]} = Relay.native_sign_in_methods(relay)
    assert {:ok, @person} = Relay.certificate_device_person(relay, @overlap)
    assert {:error, :unauthorized} = Relay.certificate_device_person(relay, @other)
    assert {:error, :invalid_sign_in} = Relay.consume_browser_sign_in(relay, old_grant)
    :ok = GenServer.stop(relay)

    {:ok, store} = Store.open_relay(path)
    assert {:ok, %{minimum_generation: 1}} = AuthSchema.read(store.db)
    assert store.engine.browser_sign_ins == %{}
    :ok = Store.close(store)

    {:ok, restarted} = Relay.start_link(options)
    assert {:ok, ["owner_code"]} = Relay.native_sign_in_methods(restarted)
    assert {:error, :unauthorized} = Relay.certificate_device_person(restarted, @other)
    refute Relay.legacy_device_enrollment?(restarted)
    assert {:ok, @person} = Relay.removal_identity(restarted, @overlap)
    :ok = GenServer.stop(restarted)

    {:ok, db} = Sqlite3.open(path)
    assert :ok = Sqlite3.execute(db, "UPDATE harklight_auth_schema SET device_certificates_disabled = 1")
    :ok = Sqlite3.close(db)
    {:ok, denied} = Relay.start_link(options)
    assert {:error, :unauthorized} = Relay.certificate_device_person(denied, @overlap)
    assert {:error, :not_found} = Relay.removal_identity(denied, @overlap)
    assert {:error, :not_found} = Relay.device_remove(denied, @overlap)
    refute Relay.legacy_device_enrollment?(denied)
  end

  test "the in-place FORMAT31 conversion retains the auth generation and person receipts",
       %{tmp_dir: dir} do
    path = Path.join(dir, "converted.sqlite3")
    {:ok, store} = Store.open(path, :snapshot)
    {:ok, engine} = Engine.operator_bootstrap(store.engine, @person, @owner)
    {:ok, engine} = Engine.setup_device(engine, @person, @overlap)
    {:ok, store} = Store.commit(store, engine, %{person: @person, actor: @owner, action: "fixture"})
    receipt_store = %{store | change_log: true}
    idem = AttentionServer.ClientAPI.Idempotency
    assert :ok = idem.save(receipt_store, @person, "old-key", "digest", 200, %{"settings" => true})
    assert {:ok, %{minimum_generation: 1}} = AuthSchema.initialize(store.db, store.engine, [@overlap])
    assert :ok = Sqlite3.execute(store.db, "UPDATE harklight_auth_schema SET device_certificates_disabled = 1")
    :ok = Store.close(store)

    {:ok, converted} = Store.open(path, :keyed_cutover)
    assert converted.mode == :keyed
    assert converted.auth_schema == %{minimum_generation: 1, device_certificates_disabled: true}
    assert {:ok, %{digest: "digest", status: 200}} = idem.lookup_retained(converted.db, @person, "old-key")
    assert {:ok, @person, "legacy"} = AttentionServer.DeviceMigration.phase(converted.db, @overlap)
    :ok = Store.close(converted)
  end

end
