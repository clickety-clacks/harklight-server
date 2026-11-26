defmodule AttentionServer.SignInActivationTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Engine, Relay, Store}
  alias AttentionServer.Store.AuthSchema

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
    assert {:error, _} = Relay.start_link(failed)

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
  end
end
