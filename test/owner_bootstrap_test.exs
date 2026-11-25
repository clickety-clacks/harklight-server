defmodule AttentionServer.OwnerBootstrapTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DeviceIdentity, Message, Relay, TLSFixtures}

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup do
    dir = Path.join(System.tmp_dir!(), "attention-owner-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{path: Path.join(dir, "relay.sqlite3")}
  end

  test "operator bootstrap mints stable person and public owner identity; recovery fences old key",
       %{certs: certs, path: path} do
    {:ok, relay} = Relay.start_link(store_path: path)
    a1 = TLSFixtures.certificate_der!(certs, "a1")
    a2 = TLSFixtures.certificate_der!(certs, "a2")
    b1 = TLSFixtures.certificate_der!(certs, "b1")
    {:ok, a1_key} = DeviceIdentity.from_certificate(a1)
    {:ok, a2_key} = DeviceIdentity.from_certificate(a2)
    {:ok, b1_key} = DeviceIdentity.from_certificate(b1)

    assert {:ok, person} = Relay.operator_bootstrap(relay, a1)
    assert Message.uuid_v4?(person)
    assert {:ok, 1} = Relay.owner_revision(relay, person, a1_key)
    assert {:error, :forbidden} = Relay.acknowledged_cursor(relay, a1_key)
    assert {:error, :key_in_use} = Relay.setup_device_certificate(relay, person, a1)
    assert {:error, :key_in_use} = Relay.operator_bootstrap(relay, a1)

    assert :ok = Relay.operator_recover(relay, person, a2)
    assert {:error, :forbidden} = Relay.owner_revision(relay, person, a1_key)
    assert {:ok, 2} = Relay.owner_revision(relay, person, a2_key)
    assert {:error, :key_in_use} = Relay.setup_device_certificate(relay, person, a1)
    assert {:error, :key_in_use} = Relay.operator_bootstrap(relay, a1)
    assert {:error, :key_in_use} = Relay.operator_recover(relay, person, a1)

    assert {:ok, other} = Relay.operator_bootstrap(relay, b1)
    assert other != person
    assert {:ok, 1} = Relay.owner_revision(relay, other, b1_key)
    assert {:error, :key_in_use} = Relay.operator_recover(relay, other, a2)
    assert {:ok, 1} = Relay.owner_revision(relay, other, b1_key)
    assert {:error, :not_found} = Relay.operator_recover(relay, "unknown", a1)
    GenServer.stop(relay)

    {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, 2} = Relay.owner_revision(restarted, person, a2_key)
    assert {:error, :forbidden} = Relay.owner_revision(restarted, person, a1_key)
    assert {:ok, 1} = Relay.owner_revision(restarted, other, b1_key)
    assert {:error, :key_in_use} = Relay.operator_bootstrap(restarted, a1)
    assert Map.has_key?(:sys.get_state(restarted).store.engine.persons, person)
    GenServer.stop(restarted)
  end

end
