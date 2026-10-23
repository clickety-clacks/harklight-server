defmodule AttentionServer.OperatorToolTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DeviceIdentity, OperatorTool, Relay, TLSFixtures}

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  test "local public-certificate provisioner creates and recovers durable owner without network bootstrap",
       %{certs: certs} do
    dir =
      Path.join(
        System.tmp_dir!(),
        "attention-operator-tool-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")
    first = Path.join(certs, "a1.pem")
    second = Path.join(certs, "a2.pem")
    {:ok, first_key} = DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "a1"))
    {:ok, second_key} = DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "a2"))

    assert {:ok, person} = OperatorTool.bootstrap(path, first)
    assert {:error, :key_in_use} = OperatorTool.bootstrap(path, first)
    assert :ok = OperatorTool.recover(path, person, second)
    assert {:error, :key_in_use} = OperatorTool.recover(path, person, first)

    {:ok, relay} = Relay.start_link(store_path: path)
    assert {:error, :forbidden} = Relay.owner_revision(relay, person, first_key)
    assert {:ok, 2} = Relay.owner_revision(relay, person, second_key)

    assert {:error, :not_found} =
             OperatorTool.recover(Path.join(dir, "separate.sqlite3"), person, first)

    GenServer.stop(relay)
  end
end
