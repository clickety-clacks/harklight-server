defmodule AttentionServer.OperatorToolTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{ChangeLog, DeviceIdentity, OperatorTool, Relay, TLSFixtures}

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

  @tag :tmp_dir
  test "an unavailable store is returned without exiting the operator process", %{
    certs: certs,
    tmp_dir: dir
  } do
    cert = Path.join(certs, "a1.pem")
    person = "00000000-0000-4000-8000-000000000001"
    file = Path.join(dir, "regular-file")
    File.write!(file, "not a directory")

    for store <- [dir, Path.join(file, "relay.sqlite3")] do
      assert {:error, :store_open_failed} = OperatorTool.bootstrap(store, cert)
      assert {:error, :store_open_failed} = OperatorTool.recover(store, person, cert)
    end
  end

  @tag :tmp_dir
  test "the development operator task raises a Mix error on store-open failure", %{
    certs: certs,
    tmp_dir: dir
  } do
    cert = Path.join(certs, "a1.pem")
    options = ["--store", dir, "--owner-cert", cert]
    person = "00000000-0000-4000-8000-000000000001"

    message =
      "Cannot open the local Harklight store. Check the --store path and permissions, then retry."

    assert_raise Mix.Error, message, fn ->
      Mix.Tasks.Attention.Operator.run(["bootstrap" | options])
    end

    assert_raise Mix.Error, message, fn ->
      Mix.Tasks.Attention.Operator.run(["recover", "--person", person | options])
    end
  end

  # The tool's Relay writes the change log like the Server's, so an owner
  # recovery run while the Server is stopped does not expire clients' cursors.
  @tag :tmp_dir
  test "a recovery by the local tool is in the change log and keeps client cursors", %{
    certs: certs,
    tmp_dir: dir
  } do
    path = Path.join(dir, "relay.sqlite3")
    assert {:ok, person} = OperatorTool.bootstrap(path, Path.join(certs, "a1.pem"))
    cursor = head(path, person)
    assert :ok = OperatorTool.recover(path, person, Path.join(certs, "a2.pem"))

    {:ok, relay} = Relay.start_link(store_path: path)
    store = :sys.get_state(relay).store
    assert {:ok, _changes, false, position} = ChangeLog.read(store, person, cursor, 500)
    assert position >= cursor
    GenServer.stop(relay)
  end

  defp head(path, person) do
    {:ok, relay} = Relay.start_link(store_path: path)
    assert {:ok, head} = ChangeLog.head(:sys.get_state(relay).store, person)
    GenServer.stop(relay)
    head
  end
end
