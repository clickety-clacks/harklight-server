defmodule AttentionServer.PersistenceReadBarrierTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Engine, Store}
  alias AttentionServer.Store.Persistence

  setup do
    root =
      Path.join(System.tmp_dir!(), "persistence-barrier-#{System.unique_integer([:positive])}")

    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    %{path: Path.join(root, "state.sqlite3")}
  end

  defp seed(path) do
    {:ok, store} = Store.open(path)
    {:ok, engine} = Engine.setup_person(store.engine, "person")
    {:ok, engine} = Engine.setup_device(engine, "person", "device")
    {:ok, store} = Store.commit(store, engine, audit())
    store
  end

  defp audit, do: %{person: "person", actor: "test", action: "barrier"}

  defp read(store, {:device_revoked?, key}, _from),
    do: Engine.device_revoked?(store.engine, key)

  test "reads see only committed state; ordered mutations and success wait for persistence", %{
    path: path
  } do
    parent = self()

    {owner, monitor} =
      spawn_monitor(fn ->
        store = seed(path)
        {:ok, next} = Engine.revoke_device(store.engine, "device")

        {:ok, committed} =
          Persistence.with_reads(store, &read/3, fn ->
            result =
              Persistence.run(fn ->
                send(parent, {:writing, self()})
                receive do: (:allow_commit -> Store.commit(store, next, audit()))
              end)

            {:ok, committed} = result
            Persistence.committed(committed)
            send(parent, :first_committed)

            # A second commit in the same Relay callback must read the first
            # committed generation, not the callback-entry snapshot.
            Persistence.run(fn ->
              send(parent, {:second_write, self()})
              receive do: (:finish -> result)
            end)
          end)

        receive do
          {:"$gen_call", from, {:setup_device, key}} ->
            {:ok, next} = Engine.setup_device(committed.engine, "person", key)
            {:ok, committed} = Store.commit(committed, next, audit())
            GenServer.reply(from, :ok)
            Store.close(committed)
        end
      end)

    assert_receive {:writing, writer}
    queued = Task.async(fn -> GenServer.call(owner, {:setup_device, "second"}) end)
    refute GenServer.call(owner, {:device_revoked?, "device"})
    refute_received :first_committed
    queued_ref = queued.ref
    refute_received {^queued_ref, _}
    send(writer, :allow_commit)
    assert_receive :first_committed
    assert_receive {:second_write, second_writer}
    assert GenServer.call(owner, {:device_revoked?, "device"})
    queued_ref = queued.ref
    refute_received {^queued_ref, _}
    send(second_writer, :finish)
    assert Task.await(queued) == :ok
    assert_receive {:DOWN, ^monitor, :process, ^owner, :normal}

    {:ok, reopened} = Store.open(path)
    assert Engine.device_revoked?(reopened.engine, "device")
    refute Engine.device_revoked?(reopened.engine, "second")
    Store.close(reopened)
  end

  test "worker death fails its owner without reporting persistence success", %{path: path} do
    parent = self()

    {owner, monitor} =
      spawn_monitor(fn ->
        store = seed(path)

        Persistence.with_reads(store, &read/3, fn ->
          Persistence.run(fn ->
            send(parent, {:writing, self()})
            receive do: (:fail -> exit(:persistence_failed))
          end)

          send(parent, :unexpected_success)
        end)
      end)

    assert_receive {:writing, writer}
    refute GenServer.call(owner, {:device_revoked?, "device"})
    send(writer, :fail)
    assert_receive {:DOWN, ^monitor, :process, ^owner, :persistence_failed}
    refute_received :unexpected_success
    {:ok, reopened} = Store.open(path)
    refute Engine.device_revoked?(reopened.engine, "device")
    Store.close(reopened)
  end
end
