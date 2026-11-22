defmodule AttentionServer.PreChangeLogStore do
  @moduledoc """
  Writes a store the way a Server before 0.2.0 did: plain `Store` commits with
  the client change logs never enabled. Tests use it to seed data from before
  the cut-over, or commits that an older Server made after a newer one had
  run. It needs no switch in the Relay, which always enables the logs.
  """

  alias AttentionServer.{Engine, JSON, Store}

  @doc "Opens `path` without the change logs, runs `fun` on the store, and closes it."
  def write(path, fun) do
    {:ok, %Store{change_log: false} = store} = Store.open(path)
    %Store{change_log: false} = store = fun.(store)
    :ok = Store.close(store)
  end

  def operator_bootstrap(store, person, key),
    do:
      commit(
        store,
        Engine.operator_bootstrap(store.engine, person, key),
        %{person: person, actor: "local_operator", action: "owner_bootstrap"}
      )

  def setup_person(store, person),
    do: commit(store, Engine.setup_person(store.engine, person), setup(person, "person_setup"))

  def setup_device(store, person, key),
    do:
      commit(store, Engine.setup_device(store.engine, person, key), setup(person, "device_setup"))

  def setup_sender(store, key, pattern),
    do:
      commit(
        store,
        Engine.setup_sender(store.engine, key, pattern),
        setup(person(store, key), "sender_setup")
      )

  @doc """
  Applies one device request frame through the Engine and commits it, as the
  Relay does when the engine changed. Returns the decoded reply and the store.
  """
  def request(store, key, frame) do
    raw = JSON.encode!(Map.put(frame, "v", 1)) <> "\n"

    {next, {:ok, line}} =
      Engine.request_line(store.engine, key, raw, System.system_time(:millisecond))

    {:ok, reply} = JSON.decode_line(line)

    if next == store.engine do
      {reply, store}
    else
      audit = %{person: person(store, key), actor: key, action: frame["type"]}
      {:ok, store} = Store.commit(store, next, audit)
      {reply, store}
    end
  end

  @doc """
  Runs a device's paged startup through its checkpoint, as a Server before
  0.2.0 did for a 0.1 client: the snapshot of the device's inbox is stored and
  accepted, which moves the device cursor to the snapshot's high water.
  """
  def paging_checkpoint(store, key, startup_id) do
    {snapshot, counts} = paging_snapshot(store, key, startup_id)
    :ok = Store.finish_inbox_snapshot(store, snapshot, counts)
    person = person(store, key)

    {:ok, store, false} =
      Store.accept_inbox_checkpoint(
        store,
        store.engine,
        person,
        key,
        snapshot.snapshot_id,
        snapshot.high_water,
        [],
        %{person: person, actor: key, action: "inbox_checkpoint"}
      )

    store
  end

  @doc "Leaves a paged startup building, as a Server stopped mid-snapshot did."
  def paging_building(store, key, startup_id) do
    {_snapshot, _counts} = paging_snapshot(store, key, startup_id)
    store
  end

  @doc "Acknowledges a device's conversation feed up to `seq`, as a 0.1 session did."
  def acknowledge_conversation(store, key, seq) do
    {:ok, engine, _reply} = Engine.acknowledge_conversation(store.engine, key, seq, seq)
    audit = %{person: person(store, key), actor: key, action: "conversation_ack"}
    {:ok, store} = Store.commit(store, engine, audit)
    store
  end

  defp paging_snapshot(store, key, startup_id) do
    {:ok, items, receipts, operation_ids, high} = Engine.inbox_snapshot(store.engine, key, 0, [])

    {:ok, snapshot} =
      Store.begin_inbox_snapshot(store, %{
        person_id: person(store, key),
        device_id: key,
        startup_id: startup_id,
        snapshot_id: startup_id <> "-snapshot",
        since: 0,
        high_water: high,
        cursor_secret: :crypto.strong_rand_bytes(32)
      })

    for {kind, values} <- [items: items, receipts: receipts, operation_ids: operation_ids],
        {chunk, n} <- values |> Enum.chunk_every(20) |> Enum.with_index() do
      :ok = Store.save_inbox_snapshot_chunk(store, snapshot, kind, n * 20, chunk)
    end

    counts = %{
      items: length(items),
      receipts: length(receipts),
      operation_ids: length(operation_ids)
    }

    {snapshot, counts}
  end

  defp commit(store, {:ok, engine}, audit) do
    {:ok, store} = Store.commit(store, engine, audit)
    store
  end

  defp setup(person, action), do: %{person: person, actor: "local_setup", action: action}

  defp person(store, key), do: get_in(store.engine.devices, [key, :person])
end
