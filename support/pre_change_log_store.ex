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

  defp commit(store, {:ok, engine}, audit) do
    {:ok, store} = Store.commit(store, engine, audit)
    store
  end

  defp setup(person, action), do: %{person: person, actor: "local_setup", action: action}

  defp person(store, key), do: get_in(store.engine.devices, [key, :person])
end
