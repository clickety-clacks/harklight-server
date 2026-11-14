defmodule AttentionServer.Store.Persistence do
  @moduledoc false

  @snapshot {__MODULE__, :committed}

  # Only Relay installs this scope. Standalone Store users retain their blocking
  # API. It is deliberately not a second reader of SQLite: these replies use the
  # last committed in-memory engine, never the worker's proposed state.
  def with_reads(store, reader, fun) do
    previous = Process.put(@snapshot, {store, reader})

    try do
      fun.()
    after
      if previous, do: Process.put(@snapshot, previous), else: Process.delete(@snapshot)
    end
  end

  def committed(store) do
    case Process.get(@snapshot) do
      {_previous, reader} -> Process.put(@snapshot, {store, reader})
      nil -> :ok
    end

    :ok
  end

  def run(fun) do
    task = Task.async(fun)

    if Process.get(@snapshot) do
      await(task)
    else
      Task.await(task, :infinity)
    end
  end

  # Mutations, timers, session/socket changes and store-reading requests remain
  # in the mailbox in arrival order. Their callback must not run against the
  # pending generation. Pure engine reads can linearize before this commit.
  # The linked worker dying fails the owner rather than acknowledging an
  # uncertain commit; restart reloads SQLite using the normal recovery path.
  defp await(%Task{ref: ref, pid: pid} = task) do
    receive do
      {^ref, result} ->
        Process.demonitor(ref, [:flush])
        result

      {:DOWN, ^ref, :process, ^pid, reason} ->
        exit({reason, {__MODULE__, :await}})

      {:"$gen_call", from, request}
      when is_tuple(request) and
             elem(request, 0) in [
               :cursor,
               :conversation_cursor,
               :device_revoked?,
               :owner_identity,
               :agent_identity,
               :events,
               :conversation_events,
               :owner_revision,
               :origin_file_limits,
               :origin_message_get,
               :origin_advisory_feed,
               :origin_coverage,
               :owner_list_agents,
               :dashboard_inventory,
               :dashboard_levels
             ] ->
        {store, reader} = Process.get(@snapshot)
        GenServer.reply(from, reader.(store, request, from))
        await(task)
    end
  end
end
