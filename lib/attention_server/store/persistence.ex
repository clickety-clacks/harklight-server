defmodule AttentionServer.Store.Persistence do
  @moduledoc false

  alias AttentionServer.Repository.CommittedRead
  require CommittedRead

  @snapshot {__MODULE__, :committed}

  # Only Relay installs this scope. Standalone Store users retain their blocking
  # API. It is deliberately not a second reader of SQLite: these replies use the
  # last committed in-memory engine, never the worker's proposed state.
  def with_reads(engine, fun) do
    previous = Process.put(@snapshot, engine)

    try do
      fun.()
    after
      if previous, do: Process.put(@snapshot, previous), else: Process.delete(@snapshot)
    end
  end

  def committed(engine) do
    if Process.get(@snapshot), do: Process.put(@snapshot, engine)
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

      {:"$gen_call", from, request} when CommittedRead.supported?(request) ->
        GenServer.reply(from, CommittedRead.execute(Process.get(@snapshot), request))
        await(task)
    end
  end
end
