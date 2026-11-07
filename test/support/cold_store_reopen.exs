# Keep persisted Conversation field names as strings until after Store.open.
# The helper runs in a separate OS process so an already loaded parent module
# cannot make the safe ETF decode pass accidentally.
field_names = ["gone", "last_detached_at"]

atom_available? = fn name ->
  try do
    String.to_existing_atom(name)
    true
  rescue
    ArgumentError -> false
  end
end

{:ok, _started} = Application.ensure_all_started(:crypto)

if System.pid() == System.fetch_env!("COLD_PARENT_OS_PID") do
  raise "cold reopen did not start a separate OS process"
end

if Enum.any?(field_names, atom_available?) do
  raise "persisted Conversation field atoms were present before cold Store.open"
end

store_path = System.fetch_env!("COLD_STORE_PATH")
person = System.fetch_env!("COLD_PERSON")
origin = System.fetch_env!("COLD_ORIGIN")
handle = System.fetch_env!("COLD_HANDLE")
{:ok, store} = AttentionServer.Store.open(store_path)

support_record =
  try do
    if not Enum.all?(field_names, atom_available?) do
      raise "cold Store.open did not load persisted Conversation field atoms"
    end

    session = Map.fetch!(store.engine.origin_sessions, {person, origin, handle})

    gone_key = String.to_existing_atom("gone")
    detached_key = String.to_existing_atom("last_detached_at")
    support_key = String.to_existing_atom("support")

    false = Map.fetch!(session, gone_key)
    nil = Map.fetch!(session, detached_key)
    support_record = Map.fetch!(session, support_key)
    "claude-code" = support_record["route"]

    IO.puts("COLD_STORE_OPEN_OK")
    support_record
  after
    :ok = AttentionServer.Store.close(store)
  end

{:ok, relay} = AttentionServer.Relay.start_link(store_path: store_path)

try do
  {:ok, status} = AttentionServer.Relay.origin_status(relay, origin, handle)
  ^support_record = status["listening_support"]

  support = %{
    "type" => "support",
    "op_id" => System.fetch_env!("COLD_SUPPORT_OP"),
    "session_handle" => handle,
    "route" => "claude-code",
    "cli_version" => "test-only",
    "unsupported_reason" => nil
  }

  {:ok, %{"duplicate" => true, "listening_support" => ^support_record}} =
    AttentionServer.Relay.origin_support(relay, origin, support)

  IO.puts("COLD_STORE_REOPEN_AND_USE_OK")
after
  GenServer.stop(relay)
end
