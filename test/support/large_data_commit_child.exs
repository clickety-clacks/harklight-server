path = System.fetch_env!("WI2F2_STORE_PATH")
ready_path = System.fetch_env!("WI2F2_READY_PATH")
go_path = System.fetch_env!("WI2F2_GO_PATH")

{:ok, relay} = AttentionServer.Relay.start_link(store_path: path)
File.write!(ready_path, "ready")

wait_for_go = fn wait ->
  if File.exists?(go_path) do
    :ok
  else
    Process.sleep(10)
    wait.(wait)
  end
end

:ok = wait_for_go.(wait_for_go)

request = %{
  "v" => 1,
  "type" => "forward",
  "op_id" => "00000000-0000-4000-8000-000000000777",
  "session_handle" => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  "message" => %{
    "sender_id" => "app:test",
    "message_id" => "crash-before-commit",
    "created_at" => 1,
    "title" => "Crash boundary",
    "body" => String.duplicate("x", 2 * 1024 * 1024 + 1)
  }
}

_ =
  AttentionServer.Relay.request_line(
    relay,
    "origin",
    AttentionServer.JSON.encode!(request) <> "\n"
  )

receive do
  :stop -> GenServer.stop(relay)
end
