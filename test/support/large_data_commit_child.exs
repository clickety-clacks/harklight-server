path = System.fetch_env!("WI2F2_STORE_PATH")

{:ok, relay} = AttentionServer.Relay.start_link(store_path: path)

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
