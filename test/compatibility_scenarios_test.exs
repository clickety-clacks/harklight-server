Code.require_file("../scripts/compatibility/scenarios.exs", __DIR__)

defmodule AttentionServer.CompatibilityScenariosTest do
  use ExUnit.Case, async: false
  alias AttentionServer.CompatibilityScenarios, as: Scenarios

  @tag timeout: 60_000
  test "portable API manifest survives restart, retries, and cursor retention" do
    root = Path.join(System.tmp_dir!(), "compatibility-#{System.unique_integer([:positive])}")
    fixture = Scenarios.fixture(root)
    on_exit(fn -> File.rm_rf!(root) end)
    {:ok, relay} = Scenarios.start(fixture)

    manifest =
      try do
        manifest = Scenarios.run(relay, fixture)
        assert :ok = Scenarios.verify(relay, fixture, manifest)

        corrupted =
          Map.update!(manifest, "observations", fn rows ->
            Enum.map(rows, fn row ->
              if is_map(row["body"]["item"]),
                do: put_in(row, ["body", "item", "message", "title"], "incorrect content"),
                else: row
            end)
          end)

        assert_raise RuntimeError, ~r/\.item\.message\.title/, fn ->
          Scenarios.verify(relay, fixture, corrupted)
        end

        manifest
      after
        GenServer.stop(relay)
      end

    # Serialize the transport manifest so no tuple/atom identity is assumed.
    encoded = AttentionServer.JSON.encode!(manifest)
    {:ok, manifest} = AttentionServer.JSON.decode_line(encoded <> "\n")
    {:ok, reopened} = Scenarios.start(fixture)

    try do
      assert :ok = Scenarios.verify(reopened, fixture, manifest)

      for {fault, message} <- [
            {:pagination, ~r/candidate paginated baseline differs/},
            {:header_resume, ~r/candidate Last-Event-ID resume differs/},
            {:send_retry_flag, ~r/send replay lost/},
            {:send_item, ~r/send retry response differs.*message.item_id/},
            {:receipt_entry, ~r/receipt retry response differs.*entry.entry_id/},
            {:receipt_item, ~r/receipt retry response differs.*entry.item_id/},
            {:receipt_content, ~r/receipt retry response differs.*entry.text/}
          ] do
        broken = Map.put(fixture, :response_filter, &fault_response(&1, fault))
        assert_raise RuntimeError, message, fn -> Scenarios.verify(reopened, broken, manifest) end
      end

      # Only a newer envelope position is permitted; nested resource fields stay exact.
      advanced = Map.put(fixture, :response_filter, &fault_response(&1, :envelope_revision))
      assert :ok = Scenarios.verify(reopened, advanced, manifest)
      assert :ok = Scenarios.verify(reopened, fixture, manifest)
      assert :ok = Scenarios.expire(reopened, fixture, manifest)
    after
      GenServer.stop(reopened)
      Agent.stop(fixture.clock)
    end
  end

  defp fault_response(conn, fault) do
    replay? = Plug.Conn.get_resp_header(conn, "idempotent-replayed") == ["true"]
    receipt? = conn.method == "POST" and String.ends_with?(conn.request_path, "/receipt")
    send? = conn.method == "POST" and conn.request_path == "/v2/agent/messages"

    cond do
      fault == :pagination and conn.request_path == "/v2/inbox" and
          URI.decode_query(conn.query_string)["limit"] == "1" ->
        change_body(conn, &Map.put(&1, "items", []))

      fault == :header_resume and Plug.Conn.get_req_header(conn, "last-event-id") != [] ->
        change_body(conn, &Map.put(&1, "changes", []))

      fault == :send_retry_flag and send? ->
        %{conn | resp_headers: List.keydelete(conn.resp_headers, "idempotent-replayed", 0)}

      fault == :send_item and send? ->
        change_body(conn, &put_in(&1, ["message", "item_id"], "wrong-item"))

      fault == :receipt_entry and receipt? ->
        change_body(conn, &put_in(&1, ["entry", "entry_id"], "wrong-entry"))

      fault == :receipt_item and receipt? ->
        change_body(conn, &put_in(&1, ["entry", "item_id"], "wrong-item"))

      fault == :receipt_content and receipt? ->
        change_body(conn, &put_in(&1, ["entry", "text"], "wrong-content"))

      fault == :envelope_revision and replay? ->
        change_body(conn, &Map.update!(&1, "rev", fn rev -> rev + 1 end))

      true ->
        conn
    end
  end

  defp change_body(conn, change) do
    {:ok, body} = AttentionServer.JSON.decode_line(conn.resp_body <> "\n")
    %{conn | resp_body: AttentionServer.JSON.encode!(change.(body))}
  end
end
