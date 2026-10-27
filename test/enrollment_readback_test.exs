defmodule AttentionServer.EnrollmentReadbackTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{JSON, Relay, Store}
  alias Exqlite.Sqlite3

  defp key(char), do: String.duplicate(char, 64)

  defp uuid(n),
    do: "00000000-0000-4000-8000-" <> String.pad_leading(Integer.to_string(n), 12, "0")

  setup do
    dir =
      Path.join(System.tmp_dir!(), "attention-enrollment-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")

    {:ok, relay} =
      Relay.start_link(store_path: path, enrollment_fixture_mode: true, agent_fixture_mode: true)

    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "A", key("a"))
    assert :ok = Relay.setup_device(relay, "A", key("b"))
    assert :ok = Relay.setup_device(relay, "A", key("c"))
    assert :ok = Relay.setup_device(relay, "B", key("d"))
    %{relay: relay, path: path}
  end

  defp target(kind, id, fingerprint),
    do: %{"kind" => kind, "id" => id, "key_fingerprint" => fingerprint}

  defp readback(relay, key, grant, source, target, extras \\ %{}) do
    request =
      Map.merge(
        %{
          "v" => 1,
          "type" => "enrollment_readback",
          "proof_version" => 1,
          "grant_id" => grant,
          "source_instance_id" => source,
          "target" => target
        },
        extras
      )

    assert {:ok, line} = Relay.request_line(relay, key, JSON.encode!(request) <> "\n")
    assert {:ok, reply} = JSON.decode_line(line)
    reply
  end

  defp wire(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, reply} = JSON.decode_line(line)
    reply
  end

  test "exact device grant observes current target without adding read or response authority", %{
    relay: relay,
    path: path
  } do
    a1 = key("a")
    a2 = key("b")
    a3 = key("c")
    b1 = key("d")
    exact = target("device", a2, a2)
    grant = uuid(1)

    assert %{"reason" => "not_found"} = readback(relay, a1, grant, "source-X", exact)

    assert :ok =
             Relay.grant_enrollment_read_fixture(
               relay,
               "A",
               a1,
               grant,
               "source-X",
               exact,
               "owner-A-grant"
             )

    before = :sys.get_state(relay).store.engine
    assert %{"proof" => proof} = readback(relay, a1, grant, "source-X", exact)
    assert :sys.get_state(relay).store.engine == before
    assert proof["proof_version"] == 1
    assert proof["relay_instance_id"] == before.relay_instance_id
    assert proof["person_id"] == "A"
    assert proof["connector_device_id"] == a1
    assert proof["grant_id"] == grant
    assert proof["source_instance_id"] == "source-X"
    assert proof["target"] == exact
    assert proof["state"] == "active"
    assert proof["enrollment_revision"] == 1
    assert {:ok, _, _} = DateTime.from_iso8601(proof["observed_at"])
    refute Map.has_key?(proof, "message")
    refute Map.has_key?(proof, "answer")

    for {caller, grant_id, source, requested} <- [
          {a3, grant, "source-X", exact},
          {b1, grant, "source-X", exact},
          {a1, uuid(2), "source-X", exact},
          {a1, grant, "source-Y", exact},
          {a1, grant, "source-X", target("device", a2, a3)},
          {a1, grant, "source-X", target("device", a3, a3)},
          {a1, grant, "source-X", target("device", b1, b1)},
          {a1, grant, "source-X", target("agent", uuid(3), key("e"))}
        ] do
      assert %{"type" => "error", "reason" => "not_found"} =
               readback(relay, caller, grant_id, source, requested)
    end

    assert %{"reason" => "unsupported_version"} =
             readback(relay, a1, grant, "source-X", exact, %{"proof_version" => 2})

    assert %{"reason" => "invalid_field"} =
             readback(relay, a1, grant, "source-X", exact, %{"person_id" => "B"})

    assert %{"reason" => "invalid_field"} =
             readback(relay, a1, grant, "source-X", Map.put(exact, "id", "wrong"))

    assert :ok = Relay.revoke_device(relay, a2)
    assert %{"proof" => revoked} = readback(relay, a1, grant, "source-X", exact)
    assert revoked["state"] == "revoked"
    assert revoked["enrollment_revision"] == 2

    GenServer.stop(relay)

    {:ok, restarted} =
      Relay.start_link(store_path: path, enrollment_fixture_mode: true, agent_fixture_mode: true)

    assert %{"proof" => retained} = readback(restarted, a1, grant, "source-X", exact)
    assert retained["state"] == "revoked" and retained["enrollment_revision"] == 2

    assert :ok =
             Relay.grant_enrollment_read_fixture(
               restarted,
               "A",
               a1,
               grant,
               "source-X",
               exact,
               "owner-A-grant"
             )

    assert :ok = Relay.revoke_enrollment_read_fixture(restarted, "A", grant)
    assert %{"reason" => "not_found"} = readback(restarted, a1, grant, "source-X", exact)

    assert {:error, :grant_conflict} =
             Relay.grant_enrollment_read_fixture(
               restarted,
               "A",
               a1,
               grant,
               "source-X",
               exact,
               "owner-A-grant"
             )

    assert {:error, :invalid_target} =
             Relay.grant_enrollment_read_fixture(
               restarted,
               "A",
               a1,
               uuid(4),
               "source-X",
               exact,
               "owner-A-grant"
             )

    GenServer.stop(restarted)
  end

  test "agent proof observes registration only and no agent device-channel access", %{
    relay: relay
  } do
    agent_id = uuid(20)
    agent_key = key("e")
    a1 = key("a")
    grant = uuid(21)
    agent_target = target("agent", agent_id, agent_key)

    assert :ok = Relay.setup_agent_fixture(relay, "A", agent_id, agent_key, ["app:test"])

    assert :ok =
             Relay.grant_enrollment_read_fixture(
               relay,
               "A",
               a1,
               grant,
               "source-X",
               agent_target,
               "owner-A-grant"
             )

    assert %{"proof" => proof} = readback(relay, a1, grant, "source-X", agent_target)
    assert proof["target"] == agent_target
    assert proof["state"] == "active" and proof["enrollment_revision"] == 1

    assert {:error, :forbidden} =
             Relay.request_line(
               relay,
               agent_key,
               JSON.encode!(%{"v" => 1, "type" => "enrollment_readback"}) <> "\n"
             )

    assert :ok = Relay.revoke_agent_fixture(relay, agent_key)
    assert %{"proof" => revoked} = readback(relay, a1, grant, "source-X", agent_target)
    assert revoked["state"] == "revoked" and revoked["enrollment_revision"] == 2
  end

  test "later enrollment revocation does not rewrite the historical G4 winner", %{relay: relay} do
    a1 = key("a")
    a2 = key("b")
    exact = target("device", a2, a2)
    grant = uuid(25)
    assert :ok = Relay.setup_sender(relay, a1, "app:test")

    assert %{"seq" => 1} =
             wire(relay, a1, %{
               "type" => "forward",
               "op_id" => uuid(26),
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "historical",
                 "created_at" => 1,
                 "title" => "source-approved-content"
               }
             })

    assert %{"seq" => 2} =
             wire(relay, a2, %{
               "type" => "answer",
               "op_id" => uuid(27),
               "sender_id" => "app:test",
               "message_id" => "historical",
               "answer" => "yes"
             })

    assert :ok =
             Relay.grant_enrollment_read_fixture(
               relay,
               "A",
               a1,
               grant,
               "source-X",
               exact,
               "owner-A-grant"
             )

    assert %{"proof" => %{"state" => "active"}} =
             readback(relay, a1, grant, "source-X", exact)

    assert :ok = Relay.revoke_device(relay, a2)

    assert %{"proof" => %{"state" => "revoked", "enrollment_revision" => 2}} =
             readback(relay, a1, grant, "source-X", exact)

    assert %{"receipt" => receipt} =
             wire(relay, a1, %{
               "type" => "answer_receipt",
               "receipt_version" => 1,
               "sender_id" => "app:test",
               "message_id" => "historical",
               "terminal_seq" => 2
             })

    assert receipt["actor"] == %{
             "kind" => "device",
             "id" => a2,
             "key_fingerprint" => a2
           }

    assert receipt["has_answer"] == true and receipt["answer"] == "yes"
    refute Map.has_key?(receipt, "source_mapping")

    assert {:ok, [delivery, lifecycle], 2} = Relay.events(relay, a1, 0)
    assert delivery["message_id"] == "historical"
    assert lifecycle["message_id"] == "historical"
    assert lifecycle["answer"] == "yes"
  end

  test "owner-management fixture is unavailable on a normal Relay", %{relay: _relay} do
    {:ok, normal} = Relay.start_link()
    assert :ok = Relay.setup_person(normal, "A")
    assert :ok = Relay.setup_device(normal, "A", key("a"))

    assert {:error, :not_supported} =
             Relay.grant_enrollment_read_fixture(
               normal,
               "A",
               key("a"),
               uuid(99),
               "source-X",
               target("device", key("a"), key("a")),
               "owner"
             )

    assert {:error, :not_supported} =
             Relay.revoke_enrollment_read_fixture(normal, "A", uuid(99))

    assert %{"reason" => "not_found"} =
             readback(
               normal,
               key("a"),
               uuid(99),
               "source-X",
               target("device", key("a"), key("a"))
             )

    GenServer.stop(normal)
  end

  test "legacy retained enrollment records get one durable attributable revision migration", %{
    relay: relay,
    path: path
  } do
    agent_id = uuid(30)
    agent_key = key("e")
    assert :ok = Relay.setup_agent_fixture(relay, "A", agent_id, agent_key, ["app:test"])
    assert :ok = Relay.revoke_agent_fixture(relay, agent_key)
    before = :sys.get_state(relay).store
    GenServer.stop(relay)

    old = %{
      before.engine
      | devices:
          Map.new(before.engine.devices, fn {key, record} ->
            {key, Map.delete(record, :enrollment_revision)}
          end),
        agents:
          Map.new(before.engine.agents, fn {key, record} ->
            {key, Map.delete(record, :enrollment_revision)}
          end)
    }

    payload = :erlang.term_to_binary({:attention_relay_state, 2, old}, [{:compressed, 6}])
    assert {:ok, db} = Sqlite3.open(path)

    assert {:ok, stmt} =
             Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ? WHERE id = 1")

    assert :ok = Sqlite3.bind(stmt, [{:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}])
    assert :done = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)

    assert {:ok, migrated} = Store.open(path)
    assert migrated.version == before.version + 1
    assert migrated.engine.devices[key("a")].enrollment_revision == 1
    assert migrated.engine.agents[agent_key].revoked == true
    assert migrated.engine.agents[agent_key].enrollment_revision == 1
    assert :ok = Store.close(migrated)

    assert {:ok, again} = Store.open(path)
    assert again.version == migrated.version
    assert :ok = Store.close(again)

    assert {:ok, db} = Sqlite3.open(path)

    assert {:ok, stmt} =
             Sqlite3.prepare(
               db,
               "SELECT actor_id, action FROM relay_audit ORDER BY id DESC LIMIT 1"
             )

    assert {:row, ["schema_migration", "enrollment_revision_init"]} = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)

    # A current-format snapshot must not silently repair a missing revision.
    malformed = :erlang.term_to_binary({:attention_relay_state, 3, old}, [{:compressed, 6}])
    assert {:ok, db} = Sqlite3.open(path)

    assert {:ok, stmt} =
             Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ? WHERE id = 1")

    assert :ok =
             Sqlite3.bind(stmt, [{:blob, malformed}, {:blob, :crypto.hash(:sha256, malformed)}])

    assert :done = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)
    assert {:error, {:storage_unavailable, {:error, :invalid_state}}} = Store.open(path)
  end
end
