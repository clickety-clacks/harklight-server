defmodule AttentionServer.LevelsDeviceAccessTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{ChangeLog, DeviceIdentity, JSON, Relay, RelayReaders, TLSFixtures}
  alias Exqlite.Sqlite3

  @rows_a [
    %{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil}
  ]
  @rows_b [
    %{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil},
    %{"name" => "Follow up", "lower_bound" => 0.6, "bounce_after_days" => 1.5}
  ]
  @rows_c [
    %{"name" => "Later", "lower_bound" => 0, "bounce_after_days" => 3}
  ]
  @default_rows [
    %{"name" => "Low", "lower_bound" => 0, "bounce_after_days" => 7},
    %{"name" => "Normal", "lower_bound" => 0.3, "bounce_after_days" => 3},
    %{"name" => "High", "lower_bound" => 0.6, "bounce_after_days" => 1},
    %{"name" => "Urgent", "lower_bound" => 0.8, "bounce_after_days" => 0.5}
  ]

  setup do
    dir =
      Path.join(
        System.tmp_dir!(),
        "harklight-levels-device-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{path: Path.join(dir, "relay.sqlite3")}
  end

  defp op(n),
    do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp raw(frame), do: JSON.encode!(Map.put_new(frame, "v", 1)) <> "\n"

  defp start_relay(path) do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    Process.unlink(relay)
    on_exit(fn -> if Process.alive?(relay), do: GenServer.stop(relay) end)
    relay
  end

  # The level frames reach the Engine through the Relay's request line, as
  # client API v2 settings reads and writes do.
  defp device_request(relay, key, frame) do
    assert {:ok, line} = Relay.request_line(relay, key, raw(frame))
    assert {:ok, response} = JSON.decode_line(line)
    [response]
  end

  # The person's change-log rows after `after_rev`, read with the Relay paused
  # so its own writes stay off the shared SQLite connection.
  defp log(relay, person, after_rev) do
    :ok = :sys.suspend(relay)

    try do
      assert {:ok, rows, _more, head} =
               ChangeLog.read(:sys.get_state(relay).store, person, after_rev, 500)

      {rows, head}
    after
      :ok = :sys.resume(relay)
    end
  end

  defp head(relay, person), do: relay |> log(person, 0) |> elem(1)

  defp settings_rows(relay, person, after_rev) do
    relay |> log(person, after_rev) |> elem(0) |> Enum.filter(&(&1.kind == :settings))
  end

  defp get_levels(relay, key, ref) do
    assert [%{"type" => "ok", "ref" => ^ref, "levels" => levels}] =
             device_request(relay, key, %{"type" => "levels_get", "ref" => ref})

    levels
  end

  defp set_levels(relay, key, ref, operation, rows) do
    [reply] =
      device_request(relay, key, %{
        "type" => "levels_set",
        "ref" => ref,
        "op_id" => operation,
        "levels" => rows
      })

    reply
  end

  defp wire_rows(rows) do
    assert {:ok, %{"levels" => decoded}} =
             JSON.decode_line(JSON.encode!(%{"levels" => rows}) <> "\n")

    decoded
  end

  defp default_levels, do: wire_rows(@default_rows)

  test "device reads and whole-table saves are account-scoped, validated and recover receipts after restart",
       %{path: path} do
    relay = start_relay(path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "A", "a1")
    assert :ok = Relay.setup_device(relay, "A", "a2")
    assert :ok = Relay.setup_device(relay, "B", "b1")

    a1 = "a1"
    a2 = "a2"
    b1 = "b1"
    op_a = op(1)
    op_b = op(3)
    op_c = op(4)
    defaults = default_levels()

    assert defaults == get_levels(relay, a1, "get-default")
    assert defaults == get_levels(relay, b1, "get-b-default")
    assert :sys.get_state(relay).store.engine.bounce_levels == %{}

    assert %{
             "type" => "ok",
             "ref" => "save-a",
             "op_id" => ^op_a,
             "levels" => saved_a
           } = set_levels(relay, a1, "save-a", op_a, @rows_a)

    assert saved_a == wire_rows(@rows_a)
    assert get_levels(relay, a2, "get-a-from-second-device") == saved_a
    assert defaults == get_levels(relay, b1, "get-b-isolated")

    assert [
             %{
               "type" => "error",
               "ref" => "cross-account",
               "reason" => "invalid_field",
               "field" => "account_id"
             }
           ] =
             device_request(relay, a1, %{
               "type" => "levels_get",
               "ref" => "cross-account",
               "account_id" => "B"
             })

    invalid_tables = [
      [],
      [%{"name" => "Routine", "lower_bound" => 0}],
      [%{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil, "extra" => true}],
      [%{"name" => "  ", "lower_bound" => 0, "bounce_after_days" => nil}],
      [%{"name" => "Misconfigured", "lower_bound" => 0.2, "bounce_after_days" => nil}],
      [%{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => 0}],
      [%{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => -1}],
      [
        %{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil},
        %{"name" => "High", "lower_bound" => 1.1, "bounce_after_days" => 1}
      ],
      [
        %{"name" => "High", "lower_bound" => 0.2, "bounce_after_days" => 1},
        %{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil}
      ]
    ]

    for {invalid_levels, index} <- Enum.with_index(invalid_tables) do
      ref = "invalid-table-#{index}"

      assert %{
               "type" => "error",
               "ref" => ^ref,
               "reason" => "invalid_field",
               "field" => "levels"
             } = set_levels(relay, a1, ref, op(index + 100), invalid_levels)
    end

    assert [
             %{
               "type" => "error",
               "ref" => "save-cross-account",
               "reason" => "invalid_field",
               "field" => "account_id"
             }
           ] =
             device_request(relay, a1, %{
               "type" => "levels_set",
               "ref" => "save-cross-account",
               "op_id" => op(200),
               "account_id" => "B",
               "levels" => @rows_b
             })

    assert get_levels(relay, a1, "prior-table-intact") == saved_a

    assert %{"type" => "ok", "op_id" => ^op_b, "levels" => saved_b} =
             set_levels(relay, a2, "save-b", op_b, @rows_b)

    assert saved_b == wire_rows(@rows_b)
    config_b = :sys.get_state(relay).store.engine.bounce_levels["A"]
    assert get_levels(relay, a1, "latest-table") == saved_b
    assert defaults == get_levels(relay, b1, "other-account-still-default")

    GenServer.stop(relay)
    restarted = start_relay(path)
    restarted_a1 = "a1"
    restarted_b1 = "b1"

    assert %{
             "type" => "ok",
             "ref" => "retry-original",
             "op_id" => ^op_a,
             "duplicate" => true,
             "levels" => ^saved_a
           } = set_levels(restarted, restarted_a1, "retry-original", op_a, @rows_a)

    assert get_levels(restarted, restarted_a1, "retry-does-not-overwrite") == saved_b

    assert %{
             "type" => "error",
             "ref" => "reused-operation",
             "reason" => "operation_conflict"
           } = set_levels(restarted, restarted_a1, "reused-operation", op_a, @rows_b)

    assert %{"type" => "ok", "op_id" => ^op_c, "levels" => ^saved_b} =
             set_levels(restarted, restarted_a1, "same-table-new-operation", op_c, @rows_b)

    assert :sys.get_state(restarted).store.engine.bounce_levels["A"] == config_b
    assert defaults == get_levels(restarted, restarted_b1, "b-remains-default")
  end

  test "failed level commit leaves the table and operation receipt unchanged", %{path: path} do
    relay = start_relay(path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "a1")
    key = "a1"
    operation = op(300)
    db = :sys.get_state(relay).store.db

    assert :ok =
             Sqlite3.execute(
               db,
               "CREATE TRIGGER refuse_levels BEFORE UPDATE ON relay_state BEGIN SELECT RAISE(ABORT, 'blocked'); END"
             )

    assert %{
             "type" => "error",
             "ref" => "commit-fails",
             "reason" => "storage_unavailable"
           } = set_levels(relay, key, "commit-fails", operation, @rows_b)

    assert default_levels() == get_levels(relay, key, "failed-save-kept-default")

    assert :ok = Sqlite3.execute(db, "DROP TRIGGER refuse_levels")

    assert %{"type" => "ok", "op_id" => ^operation, "levels" => levels} =
             set_levels(relay, key, "retry-after-failure", operation, @rows_b)

    assert levels == wire_rows(@rows_b)
  end

  test "dashboard and device level changes write a settings row only for that account",
       %{path: path} do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    relay = start_relay(path)

    owner_a_cert = TLSFixtures.certificate_der!(certs, "a1")
    owner_b_cert = TLSFixtures.certificate_der!(certs, "b1")
    assert {:ok, person_a} = Relay.operator_bootstrap(relay, owner_a_cert)
    assert {:ok, person_b} = Relay.operator_bootstrap(relay, owner_b_cert)
    assert {:ok, owner_a} = DeviceIdentity.from_certificate(owner_a_cert)
    rows_a = @rows_a
    rows_b = @rows_b
    rows_c = @rows_c

    for {person, device} <- [{person_a, "a1"}, {person_a, "a2"}, {person_b, "b1"}] do
      assert :ok = Relay.setup_device(relay, person, device)
    end

    defaults = default_levels()
    assert defaults == get_levels(relay, "a1", "a1-initial")
    assert defaults == get_levels(relay, "a2", "a2-initial")
    assert defaults == get_levels(relay, "b1", "b1-initial")
    assert {:error, :forbidden} = Relay.dashboard_levels(relay, person_a, "a1")
    assert {:error, :forbidden} = Relay.save_dashboard_levels(relay, person_a, "a1", @rows_a)

    baseline = %{
      events: RelayReaders.events(relay, "a1", 0),
      ordinary_cursor: Relay.acknowledged_cursor(relay, "a1"),
      conversation_events: RelayReaders.conversation_events(relay, "a1", 0),
      conversation_cursor: RelayReaders.conversation_cursor(relay, "a1")
    }

    # Every device of the account reads its settings rows from one change log.
    changed = fn change ->
      a_head = head(relay, person_a)
      b_head = head(relay, person_b)
      change.()
      {settings_rows(relay, person_a, a_head), settings_rows(relay, person_b, b_head)}
    end

    assert {[%{resource_id: "settings"}], []} =
             changed.(fn ->
               assert {:ok, %{"levels" => ^rows_a}} =
                        Relay.save_dashboard_levels(relay, person_a, owner_a, rows_a)
             end)

    assert {[], []} =
             changed.(fn ->
               assert {:ok, %{"levels" => ^rows_a}} =
                        Relay.save_dashboard_levels(relay, person_a, owner_a, rows_a)
             end)

    assert {[%{resource_id: "settings"}], []} =
             changed.(fn ->
               assert %{"type" => "ok", "levels" => _} =
                        set_levels(relay, "a1", "device-change", op(10), rows_b)
             end)

    assert {[], []} =
             changed.(fn ->
               assert %{"type" => "ok", "op_id" => _, "levels" => _} =
                        set_levels(relay, "a1", "device-noop", op(11), rows_b)
             end)

    assert {[%{resource_id: "settings"}], []} =
             changed.(fn ->
               assert {:ok, %{"levels" => ^rows_c}} =
                        Relay.save_dashboard_levels(relay, person_a, owner_a, rows_c)
             end)

    assert get_levels(relay, "a2", "dashboard-snapshot") == wire_rows(rows_c)
    assert defaults == get_levels(relay, "b1", "b1-unchanged")

    after_changes = %{
      events: RelayReaders.events(relay, "a1", 0),
      ordinary_cursor: Relay.acknowledged_cursor(relay, "a1"),
      conversation_events: RelayReaders.conversation_events(relay, "a1", 0),
      conversation_cursor: RelayReaders.conversation_cursor(relay, "a1")
    }

    assert after_changes == baseline
  end

  test "revoked devices cannot keep reading or saving levels", %{path: path} do
    relay = start_relay(path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "a1")
    assert default_levels() == get_levels(relay, "a1", "before-revocation")

    assert :ok = Relay.revoke_device(relay, "a1")

    assert {:error, :forbidden} =
             Relay.request_line(relay, "a1", raw(%{"type" => "levels_get", "ref" => "direct"}))

    assert {:error, :forbidden} =
             Relay.request_line(
               relay,
               "a1",
               raw(%{
                 "type" => "levels_set",
                 "ref" => "direct-save",
                 "op_id" => op(400),
                 "levels" => @rows_a
               })
             )

    assert {:error, :forbidden} = Relay.device_person(relay, "a1")
  end
end
