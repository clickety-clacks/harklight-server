defmodule AttentionServer.LevelsDeviceAccessTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DeviceIdentity, JSON, Relay, TLSFixtures}

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

  defp session_request(relay, session, frame) do
    assert {:ok, lines} = Relay.session_request_line(relay, session, raw(frame))

    Enum.map(lines, fn line ->
      assert {:ok, response} = JSON.decode_line(line)
      response
    end)
  end

  defp session_drain(relay, session) do
    assert {:ok, lines} = Relay.session_drain(relay, session)

    Enum.map(lines, fn line ->
      assert {:ok, response} = JSON.decode_line(line)
      response
    end)
  end

  defp connected_session(relay, key) do
    assert {:ok, session} = Relay.open_session(relay, key)

    assert Enum.any?(session_request(relay, session, %{"type" => "resync", "since" => 0}), fn
             %{"type" => "resynced"} -> true
             _ -> false
           end)

    session
  end

  defp get_levels(relay, session, ref) do
    assert [%{"type" => "ok", "ref" => ^ref, "levels" => levels}] =
             session_request(relay, session, %{"type" => "levels_get", "ref" => ref})

    levels
  end

  defp set_levels(relay, session, ref, operation, rows) do
    [reply] =
      session_request(relay, session, %{
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

  test "device reads and whole-table saves are account-scoped, validated and recover receipts after restart",
       %{path: path} do
    relay = start_relay(path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "A", "a1")
    assert :ok = Relay.setup_device(relay, "A", "a2")
    assert :ok = Relay.setup_device(relay, "B", "b1")

    a1 = connected_session(relay, "a1")
    a2 = connected_session(relay, "a2")
    b1 = connected_session(relay, "b1")
    op_a = op(1)
    op_invalid = op(2)
    op_b = op(3)
    op_c = op(4)

    assert [] = get_levels(relay, a1, "get-default")
    assert [] = get_levels(relay, b1, "get-b-default")

    assert %{
             "type" => "ok",
             "ref" => "save-a",
             "op_id" => ^op_a,
             "levels" => saved_a
           } = set_levels(relay, a1, "save-a", op_a, @rows_a)

    assert saved_a == wire_rows(@rows_a)
    assert get_levels(relay, a2, "get-a-from-second-device") == saved_a
    assert [] = get_levels(relay, b1, "get-b-isolated")

    assert [
             %{
               "type" => "error",
               "ref" => "cross-account",
               "reason" => "invalid_field",
               "field" => "account_id"
             }
           ] =
             session_request(relay, a1, %{
               "type" => "levels_get",
               "ref" => "cross-account",
               "account_id" => "B"
             })

    assert [
             %{
               "type" => "error",
               "ref" => "invalid-table",
               "reason" => "invalid_field",
               "field" => "levels"
             }
           ] =
             session_request(relay, a1, %{
               "type" => "levels_set",
               "ref" => "invalid-table",
               "op_id" => op_invalid,
               "levels" => [
                 %{
                   "name" => "Misconfigured",
                   "lower_bound" => 0.2,
                   "bounce_after_days" => nil
                 }
               ]
             })

    assert get_levels(relay, a1, "prior-table-intact") == saved_a

    assert %{"type" => "ok", "op_id" => ^op_b, "levels" => saved_b} =
             set_levels(relay, a2, "save-b", op_b, @rows_b)

    assert saved_b == wire_rows(@rows_b)
    config_b = :sys.get_state(relay).store.engine.bounce_levels["A"]
    assert get_levels(relay, a1, "latest-table") == saved_b
    assert [] = get_levels(relay, b1, "other-account-still-default")

    GenServer.stop(relay)
    restarted = start_relay(path)
    restarted_a1 = connected_session(restarted, "a1")
    restarted_b1 = connected_session(restarted, "b1")

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
    assert [] = get_levels(restarted, restarted_b1, "b-remains-default")
  end

  test "dashboard and device changes send coalesced hints only to same-account sessions that used levels",
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

    for {person, device} <- [
          {person_a, "a1"},
          {person_a, "a2"},
          {person_a, "legacy"},
          {person_b, "b1"}
        ] do
      assert :ok = Relay.setup_device(relay, person, device)
    end

    a1 = connected_session(relay, "a1")
    a2 = connected_session(relay, "a2")
    legacy = connected_session(relay, "legacy")
    b1 = connected_session(relay, "b1")

    assert [] = get_levels(relay, a1, "a1-initial")
    assert [] = get_levels(relay, a2, "a2-initial")
    assert [] = get_levels(relay, b1, "b1-initial")
    assert {:error, :forbidden} = Relay.dashboard_levels(relay, person_a, "a1")
    assert {:error, :forbidden} = Relay.save_dashboard_levels(relay, person_a, "a1", @rows_a)

    for session <- [a1, a2, legacy, b1], do: session_drain(relay, session)

    baseline = %{
      events: Relay.events(relay, "a1", 0),
      ordinary_cursor: Relay.acknowledged_cursor(relay, "a1"),
      conversation_events: Relay.conversation_events(relay, "a1", 0),
      conversation_cursor: Relay.conversation_cursor(relay, "a1")
    }

    assert {:ok, %{"levels" => ^rows_a}} =
             Relay.save_dashboard_levels(relay, person_a, owner_a, rows_a)

    assert [%{"v" => 1, "type" => "levels_changed"}] = session_drain(relay, a1)
    assert [%{"v" => 1, "type" => "levels_changed"}] = session_drain(relay, a2)
    assert [] = session_drain(relay, legacy)
    assert [] = session_drain(relay, b1)

    assert {:ok, %{"levels" => ^rows_a}} =
             Relay.save_dashboard_levels(relay, person_a, owner_a, rows_a)

    assert [] = session_drain(relay, a1)
    assert [] = session_drain(relay, a2)

    assert %{"type" => "ok", "levels" => _} =
             set_levels(relay, a1, "device-change", op(10), rows_b)

    assert {:ok, %{"levels" => ^rows_c}} =
             Relay.save_dashboard_levels(relay, person_a, owner_a, rows_c)

    assert [%{"v" => 1, "type" => "levels_changed"}] = session_drain(relay, a1)
    assert [%{"v" => 1, "type" => "levels_changed"}] = session_drain(relay, a2)
    assert [] = session_drain(relay, legacy)
    assert [] = session_drain(relay, b1)
    assert [] = session_drain(relay, a1)
    assert [] = session_drain(relay, a2)
    assert get_levels(relay, a2, "dashboard-snapshot") == wire_rows(rows_c)

    after_changes = %{
      events: Relay.events(relay, "a1", 0),
      ordinary_cursor: Relay.acknowledged_cursor(relay, "a1"),
      conversation_events: Relay.conversation_events(relay, "a1", 0),
      conversation_cursor: Relay.conversation_cursor(relay, "a1")
    }

    assert after_changes == baseline
  end

  test "revoked devices cannot keep reading or saving levels on a prior connection", %{path: path} do
    relay = start_relay(path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "a1")
    session = connected_session(relay, "a1")
    assert [] = get_levels(relay, session, "before-revocation")

    assert :ok = Relay.revoke_device(relay, "a1")

    assert {:error, session_error} =
             Relay.session_request_line(
               relay,
               session,
               raw(%{"type" => "levels_get", "ref" => "after-revocation"})
             )

    assert session_error in [:unknown_session, :forbidden]

    assert {:error, :forbidden} =
             Relay.request_line(relay, "a1", raw(%{"type" => "levels_get", "ref" => "direct"}))

    assert {:error, :forbidden} = Relay.open_session(relay, "a1")
  end
end
