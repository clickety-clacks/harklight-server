defmodule AttentionServer.PendingRetentionTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{Relay, TLSFixtures}

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  test "approved compact receipt preserves secret readback and same-owner retry across restart",
       %{certs: certs} do
    path = temp_path("approved")
    {:ok, relay} = Relay.start_link(store_path: path)
    owner = TLSFixtures.certificate_der!(certs, "a1")
    device = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, person} = Relay.operator_bootstrap(relay, owner)
    {:ok, owner_key} = AttentionServer.DeviceIdentity.from_certificate(owner)
    assert {:ok, request} = Relay.pending_enroll(relay, device, "new device")

    assert {:ok, key} =
             relay_approve(relay, person, owner_key, request.approval_code, [])

    state = :sys.get_state(relay).store.engine
    refute Map.has_key?(state.pending_enrollments, request.approval_code)
    receipt = state.pending_history[request.approval_code]

    assert receipt == %{
             state: :approved,
             secret_hash:
               :crypto.hash(:sha256, Base.decode16!(request.status_secret, case: :lower)),
             person: person,
             key: key,
             label: "new device",
             expires_at: request.expires_at,
             status_expires_at: request.status_expires_at,
             approved_by: owner_key,
             request_generation: 1,
             joined: false
           }

    assert {:ok, %{"state" => "approved", "person_id" => ^person, "device_id" => ^key}} =
             Relay.pending_status(relay, request.status_secret)

    assert {:error, :not_found} =
             Relay.pending_resume(relay, request.status_secret, device)

    GenServer.stop(relay)
    {:ok, restarted} = Relay.start_link(store_path: path)
    on_exit(fn -> if Process.alive?(restarted), do: GenServer.stop(restarted) end)

    assert {:ok, ^key} =
             relay_approve(restarted, person, owner_key, request.approval_code, [])

    assert {:ok, %{"state" => "approved", "device_id" => ^key}} =
             Relay.pending_status(restarted, request.status_secret)

    replacement = TLSFixtures.certificate_der!(certs, "unknown")
    {:ok, replacement_key} = AttentionServer.DeviceIdentity.from_certificate(replacement)
    assert :ok = Relay.operator_recover(restarted, person, replacement)

    assert {:error, :forbidden} =
             relay_approve(restarted, person, owner_key, request.approval_code, [])

    assert {:error, :not_found} =
             relay_approve(restarted, person, replacement_key, request.approval_code, [])

    assert {:ok, %{"state" => "approved", "device_id" => ^key}} =
             Relay.pending_status(restarted, request.status_secret)
  end

  defp code(index), do: Base.encode32(<<index::40>>, padding: false) <> "AA"
  defp secret_hash(index), do: :crypto.hash(:sha256, <<index::32>>)

  defp temp_path(label) do
    dir = Path.join(System.tmp_dir!(), "attention-#{label}-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    Path.join(dir, "relay.sqlite3")
  end

  defp relay_approve(relay, person, owner, code, senders) do
    engine = :sys.get_state(relay).store.engine
    item = Map.get(engine.pending_enrollments, code) || Map.get(engine.pending_history, code)
    Relay.owner_pending_approve(relay, person, owner, code, item.key, item.expires_at, senders)
  end
end
