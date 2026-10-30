defmodule AttentionServer.HeadlessToolTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    DeviceIdentity,
    EnrollmentHTTPS,
    HeadlessEnrollment,
    HeadlessOwner,
    HeadlessTLS,
    JSON,
    OperatorTool,
    OwnerTLS,
    Relay,
    Store,
    TLSFixtures,
    WebSocketRelay
  }

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  test "headless key stays local through HTTPS request, owner proof, status and first relay sync",
       %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "attention-headless-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    db_path = Path.join(dir, "relay.sqlite3")
    identity_dir = Path.join(dir, "new-device")
    assert {:ok, person} = OperatorTool.bootstrap(db_path, Path.join(certs, "a1.pem"))
    {:ok, relay} = Relay.start_link(store_path: db_path)

    {:ok, enrollment} =
      EnrollmentHTTPS.start_link(
        relay: relay,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key")
      )

    {:ok, owner} =
      OwnerTLS.start_link(
        relay: relay,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem")
      )

    {:ok, web} =
      WebSocketRelay.start_link(
        relay: relay,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem")
      )

    on_exit(fn ->
      for pid <- [web, owner, enrollment, relay], Process.alive?(pid), do: GenServer.stop(pid)
    end)

    enrollment_endpoint = endpoint(certs, EnrollmentHTTPS.port(enrollment))
    owner_endpoint = endpoint(certs, OwnerTLS.port(owner))
    relay_endpoint = endpoint(certs, WebSocketRelay.port(web))
    owner_cert = Path.join(certs, "a1.pem")
    owner_key = Path.join(certs, "a1.key")

    assert {:error, _} =
             AttentionServer.HeadlessTLS.post_json(
               %{enrollment_endpoint | name: "wrong.test"},
               "/enroll/status",
               %{"status_secret" => String.duplicate("0", 64)}
             )

    assert {:error, _} =
             AttentionServer.HeadlessTLS.post_json(
               %{enrollment_endpoint | ca_file: Path.join(certs, "untrusted-ca.pem")},
               "/enroll/status",
               %{"status_secret" => String.duplicate("0", 64)}
             )

    assert {:ok, %{approval_code: code}} =
             HeadlessEnrollment.start(identity_dir, enrollment_endpoint, "fresh laptop")

    assert {:ok, %{"state" => "pending"}} =
             HeadlessEnrollment.submit_existing(identity_dir, enrollment_endpoint, "fresh laptop")

    assert {:ok, server_stat} = File.stat(Path.join(identity_dir, "server.json"))
    assert {:ok, pending_stat} = File.stat(Path.join(identity_dir, "pending.json"))
    assert Bitwise.band(server_stat.mode, 0o777) == 0o600
    assert Bitwise.band(pending_stat.mode, 0o777) == 0o600

    pending_path = Path.join(identity_dir, "pending.json")
    backup_path = Path.join(identity_dir, "pending.saved")
    assert :ok = File.rename(pending_path, backup_path)

    assert {:error, :key_pending} =
             HeadlessEnrollment.submit_existing(identity_dir, enrollment_endpoint, "fresh laptop")

    error =
      assert_raise Mix.Error, fn ->
        Mix.Tasks.Attention.Enroll.run([
          "retry",
          "--directory",
          identity_dir,
          "--connect",
          "127.0.0.1",
          "--name",
          "relay.test",
          "--port",
          Integer.to_string(EnrollmentHTTPS.port(enrollment)),
          "--ca",
          Path.join(certs, "ca.pem"),
          "--label",
          "fresh laptop"
        ])
      end

    assert Exception.message(error) =~ "no old code or secret is recoverable"

    refute File.exists?(pending_path)
    assert :ok = File.rename(backup_path, pending_path)

    server_path = Path.join(identity_dir, "server.json")
    assert {:ok, saved_server} = server_path |> File.read!() |> JSON.decode_line()
    File.write!(server_path, JSON.encode!(Map.put(saved_server, "origin_port", 18_446)) <> "\n")
    server_with_metadata = File.read!(server_path)

    for mismatched <- [
          %{enrollment_endpoint | address: {127, 0, 0, 2}},
          %{enrollment_endpoint | name: "other.test"},
          %{enrollment_endpoint | port: enrollment_endpoint.port + 1},
          %{enrollment_endpoint | ca_file: Path.join(certs, "server.pem")}
        ] do
      assert {:error, :wrong_server} = HeadlessEnrollment.status(identity_dir, mismatched)
    end

    assert {:error, :existing_identity} =
             HeadlessEnrollment.start(identity_dir, enrollment_endpoint, "overwrite")

    assert {:ok, %{"state" => "pending"}} =
             HeadlessEnrollment.status(identity_dir, enrollment_endpoint)

    assert File.read!(server_path) == server_with_metadata

    assert {:ok, key_stat} = File.stat(Path.join(identity_dir, "identity.key"))
    assert {:ok, dir_stat} = File.stat(identity_dir)
    assert Bitwise.band(key_stat.mode, 0o777) == 0o600
    assert Bitwise.band(dir_stat.mode, 0o777) == 0o700
    assert :ok = HeadlessEnrollment.certificate_lifecycle(identity_dir)

    assert {:ok,
            %{"label" => "fresh laptop", "device_id" => device_key, "expires_at" => deadline}} =
             HeadlessOwner.lookup(owner_endpoint, owner_cert, owner_key, person, code)

    {:ok, certificate_pem} = File.read(Path.join(identity_dir, "identity.pem"))
    [{:Certificate, der, :not_encrypted}] = :public_key.pem_decode(certificate_pem)
    assert {:ok, ^device_key} = DeviceIdentity.from_certificate(der)

    assert :nomatch ==
             :binary.match(
               :erlang.term_to_binary(:sys.get_state(relay).store.engine),
               File.read!(Path.join(identity_dir, "identity.key"))
             )

    assert {:ok, %{"device_id" => ^device_key}} =
             HeadlessOwner.approve(
               owner_endpoint,
               owner_cert,
               owner_key,
               person,
               code,
               device_key,
               deadline,
               []
             )

    assert {:ok, %{"device_id" => ^device_key}} =
             HeadlessOwner.approve(
               owner_endpoint,
               owner_cert,
               owner_key,
               person,
               code,
               device_key,
               deadline,
               []
             )

    assert {:ok, %{"state" => "approved", "person_id" => ^person, "device_id" => ^device_key}} =
             HeadlessEnrollment.status(identity_dir, enrollment_endpoint)

    assert :ok = HeadlessEnrollment.verify_relay(identity_dir, relay_endpoint)

    assert {:ok, %{"state" => "approved"}} =
             HeadlessEnrollment.status(identity_dir, enrollment_endpoint)

    assert :ok = Relay.setup_sender(relay, device_key, "app:status")

    assert {:ok, answerer} =
             Relay.setup_device_certificate(
               relay,
               person,
               TLSFixtures.certificate_der!(certs, "a2")
             )

    forward = %{
      "v" => 1,
      "type" => "forward",
      "op_id" => "00000000-0000-4000-8000-000000000001",
      "message" => %{
        "sender_id" => "app:status",
        "message_id" => "completed-before-status",
        "created_at" => 1,
        "title" => "Check an old answer"
      }
    }

    assert {:ok, sent} = Relay.request_line(relay, device_key, JSON.encode!(forward) <> "\n")
    assert {:ok, %{"type" => "ok", "seq" => 1}} = JSON.decode_line(sent)

    answer = %{
      "v" => 1,
      "type" => "answer",
      "op_id" => "00000000-0000-4000-8000-000000000002",
      "sender_id" => "app:status",
      "message_id" => "completed-before-status",
      "answer" => nil
    }

    assert {:ok, answered} = Relay.request_line(relay, answerer, JSON.encode!(answer) <> "\n")
    assert {:ok, %{"type" => "ok", "seq" => 2}} = JSON.decode_line(answered)

    old_shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(old_shell) end)

    assert :ok =
             Mix.Tasks.Attention.Enroll.run([
               "status",
               "--directory",
               identity_dir,
               "--connect",
               "127.0.0.1",
               "--name",
               "relay.test",
               "--port",
               Integer.to_string(EnrollmentHTTPS.port(enrollment)),
               "--relay-port",
               Integer.to_string(WebSocketRelay.port(web)),
               "--ca",
               Path.join(certs, "ca.pem")
             ])

    assert_receive {:mix_shell, :info, [approved]}
    assert approved == "approved person=#{person} device=#{device_key}"

    {:ok, owner_fingerprint} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "a1"))

    assert :ok = Relay.owner_revoke_device(relay, person, owner_fingerprint, device_key)

    assert :ok =
             Mix.Tasks.Attention.Enroll.run([
               "retry",
               "--directory",
               identity_dir,
               "--connect",
               "127.0.0.1",
               "--name",
               "relay.test",
               "--port",
               Integer.to_string(EnrollmentHTTPS.port(enrollment)),
               "--ca",
               Path.join(certs, "ca.pem"),
               "--label",
               "fresh laptop"
             ])

    assert_receive {:mix_shell, :info, ["revoked"]}
  end

  test "failed private save hides a committed code and keeps the original key", %{certs: certs} do
    dir =
      Path.join(System.tmp_dir!(), "attention-save-failure-#{System.unique_integer([:positive])}")

    identity_dir = Path.join(dir, "device")
    File.mkdir_p!(identity_dir)

    on_exit(fn ->
      File.chmod(identity_dir, 0o700)
      File.rm_rf!(dir)
    end)

    {:ok, relay} = Relay.start_link(store_path: Path.join(dir, "relay.sqlite3"))

    {:ok, listener} =
      EnrollmentHTTPS.start_link(
        relay: relay,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key")
      )

    on_exit(fn ->
      if Process.alive?(listener), do: GenServer.stop(listener)
      if Process.alive?(relay), do: GenServer.stop(relay)
    end)

    endpoint = endpoint(certs, EnrollmentHTTPS.port(listener))
    File.cp!(Path.join(certs, "a2.pem"), Path.join(identity_dir, "identity.pem"))
    File.cp!(Path.join(certs, "a2.key"), Path.join(identity_dir, "identity.key"))
    File.chmod!(Path.join(identity_dir, "identity.key"), 0o600)

    server = %{
      "address" => "127.0.0.1",
      "name" => endpoint.name,
      "port" => endpoint.port,
      "ca_sha256" =>
        :crypto.hash(:sha256, File.read!(endpoint.ca_file)) |> Base.encode16(case: :lower)
    }

    File.write!(
      Path.join(identity_dir, "server.json"),
      AttentionServer.JSON.encode!(server) <> "\n"
    )

    File.chmod!(Path.join(identity_dir, "server.json"), 0o600)
    File.chmod!(identity_dir, 0o500)

    error =
      assert_raise Mix.Error, fn ->
        Mix.Tasks.Attention.Enroll.run([
          "retry",
          "--directory",
          identity_dir,
          "--connect",
          "127.0.0.1",
          "--name",
          endpoint.name,
          "--port",
          Integer.to_string(endpoint.port),
          "--ca",
          endpoint.ca_file,
          "--label",
          "save failure"
        ])
      end

    assert Exception.message(error) =~ "submission indeterminate"
    assert [{code, pending}] = Map.to_list(:sys.get_state(relay).store.engine.pending_enrollments)
    refute Exception.message(error) =~ code
    refute File.exists?(Path.join(identity_dir, "pending.json"))
    assert pending.label == "save failure"
    assert File.exists?(Path.join(identity_dir, "identity.key"))

    File.chmod!(identity_dir, 0o700)

    assert {:error, :key_pending} =
             HeadlessEnrollment.submit_existing(identity_dir, endpoint, "save failure")
  end

  test "retry archives an expired status handle and submits the same private key", %{certs: certs} do
    dir =
      Path.join(
        System.tmp_dir!(),
        "attention-expired-retry-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    identity_dir = Path.join(dir, "device")
    {:ok, relay} = Relay.start_link(store_path: Path.join(dir, "relay.sqlite3"))

    {:ok, listener} =
      EnrollmentHTTPS.start_link(
        relay: relay,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key")
      )

    on_exit(fn ->
      if Process.alive?(listener), do: GenServer.stop(listener)
      if Process.alive?(relay), do: GenServer.stop(relay)
    end)

    endpoint = endpoint(certs, EnrollmentHTTPS.port(listener))

    assert {:ok, %{approval_code: old_code}} =
             HeadlessEnrollment.start(identity_dir, endpoint, "retry")

    old_key = File.read!(Path.join(identity_dir, "identity.key"))
    old_pending = File.read!(Path.join(identity_dir, "pending.json"))
    {:ok, %{"status_secret" => old_secret}} = AttentionServer.JSON.decode_line(old_pending)
    state = :sys.get_state(relay)
    item = state.store.engine.pending_enrollments[old_code]
    expired_at = System.system_time(:millisecond) - 1

    engine = %{
      state.store.engine
      | pending_enrollments:
          Map.put(state.store.engine.pending_enrollments, old_code, %{
            item
            | expires_at: expired_at,
              status_expires_at: expired_at + 86_400_000
          })
    }

    assert {:ok, stored} =
             Store.commit(state.store, engine, %{
               person: "unassigned",
               actor: "test_fixture",
               action: "expire_saved_request"
             })

    :sys.replace_state(relay, fn current -> %{current | store: stored} end)
    old_shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(old_shell) end)

    assert :ok =
             Mix.Tasks.Attention.Enroll.run([
               "retry",
               "--directory",
               identity_dir,
               "--connect",
               "127.0.0.1",
               "--name",
               endpoint.name,
               "--port",
               Integer.to_string(endpoint.port),
               "--ca",
               endpoint.ca_file,
               "--label",
               "retry"
             ])

    assert_receive {:mix_shell, :info, ["approval code: " <> display]}
    assert File.read!(Path.join(identity_dir, "identity.key")) == old_key
    assert File.read!(Path.join(identity_dir, "pending-#{old_code}.json")) == old_pending

    {:ok, %{"approval_code" => new_code}} =
      AttentionServer.JSON.decode_line(File.read!(Path.join(identity_dir, "pending.json")))

    assert new_code != old_code
    assert display =~ new_code

    assert {:ok, 200, %{"state" => "expired"}} =
             HeadlessTLS.post_json(endpoint, "/enroll/status", %{"status_secret" => old_secret})

    assert {:ok, %{"state" => "pending"}} = HeadlessEnrollment.status(identity_dir, endpoint)
    assert map_size(:sys.get_state(relay).store.engine.pending_history) == 1
    assert map_size(:sys.get_state(relay).store.engine.pending_enrollments) == 1
  end

  test "owner approval prompt visibly escapes attacker label and keeps account and key legible",
       %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "attention-label-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, relay} = Relay.start_link(store_path: Path.join(dir, "relay.sqlite3"))

    assert {:ok, person} =
             Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))

    {:ok, enrollment} =
      EnrollmentHTTPS.start_link(
        relay: relay,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key")
      )

    {:ok, owner} =
      OwnerTLS.start_link(
        relay: relay,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem")
      )

    on_exit(fn ->
      for pid <- [owner, enrollment, relay], Process.alive?(pid), do: GenServer.stop(pid)
    end)

    label = "laptop\e[8m\r\e[2Kfake\nPublic-key fingerprint: deadbeef\\\u202E"

    assert {:ok, 200, %{"approval_code" => code}} =
             HeadlessTLS.post_json(
               endpoint(certs, EnrollmentHTTPS.port(enrollment)),
               "/enroll",
               %{
                 "public_certificate" => File.read!(Path.join(certs, "a2.pem")),
                 "label" => label
               }
             )

    owner_endpoint = endpoint(certs, OwnerTLS.port(owner))

    assert {:ok, %{"label" => ^label, "device_id" => device, "expires_at" => deadline}} =
             HeadlessOwner.lookup(
               owner_endpoint,
               Path.join(certs, "a1.pem"),
               Path.join(certs, "a1.key"),
               person,
               code
             )

    old_shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(old_shell) end)
    send(self(), {:mix_shell_input, :prompt, "CANCEL"})

    args = [
      "approve",
      "--connect",
      "127.0.0.1",
      "--name",
      "relay.test",
      "--port",
      Integer.to_string(OwnerTLS.port(owner)),
      "--ca",
      Path.join(certs, "ca.pem"),
      "--cert",
      Path.join(certs, "a1.pem"),
      "--key",
      Path.join(certs, "a1.key"),
      "--person",
      person,
      "--code",
      code
    ]

    assert_raise Mix.Error, "approval cancelled", fn -> Mix.Tasks.Attention.Owner.run(args) end
    assert_receive {:mix_shell, :info, [<<"Account: ", _::binary>> = display]}

    assert display ==
             "Account: #{person}\nDevice label (unsafe bytes escaped): laptop\\x1B[8m\\x0D\\x1B[2Kfake\\x0APublic-key fingerprint: deadbeef\\x5C\\xE2\\x80\\xAE\nPublic-key fingerprint: #{device}\nExpires: " <>
               Integer.to_string(deadline)

    assert :binary.match(display, <<27>>) == :nomatch
    assert :binary.match(display, <<13>>) == :nomatch
    assert length(String.split(display, "\n")) == 4
    refute Map.has_key?(:sys.get_state(relay).store.engine.devices, device)

    send(self(), {:mix_shell_input, :prompt, "APPROVE"})
    assert :ok = Mix.Tasks.Attention.Owner.run(args)
    assert_receive {:mix_shell, :info, ["approved " <> ^device]}
    assert Map.has_key?(:sys.get_state(relay).store.engine.devices, device)
  end

  defp endpoint(certs, port),
    do: %{
      address: {127, 0, 0, 1},
      name: "relay.test",
      ca_file: Path.join(certs, "ca.pem"),
      port: port
    }
end
