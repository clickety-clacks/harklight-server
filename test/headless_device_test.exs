defmodule AttentionServer.HeadlessDeviceTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    HeadlessDevice,
    HeadlessEnrollment,
    JSON,
    OriginWire,
    Relay,
    TLSFixtures,
    WebSocketRelay
  }

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "harklight-device-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")
    {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")

    keys =
      Map.new(~w(a1 a2 b1), fn name ->
        person = if name == "b1", do: "B", else: "A"

        assert {:ok, key} =
                 Relay.setup_device_certificate(
                   relay,
                   person,
                   TLSFixtures.certificate_der!(certs, name)
                 )

        {name, key}
      end)

    assert :ok = Relay.setup_sender(relay, keys["a1"], "app:test")

    {:ok, web} =
      WebSocketRelay.start_link(
        relay: relay,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem")
      )

    endpoint = %{
      address: {127, 0, 0, 1},
      name: "relay.test",
      port: WebSocketRelay.port(web),
      ca_file: Path.join(certs, "ca.pem")
    }

    identities =
      Map.new(~w(a1 a2 b1), fn name ->
        identity = Path.join(dir, name)
        File.mkdir!(identity)
        File.chmod!(identity, 0o700)
        File.cp!(Path.join(certs, "#{name}.pem"), Path.join(identity, "identity.pem"))
        File.cp!(Path.join(certs, "#{name}.key"), Path.join(identity, "identity.key"))
        File.chmod!(Path.join(identity, "identity.key"), 0o600)

        File.write!(
          Path.join(identity, "enrollment.json"),
          JSON.encode!(%{
            "person_id" => if(name == "b1", do: "B", else: "A"),
            "device_id" => keys[name]
          }) <> "\n"
        )

        File.write!(
          Path.join(identity, "server.json"),
          JSON.encode!(%{
            "address" => "127.0.0.1",
            "name" => "relay.test",
            "port" => endpoint.port,
            "ca_sha256" =>
              :crypto.hash(:sha256, File.read!(endpoint.ca_file)) |> Base.encode16(case: :lower)
          }) <> "\n"
        )

        {name, identity}
      end)

    on_exit(fn ->
      if Process.alive?(web), do: GenServer.stop(web)
      if Process.alive?(relay), do: GenServer.stop(relay)
    end)

    %{
      dir: dir,
      path: path,
      relay: relay,
      web: web,
      endpoint: endpoint,
      identities: identities,
      keys: keys,
      certs: certs
    }
  end

  test "previously stored requests stay readable and answerable by enrolled devices", ctx do
    %{relay: relay, endpoint: endpoint, identities: identities, keys: keys, dir: dir} = ctx

    message = %{
      "sender_id" => "app:test",
      "message_id" => "existing-request",
      "created_at" => 1,
      "title" => "Existing request",
      "body" => "Stored before the device send route was removed",
      "answer_ui" => %{"kind" => "choice", "items" => ["yes", "no"]},
      "extra" => %{"legacy" => true}
    }

    assert %{"type" => "ok", "seq" => 1} = seed_preexisting_message(relay, keys["a1"], message, 1)

    assert {:ok,
            %{
              "cursor" => 1,
              "events" => [%{"type" => "delivery", "message" => ^message} = delivery]
            }} =
             HeadlessDevice.read(identities["a2"], endpoint)

    assert delivery["origin"] == keys["a1"]

    answer_file = Path.join(dir, "answer.json")
    answer = %{"text" => "continue", "note" => nil}
    File.write!(answer_file, JSON.encode!(answer) <> "\n")

    assert {:ok, %{"type" => "ok", "seq" => 2, "op_id" => op_id}} =
             HeadlessDevice.respond(
               identities["a2"],
               endpoint,
               "app:test",
               "existing-request",
               answer_file
             )

    assert is_binary(op_id)

    assert {:ok, %{"cursor" => 2, "events" => [origin, lifecycle]}} =
             HeadlessDevice.read(identities["a1"], endpoint)

    assert origin["type"] == "delivery"
    refute Map.has_key?(origin, "message")
    assert lifecycle["type"] == "lifecycle"
    assert lifecycle["answer"] == answer
    assert lifecycle["message_id"] == "existing-request"

    GenServer.stop(ctx.web)
    GenServer.stop(relay)
    {:ok, reopened} = Relay.start_link(store_path: ctx.path)

    {:ok, reopened_web} =
      WebSocketRelay.start_link(
        relay: reopened,
        certfile: Path.join(ctx.certs, "server.pem"),
        keyfile: Path.join(ctx.certs, "server.key"),
        cacertfile: Path.join(ctx.certs, "ca.pem")
      )

    endpoint = %{endpoint | port: WebSocketRelay.port(reopened_web)}

    on_exit(fn ->
      if Process.alive?(reopened_web), do: GenServer.stop(reopened_web)
      if Process.alive?(reopened), do: GenServer.stop(reopened)
    end)

    assert {:ok, %{"cursor" => 2, "events" => [_, _]}} =
             HeadlessDevice.read(identities["a2"], endpoint)

    assert {:ok, %{"cursor" => 2, "events" => [_, _]}} =
             HeadlessDevice.read(identities["a1"], endpoint)
  end

  test "stored messages remain scoped to their person", ctx do
    %{relay: relay, endpoint: endpoint, identities: identities, keys: keys, dir: dir} = ctx

    message = %{
      "sender_id" => "app:test",
      "message_id" => "tenant-request",
      "created_at" => 1,
      "title" => "Tenant"
    }

    assert %{"type" => "ok", "seq" => 1} = store_existing(relay, keys["a1"], message, 2)

    assert {:ok, %{"cursor" => 0, "events" => []}} =
             HeadlessDevice.read(identities["b1"], endpoint)

    answer_file = Path.join(dir, "answer.json")
    File.write!(answer_file, "null\n")

    assert {:error, :not_in_inbox} =
             HeadlessDevice.respond(
               identities["b1"],
               endpoint,
               "app:test",
               "tenant-request",
               answer_file
             )

    assert {:ok, %{"cursor" => 1, "events" => [%{"message" => ^message}]}} =
             HeadlessDevice.read(identities["a2"], endpoint)

    wrong = Path.join(dir, "wrong")
    File.cp_r!(identities["a1"], wrong)

    File.write!(
      Path.join(wrong, "enrollment.json"),
      JSON.encode!(%{"person_id" => "A", "device_id" => keys["a2"]}) <> "\n"
    )

    assert {:error, :identity_or_server_mismatch} = HeadlessDevice.read(wrong, endpoint)

    File.rm!(Path.join([identities["a2"], "events", "00000000000000000001.json"]))
    assert {:error, :replay_failed} = HeadlessDevice.read(identities["a2"], endpoint)
    assert {:ok, 1} = Relay.acknowledged_cursor(relay, keys["a2"])
  end

  test "a previously stored near-limit message remains readable and answerable", ctx do
    %{relay: relay, endpoint: endpoint, identities: identities, keys: keys, dir: dir} = ctx

    message = %{
      "sender_id" => "app:test",
      "message_id" => "large-existing-request",
      "created_at" => 1,
      "title" => "T",
      "body" => String.duplicate("x", 2_096_948)
    }

    assert %{"type" => "ok", "seq" => 1} = store_existing(relay, keys["a1"], message, 3)
    assert :ok = HeadlessEnrollment.verify_relay(identities["a2"], endpoint)

    assert {:ok, %{"cursor" => 1, "events" => [%{"message" => ^message} = delivery]}} =
             HeadlessDevice.read(identities["a2"], endpoint)

    assert byte_size(JSON.encode!(delivery)) > 2 * 1024 * 1024

    answer_file = Path.join(dir, "large-answer.json")
    File.write!(answer_file, "null\n")

    assert {:ok, %{"type" => "ok", "seq" => 2}} =
             HeadlessDevice.respond(
               identities["a2"],
               endpoint,
               "app:test",
               "large-existing-request",
               answer_file
             )
  end

  test "device commands send and retrieve a correlated receipt while read and respond continue",
       ctx do
    %{dir: dir, endpoint: endpoint, identities: identities, keys: keys} = ctx

    message = %{
      "sender_id" => "app:test",
      "message_id" => "compat-request",
      "created_at" => 1,
      "title" => "Choose one",
      "body" => "Device command compatibility",
      "answer_ui" => %{"kind" => "choice", "items" => ["yes", "no"]}
    }

    message_file = Path.join(dir, "message.json")
    answer_file = Path.join(dir, "answer.json")
    answer = %{"choice" => "yes"}
    File.write!(message_file, JSON.encode!(message) <> "\n")
    File.write!(answer_file, JSON.encode!(answer) <> "\n")

    old_shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(old_shell) end)

    assert :ok = task("send", identities["a1"], endpoint, ["--message", message_file])
    assert_receive {:mix_shell, :info, [send_line]}

    assert {:ok, %{"type" => "ok", "seq" => 1, "op_id" => send_op}} =
             JSON.decode_line(send_line <> "\n")

    assert :ok =
             task("receipt", identities["a1"], endpoint, [
               "--sender",
               "app:test",
               "--message-id",
               "compat-request"
             ])

    assert_receive {:mix_shell, :info, [pending_line]}

    assert {:ok, %{"status" => "pending", "acceptance_seq" => 1, "observed_through_seq" => 1}} =
             JSON.decode_line(pending_line <> "\n")

    assert :ok = task("read", identities["a2"], endpoint, [])
    assert_receive {:mix_shell, :info, [read_line]}

    assert {:ok, %{"events" => [%{"message" => ^message} = delivery]}} =
             JSON.decode_line(read_line <> "\n")

    assert delivery["origin"] == keys["a1"]

    assert :ok =
             task("respond", identities["a2"], endpoint, [
               "--sender",
               "app:test",
               "--message-id",
               "compat-request",
               "--answer",
               answer_file
             ])

    assert_receive {:mix_shell, :info, [respond_line]}

    assert {:ok, %{"type" => "ok", "seq" => 2, "op_id" => answer_op}} =
             JSON.decode_line(respond_line <> "\n")

    assert :ok =
             task("receipt", identities["a1"], endpoint, [
               "--sender",
               "app:test",
               "--message-id",
               "compat-request"
             ])

    assert_receive {:mix_shell, :info, [receipt_line]}
    assert {:ok, %{"receipt" => proof}} = JSON.decode_line(receipt_line <> "\n")
    assert proof["receipt_version"] == 1
    assert is_binary(proof["relay_instance_id"])
    assert proof["person_id"] == "A"
    assert proof["sender_id"] == "app:test"
    assert proof["message_id"] == "compat-request"
    assert proof["acceptance_seq"] == 1
    assert proof["terminal_seq"] == 2
    assert proof["terminal_kind"] == "answer"
    assert proof["state"] == "answered"
    assert is_integer(proof["at"])
    assert proof["has_answer"] == true
    assert proof["answer"] == answer
    assert proof["winning_op_id"] == answer_op

    assert proof["actor"] == %{
             "kind" => "device",
             "id" => keys["a2"],
             "key_fingerprint" => keys["a2"]
           }

    assert {:ok, %{"receipt" => ^proof}} =
             HeadlessDevice.receipt(identities["a1"], endpoint, "app:test", "compat-request")

    assert {:ok, %{"seq" => 1, "duplicate" => true, "op_id" => ^send_op}} =
             HeadlessDevice.send_message(identities["a1"], endpoint, message_file)
  end

  defp task(command, directory, endpoint, additional) do
    Mix.Tasks.Attention.Device.run([
      command,
      "--directory",
      directory,
      "--connect",
      "127.0.0.1",
      "--name",
      endpoint.name,
      "--port",
      Integer.to_string(endpoint.port),
      "--ca",
      endpoint.ca_file
      | additional
    ])
  end

  defp store_existing(relay, key, message, operation_number) do
    OriginWire.command(relay, key, %{
      "v" => 1,
      "type" => "send",
      "op_id" =>
        "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(operation_number), 12, "0")}",
      "message" => message
    })
  end

  defp seed_preexisting_message(relay, key, message, operation_number) do
    frame = %{
      "v" => 1,
      "type" => "forward",
      "op_id" =>
        "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(operation_number), 12, "0")}",
      "message" => message
    }

    assert {:ok, line} = Relay.request_line(relay, key, JSON.encode!(frame) <> "\n")
    assert {:ok, response} = JSON.decode_line(line)
    response
  end
end
