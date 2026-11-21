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

    # These are linked to the test process, so they may already be shutting
    # down when on_exit runs; stopping one that is mid-exit is not a failure.
    on_exit(fn ->
      stop_quietly(web)
      stop_quietly(relay)
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

  for {format, ending} <- [{"Agent compact JSON", ""}, {"LF-terminated JSON", "\n"}] do
    test "#{format} metadata supports read, persisted replay and operation retry", ctx do
      %{relay: relay, endpoint: endpoint, identities: identities, keys: keys, dir: dir} = ctx
      identity = identities["a2"]
      metadata = agent_metadata(ctx)

      for {name, bytes} <- metadata do
        File.write!(Path.join(identity, name), bytes <> unquote(ending))
      end

      message = %{
        "sender_id" => "app:test",
        "message_id" => "agent-metadata",
        "created_at" => 1,
        "title" => "Read and answer"
      }

      assert %{"type" => "ok", "seq" => 1} = store_existing(relay, keys["a1"], message, 5)

      assert {:ok, %{"cursor" => 1, "events" => [%{"message" => ^message}]}} =
               HeadlessDevice.read(identity, endpoint)

      assert {:ok, 1} = Relay.acknowledged_cursor(relay, keys["a2"])
      answer_file = Path.join(dir, "answer.json")
      File.write!(answer_file, ~s|{"text":"continue"}|)

      assert {:ok, %{"type" => "ok", "seq" => 2, "op_id" => op_id}} =
               HeadlessDevice.respond(
                 identity,
                 endpoint,
                 "app:test",
                 "agent-metadata",
                 answer_file
               )

      # Exercise the same file reader for the persisted cursor, events and
      # operation record, using both document endings on the next invocation.
      for path <- Path.wildcard(Path.join(identity, "**/*.json")),
          Path.basename(path) not in ["server.json", "enrollment.json"] do
        File.write!(path, String.trim_trailing(File.read!(path), "\n") <> unquote(ending))
      end

      [operation] = Path.wildcard(Path.join(identity, "operations/*.json"))
      operation_bytes = File.read!(operation)

      assert {:ok, %{"type" => "ok", "seq" => 2, "op_id" => ^op_id}} =
               HeadlessDevice.respond(
                 identity,
                 endpoint,
                 "app:test",
                 "agent-metadata",
                 answer_file
               )

      assert File.read!(operation) == operation_bytes
      assert Path.wildcard(Path.join(identity, "operations/*.json")) == [operation]

      assert {:ok,
              %{"cursor" => 2, "events" => [_, %{"type" => "lifecycle", "state" => "answered"}]}} =
               HeadlessDevice.read(identity, endpoint)

      assert {:ok, 2} = Relay.acknowledged_cursor(relay, keys["a2"])

      for {name, bytes} <- metadata do
        assert File.read!(Path.join(identity, name)) == bytes <> unquote(ending)
      end
    end
  end

  test "stored metadata keeps its byte limit with and without LF", ctx do
    identity = ctx.identities["a2"]
    max_bytes = 2 * (2 * 1024 * 1024) + 16_384

    for ending <- ["", "\n"], {name, bytes} <- agent_metadata(ctx) do
      path = Path.join(identity, name)
      padded = bytes <> String.duplicate(" ", max_bytes - byte_size(bytes) - byte_size(ending))
      File.write!(path, padded <> ending)
      assert {:ok, %{"cursor" => 0, "events" => []}} = HeadlessDevice.read(identity, ctx.endpoint)
      File.write!(path, padded <> " " <> ending)
      assert {:error, :identity_or_server_mismatch} = HeadlessDevice.read(identity, ctx.endpoint)
      File.write!(path, bytes)
    end
  end

  test "stored metadata still rejects malformed JSON, trailing data and mismatched identity",
       ctx do
    identity = ctx.identities["a2"]

    for {name, bytes} <- agent_metadata(ctx) do
      path = Path.join(identity, name)

      for invalid <- ["{oops}", bytes <> "{}", bytes <> "\ntrue\n", "{\"x\":1,\"x\":2}"] do
        File.write!(path, invalid)

        assert {:error, :identity_or_server_mismatch} =
                 HeadlessDevice.read(identity, ctx.endpoint)
      end

      File.write!(path, bytes)
    end

    for {field, value} <- [
          {"address", "127.0.0.2"},
          {"name", "other.test"},
          {"ca_sha256", "wrong"}
        ] do
      server = agent_metadata(ctx)["server.json"]
      assert {:ok, saved} = JSON.decode_line(server <> "\n")
      File.write!(Path.join(identity, "server.json"), JSON.encode!(Map.put(saved, field, value)))
      assert {:error, :identity_or_server_mismatch} = HeadlessDevice.read(identity, ctx.endpoint)
    end

    File.write!(Path.join(identity, "server.json"), agent_metadata(ctx)["server.json"])

    File.write!(
      Path.join(identity, "enrollment.json"),
      ~s|{"person_id":"A","device_id":"#{ctx.keys["a1"]}"}|
    )

    assert {:error, :identity_or_server_mismatch} = HeadlessDevice.read(identity, ctx.endpoint)
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

  test "large WebSocket answer is not refused by the client", ctx do
    %{relay: relay, endpoint: endpoint, identities: identities, keys: keys, dir: dir} = ctx

    message = %{
      "sender_id" => "app:test",
      "message_id" => "oversized-answer",
      "created_at" => 1,
      "title" => "T",
      "body" => "short"
    }

    assert %{"type" => "ok", "seq" => 1} = store_existing(relay, keys["a1"], message, 4)
    assert {:ok, %{"cursor" => 1}} = HeadlessDevice.read(identities["a2"], endpoint)

    answer_file = Path.join(dir, "oversized-answer.json")
    File.write!(answer_file, JSON.encode!(%{"text" => String.duplicate("x", 2 * 1024 * 1024)}))

    case HeadlessDevice.respond(
           identities["a2"],
           endpoint,
           "app:test",
           "oversized-answer",
           answer_file
         ) do
      {:ok, %{"type" => "ok"}} ->
        :ok

      {:error, :invalid_reply} ->
        :ok

      {:error, {:server, %{"type" => "error"}}} ->
        :ok

      other ->
        flunk("expected a server response to the large answer, got: #{inspect(other)}")
    end
  end

  test "device CLI no longer exposes the agent send or receipt commands" do
    Code.ensure_loaded!(HeadlessDevice)

    assert_raise Mix.Error, ~r/use read or respond/, fn ->
      AttentionServer.CLI.with_io(Mix.Shell.Process, Mix.Error, fn ->
        AttentionServer.CLI.Device.run(["send"])
      end)
    end

    assert_raise Mix.Error, ~r/use read or respond/, fn ->
      AttentionServer.CLI.with_io(Mix.Shell.Process, Mix.Error, fn ->
        AttentionServer.CLI.Device.run(["receipt"])
      end)
    end

    refute function_exported?(HeadlessDevice, :send_message, 3)
    refute function_exported?(HeadlessDevice, :receipt, 4)
  end

  # Match ServerFile/EnrollmentFile field order and compact serde_json::to_vec
  # output from the Agent, including its optional origin-port metadata.
  defp agent_metadata(ctx) do
    ca_hash =
      :crypto.hash(:sha256, File.read!(ctx.endpoint.ca_file)) |> Base.encode16(case: :lower)

    %{
      "server.json" =>
        ~s|{"address":"127.0.0.1","name":"relay.test","port":#{ctx.endpoint.port},"ca_sha256":"#{ca_hash}","origin_port":18446}|,
      "enrollment.json" => ~s|{"person_id":"A","device_id":"#{ctx.keys["a2"]}"}|
    }
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

  defp stop_quietly(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  catch
    :exit, _reason -> :ok
  end
end
