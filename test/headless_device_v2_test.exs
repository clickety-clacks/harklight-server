defmodule AttentionServer.HeadlessDeviceV2Test do
  # The headless device tools over client API v2 (behind client_api_v2):
  # device read (inbox snapshot stored before its cursor) and the enrollment
  # proof (one GET /v2/inbox).
  use ExUnit.Case, async: false

  alias AttentionServer.{ClientAPI, HeadlessDevice, HeadlessEnrollment, JSON, Relay, TLSFixtures}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir =
      Path.join(System.tmp_dir!(), "harklight-device-v2-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    flags = Path.join(dir, "flags.json")
    File.write!(flags, ~s({"client_api_v2":true}))

    relay =
      start_supervised!(
        {Relay,
         store_path: Path.join(dir, "state.sqlite3"),
         feature_flags_path: flags,
         agent_fixture_mode: true,
         enrollment_fixture_mode: true},
        restart: :temporary
      )

    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    keys =
      Map.new(~w(a1 b1), fn name ->
        person = if name == "b1", do: "B", else: "A"
        if person == "B", do: Relay.setup_person(relay, "B")

        assert {:ok, key} =
                 Relay.setup_device_certificate(
                   relay,
                   person,
                   TLSFixtures.certificate_der!(certs, name)
                 )

        {name, key}
      end)

    listener =
      start_supervised!(
        {ClientAPI.Listener,
         relay: relay,
         certfile: Path.join(certs, "server.pem"),
         keyfile: Path.join(certs, "server.key"),
         cacertfile: Path.join(certs, "ca.pem")}
      )

    endpoint = %{
      address: {127, 0, 0, 1},
      name: "relay.test",
      port: ClientAPI.Listener.port(listener),
      ca_file: Path.join(certs, "ca.pem"),
      client_api_v2: true
    }

    identities = Map.new(~w(a1 b1), &{&1, identity!(dir, certs, &1, keys[&1], endpoint)})
    %{relay: relay, endpoint: endpoint, identities: identities, keys: keys, dir: dir}
  end

  test "device read stores the whole inbox, then its cursor, and reads again", ctx do
    for {id, n} <- [{"one", 2}, {"two", 3}, {"three", 4}], do: forward!(ctx.relay, id, n)
    directory = ctx.identities["a1"]

    # Two items per page, so the read pages.
    endpoint = Map.put(ctx.endpoint, :v2_page_limit, 2)
    assert {:ok, %{"rev" => rev, "items" => items}} = HeadlessDevice.read(directory, endpoint)
    assert is_integer(rev) and rev >= 1
    assert length(items) == 3
    keys = Enum.map(items, & &1["rank"]["key"])
    assert keys == Enum.sort(keys)

    assert Enum.sort(Enum.map(items, & &1["message"]["title"])) == ~w(one three two)
    assert read_json!(directory, "v2/cursor.json") == %{"rev" => rev}
    assert read_json!(directory, "v2/items.json") == %{"items" => items}

    for file <- ~w(v2 v2/items.json v2/cursor.json) do
      assert {:ok, %File.Stat{mode: mode}} = File.stat(Path.join(directory, file))
      assert Bitwise.band(mode, 0o077) == 0
    end

    forward!(ctx.relay, "four", 5)
    assert {:ok, %{"rev" => later, "items" => more}} = HeadlessDevice.read(directory, endpoint)
    assert later > rev
    assert length(more) == 4
    assert read_json!(directory, "v2/cursor.json") == %{"rev" => later}

    # Another person's device sees none of it.
    assert {:ok, %{"items" => []}} = HeadlessDevice.read(ctx.identities["b1"], ctx.endpoint)
  end

  test "device read refuses a revoked device and keeps its stored cursor", ctx do
    forward!(ctx.relay, "one", 2)
    directory = ctx.identities["a1"]
    assert {:ok, %{"rev" => rev}} = HeadlessDevice.read(directory, ctx.endpoint)
    assert :ok = Relay.revoke_device(ctx.relay, ctx.keys["a1"])
    assert {:error, _reason} = HeadlessDevice.read(directory, ctx.endpoint)
    assert read_json!(directory, "v2/cursor.json") == %{"rev" => rev}
  end

  test "device respond over client API v2 refuses until it is available", ctx do
    answer = Path.join(ctx.dir, "answer.json")
    File.write!(answer, "null")

    assert {:error, :client_api_v2_respond_unavailable} =
             HeadlessDevice.respond(ctx.identities["a1"], ctx.endpoint, "app:test", "one", answer)
  end

  test "the enrollment proof is one GET /v2/inbox with the approved key", ctx do
    assert :ok = HeadlessEnrollment.verify_relay(ctx.identities["a1"], ctx.endpoint)
    assert :ok = Relay.revoke_device(ctx.relay, ctx.keys["a1"])

    assert {:error, :relay_proof_failed} =
             HeadlessEnrollment.verify_relay(ctx.identities["a1"], ctx.endpoint)
  end

  test "with the flag off the enrollment proof still needs /relay", ctx do
    # The /v2 listener has no /relay upgrade, so the old proof fails as before.
    endpoint = Map.put(ctx.endpoint, :client_api_v2, false)
    assert {:error, _reason} = HeadlessEnrollment.verify_relay(ctx.identities["a1"], endpoint)
  end

  test "the tools read client_api_v2 from the server's feature-flags file", ctx do
    previous = System.get_env("ATTENTION_SERVER_FEATURE_FLAGS_PATH")

    on_exit(fn ->
      if previous,
        do: System.put_env("ATTENTION_SERVER_FEATURE_FLAGS_PATH", previous),
        else: System.delete_env("ATTENTION_SERVER_FEATURE_FLAGS_PATH")
    end)

    System.delete_env("ATTENTION_SERVER_FEATURE_FLAGS_PATH")
    refute AttentionServer.HeadlessTLS.client_api_v2?()

    on = Path.join(ctx.dir, "tool-flags.json")
    File.write!(on, ~s({"client_api_v2":true}))
    System.put_env("ATTENTION_SERVER_FEATURE_FLAGS_PATH", on)
    assert AttentionServer.HeadlessTLS.client_api_v2?()

    File.write!(on, ~s({"client_api_v2":false}))
    refute AttentionServer.HeadlessTLS.client_api_v2?()
  end

  defp forward!(relay, id, n) do
    frame = %{
      "type" => "forward",
      "op_id" => "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}",
      "session_handle" => @handle,
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => id,
        "created_at" => n,
        "title" => id
      }
    }

    assert {:ok, line} =
             Relay.request_line(relay, "origin", JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(line)
  end

  defp identity!(dir, certs, name, key, endpoint) do
    identity = Path.join(dir, name)
    File.mkdir!(identity)
    File.chmod!(identity, 0o700)
    File.cp!(Path.join(certs, "#{name}.pem"), Path.join(identity, "identity.pem"))
    File.cp!(Path.join(certs, "#{name}.key"), Path.join(identity, "identity.key"))
    File.chmod!(Path.join(identity, "identity.key"), 0o600)

    File.write!(
      Path.join(identity, "enrollment.json"),
      JSON.encode!(%{"person_id" => if(name == "b1", do: "B", else: "A"), "device_id" => key}) <>
        "\n"
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

    identity
  end

  defp read_json!(directory, file) do
    assert {:ok, value} = JSON.decode_line(File.read!(Path.join(directory, file)))
    value
  end
end
