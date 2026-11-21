defmodule AttentionServer.HeadlessDeviceV2Test do
  # The headless device tools over client API v2 (behind client_api_v2):
  # device read (inbox snapshot stored before its cursor) and the enrollment
  # proof (one GET /v2/inbox).
  use ExUnit.Case, async: false

  alias AttentionServer.{ClientAPI, HeadlessDevice, HeadlessEnrollment, JSON, Relay, TLSFixtures}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  @choices %{
    "schema" => "attention-client.actions/1",
    "actions" => [
      %{"id" => "yes", "label" => "Yes", "kind" => "value", "value" => %{"pick" => "yes"}},
      %{"id" => "reply", "label" => "Reply", "kind" => "text"}
    ]
  }

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
    # One file holds the cursor and the items it covers.
    assert read_json!(directory, "v2/items.json") ==
             %{"rev" => rev, "items" => items, "tombstones" => %{}}

    refute File.exists?(Path.join(directory, "v2/cursor.json"))

    for file <- ~w(v2 v2/items.json) do
      assert {:ok, %File.Stat{mode: mode}} = File.stat(Path.join(directory, file))
      assert Bitwise.band(mode, 0o077) == 0
    end

    forward!(ctx.relay, "four", 5)
    assert {:ok, %{"rev" => later, "items" => more}} = HeadlessDevice.read(directory, endpoint)
    assert later > rev
    assert length(more) == 4
    assert read_json!(directory, "v2/items.json")["rev"] == later

    # Another person's device sees none of it.
    assert {:ok, %{"items" => []}} = HeadlessDevice.read(ctx.identities["b1"], ctx.endpoint)
  end

  test "a later read applies only the change batches after the stored cursor", ctx do
    forward!(ctx.relay, "one", 2)
    directory = ctx.identities["a1"]
    assert {:ok, %{"rev" => rev}} = HeadlessDevice.read(directory, ctx.endpoint)

    # Mark the held copy locally. A snapshot would overwrite it; applying only
    # the changes after the cursor leaves an unchanged item as held.
    held = read_json!(directory, "v2/items.json")
    marked = update_in(held, ["items", Access.at(0), "message", "title"], fn _ -> "held" end)
    File.write!(Path.join(directory, "v2/items.json"), JSON.encode!(marked))

    for {id, n} <- [{"two", 3}, {"three", 4}, {"four", 5}], do: forward!(ctx.relay, id, n)

    # One change per batch, so the read follows `more` across batches.
    endpoint = Map.put(ctx.endpoint, :v2_batch_limit, 1)
    assert {:ok, %{"rev" => later, "items" => items}} = HeadlessDevice.read(directory, endpoint)
    assert later > rev
    titles = Enum.map(items, & &1["message"]["title"])
    assert Enum.sort(titles) == ~w(four held three two)
    assert %{"rev" => ^later, "items" => ^items} = read_json!(directory, "v2/items.json")

    # Nothing new: the cursor stays and the items are as held.
    assert {:ok, %{"rev" => ^later, "items" => ^items}} = HeadlessDevice.read(directory, endpoint)
  end

  test "an expired cursor rebaselines from a fresh snapshot", ctx do
    forward!(ctx.relay, "one", 2)
    directory = ctx.identities["a1"]
    assert {:ok, %{"rev" => rev, "items" => items}} = HeadlessDevice.read(directory, ctx.endpoint)

    # A cursor the server no longer serves (here, past its position) is 410.
    File.write!(
      Path.join(directory, "v2/items.json"),
      JSON.encode!(%{"rev" => rev + 1000, "items" => [], "tombstones" => %{}})
    )

    assert {:ok, %{"rev" => ^rev, "items" => ^items}} =
             HeadlessDevice.read(directory, ctx.endpoint)

    assert read_json!(directory, "v2/items.json")["rev"] == rev
  end

  test "device read refuses a revoked device and keeps its stored cursor", ctx do
    forward!(ctx.relay, "one", 2)
    directory = ctx.identities["a1"]
    assert {:ok, %{"rev" => rev}} = HeadlessDevice.read(directory, ctx.endpoint)
    assert :ok = Relay.revoke_device(ctx.relay, ctx.keys["a1"])
    assert {:error, _reason} = HeadlessDevice.read(directory, ctx.endpoint)
    assert read_json!(directory, "v2/items.json")["rev"] == rev
  end

  # Review C6-1. A respond reads internally, so it can overlap a read on the
  # same directory, and the older read's write can land last. Whatever lands
  # last must hold a cursor and the items it covers, so no change is skipped.
  test "an older read's state landing last is consistent and skips nothing", ctx do
    forward!(ctx.relay, "one", 2)
    directory = ctx.identities["a1"]
    state = Path.join(directory, "v2")
    assert {:ok, _} = HeadlessDevice.read(directory, ctx.endpoint)
    older = File.read!(Path.join(state, "items.json"))

    forward!(ctx.relay, "two", 3)
    assert {:ok, %{"items" => newer}} = HeadlessDevice.read(directory, ctx.endpoint)
    assert length(newer) == 2

    # The older read's items write lands after the newer read's writes, as in
    # the interleaving A items, B items, B cursor, A cursor (review C6-1).
    File.write!(Path.join(state, "items.json"), older)

    forward!(ctx.relay, "three", 4)
    assert {:ok, %{"items" => items}} = HeadlessDevice.read(directory, ctx.endpoint)
    assert Enum.sort(Enum.map(items, & &1["message"]["title"])) == ~w(one three two)
  end

  # Review C6-2. The stored state is local, so its size is not capped by the
  # server frame limit: a large store still reads forward instead of
  # re-snapshotting.
  test "a stored state larger than a server frame still reads forward", ctx do
    big = String.duplicate("x", 900_000)
    for n <- 1..6, do: forward!(ctx.relay, "big-#{n}", n + 1, %{"body" => big})
    directory = ctx.identities["a1"]
    assert {:ok, _} = HeadlessDevice.read(directory, ctx.endpoint)
    assert File.stat!(Path.join(directory, "v2/items.json")).size > 5 * 1024 * 1024

    held = read_json!(directory, "v2/items.json")
    marked = update_in(held, ["items", Access.at(0), "message", "title"], fn _ -> "held" end)
    File.write!(Path.join(directory, "v2/items.json"), JSON.encode!(marked))

    forward!(ctx.relay, "small", 20)
    assert {:ok, %{"items" => items}} = HeadlessDevice.read(directory, ctx.endpoint)
    assert length(items) == 7
    assert "held" in Enum.map(items, & &1["message"]["title"])
  end

  describe "against a server sending a bad batch" do
    setup ctx do
      server = start_supervised!({Bandit, fake_server_opts(ctx)}, id: :fake_server)
      {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
      directory = ctx.identities["a1"]

      File.write!(
        Path.join(directory, "v2/items.json") |> tap(&File.mkdir_p!(Path.dirname(&1))),
        JSON.encode!(%{"rev" => 5, "items" => [], "tombstones" => %{}})
      )

      File.chmod!(Path.join(directory, "v2"), 0o700)
      %{fake: Map.put(ctx.endpoint, :port, port), directory: directory}
    end

    # Review C6-3: a malformed item change fails the read; the cursor stays.
    test "a malformed item change fails loudly and keeps the cursor", ctx do
      for bad <- [
            %{"rev" => 6, "kind" => "item", "data" => %{}},
            %{"rev" => 6, "kind" => "item"},
            %{"rev" => 6, "kind" => "item_removed", "data" => %{"item_id" => 1}}
          ] do
        :persistent_term.put({__MODULE__, :batch}, %{
          "changes" => [bad],
          "rev" => 6,
          "more" => false
        })

        assert {:error, :invalid_v2_change} = HeadlessDevice.read(ctx.directory, ctx.fake)
        assert read_json!(ctx.directory, "v2/items.json")["rev"] == 5
      end
    end

    # Review C6-4: `more` with no progress is an error, not an endless loop.
    test "more with no progress fails instead of repeating forever", ctx do
      :persistent_term.put({__MODULE__, :batch}, %{"changes" => [], "rev" => 5, "more" => true})
      assert {:error, :no_progress} = HeadlessDevice.read(ctx.directory, ctx.fake)
      assert read_json!(ctx.directory, "v2/items.json")["rev"] == 5
    end
  end

  defmodule FakeServer do
    @moduledoc false
    import Plug.Conn

    def init(test), do: test

    def call(%{request_path: "/v2/changes"} = conn, test) do
      body = :persistent_term.get({test, :batch})

      conn
      |> put_resp_content_type("application/json")
      |> send_resp(200, AttentionServer.JSON.encode!(body))
    end

    def call(conn, _test), do: send_resp(conn, 404, ~s({"error":"not_found","retryable":false}))
  end

  defp fake_server_opts(%{certs: certs}) do
    [
      plug: {FakeServer, __MODULE__},
      scheme: :https,
      ip: {127, 0, 0, 1},
      port: 0,
      certfile: Path.join(certs, "server.pem"),
      keyfile: Path.join(certs, "server.key"),
      http_2_options: [enabled: false],
      startup_log: false
    ]
  end

  test "device respond answers once, storing its key and exact body before sending", ctx do
    forward!(ctx.relay, "pick", 7, %{"message_type" => "request", "answer_ui" => @choices})
    directory = ctx.identities["a1"]
    answer = answer_file!(ctx.dir, %{"choice" => "yes"})

    assert {:ok, %{"rev" => rev, "item" => item, "idempotency_key" => key, "replayed" => false}} =
             HeadlessDevice.respond(directory, ctx.endpoint, "app:test", "pick", answer)

    assert is_integer(rev)
    assert %{"terminal" => %{"state" => "answered"}} = item
    assert key =~ ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/

    assert [saved] = Path.wildcard(Path.join([directory, "operations", "v2-*.json"]))
    assert {:ok, stored} = JSON.decode_line(File.read!(saved))
    assert stored["idempotency_key"] == key
    assert stored["item_id"] == item["item_id"]
    assert stored["body"] == %{"choice" => "yes", "request_revision" => 1}

    # A retry of the same intent resends the same key and body: the stored outcome.
    assert {:ok, %{"idempotency_key" => ^key, "replayed" => true, "rev" => ^rev}} =
             HeadlessDevice.respond(directory, ctx.endpoint, "app:test", "pick", answer)

    # A different answer is a new intent, refused because the request is settled.
    other = answer_file!(ctx.dir, %{"comment" => "Actually no"}, "other.json")

    assert {:error, {:server, %{"error" => "already_answered", "retryable" => false}}} =
             HeadlessDevice.respond(directory, ctx.endpoint, "app:test", "pick", other)
  end

  test "device respond refuses what this device was not delivered", ctx do
    forward!(ctx.relay, "pick", 7, %{"message_type" => "request", "answer_ui" => @choices})
    answer = answer_file!(ctx.dir, %{})

    assert {:error, :not_in_inbox} =
             HeadlessDevice.respond(
               ctx.identities["a1"],
               ctx.endpoint,
               "app:test",
               "nope",
               answer
             )

    assert {:error, :not_in_inbox} =
             HeadlessDevice.respond(
               ctx.identities["a1"],
               ctx.endpoint,
               "app:other",
               "pick",
               answer
             )

    assert {:error, :not_in_inbox} =
             HeadlessDevice.respond(
               ctx.identities["b1"],
               ctx.endpoint,
               "app:test",
               "pick",
               answer
             )

    assert Path.wildcard(Path.join([ctx.identities["a1"], "operations", "v2-*.json"])) == []
  end

  test "device respond takes only the contract's answer body", ctx do
    forward!(ctx.relay, "pick", 7, %{"message_type" => "request", "answer_ui" => @choices})

    for value <- [nil, "yes", %{"choice" => "yes", "comment" => "x"}, %{"note" => nil}] do
      answer = answer_file!(ctx.dir, value)

      assert {:error, :invalid_answer} =
               HeadlessDevice.respond(
                 ctx.identities["a1"],
                 ctx.endpoint,
                 "app:test",
                 "pick",
                 answer
               )
    end
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

  defp answer_file!(dir, value, name \\ "answer.json") do
    path = Path.join(dir, name)
    File.write!(path, JSON.encode!(value))
    path
  end

  defp forward!(relay, id, n, extra \\ %{}) do
    frame = %{
      "type" => "forward",
      "op_id" => "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}",
      "session_handle" => @handle,
      "message" =>
        Map.merge(
          %{"sender_id" => "app:test", "message_id" => id, "created_at" => n, "title" => id},
          extra
        )
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
