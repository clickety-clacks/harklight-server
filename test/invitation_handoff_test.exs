defmodule AttentionServer.InvitationHandoffTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import ExUnit.CaptureLog

  alias AttentionServer.{
    DeviceIdentity,
    Engine,
    EnrollmentHTTPS,
    GuidedJoin,
    HeadlessTLS,
    InvitationHandoff,
    JSON,
    OwnerTLS,
    Relay,
    Store,
    TLSFixtures
  }

  @request "00000000-0000-4000-8000-000000000001"
  @person "00000000-0000-4000-8000-000000000002"
  @owner String.duplicate("a", 64)

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "invite-handoff-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")
    relay = start_supervised!({Relay, store_path: path})
    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))
    {:ok, other} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "b1"))
    owner = start_supervised!({OwnerTLS, owner_options(certs, relay)})

    enrollment =
      start_supervised!(
        {EnrollmentHTTPS,
         relay: relay,
         certfile: Path.join(certs, "server.pem"),
         keyfile: Path.join(certs, "server.key")}
      )

    {:ok, config} =
      GuidedJoin.configuration(
        "relay.test",
        EnrollmentHTTPS.port(enrollment),
        18443,
        File.read!(Path.join(certs, "ca.pem"))
      )

    :sys.replace_state(relay, fn state -> %{state | join_configuration: config} end)

    endpoint = %{
      address: {127, 0, 0, 1},
      name: "relay.test",
      port: OwnerTLS.port(owner),
      ca_file: Path.join(certs, "ca.pem")
    }

    %{
      relay: relay,
      owner: owner,
      endpoint: endpoint,
      person: person,
      other: other,
      path: path,
      config: config,
      enrollment: enrollment,
      dir: dir
    }
  end

  test "one owner-agent action directly returns a shareable destination/account/expiry and usable join",
       c do
    result = create(c)
    assert result.state == "ready"
    assert result.account == c.person
    assert result.destination["enrollment_url"] == c.config["enrollment_url"]
    assert result.expires_at > System.system_time(:millisecond)
    refute inspect(result) =~ "harklight://join#"
    {:ok, rendered} = JSON.decode_line(InvitationHandoff.encode_result(result) <> "\n")
    assert rendered["share_url"] == result.share_url
    invitation = decode_link(result.share_url)
    assert invitation["person_id"] == c.person
    assert invitation["ca_sha256"] == c.config["ca_sha256"]
    assert invitation["expires_at"] == result.expires_at
    endpoint = %{c.endpoint | port: URI.parse(invitation["enrollment_url"]).port}

    assert {:ok, 200, %{"state" => "pending"}} =
             HeadlessTLS.post_json(endpoint, "/enroll", %{
               "invite_token" => invitation["invite_token"],
               "status_secret" => String.duplicate("b", 64),
               "public_certificate" => File.read!(Path.join(c.certs, "a2.pem"))
             })

    engine = :sys.get_state(c.relay).store.engine
    assert engine.devices == %{}
    assert engine.sender_grants == %{}
    assert map_size(engine.pending_enrollments) == 1
    assert create(c).state == "used"
    assert map_size(:sys.get_state(c.relay).store.engine.join_invitations) == 1
  end

  test "authorized agent handoff prefers the tappable route and retains the exact full invitation",
       c do
    :sys.replace_state(c.relay, fn state ->
      %{state | dashboard_origin: "https://relay.test:18447"}
    end)

    result = create(c)
    assert result.state == "ready"
    assert result.share_url =~ ~r|\Ahttps://relay\.test:18447/join#[0-9a-f]{64}\z|
    assert byte_size(result.share_url) < 512
    assert result.full_url =~ "harklight://join#"
    assert decode_link(result.full_url)["person_id"] == c.person
    refute inspect(result) =~ "join#"
    assert {:ok, encoded} = JSON.decode_line(InvitationHandoff.encode_result(result) <> "\n")
    assert encoded["share_url"] == result.share_url
    assert encoded["full_url"] == result.full_url
    assert map_size(:sys.get_state(c.relay).store.engine.join_invitations) == 1
    assert :sys.get_state(c.relay).store.engine.sender_grants == %{}
  end

  test "explicit CLI handoff is a direct JSON product result and never enters generic logs", c do
    logs =
      capture_log(fn ->
        output =
          capture_io(fn ->
            AttentionServer.CLI.Owner.run([
              "invite",
              "--handoff",
              "--request-id",
              @request,
              "--connect",
              "127.0.0.1",
              "--name",
              "relay.test",
              "--port",
              to_string(c.endpoint.port),
              "--ca",
              c.endpoint.ca_file,
              "--cert",
              Path.join(c.certs, "a1.pem"),
              "--key",
              Path.join(c.certs, "a1.key"),
              "--person",
              c.person
            ])
          end)

        assert {:ok, %{"state" => "ready", "share_url" => url, "account" => person}} =
                 JSON.decode_line(output)

        assert person == c.person
        assert decode_link(url)["person_id"] == person
        refute output =~ "Invitation saved to"
      end)

    refute logs =~ "harklight://join#"
    refute logs =~ "invite_token"
  end

  test "concurrent request retries return the original link without duplicate active invitations",
       c do
    tasks = for _ <- 1..4, do: Task.async(fn -> create(c) end)
    results = Enum.map(tasks, &Task.await/1)
    assert Enum.all?(results, &(&1.state == "ready"))
    assert length(Enum.uniq(Enum.map(results, &{&1.share_url, &1.expires_at}))) == 1
    engine = :sys.get_state(c.relay).store.engine
    assert map_size(engine.join_invitations) == 1
    invitation = decode_link(hd(results).share_url)
    refute :erlang.term_to_binary(engine) =~ invitation["invite_token"]
    raw = Base.decode16!(invitation["invite_token"], case: :lower)
    refute :erlang.term_to_binary(engine) =~ raw
    assert engine.sender_grants == %{}
  end

  test "a committed but discarded reply recovers the identical handoff after SQLite restart", c do
    {:ok, socket} =
      HeadlessTLS.connect(
        c.endpoint,
        {Path.join(c.certs, "a1.pem"), Path.join(c.certs, "a1.key")}
      )

    :ok =
      :ssl.send(
        socket,
        JSON.encode!(%{
          "v" => 1,
          "type" => "invite_create",
          "person_id" => c.person,
          "request_id" => @request
        }) <> "\n"
      )

    await_invitation(c.relay, System.monotonic_time(:millisecond) + 3000)
    # Discard the server response; the following call must recover, not create.
    :ssl.close(socket)
    original = create(c)
    stop_supervised(EnrollmentHTTPS)
    stop_supervised(OwnerTLS)
    stop_supervised(Relay)
    relay = start_supervised!({Relay, store_path: c.path, join_configuration: c.config})
    owner = start_supervised!({OwnerTLS, owner_options(c.certs, relay)})

    restarted = %{
      c
      | relay: relay,
        owner: owner,
        endpoint: %{c.endpoint | port: OwnerTLS.port(owner)}
    }

    assert create(restarted) == original
    assert map_size(:sys.get_state(relay).store.engine.join_invitations) == 1
  end

  test "wrong person or enrolled-device credentials cannot issue an owner invitation", c do
    assert create(%{c | person: c.other}).state == "refused"
    cert = TLSFixtures.certificate_der!(c.certs, "a2")
    {:ok, _} = Relay.setup_device_certificate(c.relay, c.person, cert)

    result =
      InvitationHandoff.create(
        c.endpoint,
        Path.join(c.certs, "a2.pem"),
        Path.join(c.certs, "a2.key"),
        c.person,
        @request
      )

    refute result.state == "ready"
    assert result.share_url == nil
    assert :sys.get_state(c.relay).store.engine.join_invitations == %{}
  end

  test "transport failure honestly preserves the request ID for recovery without a fake invitation",
       c do
    {:ok, listener} = :gen_tcp.listen(0, active: false)
    {:ok, {_, port}} = :inet.sockname(listener)
    :gen_tcp.close(listener)
    result = create(%{c | endpoint: %{c.endpoint | port: port}})
    assert result.state == "unconfirmed"
    assert result.request_id == @request
    assert result.share_url == nil
    assert result.recovery =~ "same request UUID"
    assert :sys.get_state(c.relay).store.engine.join_invitations == %{}
  end

  test "request expiry cannot extend or resurrect a shared link and a changed destination refuses",
       c do
    {:ok, engine} = Engine.operator_bootstrap(Engine.new(), @person, @owner)

    {:ok, issued, invite, token} =
      GuidedJoin.create_for_request(engine, @person, @owner, @request, c.config, 100)

    assert {:ok, ^issued, ^invite, ^token} =
             GuidedJoin.create_for_request(issued, @person, @owner, @request, c.config, 101)

    {:ok, changed} = GuidedJoin.configuration("other.test", 18444, 18443, c.config["ca_pem"])

    assert {:error, :request_conflict} =
             GuidedJoin.create_for_request(issued, @person, @owner, @request, changed, 102)

    assert {:error, :expired, observed} =
             GuidedJoin.create_for_request(
               issued,
               @person,
               @owner,
               @request,
               c.config,
               invite.expires_at
             )

    assert {:error, _} =
             GuidedJoin.create_for_request(observed, @person, @owner, @request, c.config, 101)

    reclaimed = GuidedJoin.compact(observed, invite.retain_until)

    assert {:ok, _, _, replacement} =
             GuidedJoin.create_for_request(
               reclaimed,
               @person,
               @owner,
               @request,
               c.config,
               invite.retain_until
             )

    refute token == replacement
    {:ok, recovered} = Engine.operator_recover(issued, @person, String.duplicate("c", 64))

    assert {:error, :forbidden} =
             GuidedJoin.create_for_request(recovered, @person, @owner, @request, c.config, 102)
  end

  test "stale SQLite writer cannot return an issued handoff", c do
    {:ok, stale} = Relay.start_link(store_path: c.path, join_configuration: c.config)
    on_exit(fn -> if Process.alive?(stale), do: GenServer.stop(stale) end)

    {:ok, owner_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a1"))

    assert create(c).state == "ready"

    assert {:error, :storage_unavailable} =
             Relay.owner_invite_create(stale, c.person, owner_key, @request)

    assert map_size(:sys.get_state(c.relay).store.engine.join_invitations) == 1
  end

  test "format14 invitation survives request-ID migration without changing its bearer or grants",
       c do
    path = Path.join(c.dir, "legacy.sqlite3")
    {:ok, empty} = Store.open(path)
    Store.close(empty)
    {:ok, engine} = Engine.operator_bootstrap(Engine.new(), @person, @owner)
    token = :crypto.strong_rand_bytes(32)
    hash = :crypto.hash(:sha256, token)
    {:ok, engine, invitation} = GuidedJoin.create(engine, @person, @owner, hash, c.config, 100)
    legacy_invitation = Map.delete(invitation, :request_id)
    legacy = %{engine | join_invitations: %{hash => legacy_invitation}}
    payload = :erlang.term_to_binary({:attention_relay_state, 14, legacy}, [{:compressed, 6}])
    {:ok, db} = Exqlite.Sqlite3.open(path)

    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, "UPDATE relay_state SET payload=?, checksum=? WHERE id=1")

    :ok = Exqlite.Sqlite3.bind(stmt, [{:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}])
    :done = Exqlite.Sqlite3.step(db, stmt)
    :ok = Exqlite.Sqlite3.release(db, stmt)
    :ok = Exqlite.Sqlite3.close(db)
    {:ok, loaded} = Store.open(path)
    assert Map.delete(loaded.engine.join_invitations[hash], :request_id) == legacy_invitation
    assert loaded.engine.owners == legacy.owners
    assert loaded.engine.sender_grants == legacy.sender_grants

    assert {:ok, pending, %{"state" => "pending"}} =
             GuidedJoin.redeem(
               loaded.engine,
               hash,
               TLSFixtures.certificate_der!(c.certs, "a2"),
               "legacy joining device",
               :crypto.hash(:sha256, "legacy-status"),
               "ABCDEFGH23",
               c.config,
               101
             )

    assert pending.pending_enrollments["ABCDEFGH23"].person == @person
    assert pending.sender_grants == %{}
    Store.close(loaded)
  end

  defp create(c),
    do:
      InvitationHandoff.create(
        c.endpoint,
        Path.join(c.certs, "a1.pem"),
        Path.join(c.certs, "a1.key"),
        c.person,
        @request
      )

  defp decode_link(url) do
    bytes = URI.parse(url).fragment |> Base.url_decode64!(padding: false)
    {:ok, invitation} = JSON.decode_line(bytes <> "\n")
    invitation
  end

  defp owner_options(certs, relay),
    do: [
      relay: relay,
      certfile: Path.join(certs, "server.pem"),
      keyfile: Path.join(certs, "server.key"),
      cacertfile: Path.join(certs, "ca.pem")
    ]

  defp await_invitation(relay, deadline) do
    if map_size(:sys.get_state(relay).store.engine.join_invitations) == 0 do
      if System.monotonic_time(:millisecond) >= deadline, do: flunk("owner action did not commit")
      Process.sleep(10)
      await_invitation(relay, deadline)
    end
  end
end
