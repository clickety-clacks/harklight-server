defmodule AttentionServer.GuidedJoinHTTPSTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    DeviceIdentity,
    Engine,
    EnrollmentHTTPS,
    GuidedJoin,
    JSON,
    OwnerTLS,
    Relay,
    Store,
    TLSFixtures
  }

  setup_all do
    certs = TLSFixtures.create!()
    TLSFixtures.make_self_signed_client!(certs, "self_device")
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "guided-https-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")

    {:ok, config} =
      GuidedJoin.configuration("relay.test", 18444, 18443, File.read!(Path.join(certs, "ca.pem")))

    relay = start_supervised!({Relay, store_path: path, join_configuration: config})
    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))
    {:ok, other} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "b1"))

    owner =
      start_supervised!(
        {OwnerTLS,
         relay: relay,
         certfile: Path.join(certs, "server.pem"),
         keyfile: Path.join(certs, "server.key"),
         cacertfile: Path.join(certs, "ca.pem")}
      )

    listener =
      start_supervised!(
        {EnrollmentHTTPS,
         relay: relay,
         cacertfile: Path.join(certs, "ca.pem"),
         certfile: Path.join(certs, "server.pem"),
         keyfile: Path.join(certs, "server.key")}
      )

    %{
      dir: dir,
      path: path,
      config: config,
      relay: relay,
      owner: owner,
      listener: listener,
      person: person,
      other: other
    }
  end

  test "owner CLI writes a usable pinned invitation; lost HTTP reply retries exactly and approval grants no sender",
       c do
    output = Path.join(c.dir, "invite.json")

    AttentionServer.CLI.Owner.run([
      "invite",
      "--connect",
      "127.0.0.1",
      "--name",
      "relay.test",
      "--port",
      Integer.to_string(OwnerTLS.port(c.owner)),
      "--ca",
      Path.join(c.certs, "ca.pem"),
      "--cert",
      Path.join(c.certs, "a1.pem"),
      "--key",
      Path.join(c.certs, "a1.key"),
      "--person",
      c.person,
      "--output",
      output
    ])

    {:ok, invitation} = JSON.decode_line(File.read!(output))
    assert Map.take(invitation, Map.keys(c.config)) == c.config
    assert invitation["person_id"] == c.person
    payload = URI.parse(invitation["join_url"]).fragment |> Base.url_decode64!(padding: false)
    assert {:ok, decoded} = JSON.decode_line(payload <> "\n")
    assert decoded == Map.delete(invitation, "join_url")
    secret = String.duplicate("a", 64)

    request = %{
      "invite_token" => invitation["invite_token"],
      "status_secret" => secret,
      "public_certificate" => File.read!(Path.join(c.certs, "a2.pem"))
    }

    post(c, "/enroll", request, :drop)
    wait_pending(c.relay, System.monotonic_time(:millisecond) + 3_000)
    [{code, original}] = Map.to_list(:sys.get_state(c.relay).store.engine.pending_enrollments)
    assert original.person == c.person
    assert {200, reply} = post(c, "/enroll", request)
    assert reply["approval_code"] == code
    assert reply["expires_at"] == original.expires_at
    assert {200, ^reply} = post(c, "/enroll", Map.put(request, "label", "retry label"))

    assert {409, %{"error" => "used"}} =
             post(c, "/enroll", %{request | "status_secret" => String.duplicate("b", 64)})

    assert {400, %{"error" => "invalid_request"}} =
             post(c, "/enroll", Map.put(request, "person_id", c.other))

    assert {200, %{"state" => "pending"}} =
             post(c, "/enroll/status", %{"status_secret" => secret})

    assert %{"error" => "not_found"} =
             owner(c, "b1", %{
               "type" => "pending_lookup",
               "person_id" => c.other,
               "approval_code" => code
             })

    approval = %{
      "type" => "pending_approve",
      "approval_code" => code,
      "expected_device_id" => original.key,
      "expected_expires_at" => original.expires_at,
      "sender_grants" => ["app:unrequested"]
    }

    assert %{"error" => "not_found"} = owner(c, "a1", approval)

    assert %{"ok" => true, "device_id" => device} =
             owner(c, "a1", %{approval | "sender_grants" => []})

    assert device == original.key

    assert {200, %{"state" => "approved", "person_id" => person, "device_id" => ^device}} =
             post(c, "/enroll/status", %{"status_secret" => secret})

    assert person == c.person
    assert :sys.get_state(c.relay).store.engine.sender_grants == %{}
    assert {:ok, _cursor} = Relay.acknowledged_cursor(c.relay, device)

    # A long-offline phone can read its approved result after the old status
    # horizon; a historical status query still ends at that horizon.
    state = :sys.get_state(c.relay)
    receipt = state.store.engine.pending_history[code]
    old_deadline = System.system_time(:millisecond) - 2 * 86_400_000

    aged = %{
      state.store.engine
      | pending_history:
          Map.put(state.store.engine.pending_history, code, %{
            receipt
            | expires_at: old_deadline,
              status_expires_at: old_deadline + 86_400_000
          })
    }

    {:ok, committed} =
      Store.commit(state.store, aged, %{
        person: c.person,
        actor: "fixture",
        action: "approval_result_past_status_horizon"
      })

    :sys.replace_state(c.relay, fn s -> %{s | store: committed} end)

    assert {404, %{"error" => "not_found"}} =
             post(c, "/enroll/status", %{"status_secret" => secret})

    assert {:ok, _other_request} =
             Relay.pending_enroll(
               c.relay,
               TLSFixtures.certificate_der!(c.certs, "unknown"),
               "other"
             )

    assert {200,
            %{
              "state" => "approved",
              "person_id" => ^person,
              "device_id" => ^device,
              "approval_code" => ^code,
              "request_generation" => 1,
              "expires_at" => ^old_deadline
            }} =
             post(c, "/enroll/resume", %{
               "status_secret" => secret,
               "public_certificate" => File.read!(Path.join(c.certs, "a2.pem"))
             })

    assert {:ok, persisted} = Store.open(c.path)
    assert persisted.engine.pending_history[code].joined
    Store.close(persisted)

    {:ok, owner_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a1"))

    assert :ok = Relay.owner_revoke_device(c.relay, c.person, owner_key, device)

    assert {200, %{"state" => "revoked"}} =
             post(c, "/enroll/resume", %{
               "status_secret" => secret,
               "public_certificate" => File.read!(Path.join(c.certs, "a2.pem"))
             })
  end

  test "invitation refuses caller-chosen trust and honest declined/expired states remain durable",
       c do
    assert %{"error" => "invalid_request"} =
             owner(c, "a1", %{
               "type" => "invite_create",
               "enrollment_url" => "https://other.test/enroll"
             })

    assert %{"ok" => true, "invitation" => invitation} =
             owner(c, "a1", %{"type" => "invite_create"})

    secret = String.duplicate("c", 64)

    request = %{
      "invite_token" => invitation["invite_token"],
      "status_secret" => secret,
      "public_certificate" => File.read!(Path.join(c.certs, "a2.pem")),
      "label" => "phone"
    }

    assert {200, %{"approval_code" => code, "expires_at" => deadline}} =
             post(c, "/enroll", request)

    {:ok, key} = DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a2"))

    decline = %{
      "type" => "pending_decline",
      "approval_code" => code,
      "expected_device_id" => key,
      "expected_expires_at" => deadline
    }

    assert %{"error" => "not_found"} = owner(c, "b1", Map.put(decline, "person_id", c.other))
    assert %{"ok" => true} = owner(c, "a1", decline)

    assert {200, %{"state" => "declined"}} =
             post(c, "/enroll/status", %{"status_secret" => secret})

    assert {200, %{"state" => "declined"}} = post(c, "/enroll", request)
    assert :sys.get_state(c.relay).store.engine.devices == %{}
    {:ok, stored} = Store.open(c.path)
    {:ok, raw_secret} = Base.decode16(secret, case: :lower)

    assert {:ok, %{"state" => "declined"}} =
             Engine.pending_status(
               stored.engine,
               :crypto.hash(:sha256, raw_secret),
               System.system_time(:millisecond)
             )

    Store.close(stored)
    assert %{"ok" => true, "invitation" => expired} = owner(c, "a1", %{"type" => "invite_create"})
    {:ok, token} = Base.decode16(expired["invite_token"], case: :lower)
    hash = :crypto.hash(:sha256, token)
    state = :sys.get_state(c.relay)
    invite = state.store.engine.join_invitations[hash]

    engine = %{
      state.store.engine
      | join_invitations:
          Map.put(state.store.engine.join_invitations, hash, %{
            invite
            | expires_at: System.system_time(:millisecond) - 1
          })
    }

    {:ok, updated} =
      Store.commit(state.store, engine, %{
        person: c.person,
        actor: "fixture",
        action: "expire_invite"
      })

    :sys.replace_state(c.relay, fn s -> %{s | store: updated} end)

    assert {410, %{"error" => "expired"}} =
             post(c, "/enroll", %{request | "invite_token" => expired["invite_token"]})

    assert {404, %{"error" => "refused"}} =
             post(c, "/enroll", %{request | "invite_token" => "not-an-invite"})

    assert map_size(:sys.get_state(c.relay).store.engine.pending_history) == 1
  end

  test "exact requester cancellation survives restart and prevents a later approval", c do
    {:ok, owner_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a1"))

    {:ok, invitation} = Relay.owner_invite_create(c.relay, c.person, owner_key)
    secret = String.duplicate("d", 64)
    {:ok, key} = DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a2"))

    assert {200, %{"approval_code" => code, "expires_at" => deadline}} =
             post(c, "/enroll", %{
               "invite_token" => invitation["invite_token"],
               "status_secret" => secret,
               "public_certificate" => File.read!(Path.join(c.certs, "a2.pem"))
             })

    cancel = %{
      "status_secret" => secret,
      "approval_code" => code,
      "expected_device_id" => key,
      "expected_expires_at" => deadline
    }

    assert {404, %{"error" => "not_found"}} =
             post(c, "/enroll/cancel", %{cancel | "status_secret" => String.duplicate("e", 64)})

    assert {409, %{"error" => "stale_request", "current" => %{"approval_code" => ^code}}} =
             post(c, "/enroll/cancel", %{cancel | "expected_expires_at" => deadline + 1})

    assert {200, %{"state" => "cancelled"}} = post(c, "/enroll/cancel", cancel)
    assert {200, %{"state" => "cancelled"}} = post(c, "/enroll/cancel", cancel)

    assert {200, %{"state" => "cancelled"}} =
             post(c, "/enroll/status", %{"status_secret" => secret})

    assert {:error, :cancelled} =
             Relay.owner_pending_approve(c.relay, c.person, owner_key, code, key, deadline, [])

    {:ok, saved} = Store.open(c.path)
    assert saved.engine.devices == %{}
    Store.close(saved)
    stop_supervised(EnrollmentHTTPS)
    stop_supervised(OwnerTLS)
    stop_supervised(Relay)
    {:ok, restarted} = Relay.start_link(store_path: c.path, join_configuration: c.config)
    on_exit(fn -> if Process.alive?(restarted), do: GenServer.stop(restarted) end)

    assert {:ok, %{"state" => "cancelled"}} =
             Relay.pending_cancel(restarted, secret, code, key, deadline)

    assert {:error, :cancelled} =
             Relay.owner_pending_approve(restarted, c.person, owner_key, code, key, deadline, [])
  end

  test "expired guided request resumes once under the original person and key, then exact owner approval",
       c do
    {:ok, owner_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a1"))

    cert = TLSFixtures.certificate_der!(c.certs, "a2")
    pem = File.read!(Path.join(c.certs, "a2.pem"))
    {:ok, device_key} = DeviceIdentity.from_certificate(cert)
    {:ok, invitation} = Relay.owner_invite_create(c.relay, c.person, owner_key)
    secret = String.duplicate("7", 64)

    assert {200, %{"approval_code" => old_code, "expires_at" => original_deadline}} =
             post(c, "/enroll", %{
               "invite_token" => invitation["invite_token"],
               "status_secret" => secret,
               "public_certificate" => pem
             })

    assert original_deadline - System.system_time(:millisecond) > 86_390_000

    state = :sys.get_state(c.relay)
    old = state.store.engine.pending_enrollments[old_code]
    expired_at = System.system_time(:millisecond) - 2 * 86_400_000

    changed = %{
      state.store.engine
      | pending_enrollments:
          Map.put(state.store.engine.pending_enrollments, old_code, %{
            old
            | expires_at: expired_at,
              status_expires_at: expired_at + 86_400_000
          })
    }

    {:ok, committed} =
      Store.commit(state.store, changed, %{
        person: c.person,
        actor: "fixture",
        action: "past_ordinary_approval_deadline"
      })

    :sys.replace_state(c.relay, fn s -> %{s | store: committed} end)

    assert {404, %{"error" => "not_found"}} =
             post(c, "/enroll/status", %{"status_secret" => secret})

    request = %{"status_secret" => secret, "public_certificate" => pem}

    assert {404, %{"error" => "not_found"}} =
             post(c, "/enroll/resume", %{
               request
               | "public_certificate" => File.read!(Path.join(c.certs, "b1.pem"))
             })

    assert :ok = post(c, "/enroll/resume", request, :drop)
    wait_generation(c.relay, 2, System.monotonic_time(:millisecond) + 3_000)
    assert {200, resumed} = post(c, "/enroll/resume", request)
    assert resumed["state"] == "pending"
    assert resumed["person_id"] == c.person
    assert resumed["device_id"] == device_key
    assert resumed["request_generation"] == 2
    assert resumed["approval_code"] != old_code
    assert resumed["expires_at"] - System.system_time(:millisecond) > 86_390_000
    assert {200, ^resumed} = post(c, "/enroll/resume", request)

    assert {200, ^resumed} =
             post(c, "/enroll/resume", Map.put(request, "label", "not an authoritative rename"))

    assert {400, %{"error" => "invalid_request"}} =
             post(c, "/enroll/resume", Map.put(request, "person_id", c.other))

    assert {400, %{"error" => "invalid_request"}} =
             post(c, "/enroll/resume", %{request | "public_certificate" => "not a certificate"})

    assert map_size(:sys.get_state(c.relay).store.engine.pending_enrollments) == 1

    assert {409,
            %{
              "error" => "stale_request",
              "current" => %{"approval_code" => current_code, "request_generation" => 2}
            }} =
             post(c, "/enroll/cancel", %{
               "status_secret" => secret,
               "approval_code" => old_code,
               "expected_device_id" => device_key,
               "expected_expires_at" => expired_at
             })

    assert current_code == resumed["approval_code"]

    assert {:error, :not_found} =
             Relay.owner_pending_approve(
               c.relay,
               c.person,
               owner_key,
               old_code,
               device_key,
               expired_at,
               []
             )

    {:ok, saved} = Store.open(c.path)
    assert saved.engine.pending_enrollments[resumed["approval_code"]].person == c.person
    Store.close(saved)

    assert %{"ok" => true, "device_id" => ^device_key} =
             owner(c, "a1", %{
               "type" => "pending_approve",
               "approval_code" => resumed["approval_code"],
               "expected_device_id" => device_key,
               "expected_expires_at" => resumed["expires_at"],
               "sender_grants" => []
             })

    assert :sys.get_state(c.relay).store.engine.sender_grants == %{}
  end

  test "saved cancellation without the first reply reconciles the exact tuple after expiry and renewal",
       c do
    {:ok, owner_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a1"))

    cert = TLSFixtures.certificate_der!(c.certs, "a2")
    pem = File.read!(Path.join(c.certs, "a2.pem"))
    {:ok, device_key} = DeviceIdentity.from_certificate(cert)
    {:ok, invitation} = Relay.owner_invite_create(c.relay, c.person, owner_key)
    secret = String.duplicate("6", 64)

    assert {200, %{"approval_code" => old_code}} =
             post(c, "/enroll", %{
               "invite_token" => invitation["invite_token"],
               "status_secret" => secret,
               "public_certificate" => pem
             })

    state = :sys.get_state(c.relay)
    old = state.store.engine.pending_enrollments[old_code]
    expired_at = System.system_time(:millisecond) - 2 * 86_400_000

    aged = %{
      state.store.engine
      | pending_enrollments:
          Map.put(state.store.engine.pending_enrollments, old_code, %{
            old
            | expires_at: expired_at,
              status_expires_at: expired_at + 86_400_000
          })
    }

    {:ok, committed} =
      Store.commit(state.store, aged, %{
        person: c.person,
        actor: "fixture",
        action: "cancellation_after_lost_first_reply"
      })

    :sys.replace_state(c.relay, fn s -> %{s | store: committed} end)

    assert {404, %{"error" => "not_found"}} =
             post(c, "/enroll/status", %{"status_secret" => secret})

    placeholder = %{
      "status_secret" => secret,
      "approval_code" => "AAAAAAAAAA",
      "expected_device_id" => device_key,
      "expected_expires_at" => 1
    }

    assert {409,
            %{
              "error" => "stale_request",
              "current" => %{
                "state" => "expired",
                "person_id" => person,
                "device_id" => ^device_key,
                "approval_code" => ^old_code,
                "expires_at" => ^expired_at,
                "request_generation" => 1
              }
            }} = post(c, "/enroll/cancel", placeholder)

    assert person == c.person
    assert :sys.get_state(c.relay).store.engine.pending_enrollments[old_code].state == :pending
    assert :sys.get_state(c.relay).store.engine.devices == %{}

    assert {200, %{"state" => "pending", "request_generation" => 2} = renewed} =
             post(c, "/enroll/resume", %{
               "status_secret" => secret,
               "public_certificate" => pem
             })

    assert {200, ^renewed} = post(c, "/enroll/status", %{"status_secret" => secret})

    assert {409, %{"error" => "stale_request", "current" => ^renewed}} =
             post(c, "/enroll/cancel", placeholder)

    assert {200, %{"state" => "cancelled"}} =
             post(c, "/enroll/cancel", %{
               placeholder
               | "approval_code" => renewed["approval_code"],
                 "expected_expires_at" => renewed["expires_at"]
             })

    assert {200, %{"state" => "cancelled"}} =
             post(c, "/enroll/resume", %{
               "status_secret" => secret,
               "public_certificate" => pem
             })

    assert {200, %{"state" => "cancelled"}} =
             post(c, "/enroll/status", %{"status_secret" => secret})

    assert {:error, :cancelled} =
             Relay.owner_pending_approve(
               c.relay,
               c.person,
               owner_key,
               renewed["approval_code"],
               device_key,
               renewed["expires_at"],
               []
             )

    {:ok, persisted} = Store.open(c.path)
    assert persisted.engine.devices == %{}
    assert persisted.engine.pending_history[renewed["approval_code"]].state == :cancelled
    Store.close(persisted)
  end

  test "expired request cancellation remains terminal and a stale cancellation cannot target its successor",
       c do
    {:ok, owner_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a1"))

    cert = TLSFixtures.certificate_der!(c.certs, "a2")
    {:ok, device_key} = DeviceIdentity.from_certificate(cert)
    {:ok, invitation} = Relay.owner_invite_create(c.relay, c.person, owner_key)
    secret = String.duplicate("8", 64)

    {:ok, first} =
      Relay.invited_enroll(c.relay, invitation["invite_token"], cert, "phone", secret)

    old_code = first["approval_code"]
    state = :sys.get_state(c.relay)
    item = state.store.engine.pending_enrollments[old_code]
    expired_at = System.system_time(:millisecond) - 1

    {:ok, committed} =
      Store.commit(
        state.store,
        %{
          state.store.engine
          | pending_enrollments:
              Map.put(state.store.engine.pending_enrollments, old_code, %{
                item
                | expires_at: expired_at,
                  status_expires_at: expired_at + 86_400_000
              })
        },
        %{person: c.person, actor: "fixture", action: "past_approval_deadline"}
      )

    :sys.replace_state(c.relay, fn s -> %{s | store: committed} end)

    assert {:ok, %{"state" => "cancelled"}} =
             Relay.pending_cancel(c.relay, secret, old_code, device_key, expired_at)

    assert {:ok, %{"state" => "cancelled"}} = Relay.pending_resume(c.relay, secret, cert)
    assert :sys.get_state(c.relay).store.engine.devices == %{}
  end

  test "format-15 expired history resumes only when its consumed invitation proves the original person",
       c do
    {:ok, owner_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a1"))

    cert = TLSFixtures.certificate_der!(c.certs, "a2")
    {:ok, key} = DeviceIdentity.from_certificate(cert)
    {:ok, invitation} = Relay.owner_invite_create(c.relay, c.person, owner_key)
    secret = String.duplicate("9", 64)

    {:ok, reply} =
      Relay.invited_enroll(c.relay, invitation["invite_token"], cert, "phone", secret)

    code = reply["approval_code"]
    engine = :sys.get_state(c.relay).store.engine
    item = engine.pending_enrollments[code]
    past = System.system_time(:millisecond) - 1

    old = %{
      engine
      | pending_enrollments: %{},
        pending_history: %{
          code => %{
            state: :expired,
            key: key,
            secret_hash: item.secret_hash,
            expires_at: past,
            status_expires_at: past + 86_400_000
          }
        }
    }

    stop_supervised(EnrollmentHTTPS)
    stop_supervised(OwnerTLS)
    stop_supervised(Relay)
    payload = :erlang.term_to_binary({:attention_relay_state, 15, old}, [{:compressed, 6}])
    {:ok, db} = Exqlite.Sqlite3.open(c.path)

    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ? WHERE id = 1")

    :ok = Exqlite.Sqlite3.bind(stmt, [{:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}])
    :done = Exqlite.Sqlite3.step(db, stmt)
    :ok = Exqlite.Sqlite3.release(db, stmt)
    :ok = Exqlite.Sqlite3.close(db)

    {:ok, migrated} = Store.open(c.path)
    recovered = migrated.engine.pending_enrollments[code]
    assert recovered.person == c.person
    assert recovered.key == key
    assert recovered.certificate == nil
    assert recovered.request_generation == 1
    assert migrated.engine.pending_history == %{}
    Store.close(migrated)

    {:ok, resumed_relay} = Relay.start_link(store_path: c.path, join_configuration: c.config)
    on_exit(fn -> if Process.alive?(resumed_relay), do: GenServer.stop(resumed_relay) end)

    assert {:ok, %{"state" => "pending", "person_id" => person, "request_generation" => 2}} =
             Relay.pending_resume(resumed_relay, secret, cert)

    assert person == c.person

    assert {:error, :not_found} =
             Relay.pending_resume(
               resumed_relay,
               secret,
               TLSFixtures.certificate_der!(c.certs, "b1")
             )
  end

  test "serialized approval/cancel race has one durable truthful result", c do
    {:ok, owner_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a1"))

    cert = TLSFixtures.certificate_der!(c.certs, "a2")
    {:ok, key} = DeviceIdentity.from_certificate(cert)
    {:ok, invitation} = Relay.owner_invite_create(c.relay, c.person, owner_key)
    secret = String.duplicate("f", 64)
    {:ok, reply} = Relay.invited_enroll(c.relay, invitation["invite_token"], cert, "race", secret)
    code = reply["approval_code"]
    deadline = reply["expires_at"]
    cancel = Task.async(fn -> Relay.pending_cancel(c.relay, secret, code, key, deadline) end)

    approval =
      Task.async(fn ->
        Relay.owner_pending_approve(c.relay, c.person, owner_key, code, key, deadline, [])
      end)

    result = Task.await(cancel)
    outcome = Task.await(approval)

    assert {result, outcome} in [
             {{:ok, %{"state" => "cancelled"}}, {:error, :cancelled}},
             {{:ok, %{"state" => "approved", "person_id" => c.person, "device_id" => key}},
              {:ok, key}}
           ]

    {:ok, saved} = Store.open(c.path)

    assert {:ok, status} =
             Engine.pending_status(
               saved.engine,
               :crypto.hash(:sha256, Base.decode16!(secret, case: :lower)),
               System.system_time(:millisecond)
             )

    assert status == elem(result, 1)
    assert Map.has_key?(saved.engine.devices, key) == (status["state"] == "approved")
    assert saved.engine.sender_grants == %{}
    Store.close(saved)
  end

  test "only proved current device removes itself; lost-reply retry preserves others and revokes access",
       c do
    cert = TLSFixtures.certificate_der!(c.certs, "self_device")
    other_cert = TLSFixtures.certificate_der!(c.certs, "unknown")
    {:ok, key} = Relay.setup_device_certificate(c.relay, c.person, cert)
    {:ok, other_key} = Relay.setup_device_certificate(c.relay, c.other, other_cert)
    {:ok, session} = Relay.open_session(c.relay, key)
    request = %{"expected_device_id" => key}
    assert {404, %{"error" => "not_found"}} = post(c, "/device/remove", request)
    assert {404, %{"error" => "not_found"}} = post(c, "/device/remove", request, :read, "unknown")

    assert {400, %{"error" => "invalid_request"}} =
             post(
               c,
               "/device/remove",
               Map.put(request, "person_id", c.person),
               :read,
               "self_device"
             )

    assert {404, %{"error" => "not_found"}} = post(c, "/device/remove", request, :read, "a1")
    post(c, "/device/remove", request, :drop, "self_device")
    # Reading a second response proves retry after the first reply was lost.
    assert {200, removed} = post(c, "/device/remove", request, :read, "self_device")
    assert removed["state"] == "removed"
    assert removed["device_id"] == key
    assert removed["person_id"] == c.person
    assert removed["enrollment_revision"] == 2
    assert {:error, :forbidden} = Relay.acknowledged_cursor(c.relay, key)

    assert {:error, :unknown_session} =
             Relay.session_request_line(
               c.relay,
               session,
               JSON.encode!(%{"v" => 1, "type" => "resync", "since" => 0}) <> "\n"
             )

    assert {:ok, _} = Relay.acknowledged_cursor(c.relay, other_key)
    {:ok, saved} = Store.open(c.path)
    assert saved.engine.devices[key].revoked
    refute saved.engine.devices[other_key].revoked
    engine = saved.engine
    assert {:ok, ^engine, ^removed} = Engine.device_remove(engine, key)
    Store.close(saved)
    stop_supervised(EnrollmentHTTPS)
    stop_supervised(OwnerTLS)
    stop_supervised(Relay)
    restarted = start_supervised!({Relay, store_path: c.path, join_configuration: c.config})

    listener =
      start_supervised!(
        {EnrollmentHTTPS,
         relay: restarted,
         certfile: Path.join(c.certs, "server.pem"),
         keyfile: Path.join(c.certs, "server.key"),
         cacertfile: Path.join(c.certs, "ca.pem")}
      )

    assert {200, ^removed} =
             post(
               %{c | relay: restarted, listener: listener},
               "/device/remove",
               request,
               :read,
               "self_device"
             )

    assert {:error, :forbidden} = Relay.acknowledged_cursor(restarted, key)
  end

  defp owner(c, identity, request) do
    {:ok, socket} =
      :ssl.connect({127, 0, 0, 1}, OwnerTLS.port(c.owner),
        versions: [:"tlsv1.3"],
        verify: :verify_peer,
        active: false,
        mode: :binary,
        packet: :line,
        cacertfile: String.to_charlist(Path.join(c.certs, "ca.pem")),
        certfile: String.to_charlist(Path.join(c.certs, identity <> ".pem")),
        keyfile: String.to_charlist(Path.join(c.certs, identity <> ".key")),
        server_name_indication: ~c"relay.test",
        session_tickets: :disabled
      )

    frame = Map.merge(%{"v" => 1, "person_id" => c.person}, request)
    :ok = :ssl.send(socket, JSON.encode!(frame) <> "\n")
    {:ok, line} = :ssl.recv(socket, 0, 3_000)
    :ssl.close(socket)
    {:ok, reply} = JSON.decode_line(line)
    reply
  end

  defp post(c, path, request, mode \\ :read, identity \\ nil) do
    identity_opts =
      if identity,
        do: [
          certfile: String.to_charlist(Path.join(c.certs, identity <> ".pem")),
          keyfile: String.to_charlist(Path.join(c.certs, identity <> ".key"))
        ],
        else: []

    {:ok, socket} =
      :ssl.connect(
        {127, 0, 0, 1},
        EnrollmentHTTPS.port(c.listener),
        identity_opts ++
          [
            versions: [:"tlsv1.3"],
            verify: :verify_peer,
            active: false,
            mode: :binary,
            packet: :raw,
            cacertfile: String.to_charlist(Path.join(c.certs, "ca.pem")),
            server_name_indication: ~c"relay.test",
            session_tickets: :disabled
          ]
      )

    body = JSON.encode!(request)

    :ok =
      :ssl.send(socket, [
        "POST #{path} HTTP/1.1\r\nHost: relay.test\r\nContent-Type: application/json\r\n",
        "Content-Length: #{byte_size(body)}\r\nConnection: close\r\n\r\n",
        body
      ])

    if mode == :drop do
      :ssl.close(socket)
      :ok
    else
      bytes = receive_all(socket, <<>>)
      :ssl.close(socket)
      [header, response] = :binary.split(bytes, "\r\n\r\n")
      [first | _] = String.split(header, "\r\n")
      ["HTTP/1.1", status | _] = String.split(first, " ")
      {:ok, json} = JSON.decode_line(response <> "\n")
      {String.to_integer(status), json}
    end
  end

  defp receive_all(socket, bytes) do
    case :ssl.recv(socket, 0, 3_000) do
      {:ok, chunk} -> receive_all(socket, bytes <> chunk)
      {:error, :closed} -> bytes
    end
  end

  defp wait_pending(relay, deadline) do
    if map_size(:sys.get_state(relay).store.engine.pending_enrollments) == 0 do
      if System.monotonic_time(:millisecond) >= deadline,
        do: flunk("request did not commit before lost reply")

      Process.sleep(10)
      wait_pending(relay, deadline)
    else
      :ok
    end
  end

  defp wait_generation(relay, generation, deadline) do
    reached =
      Enum.any?(:sys.get_state(relay).store.engine.pending_enrollments, fn {_code, item} ->
        item.request_generation == generation
      end)

    unless reached do
      if System.monotonic_time(:millisecond) >= deadline,
        do: flunk("resume did not commit before lost reply")

      Process.sleep(10)
      wait_generation(relay, generation, deadline)
    end

    :ok
  end
end
