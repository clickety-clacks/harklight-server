defmodule AttentionServer.OriginHTTPSTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    DeviceIdentity,
    HeadlessTLS,
    JSON,
    Relay,
    Server,
    Store,
    TLSFixtures
  }

  alias Exqlite.Sqlite3

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  setup_all do
    certs = TLSFixtures.create!()
    TLSFixtures.make_self_signed_client!(certs, "reader")
    TLSFixtures.make_self_signed_client!(certs, "b2")
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "origin-https-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    store_path = Path.join(dir, "state.sqlite3")
    on_exit(fn -> File.rm_rf!(dir) end)

    {:ok, server} =
      Server.start_link(
        store_path: store_path,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        cacertfile: Path.join(certs, "ca.pem"),
        origin_port: 0,
        relay_name: :"origin_https_#{System.unique_integer([:positive])}"
      )

    Process.unlink(server)
    on_exit(fn -> if Process.alive?(server), do: Supervisor.stop(server) end)
    relay = Server.relay(server)
    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))
    {:ok, other} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "b1"))

    {:ok, origin} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "a2"))

    {:ok, reader} =
      Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(certs, "reader"))

    {:ok, other_origin} =
      Relay.setup_device_certificate(relay, other, TLSFixtures.certificate_der!(certs, "b2"))

    :ok = Relay.setup_sender(relay, origin, "app:test")
    :ok = Relay.setup_sender(relay, other_origin, "app:other")

    %{
      server: server,
      store_path: store_path,
      relay: relay,
      person: person,
      other_person: other,
      origin: origin,
      other_origin: other_origin,
      reader: reader,
      endpoint: %{
        address: {127, 0, 0, 1},
        name: "relay.test",
        ca_file: Path.join(certs, "ca.pem"),
        port: Server.origin_port(server)
      }
    }
  end

  test "focus reports are shape-only, nonterminal, and gated by the see switch", c do
    {:ok, owner_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a1"))

    assert {:ok, 200, %{"type" => "error", "reason" => "forbidden"}} =
             request(c, "reader", %{"v" => 1, "type" => "focus_get"})

    # Permission is checked before request fields for pushes.
    assert {:ok, 200, %{"type" => "error", "reason" => "forbidden"}} =
             request(c, "reader", %{"v" => 1, "type" => "view_show"})

    :ok =
      Relay.save_dashboard_focus_permission(c.relay, c.person, owner_key, c.reader, :see, true)

    {:ok, session} = Relay.open_session(c.relay, c.origin)

    malformed = %{
      "v" => 1,
      "type" => "focus_report",
      "ref" => "bad-screen",
      "screen" => "unknown"
    }

    assert {:ok, [line]} =
             Relay.session_request_line(c.relay, session, JSON.encode!(malformed) <> "\n")

    assert {:ok,
            %{
              "type" => "error",
              "reason" => "invalid_field",
              "field" => "screen",
              "ref" => "bad-screen"
            }} = JSON.decode_line(line)

    report = %{
      "v" => 1,
      "type" => "focus_report",
      "ref" => "focus-1",
      "screen" => "chat",
      "sender_id" => "app:unknown",
      "message_id" => "message-not-stored",
      "visible_event_id" => "4182"
    }

    assert {:ok, [line]} =
             Relay.session_request_line(c.relay, session, JSON.encode!(report) <> "\n")

    assert {:ok, %{"type" => "ok", "ref" => "focus-1"}} = JSON.decode_line(line)

    assert {:ok, 200,
            %{
              "type" => "ok",
              "clients" => [
                %{
                  "client_id" => client_id,
                  "device_name" => device_name,
                  "screen" => "chat",
                  "sender_id" => "app:unknown",
                  "message_id" => "message-not-stored",
                  "visible_event_id" => "4182",
                  "in_front" => true,
                  "connected" => true,
                  "reported_at" => reported_at
                }
              ]
            }} = request(c, "reader", %{"v" => 1, "type" => "focus_get"})

    assert client_id == c.origin
    assert device_name == "Device " <> String.slice(c.origin, 0, 8)
    assert is_integer(reported_at)

    assert {:ok, 200, %{"type" => "error", "reason" => "forbidden"}} =
             request(c, "reader", %{"v" => 1, "type" => "view_show"})

    :ok = Relay.close_session(c.relay, session)
  end

  test "focus SSE is independent, supports multiple streams, and closes when see is disabled",
       c do
    {:ok, owner_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a1"))

    :ok =
      Relay.save_dashboard_focus_permission(c.relay, c.person, owner_key, c.reader, :see, true)

    :ok = Relay.origin_stream_attach(c.relay, c.reader, :origin, self(), true)
    on_exit(fn -> Relay.origin_stream_detach(c.relay, c.reader, :origin, self()) end)

    assert {:ok, first, first_event} = focus_stream_request(c, "reader")
    assert first_event =~ "event: focus_change\n"
    assert first_event =~ "data: {\"clients\":[]}"

    assert {:ok, second, second_event} = focus_stream_request(c, "reader")
    assert second_event =~ "data: {\"clients\":[]}"

    on_exit(fn ->
      :ssl.close(first)
      :ssl.close(second)
    end)

    {:ok, session} = Relay.open_session(c.relay, c.origin)

    report = %{
      "v" => 1,
      "type" => "focus_report",
      "ref" => "stream-report",
      "screen" => "inbox"
    }

    assert {:ok, [_]} =
             Relay.session_request_line(c.relay, session, JSON.encode!(report) <> "\n")

    assert {:ok, first_update} = recv_until(first, "\n\n", <<>>, 20)
    assert first_update =~ "event: focus_change\n"
    assert first_update =~ c.origin

    assert {:ok, second_update} = recv_until(second, "\n\n", <<>>, 20)
    assert second_update =~ "event: focus_change\n"
    assert second_update =~ c.origin

    refute_receive :origin_scope_replaced, 20

    :ok =
      Relay.save_dashboard_focus_permission(c.relay, c.person, owner_key, c.reader, :see, false)

    assert {:error, :closed} = :ssl.recv(first, 0, 5_000)
    assert {:error, :closed} = :ssl.recv(second, 0, 5_000)

    assert {:ok, 200, %{"type" => "error", "reason" => "forbidden"}} =
             request(c, "reader", %{"v" => 1, "type" => "focus_get"})

    :ok = Relay.close_session(c.relay, session)
  end

  test "owner revocation closes every open focus stream for that agent", c do
    {:ok, owner_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a1"))

    :ok =
      Relay.save_dashboard_focus_permission(c.relay, c.person, owner_key, c.reader, :see, true)

    assert {:ok, first, _event} = focus_stream_request(c, "reader")
    assert {:ok, second, _event} = focus_stream_request(c, "reader")

    on_exit(fn ->
      :ssl.close(first)
      :ssl.close(second)
    end)

    :ok = Relay.owner_revoke_device(c.relay, c.person, owner_key, c.reader)

    assert {:error, :closed} = :ssl.recv(first, 0, 5_000)
    assert {:error, :closed} = :ssl.recv(second, 0, 5_000)

    assert {:error, :forbidden} = Relay.focus_get(c.relay, c.reader)
  end

  test "view_show is account-scoped, selects by device name, and delivers only the latest held show",
       c do
    {:ok, owner_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a1"))

    assert %{"type" => "ok"} =
             relay_request(c.relay, c.origin, %{
               "type" => "forward",
               "op_id" => op(951),
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "first",
                 "created_at" => 1,
                 "title" => "First"
               }
             })

    assert %{"type" => "ok"} =
             relay_request(c.relay, c.origin, %{
               "type" => "forward",
               "op_id" => op(952),
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "second",
                 "created_at" => 2,
                 "title" => "Second"
               }
             })

    assert %{"type" => "ok"} =
             relay_request(c.relay, c.other_origin, %{
               "type" => "forward",
               "op_id" => op(953),
               "message" => %{
                 "sender_id" => "app:other",
                 "message_id" => "other-only",
                 "created_at" => 3,
                 "title" => "Other account"
               }
             })

    :ok =
      Relay.save_dashboard_focus_permission(c.relay, c.person, owner_key, c.reader, :push, true)

    :ok =
      Relay.save_dashboard_focus_permission(c.relay, c.person, owner_key, c.reader, :see, true)

    assert {:ok, session} = Relay.open_session(c.relay, c.origin)

    report = %{
      "v" => 1,
      "type" => "focus_report",
      "ref" => "show-report",
      "screen" => "message",
      "sender_id" => "app:test",
      "message_id" => "first"
    }

    assert {:ok, [_]} =
             Relay.session_request_line(c.relay, session, JSON.encode!(report) <> "\n")

    device_name = "dEvIcE " <> String.upcase(String.slice(c.origin, 0, 8))
    expected_device_name = "Device " <> String.slice(c.origin, 0, 8)

    assert {:ok, 200,
            %{
              "type" => "ok",
              "client_id" => client_id,
              "device_name" => ^expected_device_name,
              "delivery" => "held"
            }} =
             request(c, "reader", %{
               "v" => 1,
               "type" => "view_show",
               "ref" => "show-1",
               "sender_id" => "app:test",
               "message_id" => "first",
               "target" => "message",
               "target_device_name" => device_name
             })

    assert client_id == c.origin

    origin_key = c.origin

    assert {:ok, 200, %{"type" => "ok", "client_id" => ^origin_key, "delivery" => "held"}} =
             request(c, "reader", %{
               "v" => 1,
               "type" => "view_show",
               "ref" => "show-2",
               "sender_id" => "app:test",
               "message_id" => "second",
               "target" => "chat",
               "target_client" => c.origin
             })

    assert {:ok, 200, %{"type" => "error", "reason" => "not_found", "field" => "message_id"}} =
             request(c, "reader", %{
               "v" => 1,
               "type" => "view_show",
               "sender_id" => "app:other",
               "message_id" => "other-only",
               "target" => "message",
               "target_client" => c.origin
             })

    assert {:ok, 200, %{"type" => "error", "reason" => "not_found", "field" => "target_client"}} =
             request(c, "reader", %{
               "v" => 1,
               "type" => "view_show",
               "sender_id" => "app:test",
               "message_id" => "first",
               "target" => "message",
               "target_client" => c.other_origin
             })

    :ok =
      Relay.save_dashboard_focus_permission(c.relay, c.person, owner_key, c.reader, :push, false)

    assert {:ok, 200, %{"type" => "error", "reason" => "forbidden"}} =
             request(c, "reader", %{
               "v" => 1,
               "type" => "view_show",
               "sender_id" => "app:test",
               "message_id" => "first",
               "target" => "message"
             })

    delivered = %{report | "ref" => "show-report-2", "screen" => "chat", "message_id" => "second"}

    assert {:ok, lines} =
             Relay.session_request_line(c.relay, session, JSON.encode!(delivered) <> "\n")

    assert [
             %{"type" => "ok", "ref" => "show-report-2"},
             %{
               "v" => 1,
               "type" => "view_show",
               "sender_id" => "app:test",
               "message_id" => "second",
               "target" => "chat",
               "held_ms" => held_ms
             }
           ] =
             Enum.map(lines, fn line ->
               {:ok, frame} = JSON.decode_line(line)
               frame
             end)

    assert held_ms >= 0

    assert {:ok, 200,
            %{
              "type" => "ok",
              "clients" => [
                %{"client_id" => ^origin_key, "screen" => "chat", "message_id" => "second"}
              ]
            }} = request(c, "reader", %{"v" => 1, "type" => "focus_get"})

    :ok = Relay.close_session(c.relay, session)
  end

  test "authenticated origin reports limits and an invalid SaaS cap only disables hosted routes",
       c do
    assert {:ok, 200,
            %{
              "type" => "ok",
              "inline_decoded_bytes_per_entry" => 262_144,
              "upload_cap_bytes_per_file" => "none"
            }} = request(c, "a2", %{"v" => 1, "type" => "file_limits"})

    assert {:error, _} = request(c, "unknown", %{"v" => 1, "type" => "file_limits"})

    opts = [
      store_path:
        Path.join(
          System.tmp_dir!(),
          "saas-file-limit-#{System.unique_integer([:positive])}.sqlite3"
        ),
      certfile: Path.join(c.certs, "server.pem"),
      keyfile: Path.join(c.certs, "server.key"),
      cacertfile: Path.join(c.certs, "ca.pem"),
      deployment_mode: :saas,
      origin_port: 0,
      relay_name: :"saas_file_limit_#{System.unique_integer([:positive])}"
    ]

    assert {:ok, {_, children}} = Server.init(Keyword.put(opts, :hosted_port, 0))
    refute Enum.any?(children, fn child_spec -> child_spec.id == :hosted_http end)

    assert {:ok, {_flags, _children}} =
             Server.init(Keyword.merge(opts, deployment_mode: :self_hosted, upload_cap: 0))

    {:ok, server} = Server.start_link(Keyword.put(opts, :upload_cap, 1_024))
    Process.unlink(server)
    on_exit(fn -> if Process.alive?(server), do: Supervisor.stop(server) end)

    {:ok, person} =
      Server.relay(server)
      |> Relay.operator_bootstrap(TLSFixtures.certificate_der!(c.certs, "a1"))

    {:ok, _origin} =
      Relay.setup_device_certificate(
        Server.relay(server),
        person,
        TLSFixtures.certificate_der!(c.certs, "a2")
      )

    endpoint = %{c.endpoint | port: Server.origin_port(server)}

    assert {:ok, 200,
            %{
              "type" => "ok",
              "inline_decoded_bytes_per_entry" => 262_144,
              "upload_cap_bytes_per_file" => 1_024
            }} = request(%{c | endpoint: endpoint}, "a2", %{"v" => 1, "type" => "file_limits"})

    assert {:ok, 413, %{"reason" => "upload_cap_exceeded", "upload_cap_bytes_per_file" => 1_024}} =
             upload_request(
               %{c | endpoint: endpoint},
               "a2",
               %{
                 op_id: op(700),
                 sender_id: "app:test",
                 message_id: "over-cap",
                 revision: 1,
                 name: "large.bin"
               },
               :binary.copy(<<1>>, 1_025)
             )
  end

  test "a large exact upload is invisible until its bound request commits and remains fetchable after restart",
       c do
    bytes = :binary.copy(<<0, 1, 255, 42>>, 530_000)
    hash = Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

    attrs = %{
      op_id: op(701),
      sender_id: "app:test",
      message_id: "large-file",
      revision: 1,
      name: "screen.bin"
    }

    assert {:ok, 200,
            %{
              "type" => "ok",
              "upload_id" => id,
              "size" => 2_120_000,
              "sha256" => ^hash,
              "duplicate" => false
            }} =
             upload_request(c, "a2", attrs, bytes)

    assert {:ok, 200, %{"upload_id" => ^id, "duplicate" => true}} =
             upload_request(c, "a2", attrs, bytes)

    assert {:ok, 404, %{"reason" => "not_found"}} = fetch_json(c, "reader", id)
    assert {:ok, 404, %{"reason" => "not_found"}} = fetch_json(c, "b2", id)

    ref = %{
      "name" => "screen.bin",
      "upload_id" => id,
      "size" => byte_size(bytes),
      "sha256" => hash
    }

    assert {:ok, 200, %{"type" => "ok", "seq" => 1}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "send",
               "op_id" => op(701),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "large-file",
                 "created_at" => 1,
                 "title" => "Please inspect"
               },
               "files" => [ref]
             })

    assert {:ok, 200, ^bytes} = fetch_bytes(c, "reader", id)
    assert {:ok, 404, %{"reason" => "not_found"}} = fetch_json(c, "b2", id)

    assert {:ok, 200, %{"type" => "ok", "entries" => [%{"files" => [stored]}]}} =
             request(c, "a2", %{"v" => 1, "type" => "feed", "since" => 0})

    assert stored == Map.put(ref, "tier", "upload")
    assert {:ok, [recipient], 1} = Relay.events(c.relay, c.reader, 0)
    assert recipient["files"] == [stored]

    Supervisor.stop(c.server)

    {:ok, restarted} =
      Server.start_link(
        store_path: c.store_path,
        certfile: Path.join(c.certs, "server.pem"),
        keyfile: Path.join(c.certs, "server.key"),
        cacertfile: Path.join(c.certs, "ca.pem"),
        origin_port: 0,
        relay_name: :"upload_restart_#{System.unique_integer([:positive])}"
      )

    Process.unlink(restarted)
    on_exit(fn -> if Process.alive?(restarted), do: Supervisor.stop(restarted) end)

    restored = %{
      c
      | server: restarted,
        relay: Server.relay(restarted),
        endpoint: %{c.endpoint | port: Server.origin_port(restarted)}
    }

    assert {:ok, 200, ^bytes} = fetch_bytes(restored, "reader", id)
  end

  test "committed attachments remain readable across same-person origin grants", c do
    bytes = <<0, 1, 255>>
    hash = Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

    attrs = %{
      op_id: op(702),
      sender_id: "app:test",
      message_id: "owned-file",
      revision: 1,
      name: "owned.bin"
    }

    assert {:ok, 200, %{"upload_id" => id}} = upload_request(c, "a2", attrs, bytes)

    assert {:ok, 200, %{"type" => "ok"}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "send",
               "op_id" => attrs.op_id,
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => attrs.sender_id,
                 "message_id" => attrs.message_id,
                 "created_at" => 1,
                 "title" => "File"
               },
               "files" => [
                 %{
                   "name" => attrs.name,
                   "upload_id" => id,
                   "size" => byte_size(bytes),
                   "sha256" => hash
                 }
               ]
             })

    assert {:ok, 200, ^bytes} = fetch_bytes(c, "reader", id)
    :ok = Relay.setup_sender(c.relay, c.reader, "app:reader")
    assert {:ok, 200, ^bytes} = fetch_bytes(c, "reader", id)
    assert {:ok, 200, ^bytes} = fetch_bytes(c, "a2", id)
    assert {:ok, 404, %{"reason" => "not_found"}} = fetch_json(c, "b2", id)

    assert {:ok, 200, %{"type" => "ok"}} =
             request(c, "reader", %{
               "v" => 1,
               "type" => "send",
               "op_id" => op(703),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:reader",
                 "message_id" => "reader-request",
                 "created_at" => 2,
                 "title" => "Own request"
               }
             })

    {:ok, owner_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "a1"))

    assert :ok = Relay.owner_revoke_sender(c.relay, c.person, owner_key, c.reader, "app:reader")
    assert {:ok, 200, ^bytes} = fetch_bytes(c, "reader", id)
    assert {:ok, 404, %{"reason" => "not_found"}} = fetch_json(c, "b2", id)
  end

  test "a missing file fails its fetch without stopping other requests after restart", c do
    bytes = <<1, 2, 3>>
    hash = Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

    attrs = %{
      op_id: op(703),
      sender_id: "app:test",
      message_id: "lost-file",
      revision: 1,
      name: "lost.bin"
    }

    assert {:ok, 200, %{"upload_id" => id}} = upload_request(c, "a2", attrs, bytes)

    assert {:ok, 200, %{"type" => "ok"}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "send",
               "op_id" => attrs.op_id,
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => attrs.sender_id,
                 "message_id" => attrs.message_id,
                 "created_at" => 1,
                 "title" => "File"
               },
               "files" => [
                 %{
                   "name" => attrs.name,
                   "upload_id" => id,
                   "size" => byte_size(bytes),
                   "sha256" => hash
                 }
               ]
             })

    Supervisor.stop(c.server)
    :ok = File.rm(Path.join(c.store_path <> ".uploads", id))

    {:ok, restarted} =
      Server.start_link(
        store_path: c.store_path,
        certfile: Path.join(c.certs, "server.pem"),
        keyfile: Path.join(c.certs, "server.key"),
        cacertfile: Path.join(c.certs, "ca.pem"),
        origin_port: 0,
        relay_name: :"missing_upload_restart_#{System.unique_integer([:positive])}"
      )

    Process.unlink(restarted)
    on_exit(fn -> if Process.alive?(restarted), do: Supervisor.stop(restarted) end)

    restored = %{
      c
      | server: restarted,
        relay: Server.relay(restarted),
        endpoint: %{c.endpoint | port: Server.origin_port(restarted)}
    }

    assert {:ok, 503, %{"reason" => "storage_unavailable"}} = fetch_json(restored, "reader", id)

    assert {:ok, 200, %{"type" => "ok", "cursor" => 1}} =
             request(restored, "a2", %{"v" => 1, "type" => "feed", "since" => 0})
  end

  test "an unsubmitted agent upload expires after one idle hour and needs real re-upload", c do
    bytes = <<0, 255, 42>>
    hash = Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

    attrs = %{
      op_id: op(720),
      sender_id: "app:test",
      message_id: "abandoned",
      revision: 1,
      name: "a.bin"
    }

    assert {:ok, 200, %{"upload_id" => old_id, "duplicate" => false}} =
             upload_request(c, "a2", attrs, bytes)

    old_path = Path.join(c.store_path <> ".uploads", old_id)
    assert File.read!(old_path) == bytes
    now = System.system_time(:millisecond)

    assert {:ok, 0} = Relay.sweep_uploads(c.relay, now + 59 * 60_000)
    assert File.exists?(old_path)
    Process.sleep(10)

    active_attrs = %{
      "op_id" => attrs.op_id,
      "sender_id" => attrs.sender_id,
      "message_id" => attrs.message_id,
      "revision" => attrs.revision,
      "name" => "later.bin"
    }

    assert {:ok, lease} = Relay.upload_begin(c.relay, c.origin, active_attrs, byte_size(bytes))
    assert {:ok, 0} = Relay.sweep_uploads(c.relay, now + 60 * 60_000 + 1)
    assert File.exists?(old_path)
    assert :ok = Relay.upload_end(c.relay, lease)
    assert {:ok, observed} = Store.open(c.store_path)
    activity = observed.engine.uploads[old_id].last_activity_ms
    assert activity > now
    assert :ok = Store.close(observed)
    assert {:ok, 0} = Relay.sweep_uploads(c.relay, activity + 60 * 60_000 - 1)
    assert {:ok, 1} = Relay.sweep_uploads(c.relay, activity + 60 * 60_000)
    refute File.exists?(old_path)

    old_ref = %{
      "name" => "a.bin",
      "upload_id" => old_id,
      "size" => byte_size(bytes),
      "sha256" => hash
    }

    assert {:ok, 200, %{"type" => "error", "field" => "files"}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "send",
               "op_id" => attrs.op_id,
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => attrs.sender_id,
                 "message_id" => attrs.message_id,
                 "created_at" => 1,
                 "title" => "Needs the file"
               },
               "files" => [old_ref]
             })

    assert {:ok, 200, %{"upload_id" => new_id, "duplicate" => false}} =
             upload_request(c, "a2", attrs, bytes)

    refute new_id == old_id

    assert {:ok, 200, %{"type" => "ok", "seq" => 1}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "send",
               "op_id" => attrs.op_id,
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => attrs.sender_id,
                 "message_id" => attrs.message_id,
                 "created_at" => 1,
                 "title" => "Needs the file"
               },
               "files" => [%{old_ref | "upload_id" => new_id}]
             })

    assert {:ok, 0} = Relay.sweep_uploads(c.relay, now + 2 * 60 * 60_000)
    assert File.read!(Path.join(c.store_path <> ".uploads", new_id)) == bytes
  end

  test "a client upload before its first message waits for explicit navigation cleanup", c do
    bytes = <<17, 18, 19>>

    attrs = %{
      op_id: op(727),
      sender_id: "app:client",
      message_id: "draft-before-entry",
      revision: 1,
      name: "draft.bin"
    }

    :ok = Relay.setup_sender(c.relay, c.reader, "app:other")

    assert {:ok, 200, %{"upload_id" => id, "duplicate" => false}} =
             upload_request(c, "reader", attrs, bytes)

    path = Path.join(c.store_path <> ".uploads", id)
    assert File.read!(path) == bytes

    assert {:ok, 0} =
             Relay.sweep_uploads(c.relay, System.system_time(:millisecond) + 2 * 60 * 60_000)

    assert File.exists?(path)

    assert {:ok, 200, %{"type" => "ok", "state" => "discarded", "discarded" => 1}} =
             request(c, "reader", %{
               "v" => 1,
               "type" => "discard_uploads",
               "op_id" => op(728),
               "sender_id" => attrs.sender_id,
               "message_id" => attrs.message_id,
               "revision" => attrs.revision
             })

    refute File.exists?(path)
  end

  test "upgrade preserves a legacy unsubmitted upload before its new idle deadline", c do
    attrs = %{
      op_id: op(723),
      sender_id: "app:test",
      message_id: "legacy-upload",
      revision: 1,
      name: "old.bin"
    }

    bytes = <<1, 2, 3>>
    assert {:ok, 200, %{"upload_id" => id}} = upload_request(c, "a2", attrs, bytes)
    path = Path.join(c.store_path <> ".uploads", id)
    Supervisor.stop(c.server)

    assert {:ok, before} = Store.open(c.store_path)
    prior_version = before.version

    legacy_uploads =
      Map.new(before.engine.uploads, fn {key, upload} ->
        {key, Map.delete(upload, :last_activity_ms)}
      end)

    legacy = %{before.engine | uploads: legacy_uploads}
    assert :ok = Store.close(before)

    payload = :erlang.term_to_binary({:attention_relay_state, 20, legacy}, [{:compressed, 6}])
    assert {:ok, db} = Sqlite3.open(c.store_path)
    assert {:ok, stmt} = Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ?")
    assert :ok = Sqlite3.bind(stmt, [{:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}])
    assert :done = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)

    {:ok, restarted} =
      Server.start_link(
        store_path: c.store_path,
        certfile: Path.join(c.certs, "server.pem"),
        keyfile: Path.join(c.certs, "server.key"),
        cacertfile: Path.join(c.certs, "ca.pem"),
        origin_port: 0,
        relay_name: :"upload_upgrade_#{System.unique_integer([:positive])}"
      )

    Process.unlink(restarted)
    on_exit(fn -> if Process.alive?(restarted), do: Supervisor.stop(restarted) end)

    assert File.read!(path) == bytes
    assert {:ok, migrated} = Store.open(c.store_path)
    assert migrated.version == prior_version + 1
    migrated_at = migrated.engine.uploads[id].last_activity_ms
    assert is_integer(migrated_at)
    assert :ok = Store.close(migrated)
    assert {:ok, 0} = Relay.sweep_uploads(Server.relay(restarted), migrated_at + 60 * 60_000 - 1)
    assert File.read!(path) == bytes
    assert {:ok, 1} = Relay.sweep_uploads(Server.relay(restarted), migrated_at + 60 * 60_000)
    refute File.exists?(path)
  end

  test "navigation discards only unsubmitted person uploads and fences an active transfer", c do
    assert {:ok, 200, %{"type" => "ok", "seq" => 1}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "send",
               "op_id" => op(721),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "navigation",
                 "created_at" => 1,
                 "title" => "Please respond"
               }
             })

    attrs = %{
      op_id: op(722),
      sender_id: "app:test",
      message_id: "navigation",
      revision: 1,
      name: "person.bin"
    }

    bytes = <<7, 8, 9>>
    hash = Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

    assert {:ok, 200, %{"upload_id" => old_id}} = upload_request(c, "reader", attrs, bytes)
    old_path = Path.join(c.store_path <> ".uploads", old_id)
    assert File.exists?(old_path)

    other_attrs = %{attrs | op_id: op(724), name: "other.bin"}

    assert {:ok, 200, %{"upload_id" => other_id}} =
             upload_request(c, "reader", other_attrs, bytes)

    other_path = Path.join(c.store_path <> ".uploads", other_id)
    assert File.exists?(other_path)

    # The agent's inactivity sweep does not stand in for a person's navigation.
    assert {:ok, 0} =
             Relay.sweep_uploads(c.relay, System.system_time(:millisecond) + 2 * 60 * 60_000)

    assert File.exists?(old_path)

    discard = %{
      "v" => 1,
      "type" => "discard_uploads",
      "op_id" => op(725),
      "sender_id" => attrs.sender_id,
      "message_id" => attrs.message_id,
      "revision" => attrs.revision
    }

    assert {:ok, 200, %{"type" => "ok", "state" => "discarded", "discarded" => 0}} =
             request(c, "a2", discard)

    assert File.exists?(old_path)
    assert File.exists?(other_path)

    active_attrs = %{
      "op_id" => op(726),
      "sender_id" => attrs.sender_id,
      "message_id" => attrs.message_id,
      "revision" => attrs.revision,
      "name" => "second.bin"
    }

    assert {:ok, lease} = Relay.upload_begin(c.relay, c.reader, active_attrs, byte_size(bytes))

    assert {:ok, 200, %{"type" => "ok", "state" => "discarded", "discarded" => 2}} =
             request(c, "reader", discard)

    refute File.exists?(old_path)
    refute File.exists?(other_path)

    assert {:ok, 200, %{"type" => "ok", "state" => "discarded", "discarded" => 0}} =
             request(c, "reader", discard)

    path = Path.join(Path.dirname(c.store_path), "inflight.bin")
    File.write!(path, bytes)

    assert {:error, :upload_discarded} =
             Relay.upload_file(
               c.relay,
               c.reader,
               active_attrs,
               path,
               byte_size(bytes),
               hash,
               lease
             )

    assert :ok = Relay.upload_end(c.relay, lease)

    assert {:ok, 200, %{"upload_id" => new_id, "duplicate" => false}} =
             upload_request(c, "reader", attrs, bytes)

    assert %{"type" => "ok", "event_id" => "2"} =
             relay_request(c.relay, c.reader, %{
               "type" => "discussion",
               "op_id" => attrs.op_id,
               "sender_id" => attrs.sender_id,
               "message_id" => attrs.message_id,
               "request_revision" => 1,
               "text" => "Here is the file",
               "files" => [
                 %{
                   "name" => attrs.name,
                   "upload_id" => new_id,
                   "size" => byte_size(bytes),
                   "sha256" => hash
                 }
               ]
             })

    assert {:ok, 200, %{"type" => "ok", "state" => "committed", "discarded" => 0}} =
             request(c, "reader", discard)

    assert {:ok, 200, ^bytes} = fetch_bytes(c, "a2", new_id)
  end

  test "uploaded person file transfer and use remain separate from discussion acceptance", c do
    assert {:ok, 200, %{"type" => "ok"}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "send",
               "op_id" => op(710),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "mixed-files",
                 "created_at" => 1,
                 "title" => "Please inspect"
               }
             })

    uploaded = :binary.copy(<<0, 255, 13>>, 50_000)
    inline = :binary.copy(<<7>>, 150_000)
    hash = Base.encode16(:crypto.hash(:sha256, uploaded), case: :lower)

    assert {:ok, 200, %{"upload_id" => id}} =
             upload_request(
               c,
               "reader",
               %{
                 op_id: op(711),
                 sender_id: "app:test",
                 message_id: "mixed-files",
                 revision: 1,
                 name: "uploaded.bin"
               },
               uploaded
             )

    assert %{"type" => "error", "field" => "files"} =
             relay_request(c.relay, c.reader, %{
               "type" => "discussion",
               "op_id" => op(712),
               "sender_id" => "app:test",
               "message_id" => "mixed-files",
               "request_revision" => 1,
               "text" => "This must not attach the wrong operation",
               "files" => [
                 %{
                   "name" => "uploaded.bin",
                   "upload_id" => id,
                   "size" => byte_size(uploaded),
                   "sha256" => hash
                 }
               ]
             })

    assert %{"type" => "ok", "event_id" => "2"} =
             relay_request(c.relay, c.reader, %{
               "type" => "discussion",
               "op_id" => op(711),
               "sender_id" => "app:test",
               "message_id" => "mixed-files",
               "request_revision" => 1,
               "text" => "Please see both files",
               "files" => [
                 %{"name" => "inline.bin", "data_base64" => Base.encode64(inline)},
                 %{
                   "name" => "uploaded.bin",
                   "upload_id" => id,
                   "size" => byte_size(uploaded),
                   "sha256" => hash
                 }
               ]
             })

    assert {:ok, 200, ^uploaded} = fetch_bytes(c, "a2", id)

    reports = [
      %{"name" => "inline.bin", "status" => "transferred"},
      %{"name" => "uploaded.bin", "status" => "transferred"}
    ]

    assert {:ok, 200, %{"type" => "ok", "status" => "delivered", "file_transfers" => transfers}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "ack",
               "event_id" => "2",
               "file_transfers" => reports
             })

    assert transfers["uploaded.bin"] == %{"status" => "transferred"}

    assert {:ok, 200, %{"type" => "ok", "status" => "used"}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "file_use",
               "event_id" => "2",
               "file" => "uploaded.bin",
               "status" => "used",
               "op_id" => op(713)
             })

    assert {:ok, %{"entries" => entries}} = Relay.origin_feed(c.relay, c.origin, 0)
    assert Enum.at(entries, 1)["file_use"]["uploaded.bin"] == %{"status" => "used"}
  end

  test "a handle's authenticated route support is durable metadata, not inferred from an attached stream",
       c do
    assert {:ok, 200, %{"type" => "ok"}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "send",
               "op_id" => op(61),
               "session_handle" => @handle,
               "route" => "codex",
               "cli_version" => "test-only",
               "unsupported_reason" => "listener unavailable",
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "route-support",
                 "created_at" => 1,
                 "title" => "Question"
               }
             })

    assert {:ok, 200,
            %{
              "availability" => "unsupported",
              "unsupported_reason" => "listener unavailable",
              "listening_support" => %{
                "route" => "codex",
                "cli_version" => "test-only",
                "origin" => origin
              }
            }} = request(c, "a2", %{"v" => 1, "type" => "status", "session_handle" => @handle})

    assert origin == c.origin

    {:ok, stream} = socket(c, "a2")

    :ok =
      :ssl.send(
        stream,
        "GET /origin/v1/stream?session=#{@handle} HTTP/1.1\r\nHost: relay.test\r\n\r\n"
      )

    assert {:ok, _} = recv_until(stream, "\n\n", <<>>, 20)

    assert {:ok, 200, %{"availability" => "unsupported"}} =
             request(c, "a2", %{"v" => 1, "type" => "status", "session_handle" => @handle})

    update = %{
      "v" => 1,
      "type" => "support",
      "op_id" => op(62),
      "session_handle" => @handle,
      "route" => "claude-code",
      "cli_version" => "test-only-2",
      "unsupported_reason" => nil
    }

    assert {:ok, 200,
            %{"type" => "ok", "listening_support" => %{"route" => "claude-code"} = support}} =
             request(c, "a2", update)

    assert is_integer(support["reported_at"])

    assert {:ok, 200, %{"type" => "ok", "duplicate" => true, "listening_support" => ^support}} =
             request(c, "a2", update)

    assert {:ok, 200, %{"availability" => "listening", "listening_support" => ^support}} =
             request(c, "a2", %{"v" => 1, "type" => "status", "session_handle" => @handle})

    assert {:ok, [still_unsupported, restored], 2} =
             Relay.conversation_events(c.relay, c.reader, 0)

    assert still_unsupported["availability"] == "unsupported"
    assert restored["kind"] == "availability"
    assert restored["availability"] == "listening"
    assert restored["session_handle"] == @handle
    assert restored["listening_support"] == support
    assert restored["unsupported_reason"] == nil
    assert restored["request_revision"] == 1

    assert {:ok, 200, %{"type" => "error", "reason" => "operation_conflict"}} =
             request(c, "a2", %{update | "route" => "codex"})

    assert {:ok, 200, %{"type" => "error", "reason" => "not_found"}} =
             request(c, "b2", %{update | "op_id" => op(63)})

    :ssl.close(stream)
  end

  test "attachment updates are origin-bound, receipt-backed metadata changes", c do
    message = %{
      "sender_id" => "app:test",
      "message_id" => "attachment-update",
      "created_at" => 1,
      "title" => "Continue?"
    }

    assert {:ok, 200, %{"type" => "ok", "session_handle" => @handle}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "send",
               "op_id" => op(70),
               "session_handle" => @handle,
               "vendor_session_id" => "old-vendor-session",
               "message" => message
             })

    assert {:ok, 200, %{"type" => "ok"}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "support",
               "op_id" => op(71),
               "session_handle" => @handle,
               "route" => "claude-code",
               "cli_version" => "test-only",
               "unsupported_reason" => nil
             })

    assert {:ok, 200, %{"type" => "ok"} = feed_before} =
             request(c, "a2", %{"v" => 1, "type" => "feed", "since" => 0})

    assert {:ok, 200, %{"type" => "ok"} = status_before} =
             request(c, "a2", %{"v" => 1, "type" => "status", "session_handle" => @handle})

    before = :sys.get_state(c.relay).store.engine
    session_key = {c.person, c.origin, @handle}
    prior_session = before.origin_sessions[session_key]
    assert prior_session.vendor_session_id == "old-vendor-session"
    refute is_nil(prior_session.support)

    update = %{
      "v" => 1,
      "type" => "attachment_update",
      "op_id" => op(72),
      "session_handle" => @handle,
      "vendor_session_id" => "new-vendor-session"
    }

    assert {:ok, 200,
            %{
              "type" => "ok",
              "session_handle" => @handle,
              "vendor_session_id" => "new-vendor-session"
            }} = request(c, "a2", update)

    after_update = :sys.get_state(c.relay).store.engine
    expected_session = Map.put(prior_session, :vendor_session_id, "new-vendor-session")

    assert after_update.origin_sessions ==
             Map.put(before.origin_sessions, session_key, expected_session)

    assert Map.drop(after_update, [:origin_sessions, :receipts]) ==
             Map.drop(before, [:origin_sessions, :receipts])

    receipt_key = {c.person, c.origin, op(72)}

    assert Map.drop(after_update.receipts, [receipt_key]) == before.receipts

    assert after_update.receipts[receipt_key].reply == %{
             "session_handle" => @handle,
             "vendor_session_id" => "new-vendor-session"
           }

    assert {:ok, 200,
            %{
              "type" => "ok",
              "duplicate" => true,
              "session_handle" => @handle,
              "vendor_session_id" => "new-vendor-session"
            }} = request(c, "a2", update)

    assert :sys.get_state(c.relay).store.engine == after_update

    assert {:ok, 200, %{"type" => "error", "reason" => "operation_conflict"}} =
             request(c, "a2", %{update | "vendor_session_id" => "different-vendor-session"})

    assert {:ok, 200, %{"type" => "error", "reason" => "invalid_field"}} =
             request(c, "a2", %{update | "op_id" => op(73), "session_handle" => "invalid"})

    assert {:ok, 200, %{"type" => "error", "reason" => "invalid_field"}} =
             request(c, "a2", %{update | "op_id" => op(74), "vendor_session_id" => ""})

    assert {:ok, 200, %{"type" => "error", "reason" => "invalid_field"}} =
             request(c, "a2", %{update | "op_id" => op(78), "vendor_session_id" => 7})

    assert {:ok, 200, %{"type" => "error", "reason" => "invalid_field"}} =
             request(c, "a2", Map.delete(%{update | "op_id" => op(75)}, "vendor_session_id"))

    assert {:ok, 200, %{"type" => "error", "reason" => "invalid_field", "field" => "op_id"}} =
             request(c, "a2", %{update | "op_id" => "invalid"})

    assert {:ok, 200, %{"type" => "error", "reason" => "not_found"}} =
             request(c, "reader", update)

    assert {:ok, 200, %{"type" => "error", "reason" => "not_found"}} =
             request(c, "b2", %{update | "op_id" => op(76)})

    clear_update = %{update | "op_id" => op(77), "vendor_session_id" => nil}

    assert {:ok, 200,
            %{
              "type" => "ok",
              "session_handle" => @handle,
              "vendor_session_id" => nil
            }} = request(c, "a2", clear_update)

    assert {:ok, 200,
            %{
              "type" => "ok",
              "duplicate" => true,
              "session_handle" => @handle,
              "vendor_session_id" => nil
            }} = request(c, "a2", clear_update)

    after_clear = :sys.get_state(c.relay).store.engine

    cleared_session = Map.put(expected_session, :vendor_session_id, nil)

    assert after_clear.origin_sessions ==
             Map.put(before.origin_sessions, session_key, cleared_session)

    assert Map.drop(after_clear, [:origin_sessions, :receipts]) ==
             Map.drop(before, [:origin_sessions, :receipts])

    assert Map.drop(after_clear.receipts, [receipt_key, {c.person, c.origin, op(77)}]) ==
             before.receipts

    assert {:ok, 200, ^feed_before} =
             request(c, "a2", %{"v" => 1, "type" => "feed", "since" => 0})

    assert {:ok, 200, ^status_before} =
             request(c, "a2", %{"v" => 1, "type" => "status", "session_handle" => @handle})

    assert :ok = Supervisor.stop(c.server)
    assert {:ok, restarted} = Relay.start_link(store_path: c.store_path)
    Process.unlink(restarted)
    on_exit(fn -> if Process.alive?(restarted), do: GenServer.stop(restarted) end)

    assert {:ok,
            %{
              "duplicate" => true,
              "session_handle" => @handle,
              "vendor_session_id" => "new-vendor-session"
            }} = Relay.origin_attachment_update(restarted, c.origin, Map.drop(update, ["v"]))

    restarted_engine = :sys.get_state(restarted).store.engine
    assert restarted_engine.origin_sessions[session_key].vendor_session_id == nil
  end

  test "certificate-bound HTTPS send, feed, delivery and result survive response loss", c do
    message = %{
      "sender_id" => "app:test",
      "message_id" => "one",
      "created_at" => 1,
      "title" => "Proceed?",
      "display_name" => "  Agent\nλ / name  ",
      "detail" => %{"amount" => %JSON.DecimalToken{raw: "1.234567890123456789"}, "answer" => nil}
    }

    send_request = %{
      "v" => 1,
      "type" => "send",
      "op_id" => op(1),
      "session_handle" => @handle,
      "message" => message,
      "ref" => "send-1"
    }

    assert {:error, _} = request(c, "unknown", send_request)

    assert {:ok, 200, %{"type" => "error", "reason" => "invalid_field", "field" => "person_id"}} =
             request(c, "a2", Map.put(send_request, "person_id", c.person))

    assert {:ok, 200, %{"type" => "ok", "seq" => 1, "ref" => "send-1"}} =
             request(c, "a2", send_request)

    assert {:ok, 200,
            %{
              "availability" => "unsupported",
              "unsupported_reason" => "no listening support recorded",
              "listening_support" => nil
            }} = request(c, "a2", %{"v" => 1, "type" => "status", "session_handle" => @handle})

    assert {:ok, 200, %{"type" => "ok", "seq" => 1, "duplicate" => true}} =
             request(c, "a2", send_request)

    unauthorized_message =
      message
      |> Map.put("sender_id", "app:ungranted")
      |> Map.put("display_name", "app:test")

    assert {:ok, 200, %{"type" => "error", "reason" => "sender_not_authorized"}} =
             request(
               c,
               "a2",
               Map.merge(send_request, %{
                 "op_id" => op(6),
                 "message" => unauthorized_message
               })
             )

    assert {:ok, 200,
            %{"type" => "ok", "entries" => [%{"kind" => "revision", "message" => ^message}]}} =
             request(c, "a2", %{"v" => 1, "type" => "feed", "since" => 0})

    assert {:ok, 200, %{"type" => "ok", "entries" => []}} =
             request(c, "b2", %{"v" => 1, "type" => "feed", "since" => 0})

    assert %{"type" => "ok", "event_id" => "2"} =
             relay_request(c.relay, c.reader, %{
               "type" => "discussion",
               "op_id" => op(2),
               "sender_id" => "app:test",
               "message_id" => "one",
               "request_revision" => 1,
               "text" => "Explain first"
             })

    assert {:ok, 200,
            %{
              "type" => "ok",
              "entries" => [
                %{"kind" => "discussion", "text" => "Explain first", "delivered" => false}
              ]
            }} =
             request(c, "a2", %{"v" => 1, "type" => "feed", "since" => 1})

    assert {:ok, 200, %{"type" => "ok", "status" => "delivered"}} =
             request(c, "a2", %{"v" => 1, "type" => "ack", "event_id" => "2"})

    reply = %{
      "v" => 1,
      "type" => "reply",
      "op_id" => op(3),
      "sender_id" => "app:test",
      "message_id" => "one",
      "request_revision" => 1,
      "in_reply_to" => "2",
      "text" => "First a canary"
    }

    {:ok, socket} = socket(c, "a2")
    :ok = :ssl.send(socket, http("POST", "/origin/v1", JSON.encode!(reply)))
    assert :ok = await_seq(c.relay, c.origin, 3, 100)
    :ssl.close(socket)

    assert {:ok, 200, %{"type" => "ok", "seq" => 4, "event_id" => "3", "duplicate" => true}} =
             request(c, "a2", reply)

    assert %{"type" => "ok", "seq" => 2} =
             relay_request(c.relay, c.reader, %{
               "type" => "answer",
               "op_id" => op(4),
               "sender_id" => "app:test",
               "message_id" => "one",
               "request_revision" => 1,
               "answer" => nil
             })

    assert {:ok, 200,
            %{
              "type" => "ok",
              "entries" => [%{"kind" => "terminal", "has_answer" => true, "answer" => nil}]
            }} =
             request(c, "a2", %{"v" => 1, "type" => "feed", "since" => 3})

    assert {:ok, 200, %{"type" => "ok", "status" => "received"}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "result",
               "op_id" => op(5),
               "sender_id" => "app:test",
               "message_id" => "one",
               "in_reply_to" => "4",
               "status" => "received"
             })
  end

  test "authenticated HTTPS records file transfer separately from agent use", c do
    assert {:ok, 200, %{"type" => "ok", "seq" => 1}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "send",
               "op_id" => op(40),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "file-https",
                 "created_at" => 1,
                 "title" => "Inspect file"
               }
             })

    assert %{"type" => "ok", "event_id" => "2"} =
             relay_request(c.relay, c.reader, %{
               "type" => "discussion",
               "op_id" => op(41),
               "sender_id" => "app:test",
               "message_id" => "file-https",
               "request_revision" => 1,
               "text" => "File attached",
               "files" => [%{"name" => "a.bin", "data_base64" => Base.encode64(<<0, 255>>)}]
             })

    assert {:ok, 200, %{"type" => "error", "reason" => "not_found"}} =
             request(c, "b2", %{"v" => 1, "type" => "ack", "event_id" => "2"})

    assert {:ok, 200, %{"type" => "error", "reason" => "file_transfers"}} =
             request(c, "a2", %{"v" => 1, "type" => "ack", "event_id" => "2"})

    assert {:ok, 200, %{"type" => "ok", "status" => "delivered"}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "ack",
               "event_id" => "2",
               "file_transfers" => [%{"name" => "a.bin", "status" => "transferred"}]
             })

    assert {:ok, 200, %{"type" => "ok", "entries" => [%{"file_use" => %{}}]}} =
             request(c, "a2", %{"v" => 1, "type" => "feed", "since" => 1})

    assert {:ok, 200, %{"type" => "ok", "status" => "used"}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "file_use",
               "op_id" => op(42),
               "event_id" => "2",
               "file" => "a.bin",
               "status" => "used"
             })

    assert {:ok, 200,
            %{"type" => "ok", "entries" => [%{"file_use" => %{"a.bin" => %{"status" => "used"}}}]}} =
             request(c, "a2", %{"v" => 1, "type" => "feed", "since" => 1})
  end

  if binary = System.get_env("HARKLIGHT_AGENT_BIN") do
    @agent_binary binary
    @tag timeout: 120_000
    test "real CLI serves three own conversations on one origin connection", c do
      root = Path.join(Path.dirname(c.store_path), "agent-fixture")
      ca = Path.join(c.certs, "ca.pem")

      make_identity = fn name, key ->
        dir = Path.join(root, name)
        File.mkdir_p!(dir)
        File.chmod!(dir, 0o700)
        File.cp!(Path.join(c.certs, name <> ".pem"), Path.join(dir, "identity.pem"))
        File.cp!(Path.join(c.certs, name <> ".key"), Path.join(dir, "identity.key"))
        File.chmod!(Path.join(dir, "identity.key"), 0o600)
        File.write!(Path.join(dir, "enrollment.json"), JSON.encode!(%{"device_id" => key}))

        File.write!(
          Path.join(dir, "server.json"),
          JSON.encode!(%{
            "address" => "127.0.0.1",
            "name" => "relay.test",
            "port" => Server.origin_port(c.server),
            "origin_port" => Server.origin_port(c.server),
            "ca_sha256" => Base.encode16(:crypto.hash(:sha256, File.read!(ca)), case: :lower)
          })
        )

        ["--identity-dir", dir, "--ca-file", ca, "--state-dir", Path.join(dir, "state")]
      end

      args = make_identity.("a2", c.origin)
      other_args = make_identity.("reader", c.reader)
      :ok = Relay.setup_sender(c.relay, c.reader, "app:test")

      run = fn base, command ->
        {output, code} = System.cmd(@agent_binary, base ++ command, stderr_to_stdout: true)
        assert code == 0, output

        output
        |> String.split("\n", trim: true)
        |> List.last()
        |> then(&JSON.decode_line(&1 <> "\n"))
        |> elem(1)
      end

      send_args = [
        "send",
        "--session",
        "new",
        "--route",
        "other",
        "--controller",
        "--origin-listener",
        "--sender-id",
        "app:test",
        "--title",
        "Fixture question",
        "--type",
        "request",
        "--reply"
      ]

      own = for _ <- 1..3, do: run.(args, send_args)
      other = run.(other_args, send_args)

      listener =
        Port.open({:spawn_executable, @agent_binary}, [
          :binary,
          :exit_status,
          {:line, 1_000_000},
          {:args, args ++ ["listen", "--origin", "--follow"]}
        ])

      {:os_pid, listener_pid} = Port.info(listener, :os_pid)

      on_exit(fn ->
        System.cmd("kill", ["-TERM", Integer.to_string(listener_pid)], stderr_to_stdout: true)
      end)

      # Receiving an event establishes that the attach completed; no timing guess.
      Enum.each(own, fn item ->
        assert %{"type" => "ok"} =
                 relay_request(c.relay, c.reader, %{
                   "type" => "discussion",
                   "op_id" => op(950 + Enum.find_index(own, &(&1 == item))),
                   "sender_id" => "app:test",
                   "message_id" => item["request"],
                   "request_revision" => 1,
                   "text" => "Fixture reply"
                 })
      end)

      assert %{"type" => "ok"} =
               relay_request(c.relay, c.origin, %{
                 "type" => "discussion",
                 "op_id" => op(959),
                 "sender_id" => "app:test",
                 "message_id" => other["request"],
                 "request_revision" => 1,
                 "text" => "Other system"
               })

      events =
        for _ <- 1..3 do
          receive do
            {^listener, {:data, {:eol, line}}} ->
              assert {:ok, %{"kind" => "event"} = event} = JSON.decode_line(line <> "\n")
              event

            {^listener, {:exit_status, code}} ->
              flunk("listener exited #{code}")
          after
            15_000 -> flunk("missing own event")
          end
        end

      assert Enum.sort(Enum.map(events, & &1["session_handle"])) ==
               Enum.sort(Enum.map(own, & &1["session"]))

      for item <- own do
        assert {:ok, %{"availability" => "listening"}} =
                 Relay.origin_status(c.relay, c.origin, item["session"])
      end

      state = :sys.get_state(c.relay)
      assert map_size(state.origin_streams) == 1
      assert Map.has_key?(state.origin_streams, {c.person, c.origin, :origin})

      for event <- Enum.take(events, 1) do
        run.(args, ["ack", "--session", event["session_handle"], "--event", event["event_id"]])

        run.(args, [
          "reply",
          "--session",
          event["session_handle"],
          "--in-reply-to",
          event["event_id"],
          "--request",
          event["request"]["message_id"],
          "--body",
          "Handled"
        ])
      end

      # Stop the process, then resume its durable journal. Only unacted turns replay.
      {_, 0} = System.cmd("kill", ["-TERM", Integer.to_string(listener_pid)])

      receive do
        {^listener, {:exit_status, _}} -> :ok
      after
        5_000 -> flunk("listener did not stop")
      end

      restarted =
        Port.open({:spawn_executable, @agent_binary}, [
          :binary,
          :exit_status,
          {:line, 1_000_000},
          {:args, args ++ ["listen", "--origin", "--follow"]}
        ])

      {:os_pid, restarted_pid} = Port.info(restarted, :os_pid)

      on_exit(fn ->
        System.cmd("kill", ["-TERM", Integer.to_string(restarted_pid)], stderr_to_stdout: true)
      end)

      replay =
        for _ <- 1..2 do
          receive do
            {^restarted, {:data, {:eol, line}}} ->
              assert {:ok, %{"kind" => "event"} = event} = JSON.decode_line(line <> "\n")
              event["event_id"]
          after
            15_000 -> flunk("missing replay")
          end
        end

      assert Enum.sort(replay) ==
               events |> Enum.drop(1) |> Enum.map(& &1["event_id"]) |> Enum.sort()

      assert {:ok, handles} = Relay.origin_coverage(c.relay, c.origin)
      assert length(handles) == 3

      # A legacy listener journals an uncovered turn before cover announces it.
      legacy = run.(args, Enum.reject(send_args, &(&1 == "--origin-listener")))

      old_listener =
        Port.open({:spawn_executable, @agent_binary}, [
          :binary,
          :exit_status,
          {:line, 1_000_000},
          {:args, args ++ ["listen", "--session", legacy["session"], "--controller", "--follow"]}
        ])

      {:os_pid, old_pid} = Port.info(old_listener, :os_pid)

      on_exit(fn ->
        System.cmd("kill", ["-TERM", Integer.to_string(old_pid)], stderr_to_stdout: true)
      end)

      assert %{"type" => "ok"} =
               relay_request(c.relay, c.reader, %{
                 "type" => "discussion",
                 "op_id" => op(960),
                 "sender_id" => "app:test",
                 "message_id" => legacy["request"],
                 "request_revision" => 1,
                 "text" => "Migrate pending turn"
               })

      legacy_event =
        receive do
          {^old_listener, {:data, {:eol, line}}} ->
            assert {:ok, %{"kind" => "event"} = event} = JSON.decode_line(line <> "\n")
            event
        after
          15_000 -> flunk("legacy listener missing turn")
        end

      assert {:ok, %{"availability" => "listening"}} =
               Relay.origin_status(c.relay, c.origin, legacy["session"])

      assert %{"kind" => "covered"} = run.(args, ["cover", "--session", legacy["session"]])

      receive do
        {^restarted, {:data, {:eol, line}}} ->
          assert {:ok, %{"kind" => "event"} = migrated} = JSON.decode_line(line <> "\n")
          assert migrated["event_id"] == legacy_event["event_id"]
          assert migrated["session_handle"] == legacy["session"]
      after
        15_000 -> flunk("coverage did not offer already-journaled turn")
      end

      {_, 0} = System.cmd("kill", ["-TERM", Integer.to_string(old_pid)])

      receive do
        {^old_listener, {:exit_status, _}} -> :ok
      after
        5_000 -> flunk("legacy listener did not stop")
      end

      assert {:ok, %{"availability" => "listening"}} =
               Relay.origin_status(c.relay, c.origin, legacy["session"])

      # The second enrollment's feed remains disjoint even though both have the same sender grant.
      assert {:ok, feed} = Relay.origin_feed(c.relay, c.origin, 0)
      refute Enum.any?(feed["entries"], &(&1["session_handle"] == other["session"]))
      {:ok, takeover} = socket(c, "a2")

      :ok =
        :ssl.send(
          takeover,
          "GET /origin/v1/stream?scope=origin&reconnect=1 HTTP/1.1\r\nHost: relay.test\r\n\r\n"
        )

      assert {:ok, _} = recv_until(takeover, "\n\n", <<>>, 20)

      receive do
        {^restarted, {:exit_status, 6}} -> :ok
      after
        5_000 -> flunk("displaced origin listener did not exit with listener-active")
      end

      :ssl.close(takeover)
      Supervisor.stop(c.server)
      assert {:ok, durable} = Store.open(c.store_path)

      assert durable.engine.origin_sessions[{c.person, c.origin, legacy["session"]}].support[
               "origin_listener"
             ] == true

      assert durable.engine.origin_feeds[{c.person, c.origin}].high == feed["cursor"]
      Store.close(durable)
    end
  end

  test "origin stream covers explicitly declared handles and replaces independently", c do
    {:ok, stream} = socket(c, "a2")

    :ok =
      :ssl.send(
        stream,
        "GET /origin/v1/stream?scope=origin&reconnect=1 HTTP/1.1\r\nHost: relay.test\r\n\r\n"
      )

    assert {:ok, initial} = recv_until(stream, "data: {\"cursor\":0}\n\n", <<>>, 20)
    assert initial =~ "event: change"

    assert {:ok, 200, %{"reason" => "already_listening"}} =
             raw_request(
               c,
               "a2",
               "GET /origin/v1/stream?scope=origin HTTP/1.1\r\nHost: relay.test\r\n\r\n"
             )

    for option <- ["session=" <> @handle, "advisory_v1=1", "sender=app:test", "reconnect=0"] do
      assert {:ok, 400, %{"reason" => "invalid_field"}} =
               raw_request(
                 c,
                 "a2",
                 "GET /origin/v1/stream?scope=origin&#{option} HTTP/1.1\r\nHost: relay.test\r\n\r\n"
               )
    end

    assert {:ok, 200, %{"session_handles" => []}} =
             request(c, "a2", %{"v" => 1, "type" => "coverage"})

    assert {:ok, 200, %{"type" => "ok", "listening_support" => %{"origin_listener" => true}}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "send",
               "op_id" => op(910),
               "session_handle" => @handle,
               "route" => "other",
               "cli_version" => "test",
               "unsupported_reason" => nil,
               "origin_listener" => true,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "aggregate",
                 "created_at" => 1,
                 "title" => "Question"
               }
             })

    assert {:ok, notice} = recv_until(stream, "data: {\"cursor\":1}\n\n", <<>>, 20)
    assert notice =~ "event: coverage\ndata: {}"
    refute notice =~ @handle

    assert {:ok, 200, %{"session_handles" => [@handle]}} =
             request(c, "a2", %{"v" => 1, "type" => "coverage"})

    assert {:ok, 200, %{"session_handles" => []}} =
             request(c, "b2", %{"v" => 1, "type" => "coverage"})

    assert {:ok, 200, %{"availability" => "listening"}} =
             request(c, "a2", %{"v" => 1, "type" => "status", "session_handle" => @handle})

    # Whole-record replacement withdraws coverage without adding a feed entry.
    assert {:ok, 200, %{"type" => "ok"}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "support",
               "op_id" => op(911),
               "session_handle" => @handle,
               "route" => "other",
               "cli_version" => "test",
               "unsupported_reason" => nil
             })

    assert {:ok, "event: coverage\ndata: {}\n\n"} = recv_until(stream, "\n\n", <<>>, 20)

    assert {:ok, 200, %{"availability" => "not_listening", "cursor" => 1}} =
             request(c, "a2", %{"v" => 1, "type" => "status", "session_handle" => @handle})

    {:ok, replacement} = socket(c, "a2")

    :ok =
      :ssl.send(
        replacement,
        "GET /origin/v1/stream?scope=origin&reconnect=1 HTTP/1.1\r\nHost: relay.test\r\n\r\n"
      )

    assert {:ok, _} = recv_until(replacement, "data: {\"cursor\":1}\n\n", <<>>, 20)
    assert {:ok, "event: replaced\ndata: {}\n\n"} = :ssl.recv(stream, 0, 5000)
    assert {:error, :closed} = :ssl.recv(stream, 0, 5000)
    :ssl.close(replacement)
  end

  test "SSE notices contain only cursors; authoritative fetch and session availability remain separate",
       c do
    assert {:ok, 200, %{"type" => "ok"}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "send",
               "op_id" => op(10),
               "session_handle" => @handle,
               "route" => "claude-code",
               "cli_version" => "test-only",
               "unsupported_reason" => nil,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "stream",
                 "created_at" => 1,
                 "title" => "Question"
               }
             })

    assert {:ok, [accepted], 1} = Relay.events(c.relay, c.reader, 0)
    assert accepted["availability"] == "not_listening"
    assert accepted["session_handle"] == @handle

    {:ok, stream} = socket(c, "a2")

    :ok =
      :ssl.send(
        stream,
        "GET /origin/v1/stream?session=#{@handle} HTTP/1.1\r\nHost: relay.test\r\n\r\n"
      )

    assert {:ok, initial} = recv_until(stream, "\n\n", <<>>, 20)
    assert initial =~ "HTTP/1.1 200 OK"
    assert initial =~ "event: change\ndata: {\"cursor\":1}"
    refute initial =~ "Question"

    assert {:ok, 200, %{"type" => "ok", "availability" => "listening"}} =
             request(c, "a2", %{"v" => 1, "type" => "status", "session_handle" => @handle})

    assert {:ok, [attached], 1} = Relay.conversation_events(c.relay, c.reader, 0)
    assert attached["kind"] == "availability"
    assert attached["availability"] == "listening"

    assert {:ok, 200, %{"type" => "error", "reason" => "already_listening"}} =
             stream_request(c, "a2", @handle)

    assert %{"type" => "ok", "event_id" => "2"} =
             relay_request(c.relay, c.reader, %{
               "type" => "discussion",
               "op_id" => op(11),
               "sender_id" => "app:test",
               "message_id" => "stream",
               "request_revision" => 1,
               "text" => "Private words"
             })

    assert {:ok, notice} = recv_until(stream, "data: {\"cursor\":2}\n\n", <<>>, 20)
    refute notice =~ "Private words"

    assert {:ok, 200, %{"type" => "ok", "entries" => [%{"text" => "Private words"}]}} =
             request(c, "a2", %{"v" => 1, "type" => "feed", "since" => 1})

    :ssl.close(stream)
    assert :ok = await_availability(c, "not_listening", 100)
    assert {:ok, [ack1, detached], 4} = Relay.conversation_events(c.relay, c.reader, 2)
    assert ack1["kind"] == "delivery_ack"
    assert ack1["stage"] == "ack1"
    assert ack1["event_id"] == "2"
    assert ack1["op_id"] == op(11)
    assert ack1["ack2_expected"] == false
    assert detached["availability"] == "not_listening"
    assert is_integer(detached["last_detached_at"])

    {:ok, stream_again} = socket(c, "a2")

    :ok =
      :ssl.send(
        stream_again,
        "GET /origin/v1/stream?session=#{@handle} HTTP/1.1\r\nHost: relay.test\r\n\r\n"
      )

    assert {:ok, _} = recv_until(stream_again, "\n\n", <<>>, 20)
    assert {:ok, [reattached], 5} = Relay.conversation_events(c.relay, c.reader, 4)
    assert reattached["availability"] == "listening"
    assert :ok = Supervisor.terminate_child(c.server, :origin_https)
    assert {:error, :closed} = :ssl.recv(stream_again, 0, 5_000)
    assert :ok = await_relay_availability(c, "not_listening", 100)
    assert {:ok, [listener_stopped], 6} = Relay.conversation_events(c.relay, c.reader, 5)
    assert listener_stopped["availability"] == "not_listening"
    :ssl.close(stream_again)
  end

  test "explicit reconnect attaches when absent and replaces only the owned stream", c do
    assert {:ok, 200, %{"type" => "ok"}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "send",
               "op_id" => op(810),
               "session_handle" => @handle,
               "route" => "other",
               "cli_version" => "test-only",
               "unsupported_reason" => nil,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "reconnect",
                 "created_at" => 1,
                 "title" => "Question"
               }
             })

    open_stream = fn ->
      {:ok, stream} = socket(c, "a2")

      :ok =
        :ssl.send(
          stream,
          "GET /origin/v1/stream?session=#{@handle}&reconnect=1 HTTP/1.1\r\nHost: relay.test\r\n\r\n"
        )

      assert {:ok, initial} = recv_until(stream, "\n\n", <<>>, 20)
      assert initial =~ "HTTP/1.1 200 OK"
      assert initial =~ "event: change\ndata: {\"cursor\":1}"
      stream
    end

    # No worker exists: the replacement route also performs ordinary attachment.
    first = open_stream.()
    stream_key = {c.person, c.origin, @handle}
    old = :sys.get_state(c.relay).origin_streams[stream_key]

    assert {:ok, 200, %{"type" => "error", "reason" => "already_listening"}} =
             stream_request(c, "a2", @handle)

    assert {:ok, 200, %{"type" => "error", "reason" => "not_found"}} =
             raw_request(
               c,
               "reader",
               "GET /origin/v1/stream?session=#{@handle}&reconnect=1 HTTP/1.1\r\nHost: relay.test\r\n\r\n"
             )

    assert :sys.get_state(c.relay).origin_streams[stream_key].worker == old.worker

    db = :sys.get_state(c.relay).store.db
    :ok = Sqlite3.execute(db, "PRAGMA query_only=ON")

    try do
      assert {:ok, 200, %{"type" => "error", "reason" => "storage_unavailable"}} =
               raw_request(
                 c,
                 "a2",
                 "GET /origin/v1/stream?session=#{@handle}&reconnect=1 HTTP/1.1\r\nHost: relay.test\r\n\r\n"
               )

      assert :sys.get_state(c.relay).origin_streams[stream_key] == old
    after
      :ok = Sqlite3.execute(db, "PRAGMA query_only=OFF")
    end

    second = open_stream.()
    assert {:error, :closed} = :ssl.recv(first, 0, 5_000)
    replacement = :sys.get_state(c.relay).origin_streams[stream_key]
    refute replacement.worker == old.worker

    # Both late cleanup paths must leave the new registration intact.
    :ok = Relay.origin_stream_detach(c.relay, c.origin, @handle, old.worker)
    send(c.relay, {:DOWN, old.monitor, :process, old.worker, :normal})
    assert :sys.get_state(c.relay).origin_streams[stream_key] == replacement

    assert %{"type" => "ok", "event_id" => "2"} =
             relay_request(c.relay, c.reader, %{
               "type" => "discussion",
               "op_id" => op(811),
               "sender_id" => "app:test",
               "message_id" => "reconnect",
               "request_revision" => 1,
               "text" => "Still the same conversation"
             })

    assert {:ok, _} = recv_until(second, "data: {\"cursor\":2}\n\n", <<>>, 20)

    assert {:ok, 200, %{"entries" => [%{"event_id" => "2", "session_handle" => @handle}]}} =
             request(c, "a2", %{"v" => 1, "type" => "feed", "since" => 1})

    assert {:ok, 200, %{"availability" => "listening"}} =
             request(c, "a2", %{"v" => 1, "type" => "status", "session_handle" => @handle})

    :ssl.close(second)
    assert :ok = await_availability(c, "not_listening", 100)
  end

  test "an origin feed change wakes only that same-person origin identity's streams", c do
    reader_handle = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    second_origin_handle = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    :ok = Relay.setup_sender(c.relay, c.reader, "app:reader")

    assert {:ok, 200, %{"type" => "ok"}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "send",
               "op_id" => op(730),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "stream-a1",
                 "created_at" => 1,
                 "title" => "A"
               }
             })

    assert {:ok, 200, %{"type" => "ok"}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "send",
               "op_id" => op(731),
               "session_handle" => second_origin_handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "stream-a-other-handle",
                 "created_at" => 1,
                 "title" => "A second handle"
               }
             })

    assert {:ok, 200, %{"type" => "ok"}} =
             request(c, "reader", %{
               "v" => 1,
               "type" => "send",
               "op_id" => op(734),
               "session_handle" => reader_handle,
               "message" => %{
                 "sender_id" => "app:reader",
                 "message_id" => "stream-reader1",
                 "created_at" => 1,
                 "title" => "Reader"
               }
             })

    {:ok, stream_a} = socket(c, "a2")

    :ok =
      :ssl.send(
        stream_a,
        "GET /origin/v1/stream?session=#{@handle} HTTP/1.1\r\nHost: relay.test\r\n\r\n"
      )

    assert {:ok, initial_a} = recv_until(stream_a, "\n\n", <<>>, 20)
    assert initial_a =~ "data: {\"cursor\":2}"

    {:ok, stream_a_other_handle} = socket(c, "a2")

    :ok =
      :ssl.send(
        stream_a_other_handle,
        "GET /origin/v1/stream?session=#{second_origin_handle} HTTP/1.1\r\nHost: relay.test\r\n\r\n"
      )

    assert {:ok, initial_a_other_handle} = recv_until(stream_a_other_handle, "\n\n", <<>>, 20)
    assert initial_a_other_handle =~ "data: {\"cursor\":2}"

    {:ok, stream_reader} = socket(c, "reader")

    :ok =
      :ssl.send(
        stream_reader,
        "GET /origin/v1/stream?session=#{reader_handle} HTTP/1.1\r\nHost: relay.test\r\n\r\n"
      )

    assert {:ok, initial_reader} = recv_until(stream_reader, "\n\n", <<>>, 20)
    assert initial_reader =~ "data: {\"cursor\":1}"

    assert {:ok, 200, %{"type" => "ok"}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "send",
               "op_id" => op(732),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "stream-a2",
                 "created_at" => 2,
                 "title" => "A second"
               }
             })

    assert {:ok, notice_a} = recv_until(stream_a, "data: {\"cursor\":3}\n\n", <<>>, 20)
    refute notice_a =~ "A second"

    assert {:ok, notice_a_other_handle} =
             recv_until(stream_a_other_handle, "data: {\"cursor\":3}\n\n", <<>>, 20)

    refute notice_a_other_handle =~ "A second"
    assert {:error, :timeout} = :ssl.recv(stream_reader, 0, 100)

    assert {:ok, 200, %{"type" => "ok"}} =
             request(c, "reader", %{
               "v" => 1,
               "type" => "send",
               "op_id" => op(733),
               "session_handle" => reader_handle,
               "message" => %{
                 "sender_id" => "app:reader",
                 "message_id" => "stream-reader2",
                 "created_at" => 2,
                 "title" => "Reader second"
               }
             })

    assert {:ok, notice_reader} = recv_until(stream_reader, "data: {\"cursor\":2}\n\n", <<>>, 20)
    refute notice_reader =~ "Reader second"
    assert {:error, :timeout} = :ssl.recv(stream_a, 0, 100)
    assert {:error, :timeout} = :ssl.recv(stream_a_other_handle, 0, 100)

    :ssl.close(stream_a)
    :ssl.close(stream_a_other_handle)
    :ssl.close(stream_reader)
  end

  test "an authorized agent answer wakes the originating SSE stream", c do
    message_id = "agent-answer-stream"
    agent_id = "00000000-0000-4000-8000-0000000000a1"

    {:ok, agent_key} =
      DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(c.certs, "unknown"))

    owner_endpoint = %{c.endpoint | port: Server.owner_port(c.server)}
    agent_endpoint = %{c.endpoint | port: Server.agent_port(c.server)}
    owner_cert = Path.join(c.certs, "a1.pem")
    owner_key = Path.join(c.certs, "a1.key")
    agent_cert = Path.join(c.certs, "unknown.pem")
    agent_private_key = Path.join(c.certs, "unknown.key")

    assert {:ok, %{"ok" => true}} =
             HeadlessTLS.owner_request(owner_endpoint, owner_cert, owner_key, %{
               "v" => 1,
               "type" => "agent_register",
               "person_id" => c.person,
               "agent_id" => agent_id,
               "key_fingerprint" => agent_key
             })

    assert {:ok, %{"ok" => true}} =
             HeadlessTLS.owner_request(owner_endpoint, owner_cert, owner_key, %{
               "v" => 1,
               "type" => "agent_grants_replace",
               "person_id" => c.person,
               "key_fingerprint" => agent_key,
               "expected_revision" => 1,
               "read_grants" => ["app:test"],
               "respond_grants" => ["app:test"]
             })

    assert {:ok, 200, %{"type" => "ok"}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "send",
               "op_id" => op(740),
               "session_handle" => @handle,
               "route" => "claude-code",
               "cli_version" => "test-only",
               "unsupported_reason" => nil,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => message_id,
                 "created_at" => 1,
                 "title" => "Question"
               }
             })

    {:ok, stream} = socket(c, "a2")

    try do
      :ok =
        :ssl.send(
          stream,
          "GET /origin/v1/stream?session=#{@handle} HTTP/1.1\r\nHost: relay.test\r\n\r\n"
        )

      assert {:ok, initial} = recv_until(stream, "\n\n", <<>>, 20)
      assert initial =~ "HTTP/1.1 200 OK"
      assert initial =~ "event: change\ndata: {\"cursor\":1}"

      assert {:ok, 200,
              %{
                "type" => "ok",
                "items" => [%{"acceptance_seq" => acceptance_seq}]
              }} =
               HeadlessTLS.agent_https_request(agent_endpoint, agent_cert, agent_private_key, %{
                 "v" => 1,
                 "type" => "search",
                 "sender_id" => "app:test",
                 "message_id" => message_id
               })

      assert {:ok, 200, %{"type" => "ok", "relay_outcome" => "answered"}} =
               HeadlessTLS.agent_https_request(agent_endpoint, agent_cert, agent_private_key, %{
                 "v" => 1,
                 "type" => "respond",
                 "sender_id" => "app:test",
                 "message_id" => message_id,
                 "acceptance_seq" => acceptance_seq,
                 "op_id" => op(741),
                 "deliberate" => true,
                 "intent_ref" => "test-instruction-1",
                 "answer" => "Agent answer"
               })

      assert {:ok, notice} = recv_until(stream, "data: {\"cursor\":2}\n\n", <<>>, 20)
      refute notice =~ "Agent answer"

      assert {:ok, 200, %{"type" => "ok", "entries" => [terminal]}} =
               request(c, "a2", %{"v" => 1, "type" => "feed", "since" => 1})

      assert terminal["kind"] == "terminal"
      assert terminal["author"] == "agent"
      assert terminal["terminal_state"] == "answered"
    after
      :ssl.close(stream)
    end
  end

  test "malformed origin requests do not restart the HTTPS listener", c do
    assert {:ok, 405, %{"reason" => "method_not_allowed"}} =
             raw_request(c, "a2", http("GET", "/origin/v1", ""))

    assert {:ok, 400, %{"reason" => "invalid_json"}} =
             raw_request(c, "a2", http("POST", "/origin/v1", "1.25"))

    assert {:ok, 200, %{"type" => "error", "reason" => "invalid_field", "field" => "ref"}} =
             request(c, "a2", %{"v" => 1, "type" => "feed", "since" => 0, "ref" => nil})

    assert {:ok, 200, %{"type" => "ok", "entries" => []}} =
             request(c, "a2", %{"v" => 1, "type" => "feed", "since" => 0})
  end

  test "a revoked device receives a revoked refusal instead of a silent close", c do
    assert {:ok, 200, %{"type" => "ok"}} =
             request(c, "a2", %{"v" => 1, "type" => "file_limits"})

    assert :ok = Relay.revoke_device(c.relay, c.origin)

    assert {:ok, 403, %{"v" => 1, "type" => "error", "reason" => "revoked"}} =
             request(c, "a2", %{"v" => 1, "type" => "file_limits"})

    assert {:ok, 403, %{"v" => 1, "type" => "error", "reason" => "revoked"}} =
             stream_request(c, "a2", @handle)
  end

  test "revision and withdrawal stay with the sending device and session goes only on declaration",
       c do
    original = %{
      "sender_id" => "app:test",
      "message_id" => "edit",
      "created_at" => 1,
      "title" => "Old wording"
    }

    assert {:ok, 200, %{"type" => "ok", "revision" => 1}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "send",
               "op_id" => op(20),
               "session_handle" => @handle,
               "route" => "claude-code",
               "cli_version" => "test-only",
               "unsupported_reason" => nil,
               "message" => original
             })

    revision = %{
      "v" => 1,
      "type" => "revise",
      "op_id" => op(21),
      "sender_id" => "app:test",
      "message_id" => "edit",
      "base_revision" => 1,
      "message" => %{original | "title" => "New wording"}
    }

    assert {:ok, 200, %{"type" => "error", "reason" => "not_origin"}} =
             request(c, "reader", revision)

    assert {:ok, 200, %{"type" => "ok", "revision" => 2}} = request(c, "a2", revision)

    assert {:ok, 200, %{"type" => "error", "reason" => "stale_revision"}} =
             request(c, "a2", %{revision | "op_id" => op(22)})

    assert {:ok, 200,
            %{
              "type" => "ok",
              "seq" => terminal_seq,
              "state" => "retracted",
              "retracted_at" => retracted_at
            }} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "withdraw",
               "op_id" => op(23),
               "sender_id" => "app:test",
               "message_id" => "edit"
             })

    assert {:ok, 200,
            %{
              "type" => "ok",
              "seq" => ^terminal_seq,
              "state" => "retracted",
              "retracted_at" => ^retracted_at,
              "duplicate" => true
            }} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "withdraw",
               "op_id" => op(23),
               "sender_id" => "app:test",
               "message_id" => "edit"
             })

    assert {:ok, 200, %{"type" => "ok", "entries" => entries}} =
             request(c, "a2", %{"v" => 1, "type" => "feed", "since" => 0})

    assert Enum.map(entries, & &1["kind"]) == ["revision", "revision", "terminal"]
    assert List.last(entries)["terminal_state"] == "retracted"
    assert List.last(entries)["retracted_at"] == retracted_at

    assert {:ok, 200, %{"type" => "ok", "availability" => "not_listening"}} =
             request(c, "a2", %{"v" => 1, "type" => "status", "session_handle" => @handle})

    assert {:ok, 200, %{"type" => "ok", "availability" => "gone"}} =
             request(c, "a2", %{
               "v" => 1,
               "type" => "session_end",
               "session_handle" => @handle,
               "gone" => true
             })

    assert {:ok, 200, %{"type" => "ok", "availability" => "gone"}} =
             request(c, "a2", %{"v" => 1, "type" => "status", "session_handle" => @handle})
  end

  test "origin HTTPS reports duplicate UI/1 document members as an actionable send error", c do
    body =
      ~s|{"v":1,"type":"send","op_id":"#{op(36)}","session_handle":"#{@handle}","message":{"sender_id":"app:test","message_id":"duplicate-ui1-http","created_at":1,"title":"Choose","message_type":"request","answer_ui":{"schema":"attention-client.ui/1","schema":"attention-client.ui/1"}}}|

    assert {:ok, 200,
            %{
              "type" => "error",
              "code" => "request_answer_ui_invalid",
              "reason" => reason
            }} = raw_request(c, "a2", http("POST", "/origin/v1", body))

    assert reason =~ "schema"
    assert reason =~ "member names must be unique"

    assert {:ok, 200, %{"type" => "ok", "entries" => []}} =
             request(c, "a2", %{"v" => 1, "type" => "feed", "since" => 0})
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp relay_request(relay, key, frame) do
    {:ok, line} = Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")
    {:ok, reply} = JSON.decode_line(line <> "\n")
    reply
  end

  defp request(c, name, body),
    do: raw_request(c, name, http("POST", "/origin/v1", JSON.encode!(body)))

  defp upload_request(c, cert, attrs, bytes) do
    name = Base.url_encode64(attrs.name, padding: false)

    raw_request(c, cert, [
      "POST /origin/v1/upload HTTP/1.1\r\n",
      "Host: relay.test\r\n",
      "Content-Type: application/octet-stream\r\n",
      "Content-Length: #{byte_size(bytes)}\r\n",
      "X-Harklight-Op-Id: #{attrs.op_id}\r\n",
      "X-Harklight-Sender-Id: #{attrs.sender_id}\r\n",
      "X-Harklight-Message-Id: #{attrs.message_id}\r\n",
      "X-Harklight-Request-Revision: #{attrs.revision}\r\n",
      "X-Harklight-File-Name: #{name}\r\n\r\n",
      bytes
    ])
  end

  defp fetch_json(c, cert, id),
    do: raw_request(c, cert, "GET /origin/v1/files/#{id} HTTP/1.1\r\nHost: relay.test\r\n\r\n")

  defp fetch_bytes(c, cert, id) do
    with {:ok, socket} <- socket(c, cert) do
      try do
        :ok = :ssl.send(socket, "GET /origin/v1/files/#{id} HTTP/1.1\r\nHost: relay.test\r\n\r\n")
        {:ok, response} = read_all(socket, <<>>)
        [head, body] = :binary.split(response, "\r\n\r\n")
        ["HTTP/1.1", status | _] = head |> String.split("\r\n") |> hd() |> String.split(" ")
        {:ok, String.to_integer(status), body}
      after
        :ssl.close(socket)
      end
    end
  end

  defp stream_request(c, name, handle),
    do:
      raw_request(
        c,
        name,
        "GET /origin/v1/stream?session=#{handle} HTTP/1.1\r\nHost: relay.test\r\n\r\n"
      )

  defp focus_stream_request(c, name) do
    with {:ok, socket} <- socket(c, name),
         :ok <-
           :ssl.send(
             socket,
             "GET /origin/v1/stream?scope=focus HTTP/1.1\r\nHost: relay.test\r\n\r\n"
           ),
         {:ok, response} <- recv_until(socket, "\r\n\r\n", <<>>, 20),
         [_headers, body] <- :binary.split(response, "\r\n\r\n"),
         {:ok, event} <- recv_until(socket, "\n\n", body, 20) do
      {:ok, socket, event}
    else
      error -> error
    end
  end

  defp socket(c, name),
    do:
      HeadlessTLS.connect(
        c.endpoint,
        {Path.join(c.certs, "#{name}.pem"), Path.join(c.certs, "#{name}.key")}
      )

  defp raw_request(c, name, bytes) do
    with {:ok, socket} <- socket(c, name) do
      try do
        :ok = :ssl.send(socket, bytes)
        {:ok, response} = read_all(socket, <<>>)

        case :binary.split(response, "\r\n\r\n") do
          [head, body] ->
            [status_line | _] = String.split(head, "\r\n")
            ["HTTP/1.1", status | _] = String.split(status_line, " ")
            {:ok, reply} = JSON.decode_line(body <> "\n")
            {:ok, String.to_integer(status), reply}

          _ ->
            {:error, :closed_without_response}
        end
      after
        :ssl.close(socket)
      end
    end
  end

  defp read_all(socket, buffer) do
    case :ssl.recv(socket, 0, 5_000) do
      {:ok, bytes} -> read_all(socket, buffer <> bytes)
      {:error, :closed} -> {:ok, buffer}
      error -> error
    end
  end

  defp recv_until(_socket, _marker, _buffer, 0), do: {:error, :timeout}

  defp recv_until(socket, marker, buffer, left) do
    if :binary.match(buffer, marker) != :nomatch do
      {:ok, buffer}
    else
      case :ssl.recv(socket, 0, 1_000) do
        {:ok, bytes} -> recv_until(socket, marker, buffer <> bytes, left - 1)
        error -> error
      end
    end
  end

  defp await_seq(_relay, _key, _seq, 0), do: {:error, :timeout}

  defp await_seq(relay, key, seq, left) do
    case Relay.origin_feed(relay, key, seq - 1) do
      {:ok, %{"cursor" => ^seq}} ->
        :ok

      _ ->
        Process.sleep(10)
        await_seq(relay, key, seq, left - 1)
    end
  end

  defp await_availability(_c, _expected, 0), do: {:error, :timeout}

  defp await_availability(c, expected, left) do
    case request(c, "a2", %{"v" => 1, "type" => "status", "session_handle" => @handle}) do
      {:ok, 200, %{"availability" => ^expected}} ->
        :ok

      _ ->
        Process.sleep(10)
        await_availability(c, expected, left - 1)
    end
  end

  defp await_relay_availability(_c, _expected, 0), do: {:error, :timeout}

  defp await_relay_availability(c, expected, left) do
    case Relay.origin_status(c.relay, c.origin, @handle) do
      {:ok, %{"availability" => ^expected}} ->
        :ok

      _ ->
        Process.sleep(10)
        await_relay_availability(c, expected, left - 1)
    end
  end

  defp http(method, path, body),
    do: [
      "#{method} #{path} HTTP/1.1\r\nHost: relay.test\r\n",
      "Content-Type: application/json\r\nContent-Length: #{byte_size(body)}\r\n",
      "Connection: close\r\n\r\n",
      body
    ]
end
