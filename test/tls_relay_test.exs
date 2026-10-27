defmodule AttentionServer.TLSRelayTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DeviceIdentity, JSON, Relay, TLSFixtures, TLSRelay}
  alias AttentionServer.JSON.DecimalToken

  setup_all do
    dir = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(dir) end)
    %{certs: dir}
  end

  setup %{certs: certs} do
    dir =
      Path.join(System.tmp_dir!(), "attention-tls-state-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")

    {:ok, relay} =
      Relay.start_link(
        store_path: path,
        source_fixture_mode: true,
        enrollment_fixture_mode: true,
        agent_fixture_mode: true
      )

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
    assert :ok = Relay.setup_sender(relay, keys["b1"], "app:test")
    {:ok, tls} = start_tls(relay, certs)

    on_exit(fn ->
      if Process.alive?(tls), do: GenServer.stop(tls)
      if Process.alive?(relay), do: GenServer.stop(relay)
    end)

    %{relay: relay, tls: tls, keys: keys, path: path}
  end

  defp start_tls(relay, certs) do
    TLSRelay.start_link(
      relay: relay,
      certfile: Path.join(certs, "server.pem"),
      keyfile: Path.join(certs, "server.key"),
      cacertfile: Path.join(certs, "ca.pem")
    )
  end

  defp connect(tls, certs, name) do
    :ssl.connect({127, 0, 0, 1}, TLSRelay.port(tls),
      versions: [:"tlsv1.3"],
      verify: :verify_peer,
      cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
      certfile: String.to_charlist(Path.join(certs, "#{name}.pem")),
      keyfile: String.to_charlist(Path.join(certs, "#{name}.key")),
      server_name_indication: ~c"relay.test",
      session_tickets: :disabled,
      active: false,
      mode: :binary,
      packet: :line
    )
  end

  defp send_frame(socket, frame) do
    :ssl.send(socket, JSON.encode!(Map.put_new(frame, "v", 1)) <> "\n")
  end

  defp send_raw_frame(socket, frame), do: :ssl.send(socket, JSON.encode!(frame) <> "\n")

  defp stored_request(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put_new(frame, "v", 1)) <> "\n")

    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp read_frame(socket, timeout \\ 2_000) do
    assert {:ok, line} = :ssl.recv(socket, 0, timeout)
    assert {:ok, frame} = JSON.decode_line(line)
    frame
  end

  defp resync(socket, since \\ 0) do
    assert :ok = send_frame(socket, %{"type" => "resync", "since" => since})
    assert %{"type" => "ok"} = read_frame(socket)
    drain_to_barrier(socket, [])
  end

  defp drain_to_barrier(socket, acc) do
    case read_frame(socket) do
      %{"type" => "resynced"} = barrier -> {Enum.reverse(acc), barrier}
      event -> drain_to_barrier(socket, [event | acc])
    end
  end

  defp op(n),
    do:
      "00000000-0000-4000-8000-" <>
        (n |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(12, "0"))

  defp message(title) do
    %{
      "sender_id" => "app:test",
      "message_id" => "same",
      "created_at" => 1,
      "title" => title,
      "unknown" => %{
        "big" => 9_007_199_254_740_993,
        "decimal" => %DecimalToken{raw: "0.12345678901234567890"}
      }
    }
  end

  test "real TLS 1.3 peers isolate tenants and wait for first resync before push", ctx do
    %{certs: certs, tls: tls, keys: keys} = ctx
    assert {:ok, a1} = connect(tls, certs, "a1")
    assert {:ok, a2} = connect(tls, certs, "a2")
    assert {:ok, b1} = connect(tls, certs, "b1")
    assert {:ok, [protocol: :"tlsv1.3"]} = :ssl.connection_information(a1, [:protocol])
    assert {[], %{"seq" => 0}} = resync(a1)
    assert {[], %{"seq" => 0}} = resync(b1)

    assert :ok =
             send_frame(a1, %{
               "type" => "forward",
               "op_id" => op(1),
               "message" => message("A secret"),
               "person_id" => "B",
               "device_id" => keys["b1"]
             })

    assert %{"type" => "ok", "seq" => 1} = read_frame(a1)
    assert %{"type" => "delivery", "seq" => 1, "origin" => origin} = read_frame(a1)
    assert origin == keys["a1"]
    assert {:error, :timeout} = :ssl.recv(a2, 0, 200)
    assert {[%{"message" => %{"title" => "A secret"}}], %{"seq" => 1}} = resync(a2)

    assert :ok =
             send_frame(b1, %{
               "type" => "forward",
               "op_id" => op(1),
               "message" => message("B secret")
             })

    assert %{"type" => "ok", "seq" => 1} = read_frame(b1)
    assert %{"type" => "delivery", "seq" => 1, "origin" => b_origin} = read_frame(b1)
    assert b_origin == keys["b1"]
    assert {:error, :timeout} = :ssl.recv(a2, 0, 200)
    assert :ok = send_frame(a2, %{"type" => "ack", "seq" => 1})
    assert %{"type" => "ok", "seq" => 1} = read_frame(a2)
    :ssl.close(a1)
    :ssl.close(a2)
    :ssl.close(b1)
  end

  test "revision-bound No comment finalizes stored choice and reply-only requests without a payload",
       ctx do
    %{certs: certs, tls: tls, relay: relay, keys: keys} = ctx
    assert {:ok, sender} = connect(tls, certs, "a1")
    assert {:ok, person} = connect(tls, certs, "a2")

    on_exit(fn ->
      :ssl.close(sender)
      :ssl.close(person)
    end)

    assert {[], %{"seq" => 0}} = resync(sender)
    assert {[], %{"seq" => 0}} = resync(person)

    choice =
      message("Choice request")
      |> Map.put("message_id", "choice-request")
      |> Map.put("answer_ui", %{"kind" => "choice", "items" => ["yes", "no"]})

    assert %{"type" => "ok", "seq" => 1} =
             stored_request(relay, keys["a1"], %{
               "type" => "forward",
               "op_id" => op(501),
               "message" => choice
             })

    assert %{"type" => "delivery", "message_id" => "choice-request"} = read_frame(sender)
    assert %{"type" => "delivery", "message_id" => "choice-request"} = read_frame(person)

    # Extra fields are rejected. In particular, No comment cannot smuggle a
    # draft answer or be accepted as an empty text answer.
    assert :ok =
             send_raw_frame(person, %{
               "type" => "answer_no_comment",
               "request_id" => "choice-request",
               "answer" => ""
             })

    assert %{"type" => "error", "reason" => "invalid_field"} = read_frame(person)

    # The old two-field frame has no revision identity and receives no grace period.
    assert :ok =
             send_raw_frame(person, %{
               "type" => "answer_no_comment",
               "request_id" => "choice-request"
             })

    assert %{"type" => "error", "reason" => "invalid_field"} = read_frame(person)

    assert :ok =
             send_raw_frame(person, %{
               "type" => "answer_no_comment",
               "request_id" => "choice-request",
               "request_revision" => 1
             })

    assert %{"type" => "ok", "seq" => 2, "terminal_kind" => "no_comment"} =
             read_frame(person)

    choice_person_terminal = read_frame(person)
    assert choice_person_terminal["type"] == "lifecycle"
    assert choice_person_terminal["seq"] == 2
    assert choice_person_terminal["terminal_kind"] == "no_comment"
    assert choice_person_terminal["state"] == "answered"
    refute Map.has_key?(choice_person_terminal, "answer")

    choice_sender_terminal = read_frame(sender)
    assert choice_sender_terminal["type"] == "lifecycle"
    assert choice_sender_terminal["seq"] == 2
    assert choice_sender_terminal["terminal_kind"] == "no_comment"
    refute Map.has_key?(choice_sender_terminal, "answer")

    # Repeating the exact revision-bound action returns the existing final cursor.
    assert :ok =
             send_raw_frame(person, %{
               "type" => "answer_no_comment",
               "request_id" => "choice-request",
               "request_revision" => 1
             })

    assert %{
             "type" => "ok",
             "seq" => 2,
             "terminal_kind" => "no_comment",
             "duplicate" => true
           } = read_frame(person)

    reply_only =
      message("Reply-only request")
      |> Map.put("message_id", "reply-only-request")
      |> Map.put("body", "Please write a response")

    assert %{"type" => "ok", "seq" => 3} =
             stored_request(relay, keys["a1"], %{
               "type" => "forward",
               "op_id" => op(502),
               "message" => reply_only
             })

    assert %{"type" => "delivery", "message_id" => "reply-only-request"} = read_frame(sender)
    assert %{"type" => "delivery", "message_id" => "reply-only-request"} = read_frame(person)

    assert :ok =
             send_raw_frame(person, %{
               "type" => "answer_no_comment",
               "request_id" => "reply-only-request",
               "request_revision" => 1
             })

    assert %{"type" => "ok", "seq" => 4, "terminal_kind" => "no_comment"} =
             read_frame(person)

    reply_person_terminal = read_frame(person)
    assert reply_person_terminal["type"] == "lifecycle"
    assert reply_person_terminal["seq"] == 4
    assert reply_person_terminal["terminal_kind"] == "no_comment"
    refute Map.has_key?(reply_person_terminal, "answer")

    reply_sender_terminal = read_frame(sender)
    assert reply_sender_terminal["type"] == "lifecycle"
    assert reply_sender_terminal["seq"] == 4
    assert reply_sender_terminal["terminal_kind"] == "no_comment"
    refute Map.has_key?(reply_sender_terminal, "answer")

    assert {:ok, _events, 4} = Relay.events(relay, keys["a1"], 0)
  end

  test "new authenticated session fences old socket and revocation fences current socket", ctx do
    %{certs: certs, tls: tls, relay: relay, keys: keys} = ctx
    assert {:ok, old} = connect(tls, certs, "a1")
    assert {[], %{"seq" => 0}} = resync(old)
    assert {:ok, current} = connect(tls, certs, "a1")
    assert {[], %{"seq" => 0}} = resync(current)
    assert {:error, :closed} = :ssl.recv(old, 0, 2_000)
    assert :ok = Relay.revoke_device(relay, keys["a1"])
    assert {:error, :closed} = :ssl.recv(current, 0, 2_000)
    assert {:ok, denied} = connect(tls, certs, "a1")
    assert :ok = send_frame(denied, %{"type" => "resync", "since" => 0})
    assert {:error, :closed} = :ssl.recv(denied, 0, 2_000)
  end

  test "unenrolled and invalid certificate paths never reach relay frames", ctx do
    %{certs: certs, tls: tls, relay: relay} = ctx
    assert {:ok, unknown} = connect(tls, certs, "unknown")
    _ = send_frame(unknown, %{"type" => "resync", "since" => 0})
    assert {:error, :closed} = :ssl.recv(unknown, 0, 2_000)

    for name <- ~w(expired future) do
      assert {:ok, _key} =
               Relay.setup_device_certificate(
                 relay,
                 "A",
                 TLSFixtures.certificate_der!(certs, name)
               )

      case connect(tls, certs, name) do
        {:ok, socket} -> assert {:error, _} = :ssl.recv(socket, 0, 2_000)
        {:error, _} -> :ok
      end
    end

    wrong_key =
      :ssl.connect({127, 0, 0, 1}, TLSRelay.port(tls),
        versions: [:"tlsv1.3"],
        verify: :verify_peer,
        cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
        certfile: String.to_charlist(Path.join(certs, "a1.pem")),
        keyfile: String.to_charlist(Path.join(certs, "b1.key")),
        server_name_indication: ~c"relay.test",
        active: false
      )

    assert_tls_unusable(wrong_key)

    assert {:error, _} =
             :ssl.connect({127, 0, 0, 1}, TLSRelay.port(tls),
               versions: [:"tlsv1.3"],
               verify: :verify_peer,
               cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
               certfile: String.to_charlist(Path.join(certs, "a1.pem")),
               keyfile: String.to_charlist(Path.join(certs, "a1.key")),
               server_name_indication: ~c"wrong.test",
               active: false
             )

    no_certificate =
      :ssl.connect({127, 0, 0, 1}, TLSRelay.port(tls),
        versions: [:"tlsv1.3"],
        verify: :verify_peer,
        cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
        server_name_indication: ~c"relay.test",
        active: false
      )

    assert_tls_unusable(no_certificate)

    untrusted_server =
      :ssl.connect({127, 0, 0, 1}, TLSRelay.port(tls),
        versions: [:"tlsv1.3"],
        verify: :verify_peer,
        cacertfile: String.to_charlist(Path.join(certs, "untrusted-ca.pem")),
        certfile: String.to_charlist(Path.join(certs, "a1.pem")),
        keyfile: String.to_charlist(Path.join(certs, "a1.key")),
        server_name_indication: ~c"relay.test",
        active: false
      )

    assert_tls_unusable(untrusted_server)

    rogue_client = connect(tls, certs, "rogue")
    assert_tls_unusable(rogue_client)

    old_tls =
      :ssl.connect({127, 0, 0, 1}, TLSRelay.port(tls),
        versions: [:"tlsv1.2"],
        verify: :verify_peer,
        cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
        certfile: String.to_charlist(Path.join(certs, "a1.pem")),
        keyfile: String.to_charlist(Path.join(certs, "a1.key")),
        server_name_indication: ~c"relay.test",
        active: false
      )

    assert_tls_unusable(old_tls)

    assert {:ok, [], 0} = Relay.events(relay, ctx.keys["a1"], 0)
  end

  defp assert_tls_unusable({:error, _}), do: :ok

  defp assert_tls_unusable({:ok, socket}) do
    assert {:error, _} = :ssl.recv(socket, 0, 2_000)
    :ssl.close(socket)
  end

  test "SPKI identity agrees with independent OpenSSL DER extraction", %{
    certs: certs,
    keys: keys,
    relay: relay
  } do
    assert {:ok, key} = DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "a1"))
    assert key == keys["a1"]

    assert {:error, :key_in_use} =
             Relay.setup_device_certificate(relay, "B", TLSFixtures.certificate_der!(certs, "a1"))

    {public_pem, 0} =
      System.cmd("openssl", ["x509", "-in", Path.join(certs, "a1.pem"), "-pubkey", "-noout"])

    public_path = Path.join(certs, "a1-public.pem")
    File.write!(public_path, public_pem)

    {spki_der, 0} =
      System.cmd("openssl", ["pkey", "-pubin", "-in", public_path, "-outform", "DER"])

    assert key == Base.encode16(:crypto.hash(:sha256, spki_der), case: :lower)
  end

  test "queued history cannot escape after revocation and restart replays committed state", ctx do
    %{certs: certs, tls: tls, relay: relay, path: path, keys: keys} = ctx
    assert {:ok, a1} = connect(tls, certs, "a1")
    assert {:ok, a2} = connect(tls, certs, "a2")
    assert {[], %{"seq" => 0}} = resync(a1)

    assert %{"type" => "ok", "seq" => 1} =
             stored_request(relay, keys["a1"], %{
               "type" => "forward",
               "op_id" => op(99),
               "message" => message("durable")
             })

    assert %{"type" => "delivery", "seq" => 1} = read_frame(a1)
    assert {:error, :timeout} = :ssl.recv(a2, 0, 200)
    assert :ok = Relay.revoke_device(relay, keys["a2"])
    assert {:error, :closed} = :ssl.recv(a2, 0, 2_000)
    :ssl.close(a1)
    GenServer.stop(tls)
    GenServer.stop(relay)

    assert {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, restarted_tls} = start_tls(restarted, certs)
    assert {:ok, origin} = connect(restarted_tls, certs, "a1")

    assert {[], %{"seq" => 1}} = resync(origin, 1)
    assert :ok = send_frame(origin, %{"type" => "ack", "seq" => 1})
    assert %{"type" => "error", "reason" => "invalid_cursor"} = read_frame(origin)

    assert {[%{"type" => "delivery", "seq" => 1, "origin" => origin_id}], %{"seq" => 1}} =
             resync(origin)

    assert origin_id == keys["a1"]
    assert :ok = send_frame(origin, %{"type" => "ack", "seq" => 1})
    assert %{"type" => "ok", "seq" => 1} = read_frame(origin)
    assert {:ok, revoked} = connect(restarted_tls, certs, "a2")
    assert :ok = send_frame(revoked, %{"type" => "resync", "since" => 0})
    assert {:error, :closed} = :ssl.recv(revoked, 0, 2_000)
    :ssl.close(origin)
    GenServer.stop(restarted_tls)
    GenServer.stop(restarted)
  end

  test "stopping only the listener fences accepted sockets before it returns", ctx do
    %{certs: certs, tls: tls, relay: relay, keys: keys} = ctx
    assert {:ok, a1} = connect(tls, certs, "a1")
    assert {:ok, a2} = connect(tls, certs, "a2")
    assert {[], %{"seq" => 0}} = resync(a1)
    assert {[], %{"seq" => 0}} = resync(a2)

    assert %{"type" => "ok", "seq" => 1} =
             stored_request(relay, keys["a1"], %{
               "type" => "forward",
               "op_id" => op(201),
               "message" => message("before stop")
             })

    assert %{"type" => "delivery", "seq" => 1} = read_frame(a1)
    assert %{"type" => "delivery", "seq" => 1} = read_frame(a2)

    assert :ok = GenServer.stop(tls)
    assert Process.alive?(relay)
    assert {:error, :closed} = :ssl.recv(a1, 0, 2_000)
    assert {:error, :closed} = :ssl.recv(a2, 0, 2_000)
    assert {:error, :listener_stopped} = Relay.open_tls_session(relay, keys["a1"], :dummy, tls)

    _ =
      send_frame(a1, %{
        "type" => "forward",
        "op_id" => op(202),
        "message" => %{message("after stop") | "message_id" => "new"}
      })

    _ =
      send_frame(a2, %{
        "type" => "answer",
        "op_id" => op(203),
        "sender_id" => "app:test",
        "message_id" => "same",
        "answer" => "must not commit"
      })

    _ = send_frame(a1, %{"type" => "ack", "seq" => 1})
    assert {:ok, [delivery], 1} = Relay.events(relay, keys["a1"], 0)
    assert delivery["type"] == "delivery"
    assert {:ok, 0} = Relay.acknowledged_cursor(relay, keys["a1"])
    assert is_nil(:sys.get_state(relay).store.engine.messages[{"A", "app:test", "same"}].terminal)
  end

  test "authenticated TLS origin alone obtains winner proof and delegated source reports",
       ctx do
    %{certs: certs, tls: tls, relay: relay, keys: keys} = ctx
    assert {:ok, a1} = connect(tls, certs, "a1")
    assert {:ok, a2} = connect(tls, certs, "a2")
    assert {[], %{"seq" => 0}} = resync(a1)
    assert {[], %{"seq" => 0}} = resync(a2)

    assert %{"type" => "ok", "seq" => 1} =
             stored_request(relay, keys["a1"], %{
               "type" => "forward",
               "op_id" => op(401),
               "message" => message("PRIVATE SOURCE CONTENT")
             })

    assert %{"type" => "delivery"} = read_frame(a1)
    assert %{"type" => "delivery"} = read_frame(a2)

    query = %{
      "type" => "answer_receipt",
      "receipt_version" => 1,
      "sender_id" => "app:test",
      "message_id" => "same",
      "terminal_seq" => 2
    }

    assert :ok = send_frame(a1, query)
    assert %{"type" => "ok", "status" => "pending"} = read_frame(a1)
    assert :ok = send_frame(a2, query)
    assert %{"type" => "error", "reason" => "not_found"} = read_frame(a2)

    assert :ok =
             send_frame(a2, %{
               "type" => "answer",
               "op_id" => op(402),
               "sender_id" => "app:test",
               "message_id" => "same",
               "answer" => nil
             })

    assert %{"type" => "ok", "seq" => 2} = read_frame(a2)
    assert %{"type" => "lifecycle", "seq" => 2} = read_frame(a2)
    assert %{"type" => "lifecycle", "seq" => 2} = read_frame(a1)
    assert :ok = send_frame(a1, query)
    assert %{"type" => "ok", "receipt" => proof} = read_frame(a1)

    assert proof["actor"] == %{
             "kind" => "device",
             "id" => keys["a2"],
             "key_fingerprint" => keys["a2"]
           }

    assert proof["terminal_kind"] == "answer"

    assert proof["has_answer"] == true and Map.has_key?(proof, "answer") and
             is_nil(proof["answer"])

    refute String.contains?(JSON.encode!(proof), "PRIVATE SOURCE CONTENT")

    bind = %{
      "type" => "source_bind",
      "op_id" => op(403),
      "sender_id" => "app:test",
      "message_id" => "same",
      "acceptance_seq" => 1,
      "source_instance_id" => "fixture-source",
      "request_id" => "req",
      "request_revision" => "r1"
    }

    assert :ok = send_frame(a1, bind)
    assert %{"type" => "error", "reason" => "forbidden"} = read_frame(a1)

    assert :ok =
             Relay.grant_source_report(
               relay,
               "A",
               keys["a1"],
               "fixture-source",
               ["app:test"],
               "recorded-source-owner-fixture"
             )

    assert :ok = send_frame(a1, bind)
    assert %{"type" => "ok", "acceptance_seq" => 1} = read_frame(a1)

    result = %{
      "type" => "source_result",
      "op_id" => op(404),
      "sender_id" => "app:test",
      "message_id" => "same",
      "terminal_seq" => 2,
      "source_instance_id" => "fixture-source",
      "request_id" => "req",
      "request_revision" => "r1",
      "result_revision" => 1,
      "status" => "refused",
      "result_ref" => "fixture-refusal-1",
      "reason" => "unmapped_actor"
    }

    assert :ok = send_frame(a1, result)

    assert %{"type" => "ok", "source_status" => "source_reported", "status" => "refused"} =
             read_frame(a1)

    assert :ok = Relay.revoke_source_report(relay, "A", keys["a1"], "fixture-source")
    assert :ok = send_frame(a1, %{result | "op_id" => op(405), "result_revision" => 2})
    assert %{"type" => "error", "reason" => "forbidden"} = read_frame(a1)
    :ssl.close(a1)
    :ssl.close(a2)
  end

  test "G8 TLS release observes current target and grant revocation order", ctx do
    %{certs: certs, tls: tls, relay: relay, keys: keys} = ctx
    a1_key = keys["a1"]
    a2_key = keys["a2"]
    target = %{"kind" => "device", "id" => a2_key, "key_fingerprint" => a2_key}
    grant = op(501)

    assert :ok =
             Relay.grant_enrollment_read_fixture(
               relay,
               "A",
               a1_key,
               grant,
               "source-X",
               target,
               "owner-A-fixture"
             )

    assert {:ok, a1} = connect(tls, certs, "a1")
    assert {[], %{"seq" => 0}} = resync(a1)

    request = %{
      "type" => "enrollment_readback",
      "proof_version" => 1,
      "grant_id" => grant,
      "source_instance_id" => "source-X",
      "target" => target
    }

    assert :ok = send_frame(a1, request)
    assert %{"type" => "ok", "proof" => active} = read_frame(a1)
    assert active["state"] == "active" and active["enrollment_revision"] == 1

    [{session, _}] = Map.to_list(:sys.get_state(relay).sessions)
    staged_request = JSON.encode!(Map.put(request, "v", 1)) <> "\n"
    assert {:ok, [staged]} = Relay.session_request_line(relay, session, staged_request)
    assert {:ok, %{"proof" => %{"state" => "active"}}} = JSON.decode_line(staged)

    assert :ok = Relay.revoke_device(relay, a2_key)
    assert :ok = Relay.session_send(relay, session, [staged])
    assert %{"proof" => revoked} = read_frame(a1)
    assert revoked["state"] == "revoked" and revoked["enrollment_revision"] == 2

    assert {:ok, [staged_again]} = Relay.session_request_line(relay, session, staged_request)
    assert :ok = Relay.revoke_enrollment_read_fixture(relay, "A", grant)
    assert :ok = Relay.session_send(relay, session, [staged_again])
    assert %{"type" => "error", "reason" => "not_found"} = read_frame(a1)

    agent_id = op(502)
    agent_key = String.duplicate("e", 64)

    agent_target = %{
      "kind" => "agent",
      "id" => agent_id,
      "key_fingerprint" => agent_key
    }

    assert :ok = Relay.setup_agent_fixture(relay, "A", agent_id, agent_key, ["app:test"])

    assert :ok =
             Relay.grant_enrollment_read_fixture(
               relay,
               "A",
               a1_key,
               op(503),
               "source-X",
               agent_target,
               "owner-A-fixture"
             )

    agent_request = %{
      request
      | "grant_id" => op(503),
        "target" => agent_target
    }

    assert :ok = send_frame(a1, agent_request)
    assert %{"proof" => %{"state" => "active", "target" => ^agent_target}} = read_frame(a1)

    assert :ok = Relay.revoke_agent_fixture(relay, agent_key)
    assert :ok = send_frame(a1, agent_request)

    assert %{"proof" => %{"state" => "revoked", "enrollment_revision" => 2}} =
             read_frame(a1)

    # A valid staged proof cannot escape when the connector itself is fenced.
    assert {:ok, [staged_agent]} =
             Relay.session_request_line(
               relay,
               session,
               JSON.encode!(Map.put(agent_request, "v", 1)) <> "\n"
             )

    assert :ok = Relay.revoke_device(relay, a1_key)
    assert {:error, :fenced} = Relay.session_send(relay, session, [staged_agent])
    assert {:error, :closed} = :ssl.recv(a1, 0, 2_000)
  end
end
