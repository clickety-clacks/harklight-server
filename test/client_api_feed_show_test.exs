defmodule AttentionServer.ClientAPIFeedShowTest do
  # A device's held shows on its change feed (contract rulings round 11 r4
  # §3b): an ephemeral `event: show` with no `id:` line, from the Relay's
  # memory-only focus state, never in a batch and never replayed.
  use ExUnit.Case, async: false

  import AttentionServer.ClientAPIFeedClient

  alias AttentionServer.{ChangeLog, ClientAPI, JSON, Relay, Server, TLSFixtures}
  alias AttentionServer.ClientAPI.{Context, FeedStream, Writes}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @agent String.duplicate("e", 64)

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "harklight-show-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    flags = Path.join(dir, "flags.json")
    File.write!(flags, ~s({"client_api_v2":true}))

    relay =
      start_supervised!(
        {Relay,
         store_path: Path.join(dir, "state.sqlite3"),
         feature_flags_path: flags,
         agent_fixture_mode: true},
        restart: :temporary
      )

    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", @agent)
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert {:ok, reader} =
             Relay.setup_device_certificate(relay, "A", TLSFixtures.certificate_der!(certs, "a1"))

    forward(relay, "one", 1)

    # The agent may push views, as an owner's switches allow.
    :sys.replace_state(relay, fn state ->
      engine = state.store.engine
      device = %{engine.devices[@agent] | see_focus: true, push_view: true}
      put_in(state.store.engine, %{engine | devices: Map.put(engine.devices, @agent, device)})
    end)

    # A held show needs an accepted focus report from the device.
    assert {:ok, 200, _, false} =
             Writes.focus(relay, reader, %{"screen" => "inbox", "ttl_s" => 120})

    listener =
      start_supervised!(
        {ClientAPI.Listener,
         [
           relay: relay,
           certfile: Path.join(certs, "server.pem"),
           keyfile: Path.join(certs, "server.key"),
           cacertfile: Path.join(certs, "ca.pem")
         ]},
        id: make_ref()
      )

    %{
      relay: relay,
      reader: reader,
      port: ClientAPI.Listener.port(listener),
      certs: certs,
      dir: dir
    }
  end

  defp forward(relay, id, n) do
    frame = %{
      "v" => 1,
      "type" => "forward",
      "op_id" => "00000000-0000-4000-8000-" <> String.pad_leading("#{n}", 12, "0"),
      "session_handle" => @handle,
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => id,
        "created_at" => n,
        "title" => id
      }
    }

    assert {:ok, line} = Relay.request_line(relay, "origin", JSON.encode!(frame) <> "\n")
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(line)
  end

  defp push_view(relay, target) do
    Relay.view_show(relay, @agent, %{
      "sender_id" => "app:test",
      "message_id" => "one",
      "target" => target
    })
  end

  defp head(relay), do: elem(ChangeLog.head(:sys.get_state(relay).store, "A"), 1)

  # Reads events until a `show`, skipping heartbeats; returns the events seen.
  defp until_show(conn, feed, acc \\ []) do
    {conn, feed, [event]} = events(conn, feed, 1)

    if event[:event] == "show",
      do: {conn, feed, event, Enum.reverse(acc)},
      else: until_show(conn, feed, [event | acc])
  end

  defp heartbeat_fast do
    Application.put_env(:attention_server, :client_api_feed_heartbeat_ms, 50)
    on_exit(fn -> Application.delete_env(:attention_server, :client_api_feed_heartbeat_ms) end)
  end

  test "a held show reaches the device feed but not the same key's agent feed", ctx do
    heartbeat_fast()
    client = connect(ctx.port, ctx.certs, "a1")
    {client, client_feed} = open_feed(client, "?after=#{head(ctx.relay)}")
    {client, client_feed} = await_open(client, client_feed)

    agent = connect(ctx.port, ctx.certs, "a1")

    {:ok, agent, ref} =
      Mint.HTTP.request(agent, "GET", "/v2/agent/changes", [{"accept", "text/event-stream"}], nil)

    agent_feed = %{ref: ref, buffer: "", status: nil, headers: [], done: false}
    {agent, agent_feed} = await_open(agent, agent_feed)
    assert client_feed.status == 200 and agent_feed.status == 200

    assert {:ok, %{"delivery" => "held"}} = push_view(ctx.relay, "chat")
    {_client, _client_feed, event, _prior} = until_show(client, client_feed)
    assert %{event: "show", data: %{"target" => "chat"}} = event
    {_agent, _agent_feed, agent_events} = events(agent, agent_feed, 3)
    assert agent_events == List.duplicate(%{event: "heartbeat", data: %{}}, 3)
  end

  defmodule PausedStream do
    def send_chunked(test, 200, _headers) do
      send(test, {:stream_paused, self()})

      receive do
        :resume -> {:ok, "", test}
      end
    end

    def chunk(test, data) do
      send(test, {:stream_chunk, IO.iodata_to_binary(data)})
      :ok
    end
  end

  defp pause_before_first_chunk(ctx) do
    test = self()
    cursor = head(ctx.relay)

    stream =
      Task.async(fn ->
        conn = %{Plug.Test.conn(:get, "/v2/changes") | adapter: {PausedStream, test}}

        stream_ctx = %Context{
          principal: %{kind: :device, key: ctx.reader, person: "A"},
          relay: ctx.relay,
          request_id: "paused-show-test"
        }

        FeedStream.serve(conn, stream_ctx, cursor)
      end)

    assert_receive {:stream_paused, pid}, 1_000
    assert pid == stream.pid
    stream
  end

  test "the stream writes only the current show when a newer show arrives before delivery", ctx do
    heartbeat_fast()
    stream = pause_before_first_chunk(ctx)
    assert {:ok, %{"delivery" => "held"}} = push_view(ctx.relay, "message")
    assert {:ok, %{"delivery" => "held"}} = push_view(ctx.relay, "chat")

    send(stream.pid, :resume)

    assert_receive {:stream_chunk, "event: show\ndata: " <> data}, 1_000
    assert {:ok, %{"target" => "chat"}} = JSON.decode_line(data)

    assert_receive {:stream_chunk, next}, 1_000
    assert next == "event: heartbeat\ndata: {}\n\n"

    assert :ok = Relay.revoke_device(ctx.relay, ctx.reader)

    assert_receive {:stream_chunk, "event: closing\ndata: {\"reason\":\"revoked\"}\n\n"}, 1_000
    Task.await(stream)
    refute_received {:stream_chunk, "event: show\ndata: " <> _}
  end

  test "the stream closes without a show when the device is revoked before delivery", ctx do
    stream = pause_before_first_chunk(ctx)
    assert {:ok, %{"delivery" => "held"}} = push_view(ctx.relay, "chat")
    assert :ok = Relay.revoke_device(ctx.relay, ctx.reader)

    send(stream.pid, :resume)

    assert_receive {:stream_chunk, first}, 1_000
    assert first == "event: closing\ndata: {\"reason\":\"revoked\"}\n\n"
    Task.await(stream)
    refute_received {:stream_chunk, "event: show\ndata: " <> _}
  end

  test "signing out suppresses live delivery and preserves the held show", ctx do
    heartbeat_fast()
    stream = pause_before_first_chunk(ctx)
    assert {:ok, %{"delivery" => "held"}} = push_view(ctx.relay, "chat")

    assert {:ok, 200, signed_out, false} =
             Writes.focus(ctx.relay, ctx.reader, %{"screen" => "signed_out", "ttl_s" => 120})

    refute Map.has_key?(signed_out, "show")
    send(stream.pid, :resume)

    assert_receive {:stream_chunk, first}, 1_000
    assert first == "event: heartbeat\ndata: {}\n\n"

    assert {:ok, 200, %{"show" => %{"target" => "chat"}}, false} =
             Writes.focus(ctx.relay, ctx.reader, %{"screen" => "inbox", "ttl_s" => 120})

    assert :ok = Relay.revoke_device(ctx.relay, ctx.reader)
    assert_receive {:stream_chunk, "event: closing\ndata: {\"reason\":\"revoked\"}\n\n"}, 1_000
    Task.await(stream)
    refute_received {:stream_chunk, "event: show\ndata: " <> _}
  end

  test "a held show reaches the device's open feed as an id-less show event", ctx do
    conn = connect(ctx.port, ctx.certs, "a1")
    {conn, feed} = open_feed(conn, "?after=#{head(ctx.relay)}")
    {conn, feed} = await_open(conn, feed)

    sent_at = System.system_time(:millisecond)
    assert {:ok, %{"delivery" => "held"}} = push_view(ctx.relay, "chat")
    {_conn, _feed, show, _before} = until_show(conn, feed)

    # No id: line, so Last-Event-ID never moves.
    refute Map.has_key?(show, :id)

    assert %{
             "show_id" => "s" <> _,
             "device_id" => device_id,
             "item_id" => "i" <> _,
             "target" => "chat",
             "held_ms" => held_ms
           } = show.data

    assert device_id == ctx.reader
    assert is_integer(held_ms) and held_ms >= 0
    assert held_ms <= System.system_time(:millisecond) - sent_at + 50

    # It is the same show the device's next focus write returns.
    assert {:ok, 200, %{"show" => %{"show_id" => show_id}}, false} =
             Writes.focus(ctx.relay, ctx.reader, %{"screen" => "inbox", "ttl_s" => 120})

    assert show_id == show.data["show_id"]
  end

  test "the enrolled TLS route uses the push grant and delivers one id on focus and SSE", ctx do
    assert {:ok, origin} =
             Relay.setup_device_certificate(
               ctx.relay,
               "A",
               TLSFixtures.certificate_der!(ctx.certs, "a2")
             )

    origin_conn = connect(ctx.port, ctx.certs, "a2")
    reader_conn = connect(ctx.port, ctx.certs, "a1")
    {reader_conn, feed} = open_feed(reader_conn, "?after=#{head(ctx.relay)}")
    {reader_conn, feed} = await_open(reader_conn, feed)

    assert {:ok, %{"items" => [%{"item_id" => item}]}} =
             ClientAPI.Reads.inbox(ctx.relay, ctx.reader)

    request = %{"item_id" => item, "target" => "chat"}
    key = "00000000-0000-4000-8000-000000000007"

    {origin_conn, refused} = show_request(origin_conn, key, request)
    assert refused.status == 403
    assert body(refused)["error"] == "forbidden"

    # Enable only the existing push switch on the certificate used by the request.
    :sys.replace_state(ctx.relay, fn state ->
      put_in(state.store.engine.devices[origin].push_view, true)
    end)

    rev = head(ctx.relay)
    {origin_conn, accepted} = show_request(origin_conn, key, request)
    assert accepted.status == 200
    assert %{"show_id" => id, "device_id" => reader, "state" => "sent"} = body(accepted)
    assert reader == ctx.reader

    {reader_conn, feed, received, _} = until_show(reader_conn, feed)

    assert %{event: "show", data: %{"show_id" => ^id, "target" => "chat", "held_ms" => age}} =
             received

    refute Map.has_key?(received, :id)
    assert age >= 0

    report = JSON.encode!(%{"screen" => "inbox", "ttl_s" => 120})

    assert {:ok, reader_conn, ref} =
             Mint.HTTP.request(reader_conn, "PUT", "/v2/devices/me/focus", [], report)

    {reader_conn, focused} = collect(reader_conn, ref)
    assert focused.status == 200
    assert %{"show" => %{"show_id" => ^id, "held_ms" => later_age}} = body(focused)
    assert later_age >= age

    {origin_conn, replay} = show_request(origin_conn, key, request)
    assert replay.status == 200
    assert {"idempotent-replayed", "true"} in replay.headers
    assert body(replay) == body(accepted)

    {origin_conn, conflict} = show_request(origin_conn, key, %{request | "target" => "message"})
    assert conflict.status == 409
    assert body(conflict)["error"] == "idempotency_conflict"

    assert {:ok, origin_conn, ref} =
             Mint.HTTP.request(
               origin_conn,
               "POST",
               "/v2/agent/sessions/unknown/end",
               [{"idempotency-key", key}],
               "{}"
             )

    {origin_conn, cross_route} = collect(origin_conn, ref)
    assert cross_route.status == 409
    assert body(cross_route)["error"] == "idempotency_conflict"
    assert head(ctx.relay) == rev

    Mint.HTTP.close(origin_conn)
    Mint.HTTP.close(reader_conn)
    assert feed.status == 200
  end

  test "the TLS route holds an explicitly named device show until focus refresh", ctx do
    assert {:ok, origin} =
             Relay.setup_device_certificate(
               ctx.relay,
               "A",
               TLSFixtures.certificate_der!(ctx.certs, "a2")
             )

    :sys.replace_state(ctx.relay, fn state ->
      state = put_in(state.store.engine.devices[origin].push_view, true)
      put_in(state.store.engine.devices[ctx.reader][:label], "Reader’s Tablet")
    end)

    conn = connect(ctx.port, ctx.certs, "a2")

    assert {:ok, %{"items" => [%{"item_id" => item}]}} =
             ClientAPI.Reads.inbox(ctx.relay, ctx.reader)

    request = %{"item_id" => item, "target" => "message", "device_name" => "READER'S TABLET"}
    key = "00000000-0000-4000-8000-000000000008"
    {conn, invalid} = show_request(conn, key, Map.put(request, "device_id", ctx.reader))
    assert invalid.status == 400
    assert %{"error" => "invalid_field", "field" => "device_id"} = body(invalid)

    {conn, accepted} = show_request(conn, key, request)
    assert accepted.status == 200

    assert %{
             "show_id" => id,
             "device_id" => reader,
             "device_name" => "Reader’s Tablet",
             "state" => "held"
           } = body(accepted)

    assert reader == ctx.reader

    assert {:ok, 200, %{"show" => %{"show_id" => ^id, "item_id" => ^item}}, false} =
             Writes.focus(ctx.relay, ctx.reader, %{"screen" => "inbox", "ttl_s" => 120})

    explicit_key = "00000000-0000-4000-8000-000000000009"
    explicit = Map.put(Map.delete(request, "device_name"), "device_id", origin)
    {conn, unlisted} = show_request(conn, explicit_key, explicit)
    assert unlisted.status == 404

    {conn, accepted} =
      show_request(conn, explicit_key, %{explicit | "device_id" => ctx.reader})

    assert accepted.status == 200
    assert %{"show_id" => explicit_id, "device_id" => ^reader, "state" => "held"} = body(accepted)
    assert explicit_id != id
    Mint.HTTP.close(conn)
  end

  test "the flag-off device listener has no agent show route", ctx do
    flags = Path.join(ctx.dir, "flags-disabled.json")
    File.write!(flags, ~s({"client_api_v2":false}))

    server =
      start_supervised!(
        {Server,
         store_path: Path.join(ctx.dir, "disabled.sqlite3"),
         feature_flags_path: flags,
         enrollment_enabled: false,
         certfile: Path.join(ctx.certs, "server.pem"),
         keyfile: Path.join(ctx.certs, "server.key"),
         cacertfile: Path.join(ctx.certs, "ca.pem")}
      )

    refute Server.client_api_v2?(server)
    relay = Server.relay(server)
    assert :ok = Relay.setup_person(relay, "A")

    assert {:ok, _key} =
             Relay.setup_device_certificate(
               relay,
               "A",
               TLSFixtures.certificate_der!(ctx.certs, "a2")
             )

    conn = connect(Server.port(server), ctx.certs, "a2", :http1)

    {conn, refused} =
      show_request(conn, "00000000-0000-4000-8000-000000000010", %{
        "item_id" => "i1",
        "target" => "message"
      })

    assert refused.status == 400
    assert refused.body == ""
    refute Enum.any?(refused.headers, fn {name, _} -> name == "request-id" end)
    Mint.HTTP.close(conn)
  end

  defmodule ControlledShowStream do
    def send_chunked({test, _} = opts, 200, _headers) do
      send(test, {:controlled_open, self()})
      {:ok, "", opts}
    end

    def chunk({test, block_backlog?}, data) do
      case IO.iodata_to_binary(data) do
        "event: show\n" <> _ ->
          wait_for_result(test, :show)

        _ ->
          if block_backlog? and is_nil(Process.put(:controlled_backlog, true)),
            do: wait_for_result(test, :backlog),
            else: :ok
      end
    end

    defp wait_for_result(test, kind) do
      send(test, {:controlled_chunk, self(), kind})

      receive do
        {:chunk_result, result} -> result
      end
    end
  end

  defp controlled_writer(ctx, cursor) do
    test = self()

    writer =
      Task.async(fn ->
        conn = %{
          Plug.Test.conn(:get, "/v2/changes")
          | adapter: {ControlledShowStream, {test, cursor == 0}}
        }

        stream_ctx = %Context{
          principal: %{kind: :device, key: ctx.reader, person: "A"},
          relay: ctx.relay,
          request_id: "controlled-show-test"
        }

        FeedStream.serve(conn, stream_ctx, cursor)
      end)

    assert_receive {:controlled_open, pid}, 1_000
    assert pid == writer.pid
    writer
  end

  defp show_in_request(ctx) do
    assert {:ok, origin} =
             Relay.setup_device_certificate(
               ctx.relay,
               "A",
               TLSFixtures.certificate_der!(ctx.certs, "a2")
             )

    :sys.replace_state(ctx.relay, fn state ->
      put_in(state.store.engine.devices[origin].push_view, true)
    end)

    assert {:ok, %{"items" => [%{"item_id" => item}]}} =
             ClientAPI.Reads.inbox(ctx.relay, ctx.reader)

    Task.async(fn ->
      conn = connect(ctx.port, ctx.certs, "a2")

      {conn, response} =
        show_request(conn, "00000000-0000-4000-8000-000000000011", %{
          "item_id" => item,
          "target" => "chat"
        })

      Mint.HTTP.close(conn)
      response
    end)
  end

  test "a failed server write reports held and keeps the focus fallback", ctx do
    writer = controlled_writer(ctx, head(ctx.relay))
    request = show_in_request(ctx)
    assert_receive {:controlled_chunk, pid, :show}, 2_000
    assert pid == writer.pid

    send(pid, {:chunk_result, {:error, :closed}})
    response = Task.await(request)
    assert response.status == 200
    assert %{"state" => "held", "show_id" => id} = body(response)

    assert {:ok, 200, %{"show" => %{"show_id" => ^id}}, false} =
             Writes.focus(ctx.relay, ctx.reader, %{"screen" => "inbox", "ttl_s" => 120})

    Task.await(writer)
  end

  test "the request waits for a successful chunk without blocking Relay", ctx do
    writer = controlled_writer(ctx, head(ctx.relay))
    request = show_in_request(ctx)
    assert_receive {:controlled_chunk, pid, :show}, 2_000
    assert pid == writer.pid
    assert Task.yield(request, 50) == nil
    assert :ok = Relay.setup_person(ctx.relay, "unrelated")

    send(pid, {:chunk_result, :ok})
    response = Task.await(request)
    assert response.status == 200
    assert %{"state" => "sent"} = body(response)
    Task.shutdown(writer, :brutal_kill)
  end

  test "the writer skips a show already consumed by focus", ctx do
    writer = controlled_writer(ctx, 0)
    assert_receive {:controlled_chunk, pid, :backlog}, 1_000
    assert pid == writer.pid
    request = show_in_request(ctx)

    pending =
      Enum.find_value(1..100, fn _ ->
        show = :sys.get_state(ctx.relay).held_shows[ctx.reader]
        if show, do: show, else: Process.sleep(10) && nil
      end)

    assert %{"show_id" => id} = pending

    assert {:ok, 200, %{"show" => %{"show_id" => ^id}}, false} =
             Writes.focus(ctx.relay, ctx.reader, %{"screen" => "inbox", "ttl_s" => 120})

    send(pid, {:chunk_result, :ok})
    response = Task.await(request)
    assert response.status == 200
    assert %{"state" => "held", "show_id" => ^id} = body(response)
    refute_received {:controlled_chunk, ^pid, :show}
    Task.shutdown(writer, :brutal_kill)
  end

  test "an unresponsive writer is held after the shared request timeout", ctx do
    Application.put_env(:attention_server, :client_api_request_timeout_ms, 150)
    on_exit(fn -> Application.delete_env(:attention_server, :client_api_request_timeout_ms) end)

    writer = controlled_writer(ctx, head(ctx.relay))
    request = show_in_request(ctx)
    assert_receive {:controlled_chunk, pid, :show}, 2_000
    assert pid == writer.pid
    response = Task.await(request, 2_000)
    assert response.status == 200
    assert %{"state" => "held", "show_id" => id} = body(response)

    send(pid, {:chunk_result, :ok})

    assert {:ok, 200, %{"show" => %{"show_id" => ^id}}, false} =
             Writes.focus(ctx.relay, ctx.reader, %{"screen" => "inbox", "ttl_s" => 120})

    Task.shutdown(writer, :brutal_kill)
  end

  test "one TLS connection retrying an in-flight key produces one show", ctx do
    assert {:ok, origin} =
             Relay.setup_device_certificate(
               ctx.relay,
               "A",
               TLSFixtures.certificate_der!(ctx.certs, "a2")
             )

    :sys.replace_state(ctx.relay, fn state ->
      put_in(state.store.engine.devices[origin].push_view, true)
    end)

    writer = controlled_writer(ctx, head(ctx.relay))

    assert {:ok, %{"items" => [%{"item_id" => item}]}} =
             ClientAPI.Reads.inbox(ctx.relay, ctx.reader)

    test = self()
    key = "00000000-0000-4000-8000-000000000012"
    data = JSON.encode!(%{"item_id" => item, "target" => "chat"})
    headers = [{"idempotency-key", key}]

    request =
      Task.async(fn ->
        conn = connect(ctx.port, ctx.certs, "a2")

        assert {:ok, conn, first} =
                 Mint.HTTP.request(conn, "POST", "/v2/agent/show", headers, data)

        receive do
          :same_key_retry -> :ok
        end

        assert {:ok, conn, second} =
                 Mint.HTTP.request(conn, "POST", "/v2/agent/show", headers, data)

        send(test, :second_show_request_sent)
        collect_show_pair(conn, first, second, %{})
      end)

    assert_receive {:controlled_chunk, pid, :show}, 2_000
    assert pid == writer.pid
    first_id = :sys.get_state(ctx.relay).held_shows[ctx.reader]["show_id"]
    send(request.pid, :same_key_retry)
    assert_receive :second_show_request_sent, 2_000

    duplicate_queued? =
      Enum.any?(1..100, fn _ ->
        {:messages, messages} = Process.info(pid, :messages)

        Enum.any?(messages, fn
          {:client_api_show, _, id, _} -> id != first_id
          _ -> false
        end) || (Process.sleep(5) && false)
      end)

    send(pid, {:chunk_result, :ok})

    if duplicate_queued? do
      assert_receive {:controlled_chunk, ^pid, :show}, 2_000
      send(pid, {:chunk_result, :ok})
    end

    {first, replay} = Task.await(request)
    assert first.status == 200
    assert replay.status == 200
    assert %{"show_id" => ^first_id, "state" => "sent"} = body(first)
    assert body(replay) == body(first)
    assert {"idempotent-replayed", "true"} in replay.headers
    refute duplicate_queued?
    Task.shutdown(writer, :brutal_kill)
  end

  defp collect_show_pair(conn, first, second, responses) do
    case responses do
      %{^first => %{done: true} = original, ^second => %{done: true} = replay} ->
        Mint.HTTP.close(conn)
        {original, replay}

      _ ->
        assert {:ok, conn, events} = Mint.HTTP.recv(conn, 0, 4_000)

        responses =
          Enum.reduce(events, responses, fn
            {:status, ref, status}, acc ->
              Map.put(acc, ref, %{status: status, headers: [], body: ""})

            {:headers, ref, headers}, acc ->
              update_in(acc[ref].headers, &(&1 ++ headers))

            {:data, ref, data}, acc ->
              update_in(acc[ref].body, &(&1 <> data))

            {:done, ref}, acc ->
              put_in(acc[ref][:done], true)

            _, acc ->
              acc
          end)

        collect_show_pair(conn, first, second, responses)
    end
  end

  defp show_request(conn, key, request) do
    assert {:ok, conn, ref} =
             Mint.HTTP.request(
               conn,
               "POST",
               "/v2/agent/show",
               [{"idempotency-key", key}],
               JSON.encode!(request)
             )

    collect(conn, ref)
  end

  defp body(response) do
    assert {:ok, body} = JSON.decode_line(response.body <> "\n")
    body
  end

  test "a show is never in a batch and never replayed on resume", ctx do
    heartbeat_fast()
    start = head(ctx.relay)
    conn = connect(ctx.port, ctx.certs, "a1")
    {conn, feed} = open_feed(conn, "?after=#{start}")
    {conn, feed} = await_open(conn, feed)

    assert {:ok, _} = push_view(ctx.relay, "message")
    {conn, _feed, _show, _} = until_show(conn, feed)
    Mint.HTTP.close(conn)

    # Nothing about the show is in the log.
    assert head(ctx.relay) == start
    conn = connect(ctx.port, ctx.certs, "a1")
    {conn, 200, batch} = batch(conn, "?after=0")
    refute Enum.any?(batch["changes"], &(&1["kind"] == "show"))

    # Resuming from the same position replays no show, only heartbeats.
    {conn, feed} = open_feed(conn, "", [{"last-event-id", Integer.to_string(start)}])
    {_conn, _feed, later} = events(conn, feed, 4)
    assert Enum.all?(later, &(&1 == %{event: "heartbeat", data: %{}}))
  end

  test "a device's feed subscribes to its own shows only, and releases them", ctx do
    conn = connect(ctx.port, ctx.certs, "a1")
    {conn, feed} = open_feed(conn)
    {conn, _feed} = await_open(conn, feed)

    assert [{pid, _}] = Registry.lookup(ChangeLog.Registry, {:client_api_show, ctx.reader})
    assert Registry.lookup(ChangeLog.Registry, {:client_api_show, @agent}) == []

    ref = Process.monitor(pid)
    Mint.HTTP.close(conn)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000

    # The registry drops a dead process's keys asynchronously.
    assert Enum.any?(1..100, fn _ ->
             Registry.lookup(ChangeLog.Registry, {:client_api_show, ctx.reader}) == [] or
               (Process.sleep(20) && false)
           end)
  end
end
