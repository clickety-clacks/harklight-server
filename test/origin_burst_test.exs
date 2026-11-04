defmodule AttentionServer.OriginBurstTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    JSON,
    Relay,
    Server,
    TLSFixtures
  }

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

  @tag timeout: 240_000
  test "350 existing origin handles receive definite responses to an unpaced stream burst", c do
    run_burst(c, :stream)
  end

  @tag timeout: 240_000
  test "350 existing origin handles receive definite controller attachment responses", c do
    run_burst(c, :attachment)
  end

  defp run_burst(c, phase) do
    count = 350
    handles = Enum.map(1..count, &uuid/1)

    for {handle, n} <- Enum.with_index(handles, 1) do
      assert {:ok, 200, %{"type" => "ok"}} =
               request(c, %{
                 "v" => 1,
                 "type" => "send",
                 "op_id" => uuid(10_000 + n),
                 "session_handle" => handle,
                 "route" => "other",
                 "cli_version" => "test-only",
                 "unsupported_reason" => nil,
                 "message" => %{
                   "sender_id" => "app:test",
                   "message_id" => "burst-#{n}",
                   "created_at" => 1,
                   "title" => "Synthetic reconnect burst"
                 }
               })
    end

    {:ok, feed_before} = Relay.origin_feed(c.relay, c.origin, 0)
    parent = self()
    # Each child waits at the same barrier before opening its own TLS connection.
    # No stagger, rate limiter, retry, listener pause or artificial accept delay.
    workers =
      for handle <- handles do
        spawn_monitor(fn ->
          send(parent, {:ready, self()})

          receive do
            {:go, released_at} ->
              {result, socket} =
                case phase do
                  :stream -> stream_attach(c, handle, released_at)
                  :attachment -> controller_attach(c, handle, released_at)
                end

              send(parent, {:outcome, self(), result})

              receive do
                :release -> if socket, do: :ssl.close(socket)
              after
                130_000 -> if socket, do: :ssl.close(socket)
              end
          end
        end)
      end

    on_exit(fn ->
      for {pid, _} <- workers, Process.alive?(pid), do: Process.exit(pid, :kill)
    end)

    for {pid, _} <- workers do
      assert_receive {:ready, ^pid}, 10_000
    end

    before = counters()

    queue =
      if System.find_executable("ss"),
        do: System.cmd("ss", ["-lnt", "sport = :#{c.endpoint.port}"], stderr_to_stdout: true),
        else: :unavailable

    IO.puts("BURST_LISTENER " <> inspect(queue))
    start = System.monotonic_time(:millisecond)
    for {pid, _} <- workers, do: send(pid, {:go, start})

    outcomes =
      for {pid, _} <- workers do
        receive do
          {:outcome, ^pid, result} -> result
        after
          125_000 -> flunk("owned burst worker did not return: #{inspect(pid)}")
        end
      end

    after_counters = counters()
    {:ok, feed_after} = Relay.origin_feed(c.relay, c.origin, 0)

    IO.puts(
      "BURST_RESULT " <>
        JSON.encode!(%{
          "phase" => Atom.to_string(phase),
          "count" => count,
          "outcomes" => outcomes,
          "net_before" => before,
          "net_after" => after_counters,
          "elapsed_ms" => System.monotonic_time(:millisecond) - start,
          "pending_feed_preserved" => feed_before == feed_after
        })
    )

    for {pid, _} <- workers, do: send(pid, :release)

    for {_pid, monitor} <- workers do
      assert_receive {:DOWN, ^monitor, :process, _, :normal}, 10_000
    end

    assert feed_before == feed_after
    # This seeded valid case should attach; definite application refusals are
    # recorded separately rather than being confused with transport errors.
    assert Enum.all?(outcomes, &(&1["outcome"] == "attached"))
  end

  defp uuid(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp connect(c) do
    :ssl.connect(
      c.endpoint.address,
      c.endpoint.port,
      [
        versions: [:"tlsv1.3"],
        verify: :verify_peer,
        cacertfile: String.to_charlist(c.endpoint.ca_file),
        server_name_indication: ~c"relay.test",
        session_tickets: :disabled,
        active: false,
        mode: :binary,
        packet: :raw,
        certfile: String.to_charlist(Path.join(c.certs, "a2.pem")),
        keyfile: String.to_charlist(Path.join(c.certs, "a2.key"))
      ],
      30_000
    )
  end

  defp stream_attach(c, handle, released_at) do
    started = System.monotonic_time(:millisecond)

    case connect(c) do
      {:ok, socket} ->
        tls_at = System.monotonic_time(:millisecond)

        result =
          case :ssl.send(
                 socket,
                 "GET /origin/v1/stream?session=#{handle}&reconnect=1 HTTP/1.1\r\nHost: relay.test\r\n\r\n"
               ) do
            :ok -> response(socket, <<>>)
            {:error, reason} -> {:error, {:send, reason}}
          end

        {%{
           "handle" => handle,
           "start_ms" => started - released_at,
           "tls_ms" => tls_at - started,
           "total_ms" => System.monotonic_time(:millisecond) - started,
           "outcome" => classify(result),
           "detail" => inspect(result)
         }, socket}

      {:error, reason} ->
        {%{
           "handle" => handle,
           "start_ms" => started - released_at,
           "total_ms" => System.monotonic_time(:millisecond) - started,
           "outcome" => "transport_error",
           "detail" => inspect(reason)
         }, nil}
    end
  end

  defp classify({:ok, data}) do
    cond do
      String.contains?(data, "HTTP/1.1 200") and String.contains?(data, "event: change") ->
        "attached"

      String.contains?(data, "\"type\":\"error\"") ->
        "explicit_refusal"

      true ->
        "unexpected_response"
    end
  end

  defp classify(_), do: "transport_error"

  defp response(socket, bytes) do
    cond do
      String.contains?(bytes, "event: change") and String.contains?(bytes, "\n\n") ->
        {:ok, bytes}

      true ->
        case :ssl.recv(socket, 0, 30_000) do
          {:ok, next} -> response(socket, bytes <> next)
          {:error, :closed} when byte_size(bytes) > 0 -> {:ok, bytes}
          error -> error
        end
    end
  end

  # Wire-level model of Agent .6 controller_attach, not execution of the CLI.
  # attachment_support is the journal operation kind; its wire type is support.
  defp controller_attach(c, handle, released_at) do
    started = System.monotonic_time(:millisecond)
    n = handle |> String.split("-") |> List.last() |> String.to_integer()

    commands = [
      {"attachment_update",
       %{
         "v" => 1,
         "type" => "attachment_update",
         "op_id" => uuid(20_000 + n),
         "session_handle" => handle,
         "vendor_session_id" => nil
       }},
      {"attachment_support",
       %{
         "v" => 1,
         "type" => "support",
         "op_id" => uuid(30_000 + n),
         "session_handle" => handle,
         "route" => "other",
         "cli_version" => "0.1.6",
         "unsupported_reason" => nil
       }},
      {"status", %{"v" => 1, "type" => "status", "session_handle" => handle}}
    ]

    {outcome, phases} =
      Enum.reduce_while(commands, {"attached", []}, fn {phase, command}, {_, phases} ->
        t = System.monotonic_time(:millisecond)

        reply =
          try do
            request(c, command)
          rescue
            error -> {:error, Exception.message(error)}
          catch
            kind, reason -> {:error, inspect({kind, reason})}
          end

        result =
          case reply do
            {:ok, 200, %{"type" => "ok"} = body} ->
              cond do
                phase == "attachment_update" and
                    (body["session_handle"] != handle or body["vendor_session_id"] != nil) ->
                  "invalid_attachment_receipt"

                phase == "status" and
                    (body["listening_support"]["route"] != "other" or
                       body["listening_support"]["cli_version"] != "0.1.6" or
                       body["listening_support"]["unsupported_reason"] != nil) ->
                  "invalid_support_receipt"

                true ->
                  "attached"
              end

            {:ok, _, %{"type" => "error", "reason" => _}} ->
              "explicit_refusal"

            {:error, _} ->
              "fixture_or_transport_error"

            _ ->
              "unexpected_response"
          end

        phases =
          phases ++
            [
              %{
                "phase" => phase,
                "elapsed_ms" => System.monotonic_time(:millisecond) - t,
                "outcome" => result,
                "reply" => inspect(reply)
              }
            ]

        if result == "attached", do: {:cont, {result, phases}}, else: {:halt, {result, phases}}
      end)

    {%{
       "handle" => handle,
       "start_ms" => started - released_at,
       "total_ms" => System.monotonic_time(:millisecond) - started,
       "outcome" => outcome,
       "phases" => phases
     }, nil}
  end

  defp request(c, body) do
    {:ok, socket} = connect(c)

    try do
      encoded = JSON.encode!(body)

      :ok =
        :ssl.send(
          socket,
          "POST /origin/v1 HTTP/1.1\r\nHost: relay.test\r\nContent-Type: application/json\r\nContent-Length: #{byte_size(encoded)}\r\n\r\n" <>
            encoded
        )

      {:ok, response} = read_all(socket, <<>>)
      [head, body] = :binary.split(response, "\r\n\r\n")
      ["HTTP/1.1", status | _] = head |> String.split("\r\n") |> hd() |> String.split(" ")
      {:ok, decoded} = JSON.decode_line(body <> "\n")
      {:ok, String.to_integer(status), decoded}
    after
      :ssl.close(socket)
    end
  end

  defp read_all(socket, bytes) do
    case :ssl.recv(socket, 0, 30_000) do
      {:ok, next} -> read_all(socket, bytes <> next)
      {:error, :closed} -> {:ok, bytes}
      error -> error
    end
  end

  # Linux diagnostics supplement the transport assertions; other platforms still
  # exercise the same burst and pending-feed invariant without these counters.
  defp counters do
    case File.read("/proc/net/netstat") do
      {:ok, data} ->
        lines = String.split(data, "\n", trim: true)
        [names, values] = Enum.filter(lines, &String.starts_with?(&1, "TcpExt:"))

        Enum.zip(String.split(names), String.split(values))
        |> Enum.filter(fn {key, _} -> key in ["ListenOverflows", "ListenDrops"] end)
        |> Map.new(fn {key, value} -> {key, String.to_integer(value)} end)

      {:error, :enoent} ->
        %{}
    end
  end
end
