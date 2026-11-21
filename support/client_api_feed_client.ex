defmodule AttentionServer.ClientAPIFeedClient do
  @moduledoc """
  A test client for `GET /v2/changes` over Mint in passive mode: JSON batches
  and Server-Sent Events, for device (mutual TLS) and browser (cookie)
  principals.
  """

  import ExUnit.Assertions

  alias AttentionServer.JSON

  @doc "Prunes the test Relay's log with its SQLite connection idle."
  def prune_changes(relay, before_ms) do
    :ok = :sys.suspend(relay)

    try do
      AttentionServer.ChangeLog.prune(:sys.get_state(relay).store, before_ms)
    after
      :ok = :sys.resume(relay)
    end
  end

  @doc "Connects with a device certificate (`name`), or with none for a browser."
  def connect(port, certs, name \\ nil, protocol \\ :http2) do
    client_cert =
      if name,
        do: [certfile: Path.join(certs, "#{name}.pem"), keyfile: Path.join(certs, "#{name}.key")],
        else: []

    assert {:ok, conn} =
             Mint.HTTP.connect(:https, "127.0.0.1", port,
               hostname: "relay.test",
               protocols: [protocol],
               mode: :passive,
               transport_opts:
                 [
                   versions: [:"tlsv1.3"],
                   cacertfile: Path.join(certs, "ca.pem"),
                   server_name_indication: ~c"relay.test",
                   timeout: 5_000
                 ] ++ client_cert
             )

    conn
  end

  @doc "One JSON batch: `{conn, status, body}`."
  def batch(conn, query, headers \\ []) do
    {:ok, conn, ref} =
      Mint.HTTP.request(
        conn,
        "GET",
        "/v2/changes" <> query,
        [{"accept", "application/json"} | headers],
        nil
      )

    {conn, response} = collect(conn, ref)
    assert {:ok, body} = JSON.decode_line(response.body <> "\n")
    {conn, response.status, body}
  end

  @doc "Reads one whole response."
  def collect(conn, ref, acc \\ %{headers: [], body: ""}) do
    {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, 5_000)

    acc =
      Enum.reduce(responses, acc, fn
        {:status, ^ref, status}, acc -> Map.put(acc, :status, status)
        {:headers, ^ref, headers}, acc -> Map.update!(acc, :headers, &(&1 ++ headers))
        {:data, ^ref, data}, acc -> Map.update!(acc, :body, &(&1 <> data))
        {:done, ^ref}, acc -> Map.put(acc, :done, true)
        _, acc -> acc
      end)

    if acc[:done], do: {conn, acc}, else: collect(conn, ref, acc)
  end

  @doc "Starts an event stream; returns the connection and the feed's state."
  def open_feed(conn, query \\ "", headers \\ []) do
    {:ok, conn, ref} =
      Mint.HTTP.request(
        conn,
        "GET",
        "/v2/changes" <> query,
        [{"accept", "text/event-stream"} | headers],
        nil
      )

    {conn, %{ref: ref, buffer: "", status: nil, headers: [], done: false}}
  end

  @doc "Receives whatever has arrived for the feed."
  def recv(conn, feed) do
    {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, 5_000)

    feed =
      Enum.reduce(responses, feed, fn
        {:status, ref, status}, f when ref == feed.ref -> %{f | status: status}
        {:headers, ref, headers}, f when ref == feed.ref -> %{f | headers: f.headers ++ headers}
        {:data, ref, data}, f when ref == feed.ref -> %{f | buffer: f.buffer <> data}
        {:done, ref}, f when ref == feed.ref -> %{f | done: true}
        _, f -> f
      end)

    {conn, feed}
  end

  @doc "Waits for the feed's status line."
  def await_open(conn, %{status: nil} = feed) do
    {conn, feed} = recv(conn, feed)
    await_open(conn, feed)
  end

  def await_open(conn, feed), do: {conn, feed}

  @doc "Reads until `count` events, heartbeats included, have arrived."
  def events(conn, feed, count, acc \\ [])

  def events(conn, feed, count, acc) when length(acc) >= count,
    do: {conn, feed, Enum.reverse(acc)}

  def events(conn, feed, count, acc) do
    case String.split(feed.buffer, "\n\n", parts: 2) do
      [block, rest] ->
        events(conn, %{feed | buffer: rest}, count, [parse(block) | acc])

      [_incomplete] ->
        if feed.done, do: flunk("feed ended after #{length(acc)} events")
        {conn, feed} = recv(conn, feed)
        events(conn, feed, count, acc)
    end
  end

  @doc "Reads change events until one has an `id` of at least `rev`, skipping heartbeats."
  def changes_through(conn, feed, rev, acc \\ []) do
    {conn, feed, [event]} = events(conn, feed, 1)

    cond do
      event[:event] == "heartbeat" -> changes_through(conn, feed, rev, acc)
      event.id >= rev -> {conn, feed, Enum.reverse([event | acc])}
      true -> changes_through(conn, feed, rev, [event | acc])
    end
  end

  @doc "Reads events until a `closing` event; returns it."
  def until_closing(conn, feed) do
    {conn, feed, [event]} = events(conn, feed, 1)
    if event[:event] == "closing", do: {conn, feed, event}, else: until_closing(conn, feed)
  end

  @doc "Reads to the end of the stream."
  def drain(conn, %{done: true} = feed), do: {conn, feed}

  def drain(conn, feed) do
    {conn, feed} = recv(conn, feed)
    drain(conn, feed)
  end

  @doc "One event block as `%{id?, event, data}`."
  def parse(block) do
    block
    |> String.split("\n")
    |> Enum.reduce(%{}, fn line, event ->
      case String.split(line, ": ", parts: 2) do
        ["id", id] -> Map.put(event, :id, String.to_integer(id))
        ["event", kind] -> Map.put(event, :event, kind)
        ["data", data] -> Map.put(event, :data, elem(JSON.decode_line(data <> "\n"), 1))
        _ -> event
      end
    end)
  end
end
