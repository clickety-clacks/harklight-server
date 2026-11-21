defmodule AttentionServer.ClientAPIFeedBatchTest.FailingRenderer do
  # render_many always raises; render raises for the item titled "bad".
  alias AttentionServer.ClientAPI.FeedRenderer

  def render_many(_context, _principal, _rows), do: raise("render_many failed")

  def render(context, principal, row) do
    case FeedRenderer.render(context, principal, row) do
      {:ok, %{"message" => %{"title" => "bad"}}} -> raise "bad row"
      result -> result
    end
  end
end

defmodule AttentionServer.ClientAPIFeedBatchTest.StoreFailingRenderer do
  # Reports a failed store for the item titled "unreadable".
  alias AttentionServer.ClientAPI.FeedRenderer

  def render_many(context, principal, rows) do
    Enum.map(FeedRenderer.render_many(context, principal, rows), fn
      {row, {:ok, %{"message" => %{"title" => "unreadable"}}}} ->
        {row, {:error, :storage_unavailable}}

      result ->
        result
    end)
  end
end

defmodule AttentionServer.ClientAPIFeedBatchTest do
  # The JSON batch form of GET /v2/changes (docs/client-api-v2.md §6) through
  # the router: its cursor rules and 410, contract shapes, person scope, and
  # replaying the feed onto a snapshot (spec §10, contract §6.1, round 12).
  use ExUnit.Case, async: false

  import Plug.Test
  import Plug.Conn

  alias AttentionServer.{ChangeLog, ClientAPIFeedClient, JSON, Relay, TLSFixtures}
  alias AttentionServer.ClientAPI.{FeedStep, Principal, Reads, Router}
  alias AttentionServer.ClientAPISchema, as: Schema

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @contract Path.expand("../docs/client-api-v2.md", __DIR__)

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir =
      Path.join(System.tmp_dir!(), "harklight-feed-batch-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    flags = Path.join(dir, "flags.json")
    File.write!(flags, ~s({"client_api_v2":true}))

    assert {:ok, relay} =
             Relay.start_link(
               store_path: Path.join(dir, "state.sqlite3"),
               feature_flags_path: flags,
               agent_fixture_mode: true
             )

    on_exit(fn -> if Process.alive?(relay), do: GenServer.stop(relay) end)

    ders = Map.new(~w(a1 b1), &{&1, TLSFixtures.certificate_der!(certs, &1)})

    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "other")
    assert :ok = Relay.setup_device(relay, "B", "origin-b")
    assert {:ok, reader} = Relay.setup_device_certificate(relay, "A", ders["a1"])
    assert {:ok, _} = Relay.setup_device_certificate(relay, "B", ders["b1"])
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")
    assert :ok = Relay.setup_sender(relay, "origin-b", "app:other")

    %{relay: relay, reader: reader, ders: ders, router: Router.init(relay: relay)}
  end

  ## Helpers

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp request(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp forward(relay, origin \\ "origin", sender \\ "app:test", id, n) do
    assert %{"type" => "ok"} =
             request(relay, origin, %{
               "type" => "forward",
               "op_id" => op(n),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => sender,
                 "message_id" => id,
                 "created_at" => n,
                 "title" => id
               }
             })
  end

  # A conversation turn from the person, the agent's reply, and an answer, so
  # the log holds items, entries and counts.
  defp converse(%{relay: relay, reader: reader}, id, n) do
    assert %{"type" => "ok", "event_id" => event_id} =
             request(relay, reader, %{
               "type" => "discussion",
               "op_id" => op(n),
               "sender_id" => "app:test",
               "message_id" => id,
               "request_revision" => 1,
               "text" => "Question"
             })

    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "conversation_reply",
               "op_id" => op(n + 1),
               "sender_id" => "app:test",
               "message_id" => id,
               "request_revision" => 1,
               "in_reply_to" => String.to_integer(event_id),
               "text" => "Answer"
             })
  end

  # The person reads the agent's reply on another of their devices (round 12).
  defp read_elsewhere(%{relay: relay} = ctx, id, n) do
    {:ok, %{"items" => items}} = Reads.inbox(relay, principal(ctx), %{"limit" => "200"})
    item = Enum.find(items, &(&1["message"]["message_id"] == id))

    {:ok, %{"entries" => entries}} =
      Reads.entries(relay, principal(ctx), item["item_id"], %{"limit" => "200"})

    through = entries |> Enum.map(& &1["position"]) |> Enum.max()

    assert %{"type" => "ok"} =
             request(relay, "other", %{
               "type" => "conversation_read",
               "op_id" => op(n),
               "sender_id" => "app:test",
               "message_id" => id,
               "read_through_conversation_seq" => through
             })
  end

  defp answer(%{relay: relay, reader: reader}, id, n) do
    assert %{"type" => "ok"} =
             request(relay, reader, %{
               "type" => "answer",
               "op_id" => op(n),
               "sender_id" => "app:test",
               "message_id" => id,
               "answer" => nil
             })
  end

  defp get(ctx, path, headers \\ [], der \\ nil) do
    conn =
      Enum.reduce(headers, conn(:get, path), fn {k, v}, conn -> put_req_header(conn, k, v) end)

    conn
    |> put_peer_data(%{address: {127, 0, 0, 1}, port: 1, ssl_cert: der || ctx.ders["a1"]})
    |> Router.call(ctx.router)
  end

  defp body(conn) do
    assert {:ok, body} = JSON.decode_line(conn.resp_body <> "\n")
    body
  end

  defp batch(ctx, query, headers \\ [], der \\ nil) do
    conn = get(ctx, "/v2/changes" <> query, [{"accept", "application/json"} | headers], der)
    {conn.status, body(conn)}
  end

  defp head(relay, person \\ "A"),
    do: elem(ChangeLog.head(:sys.get_state(relay).store, person), 1)

  defp principal(ctx), do: %Principal{person: "A", kind: :device, key: ctx.reader}

  # Reads every batch from `after` until `more` is false: the changes, and the
  # position the last batch reported.
  defp all_changes(ctx, after_rev, limit) do
    {200, page} = batch(ctx, "?after=#{after_rev}&limit=#{limit}")

    if page["more"] do
      {rest, rev} = all_changes(ctx, page["rev"], limit)
      {page["changes"] ++ rest, rev}
    else
      {page["changes"], page["rev"]}
    end
  end

  # What a client holds: a snapshot of every item, its entries and the counts.
  defp snapshot(ctx) do
    assert {:ok, inbox} = Reads.inbox(ctx.relay, principal(ctx), %{"limit" => "200"})
    refute Map.has_key?(inbox, "next_page")

    entries =
      for item <- inbox["items"], into: %{} do
        assert {:ok, %{"entries" => entries}} =
                 Reads.entries(ctx.relay, principal(ctx), item["item_id"], %{"limit" => "200"})

        {item["item_id"], Map.new(entries, &{&1["entry_id"], &1})}
      end

    %{
      rev: inbox["rev"],
      items: Map.new(inbox["items"], &{&1["item_id"], &1}),
      entries: entries,
      counts: inbox["counts"]
    }
  end

  # Applies changes with the version rule, as a client does (spec §1, §5.1).
  defp apply_changes(model, changes) do
    Enum.reduce(changes, model, fn %{"rev" => rev, "kind" => kind, "data" => data}, model ->
      case kind do
        "item" ->
          if newer?(model.items[data["item_id"]], rev),
            do: put_in(model, [:items, data["item_id"]], data),
            else: model

        "item_removed" ->
          %{
            model
            | items: Map.delete(model.items, data["item_id"]),
              entries: Map.delete(model.entries, data["item_id"])
          }

        "entry" ->
          held = get_in(model, [:entries, data["item_id"], data["entry_id"]])

          if newer?(held, rev),
            do:
              update_in(model.entries, fn entries ->
                Map.update(entries, data["item_id"], %{data["entry_id"] => data}, fn item ->
                  Map.put(item, data["entry_id"], data)
                end)
              end),
            else: model

        "counts" ->
          %{model | counts: data}

        "settings" ->
          model
      end
    end)
  end

  defp newer?(nil, _rev), do: true
  defp newer?(%{"rev" => held}, rev), do: rev > held

  ## The JSON batch

  test "a batch pages through the log gap-free with limit, more and rev", ctx do
    for {id, n} <- [{"m1", 1}, {"m2", 2}, {"m3", 3}], do: forward(ctx.relay, id, n)

    {200, all} = batch(ctx, "?after=0")
    assert all["more"] == false
    assert all["rev"] == head(ctx.relay)
    revs = Enum.map(all["changes"], & &1["rev"])
    assert revs == Enum.sort(revs) and revs == Enum.uniq(revs)
    assert all["rev"] == List.last(revs)

    {200, first} = batch(ctx, "?after=0&limit=2")
    assert length(first["changes"]) == 2
    assert first["more"] == true
    assert first["rev"] == List.last(first["changes"])["rev"]
    {200, rest} = batch(ctx, "?after=#{first["rev"]}")
    assert first["changes"] ++ rest["changes"] == all["changes"]

    # An empty batch reports the request's after.
    assert {200, %{"changes" => [], "rev" => rev, "more" => false}} =
             batch(ctx, "?after=#{all["rev"]}")

    assert rev == all["rev"]

    # No cursor means from the start; no Accept is a batch too.
    assert {200, ^all} = batch(ctx, "")
    assert body(get(ctx, "/v2/changes")) == all
  end

  test "every batch change matches the contract", ctx do
    forward(ctx.relay, "m1", 1)
    forward(ctx.relay, "m2", 2)
    converse(ctx, "m1", 10)
    answer(ctx, "m2", 20)

    defs = Schema.contract_defs(@contract)
    {200, batch} = batch(ctx, "?after=0")
    assert %{"changes" => changes, "rev" => rev, "more" => false} = batch
    assert map_size(batch) == 3 and is_integer(rev)

    kinds = changes |> Enum.map(& &1["kind"]) |> Enum.uniq() |> Enum.sort()
    assert "item" in kinds and "entry" in kinds and "counts" in kinds

    # The Change schema's data by kind (its allOf, checked branch by branch).
    for %{"rev" => rev, "kind" => kind, "data" => data} = change <- changes do
      assert map_size(change) == 3 and is_integer(rev) and rev >= 1

      case kind do
        "item_removed" ->
          assert Map.keys(data) == ["item_id"]

        kind ->
          name = %{
            "item" => "Item",
            "entry" => "Entry",
            "counts" => "Counts",
            "settings" => "Settings"
          }

          assert {rev, Schema.errors(defs, Map.fetch!(name, kind), data)} == {rev, []}
          if kind != "counts", do: assert(data["rev"] == rev)
      end
    end
  end

  test "Last-Event-ID takes precedence over after", ctx do
    for {id, n} <- [{"m1", 1}, {"m2", 2}], do: forward(ctx.relay, id, n)
    {200, %{"changes" => [first | _] = all}} = batch(ctx, "?after=0")

    {200, resumed} = batch(ctx, "?after=0", [{"last-event-id", "#{first["rev"]}"}])
    assert resumed["changes"] == tl(all)
  end

  test "malformed cursors and limits are 400 invalid_field", ctx do
    assert {400, %{"error" => "invalid_field", "field" => "after", "retryable" => false}} =
             batch(ctx, "?after=-1")

    assert {400, %{"field" => "after"}} = batch(ctx, "?after=x")
    assert {400, %{"field" => "limit"}} = batch(ctx, "?limit=0")
    assert {400, %{"field" => "limit"}} = batch(ctx, "?limit=ten")
    assert {400, %{"field" => "last_event_id"}} = batch(ctx, "", [{"last-event-id", "x"}])

    # A limit above the maximum is capped, not refused.
    assert {200, %{"more" => false}} = batch(ctx, "?limit=100000")
  end

  ## Retention

  test "a cursor past retention is 410 cursor_expired", ctx do
    forward(ctx.relay, "m1", 1)
    {200, %{"rev" => rev}} = batch(ctx, "?after=0")

    assert {:ok, _} =
             ClientAPIFeedClient.prune_changes(ctx.relay, System.system_time(:millisecond) + 1)

    assert {410, %{"error" => "cursor_expired", "retryable" => false, "request_id" => _}} =
             batch(ctx, "?after=0")

    assert {410, %{"error" => "cursor_expired"}} =
             batch(ctx, "", [{"last-event-id", "0"}])

    # The newest position stays readable: nothing after it was pruned.
    assert {200, %{"changes" => [], "rev" => ^rev}} = batch(ctx, "?after=#{rev}")
  end

  test "a cursor ahead of the person's position is 410 cursor_expired", ctx do
    forward(ctx.relay, "m1", 1)
    assert {410, %{"error" => "cursor_expired"}} = batch(ctx, "?after=#{head(ctx.relay) + 1}")
  end

  ## Replay equals snapshot (spec §10, contract §6.1)

  test "replaying the feed from empty reaches the snapshot", ctx do
    for {id, n} <- [{"m1", 1}, {"m2", 2}, {"m3", 3}], do: forward(ctx.relay, id, n)
    converse(ctx, "m1", 10)
    answer(ctx, "m2", 20)
    read_elsewhere(ctx, "m1", 30)

    # Small pages, so the replay crosses batch boundaries.
    {changes, rev} = all_changes(ctx, 0, 2)
    replayed = apply_changes(%{rev: 0, items: %{}, entries: %{}, counts: nil}, changes)
    expected = snapshot(ctx)

    assert rev == expected.rev
    assert replayed.items == expected.items
    assert replayed.counts == expected.counts

    for {item_id, entries} <- expected.entries do
      assert Map.get(replayed.entries, item_id, %{}) == entries
    end
  end

  test "a snapshot plus the feed from its rev reaches a later snapshot", ctx do
    for {id, n} <- [{"m1", 1}, {"m2", 2}], do: forward(ctx.relay, id, n)
    before = snapshot(ctx)

    # Changes after the snapshot: a new item, a conversation on an old one,
    # and an answer.
    forward(ctx.relay, "m3", 3)
    converse(ctx, "m1", 10)
    answer(ctx, "m2", 20)
    middle = snapshot(ctx)
    # A read on another device changes the item's chat state (round 12).
    read_elsewhere(ctx, "m1", 30)
    assert middle.items != snapshot(ctx).items

    {changes, rev} = all_changes(ctx, before.rev, 2)
    assert changes != []
    replayed = apply_changes(before, changes)
    expected = snapshot(ctx)

    assert rev == expected.rev
    assert replayed.items == expected.items
    assert replayed.counts == expected.counts

    for {item_id, entries} <- expected.entries do
      assert Map.get(replayed.entries, item_id, %{}) == entries
    end
  end

  ## Person scope

  test "a person's feed never carries another person's changes", ctx do
    forward(ctx.relay, "m1", 1)
    forward(ctx.relay, "origin-b", "app:other", "other", 2)

    {200, a} = batch(ctx, "?after=0")
    {200, b} = batch(ctx, "?after=0", [], ctx.ders["b1"])

    assert a["rev"] == head(ctx.relay, "A")
    assert b["rev"] == head(ctx.relay, "B")

    titles = fn batch ->
      for %{"kind" => "item", "data" => item} <- batch["changes"], do: item["message"]["title"]
    end

    assert titles.(a) == ["m1"]
    assert titles.(b) == ["other"]
  end

  test "an unenrolled certificate reads no feed", ctx do
    conn =
      conn(:get, "/v2/changes")
      |> put_peer_data(%{address: {127, 0, 0, 1}, port: 1, ssl_cert: "not a certificate"})
      |> Router.call(ctx.router)

    assert conn.status == 401
  end

  ## Relay cost (review H2)

  test "a long chat's backlog is read in one relay step in linear time", ctx do
    forward(ctx.relay, "long", 1)

    chat = fn from, to ->
      for n <- from..to do
        assert %{"type" => "ok"} =
                 request(ctx.relay, ctx.reader, %{
                   "type" => "discussion",
                   "op_id" => op(1_000 + n),
                   "sender_id" => "app:test",
                   "message_id" => "long",
                   "request_revision" => 1,
                   "text" => "Turn #{n}"
                 })
      end
    end

    # The whole backlog in one step, as the Relay runs it: best of three.
    step = fn ->
      1..3
      |> Enum.map(fn _ ->
        {us, {:ok, changes, false, _}} =
          :timer.tc(fn -> FeedStep.read(ctx.relay, principal(ctx), 0, 500) end)

        {us, Enum.count(changes, &(&1["kind"] == "entry"))}
      end)
      |> Enum.min()
    end

    chat.(1, 100)
    {short_us, short_entries} = step.()
    chat.(101, 400)
    {long_us, long_entries} = step.()

    assert short_entries >= 100 and long_entries >= 400

    # Four times the entries may cost at most eight times as much. Rebuilding
    # the item's entries for each row is quadratic: about sixteen times.
    assert long_us < 8 * max(short_us, 1_000),
           "100 entries: #{short_us} us, 400 entries: #{long_us} us"

    # Another Relay caller during the 400-entry catch-up waits only for one
    # short step. Row by row, it waited about 290 ms on a test host.
    waits =
      for _ <- 1..3 do
        catch_up = Task.async(fn -> FeedStep.read(ctx.relay, principal(ctx), 0, 500) end)
        Process.sleep(1)
        {us, _} = :timer.tc(fn -> :sys.get_state(ctx.relay) end)
        Task.await(catch_up)
        us
      end

    assert Enum.min(waits) < 100_000, "a concurrent Relay call waited #{Enum.min(waits)} us"
  end

  test "a failing renderer stops the batch and leaves every row available for replay", ctx do
    renderer = AttentionServer.ClientAPIFeedBatchTest.FailingRenderer
    Application.put_env(:attention_server, :client_api_feed_renderer, renderer)
    on_exit(fn -> Application.delete_env(:attention_server, :client_api_feed_renderer) end)

    forward(ctx.relay, "good", 1)
    forward(ctx.relay, "bad", 2)

    assert {500, %{"error" => "internal_error", "retryable" => false}} =
             batch(ctx, "?after=0")

    Application.delete_env(:attention_server, :client_api_feed_renderer)
    {200, batch} = batch(ctx, "?after=0")

    titles =
      for %{"kind" => "item", "data" => item} <- batch["changes"], do: item["message"]["title"]

    assert titles == ["good", "bad"]
    assert Enum.any?(batch["changes"], &(&1["kind"] == "counts"))
    assert Process.alive?(ctx.relay)
  end

  test "a row the renderer cannot read from its store fails the step, never a silent gap", ctx do
    renderer = AttentionServer.ClientAPIFeedBatchTest.StoreFailingRenderer
    Application.put_env(:attention_server, :client_api_feed_renderer, renderer)
    on_exit(fn -> Application.delete_env(:attention_server, :client_api_feed_renderer) end)

    forward(ctx.relay, "fine", 1)
    assert {200, %{"changes" => [_ | _]}} = batch(ctx, "?after=0")
    forward(ctx.relay, "unreadable", 2)

    # The client retries later from the same cursor; nothing is skipped.
    assert {503, %{"error" => "storage_unavailable", "retryable" => true}} =
             batch(ctx, "?after=0")

    assert Process.alive?(ctx.relay)
  end
end
