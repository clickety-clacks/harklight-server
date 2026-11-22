defmodule AttentionServer.Store.KeyedBounceValidationTest do
  # W3c: bounce state (inventory rows 29-34) and the open default index (#3)
  # checked per touched address. Each refused proposal is refused by the
  # whole-state check; the per-address verdict must refuse it for the stated
  # reason, and committing it through the keyed Store, which runs both
  # validators in the test environment and raises when they disagree, must
  # fail. The acceptance tests commit real bounce transitions with the touched
  # set the engine reports.
  use ExUnit.Case, async: true

  alias AttentionServer.{BounceDecision, Engine, JSON, LargeData, Store}
  alias AttentionServer.Store.RecordCodec

  @moduletag :tmp_dir
  @moduletag :capture_log

  @person "a"
  @other "b"
  @sender "app:test/worker"
  @origin String.duplicate("a", 64)
  @reader String.duplicate("b", 64)
  @handle "00000000-0000-4000-8000-000000000001"
  @handle2 "00000000-0000-4000-8000-000000000002"
  @unknown "00000000-0000-4000-8000-0000000000d1"
  @m1 {@person, @sender, "m1"}
  @m2 {@person, @sender, "m2"}
  @m3 {@person, @sender, "m3"}
  @feed {@person, @origin, @handle}
  @default_at 10_000_000_000_000

  setup %{tmp_dir: directory} do
    path = Path.join(directory, "state.sqlite3")
    {:ok, legacy} = Store.open(path)
    {:ok, engine} = Engine.setup_person(legacy.engine, @person)
    {:ok, engine} = Engine.setup_person(engine, @other)
    {:ok, engine} = Engine.setup_device(engine, @person, @origin)
    {:ok, engine} = Engine.setup_device(engine, @person, @reader)
    {:ok, engine} = Engine.setup_sender(engine, @origin, @sender)

    engine =
      [
        forward("m1"),
        forward("m2", %{
          "default" => %{"value" => %{"acknowledgment" => "seen"}, "at" => @default_at}
        }),
        forward("m3")
      ]
      |> Enum.with_index(1)
      |> Enum.reduce(engine, fn {frame, n}, engine ->
        raw = frame |> Map.merge(%{"v" => 1, "op_id" => op(n)}) |> JSON.encode_line()
        {engine, {:ok, response}} = Engine.request_line(engine, @origin, raw, 1_000 + n)
        refute response =~ ~s("type":"error"), response
        engine
      end)

    {:ok, legacy} = Store.commit(legacy, engine, audit())
    :ok = Store.close(legacy)
    {:ok, staged} = Store.open(path, :keyed_cutover)
    :ok = Store.close(staged)
    {:ok, store} = Store.open(path, :keyed)
    on_exit(fn -> Store.close(store) end)
    %{store: store, path: path}
  end

  test "the fixture has bounce rows, a default and an unsupported session", %{store: store} do
    engine = store.engine
    assert %{current_substantive_revision: 1} = engine.bounce_revisions[@m1]
    assert %{current_substantive_revision: 1} = engine.bounce_revisions[@m3]
    assert engine.open_default_index == %{@m2 => @default_at}
    assert engine.origin_sessions[@feed].support["advisory_v1"] == false

    assert MapSet.member?(
             engine.bounce_origin_entries,
             {@person, @origin, @handle, @sender, "m1"}
           )
  end

  # --- acceptance --------------------------------------------------------------

  describe "real bounce transitions commit with the reported touches" do
    test "score, defer, requeue, bounce and acknowledge", %{store: store, path: path} do
      store = scored(store)
      assert selection(store.engine, @m1)["decision"] == "pending"
      assert BounceDecision.next_deadline(store.engine) == due(store.engine)

      store = deferred(store)
      assert selection(store.engine, @m1)["decision"] == "pending"
      assert MapSet.size(store.engine.bounce_waiting_routes[@feed]) == 2
      assert BounceDecision.next_deadline(store.engine) == nil

      store = requeued(store)
      refute Map.has_key?(store.engine.bounce_waiting_routes, @feed)
      assert BounceDecision.next_deadline(store.engine) == due(store.engine)

      store = transition(store, &BounceDecision.evaluate_due(&1, due(&1, @m3) + 50))
      assert selection(store.engine, @m1)["decision"] == "bounced"
      assert selection(store.engine, @m3)["decision"] == "bounced"
      assert store.engine.advisory_feeds[@feed].high == 2

      id = advisory_id(store.engine, 2)
      store = transition(store, &ack(&1, id))
      assert store.engine.advisory_feeds[@feed].receipts == MapSet.new([id])

      # Acknowledging again changes nothing.
      store = transition(store, &ack(&1, id))

      :ok = Store.close(store)
      assert {:ok, reopened} = Store.open(path, :keyed)
      assert reopened.engine.advisory_feeds == store.engine.advisory_feeds
      :ok = Store.close(reopened)
    end

    test "a due revision in a supported session bounces directly", %{store: store} do
      store = transition(store, &supported/1) |> scored()
      store = transition(store, &BounceDecision.evaluate_due(&1, due(&1)))
      assert selection(store.engine, @m1)["decision"] == "bounced"
      assert store.engine.bounce_waiting_routes == %{}
    end

    test "revising, opening, discussing and answering scored messages", %{store: store} do
      store = scored(store)

      store =
        wire(store, @origin, 20, %{
          "type" => "conversation_revise",
          "sender_id" => @sender,
          "message_id" => "m1",
          "base_revision" => 1,
          "message" => message("m1", "revised")
        })

      assert store.engine.messages[@m1].revision == 2
      assert store.engine.bounce_revisions[@m1].current_substantive_revision == 2
      assert selection(store.engine, @m1, 1)["reason"] == "superseded"

      store = wire(store, @reader, 21, opened("m1", 2))
      assert MapSet.member?(store.engine.opened_revisions, {@person, @sender, "m1", 2})

      store =
        wire(store, @reader, 22, %{
          "type" => "discussion",
          "sender_id" => @sender,
          "message_id" => "m3",
          "request_revision" => 1,
          "text" => "p"
        })

      assert MapSet.member?(store.engine.bounce_discussed_messages, @m3)

      store =
        wire(store, @reader, 23, %{
          "type" => "answer",
          "sender_id" => @sender,
          "message_id" => "m2",
          "answer" => %{"acknowledgment" => "seen"}
        })

      refute Map.has_key?(store.engine.open_default_index, @m2)

      store = transition(store, &BounceDecision.evaluate_due(&1, due(&1, @m3)))
      assert selection(store.engine, @m3)["reason"] == "person_discussion"
    end

    test "a pending revision superseded while waiting leaves its route", %{store: store} do
      store = deferred(store)

      store =
        wire(store, @origin, 20, %{
          "type" => "conversation_revise",
          "sender_id" => @sender,
          "message_id" => "m1",
          "base_revision" => 1,
          "message" => message("m1", "revised")
        })

      assert store.engine.bounce_waiting_routes[@feed] == MapSet.new([{@m3, 1}])
    end
  end

  # --- #30 bounce_revisions ------------------------------------------------------

  describe "#30 bounce revision state" do
    test "a malformed row", %{store: store} do
      refuse(store, &put_in(&1.bounce_revisions[@m1], %{}), {:row, :bounce_revisions, @m1})
    end

    test "a message revision bump with a stale bounce row", %{store: store} do
      {revised, _reply, touched} =
        Engine.request_line_with_touches(
          store.engine,
          @origin,
          line(20, %{
            "type" => "conversation_revise",
            "sender_id" => @sender,
            "message_id" => "m1",
            "base_revision" => 1,
            "message" => message("m1", "revised")
          }),
          5_000
        )

      assert revised.messages[@m1].revision == 2
      stale = put_in(revised.bounce_revisions[@m1], store.engine.bounce_revisions[@m1])
      touched = MapSet.delete(touched, {:row, :bounce_revisions, @m1})
      assert_refused(store, stale, touched, {:address, {:row, :bounce_revisions, @m1}})
    end
  end

  # --- #31 opened_revisions ------------------------------------------------------

  describe "#31 opened revisions" do
    test "a member for a revision the message lacks", %{store: store} do
      member = {@person, @sender, "m1", 5}

      refuse(
        store,
        &%{&1 | opened_revisions: MapSet.put(&1.opened_revisions, member)},
        {:member, :opened_revisions, member}
      )
    end

    test "removing the bounce row an opened revision names", %{store: store} do
      store = wire(store, @reader, 20, opened("m1", 1))
      member = {@person, @sender, "m1", 1}
      assert MapSet.member?(store.engine.opened_revisions, member)

      refuse(
        store,
        &%{&1 | bounce_revisions: Map.delete(&1.bounce_revisions, @m1)},
        {:member, :opened_revisions, member}
      )
    end
  end

  # --- #32 advisory feeds --------------------------------------------------------

  describe "#32 advisory feeds and entries" do
    setup %{store: store}, do: %{store: bounced(store)}

    test "an entry whose text disagrees", %{store: store} do
      refuse(
        store,
        &put_in(&1.advisory_feeds[@feed].entries[1]["text"], "x"),
        {:advisory, @feed, {:entry, 1}}
      )
    end

    test "a bounced selection changed under its advisory entry", %{store: store} do
      seq = advisory_seq(store.engine, @m1)

      refuse(
        store,
        &update_in(&1.bounce_revisions[@m1].revisions[1].selection, fn selection ->
          %{selection | "level_name" => "Renamed"}
        end),
        {:advisory, @feed, {:entry, seq}}
      )
    end

    test "a message moved to another session under its advisory entry", %{store: store} do
      seq = advisory_seq(store.engine, @m1)

      refuse(
        store,
        &put_in(&1.messages[@m1].session_handle, @handle2),
        {:advisory, @feed, {:entry, seq}}
      )
    end

    test "a receipt for an id no entry has", %{store: store} do
      refuse(
        store,
        &update_in(&1.advisory_feeds[@feed].receipts, fn r -> MapSet.put(r, @unknown) end),
        {:advisory, @feed, {:receipt, @unknown}}
      )
    end

    test "removing an acknowledged entry", %{store: store} do
      id = advisory_id(store.engine, 2)
      store = transition(store, &ack(&1, id))

      refuse(
        store,
        fn engine ->
          engine
          |> update_in([Access.key(:advisory_feeds), @feed], fn feed ->
            %{feed | high: 1, entries: Map.delete(feed.entries, 2)}
          end)
          |> Map.update!(:advisory_id_index, &Map.delete(&1, {@feed, id}))
        end,
        {:advisory, @feed, {:receipt, id}}
      )
    end

    test "a lowered high that leaves a later entry", %{store: store} do
      id = advisory_id(store.engine, 1)

      refuse(
        store,
        fn engine ->
          engine
          |> update_in([Access.key(:advisory_feeds), @feed], fn feed ->
            %{feed | high: 1, entries: Map.delete(feed.entries, 1)}
          end)
          |> Map.update!(:advisory_id_index, &Map.delete(&1, {@feed, id}))
        end,
        {:advisory, @feed, {:entry, 2}}
      )
    end

    test "removing an entry without lowering the high", %{store: store} do
      id = advisory_id(store.engine, 1)

      refuse(
        store,
        fn engine ->
          engine
          |> update_in([Access.key(:advisory_feeds), @feed, :entries], &Map.delete(&1, 1))
          |> Map.update!(:advisory_id_index, &Map.delete(&1, {@feed, id}))
        end,
        {:advisory, @feed, :high}
      )
    end

    test "an entry count that disagrees with the high", %{store: store} do
      refuse(store, &put_in(&1.advisory_feeds[@feed].high, 3), {:advisory, @feed, :high})
    end

    test "removing the origin session of a feed", %{store: store} do
      refuse(
        store,
        &%{&1 | origin_sessions: Map.delete(&1.origin_sessions, @feed)},
        {:advisory, @feed, :high}
      )
    end

    test "reassigning the origin device of a feed", %{store: store} do
      # The feed header reads the device through its origin session, which
      # the authority index names when the device's person changes.
      refuse(store, &put_in(&1.devices[@origin].person, @other), {:advisory, @feed, :high})
    end
  end

  # --- #33 bounce_waiting_routes -------------------------------------------------

  describe "#33 waiting routes" do
    test "an item that is not pending", %{store: store} do
      refuse(
        store,
        &put_in(&1.bounce_waiting_routes[@feed], MapSet.new([{@m1, 1}])),
        {:row, :bounce_waiting_routes, @feed}
      )
    end

    test "an item for a missing message", %{store: store} do
      missing = {@person, @sender, "missing"}

      refuse(
        store,
        &put_in(&1.bounce_waiting_routes[@feed], MapSet.new([{missing, 1}])),
        {:row, :bounce_waiting_routes, @feed}
      )
    end

    test "an item whose selection became terminal under the route", %{store: store} do
      store = deferred(store)

      refuse(
        store,
        &update_in(&1.bounce_revisions[@m1].revisions[1].selection, fn selection ->
          %{selection | "decision" => "suppressed", "reason" => "terminal"}
        end),
        {:bounce_route_item, @feed, {@m1, 1}}
      )
    end

    test "an item whose message moved to another session", %{store: store} do
      store = deferred(store)
      moved = {@person, @origin, @handle2}

      # The item also waits under the new session, so only the old route,
      # which is not touched, is wrong.
      refuse(
        store,
        fn engine ->
          engine
          |> put_in([Access.key(:messages), @m1, :session_handle], @handle2)
          |> put_in([Access.key(:bounce_waiting_routes), moved], MapSet.new([{@m1, 1}]))
        end,
        {:bounce_route_item, @feed, {@m1, 1}}
      )
    end

    test "a route under the wrong session key", %{store: store} do
      store = deferred(store)
      wrong = {@person, @origin, @handle2}

      refuse(
        store,
        &put_in(&1.bounce_waiting_routes[wrong], &1.bounce_waiting_routes[@feed]),
        {:row, :bounce_waiting_routes, wrong}
      )
    end
  end

  # --- #34 derived bounce indexes ------------------------------------------------

  describe "#34 derived bounce indexes" do
    test "a pending revision dropped from its route without requeueing", %{store: store} do
      store = deferred(store)

      refuse(
        store,
        &put_in(&1.bounce_waiting_routes[@feed], MapSet.new([{@m3, 1}])),
        {:bounce_queue, @m1}
      )
    end

    test "a pending deadline moved without moving its index entry", %{store: store} do
      store = scored(store)

      refuse(
        store,
        &update_in(&1.bounce_revisions[@m1].revisions[1], fn revision ->
          due_at = revision.selection["due_at_ms"]

          %{
            revision
            | accepted_at_ms: revision.accepted_at_ms + 1,
              selection: %{revision.selection | "due_at_ms" => due_at + 1}
          }
        end),
        {:bounce_queue, @m1}
      )
    end

    test "a waiting revision that is also queued", %{store: store} do
      store = deferred(store)
      due = due(store.engine)

      next =
        Map.update!(
          store.engine,
          :bounce_deadline_index,
          &:gb_trees.enter({due, @m1, 1}, true, &1)
        )

      touched = MapSet.new([{:row, :bounce_revisions, @m1}])
      assert_refused(store, next, touched, {:address, {:bounce_queue, @m1}})
    end

    test "a person discussion missing from the discussion index", %{store: store} do
      {next, _reply, touched} =
        Engine.request_line_with_touches(
          store.engine,
          @reader,
          line(20, %{
            "type" => "discussion",
            "sender_id" => @sender,
            "message_id" => "m3",
            "request_revision" => 1,
            "text" => "p"
          }),
          5_000
        )

      assert MapSet.member?(next.bounce_discussed_messages, @m3)
      next = %{next | bounce_discussed_messages: store.engine.bounce_discussed_messages}
      assert_refused(store, next, touched, {:derived, :bounce_discussed_messages})
    end

    test "an origin revision entry missing from the origin entry index", %{store: store} do
      {next, _reply, touched} =
        Engine.request_line_with_touches(
          store.engine,
          @origin,
          line(20, forward("m4")),
          5_000
        )

      member = {@person, @origin, @handle, @sender, "m4"}
      assert MapSet.member?(next.bounce_origin_entries, member)
      next = %{next | bounce_origin_entries: MapSet.delete(next.bounce_origin_entries, member)}
      assert_refused(store, next, touched, {:derived, :bounce_origin_entries})
    end

    test "an appended advisory missing from the advisory id index", %{store: store} do
      store = requeued(deferred(store))

      {next, :ok, touched} =
        Engine.with_touches(
          store.engine,
          &{BounceDecision.evaluate_due(&1, due(&1, @m3) + 50), :ok}
        )

      id = advisory_id(next, 1)
      next = %{next | advisory_id_index: Map.delete(next.advisory_id_index, {@feed, id})}
      assert_refused(store, next, touched, {:derived, :advisory_id_index})
    end

    test "a removed advisory left in the advisory id index", %{store: store} do
      store = bounced(store)
      engine = store.engine

      next =
        update_in(engine.advisory_feeds[@feed], fn feed ->
          %{feed | high: 1, entries: Map.delete(feed.entries, 2)}
        end)

      assert_refused(store, next, codec_diff(store.engine, next), {:derived, :advisory_id_index})
    end
  end

  # --- #3 open_default_index -----------------------------------------------------

  describe "#3 open default index" do
    test "a non-integer entry for a touched message", %{store: store} do
      engine = store.engine
      next = put_in(engine.open_default_index[@m2], :stale)
      touched = MapSet.new([{:row, :messages, @m2}])
      assert_refused(store, next, touched, {:derived, :open_default_index})
    end

    test "a string entry for a touched message", %{store: store} do
      engine = store.engine
      next = put_in(engine.open_default_index[@m1], "later")
      touched = MapSet.new([{:row, :messages, @m1}])
      assert_refused(store, next, touched, {:derived, :open_default_index})
    end
  end

  # --- #29 bounce_levels ---------------------------------------------------------

  describe "#29 bounce configuration" do
    test "a configuration ahead of its person's conversation high", %{store: store} do
      {:ok, next, _config} =
        BounceDecision.configure_levels(
          store.engine,
          @person,
          AttentionServer.BouncePolicy.default_levels(),
          5_000
        )

      store = accept(store, next)

      refuse(
        store,
        &put_in(&1.bounce_levels[@person].after_conversation_seq, 99),
        {:row, :bounce_levels, @person}
      )
    end

    test "removing the person of a configuration", %{store: store} do
      {:ok, next, _config} =
        BounceDecision.configure_levels(
          store.engine,
          @other,
          AttentionServer.BouncePolicy.default_levels(),
          5_000
        )

      store = accept(store, next)

      refuse(
        store,
        &%{&1 | persons: Map.delete(&1.persons, @other)},
        {:row, :bounce_levels, @other}
      )
    end
  end

  # --- states --------------------------------------------------------------------

  defp scored(store) do
    entries =
      for id <- ["m1", "m3"],
          do: %{"sender_id" => @sender, "message_id" => id, "score" => 0.9}

    transition(store, &BounceDecision.record_scores(&1, @person, entries, 2_000))
  end

  defp deferred(store) do
    store = scored(store)
    transition(store, &BounceDecision.evaluate_due(&1, due(&1, @m3)))
  end

  defp requeued(store),
    do: transition(store, &(&1 |> supported() |> BounceDecision.requeue_supported_session(@feed)))

  defp bounced(store) do
    store = requeued(deferred(store))
    transition(store, &BounceDecision.evaluate_due(&1, due(&1, @m3) + 50))
  end

  defp supported(engine) do
    session = engine.origin_sessions[@feed]
    Engine.put_row(engine, :origin_sessions, @feed, put_in(session.support["advisory_v1"], true))
  end

  defp ack(engine, id) do
    {:ok, engine, _reply} = BounceDecision.advisory_ack(engine, @origin, @handle, id)
    engine
  end

  defp due(engine, key \\ @m1), do: selection(engine, key)["due_at_ms"]

  defp selection(engine, key, revision \\ nil) do
    state = engine.bounce_revisions[key]
    get_in(state, [:revisions, revision || state.current_substantive_revision, :selection])
  end

  defp advisory_id(engine, seq), do: engine.advisory_feeds[@feed].entries[seq]["advisory_id"]

  defp advisory_seq(engine, {_, _, id}) do
    Enum.find_value(engine.advisory_feeds[@feed].entries, fn {seq, entry} ->
      if entry["message_id"] == id, do: seq
    end)
  end

  # --- helpers -------------------------------------------------------------------

  defp transition(store, fun) do
    {next, :ok, touched} = Engine.with_touches(store.engine, &{fun.(&1), :ok})
    assert MapSet.subset?(codec_diff(store.engine, next), touched)
    assert :ok = Store.incremental_verdict(store, next, touched)
    assert {:ok, store} = Store.commit(store, {next, touched}, audit())
    store
  end

  defp wire(store, actor, n, frame) do
    {next, {:ok, response}, touched} =
      Engine.request_line_with_touches(store.engine, actor, line(n, frame), 3_000 + n)

    refute response =~ ~s("type":"error"), response
    assert :ok = Store.incremental_verdict(store, next, touched)
    assert {:ok, store} = Store.commit(store, {next, touched}, audit())
    store
  end

  defp accept(store, next) do
    touched = codec_diff(store.engine, next)
    assert :ok = Store.incremental_verdict(store, next, touched)
    assert {:ok, store} = Store.commit(store, {next, touched}, audit())
    store
  end

  defp refuse(store, mutate, address) do
    next = mutate.(store.engine)
    assert_refused(store, next, codec_diff(store.engine, next), {:address, address})
  end

  defp assert_refused(store, next, touched, expected) do
    assert Store.incremental_verdict(store, next, touched) == {:error, expected}
    assert {:error, :storage_unavailable, _} = Store.commit(store, {next, touched}, audit())
  end

  defp line(n, frame), do: frame |> Map.merge(%{"v" => 1, "op_id" => op(n)}) |> JSON.encode_line()

  defp op(n), do: "00000000-0000-4000-8000-" <> String.pad_leading(Integer.to_string(n), 12, "0")

  defp opened(id, revision),
    do: %{
      "type" => "message_open",
      "sender_id" => @sender,
      "message_id" => id,
      "request_revision" => revision
    }

  defp forward(id, extra \\ %{}) do
    %{
      "type" => "forward",
      "session_handle" => @handle,
      "route" => "claude-code",
      "cli_version" => "1",
      "unsupported_reason" => nil,
      "advisory_v1" => false,
      "message" => Map.merge(message(id), extra)
    }
  end

  defp message(id, title \\ nil) do
    %{
      "sender_id" => @sender,
      "message_id" => id,
      "created_at" => 1,
      "title" => title || id,
      "message_type" => "fyi",
      "answer_ui" => %{
        "schema" => "attention-client.actions/1",
        "actions" => [
          %{
            "id" => "seen",
            "label" => "Seen",
            "kind" => "value",
            "value" => %{"acknowledgment" => "seen"}
          }
        ]
      }
    }
  end

  defp audit, do: %{person: @person, actor: @origin, action: "keyed_bounce_probe"}

  defp codec_diff(old, next) do
    a = encode(old)
    b = encode(next)

    MapSet.union(MapSet.new(Map.keys(a)), MapSet.new(Map.keys(b)))
    |> Enum.reject(&(Map.fetch(a, &1) === Map.fetch(b, &1)))
    |> MapSet.new(&elem(:erlang.binary_to_term(&1), 1))
  end

  defp encode(engine) do
    Map.new(RecordCodec.encode(LargeData.persist_refs(engine)), fn {key, bytes, _} ->
      {key, bytes}
    end)
  end
end
