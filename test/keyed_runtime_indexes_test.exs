defmodule AttentionServer.KeyedRuntimeIndexesTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Engine, JSON, PushIndex, RouteBIndex}

  test "keyed lookup rebuilds bounded candidates without changing the legacy path" do
    person = "person"
    origin = "origin"
    sender = "app:test/worker"
    handle = "00000000-0000-4000-8000-000000000010"
    item = {person, sender, "request"}
    scope = {person, origin, handle}
    reader = {person, "reader"}

    {:ok, engine} = Engine.setup_person(Engine.new(), person)
    {:ok, engine} = Engine.setup_device(engine, person, origin)
    {:ok, engine} = Engine.setup_sender(engine, origin, sender)

    line =
      JSON.encode_line(%{
        "v" => 1,
        "type" => "forward",
        "op_id" => "00000000-0000-4000-8000-000000000011",
        "session_handle" => handle,
        "message" => %{
          "sender_id" => sender,
          "message_id" => "request",
          "created_at" => 1,
          "title" => "Fixture"
        }
      })

    {engine, {:ok, body}} = Engine.request_line(engine, origin, line, 100)
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(body)

    binding = %{
      active: true,
      address: "endpoint",
      state: "queued",
      next_at: 100,
      pending_high: 1,
      pending_conversation_high: 0
    }

    engine =
      engine
      |> put_in([Access.key!(:origin_sessions), scope, :support], %{"origin_listener" => true})
      |> put_in([Access.key!(:push_bindings), reader], binding)
      |> put_in([Access.key!(:conversation_events), {person, 1}], %{
        key: item,
        payload: %{"kind" => "reply", "author" => "origin", "event_id" => "1"}
      })

    engine = %{engine | route_b_index: RouteBIndex.rebuild(engine)}
    legacy = engine.route_b_index
    assert legacy.request_keys_by_id == nil
    assert legacy.push_bindings == nil
    assert Enum.empty?(RouteBIndex.pending_messages(legacy, person))
    assert Enum.empty?(RouteBIndex.origin_replies(legacy, person))
    assert RouteBIndex.session_messages(legacy, scope) == []

    engine = RouteBIndex.enable_keyed_lookups(engine)
    index = engine.route_b_index
    assert index.request_keys_by_id[{person, "request"}] == [item]
    assert Enum.to_list(RouteBIndex.pending_messages(index, person)) == [{1, item}]
    assert Enum.to_list(RouteBIndex.origin_replies(index, person)) == [{1, item}]
    assert RouteBIndex.session_messages(index, scope) == [item]
    assert index.covered_sessions_by_origin[{person, origin}] == MapSet.new([handle])
    assert RouteBIndex.push_badge(index, person) == 1
    assert PushIndex.owner(index.push_bindings, "endpoint") == reader
    assert PushIndex.ready(index.push_bindings, 99) == nil
    assert PushIndex.ready(index.push_bindings, 100) == reader

    enabled = RouteBIndex.enable_agent_feed(engine)
    assert RouteBIndex.session_messages(enabled.route_b_index, scope) == [item]
    assert RouteBIndex.rebuild(enabled) == enabled.route_b_index
  end

  test "push index keeps unrelated due work when a person pauses and resumes" do
    a = {"a", "device"}
    b = {"b", "device"}

    due = %{
      active: true,
      state: "queued",
      next_at: 10,
      pending_high: 1,
      pending_conversation_high: 0
    }

    bindings = %{a => Map.put(due, :address, "first"), b => Map.put(due, :address, "second")}

    index = PushIndex.build(bindings, %{"a" => "STOPPED"})
    assert PushIndex.ready(index, 10) == b
    assert PushIndex.owner(index, "first") == a
    assert PushIndex.keys(index, "a") == MapSet.new([a])

    resumed = PushIndex.set_stopped(index, "a", false, bindings)
    assert PushIndex.ready(resumed, 10) == a
    paused = PushIndex.set_stopped(resumed, "a", true, bindings)
    assert PushIndex.ready(paused, 10) == b
  end
end
