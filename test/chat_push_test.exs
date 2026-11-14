defmodule AttentionServer.ChatPushTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Engine, JSON, Store}

  @person "person"
  @origin "origin"
  @reader "reader"
  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @token String.duplicate("ab", 32)
  @accepted %{state: "provider_accepted", http_status: 200}

  defp op do
    n = System.unique_integer([:positive])
    "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"
  end

  defp request(engine, actor, frame) do
    frame = Map.merge(%{"v" => 1, "op_id" => op()}, frame)
    {engine, {:ok, line}} = Engine.request_line(engine, actor, JSON.encode!(frame) <> "\n")
    {:ok, %{"type" => "ok"} = reply} = JSON.decode_line(line)
    {engine, reply}
  end

  defp setup_engine do
    {:ok, engine} = Engine.setup_person(Engine.new(), @person)
    {:ok, engine} = Engine.setup_device(engine, @person, @origin)
    {:ok, engine} = Engine.setup_device(engine, @person, @reader)
    {:ok, engine} = Engine.setup_sender(engine, @origin, "app:test")
    engine
  end

  defp register(engine, revision, token \\ @token, device \\ @reader) do
    request(engine, device, %{
      "type" => "push_register",
      "revision" => revision,
      "profile_id" => "harklight-sandbox",
      "address_kind" => "apns_token",
      "address" => token
    })
  end

  defp forward(engine, id) do
    request(engine, @origin, %{
      "type" => "forward",
      "session_handle" => @handle,
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => id,
        "created_at" => 1,
        "title" => "Request #{id}",
        "body" => "Request body",
        "display_name" => "Agent #{id}"
      }
    })
  end

  defp discussion(engine, id) do
    request(engine, @reader, %{
      "type" => "discussion",
      "sender_id" => "app:test",
      "message_id" => id,
      "request_revision" => 1,
      "text" => "Please explain"
    })
  end

  defp reply(engine, id, text) do
    {engine, discussion} = discussion(engine, id)

    request(engine, @origin, %{
      "type" => "conversation_reply",
      "sender_id" => "app:test",
      "message_id" => id,
      "request_revision" => 1,
      "in_reply_to" => String.to_integer(discussion["event_id"]),
      "text" => text
    })
  end

  defp push_binding(engine), do: engine.push_bindings[{@person, @reader}]
  defp due(engine), do: push_binding(engine).next_at

  defp assert_round_trip(engine) do
    dir = Path.join(System.tmp_dir!(), "chat-push-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "state.sqlite3")
    {:ok, store} = Store.open(path)

    assert {:ok, store} =
             Store.commit(store, engine, %{person: @person, actor: @reader, action: "test"})

    :ok = Store.close(store)
    assert {:ok, reopened} = Store.open(path)
    assert reopened.engine.push_bindings == engine.push_bindings
    :ok = Store.close(reopened)
  end

  test "request then latest chat have independent acceptance and no second batch delay" do
    {engine, _} = register(setup_engine(), 1)
    {engine, _} = forward(engine, "one")
    {engine, first} = reply(engine, "one", "First reply")
    {engine, _} = forward(engine, "two")
    {engine, latest} = reply(engine, "two", " Latest\n reply ")
    assert first["seq"] < latest["seq"]
    refute MapSet.member?(engine.conversation_opened, {@person, @reader})

    # Receiving/ACKing chat is not reading it and must not remove the alert.
    high = engine.conversation_highs[@person]
    {:ok, engine, _} = Engine.acknowledge_conversation(engine, @reader, high, high)
    now = due(engine)
    {:ok, engine, request_claim} = Engine.claim_push(engine, now)
    assert request_claim.stream == :request
    assert request_claim.conversation_high == 0
    assert request_claim.alert["title"] == "2 new requests"
    {:ok, engine} = Engine.finish_push(engine, request_claim, @accepted, now + 1)
    assert push_binding(engine).pending_high == 0
    assert push_binding(engine).accepted_high == 2
    assert push_binding(engine).accepted_conversation_high == 0
    assert push_binding(engine).pending_conversation_high > 0
    assert due(engine) == now + 1

    {:ok, engine, chat} = Engine.claim_push(engine, now + 1)
    assert chat.stream == :conversation
    assert chat.high == 2
    assert chat.alert["title"] == "Agent two"
    assert chat.alert["body"] == "Latest reply"
    assert chat.locator["target"] == "chat"
    assert chat.locator["message_id"] == "two"
    assert chat.locator["acceptance_seq"] == 2
    assert map_size(chat.locator) == 8
    {:ok, engine} = Engine.finish_push(engine, chat, @accepted, now + 2)
    assert push_binding(engine).accepted_conversation_high == chat.conversation_high
    assert push_binding(engine).pending_conversation_high == 0
    assert push_binding(engine).state == "ready"
    assert engine.chat_read_positions == %{}
    assert_round_trip(engine)
  end

  test "failed selected claims retain both streams and newer work survives chat success" do
    {engine, _} = register(setup_engine(), 1)
    {engine, _} = forward(engine, "one")
    {engine, _} = reply(engine, "one", "First")
    now = due(engine)
    {:ok, engine, claim} = Engine.claim_push(engine, now)
    {:ok, engine} = Engine.finish_push(engine, claim, %{state: "retry"}, now + 1)
    assert push_binding(engine).accepted_high == 0
    assert push_binding(engine).accepted_conversation_high == 0
    assert push_binding(engine).pending_high == 1
    assert push_binding(engine).pending_conversation_high > 0
    {:ok, engine, claim} = Engine.claim_push(engine, due(engine))
    {:ok, engine} = Engine.finish_push(engine, claim, @accepted, now + 2_000)
    {:ok, engine, chat} = Engine.claim_push(engine, due(engine))
    {engine, _} = forward(engine, "two")
    {engine, _} = reply(engine, "two", "Newer")
    pending_chat = push_binding(engine).pending_conversation_high
    {:ok, engine} = Engine.finish_push(engine, chat, @accepted, now + 2_001)
    assert push_binding(engine).accepted_high == 1
    assert push_binding(engine).pending_high == 2
    assert push_binding(engine).accepted_conversation_high == chat.conversation_high
    assert push_binding(engine).pending_conversation_high == pending_chat
    assert pending_chat > chat.conversation_high

    {:ok, engine, next_request} = Engine.claim_push(engine, due(engine))
    assert next_request.stream == :request
    assert next_request.conversation_high == chat.conversation_high
    {:ok, engine} = Engine.finish_push(engine, next_request, @accepted, now + 2_002)
    {:ok, _, next_chat} = Engine.claim_push(engine, due(engine))
    assert next_chat.alert["body"] == "Newer"
  end

  test "new requests do not postpone an already-due chat remainder" do
    {engine, _} = register(setup_engine(), 1)
    {engine, _} = forward(engine, "one")
    {engine, _} = reply(engine, "one", "Pending")
    now = due(engine)
    {:ok, engine, ordinary} = Engine.claim_push(engine, now)
    {:ok, engine} = Engine.finish_push(engine, ordinary, @accepted, now + 1)
    {engine, _} = forward(engine, "two")
    assert due(engine) == now + 1
    {:ok, engine, newer_request} = Engine.claim_push(engine, now + 1)
    assert newer_request.stream == :request
    {:ok, engine} = Engine.finish_push(engine, newer_request, @accepted, now + 2)
    {:ok, _, chat} = Engine.claim_push(engine, now + 2)
    assert chat.locator["message_id"] == "one"
    assert chat.alert["body"] == "Pending"
  end

  test "background-only streams are separate and person facts never alert" do
    {engine, _} = register(setup_engine(), 1)
    {engine, _} = forward(engine, "one")
    {:ok, engine} = Engine.open_conversation(engine, @reader)
    {engine, _} = discussion(engine, "one")

    {engine, _} =
      request(engine, @reader, %{
        "type" => "answer",
        "sender_id" => "app:test",
        "message_id" => "one",
        "request_revision" => 1,
        "answer" => "yes"
      })

    now = due(engine)
    {:ok, engine, ordinary} = Engine.claim_push(engine, now)
    assert ordinary.presentation == :background
    assert ordinary.stream == :request
    assert ordinary.conversation_high == 0
    {:ok, engine} = Engine.finish_push(engine, ordinary, @accepted, now + 1)
    {:ok, engine, conversation} = Engine.claim_push(engine, now + 1)
    assert conversation.stream == :conversation
    assert conversation.presentation == :background
    refute Map.has_key?(conversation, :locator)
    {:ok, engine} = Engine.finish_push(engine, conversation, @accepted, now + 2)
    assert push_binding(engine).state == "ready"
  end

  test "open request replies alert without opened marker and origin never receives its own preview" do
    {engine, _} = register(setup_engine(), 1)
    {engine, _} = register(engine, 1, String.duplicate("cd", 32), @origin)
    {engine, _} = forward(engine, "one")
    {engine, _} = reply(engine, "one", "Open reply")
    assert engine.messages[{@person, "app:test", "one"}].terminal == nil
    assert engine.push_bindings[{@person, @origin}].pending_conversation_high == 0
    now = due(engine)

    {engine, claims} =
      Enum.reduce(1..3, {engine, []}, fn _, {engine, claims} ->
        {:ok, engine, claim} = Engine.claim_push(engine, now + 10)
        assert claim != nil
        {:ok, engine} = Engine.finish_push(engine, claim, @accepted, now + 10)
        {engine, [claim | claims]}
      end)

    conversation_claims = Enum.filter(claims, &(&1.stream == :conversation))

    assert [%{device: @reader, presentation: :alert, alert: %{"body" => "Open reply"}}] =
             conversation_claims

    assert engine.chat_read_positions == %{}
  end

  test "reply to an answered request is closed and queues no conversation push" do
    {engine, _} = register(setup_engine(), 1)
    {engine, _} = register(engine, 1, String.duplicate("cd", 32), @origin)
    {engine, _} = forward(engine, "one")
    {engine, discussion} = discussion(engine, "one")

    {engine, _} =
      request(engine, @reader, %{
        "type" => "answer",
        "sender_id" => "app:test",
        "message_id" => "one",
        "request_revision" => 1,
        "answer" => "yes"
      })

    frame =
      %{
        "v" => 1,
        "op_id" => op(),
        "type" => "conversation_reply",
        "sender_id" => "app:test",
        "message_id" => "one",
        "request_revision" => 1,
        "in_reply_to" => String.to_integer(discussion["event_id"]),
        "text" => "After answer"
      }

    {after_reply, {:ok, line}} =
      Engine.request_line(engine, @origin, JSON.encode!(frame) <> "\n")

    assert {:ok, %{"type" => "error", "reason" => "closed"}} = JSON.decode_line(line)
    assert after_reply.push_bindings == engine.push_bindings
    assert after_reply.conversation_highs == engine.conversation_highs
  end

  test "new and replacement registration are prospective; same active endpoint preserves work and identity" do
    engine = setup_engine()
    {engine, _} = forward(engine, "one")
    {engine, _} = reply(engine, "one", "Before registration")
    baseline = engine.conversation_highs[@person]
    {engine, registered} = register(engine, 1)
    assert push_binding(engine).accepted_conversation_high == baseline
    assert push_binding(engine).pending_conversation_high == 0
    {engine, _} = reply(engine, "one", "Pending")
    before = push_binding(engine)
    high = engine.conversation_highs[@person]
    {:ok, engine, _} = Engine.acknowledge_conversation(engine, @reader, high, high)
    {engine, refreshed} = register(engine, 2)
    assert refreshed["binding_id"] == registered["binding_id"]
    assert push_binding(engine).accepted_high == before.accepted_high
    assert push_binding(engine).accepted_conversation_high == baseline
    assert push_binding(engine).pending_high == before.pending_high
    assert push_binding(engine).pending_conversation_high == before.pending_conversation_high
    assert push_binding(engine).next_at == before.next_at
    {engine, replaced} = register(engine, 3, String.duplicate("ef", 32))
    refute replaced["binding_id"] == refreshed["binding_id"]
    assert push_binding(engine).accepted_conversation_high == high
    assert push_binding(engine).pending_conversation_high == 0
  end

  test "same endpoint registration invalidates an in-flight claim without dropping pending work" do
    {engine, _} = register(setup_engine(), 1)
    {engine, _} = forward(engine, "one")
    {engine, _} = reply(engine, "one", "Pending")
    {:ok, engine, old_claim} = Engine.claim_push(engine, due(engine))
    before = push_binding(engine)
    assert_round_trip(engine)
    {engine, _} = register(engine, 2)
    assert push_binding(engine).pending_high == before.pending_high
    assert push_binding(engine).pending_conversation_high == before.pending_conversation_high
    assert {:ok, ^engine} = Engine.finish_push(engine, old_claim, @accepted, due(engine))
  end

  test "expired and rejected chat submissions never advance push acceptance" do
    for outcome <- [%{state: "blocked_configuration"}, %{state: "expired"}] do
      {engine, _} = register(setup_engine(), 1)
      {engine, _} = forward(engine, "one")
      {engine, _} = reply(engine, "one", "Pending")
      now = due(engine)
      {:ok, engine, ordinary} = Engine.claim_push(engine, now)
      {:ok, engine} = Engine.finish_push(engine, ordinary, @accepted, now + 1)
      {:ok, engine, chat} = Engine.claim_push(engine, now + 1)
      {:ok, engine} = Engine.finish_push(engine, chat, outcome, now + 2)
      assert push_binding(engine).accepted_high == ordinary.high
      assert push_binding(engine).accepted_conversation_high == 0
      assert push_binding(engine).state == outcome.state
    end
  end
end
