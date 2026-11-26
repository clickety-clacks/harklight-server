defmodule AttentionServer.SharedMessagesTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{AgentChangeLog, Engine, SharedAnswers, SharedMessages, Store}
  alias AttentionServer.ClientAPI.Idempotency
  alias Exqlite.Sqlite3

  @moduletag :tmp_dir

  @sender "10000000-0000-4000-8000-000000000001"
  @other "10000000-0000-4000-8000-000000000002"
  @third "10000000-0000-4000-8000-000000000003"
  @organization "20000000-0000-4000-8000-000000000001"
  @second_organization "20000000-0000-4000-8000-000000000002"

  test "one durable message record references distinct personal rows", %{tmp_dir: dir} do
    path = Path.join(dir, "relay.sqlite3")
    assert {:ok, store} = Store.open_relay(path)
    assert {:ok, store} = Store.enable_change_log(store)

    {engine, :ok, touched} =
      Engine.with_touches(store.engine, fn initial ->
        engine = people(initial)
        assert {:ok, engine} =
                 Engine.replace_hosted_organization(engine, "org", [@sender, @other, @third], @organization)

        assert {:ok, engine, %{organization_id: @organization}, [{@other, :reached}, {@third, :reached}]} =
                 SharedMessages.create(engine, attrs(:first), [@other, @third, @other], nil)

        {engine, :ok}
      end)

    scope = Idempotency.agent_scope(@sender, "sender-key")
    key = "30000000-0000-4000-8000-000000000001"
    outcomes = for person <- [@other, @third], do: %{"person_id" => person, "status" => "reached"}
    receipt = {{@sender, "sender-key"}, key, "request-digest", "shared-a", outcomes}
    audit = %{person: @sender, actor: "fixture", action: "send", shared_send_receipt: receipt}
    assert {:ok, nil} = Idempotency.lookup(store, scope, key)

    assert :ok =
             Sqlite3.execute(store.db, """
             CREATE TEMP TRIGGER refuse_shared_receipt BEFORE INSERT ON client_idempotency
             BEGIN SELECT RAISE(ABORT, 'receipt failpoint'); END
             """)

    assert {:error, :storage_unavailable, ^store} = Store.commit(store, {engine, touched}, audit)
    assert {:ok, nil} = Idempotency.lookup(store, scope, key)
    assert {:ok, rolled_back} = Store.open_relay(path)
    assert rolled_back.engine.shared_messages == %{}
    assert rolled_back.engine.shared_recipients == %{}
    assert :ok = Store.close(rolled_back)

    assert :ok = Sqlite3.execute(store.db, "DROP TRIGGER refuse_shared_receipt")
    assert {:ok, store} = Store.commit(store, {engine, touched}, audit)
    assert :ok = Store.close(store)
    assert {:ok, restored} = Store.open_relay(path)
    assert [{"shared-a", %{message: %{"title" => "Launch"}}}] =
             Map.to_list(restored.engine.shared_messages)

    assert %{{"shared-a", @other} => first, {"shared-a", @third} => second} =
             restored.engine.shared_recipients

    assert {:ok, restored} = Store.enable_change_log(restored)
    assert {:ok, %{body: body, digest: "request-digest", status: 200}} =
             Idempotency.lookup(restored, scope, key)
    assert %{"rev" => 1, "message" => %{"rev" => 1, "item_id" => "shared-a",
             "shared" => %{"organization_id" => @organization}}, "recipients" => ^outcomes} = body
    assert {:ok, [%{resource: {:shared_message, "shared-a"}, rev: 1}], false, 1} =
             AgentChangeLog.read(restored, {@sender, "sender-key"}, 0, 10, nil, true)
    assert {:ok, [], false, 1} = AgentChangeLog.read(restored, {@sender, "sender-key"}, 0, 10, nil)

    assert first.item_id != second.item_id
    assert Map.keys(first) |> Enum.sort() == [:item_id, :reminder, :seen_revision]
    assert Map.keys(second) |> Enum.sort() == [:item_id, :reminder, :seen_revision]
    assert :ok = Store.close(restored)
  end

  test "authorization rejects the whole send and distinguishes overlapping organizations" do
    engine = people(Engine.new())
    assert {:ok, engine} =
             Engine.replace_hosted_organization(engine, "org", [@sender, @other], @organization)

    assert {:ok, engine} =
             Engine.replace_hosted_organization(engine, "second", [@sender, @other], @second_organization)

    assert {:error, :recipient_refused} =
             SharedMessages.create(engine, attrs(:first), [@other, @third], nil)

    assert {:error, :organization_required, [@organization, @second_organization]} =
             SharedMessages.create(engine, attrs(:first), [@other], nil)

    assert {:ok, updated, %{organization_id: @second_organization}, [{@other, :reached}]} =
             SharedMessages.create(engine, attrs(:first), [@other], @second_organization)

    assert map_size(updated.shared_recipients) == 1
  end

  test "an authorized unavailable person is a delivery outcome and cannot be the named decider" do
    engine = people(Engine.new())
    assert {:ok, engine} =
             Engine.replace_hosted_organization(engine, "org", [@sender, @other, @third], @organization)

    assert {:ok, engine, "STOPPED"} = Engine.set_hosted_delivery_state(engine, @third, "STOPPED")

    outcomes = [{@other, :reached}, {@third, :unreachable}]
    assert {:error, :decider_unreachable, ^outcomes} =
             SharedMessages.create(engine, attrs({:named, [@third]}), [@other, @third], nil)

    assert {:ok, updated, _, ^outcomes} =
             SharedMessages.create(engine, attrs(:first), [@other, @third], nil)

    assert Map.keys(updated.shared_recipients) == [{"shared-a", @other}]
  end

  test "only everyone mode creates a system default for each unsettled recipient" do
    engine = people(Engine.new())
    assert {:ok, engine} =
             Engine.replace_hosted_organization(engine, "org", [@sender, @other, @third], @organization)

    message = %{
      "title" => "Date", "default" => %{"at" => 100, "value" => "Thursday"},
      "answer_ui" => %{"actions" => [%{"id" => "thursday", "kind" => "value",
                                       "value" => "Thursday", "label" => "Thursday"}]}
    }

    attrs = Map.merge(attrs(:everyone), %{:message => message, "created_at_ms" => 1})
    assert {:ok, engine, open, _} = SharedMessages.create(engine, attrs, [@other, @third], nil)
    named = %{attrs | id: "named", answer_policy: {:named, [@other]}}
    assert {:ok, engine, _, _} = SharedMessages.create(engine, named, [@other, @third], nil)

    assert SharedAnswers.next_default(engine) == 100
    assert {^engine, []} = SharedAnswers.apply_defaults(engine, 99)
    recipient = engine.shared_recipients[{open.id, @other}]
    assert {:ok, engine, _} =
             SharedAnswers.answer(engine, open, recipient, @other, 1, {:comment, "Manual"}, 90)

    assert {updated, [default_id]} = SharedAnswers.apply_defaults(engine, 100)
    answer = updated.shared_answers[default_id]
    assert answer.person == @third
    assert %{"kind" => "server"} = answer.speaker
    assert %{"kind" => "default", "value" => "Thursday"} = answer.outcome
    assert %{"by_this_person" => false} = SharedAnswers.person_terminal(answer, @third)
    assert {:ok, _} = Map.fetch(updated.shared_recipients, {"named", @third})
    refute Map.has_key?(updated.shared_recipients[{"named", @third}], :answer_id)
    assert SharedAnswers.next_default(updated) == nil
  end

  test "push counts one unit for request attention or another person's unread Chat" do
    engine = people(Engine.new())
    assert {:ok, engine} =
             Engine.replace_hosted_organization(engine, "org", [@sender, @other, @third], @organization)
    assert {:ok, engine, room, _} =
             SharedMessages.create(engine, attrs(:first), [@other, @third], nil)
    assert SharedMessages.badge_count(engine, @other) == 1

    entry = %{id: "turn", at_ms: 10, text: "Advice", speaker: %{"kind" => "person", "id" => @other}}
    assert {:ok, engine, _} = SharedMessages.reply(engine, room, {:person, @other}, entry)
    assert SharedMessages.badge_count(engine, @third) == 1

    room = engine.shared_messages[room.id]
    recipient = engine.shared_recipients[{room.id, @other}]
    assert {:ok, engine, _} =
             SharedAnswers.answer(engine, room, recipient, @other, 1, {:comment, "Accepted"}, 11)
    assert SharedMessages.badge_count(engine, @other) == 0
    assert SharedMessages.badge_count(engine, @third) == 1

    room = engine.shared_messages[room.id]
    recipient = engine.shared_recipients[{room.id, @third}]
    local_entry = SharedMessages.recipient_entry_id("turn", @third)
    assert {:ok, engine} = SharedMessages.read_position(engine, room, @third, recipient, local_entry)
    assert SharedMessages.badge_count(engine, @third) == 0

    assert {:ok, engine, _} =
             SharedMessages.reply(engine, room, :sender, %{entry | id: "later", at_ms: 12})
    assert SharedMessages.badge_count(engine, @third) == 1
    assert {:ok, engine} =
             Engine.replace_hosted_organization(engine, "org", [@sender, @other], @organization)
    assert SharedMessages.badge_count(engine, @third) == 0
  end

  defp people(engine) do
    Enum.zip([@sender, @other, @third], ["a", "b", "c"])
    |> Enum.reduce(engine, fn {person, letter}, acc ->
      assert {:ok, next, _} =
               Engine.hosted_account_create(acc, "account-" <> letter, false, person, String.duplicate(letter, 64))

      next
    end)
  end

  defp attrs(policy) do
    %{
      id: "shared-a",
      sender_person: @sender,
      sender_key: "sender-key",
      sender_id: "app:test",
      message: %{"title" => "Launch"},
      answer_policy: policy
    }
  end
end
