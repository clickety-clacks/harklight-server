defmodule AttentionServer.GroupsTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{Engine, Groups, Relay, SharedAnswers, SharedMessages, SharedReminders, Store}

  @moduletag :tmp_dir
  @manager "10000000-0000-4000-8000-000000000001"
  @invitee "10000000-0000-4000-8000-000000000002"
  @other "10000000-0000-4000-8000-000000000003"
  @group "20000000-0000-4000-8000-000000000001"
  @invitation "30000000-0000-4000-8000-000000000001"
  @organization "40000000-0000-4000-8000-000000000001"

  test "invitation acceptance follows only open conversations and leaving freezes history" do
    engine = local_people(Engine.new())
    assert {:ok, engine, %{name: "Planning", members: %{@manager => false}}} =
             Groups.create(engine, @group, @manager, " Planning ")

    assert {:error, :group_name_conflict} = Groups.create(engine, @other, @invitee, "planning")
    assert {:error, :not_found} = Groups.rename(engine, @group, @invitee, "New name")
    assert {:ok, engine, _, _} = Groups.invite(engine, @group, @manager, @invitee, @invitation)
    assert {:error, :not_found} = Groups.respond(engine, @invitation, @other, true)
    refute Map.has_key?(engine.groups[@group].members, @invitee)

    assert {:ok, engine, record, _} =
             SharedMessages.create(engine, attrs("open"), [@manager], engine.relay_instance_id)

    assert {:ok, engine, closed, _} =
             SharedMessages.create(engine, attrs("closed"), [@manager], engine.relay_instance_id)

    assert {:ok, engine} = SharedMessages.close_chat(engine, closed)
    assert {:ok, engine} = SharedMessages.revise(engine, record, 1, %{"title" => "Current"}, [], 11)
    assert {:ok, engine, _} =
             SharedMessages.reply(engine, engine.shared_messages["open"], :sender, entry("prior", 12))

    assert {:ok, engine, _} = Groups.respond(engine, @invitation, @invitee, true)
    refute Map.has_key?(engine.shared_recipients, {"closed", @invitee})

    item = engine.shared_recipients[{"open", @invitee}]
    assert %{"request_revision" => 2, "message" => %{"title" => "Current"}, "seen" => false} =
             SharedMessages.person_item(engine, @invitee, engine.shared_messages["open"], item, 1)

    assert [%{id: "prior"}] = SharedMessages.entries(engine, engine.shared_messages["open"])

    assert {:ok, engine, %{members: %{@manager => false, @invitee => true}}} =
             Groups.mute(engine, @group, @invitee, true)

    assert {:ok, engine} = Groups.leave(engine, @group, @invitee)
    assert {:error, :not_found} = Groups.member_group(engine, @group, @invitee)
    assert {:ok, engine} =
             SharedMessages.revise(engine, engine.shared_messages["open"], 2, %{"title" => "Later"}, [], 13)

    assert {:ok, engine, _} =
             SharedMessages.reply(engine, engine.shared_messages["open"], :sender, entry("later", 14))

    item = engine.shared_recipients[{"open", @invitee}]
    assert [%{id: "prior"}] =
             SharedMessages.entries(engine, engine.shared_messages["open"], item.frozen.entry_high)
    assert %{"request_revision" => 2, "message" => %{"title" => "Current"},
             "shared" => %{"chat_writable" => false, "can_answer" => false}} =
             SharedMessages.person_item(engine, @invitee, engine.shared_messages["open"], item, 2)
  end

  test "Hosted roster sync requires opted-in affiliation, preserves mute, and is durable", %{tmp_dir: dir} do
    path = Path.join(dir, "relay.sqlite3")
    assert {:ok, store} = Store.open_relay(path)

    {next, :ok, touched} =
      Engine.with_touches(store.engine, fn original ->
        {:ok, engine, _} = Engine.hosted_account_create(original, "member", false, @invitee, String.duplicate("a", 64))
        {:ok, engine} = Engine.replace_hosted_organization(engine, "business", [@invitee], @organization)
        assert {:error, :not_affiliated} = Groups.replace_hosted(engine, {"business", "default"}, @group, "Business", [@other], 1)
        {:ok, engine, _} = Groups.replace_hosted(engine, {"business", "default"}, @group, "Business", [@invitee], 1)
        {:ok, engine, _} = Groups.mute(engine, @group, @invitee, true)
        {:ok, engine, %{group_id: @group, rev: 1}} =
          Groups.replace_hosted(engine, {"business", "default"}, @other, "Business", [@invitee], 1)
        assert engine.groups[@group].members == %{@invitee => true}

        {engine, :ok}
      end)

    assert {:ok, store} = Store.commit(store, {next, touched}, %{person: @invitee, actor: "fixture", action: "groups"})
    assert :ok = Store.close(store)
    assert {:ok, reopened} = Store.open_relay(path)
    engine = reopened.engine
    assert engine.groups[@group].members == %{@invitee => true}
    assert {:error, :not_found} = Groups.leave(engine, @group, @invitee)
    assert {:ok, engine} = Engine.replace_hosted_organization(engine, "business", [], @organization)
    assert engine.groups[@group].members == %{}
    assert {:ok, engine} = Engine.delete_hosted_organization(engine, "business")
    assert engine.groups == %{}
    assert engine.hosted_group_refs[{"business", "default"}] == :deleted
    assert :ok = Store.close(reopened)
  end

  test "a late join cannot schedule reminders for a message accepted before the cutoff and revised after it" do
    now = System.system_time(:millisecond)
    accepted = now - 60_000
    set_cutoff(now)
    engine = local_people(Engine.new())
    assert {:ok, engine, _} = Groups.create(engine, @group, @manager, "Planning")
    assert {:ok, engine, _, _} = Groups.invite(engine, @group, @manager, @invitee, @invitation)
    attrs = Map.put(attrs("old"), "created_at_ms", accepted)
    assert {:ok, engine, record, _} =
             SharedMessages.create(engine, attrs, [@manager], engine.relay_instance_id)

    assert {:ok, engine} =
             SharedMessages.revise(engine, record, 1, %{"title" => "New plans"}, [], now + 1)

    {joined, :ok, touched} =
      Engine.with_touches(engine, fn engine ->
        {:ok, joined, _} = Groups.respond(engine, @invitation, @invitee, true)
        {joined, :ok}
      end)

    {scheduled, _} = SharedReminders.schedule(engine, joined, touched, now + 2)
    assert %{seen_revision: nil} = recipient = scheduled.shared_recipients[{"old", @invitee}]
    refute Map.has_key?(recipient, :reminder)
    assert {^scheduled, [], []} = SharedReminders.evaluate_due(scheduled, now + 10 * 86_400_000, true)
  end

  test "a late join keeps the substantive revision's three-day reminder deadline" do
    day = 86_400_000
    accepted = System.system_time(:millisecond)
    revised_at = accepted + div(day, 2)
    set_cutoff(accepted - 1)
    engine = local_people(Engine.new())
    assert {:ok, engine, _} = Groups.create(engine, @group, @manager, "Planning")
    assert {:ok, engine, _, _} = Groups.invite(engine, @group, @manager, @invitee, @invitation)
    attrs = Map.put(attrs("open"), "created_at_ms", accepted)
    assert {:ok, engine, record, _} =
             SharedMessages.create(engine, attrs, [@manager], engine.relay_instance_id)
    assert {:ok, engine} =
             SharedMessages.revise(engine, record, 1, %{"title" => "New plans"}, [], revised_at)

    {joined, :ok, touched} =
      Engine.with_touches(engine, fn state ->
        {:ok, joined, _} = Groups.respond(state, @invitation, @invitee, true)
        {joined, :ok}
      end)

    {scheduled, _} = SharedReminders.schedule(engine, joined, touched, revised_at + day)
    due = revised_at + 3 * day
    assert %{revision: 2, accepted_at_ms: ^revised_at, selection: %{"due_at_ms" => ^due}} =
             scheduled.shared_recipients[{"open", @invitee}].reminder
    assert {^scheduled, [], []} = SharedReminders.evaluate_due(scheduled, due - 1, true)
    assert {evaluated, [{"open", advisory_id}], [{"open", @invitee}]} =
             SharedReminders.evaluate_due(scheduled, due, true)
    assert %{"accepted_at_ms" => ^revised_at, "due_at_ms" => ^due, "bounce_after_days" => 3} =
             evaluated.shared_messages["open"].shared_advisories[advisory_id]
  end

  defp set_cutoff(at) do
    name = "ATTENTION_SERVER_BOUNCE_NOT_BEFORE"
    previous = System.get_env(name)
    on_exit(fn -> if previous, do: System.put_env(name, previous), else: System.delete_env(name) end)
    System.put_env(name, at |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601())
  end

  test "restarting with groups disabled leaves a due group default unsettled", %{tmp_dir: dir} do
    path = Path.join(dir, "relay.sqlite3")
    assert {:ok, store} = Store.open_relay(path)
    now = System.system_time(:millisecond)
    due = now + 60_000
    {next, :ok, touched} =
      Engine.with_touches(store.engine, fn initial ->
        engine = local_people(initial)
        {:ok, engine, _} = Groups.create(engine, @group, @manager, "Planning")
        message = %{
          "title" => "Date",
          "default" => %{"at" => due, "value" => "Thursday"},
          "answer_ui" => %{"actions" => [%{"id" => "thursday", "kind" => "value", "value" => "Thursday"}]}
        }
        fields = Map.merge(attrs("group-default"), %{:message => message, :answer_policy => :everyone, "created_at_ms" => now})
        {:ok, engine, _, _} = SharedMessages.create(engine, fields, [@manager], engine.relay_instance_id)
        {engine, :ok}
      end)

    assert {:ok, store} = Store.commit(store, {next, touched}, %{person: @manager, actor: "fixture", action: "group_default"})
    assert :ok = Store.close(store)
    assert {:ok, enabled} = Relay.start_link(store_path: path, shared_messages: true, groups: true)
    assert is_reference(:sys.get_state(enabled).default_timer)
    assert :ok = GenServer.stop(enabled)

    assert {:ok, disabled} = Relay.start_link(store_path: path, shared_messages: true, groups: false)
    state = :sys.replace_state(disabled, &%{&1 | clock: fn -> due end})
    assert is_nil(state.default_timer)
    send(disabled, {:default_sweep, state.default_timer_generation})
    engine = :sys.get_state(disabled).store.engine
    assert engine.shared_answers == %{}
    assert SharedAnswers.next_default(engine, true) == due
    assert :ok = GenServer.stop(disabled)
  end

  defp local_people(engine) do
    Enum.zip([@manager, @invitee, @other], ["a", "b", "c"])
    |> Enum.reduce(engine, fn {person, letter}, next ->
      {:ok, next} = Engine.operator_bootstrap(next, person, String.duplicate(letter, 64))
      {:ok, next} = Engine.set_local_organization_member(next, person, true)
      next
    end)
  end

  defp entry(id, time), do: %{id: id, at_ms: time, text: id, speaker: %{"kind" => "agent"}}

  defp attrs(id) do
    %{id: id, group_id: @group, sender_person: @manager, sender_key: "sender-key",
      sender_id: "app:test", message: %{"title" => "Planning"}, answer_policy: :first}
  end
end
