defmodule AttentionServer.GroupsTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Engine, Groups, SharedMessages, Store}

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
