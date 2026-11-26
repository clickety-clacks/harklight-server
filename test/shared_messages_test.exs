defmodule AttentionServer.SharedMessagesTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Engine, SharedMessages, Store}

  @moduletag :tmp_dir

  @sender "10000000-0000-4000-8000-000000000001"
  @other "10000000-0000-4000-8000-000000000002"
  @third "10000000-0000-4000-8000-000000000003"
  @organization "20000000-0000-4000-8000-000000000001"
  @second_organization "20000000-0000-4000-8000-000000000002"

  test "one durable message record references distinct personal rows", %{tmp_dir: dir} do
    path = Path.join(dir, "relay.sqlite3")
    assert {:ok, store} = Store.open_relay(path)

    {engine, :ok, touched} =
      Engine.with_touches(store.engine, fn initial ->
        engine = people(initial)
        assert {:ok, engine} =
                 Engine.replace_hosted_organization(engine, "org", [@sender, @other, @third], @organization)

        assert {:ok, engine, %{organization_id: @organization}, [{@other, :reached}, {@third, :reached}]} =
                 SharedMessages.create(engine, attrs(:first), [@other, @third, @other], nil)

        {engine, :ok}
      end)

    assert {:ok, store} =
             Store.commit(store, {engine, touched}, %{person: @sender, actor: "fixture", action: "send"})

    assert :ok = Store.close(store)
    assert {:ok, restored} = Store.open_relay(path)
    assert [{"shared-a", %{message: %{"title" => "Launch"}}}] =
             Map.to_list(restored.engine.shared_messages)

    assert %{{"shared-a", @other} => first, {"shared-a", @third} => second} =
             restored.engine.shared_recipients

    assert first.item_id != second.item_id
    assert Map.keys(first) |> Enum.sort() == [:item_id, :seen_revision]
    assert Map.keys(second) |> Enum.sort() == [:item_id, :seen_revision]
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
