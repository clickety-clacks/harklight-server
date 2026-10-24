defmodule AttentionServer.DashboardInventoryTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{DashboardInventory, Engine}

  @one "00000000-0000-4000-8000-000000000001"
  @two "00000000-0000-4000-8000-000000000002"
  @owner_one String.duplicate("a", 64)
  @owner_two String.duplicate("b", 64)
  @device_one String.duplicate("c", 64)
  @device_two String.duplicate("d", 64)

  test "current owner and enrolled device see only their person's devices, with unknown presence" do
    engine = seeded_engine()

    assert {:ok, own} = DashboardInventory.for_owner(engine, @one, @owner_one)
    assert Enum.map(own.devices, & &1.device_id) == [@device_one]
    assert hd(own.devices).connection == :unknown
    refute inspect(own) =~ @device_two

    assert {:ok, ^own} = DashboardInventory.for_device(engine, @device_one)
    assert {:error, :forbidden} = DashboardInventory.for_owner(engine, @one, @owner_two)
    assert {:error, :forbidden} = DashboardInventory.for_device(engine, @owner_one)
  end

  test "revoked key cannot read while its device remains visibly revoked to its owner" do
    engine = seeded_engine()
    revoked = put_in(engine.devices[@device_one].revoked, true)

    assert {:error, :forbidden} = DashboardInventory.for_device(revoked, @device_one)
    assert {:ok, own} = DashboardInventory.for_owner(revoked, @one, @owner_one)
    assert hd(own.devices).revoked
    assert hd(own.devices).connection == :unknown
  end

  test "approved label comes from the durable device record after receipt cleanup" do
    engine = seeded_engine()
    labeled = put_in(engine.devices[@device_one].label, "Kitchen iPad")

    assert {:ok, own} = DashboardInventory.for_owner(labeled, @one, @owner_one)
    assert hd(own.devices).label == "Kitchen iPad"

    assert {:ok, legacy} = DashboardInventory.for_owner(engine, @one, @owner_one)
    assert hd(legacy.devices).label == "Device " <> String.slice(@device_one, 0, 8)
  end

  test "person owners see only their pending requests while the account owner also sees CLI requests" do
    engine = seeded_engine()

    pending = %{
      key: String.duplicate("e", 64),
      label: "New iPad",
      state: :pending,
      person: @one,
      expires_at: 10
    }

    cli_pending = %{
      key: String.duplicate("f", 64),
      label: "CLI workstation",
      state: :pending,
      person: nil,
      expires_at: 10
    }

    engine = %{
      engine
      | pending_enrollments: %{"ABCDEFGH23" => pending, "KLMNPQRS34" => cli_pending}
    }

    assert {:ok, own} = DashboardInventory.for_owner(engine, @one, @owner_one, 9)
    assert [%{code: "ABCDEFGH23", label: "New iPad"}] = own.waiting

    assert {:ok, other} = DashboardInventory.for_owner(engine, @two, @owner_two, 9)
    assert other.waiting == []

    assert {:ok, account_owner} =
             DashboardInventory.for_account_owner(engine, @one, @owner_one, 9)

    assert Enum.map(account_owner.waiting, & &1.code) == ["ABCDEFGH23", "KLMNPQRS34"]
    assert Enum.find(account_owner.waiting, &(&1.code == "KLMNPQRS34")).person_name == nil

    assert {:ok, expired} = DashboardInventory.for_owner(engine, @one, @owner_one, 10)
    assert expired.waiting == []

    rolled_back = %{engine | pending_clock_high_water: 10}
    assert {:ok, fenced} = DashboardInventory.for_owner(rolled_back, @one, @owner_one, 9)
    assert fenced.waiting == []
  end

  test "explicit self-host owner can view all person metadata without granting peers the same scope" do
    engine = seeded_engine()
    assert {:ok, full} = DashboardInventory.for_account_owner(engine, @one, @owner_one)
    assert Enum.map(full.devices, & &1.device_id) == [@device_one, @device_two]
    assert Enum.map(full.devices, & &1.person_name) == [@one, @two]
    assert Enum.all?(full.devices, &(&1.connection == :unknown))

    assert {:error, :forbidden} = DashboardInventory.for_account_owner(engine, @one, @owner_two)
    assert {:ok, person_only} = DashboardInventory.for_owner(engine, @two, @owner_two)
    assert Enum.map(person_only.devices, & &1.device_id) == [@device_two]
  end

  defp seeded_engine do
    {:ok, engine} = Engine.operator_bootstrap(Engine.new(), @one, @owner_one)
    {:ok, engine} = Engine.operator_bootstrap(engine, @two, @owner_two)
    {:ok, engine} = Engine.setup_device(engine, @one, @device_one)
    {:ok, engine} = Engine.setup_device(engine, @two, @device_two)
    engine
  end
end
