defmodule AttentionServer.Migration.SubscriptionTest do
  use ExUnit.Case, async: true
  alias AttentionServer.Migration.ImportSubscriptions

  test "exact subscription revision and membership, including known-empty active devices" do
    assert {:ok, {0, []}} = ImportSubscriptions.projection(nil, %{revoked: false})
    assert {:ok, :absent} = ImportSubscriptions.projection(nil, %{revoked: true})
    source = %{revision: 7, senders: MapSet.new(["app:b", "app:a"])}
    assert {:ok, {7, ["app:a", "app:b"]}} = ImportSubscriptions.projection(source, %{revoked: false})
    assert {:ok, {7, ["app:a", "app:b"]}} = ImportSubscriptions.projection(source, %{revoked: true})
    assert {:error, :invalid_device_subscription} = ImportSubscriptions.projection(%{source | revision: -1}, %{})
    assert {:error, :invalid_device_subscription} = ImportSubscriptions.projection(%{source | senders: ["app:a"]}, %{})
    assert {:error, :invalid_device_subscription} = ImportSubscriptions.projection(%{source | senders: MapSet.new([nil])}, %{})
  end
end
