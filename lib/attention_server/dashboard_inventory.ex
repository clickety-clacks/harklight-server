defmodule AttentionServer.DashboardInventory do
  @moduledoc """
  Read-only device facts for an authenticated person or explicit self-host owner.

  A current owner key or enrolled device key selects its own person. This does
  not grant account-wide visibility. The self-hosted server can designate one
  owner person in trusted operator configuration; only that person's current
  owner key can select the account-wide metadata view. Hosted adapters should
  use the person-scoped functions. Neither view infers presence from enrollment.
  """

  alias AttentionServer.Engine

  def for_owner(%Engine{} = engine, person, owner_key, now_ms \\ System.system_time(:millisecond)) do
    case Engine.owner_revision(engine, person, owner_key) do
      {:ok, _revision} -> {:ok, snapshot(engine, {:person, person}, person, now_ms)}
      _ -> {:error, :forbidden}
    end
  end

  def for_device(%Engine{} = engine, device_key, now_ms \\ System.system_time(:millisecond)) do
    case Map.get(engine.devices, device_key) do
      %{person: person, revoked: false} ->
        {:ok, snapshot(engine, {:person, person}, person, now_ms)}

      _ ->
        {:error, :forbidden}
    end
  end

  def for_account_owner(
        %Engine{} = engine,
        configured_owner_person,
        owner_key,
        now_ms \\ System.system_time(:millisecond)
      ) do
    case Engine.owner_revision(engine, configured_owner_person, owner_key) do
      {:ok, _revision} when is_binary(configured_owner_person) ->
        {:ok, snapshot(engine, :all, "Server owner", now_ms)}

      _ ->
        {:error, :forbidden}
    end
  end

  defp snapshot(engine, scope, display_name, now_ms) do
    effective_now = max(now_ms, engine.pending_clock_high_water)

    devices =
      engine.devices
      |> Enum.filter(fn {_key, device} -> visible_person?(scope, device.person) end)
      |> Enum.map(fn {key, device} ->
        %{
          device_id: key,
          label: device_label(device, key),
          person_name: device.person,
          revoked: device.revoked,
          connection: :unknown
        }
      end)
      |> Enum.sort_by(& &1.device_id)

    waiting =
      engine.pending_enrollments
      |> Enum.filter(fn {_code, item} ->
        visible_person?(scope, item.person) and item.state == :pending and
          item.expires_at > effective_now
      end)
      |> Enum.map(fn {code, item} ->
        %{code: code, label: item.label, person_name: item.person, device_id: item.key}
      end)
      |> Enum.sort_by(& &1.code)

    %{person_name: display_name, devices: devices, waiting: waiting}
  end

  defp visible_person?({:person, person}, candidate), do: candidate == person
  defp visible_person?(:all, candidate), do: is_binary(candidate)

  defp device_label(%{label: label}, _key) when is_binary(label) and label != "", do: label
  defp device_label(_device, key), do: "Device " <> String.slice(key, 0, 8)
end
