defmodule AttentionServer.Migration.ImportSubscriptions do
  @moduledoc false
  alias AttentionServer.JSON
  alias AttentionServer.Migration.{Bundle, ImportAccounts, ImportRows, ImportSession}
  alias AttentionServer.Repository.Postgres.Attributes

  def run(db, identity, bundle) do
    with :ok <- ImportAccounts.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, _} <- ImportRows.run(db, identity, "devices/subscriptions/v1",
           Path.join(bundle, "records.frames"), manifest.data.records,
           &match?({{:row, :devices, _}, _}, &1), &write/2) do
      :ok
    end
  end

  # A missing active-device subscription has the same known-empty revision as
  # Engine's source fallback. Revoked principals receive no invented head.
  def projection(nil, %{revoked: false}), do: {:ok, {0, []}}
  def projection(nil, %{revoked: true}), do: {:ok, :absent}
  def projection(%{revision: revision, senders: %MapSet{} = senders}, _)
      when is_integer(revision) and revision >= 0 do
    if Enum.all?(senders, &(is_binary(&1) and byte_size(&1) > 0)),
      do: {:ok, {revision, Enum.sort(senders)}},
      else: {:error, :invalid_device_subscription}
  end
  def projection(_, _), do: {:error, :invalid_device_subscription}

  defp write(db, rows) do
    Enum.reduce_while(rows, :ok, fn {{:row, :devices, device}, %{person: person} = principal}, :ok ->
      result = with {:ok, source} <- source(db, person, device),
                    {:ok, projection} <- projection(source, principal),
                    :ok <- publish(db, person, device, projection) do
        :ok
      end
      case result do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp source(db, person, device) do
    case ImportSession.query(db,
      "SELECT body FROM harklight.compatibility_facts WHERE family='subscriptions' AND address=$1",
      [Attributes.key({:row, :subscriptions, {person, device}})]) do
      {:ok, %{rows: []}} -> {:ok, nil}
      {:ok, %{rows: [[body]]}} -> Attributes.decode(body)
      {:error, _} = error -> error
      _ -> {:error, :invalid_device_subscription}
    end
  end

  defp publish(_, _, _, :absent), do: :ok
  defp publish(db, person, device, {revision, senders}) do
    # ImportRows keeps every membership and its head in the same transaction
    # as the device checkpoint. Sender grants are a different authority.
    with :ok <- Enum.reduce_while(senders, :ok, fn sender, :ok ->
           case fact(db, "device_subscription_senders", {person, device, revision, sender}, true) do
             :ok -> {:cont, :ok}
             error -> {:halt, error}
           end
         end) do
      fact(db, "device_subscription_heads", {person, device}, revision)
    end
  end

  defp fact(db, family, key, value) do
    case ImportSession.query(db,
      "INSERT INTO harklight.compatibility_facts(family,address,body) VALUES($1,$2,$3::text::jsonb)",
      [family, Attributes.key(key), JSON.encode!(Attributes.encode(value))]) do
      {:ok, %{num_rows: 1}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, :device_subscription_mismatch}
    end
  end
end
