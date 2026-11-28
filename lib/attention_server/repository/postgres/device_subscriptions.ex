defmodule AttentionServer.Repository.Postgres.DeviceSubscriptions do
  @moduledoc false

  alias AttentionServer.Repository.Postgres.{Attributes, Transaction}

  # A device's exact inbound subscriptions determine message versus knock.
  # Sender grants are separate outbound/certificate authority. A published
  # subscription revision makes a missing membership a known nonmembership.
  def contains?(person, device, sender) do
    with {:ok, %{rows: [[encoded]]}} <-
           Transaction.query(
             "SELECT body FROM harklight.compatibility_facts WHERE family = 'device_subscription_heads' AND address = $1",
             [Attributes.key({person, device})]
           ),
         {:ok, revision} when is_integer(revision) and revision >= 0 <- Attributes.decode(encoded),
         {:ok, %{rows: [[subscribed?]]}} <-
           Transaction.query(
             """
             SELECT EXISTS (SELECT 1 FROM harklight.compatibility_facts
               WHERE family = 'device_subscription_senders' AND address = $1)
             """,
             [Attributes.key({person, device, revision, sender})]
           ) do
      {:ok, subscribed?}
    else
      _ -> {:error, :storage_unavailable}
    end
  end
end
