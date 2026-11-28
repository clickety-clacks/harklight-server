defmodule AttentionServer.Repository.Postgres.SharedAccess do
  @moduledoc false

  alias AttentionServer.Repository.Postgres.{Attributes, Transaction}

  def check(_person, %{shared?: false}, _flags), do: :ok

  def check(person, %{groups?: groups?}, %{shared_messages: true, groups: groups_enabled?})
      when not groups? or groups_enabled? do
    with {:ok, %{rows: [[body]]}} <-
           Transaction.query(
             "SELECT body FROM harklight.compatibility_facts WHERE family = 'shared_read_access' AND address = $1",
             [Attributes.key(person)]
           ),
         {:ok, readable?} when is_boolean(readable?) <- Attributes.decode(body) do
      if readable?, do: :ok, else: {:error, :unsupported}
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  def check(_person, _capability, _flags), do: {:error, :unsupported}
end
