defmodule AttentionServer.Repository.Postgres.SettingsFeed do
  @moduledoc false

  alias AttentionServer.ClientAPI.Resources
  alias AttentionServer.Repository.Postgres.{Attributes, Settings, Streams, Transaction}

  @max_cursor 9_223_372_036_854_775_807

  def read(person, cursor, limit) do
    with :ok <- ranks_available(person),
         true <- cursor <= @max_cursor,
         {:ok, rows, more, position} <- Streams.person_page(person, cursor, limit),
         {:ok, changes} <- render(person, rows) do
      {:ok, {changes, more, position}}
    else
      false -> {:error, :cursor_expired}
      {:error, _} = error -> error
    end
  end

  defp ranks_available(person) do
    case Transaction.query(
           "SELECT 1 FROM harklight.compatibility_facts WHERE family = 'client_rank_state' AND address = $1",
           [Attributes.key(person)]
         ) do
      {:ok, %{rows: [[1]]}} -> :ok
      _ -> {:error, :storage_unavailable}
    end
  end

  defp render(_person, []), do: {:ok, []}

  defp render(person, rows) do
    if Enum.all?(rows, &match?(%{kind: "settings", resource_id: "settings"}, &1)) do
      with {:ok, view} <- Settings.for_person(person) do
        %{"settings" => settings} = Settings.render(view)

        changes = Enum.map(rows, fn %{rev: rev} ->
          %{"rev" => rev, "kind" => "settings", "data" => Resources.stamp(settings, rev)}
        end)

        {:ok, changes}
      end
    else
      {:error, :storage_unavailable}
    end
  end
end
