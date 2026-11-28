defmodule AttentionServer.Repository.CommittedRead do
  @moduledoc """
  Queries that run entirely against a published Engine snapshot.

  Each query carries the person or key needed for authorization and selection.
  Both the ordinary Relay path and the persistence wait use the same query
  implementation. While a transaction is pending, a query can read the last
  published snapshot. Queries that need SQLite, runtime state, or side effects
  wait for the Relay to finish the transaction.
  """

  alias AttentionServer.{BounceDecision, DashboardInventory, Engine}

  defguard supported?(request)
           when is_tuple(request) and
                  ((tuple_size(request) == 2 and
                      elem(request, 0) in [:cursor, :owner_identity, :agent_identity]) or
                     (tuple_size(request) == 3 and
                        elem(request, 0) in [
                          :owner_revision,
                          :owner_list_agents,
                          :dashboard_levels
                        ]) or
                     (tuple_size(request) == 4 and elem(request, 0) == :dashboard_inventory))

  def execute(engine, {:cursor, key}), do: Engine.acknowledged_cursor(engine, key)
  def execute(engine, {:owner_identity, key}), do: Engine.owner_identity(engine, key)
  def execute(engine, {:agent_identity, key}), do: Engine.agent_identity(engine, key)

  def execute(engine, {:owner_revision, person, key}),
    do: Engine.owner_revision(engine, person, key)

  def execute(engine, {:owner_list_agents, person, key}),
    do: Engine.owner_list_agents(engine, person, key)

  def execute(engine, {:dashboard_inventory, person, key, person}) when is_binary(person),
    do: DashboardInventory.for_account_owner(engine, person, key)

  def execute(engine, {:dashboard_inventory, person, key, _configured_owner}),
    do: DashboardInventory.for_owner(engine, person, key)

  def execute(engine, {:dashboard_levels, person, key}) do
    case Engine.owner_identity(engine, key) do
      {:ok, ^person, _revision} -> {:ok, %{"levels" => BounceDecision.levels(engine, person)}}
      _ -> {:error, :forbidden}
    end
  end
end
