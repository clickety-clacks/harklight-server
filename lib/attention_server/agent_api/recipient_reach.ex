defmodule AttentionServer.AgentAPI.RecipientReach do
  @moduledoc false

  alias AttentionServer.Engine
  alias AttentionServer.Store.RecordTable

  def add(message, %{store: %{auth_schema: %{} = schema, db: db, engine: engine}} = state,
        {person, origin}) do
    overlap? = state.deployment_mode == :self_hosted and not schema.device_certificates_disabled

    case reach(db, engine, person, origin, overlap?) do
      nil -> message
      value -> Map.put(message, "recipient_reach", value)
    end
  end

  def add(message, _state, _scope), do: message

  defp reach(db, engine, person, origin, overlap?) do
    with {:ok, devices} <- devices(db, person, overlap?) do
      if Enum.any?(devices, fn [device] ->
           device != origin and match?(%{person: ^person, revoked: false}, engine.devices[device])
         end) do
        "reachable"
      else
        browser_reach(db, engine, person)
      end
    else
      {:error, _} -> nil
    end
  end

  defp devices(db, person, overlap?) do
    RecordTable.select(
      db,
      """
      SELECT device_id FROM harklight_device_sessions WHERE person_id = ?
      UNION
      SELECT device_id FROM harklight_migration_roster
        WHERE ? = 1 AND person_id = ? AND phase IN ('legacy', 'pending')
      """,
      [person, if(overlap?, do: 1, else: 0), person]
    )
  end

  defp browser_reach(db, engine, person) do
    case RecordTable.select(
           db,
           """
           SELECT DISTINCT owner_key, owner_revision FROM harklight_browser_sessions
           WHERE person_id = ?
           """,
           [person]
         ) do
      {:ok, sessions} ->
        if Enum.any?(sessions, fn [key, revision] ->
             Engine.owner_revision(engine, person, key) == {:ok, revision}
           end),
          do: "reachable",
          else: "none"

      {:error, _} ->
        nil
    end
  end
end
