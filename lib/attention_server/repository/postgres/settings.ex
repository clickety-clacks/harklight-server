defmodule AttentionServer.Repository.Postgres.Settings do
  @moduledoc false

  alias AttentionServer.{BounceDecision, BrowserAuth}
  alias AttentionServer.ClientAPI.Resources
  alias AttentionServer.Repository.Postgres.{Attributes, Identities, NativeSessions, Transaction}

  @settings_sql """
  SELECT person.settings, person.delivery_state, stream.head, stream.seed_revision,
         resource.revision, identity.cursor_epoch,
         EXISTS (
           SELECT 1 FROM harklight.allocation_highs AS allocation
           WHERE allocation.person_id = scope.person_id
             AND allocation.origin_key = '' AND allocation.session_handle = ''
             AND allocation.kind IN ('accepted', 'conversation') AND allocation.high > 0
         )
  FROM scope
  JOIN harklight.people AS person ON person.id = scope.person_id
  CROSS JOIN harklight.server_identity AS identity
  LEFT JOIN harklight.stream_heads AS stream
    ON stream.person_id = scope.person_id AND stream.origin_key = ''
  LEFT JOIN harklight.stream_resource_revisions AS resource
    ON resource.person_id = scope.person_id AND resource.origin_key = ''
    AND resource.resource_kind = 'settings' AND resource.resource_id = 'settings'
  """

  def browser(cookie) do
    params = [BrowserAuth.hash(cookie), System.system_time(:millisecond)]

    with {:ok, row} <- settings(Identities.browser_scope_sql(), params) do
      project(row, %{kind: :browser, key: nil}, %{})
    end
  end

  def native(bearer) do
    Transaction.snapshot(fn ->
      with {:ok, %{person_id: person, device_id: device}} <- NativeSessions.resolve(bearer),
           {:ok, row} <- settings("SELECT $1::text AS person_id", [person]),
           {:ok, push} <- push_binding(person, device) do
        project(row, %{kind: :device, key: device}, push)
      else
        {:error, :storage_unavailable} -> {:error, :retry_later}
        {:error, _} = error -> error
      end
    end)
  end

  defp settings(scope, params) do
    case Transaction.query("WITH scope AS (#{scope})\n" <> @settings_sql, params) do
      {:ok, %{rows: [[_attrs, "STOPPED" | _]]}} -> {:error, :account_paused}
      {:ok, %{rows: [row]}} -> {:ok, row}
      {:ok, %{rows: []}} -> {:error, :unauthorized}
      {:error, _} -> {:error, :retry_later}
    end
  end

  defp push_binding(person, device) do
    case Transaction.query(
           "SELECT body FROM harklight.compatibility_facts WHERE family = 'push_bindings' AND address = $1",
           [Attributes.key({:row, :push_bindings, {person, device}})]
         ) do
      {:ok, %{rows: [[attrs]]}} ->
        with {:ok, binding} <- Attributes.decode(attrs) do
          {:ok, %{device => Resources.push_status(binding)}}
        else
          _ -> {:error, :storage_unavailable}
        end

      {:ok, %{rows: []}} ->
        {:ok, %{}}

      {:error, _} ->
        {:error, :storage_unavailable}
    end
  end

  defp project([attrs, _state, head, seed, revision, epoch, history?], principal, push) do
    with {:ok, settings} when is_map(settings) <- decode_settings(attrs) do
      # FORMAT32 can have an implicit epoch before its first logged change.
      seeded? = history? or Map.has_key?(settings, :bounce_levels) or Map.has_key?(settings, :alert_choices)
      seed = seed || if(seeded?, do: epoch, else: 0)
      revision = revision || max(seed, 1)
      levels = BounceDecision.levels_from_configuration(Map.get(settings, :bounce_levels))
      alerts = Map.get(settings, :alert_choices, "important")

      payload =
        Resources.settings_payload(levels, alerts, push)
        |> Resources.stamp(revision)
        |> then(&Resources.render(:settings, &1, principal))

      {:ok, %{"rev" => head || seed, "settings" => payload}}
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  defp decode_settings(attrs) when attrs == %{}, do: {:ok, attrs}
  defp decode_settings(attrs), do: Attributes.decode(attrs)
end
