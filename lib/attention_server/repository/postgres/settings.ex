defmodule AttentionServer.Repository.Postgres.Settings do
  @moduledoc false

  alias AttentionServer.{BounceDecision, BrowserAuth}
  alias AttentionServer.ClientAPI.Resources
  alias AttentionServer.Repository.Postgres.{Attributes, Identities, Transaction}

  def browser(cookie) do
    sql = """
    WITH browser AS (#{Identities.browser_scope_sql()})
    SELECT person.settings, person.delivery_state, stream.head, stream.seed_revision,
           resource.revision, identity.cursor_epoch,
           EXISTS (
             SELECT 1 FROM harklight.allocation_highs AS allocation
             WHERE allocation.person_id = browser.person_id
               AND allocation.origin_key = '' AND allocation.session_handle = ''
               AND allocation.kind IN ('accepted', 'conversation') AND allocation.high > 0
           )
    FROM browser
    JOIN harklight.people AS person ON person.id = browser.person_id
    CROSS JOIN harklight.server_identity AS identity
    LEFT JOIN harklight.stream_heads AS stream
      ON stream.person_id = browser.person_id AND stream.origin_key = ''
    LEFT JOIN harklight.stream_resource_revisions AS resource
      ON resource.person_id = browser.person_id AND resource.origin_key = ''
      AND resource.resource_kind = 'settings' AND resource.resource_id = 'settings'
    """

    case Transaction.query(sql, [BrowserAuth.hash(cookie), System.system_time(:millisecond)]) do
      {:ok, %{rows: [[_attrs, "STOPPED" | _]]}} -> {:error, :account_paused}
      {:ok, %{rows: [[attrs, _, head, seed, revision, epoch, history?]]}} ->
        project(attrs, head, seed, revision, epoch, history?)

      {:ok, %{rows: []}} -> {:error, :unauthorized}
      {:error, _} -> {:error, :retry_later}
    end
  end

  defp project(attrs, head, seed, revision, epoch, history?) do
    with {:ok, settings} when is_map(settings) <- decode_settings(attrs) do
      # FORMAT32 can have an implicit epoch before its first logged change.
      seeded? = history? or Map.has_key?(settings, :bounce_levels) or Map.has_key?(settings, :alert_choices)
      seed = seed || if(seeded?, do: epoch, else: 0)
      revision = revision || max(seed, 1)
      levels = BounceDecision.levels_from_configuration(Map.get(settings, :bounce_levels))
      alerts = Map.get(settings, :alert_choices, "important")

      payload =
        Resources.settings_payload(levels, alerts, %{})
        |> Resources.stamp(revision)
        |> then(&Resources.render(:settings, &1, %{kind: :browser, key: nil}))

      {:ok, %{"rev" => head || seed, "settings" => payload}}
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  defp decode_settings(attrs) when attrs == %{}, do: {:ok, attrs}
  defp decode_settings(attrs), do: Attributes.decode(attrs)
end
