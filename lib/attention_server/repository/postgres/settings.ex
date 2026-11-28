defmodule AttentionServer.Repository.Postgres.Settings do
  @moduledoc false

  alias AttentionServer.{BounceDecision, BrowserAuth}
  alias AttentionServer.ClientAPI.Resources
  alias AttentionServer.Repository.Postgres.{Attributes, Identities, Transaction}

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

    with {:ok, view} <- read_view(Identities.browser_scope_sql(), params) do
      {:ok, render(view)}
    end
  end

  def check_active(person) do
    case Transaction.query("SELECT delivery_state FROM harklight.people WHERE id = $1", [person]) do
      {:ok, %{rows: [["STOPPED"]]}} -> {:error, :account_paused}
      {:ok, %{rows: [[_state]]}} -> :ok
      {:ok, %{rows: []}} -> {:error, :unauthorized}
      {:error, _} -> {:error, :retry_later}
    end
  end

  # Recheck the cookie, owner revision and account inside the write
  # transaction. Hold authority and the Settings row until the receipt commits.
  def lock_browser(%{session_id: session, person: person, key: key, owner_revision: revision}, at) do
    scope = Identities.browser_scope_sql() <> """
      AND session.person_id = $3 AND session.principal_key = $4
      AND session.principal_revision = $5
    FOR SHARE OF session, owner
    """

    case Transaction.query(
           """
           WITH scope AS MATERIALIZED (#{scope})
           SELECT person.delivery_state
           FROM scope JOIN harklight.people AS person ON person.id = scope.person_id
           FOR NO KEY UPDATE OF person
           """,
           [session, at, person, key, revision]
         ) do
      {:ok, %{rows: [["STOPPED"]]}} -> {:error, :account_paused}
      {:ok, %{rows: [[_state]]}} -> for_person(person)
      {:ok, %{rows: []}} -> {:error, :unauthorized}
      {:error, _} = error -> error
    end
  end

  def for_person(person), do: read_view("SELECT $1::text AS person_id", [person])

  defp read_view(scope, params) do
    case Transaction.query("WITH scope AS (#{scope})\n" <> @settings_sql, params) do
      {:ok, %{rows: [[_attrs, "STOPPED" | _]]}} -> {:error, :account_paused}
      {:ok, %{rows: [[attrs, _, head, seed, revision, epoch, history?]]}} ->
        view(attrs, head, seed, revision, epoch, history?)

      {:ok, %{rows: []}} -> {:error, :unauthorized}
      {:error, _} -> {:error, :retry_later}
    end
  end

  defp view(attrs, head, seed, revision, epoch, history?) do
    with {:ok, settings} when is_map(settings) <- decode_settings(attrs) do
      # FORMAT32 can have an implicit epoch before its first logged change.
      seeded? = history? or Map.has_key?(settings, :bounce_levels) or Map.has_key?(settings, :alert_choices)
      seed = seed || if(seeded?, do: epoch, else: 0)
      {:ok, %{head: head || seed, seed: seed, revision: revision || max(seed, 1), attributes: settings}}
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  def render(%{head: head, revision: revision, attributes: settings}) do
    levels = BounceDecision.levels_from_configuration(Map.get(settings, :bounce_levels))
    alerts = Map.get(settings, :alert_choices, "important")

    payload =
      Resources.settings_payload(levels, alerts, %{})
      |> Resources.stamp(revision)
      |> then(&Resources.render(:settings, &1, %{kind: :browser, key: nil}))

    %{"rev" => head, "settings" => payload}
  end

  defp decode_settings(attrs) when attrs == %{}, do: {:ok, attrs}
  defp decode_settings(attrs), do: Attributes.decode(attrs)
end
