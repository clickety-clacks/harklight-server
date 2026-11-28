defmodule AttentionServer.Repository.Postgres.Settings do
  @moduledoc false

  alias AttentionServer.{BounceDecision, BrowserAuth}
  alias AttentionServer.ClientAPI.Resources
  alias AttentionServer.Repository.Postgres.{Attributes, Identities, NativeSessions, Transaction}

  @settings_sql """
    SELECT person.settings, person.delivery_state, stream.head, stream.seed_revision,
           resource.revision, identity.cursor_epoch,
           coalesce(delivery.high, 0), coalesce(conversation.high, 0)
    FROM scope
    JOIN harklight.people AS person ON person.id = scope.person_id
    CROSS JOIN harklight.server_identity AS identity
    LEFT JOIN harklight.stream_heads AS stream
      ON stream.person_id = scope.person_id AND stream.origin_key = ''
    LEFT JOIN harklight.stream_resource_revisions AS resource
      ON resource.person_id = scope.person_id AND resource.origin_key = ''
      AND resource.resource_kind = 'settings' AND resource.resource_id = 'settings'
    LEFT JOIN harklight.allocation_highs AS delivery
      ON delivery.person_id = scope.person_id AND delivery.origin_key = ''
      AND delivery.session_handle = '' AND delivery.kind = 'accepted'
    LEFT JOIN harklight.allocation_highs AS conversation
      ON conversation.person_id = scope.person_id AND conversation.origin_key = ''
      AND conversation.session_handle = '' AND conversation.kind = 'conversation'
  """

  @owner_scope """
  SELECT person_id FROM harklight.principals
  WHERE person_id = $1 AND key = $2 AND kind = 'owner' AND NOT revoked
  """

  def browser(cookie) do
    params = [BrowserAuth.hash(cookie), System.system_time(:millisecond)]

    with {:ok, view} <- read_view(Identities.browser_scope_sql(), params) do
      {:ok, render(view)}
    end
  end

  def native(bearer) do
    Transaction.snapshot(fn ->
      with {:ok, %{person_id: person, device_id: device}} <- NativeSessions.resolve(bearer),
           {:ok, row} <- for_person(person),
           {:ok, body} <- project(row, %{kind: :device, person: person, key: device}) do
        {:ok, body}
      else
        {:error, :storage_unavailable} -> {:error, :retry_later}
        {:error, _} = error -> error
      end
    end)
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

    lock_view(scope, [session, at, person, key, revision], false)
  end

  def lock_native(%{person: person} = principal, at) do
    with {:ok, %{pending: pending?}} <- NativeSessions.lock(principal, at),
         {:ok, view} <- lock_view(person_scope(), [person], false) do
      if pending?, do: {:error, :forbidden}, else: {:ok, view}
    end
  end

  def owner_view(person, key), do: read_view(@owner_scope, [person, key], true)

  def lock_owner(person, key) do
    lock_view(@owner_scope <> " FOR SHARE", [person, key], true)
  end

  defp lock_view(scope, params, allow_paused?) do
    case Transaction.query(
           """
           WITH scope AS MATERIALIZED (#{scope})
           SELECT person.id, person.delivery_state
           FROM scope JOIN harklight.people AS person ON person.id = scope.person_id
           FOR NO KEY UPDATE OF person
           """,
           params
         ) do
      {:ok, %{rows: [[_person, "STOPPED"]]}} when not allow_paused? -> {:error, :account_paused}
      {:ok, %{rows: [[person, _state]]}} -> read_view(person_scope(), [person], allow_paused?)
      {:ok, %{rows: []}} -> {:error, :unauthorized}
      {:error, _} = error -> error
    end
  end

  def for_person(person), do: read_view(person_scope(), [person])
  defp person_scope, do: "SELECT $1::text AS person_id"

  def write(_person, %{attributes: attrs}, attrs, _at), do: {:ok, nil}

  def write(person, view, attrs, at) do
    scope = {person, ""}
    change = %{kind: "settings", resource_id: "settings", session_handle: nil, created_at_ms: at, first_seen_at_ms: nil}

    with {:ok, head} <- Transaction.lock_stream(scope, view.seed),
         {:ok, %{num_rows: 1}} <-
           Transaction.query("UPDATE harklight.people SET settings = $2 WHERE id = $1", [person, Attributes.encode(attrs)]) do
      Transaction.append(%{scope => head}, scope, [change])
    else
      {:error, _} = error -> error
      _ -> {:error, :storage_unavailable}
    end
  end

  defp read_view(scope, params, allow_paused? \\ false) do
    case Transaction.query("WITH scope AS (#{scope})\n" <> @settings_sql, params) do
      {:ok, %{rows: [[_attrs, "STOPPED" | _]]}} when not allow_paused? -> {:error, :account_paused}
      {:ok, %{rows: [[attrs, _, head, seed, revision, epoch, accepted, conversation]]}} ->
        view(attrs, head, seed, revision, epoch, accepted, conversation)

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

  defp view(attrs, head, seed, revision, epoch, accepted, conversation) do
    with {:ok, settings} when is_map(settings) <- decode_settings(attrs) do
      # FORMAT32 can have an implicit epoch before its first logged change.
      seeded? = accepted > 0 or conversation > 0 or Map.has_key?(settings, :bounce_levels) or Map.has_key?(settings, :alert_choices)
      seed = seed || if(seeded?, do: epoch, else: 0)
      {:ok,
       %{
         head: head || seed, seed: seed, revision: revision || max(seed, 1), attributes: settings,
         accepted_high: accepted, conversation_high: conversation
       }}
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  def render(view), do: render(view, %{kind: :browser, key: nil}, %{})

  def project(view, %{kind: :device, person: person, key: device} = principal) do
    with {:ok, push} <- push_binding(person, device) do
      {:ok, render(view, principal, push)}
    end
  end

  def project(view, %{kind: :browser}), do: {:ok, render(view)}

  defp render(%{head: head, revision: revision, attributes: settings}, principal, push) do
    levels = BounceDecision.levels_from_configuration(Map.get(settings, :bounce_levels))
    alerts = Map.get(settings, :alert_choices, "important")

    payload =
      Resources.settings_payload(levels, alerts, push)
      |> Resources.stamp(revision)
      |> then(&Resources.render(:settings, &1, principal))

    %{"rev" => head, "settings" => payload}
  end

  defp decode_settings(attrs) when attrs == %{}, do: {:ok, attrs}
  defp decode_settings(attrs), do: Attributes.decode(attrs)
end
