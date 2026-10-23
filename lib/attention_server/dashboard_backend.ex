defmodule AttentionServer.DashboardBackend do
  @moduledoc """
  Trusted read adapter for the self-hosted dashboard.

  Browser input contributes only an opaque cookie. The session process supplies
  the current person-owner principal; Relay serializes a fresh authority check
  with the metadata projection. The account owner designation comes from
  operator configuration, never from a browser request.
  """

  alias AttentionServer.{DashboardBrowserSession, GuidedJoin, Relay}

  defstruct [:relay, :sessions, :configured_owner_person, :origin]

  def resolve_session(%__MODULE__{sessions: sessions}, cookie),
    do: DashboardBrowserSession.resolve(sessions, cookie)

  def redeem_sign_in(%__MODULE__{sessions: sessions}, token),
    do: DashboardBrowserSession.redeem(sessions, token)

  def end_session(%__MODULE__{sessions: sessions}, cookie),
    do: DashboardBrowserSession.end_session(sessions, cookie)

  def list_devices(%__MODULE__{} = backend, %{person_id: person, owner_key: key} = principal) do
    case Relay.dashboard_inventory(backend.relay, person, key, backend.configured_owner_person) do
      {:ok, snapshot} ->
        waiting = Enum.map(snapshot.waiting, &Map.put(&1, :can_review, &1.person_name == person))
        {:ok, present(%{snapshot | waiting: waiting}, person, Map.get(principal, :display_name))}

      error ->
        error
    end
  end

  def list_devices(_, _), do: {:error, :forbidden}

  # Browser actions use only the signed-in person's current owner authority.
  # Account-wide metadata visibility does not grant cross-person mutations.
  def create_invitation(
        %__MODULE__{relay: relay, origin: origin},
        {%{person_id: person, owner_key: key}, request_id}
      ) do
    if is_binary(origin) and String.starts_with?(origin, "https://") do
      case Relay.owner_invite_create(relay, person, key, request_id) do
        {:ok, %{"invite_token" => token} = invitation} ->
          {:ok, Map.put(invitation, "tappable_url", GuidedJoin.bridge_link(origin, token))}

        error ->
          error
      end
    else
      {:error, :bridge_unavailable}
    end
  end

  # A hosted session carries the readable label its account supplied; the
  # dashboard names the signed-in person by it instead of the person UUID.
  # Authority checks above use the UUID only.
  defp present(snapshot, _person, nil), do: snapshot

  defp present(snapshot, person, display_name) when is_binary(display_name) do
    relabel = fn
      %{person_name: ^person} = row -> %{row | person_name: display_name}
      row -> row
    end

    %{
      snapshot
      | person_name:
          if(snapshot.person_name == person, do: display_name, else: snapshot.person_name),
        devices: Enum.map(snapshot.devices, relabel),
        waiting: Enum.map(snapshot.waiting, relabel)
    }
  end

  def resolve_invitation(%__MODULE__{relay: relay}, token),
    do: Relay.invitation_resolve(relay, token)

  def review_pending(%__MODULE__{relay: relay}, {%{person_id: person, owner_key: key}, code}),
    do: Relay.owner_pending_lookup(relay, person, key, code)

  def approve_pending(
        %__MODULE__{relay: relay},
        {%{person_id: person, owner_key: key}, code, expected_key, deadline}
      ),
      do: Relay.owner_pending_approve(relay, person, key, code, expected_key, deadline, [])

  def decline_pending(
        %__MODULE__{relay: relay},
        {%{person_id: person, owner_key: key}, code, expected_key, deadline}
      ),
      do: Relay.owner_pending_decline(relay, person, key, code, expected_key, deadline)
end
