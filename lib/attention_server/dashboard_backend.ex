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

  def open_browser_connection(%__MODULE__{sessions: sessions}, {cookie, recipient, worker}),
    do:
      apply(DashboardBrowserSession, :open_browser_connection, [
        sessions,
        cookie,
        recipient,
        worker
      ])

  def browser_session_request_line(
        %__MODULE__{sessions: sessions},
        {cookie, browser_session_id, session_id, raw}
      ),
      do:
        apply(DashboardBrowserSession, :browser_session_request_line, [
          sessions,
          cookie,
          browser_session_id,
          session_id,
          raw
        ])

  def browser_session_drain(
        %__MODULE__{sessions: sessions},
        {cookie, browser_session_id, session_id}
      ),
      do:
        apply(DashboardBrowserSession, :browser_session_drain, [
          sessions,
          cookie,
          browser_session_id,
          session_id
        ])

  def close_browser_connection(%__MODULE__{sessions: sessions}, {cookie, session_id}),
    do: apply(DashboardBrowserSession, :close_browser_connection, [sessions, cookie, session_id])

  def browser_file_command(%__MODULE__{sessions: sessions}, {cookie, recipient, request}),
    do:
      apply(DashboardBrowserSession, :browser_file_command, [sessions, cookie, recipient, request])

  def browser_file_limits(%__MODULE__{sessions: sessions}, {cookie, recipient}),
    do: apply(DashboardBrowserSession, :browser_file_limits, [sessions, cookie, recipient])

  def browser_upload_begin(
        %__MODULE__{sessions: sessions},
        {cookie, recipient, attrs, size, worker}
      ),
      do:
        apply(DashboardBrowserSession, :browser_upload_begin, [
          sessions,
          cookie,
          recipient,
          attrs,
          size,
          worker
        ])

  def browser_upload_file(
        %__MODULE__{sessions: sessions},
        {cookie, recipient, attrs, path, size, sha256, lease}
      ),
      do:
        apply(DashboardBrowserSession, :browser_upload_file, [
          sessions,
          cookie,
          recipient,
          attrs,
          path,
          size,
          sha256,
          lease
        ])

  def browser_upload_end(%__MODULE__{sessions: sessions}, {cookie, recipient, lease}),
    do:
      apply(DashboardBrowserSession, :browser_upload_end, [
        sessions,
        cookie,
        recipient,
        lease
      ])

  def browser_fetch_file(%__MODULE__{sessions: sessions}, {cookie, upload_id}),
    do: apply(DashboardBrowserSession, :browser_fetch_file, [sessions, cookie, upload_id])

  def list_devices(%__MODULE__{} = backend, %{person_id: person, owner_key: key} = principal) do
    case Relay.dashboard_inventory(backend.relay, person, key, backend.configured_owner_person) do
      {:ok, snapshot} ->
        devices =
          Enum.map(snapshot.devices, fn device ->
            Map.put(
              device,
              :can_revoke,
              device.person_name == person and not device.revoked
            )
            |> Map.put(
              :can_manage_focus,
              device.person_name == person and not device.revoked
            )
          end)

        waiting =
          Enum.map(snapshot.waiting, fn item ->
            can_review = is_nil(item.person_name) or item.person_name == person
            item = Map.put(item, :can_review, can_review)

            if is_nil(item.person_name),
              do: %{item | person_name: "Command-line enrollment"},
              else: item
          end)

        {:ok,
         present(
           %{snapshot | devices: devices, waiting: waiting},
           person,
           Map.get(principal, :display_name)
         )}

      error ->
        error
    end
  end

  def list_devices(_, _), do: {:error, :forbidden}

  def prioritization(
        %__MODULE__{relay: relay, configured_owner_person: configured_owner_person},
        %{person_id: person, owner_key: key}
      ),
      do: Relay.dashboard_prioritization(relay, person, key, configured_owner_person)

  def prioritization(_, _), do: {:error, :forbidden}

  def save_prioritization(
        %__MODULE__{relay: relay, configured_owner_person: configured_owner_person},
        {%{person_id: person, owner_key: key}, algorithm, endpoint, model}
      ),
      do:
        Relay.save_dashboard_prioritization(
          relay,
          person,
          key,
          configured_owner_person,
          algorithm,
          endpoint,
          model
        )

  def save_prioritization(_, _), do: {:error, :forbidden}

  def levels(
        %__MODULE__{relay: relay},
        %{person_id: person, owner_key: key}
      ),
      do: Relay.dashboard_levels(relay, person, key)

  def levels(_, _), do: {:error, :forbidden}

  def save_focus_permission(
        %__MODULE__{relay: relay},
        {%{person_id: person, owner_key: key}, device_key, permission, enabled}
      )
      when permission in [:see, :push] and is_boolean(enabled),
      do:
        Relay.save_dashboard_focus_permission(relay, person, key, device_key, permission, enabled)

  def save_focus_permission(_, _), do: {:error, :forbidden}

  def save_levels(
        %__MODULE__{relay: relay},
        {%{person_id: person, owner_key: key}, rows}
      ),
      do: Relay.save_dashboard_levels(relay, person, key, rows)

  def save_levels(_, _), do: {:error, :forbidden}

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
        {%{person_id: person, owner_key: key}, code, expected_key, deadline, senders}
      ),
      do: Relay.owner_pending_approve(relay, person, key, code, expected_key, deadline, senders)

  def decline_pending(
        %__MODULE__{relay: relay},
        {%{person_id: person, owner_key: key}, code, expected_key, deadline}
      ),
      do: Relay.owner_pending_decline(relay, person, key, code, expected_key, deadline)

  def revoke_device(
        %__MODULE__{relay: relay},
        {%{person_id: person, owner_key: key}, device_key}
      ),
      do: Relay.owner_revoke_device(relay, person, key, device_key)
end
