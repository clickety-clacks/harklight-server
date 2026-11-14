defmodule AttentionServer.Relay do
  @moduledoc """
  Serialized relay core with an optional durable SQLite store.

  `request_line/3` retains the first slice's trusted-key test seam. The
  `TLSRelay` and `WebSocketRelay` bind an OTP-verified TLS client certificate
  to the enrolled SPKI key before opening a socket-backed session. The
  WebSocket adapter opens it only after `/relay` subprotocol negotiation.
  Owner-approved enrollment remains separate work.
  """

  use GenServer
  require Logger
  @agent_upload_idle_ms 60 * 60 * 1_000
  @upload_sweep_ms 60_000
  @default_sweep_retry_ms 1_000
  @max_timer_delay_ms 4_294_967_295
  @prioritization_debounce_ms 250
  @route_b_default_limit 20
  @route_b_max_limit 100
  @focus_recent_ms 24 * 60 * 60 * 1_000
  @hosted_test_states ~w(no-devices device-waiting device-enrolled)
  @hosted_test_label "Test account"
  @hosted_test_device "Test device"

  alias AttentionServer.{
    AgentWire,
    AppliedScoreRecord,
    BrowserRecipient,
    BounceDecision,
    BouncePolicy,
    Conversation,
    ConversationFiles,
    DashboardInventory,
    Diagnostics,
    DeviceIdentity,
    Engine,
    InboxPaging,
    JSON,
    Message,
    JevClient,
    JevRanking,
    Placement,
    Store
  }

  @browser_frame_types ~w(
                          inbox_start inbox_page inbox_reconcile inbox_receipts
                          inbox_checkpoint_ack inbox_attach conversation_open conversation_ack
                          message_open answer answer_no_comment discussion conversation_read
                          view_get
                        )

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def setup_person(server, person), do: GenServer.call(server, {:setup_person, person})
  def setup_device(server, person, key), do: GenServer.call(server, {:setup_device, person, key})

  @doc "Local-operator bootstrap from a public owner certificate; no network caller uses this API."
  def operator_bootstrap(server, certificate_der) do
    with {:ok, key} <- DeviceIdentity.from_certificate(certificate_der),
         do: GenServer.call(server, {:operator_bootstrap, key})
  end

  @doc "Local-operator owner recovery retains the person and retires the old owner key."
  def operator_recover(server, person, certificate_der) do
    with {:ok, key} <- DeviceIdentity.from_certificate(certificate_der),
         do: GenServer.call(server, {:operator_recover, person, key})
  end

  def owner_revision(server, person, verified_owner_key),
    do: GenServer.call(server, {:owner_revision, person, verified_owner_key})

  @doc "Read device metadata under current owner authority; only a configured self-host owner can see all persons."
  def dashboard_inventory(server, person, owner_key, configured_owner_person),
    do: GenServer.call(server, {:dashboard_inventory, person, owner_key, configured_owner_person})

  def save_dashboard_focus_permission(server, person, owner_key, device_key, permission, enabled),
    do:
      GenServer.call(
        server,
        {:save_dashboard_focus_permission, person, owner_key, device_key, permission, enabled}
      )

  def dashboard_prioritization(server, person, owner_key, configured_owner_person),
    do:
      GenServer.call(
        server,
        {:dashboard_prioritization, person, owner_key, configured_owner_person}
      )

  def dashboard_levels(server, person, owner_key),
    do: GenServer.call(server, {:dashboard_levels, person, owner_key})

  def save_dashboard_levels(server, person, owner_key, levels),
    do: GenServer.call(server, {:save_dashboard_levels, person, owner_key, levels})

  def save_dashboard_prioritization(
        server,
        person,
        owner_key,
        configured_owner_person,
        algorithm,
        endpoint,
        model
      ),
      do:
        GenServer.call(
          server,
          {:save_dashboard_prioritization, person, owner_key, configured_owner_person, algorithm,
           endpoint, model}
        )

  def owner_identity(server, verified_owner_key),
    do: GenServer.call(server, {:owner_identity, verified_owner_key})

  def agent_identity(server, verified_agent_key),
    do: GenServer.call(server, {:agent_identity, verified_agent_key})

  @doc "Read the current device origin's conversation feed without advancing its cursor."
  def origin_feed(server, verified_device_key, since),
    do: GenServer.call(server, {:origin_feed, verified_device_key, since})

  @doc "Read one current-origin message without recording delivery, acknowledgment, or operation state."
  def origin_message_get(server, verified_device_key, sender_id, message_id, request_revision),
    do:
      GenServer.call(
        server,
        {:origin_message_get, verified_device_key, sender_id, message_id, request_revision}
      )

  def focus_get(server, verified_agent_key),
    do: GenServer.call(server, {:focus_get, verified_agent_key})

  def view_show(server, verified_agent_key, request),
    do: GenServer.call(server, {:view_show, verified_agent_key, request})

  def focus_stream_attach(server, verified_agent_key, worker),
    do: GenServer.call(server, {:focus_stream_attach, verified_agent_key, worker})

  def focus_stream_detach(server, verified_agent_key, worker),
    do: GenServer.call(server, {:focus_stream_detach, verified_agent_key, worker})

  def origin_advisory_feed(server, verified_device_key, handle, since),
    do: GenServer.call(server, {:origin_advisory_feed, verified_device_key, handle, since})

  def origin_advisory_ack(server, verified_device_key, handle, advisory_id),
    do: GenServer.call(server, {:origin_advisory_ack, verified_device_key, handle, advisory_id})

  @doc "Record one originating-session delivery receipt, separately from relay ACK."
  def origin_ack(server, verified_device_key, feed_seq, reports \\ nil),
    do: GenServer.call(server, {:origin_ack, verified_device_key, feed_seq, reports})

  def origin_file_use(server, verified_device_key, feed_seq, name, outcome, op_id),
    do:
      GenServer.call(
        server,
        {:origin_file_use, verified_device_key, feed_seq, name, outcome, op_id}
      )

  def origin_status(server, verified_device_key, handle),
    do: GenServer.call(server, {:origin_status, verified_device_key, handle})

  def origin_ack2(server, verified_device_key, feed_seq, declaration),
    do: GenServer.call(server, {:origin_ack2, verified_device_key, feed_seq, declaration})

  def origin_coverage(server, key), do: GenServer.call(server, {:origin_coverage, key})

  def origin_support(server, verified_device_key, declaration),
    do: GenServer.call(server, {:origin_support, verified_device_key, declaration})

  def origin_attachment_update(server, verified_device_key, declaration),
    do: GenServer.call(server, {:origin_attachment_update, verified_device_key, declaration})

  @doc "Publish file limits only to a currently enrolled origin device."
  def origin_file_limits(server, verified_device_key),
    do: GenServer.call(server, {:origin_file_limits, verified_device_key})

  def upload_begin(server, verified_device_key, attributes, size),
    do: GenServer.call(server, {:upload_begin, verified_device_key, attributes, size})

  def upload_end(server, lease), do: GenServer.call(server, {:upload_end, lease})

  def discard_uploads(server, verified_device_key, attributes),
    do: GenServer.call(server, {:discard_uploads, verified_device_key, attributes})

  def sweep_uploads(server, now_ms), do: GenServer.call(server, {:sweep_uploads, now_ms})

  def upload_file(server, verified_device_key, attributes, path, size, sha256, lease \\ nil),
    do:
      upload_file_outside_relay(
        server,
        verified_device_key,
        attributes,
        path,
        size,
        sha256,
        lease
      )

  def fetch_file(server, verified_device_key, upload_id) do
    GenServer.call(server, {:fetch_file, verified_device_key, upload_id})
    |> verify_fetched_file()
  end

  def browser_fetch_file(server, person, owner_key, owner_revision, upload_id) do
    GenServer.call(server, {:browser_fetch_file, person, owner_key, owner_revision, upload_id})
    |> verify_fetched_file()
  end

  defp verify_fetched_file({:ok, upload, path}) do
    case Store.verify_upload(path, upload) do
      :ok -> {:ok, upload, path}
      error -> error
    end
  end

  defp verify_fetched_file(error), do: error

  defp upload_file_outside_relay(server, key, attrs, path, size, hash, lease),
    do: upload_file_outside_relay(server, key, attrs, path, size, hash, lease, :device)

  defp upload_file_outside_relay(server, key, attrs, path, size, hash, lease, context) do
    prepare =
      case context do
        :device ->
          {:upload_prepare, key, attrs, size, hash, lease}

        {:browser, browser_session_id, recipient} ->
          {:browser_upload_prepare, browser_session_id, recipient, attrs, size, hash, lease}
      end

    case GenServer.call(server, prepare) do
      {:ok, :duplicate, reply} ->
        {:ok, reply}

      {:ok, upload, upload_dir} ->
        case Store.stage_upload(upload_dir, upload, path) do
          {:ok, staged} ->
            try do
              case Store.publish_staged_upload(upload_dir, upload, staged) do
                {:ok, published} ->
                  commit =
                    case context do
                      :device ->
                        {:upload_commit, key, upload, published, lease}

                      {:browser, browser_session_id, recipient} ->
                        {:browser_upload_commit, browser_session_id, recipient, upload, published,
                         lease}
                    end

                  GenServer.call(
                    server,
                    commit,
                    :infinity
                  )

                error ->
                  error
              end
            after
              _ = File.rm(staged)
            end

          error ->
            error
        end

      error ->
        error
    end
  end

  def origin_session_gone(server, verified_device_key, handle),
    do: GenServer.call(server, {:origin_session_gone, verified_device_key, handle})

  @doc "Attach one authenticated origin SSE worker to a session handle."
  def origin_stream_attach(server, verified_device_key, handle, worker, reconnect \\ false),
    do:
      GenServer.call(
        server,
        {:origin_stream_attach, verified_device_key, handle, worker, reconnect, false}
      )

  def origin_stream_attach(server, verified_device_key, handle, worker, reconnect, advisory_v1),
    do:
      GenServer.call(
        server,
        {:origin_stream_attach, verified_device_key, handle, worker, reconnect, advisory_v1}
      )

  def origin_stream_detach(server, verified_device_key, handle, worker),
    do: GenServer.call(server, {:origin_stream_detach, verified_device_key, handle, worker})

  @doc "Release one agent reply within the same serialized authority boundary."
  def agent_send_request(server, verified_agent_key, revision, request, socket),
    do:
      GenServer.call(
        server,
        {:agent_send_request, verified_agent_key, revision, request, socket},
        :infinity
      )

  @doc "Release a G6 HTTP reply in the same serialized agent-authority boundary."
  def agent_send_https(server, verified_agent_key, revision, request, socket),
    do:
      GenServer.call(
        server,
        {:agent_send_https, verified_agent_key, revision, request, socket},
        :infinity
      )

  def owner_register_agent(server, person, owner_key, id, agent_key),
    do: GenServer.call(server, {:owner_register_agent, person, owner_key, id, agent_key})

  def owner_replace_agent_grants(server, person, owner_key, agent_key, revision, read, respond),
    do:
      GenServer.call(
        server,
        {:owner_replace_agent_grants, person, owner_key, agent_key, revision, read, respond}
      )

  def owner_revoke_agent(server, person, owner_key, agent_key),
    do: GenServer.call(server, {:owner_revoke_agent, person, owner_key, agent_key})

  def owner_list_agents(server, person, owner_key),
    do: GenServer.call(server, {:owner_list_agents, person, owner_key})

  @doc "Release a bounded owner response in mutation order only while its TLS key/revision is current."
  def owner_send_line(server, person, verified_owner_key, revision, socket, line),
    do:
      GenServer.call(
        server,
        {:owner_send_line, person, verified_owner_key, revision, socket, line}
      )

  @doc "Pending-certificate submission seam for the separate narrow HTTPS listener."
  def pending_enroll(server, certificate_der, label),
    do: GenServer.call(server, {:pending_enroll, certificate_der, label})

  def pending_status(server, status_secret),
    do: GenServer.call(server, {:pending_status, status_secret})

  def pending_resume(server, status_secret, certificate_der),
    do: GenServer.call(server, {:pending_resume, status_secret, certificate_der})

  def pending_cancel(server, secret, code, expected_key, deadline),
    do: GenServer.call(server, {:pending_cancel, secret, code, expected_key, deadline})

  @doc "Key argument must come from TLS CertificateVerify, never a body-supplied identity."
  def device_remove(server, verified_key),
    do: GenServer.call(server, {:device_remove, verified_key})

  # Only cleanup TLS admission uses this retained identity; it grants no relay access.
  def removal_identity(server, key), do: GenServer.call(server, {:removal_identity, key})

  @doc "Issue a one-use browser credential under the verified current person's owner."
  def owner_browser_sign_in_create(server, person, verified_owner_key),
    do: GenServer.call(server, {:owner_browser_sign_in_create, person, verified_owner_key})

  @doc "Authenticate a bearer once, returning its stored person-owner principal only after commit."
  def consume_browser_sign_in(server, token),
    do: GenServer.call(server, {:consume_browser_sign_in, token})

  def owner_invite_create(server, person, owner_key),
    do: GenServer.call(server, {:owner_invite_create, person, owner_key})

  def owner_invite_create(server, person, owner_key, request_id),
    do: GenServer.call(server, {:owner_invite_create, person, owner_key, request_id})

  def dashboard_origin(server), do: GenServer.call(server, :dashboard_origin)

  @doc """
  Create, or return, the one person for a hosted account reference. The person's
  owner key is held by the server and never leaves it; the hosted side gets only
  the person identifier.
  """
  def hosted_account_create(server, account_ref),
    do: GenServer.call(server, {:hosted_account_create, account_ref})

  @doc """
  Issue a one-use browser sign-in for a hosted person under its server-held owner
  key, recording the readable label the dashboard shows for it. Refuses any
  person that no hosted account owns.
  """
  def hosted_sign_in_create(server, person, label),
    do: GenServer.call(server, {:hosted_sign_in_create, person, label})

  def hosted_set_algorithm(server, person, algorithm),
    do: GenServer.call(server, {:hosted_set_algorithm, person, algorithm})

  def hosted_delivery_state(server, person, target_state),
    do: GenServer.call(server, {:hosted_delivery_state, person, target_state})

  @doc """
  Put the designated test account's person into a named product state:
  `no-devices`, `device-waiting` or `device-enrolled`. Acts on no other person.
  """
  def hosted_test_reset(server, target_state),
    do: GenServer.call(server, {:hosted_test_reset, target_state}, 30_000)

  def invitation_resolve(server, token),
    do: GenServer.call(server, {:invitation_resolve, token})

  def invited_enroll(server, invite_token, certificate_der, label, status_secret),
    do:
      GenServer.call(
        server,
        {:invited_enroll, invite_token, certificate_der, label, status_secret}
      )

  def owner_pending_decline(server, person, owner_key, code, expected_key, deadline),
    do:
      GenServer.call(
        server,
        {:owner_pending_decline, person, owner_key, code, expected_key, deadline}
      )

  @doc "Owner-key arguments must come from a proved management TLS peer."
  def owner_pending_lookup(server, person, verified_owner_key, code),
    do: GenServer.call(server, {:owner_pending_lookup, person, verified_owner_key, code})

  def owner_pending_approve(
        server,
        person,
        verified_owner_key,
        code,
        expected_key,
        expected_expires_at,
        sender_grants
      ),
      do:
        GenServer.call(
          server,
          {:owner_pending_approve, person, verified_owner_key, code, expected_key,
           expected_expires_at, sender_grants}
        )

  def owner_revoke_device(server, person, verified_owner_key, device_key),
    do: GenServer.call(server, {:owner_revoke_device, person, verified_owner_key, device_key})

  def owner_grant_sender(server, person, verified_owner_key, device_key, pattern),
    do:
      GenServer.call(
        server,
        {:owner_grant_sender, person, verified_owner_key, device_key, pattern}
      )

  def owner_revoke_sender(server, person, verified_owner_key, device_key, pattern),
    do:
      GenServer.call(
        server,
        {:owner_revoke_sender, person, verified_owner_key, device_key, pattern}
      )

  def owner_grant_enrollment_read(
        server,
        person,
        verified_owner_key,
        connector,
        grant_id,
        source,
        target
      ),
      do:
        GenServer.call(
          server,
          {:owner_grant_enrollment_read, person, verified_owner_key, connector, grant_id, source,
           target}
        )

  def owner_revoke_enrollment_read(server, person, verified_owner_key, grant_id),
    do:
      GenServer.call(
        server,
        {:owner_revoke_enrollment_read, person, verified_owner_key, grant_id}
      )

  def owner_grant_maintainer(server, person, verified_owner_key, grant_id, grantee, scopes),
    do:
      GenServer.call(
        server,
        {:owner_grant_maintainer, person, verified_owner_key, grant_id, grantee, scopes}
      )

  def owner_revoke_maintainer(server, person, verified_owner_key, grant_id),
    do: GenServer.call(server, {:owner_revoke_maintainer, person, verified_owner_key, grant_id})

  @doc "Local projection fixture only; does not expose agent service authority on the wire."
  def setup_agent_fixture(server, person, id, key, sender_ids),
    do: GenServer.call(server, {:setup_agent_fixture, person, id, key, sender_ids})

  def fixture_agent_answer(server, key, frame),
    do: GenServer.call(server, {:fixture_agent_answer, key, frame})

  def revoke_agent_fixture(server, key),
    do: GenServer.call(server, {:revoke_agent_fixture, key})

  @doc "Local operator/test provisioning of a certificate's public-key identity."
  def setup_device_certificate(server, person, certificate_der) do
    with {:ok, key} <- DeviceIdentity.from_certificate(certificate_der),
         :ok <- setup_device(server, person, key),
         do: {:ok, key}
  end

  def setup_sender(server, key, pattern),
    do: GenServer.call(server, {:setup_sender, key, pattern})

  def revoke_device(server, key), do: GenServer.call(server, {:revoke_device, key})

  @doc "Trusted local fixture seam for an externally recorded source-owner delegation."
  def grant_source_report(server, person, key, source, senders, delegation_ref),
    do:
      GenServer.call(server, {:grant_source_report, person, key, source, senders, delegation_ref})

  def revoke_source_report(server, person, key, source),
    do: GenServer.call(server, {:revoke_source_report, person, key, source})

  @doc "Trusted local owner fixture for one exact enrollment disclosure grant."
  def grant_enrollment_read_fixture(server, person, connector, grant_id, source, target, issuer),
    do:
      GenServer.call(
        server,
        {:grant_enrollment_read_fixture, person, connector, grant_id, source, target, issuer}
      )

  def revoke_enrollment_read_fixture(server, person, grant_id),
    do: GenServer.call(server, {:revoke_enrollment_read_fixture, person, grant_id})

  def request_line(server, verified_key, line),
    do: GenServer.call(server, {:request_line, verified_key, line}, :infinity)

  @doc false
  def request_frame(server, verified_key, frame),
    do: GenServer.call(server, {:request_frame, verified_key, frame}, :infinity)

  @doc false
  def claim_push(server, now_ms), do: GenServer.call(server, {:claim_push, now_ms}, :infinity)

  @doc false
  def enable_push(server), do: GenServer.call(server, :enable_push)

  @doc false
  def finish_push(server, claim, outcome, now_ms),
    do: GenServer.call(server, {:finish_push, claim, outcome, now_ms}, :infinity)

  def events(server, verified_key, since),
    do: GenServer.call(server, {:events, verified_key, since}, :infinity)

  def conversation_events(server, verified_key, since),
    do: GenServer.call(server, {:conversation_events, verified_key, since}, :infinity)

  def conversation_cursor(server, verified_key),
    do: GenServer.call(server, {:conversation_cursor, verified_key}, :infinity)

  def acknowledged_cursor(server, verified_key),
    do: GenServer.call(server, {:cursor, verified_key}, :infinity)

  def device_revoked?(server, verified_key),
    do: GenServer.call(server, {:device_revoked?, verified_key}, :infinity)

  def open_session(server, verified_key),
    do: GenServer.call(server, {:open_session, verified_key, nil, nil, nil, :raw}, :infinity)

  def register_tls_listener(server, listener),
    do: GenServer.call(server, {:register_tls_listener, listener})

  def unregister_tls_listener(server, listener),
    do: GenServer.call(server, {:unregister_tls_listener, listener})

  @doc "Advertise the bound origin HTTPS listener only while it is running."
  def register_origin_listener(server, listener, port),
    do: GenServer.call(server, {:register_origin_listener, listener, port})

  def unregister_origin_listener(server, listener),
    do: GenServer.call(server, {:unregister_origin_listener, listener})

  @doc "Opens a socket-backed session after a successful client-certificate handshake."
  def open_tls_session(server, verified_key, socket, listener),
    do:
      GenServer.call(
        server,
        {:open_session, verified_key, self(), socket, listener, :raw},
        :infinity
      )

  @doc "Opens a device session only after its authenticated WebSocket negotiation."
  def open_websocket_session(server, verified_key, socket, listener),
    do:
      GenServer.call(
        server,
        {:open_session, verified_key, self(), socket, listener, :websocket},
        :infinity
      )

  def open_browser_recipient(
        server,
        person,
        owner_key,
        owner_revision,
        browser_session_id,
        recipient,
        worker
      ),
      do:
        GenServer.call(
          server,
          {:open_browser_recipient, person, owner_key, owner_revision, browser_session_id,
           recipient, worker},
          :infinity
        )

  def end_browser_session(server, browser_session_id),
    do: GenServer.call(server, {:end_browser_session, browser_session_id}, :infinity)

  def browser_session_request_line(server, browser_session_id, session_id, line),
    do:
      GenServer.call(
        server,
        {:browser_session_request_line, browser_session_id, session_id, line},
        :infinity
      )

  def browser_session_drain(server, browser_session_id, session_id),
    do:
      GenServer.call(
        server,
        {:browser_session_drain, browser_session_id, session_id},
        :infinity
      )

  def close_browser_connection(server, browser_session_id, session_id),
    do:
      GenServer.call(
        server,
        {:close_browser_connection, browser_session_id, session_id},
        :infinity
      )

  def browser_file_limits(server, browser_session_id, recipient),
    do: GenServer.call(server, {:browser_file_limits, browser_session_id, recipient})

  def browser_discard_uploads(server, browser_session_id, recipient, attributes),
    do:
      GenServer.call(
        server,
        {:browser_discard_uploads, browser_session_id, recipient, attributes}
      )

  def browser_upload_begin(server, browser_session_id, recipient, attributes, size, worker),
    do:
      GenServer.call(
        server,
        {:browser_upload_begin, browser_session_id, recipient, attributes, size, worker}
      )

  def browser_upload_file(
        server,
        browser_session_id,
        recipient,
        attributes,
        path,
        size,
        sha256,
        lease
      ),
      do:
        upload_file_outside_relay(
          server,
          nil,
          attributes,
          path,
          size,
          sha256,
          lease,
          {:browser, browser_session_id, recipient}
        )

  def session_request_line(server, session_id, line) do
    GenServer.call(server, {:session_request_line, session_id, line}, :infinity)
  end

  def session_drain(server, session_id),
    do: GenServer.call(server, {:session_drain, session_id}, :infinity)

  @doc "Serializes socket output with session replacement and revocation."
  def session_send(server, session_id, frames),
    do: GenServer.call(server, {:session_send, session_id, frames}, :infinity)

  def close_session(server, session_id),
    do: GenServer.call(server, {:close_session, session_id}, :infinity)

  @impl true
  def init(opts) do
    case Store.open(Keyword.get(opts, :store_path)) do
      {:ok, store} ->
        case startup_cleanup(store) do
          {:ok, store} ->
            Process.send_after(self(), :upload_sweep, @upload_sweep_ms)

            state = %{
              store: store,
              clock: Keyword.get(opts, :clock, fn -> System.system_time(:millisecond) end),
              default_index: Engine.default_index(store.engine),
              default_timer: nil,
              default_timer_generation: 0,
              bounce_deadline_timer: nil,
              bounce_deadline_generation: 0,
              upload_index: build_upload_index(store.engine),
              active_uploads: %{},
              sessions: %{},
              browser_recipients: %{},
              origin_streams: %{},
              origin_streams_by_identity: %{},
              focus_clients: %{},
              focus_clients_by_person: %{},
              held_shows: %{},
              focus_streams: %{},
              focus_streams_by_person: %{},
              focus_expiry_timers: %{},
              focus_expiry_generations: %{},
              tls_listeners: %{},
              origin_listener: nil,
              inbox_builds: %{},
              inbox_build_monitors: %{},
              applied_score_record:
                AppliedScoreRecord.start(
                  Keyword.get(opts, :deployment_mode, :self_hosted),
                  Keyword.get(opts, :jev_score_record_path)
                ),
              priority_views: %{},
              priority_recorded_scores: %{},
              priority_generations: %{},
              priority_timers: %{},
              priority_jobs: %{},
              priority_queued: %{},
              priority_sequences: %{},
              priority_evaluator: Keyword.get(opts, :priority_evaluator, &JevClient.evaluate/1),
              priority_caches: %{},
              priority_cache_epochs: %{},
              priority_failures: %{},
              priority_retry_timers: %{},
              priority_deadline_timers: %{},
              priority_revision_namespace:
                Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false),
              priority_revision_counter: 0,
              agent_fixture_mode: Keyword.get(opts, :agent_fixture_mode, false) == true,
              source_fixture_mode: Keyword.get(opts, :source_fixture_mode, false) == true,
              enrollment_fixture_mode: Keyword.get(opts, :enrollment_fixture_mode, false) == true,
              push_enabled: Keyword.get(opts, :push_enabled, false) == true,
              sender_names: Keyword.get(opts, :sender_names, %{}),
              join_configuration: Keyword.get(opts, :join_configuration),
              dashboard_origin: Keyword.get(opts, :dashboard_origin),
              hosted_test_account: Keyword.get(opts, :hosted_test_account),
              deployment_mode: Keyword.get(opts, :deployment_mode, :self_hosted),
              upload_cap: Keyword.get(opts, :upload_cap, :none)
            }

            state = initialize_priority_views(state)
            state = reschedule_default_sweep(state, state.default_index)
            state = reschedule_bounce_sweep(state)

            case sweep_agent_uploads(state, System.system_time(:millisecond)) do
              {:ok, next, _count} ->
                {:ok, next}

              {:error, reason, next} ->
                Diagnostics.failure(:upload_cleanup, :startup_cleanup, reason, :unavailable)
                {:ok, next}
            end

          error ->
            Store.close(store)
            {:stop, {:storage_unavailable, error}}
        end

      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp startup_cleanup(store) do
    actor_keys = browser_actor_keys(store.engine)
    engine = discard_browser_actor_state(store.engine, actor_keys)

    with :ok <- Store.discard_building_inbox_snapshots(store),
         {:ok, store} <- commit_browser_cleanup(store, engine),
         :ok <- Store.discard_browser_inbox_snapshots(store) do
      {:ok, store}
    else
      {:error, reason, _store} -> {:error, reason}
      error -> error
    end
  end

  defp commit_browser_cleanup(store, engine) do
    if engine == store.engine do
      {:ok, store}
    else
      commit(store, engine, %{
        person: "system",
        actor: "browser_session",
        action: "browser_session_restart"
      })
    end
  end

  @impl true
  def terminate(_reason, %{store: store}), do: Store.close(store)

  # Issue a one-use browser sign-in for a person under its verified or server-held
  # owner key. The URL is present only when a dashboard origin is configured.
  defp issue_browser_sign_in(state, engine, person, owner, actor) do
    token = "hbs1_" <> Base.encode16(:crypto.strong_rand_bytes(32), case: :lower)
    {:ok, hash} = AttentionServer.BrowserSignIn.token_hash(token)
    now = System.system_time(:millisecond)

    case AttentionServer.BrowserSignIn.issue(engine, person, owner, hash, now) do
      {:ok, next, record} ->
        case commit(state.store, next, %{
               person: person,
               actor: actor,
               action: "owner_browser_sign_in_create"
             }) do
          {:ok, store} ->
            response = %{
              "v" => 1,
              "person_id" => person,
              "sign_in_token" => token,
              "expires_at" => record.expires_at
            }

            response =
              case state.dashboard_origin do
                origin when is_binary(origin) ->
                  Map.put(response, "sign_in_url", origin <> "/sign-in#" <> token)

                _ ->
                  response
              end

            {:ok, response, %{state | store: store}}

          _ ->
            {:error, :storage_unavailable}
        end

      error ->
        error
    end
  end

  defp hosted_record(engine, person) do
    case Engine.hosted_account(engine, person) do
      %{} = record -> {:ok, record}
      nil -> {:error, :not_hosted}
    end
  end

  # The test account is always labelled "Test account"; any other hosted account
  # carries the label the hosted side last supplied, and keeps it when none is given.
  defp hosted_label_for(engine, person, true, _label),
    do: Engine.hosted_account_label(engine, person, @hosted_test_label)

  defp hosted_label_for(engine, _person, false, nil), do: {:ok, engine}

  defp hosted_label_for(engine, person, false, label),
    do: Engine.hosted_account_label(engine, person, label)

  defp with_hosted_label(map, key, engine), do: with_hosted_label(map, key, engine, map.person_id)

  defp with_hosted_label(map, key, engine, person) do
    case Engine.hosted_account(engine, person) do
      %{label: label} when is_binary(label) -> Map.put(map, key, label)
      _ -> map
    end
  end

  # Reach the named test state through the ordinary invitation, join and approval
  # steps with a throwaway device stand-in, so the dashboard shows real rows.
  defp hosted_test_stage(engine, _person, _owner, "no-devices", _state), do: {:ok, engine, nil}

  defp hosted_test_stage(_engine, _person, _owner, _target, %{join_configuration: nil}),
    do: {:error, :join_unavailable}

  defp hosted_test_stage(engine, person, owner, target, state) do
    now = System.system_time(:millisecond)
    token = :crypto.strong_rand_bytes(32)
    hash = :crypto.hash(:sha256, token)
    %{cert: certificate} = hosted_test_device()
    status_hash = :crypto.hash(:sha256, :crypto.strong_rand_bytes(32))
    config = state.join_configuration

    with {:ok, invited, _invite} <-
           AttentionServer.GuidedJoin.create(engine, person, owner, hash, config, now),
         {:ok, waiting, %{"approval_code" => code}} <-
           guided_pending(
             invited,
             hash,
             certificate,
             @hosted_test_device,
             status_hash,
             config,
             now,
             8
           ) do
      case target do
        "device-waiting" ->
          {:ok, waiting, code}

        "device-enrolled" ->
          item = Map.fetch!(waiting.pending_enrollments, code)

          case Engine.pending_approve(
                 waiting,
                 person,
                 owner,
                 code,
                 item.key,
                 item.expires_at,
                 [],
                 now
               ) do
            {:ok, enrolled, _device_key} -> {:ok, enrolled, code}
            {:error, reason} -> {:error, reason}
          end
      end
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :refused}
    end
  end

  defp hosted_test_device do
    :public_key.pkix_test_root_cert(String.to_charlist(@hosted_test_device),
      key: {:namedCurve, :secp256r1}
    )
  end

  defp guided_pending(_, _, _, _, _, _, _, 0), do: {:error, :refused}

  defp guided_pending(engine, hash, cert, label, secret, config, now, attempts) do
    code = :crypto.strong_rand_bytes(7) |> Base.encode32(padding: false) |> binary_part(0, 10)

    case AttentionServer.GuidedJoin.redeem(engine, hash, cert, label, secret, code, config, now) do
      {:error, :collision} ->
        guided_pending(engine, hash, cert, label, secret, config, now, attempts - 1)

      result ->
        result
    end
  end

  defp secret_hash(hex) when is_binary(hex) and byte_size(hex) == 64 do
    with {:ok, secret} <- Base.decode16(hex, case: :lower),
         true <- byte_size(secret) == 32,
         do: {:ok, :crypto.hash(:sha256, secret)},
         else: (_ -> {:error, :invalid_secret})
  end

  defp secret_hash(_), do: {:error, :invalid_secret}

  defp generate_pending(_engine, _certificate, _label, _now, 0),
    do: {:error, :storage_unavailable}

  defp generate_pending(engine, certificate, label, now, attempts) do
    secret = :crypto.strong_rand_bytes(32)
    code = :crypto.strong_rand_bytes(7) |> Base.encode32(padding: false) |> binary_part(0, 10)

    case Engine.pending_enroll(
           engine,
           certificate,
           label,
           code,
           :crypto.hash(:sha256, secret),
           now
         ) do
      {:ok, next} -> {:ok, next, code, secret}
      {:error, :collision} -> generate_pending(engine, certificate, label, now, attempts - 1)
      error -> error
    end
  end

  defp generate_resume(_engine, _hash, _cert, _now, 0), do: {:error, :storage_unavailable}

  defp generate_resume(engine, hash, cert, now, attempts) do
    code = :crypto.strong_rand_bytes(7) |> Base.encode32(padding: false) |> binary_part(0, 10)

    case Engine.pending_resume(engine, hash, cert, code, now) do
      {:error, :collision} -> generate_resume(engine, hash, cert, now, attempts - 1)
      result -> result
    end
  end

  defp fence_pending_time(state, identity, now_ms) do
    next = Engine.pending_observe_time(state.store.engine, identity, now_ms)

    if next == state.store.engine do
      {:ok, state}
    else
      case commit(state.store, next, %{
             person: "system",
             actor: "pending_deadline",
             action: "pending_clock_fence"
           }) do
        {:ok, store} -> {:ok, %{state | store: store}}
        {:error, _, _} -> {:error, :storage_unavailable}
      end
    end
  end

  @impl true
  def handle_call({:request_line, key, raw}, _from, state) do
    {reply, next} = request(state, key, raw)
    {:reply, reply, next}
  end

  def handle_call({:request_frame, key, frame}, _from, state) do
    case Engine.authorized_person(state.store.engine, key) do
      {:ok, _person} when is_map(frame) ->
        if JSON.ui1_duplicate_request?(frame) do
          {reply, next} = perform_request_frame(state, key, frame)
          {:reply, reply, next}
        else
          {:reply, {:ok, error_line(frame, "bad_frame")}, state}
        end

      {:ok, _person} ->
        {:reply, {:ok, error_line(%{}, "bad_frame")}, state}

      _ ->
        {:reply, {:error, :forbidden}, state}
    end
  end

  def handle_call(:enable_push, _from, state),
    do: {:reply, :ok, %{state | push_enabled: true}}

  def handle_call({:claim_push, now_ms}, _from, state) when is_integer(now_ms) do
    case Engine.claim_push(state.store.engine, now_ms) do
      {:ok, next_engine, claim} when next_engine == state.store.engine ->
        {:reply, {:ok, claim}, state}

      {:ok, next_engine, claim} ->
        case commit(state.store, next_engine, %{
               person: if(claim, do: claim.person, else: "system"),
               actor: "apns_dispatcher",
               action: "push_claim"
             }) do
          {:ok, store} -> {:reply, {:ok, claim}, %{state | store: store}}
          {:error, _, _} -> {:reply, {:error, :storage_unavailable}, state}
        end
    end
  end

  def handle_call({:finish_push, claim, outcome, now_ms}, _from, state) when is_integer(now_ms) do
    case Engine.finish_push(state.store.engine, claim, outcome, now_ms) do
      {:ok, next_engine} when next_engine == state.store.engine ->
        {:reply, :ok, state}

      {:ok, next_engine} ->
        case commit(state.store, next_engine, %{
               person: claim.person,
               actor: "apns_dispatcher",
               action: "push_finish"
             }) do
          {:ok, store} -> {:reply, :ok, %{state | store: store}}
          {:error, _, _} -> {:reply, {:error, :storage_unavailable}, state}
        end
    end
  end

  def handle_call({:events, key, since}, _from, state),
    do: {:reply, Engine.events(state.store.engine, key, since), state}

  def handle_call({:conversation_events, key, since}, _from, state),
    do: {:reply, Engine.conversation_events(state.store.engine, key, since), state}

  def handle_call({:conversation_cursor, key}, _from, state),
    do: {:reply, Engine.conversation_cursor(state.store.engine, key), state}

  def handle_call({:cursor, key}, _from, state),
    do: {:reply, Engine.acknowledged_cursor(state.store.engine, key), state}

  def handle_call({:device_revoked?, key}, _from, state),
    do: {:reply, Engine.device_revoked?(state.store.engine, key), state}

  def handle_call({:owner_revision, person, key}, _from, state),
    do: {:reply, Engine.owner_revision(state.store.engine, person, key), state}

  def handle_call({:dashboard_inventory, person, key, configured_owner_person}, _from, state) do
    engine = state.store.engine

    result =
      if is_binary(configured_owner_person) and person == configured_owner_person do
        DashboardInventory.for_account_owner(engine, configured_owner_person, key)
      else
        DashboardInventory.for_owner(engine, person, key)
      end

    {:reply, result, state}
  end

  def handle_call(
        {:save_dashboard_focus_permission, person, key, device_key, permission, enabled},
        _from,
        state
      )
      when permission in [:see, :push] and is_boolean(enabled) do
    device =
      case Map.get(state.store.engine.devices, device_key) do
        %{person: ^person, see_focus: see_focus, push_view: push_view} ->
          {see_focus, push_view}

        %{person: ^person} = device ->
          {Map.get(device, :see_focus, false), Map.get(device, :push_view, false)}

        _ ->
          {false, false}
      end

    {see_focus, push_view} =
      case permission do
        :see -> {enabled, elem(device, 1)}
        :push -> {elem(device, 0), enabled}
      end

    case Engine.owner_set_focus_permissions(
           state.store.engine,
           person,
           key,
           device_key,
           see_focus,
           push_view
         ) do
      {:ok, next_engine} ->
        if next_engine == state.store.engine do
          next =
            if permission == :see and not enabled,
              do: close_focus_streams(state, device_key),
              else: state

          {:reply, :ok, next}
        else
          case commit(
                 state.store,
                 next_engine,
                 %{person: person, actor: key, action: "dashboard_focus_permission"}
               ) do
            {:ok, store} ->
              next = %{state | store: store}

              next =
                if permission == :see and not enabled,
                  do: close_focus_streams(next, device_key),
                  else: next

              {:reply, :ok, next}

            _ ->
              {:reply, {:error, :storage_unavailable}, state}
          end
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(
        {:dashboard_prioritization, person, key, configured_owner_person},
        _from,
        state
      ) do
    if state.deployment_mode == :self_hosted and person == configured_owner_person and
         match?({:ok, ^person, _revision}, Engine.owner_identity(state.store.engine, key)) do
      state = ensure_priority_view(state, person)
      {:reply, {:ok, priority_settings(state, person)}, state}
    else
      {:reply, {:error, :forbidden}, state}
    end
  end

  def handle_call({:dashboard_levels, person, key}, _from, state) do
    case Engine.owner_identity(state.store.engine, key) do
      {:ok, ^person, _revision} ->
        {:reply, {:ok, %{"levels" => BounceDecision.levels(state.store.engine, person)}}, state}

      _ ->
        {:reply, {:error, :forbidden}, state}
    end
  end

  def handle_call({:save_dashboard_levels, person, key, rows}, _from, state) do
    with {:ok, ^person, _revision} <- Engine.owner_identity(state.store.engine, key),
         {:ok, levels} <- BouncePolicy.normalize_levels(rows),
         {:ok, next, config} <-
           BounceDecision.configure_levels(state.store.engine, person, levels, state.clock.()) do
      if next == state.store.engine do
        {:reply, {:ok, %{"levels" => BouncePolicy.public_levels(config.levels)}}, state}
      else
        case commit(
               state.store,
               next,
               %{person: person, actor: key, action: "dashboard_levels"}
             ) do
          {:ok, store} ->
            next_state = %{state | store: store} |> notify_levels_changed(person)

            {:reply, {:ok, %{"levels" => BouncePolicy.public_levels(config.levels)}}, next_state}

          _ ->
            {:reply, {:error, :storage_unavailable}, state}
        end
      end
    else
      {:error, :invalid_levels} -> {:reply, {:error, :invalid_levels}, state}
      _ -> {:reply, {:error, :forbidden}, state}
    end
  end

  def handle_call(
        {:save_dashboard_prioritization, person, key, configured_owner_person, algorithm,
         endpoint, model},
        _from,
        state
      ) do
    case save_priority_settings(
           state,
           person,
           key,
           configured_owner_person,
           algorithm,
           endpoint,
           model
         ) do
      {:ok, next, result} -> {:reply, {:ok, result}, next}
      {:error, reason, next, result} -> {:reply, {:error, reason, result}, next}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:owner_identity, key}, _from, state),
    do: {:reply, Engine.owner_identity(state.store.engine, key), state}

  def handle_call({:agent_identity, key}, _from, state),
    do: {:reply, Engine.agent_identity(state.store.engine, key), state}

  def handle_call({:origin_feed, key, since}, _from, state) do
    case Engine.deliver_origin_feed(state.store.engine, key, since) do
      {:ok, next, reply} ->
        case commit(state.store, next, audit(state.store.engine, key, "origin_feed")) do
          {:ok, store} -> {:reply, {:ok, reply}, %{state | store: store}}
          _ -> {:reply, {:error, :storage_unavailable}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call(
        {:origin_message_get, key, sender_id, message_id, request_revision},
        _from,
        state
      ) do
    {:reply,
     Engine.origin_message_get(state.store.engine, key, sender_id, message_id, request_revision),
     state}
  end

  def handle_call({:origin_advisory_feed, key, handle, since}, _from, state) do
    {:reply, BounceDecision.advisory_feed(state.store.engine, key, handle, since), state}
  end

  def handle_call({:origin_advisory_ack, key, handle, advisory_id}, _from, state) do
    case BounceDecision.advisory_ack(state.store.engine, key, handle, advisory_id) do
      {:ok, next, reply} when next == state.store.engine ->
        {:reply, {:ok, reply}, state}

      {:ok, next, reply} ->
        case commit(state.store, next, audit(state.store.engine, key, "origin_advisory_ack")) do
          {:ok, store} -> {:reply, {:ok, reply}, %{state | store: store}}
          _ -> {:reply, {:error, :storage_unavailable}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:origin_coverage, key}, _from, state) do
    case Engine.authorized_person(state.store.engine, key) do
      {:ok, person} -> {:reply, {:ok, covered_handles(state.store.engine, person, key)}, state}
      error -> {:reply, error, state}
    end
  end

  def handle_call({:origin_status, key, :origin}, _from, state) do
    case Engine.authorized_person(state.store.engine, key) do
      {:ok, person} ->
        high = Map.get(state.store.engine.origin_feeds, {person, key}, %{high: 0}).high
        {:reply, {:ok, %{"cursor" => high}}, state}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:origin_status, key, handle}, _from, state) do
    engine = state.store.engine

    case Engine.authorized_person(engine, key) do
      {:ok, person} ->
        listening? = origin_listening?(state, engine, {person, key, handle})
        {:reply, Engine.origin_status(engine, key, handle, listening?), state}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:origin_support, key, declaration}, _from, state) do
    case Engine.origin_support(state.store.engine, key, declaration) do
      {:ok, next, reply} when next == state.store.engine ->
        {:reply, {:ok, reply}, state}

      {:ok, next, reply} ->
        old_engine = state.store.engine
        {:ok, person} = Engine.authorized_person(next, key)
        handle = declaration["session_handle"]
        listening? = origin_listening?(state, next, {person, key, handle})
        {:ok, current} = Engine.origin_status(next, key, handle, listening?)

        next =
          Engine.origin_availability(
            next,
            person,
            key,
            handle,
            current["availability"],
            current["last_detached_at"]
          )

        session_key = {person, key, handle}

        case Store.commit(
               state.store,
               next,
               audit(old_engine, key, "origin_support")
             ) do
          {:ok, store} ->
            store = %{
              store
              | engine: BounceDecision.requeue_supported_session(store.engine, session_key)
            }

            next_state =
              %{state | store: store}
              |> notify_coverage_change(old_engine, person, key)
              |> refresh_stream_advisory_support({person, key, handle})
              |> reschedule_bounce_sweep()

            {:reply, {:ok, reply}, next_state}

          _ ->
            {:reply, {:error, :storage_unavailable}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:origin_attachment_update, key, declaration}, _from, state) do
    case Engine.origin_attachment_update(state.store.engine, key, declaration) do
      {:ok, next, reply} when next == state.store.engine ->
        {:reply, {:ok, reply}, state}

      {:ok, next, reply} ->
        {:ok, person} = Engine.authorized_person(next, key)
        proposed = %{state | store: %{state.store | engine: next}}
        next = origin_availability_changes(state, proposed, person, key)

        case commit(state.store, next, audit(state.store.engine, key, "origin_attachment_update")) do
          {:ok, store} ->
            updated =
              %{state | store: store}
              |> notify_coverage_change(state.store.engine, person, key)
              |> requeue_covered(person, key)

            {:reply, {:ok, reply}, updated}

          _ ->
            {:reply, {:error, :storage_unavailable}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:origin_file_limits, key}, _from, state) do
    file_limits_reply(state, key)
  end

  def handle_call({:browser_file_limits, browser_session_id, recipient}, _from, state) do
    case browser_recipient_session(state, browser_session_id, recipient) do
      {:ok, session} -> file_limits_reply(state, session.key)
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:discard_uploads, key, attrs}, _from, state) do
    discard_uploads_call(state, key, attrs)
  end

  def handle_call(
        {:browser_discard_uploads, browser_session_id, recipient, attrs},
        _from,
        state
      ) do
    case browser_recipient_session(state, browser_session_id, recipient) do
      {:ok, session} -> discard_uploads_call(state, session.key, attrs)
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(
        {:browser_upload_begin, browser_session_id, recipient, attrs, size, worker},
        _from,
        state
      ) do
    with {:ok, session} <- browser_recipient_session(state, browser_session_id, recipient),
         {:ok, person} <- Engine.authorized_person(state.store.engine, session.key),
         true <- is_pid(worker),
         true <- valid_upload_attributes?(attrs),
         true <- is_integer(size) and size >= 0,
         true <- state.upload_cap == :none or size <= state.upload_cap do
      case begin_upload(state, person, session.key, attrs, size, worker, browser_session_id) do
        {:ok, lease, next} -> {:reply, {:ok, lease}, next}
        {:error, reason} -> {:reply, {:error, reason}, state}
      end
    else
      {:error, reason} ->
        {:reply, {:error, reason}, state}

      false when is_integer(size) and is_integer(state.upload_cap) and size > state.upload_cap ->
        {:reply, {:error, :upload_cap_exceeded}, state}

      _ ->
        {:reply, {:error, :invalid_upload}, state}
    end
  end

  def handle_call({:upload_begin, key, attrs, size}, {worker, _}, state) do
    with {:ok, person} <- Engine.authorized_person(state.store.engine, key),
         true <- valid_upload_attributes?(attrs),
         true <- is_integer(size) and size >= 0,
         true <- state.upload_cap == :none or size <= state.upload_cap do
      case begin_upload(state, person, key, attrs, size, worker, nil) do
        {:ok, lease, next} -> {:reply, {:ok, lease}, next}
        {:error, reason} -> {:reply, {:error, reason}, state}
      end
    else
      {:error, reason} ->
        {:reply, {:error, reason}, state}

      false when is_integer(size) and is_integer(state.upload_cap) and size > state.upload_cap ->
        {:reply, {:error, :upload_cap_exceeded}, state}

      _ ->
        {:reply, {:error, :invalid_upload}, state}
    end
  end

  def handle_call({:upload_end, lease}, _from, state) do
    if active = Map.get(state.active_uploads, lease),
      do: Process.demonitor(active.monitor, [:flush])

    {:reply, :ok, %{state | active_uploads: Map.delete(state.active_uploads, lease)}}
  end

  def handle_call({:sweep_uploads, now_ms}, _from, state)
      when is_integer(now_ms) and now_ms >= 0 do
    case sweep_agent_uploads(state, now_ms) do
      {:ok, next, count} -> {:reply, {:ok, count}, next}
      {:error, reason, next} -> {:reply, {:error, reason}, next}
    end
  end

  def handle_call({:upload_prepare, key, attrs, size, hash, lease}, _from, state),
    do: upload_prepare_call(state, key, attrs, size, hash, lease, :device)

  def handle_call(
        {:browser_upload_prepare, browser_session_id, recipient, attrs, size, hash, lease},
        _from,
        state
      ) do
    case browser_recipient_session(state, browser_session_id, recipient) do
      {:ok, session} ->
        upload_prepare_call(
          state,
          session.key,
          attrs,
          size,
          hash,
          lease,
          {:browser, browser_session_id, recipient}
        )

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:upload_commit, key, upload, published, lease}, _from, state),
    do: upload_commit_call(state, key, upload, published, lease, :device)

  def handle_call(
        {:browser_upload_commit, browser_session_id, recipient, upload, published, lease},
        _from,
        state
      ) do
    case browser_recipient_session(state, browser_session_id, recipient) do
      {:ok, session} ->
        upload_commit_call(
          state,
          session.key,
          upload,
          published,
          lease,
          {:browser, browser_session_id, recipient}
        )

      {:error, reason} ->
        _ = File.rm(published)
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:fetch_file, key, id}, _from, state) do
    case Engine.authorized_person(state.store.engine, key) do
      {:ok, person} ->
        {:reply, fetched_file_for_person(state, person, id), state}

      _ ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(
        {:browser_fetch_file, person, owner_key, owner_revision, id},
        _from,
        state
      ) do
    case Engine.owner_revision(state.store.engine, person, owner_key) do
      {:ok, ^owner_revision} ->
        {:reply, fetched_file_for_person(state, person, id), state}

      _ ->
        {:reply, {:error, :session_ended}, state}
    end
  end

  def handle_call({:origin_session_gone, key, handle}, _from, state) do
    case Engine.origin_session_gone(state.store.engine, key, handle) do
      {:ok, next} ->
        {:ok, person} = Engine.authorized_person(next, key)
        listening? = origin_listening?(state, next, {person, key, handle})
        {:ok, previous} = Engine.origin_status(state.store.engine, key, handle, listening?)
        {:ok, current} = Engine.origin_status(next, key, handle, listening?)

        next =
          if previous["availability"] == current["availability"] do
            next
          else
            Engine.origin_availability(
              next,
              person,
              key,
              handle,
              current["availability"],
              current["last_detached_at"]
            )
          end

        case commit(
               state.store,
               next,
               audit(state.store.engine, key, "origin_session_gone")
             ) do
          {:ok, store} -> {:reply, :ok, %{state | store: store}}
          _ -> {:reply, {:error, :storage_unavailable}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:focus_get, key}, _from, state) do
    case focus_authorized_person(state.store.engine, key, :see) do
      {:ok, person} ->
        {:reply, {:ok, %{"clients" => focus_list(state, person)}}, state}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:view_show, key, request}, _from, state) do
    {result, next} = handle_view_show(state, key, request)
    {:reply, result, next}
  end

  def handle_call({:focus_stream_attach, key, worker}, _from, state) when is_pid(worker) do
    case focus_authorized_person(state.store.engine, key, :see) do
      {:ok, person} ->
        monitor = Process.monitor(worker)
        stream = %{key: key, person: person, monitor: monitor}
        next = put_focus_stream(state, worker, stream)
        {:reply, {:ok, focus_list(next, person)}, next}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:focus_stream_detach, key, worker}, _from, state) do
    case Map.get(state.focus_streams, worker) do
      %{key: ^key, monitor: monitor} ->
        Process.demonitor(monitor, [:flush])
        {:reply, :ok, delete_focus_stream(state, worker)}

      _ ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:origin_stream_attach, key, :origin, worker, reconnect, false}, _from, state) do
    with {:ok, person} <- Engine.authorized_person(state.store.engine, key),
         stream_key = {person, key, :origin},
         false <- not reconnect and Map.has_key?(state.origin_streams, stream_key) do
      proposed = put_in(state.origin_streams[stream_key], %{worker: worker})
      next = origin_availability_changes(state, proposed, person, key)

      case Store.commit(state.store, next, audit(state.store.engine, key, "origin_attach")) do
        {:ok, store} ->
          if previous = state.origin_streams[stream_key] do
            Process.demonitor(previous.monitor, [:flush])
            if previous.worker != worker, do: send(previous.worker, :origin_scope_replaced)
          end

          stream = %{worker: worker, monitor: Process.monitor(worker), advisory_v1: false}

          next_state = %{
            proposed
            | store: store,
              origin_streams: Map.put(proposed.origin_streams, stream_key, stream),
              origin_streams_by_identity:
                Map.update(
                  state.origin_streams_by_identity,
                  {person, key},
                  MapSet.new([stream_key]),
                  &MapSet.put(&1, stream_key)
                )
          }

          {:reply, :ok, requeue_covered(next_state, person, key)}

        _ ->
          {:reply, {:error, :storage_unavailable}, state}
      end
    else
      true -> {:reply, {:error, :already_listening}, state}
      error -> {:reply, error, state}
    end
  end

  def handle_call(
        {:origin_stream_attach, key, handle, worker, reconnect, advisory_requested},
        _from,
        state
      )
      when is_pid(worker) and is_boolean(reconnect) and is_boolean(advisory_requested) do
    engine = state.store.engine

    with {:ok, person} <- Engine.authorized_person(engine, key),
         {:ok, _} <- Engine.origin_status(engine, key, handle, false),
         false <- not reconnect and Map.has_key?(state.origin_streams, {person, key, handle}),
         {:ok, attached} <- Conversation.mark_attached(engine, person, key, handle),
         {:ok, current} <- Engine.origin_status(attached, key, handle, true),
         attached <-
           Engine.origin_availability(
             attached,
             person,
             key,
             handle,
             current["availability"],
             current["last_detached_at"]
           ),
         {:ok, store} <-
           Store.commit(state.store, attached, audit(engine, key, "origin_attach")) do
      stream_key = {person, key, handle}
      # Replace only after the attachment commit succeeds. The old worker's
      # eventual detach/DOWN must not remove this connection's registration.
      if previous = Map.get(state.origin_streams, stream_key) do
        Process.demonitor(previous.monitor, [:flush])
        if previous.worker != worker, do: send(previous.worker, :origin_stream_replaced)
      end

      monitor = Process.monitor(worker)

      streams =
        Map.put(state.origin_streams, stream_key, %{
          worker: worker,
          monitor: monitor,
          advisory_requested: advisory_requested,
          advisory_v1:
            advisory_requested and
              BounceDecision.advisory_supported?(attached, person, key, handle)
        })

      streams_by_identity =
        Map.update(
          state.origin_streams_by_identity,
          {person, key},
          MapSet.new([stream_key]),
          &MapSet.put(&1, stream_key)
        )

      store = %{
        store
        | engine: BounceDecision.requeue_supported_session(store.engine, stream_key)
      }

      next_state = %{
        state
        | store: store,
          origin_streams: streams,
          origin_streams_by_identity: streams_by_identity
      }

      next_state =
        next_state
        |> reschedule_bounce_sweep()

      {:reply, :ok, next_state}
    else
      true -> {:reply, {:error, :already_listening}, state}
      {:error, :not_found} = error -> {:reply, error, state}
      {:error, :forbidden} = error -> {:reply, error, state}
      _ -> {:reply, {:error, :storage_unavailable}, state}
    end
  end

  def handle_call({:origin_stream_detach, key, handle, worker}, _from, state) do
    case Enum.find(state.origin_streams, fn
           {{_person, ^key, ^handle}, %{worker: ^worker}} -> true
           _ -> false
         end) do
      {stream_key, _} -> {:reply, :ok, detach_origin_stream(state, stream_key)}
      nil -> {:reply, :ok, state}
    end
  end

  def handle_call({:origin_ack2, key, feed_seq, declaration}, _from, state) do
    case Engine.origin_ack2(state.store.engine, key, feed_seq, declaration) do
      {:ok, next, reply} ->
        case commit(state.store, next, audit(state.store.engine, key, "origin_ack2")) do
          {:ok, store} -> {:reply, {:ok, reply}, %{state | store: store}}
          _ -> {:reply, {:error, :storage_unavailable}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:origin_ack, key, feed_seq, reports}, _from, state) do
    case Engine.origin_ack(state.store.engine, key, feed_seq, reports) do
      {:ok, next, reply} ->
        case commit(state.store, next, audit(state.store.engine, key, "origin_ack")) do
          {:ok, store} -> {:reply, {:ok, reply}, %{state | store: store}}
          _ -> {:reply, {:error, :storage_unavailable}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:origin_file_use, key, feed_seq, name, outcome, op_id}, _from, state) do
    case Engine.origin_file_use(state.store.engine, key, feed_seq, name, outcome, op_id) do
      {:ok, next, reply} ->
        case commit(state.store, next, audit(state.store.engine, key, "origin_file_use")) do
          {:ok, store} -> {:reply, {:ok, reply}, %{state | store: store}}
          _ -> {:reply, {:error, :storage_unavailable}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:agent_send_request, key, revision, request, socket}, _from, state) do
    case Engine.agent_identity(state.store.engine, key) do
      {:ok, person, _id, ^revision} ->
        {reply, next_state} = agent_request(state, person, key, request)

        Diagnostics.response_error(
          :agent_tls,
          :request_dispatch,
          reply,
          Diagnostics.operation_id(request)
        )

        result = :ssl.send(socket, AttentionServer.JSON.encode!(reply) <> "\n")
        {:reply, result, next_state}

      _ ->
        {:reply, {:error, :forbidden}, state}
    end
  end

  def handle_call({:agent_send_https, key, revision, request, socket}, _from, state) do
    case Engine.agent_identity(state.store.engine, key) do
      {:ok, person, _id, ^revision} ->
        {reply, next_state} =
          case AgentWire.normalize(request) do
            {:ok, inner, ref} ->
              {result, updated} = agent_request(state, person, key, inner)
              {AgentWire.application_reply(result, inner, ref), updated}

            {:error, reason, field, ref} ->
              {AgentWire.error(reason, field, ref), state}
          end

        Diagnostics.response_error(
          :agent_https,
          :request_dispatch,
          reply,
          Diagnostics.operation_id(request)
        )

        result = :ssl.send(socket, AgentWire.http_response(200, reply))
        {:reply, result, next_state}

      _ ->
        {:reply, {:error, :forbidden}, state}
    end
  end

  def handle_call({:owner_register_agent, person, owner_key, id, agent_key}, _from, state),
    do:
      setup(
        state,
        Engine.owner_register_agent(state.store.engine, person, owner_key, id, agent_key),
        person,
        owner_key,
        "owner_agent_register"
      )

  def handle_call(
        {:owner_replace_agent_grants, person, owner_key, agent_key, revision, read, respond},
        _from,
        state
      ),
      do:
        setup(
          state,
          Engine.owner_replace_agent_grants(
            state.store.engine,
            person,
            owner_key,
            agent_key,
            revision,
            read,
            respond
          ),
          person,
          owner_key,
          "owner_agent_grants_replace"
        )

  def handle_call({:owner_revoke_agent, person, owner_key, agent_key}, _from, state),
    do:
      setup(
        state,
        Engine.owner_revoke_agent(state.store.engine, person, owner_key, agent_key),
        person,
        owner_key,
        "owner_agent_revoke"
      )

  def handle_call({:owner_list_agents, person, owner_key}, _from, state),
    do: {:reply, Engine.owner_list_agents(state.store.engine, person, owner_key), state}

  def handle_call({:owner_send_line, person, key, revision, socket, line}, _from, state) do
    result =
      case Engine.owner_revision(state.store.engine, person, key) do
        {:ok, ^revision} -> :ssl.send(socket, line)
        _ -> {:error, :forbidden}
      end

    {:reply, result, state}
  end

  def handle_call({:pending_enroll, certificate_der, label}, _from, state) do
    now = System.system_time(:millisecond)

    case generate_pending(state.store.engine, certificate_der, label, now, 8) do
      {:ok, next_engine, code, secret} ->
        case commit(state.store, next_engine, %{
               person: "unassigned",
               actor: "pending_submitter",
               action: "pending_enrollment"
             }) do
          {:ok, store} ->
            {:reply,
             {:ok,
              %{
                approval_code: code,
                status_secret: Base.encode16(secret, case: :lower),
                expires_at: next_engine.pending_enrollments[code].expires_at,
                status_expires_at: next_engine.pending_enrollments[code].status_expires_at
              }}, %{state | store: store}}

          {:error, _, _} ->
            {:reply, {:error, :storage_unavailable}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:owner_browser_sign_in_create, person, owner}, _from, state) do
    case issue_browser_sign_in(state, state.store.engine, person, owner, owner) do
      {:ok, response, next} -> {:reply, {:ok, response}, next}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:hosted_account_create, account_ref}, _from, state) do
    engine = state.store.engine
    test? = is_binary(state.hosted_test_account) and account_ref == state.hosted_test_account
    person = random_uuid_v4()
    owner_key = Base.encode16(:crypto.strong_rand_bytes(32), case: :lower)

    with {:ok, next, record} <-
           Engine.hosted_account_create(engine, account_ref, test?, person, owner_key),
         {:ok, next} <- hosted_label_for(next, record.person, test?, nil) do
      if next == engine do
        {:reply, {:ok, record.person}, state}
      else
        case commit(state.store, next, %{
               person: record.person,
               actor: "hosted_component",
               action: "hosted_account_create"
             }) do
          {:ok, store} ->
            next_state = ensure_priority_view_if_missing(%{state | store: store}, record.person)
            {:reply, {:ok, record.person}, next_state}
          _ -> {:reply, {:error, :storage_unavailable}, state}
        end
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:hosted_sign_in_create, person, label}, _from, state) do
    engine = state.store.engine

    with {:ok, record} <- hosted_record(engine, person),
         owner when is_binary(owner) <- Engine.hosted_owner_key(engine, person),
         true <- is_binary(label) || {:error, :invalid_label},
         true <- is_binary(state.dashboard_origin) || {:error, :no_public_origin},
         {:ok, labelled} <- hosted_label_for(engine, person, record.test, label),
         {:ok, response, next} <-
           issue_browser_sign_in(state, labelled, person, owner, "hosted_component") do
      {:reply, {:ok, Map.fetch!(response, "sign_in_url")}, next}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
      _ -> {:reply, {:error, :not_hosted}, state}
    end
  end

  def handle_call({:hosted_set_algorithm, person, algorithm}, _from, state) do
    case set_hosted_algorithm(state, person, algorithm) do
      {:ok, next} ->
        result = %{person_id: person, algorithm_id: algorithm}

        result =
          if algorithm == "jev-set" and
               match?(%{basis: "fallback"}, Map.get(next.priority_views, person)),
             do: Map.put(result, :status, "waiting"),
             else: result

        {:reply, {:ok, result}, next}

      {:error, reason, next} ->
        {:reply, {:error, reason}, next}
    end
  end

  def handle_call({:hosted_delivery_state, person, target_state}, _from, state) do
    case Engine.set_hosted_delivery_state(state.store.engine, person, target_state) do
      {:ok, next_engine, committed_state} ->
        result = %{person_id: person, state: committed_state}

        if next_engine == state.store.engine do
          {:reply, {:ok, result}, state}
        else
          case commit(state.store, next_engine, %{
                 person: person,
                 actor: "hosted_component",
                 action: "hosted_delivery_state"
               }) do
            {:ok, store} ->
              {:reply, {:ok, result}, %{state | store: store}}

            {:error, _, _} ->
              {:reply, {:error, :storage_unavailable}, state}
          end
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:hosted_test_reset, target_state}, _from, state) do
    engine = state.store.engine

    with true <- target_state in @hosted_test_states || {:error, :unknown_state},
         account_ref when is_binary(account_ref) <-
           state.hosted_test_account || {:error, :not_designated},
         %{person: person} <-
           Map.get(engine.hosted_accounts, account_ref) || {:error, :not_created},
         owner when is_binary(owner) <- Engine.hosted_owner_key(engine, person),
         {:ok, cleared, removed_keys} <- Engine.hosted_test_reset(engine, person),
         {:ok, staged, code} <- hosted_test_stage(cleared, person, owner, target_state, state),
         {:ok, store} <-
           commit(state.store, staged, %{
             person: person,
             actor: "hosted_component",
             action: "hosted_test_reset"
           }) do
      next = %{state | store: store}

      next =
        if pending_signature(engine, person) != pending_signature(staged, person),
          do: pending_set_changed(next, person),
          else: next

      fenced = Enum.reduce(removed_keys, next, &fence_sessions(&2, &1))
      result = %{state: target_state, person_id: person}
      result = if target_state == "device-waiting", do: Map.put(result, :code, code), else: result
      {:reply, {:ok, result}, fenced}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
      {:error, _, _} -> {:reply, {:error, :storage_unavailable}, state}
      _ -> {:reply, {:error, :not_created}, state}
    end
  end

  def handle_call({:consume_browser_sign_in, token}, _from, state) do
    with {:ok, hash} <- AttentionServer.BrowserSignIn.token_hash(token) do
      now = System.system_time(:millisecond)
      observed = AttentionServer.BrowserSignIn.observe_time(state.store.engine, hash, now)

      case commit(state.store, observed, %{
             person: "system",
             actor: "browser_sign_in_deadline",
             action: "browser_sign_in_clock_fence"
           }) do
        {:ok, store} ->
          fenced = %{state | store: store}

          case AttentionServer.BrowserSignIn.consume(store.engine, hash, now) do
            {:ok, next, principal} ->
              case commit(store, next, %{
                     person: principal.person_id,
                     actor: principal.owner_key,
                     action: "browser_sign_in_consume"
                   }) do
                {:ok, committed} ->
                  principal = with_hosted_label(principal, :display_name, next)
                  {:reply, {:ok, principal}, %{fenced | store: committed}}

                _ ->
                  {:reply, {:error, :storage_unavailable}, fenced}
              end

            error ->
              {:reply, error, fenced}
          end

        _ ->
          {:reply, {:error, :storage_unavailable}, state}
      end
    else
      _ -> {:reply, {:error, :invalid_sign_in}, state}
    end
  end

  def handle_call({:owner_invite_create, person, owner}, _from, state) do
    token = :crypto.strong_rand_bytes(32)
    hash = :crypto.hash(:sha256, token)
    now = System.system_time(:millisecond)

    case AttentionServer.GuidedJoin.create(
           state.store.engine,
           person,
           owner,
           hash,
           state.join_configuration,
           now
         ) do
      {:ok, next, invite} ->
        case commit(state.store, next, %{
               person: person,
               actor: owner,
               action: "owner_invite_create"
             }) do
          {:ok, store} ->
            response = AttentionServer.GuidedJoin.invitation_response(invite, token)
            {:reply, {:ok, response}, %{state | store: store}}

          _ ->
            {:reply, {:error, :storage_unavailable}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call(:dashboard_origin, _from, state),
    do: {:reply, state.dashboard_origin, state}

  def handle_call({:invitation_resolve, token}, _from, state) do
    with {:ok, hash} <- secret_hash(token) do
      now = System.system_time(:millisecond)
      engine = state.store.engine

      observed =
        case Map.get(engine.join_invitations, hash) do
          %{code: nil} -> AttentionServer.GuidedJoin.observe_time(engine, hash, now)
          _ -> engine
        end

      case commit(state.store, observed, %{
             person: "system",
             actor: "join_deadline",
             action: "join_clock_fence"
           }) do
        {:ok, store} ->
          result =
            case AttentionServer.GuidedJoin.resolve(
                   store.engine,
                   token,
                   state.join_configuration,
                   now
                 ) do
              {:ok, %{"person_id" => person} = response} ->
                {:ok, with_hosted_label(response, "account_label", store.engine, person)}

              other ->
                other
            end

          {:reply, result, %{state | store: store}}

        _ ->
          {:reply, {:error, :storage_unavailable}, state}
      end
    else
      _ -> {:reply, {:error, :unavailable}, state}
    end
  end

  def handle_call({:owner_invite_create, person, owner, request_id}, _from, state) do
    now = System.system_time(:millisecond)

    result =
      AttentionServer.GuidedJoin.create_for_request(
        state.store.engine,
        person,
        owner,
        request_id,
        state.join_configuration,
        now
      )

    case result do
      {:ok, next, invite, token} ->
        case commit(state.store, next, %{
               person: person,
               actor: owner,
               action: "owner_invite_create",
               operation_id: request_id
             }) do
          {:ok, store} ->
            response = AttentionServer.GuidedJoin.invitation_response(invite, token)
            {:reply, {:ok, response}, %{state | store: store}}

          _ ->
            {:reply, {:error, :storage_unavailable}, state}
        end

      {:error, reason, observed} ->
        case commit(state.store, observed, %{
               person: person,
               actor: owner,
               action: "invite_request_expired"
             }) do
          {:ok, store} -> {:reply, {:error, reason}, %{state | store: store}}
          _ -> {:reply, {:error, :storage_unavailable}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:invited_enroll, token, certificate, label, status_secret}, _from, state) do
    with {:ok, hash} <- secret_hash(token), {:ok, status_hash} <- secret_hash(status_secret) do
      now = System.system_time(:millisecond)
      observed = AttentionServer.GuidedJoin.observe_time(state.store.engine, hash, now)

      case commit(state.store, observed, %{
             person: "system",
             actor: "join_deadline",
             action: "join_clock_fence"
           }) do
        {:ok, store} ->
          fenced = %{state | store: store}

          case guided_pending(
                 store.engine,
                 hash,
                 certificate,
                 label,
                 status_hash,
                 state.join_configuration,
                 now,
                 8
               ) do
            {:ok, next, response} ->
              case commit(store, next, %{
                     person: next.join_invitations[hash].person,
                     actor: "invited_submitter",
                     action: "invited_pending_enrollment"
                   }) do
                {:ok, committed} -> {:reply, {:ok, response}, %{state | store: committed}}
                _ -> {:reply, {:error, :storage_unavailable}, fenced}
              end

            error ->
              {:reply, error, fenced}
          end

        _ ->
          {:reply, {:error, :storage_unavailable}, state}
      end
    else
      _ -> {:reply, {:error, :refused}, state}
    end
  end

  def handle_call({:pending_cancel, secret, code, key, deadline}, _from, state) do
    now = System.system_time(:millisecond)

    with {:ok, hash} <- secret_hash(secret),
         {:ok, fenced} <- fence_pending_time(state, {:secret, hash}, now) do
      case Engine.pending_cancel(fenced.store.engine, hash, code, key, deadline, now) do
        {:ok, next, reply} ->
          case commit(fenced.store, next, %{
                 person: "requester",
                 actor: "pending_submitter",
                 action: "pending_cancel"
               }) do
            {:ok, store} -> {:reply, {:ok, reply}, %{fenced | store: store}}
            _ -> {:reply, {:error, :storage_unavailable}, fenced}
          end

        error ->
          {:reply, error, fenced}
      end
    else
      {:error, :storage_unavailable} -> {:reply, {:error, :storage_unavailable}, state}
      _ -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:removal_identity, key}, _from, state) do
    reply =
      case Map.get(state.store.engine.devices, key) do
        %{person: person} -> {:ok, person}
        _ -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:device_remove, key}, _from, state) do
    case Engine.device_remove(state.store.engine, key) do
      {:ok, next, reply} ->
        next = revoke_origin_sessions(state, next, reply["person_id"], key)

        case commit(state.store, next, %{
               person: reply["person_id"],
               actor: key,
               action: "device_self_remove"
             }) do
          {:ok, store} ->
            next = %{state | store: store} |> remove_focus_device(key, reply["person_id"])
            {:reply, {:ok, reply}, fence_sessions(next, key)}

          _ ->
            {:reply, {:error, :storage_unavailable}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:owner_pending_decline, person, owner, code, key, deadline}, _from, state) do
    now = System.system_time(:millisecond)

    case fence_pending_time(state, {:code, code}, now) do
      {:ok, fenced} ->
        case Engine.pending_decline(fenced.store.engine, person, owner, code, key, deadline, now) do
          {:ok, next} ->
            case commit(fenced.store, next, %{
                   person: person,
                   actor: owner,
                   action: "owner_device_decline"
                 }) do
              {:ok, store} -> {:reply, :ok, %{fenced | store: store}}
              _ -> {:reply, {:error, :storage_unavailable}, fenced}
            end

          error ->
            {:reply, error, fenced}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:pending_status, secret_hex}, _from, state) when is_binary(secret_hex) do
    with {:ok, secret} <- Base.decode16(secret_hex, case: :lower),
         true <- byte_size(secret) == 32 do
      hash = :crypto.hash(:sha256, secret)
      now_ms = System.system_time(:millisecond)

      case fence_pending_time(state, {:secret, hash}, now_ms) do
        {:ok, fenced} ->
          {:reply, Engine.pending_status(fenced.store.engine, hash, now_ms), fenced}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    else
      _ -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:pending_status, _}, _from, state),
    do: {:reply, {:error, :not_found}, state}

  def handle_call({:pending_resume, secret_hex, cert_der}, _from, state) do
    with {:ok, hash} <- secret_hash(secret_hex) do
      now_ms = System.system_time(:millisecond)

      case fence_pending_time(state, {:secret, hash}, now_ms) do
        {:ok, fenced} ->
          case generate_resume(fenced.store.engine, hash, cert_der, now_ms, 8) do
            {:ok, next, reply} ->
              case commit(fenced.store, next, %{
                     person: Map.get(reply, "person_id", "requester"),
                     actor: "pending_submitter",
                     action: "pending_resume"
                   }) do
                {:ok, store} -> {:reply, {:ok, reply}, %{fenced | store: store}}
                _ -> {:reply, {:error, :storage_unavailable}, fenced}
              end

            error ->
              {:reply, error, fenced}
          end

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    else
      _ -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:owner_pending_lookup, person, key, code}, _from, state) do
    now_ms = System.system_time(:millisecond)

    case fence_pending_time(state, {:code, code}, now_ms) do
      {:ok, fenced} ->
        {:reply, Engine.pending_lookup(fenced.store.engine, person, key, code, now_ms), fenced}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(
        {:owner_pending_approve, person, key, code, expected_key, expected_expires_at, senders},
        _from,
        state
      ) do
    now_ms = System.system_time(:millisecond)

    case fence_pending_time(state, {:code, code}, now_ms) do
      {:ok, fenced} ->
        case Engine.pending_approve(
               fenced.store.engine,
               person,
               key,
               code,
               expected_key,
               expected_expires_at,
               senders,
               now_ms
             ) do
          {:ok, next_engine, device_key} ->
            case commit(fenced.store, next_engine, %{
                   person: person,
                   actor: key,
                   action: "owner_device_approve"
                 }) do
              {:ok, store} ->
                next_state = ensure_priority_view_if_missing(%{fenced | store: store}, person)
                {:reply, {:ok, device_key}, next_state}
              {:error, _, _} -> {:reply, {:error, :storage_unavailable}, fenced}
            end

          {:error, reason} ->
            {:reply, {:error, reason}, fenced}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:owner_revoke_device, person, owner_key, device_key}, _from, state) do
    case Engine.owner_revoke_device(state.store.engine, person, owner_key, device_key) do
      {:ok, next_engine} ->
        next_engine = revoke_origin_sessions(state, next_engine, person, device_key)

        case commit(state.store, next_engine, %{
               person: person,
               actor: owner_key,
               action: "owner_device_revoke"
             }) do
          {:ok, store} ->
            next = %{state | store: store} |> remove_focus_device(device_key, person)
            {:reply, :ok, fence_sessions(next, device_key)}

          {:error, _, _} ->
            {:reply, {:error, :storage_unavailable}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:owner_grant_sender, person, key, device, pattern}, _from, state),
    do:
      setup(
        state,
        Engine.owner_grant_sender(state.store.engine, person, key, device, pattern),
        person,
        key,
        "owner_sender_grant"
      )

  def handle_call({:owner_revoke_sender, person, key, device, pattern}, _from, state),
    do:
      setup(
        state,
        Engine.owner_revoke_sender(state.store.engine, person, key, device, pattern),
        person,
        key,
        "owner_sender_revoke"
      )

  def handle_call(
        {:owner_grant_enrollment_read, person, key, connector, grant_id, source, target},
        _from,
        state
      ),
      do:
        setup(
          state,
          Engine.owner_grant_enrollment_read(
            state.store.engine,
            person,
            key,
            connector,
            grant_id,
            source,
            target
          ),
          person,
          key,
          "owner_enrollment_read_grant"
        )

  def handle_call({:owner_revoke_enrollment_read, person, key, grant_id}, _from, state),
    do:
      setup(
        state,
        Engine.owner_revoke_enrollment_read(state.store.engine, person, key, grant_id),
        person,
        key,
        "owner_enrollment_read_revoke"
      )

  def handle_call(
        {:owner_grant_maintainer, person, key, grant_id, grantee, scopes},
        _from,
        state
      ),
      do:
        setup(
          state,
          Engine.owner_grant_maintainer(
            state.store.engine,
            person,
            key,
            grant_id,
            grantee,
            scopes,
            System.system_time(:millisecond)
          ),
          person,
          key,
          "owner_maintainer_grant"
        )

  def handle_call({:owner_revoke_maintainer, person, key, grant_id}, _from, state),
    do:
      setup(
        state,
        Engine.owner_revoke_maintainer(
          state.store.engine,
          person,
          key,
          grant_id,
          System.system_time(:millisecond)
        ),
        person,
        key,
        "owner_maintainer_revoke"
      )

  def handle_call({:operator_bootstrap, key}, _from, state) do
    person = random_uuid_v4()

    case Engine.operator_bootstrap(state.store.engine, person, key) do
      {:ok, next_engine} ->
        case commit(state.store, next_engine, %{
               person: person,
               actor: "local_operator",
               action: "owner_bootstrap"
             }) do
          {:ok, store} ->
            next_state = ensure_priority_view_if_missing(%{state | store: store}, person)
            {:reply, {:ok, person}, next_state}
          {:error, _, _} -> {:reply, {:error, :storage_unavailable}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:operator_recover, person, key}, _from, state),
    do:
      setup(
        state,
        Engine.operator_recover(state.store.engine, person, key),
        person,
        "local_operator",
        "owner_recovery"
      )

  def handle_call({:setup_person, person}, _from, state),
    do:
      setup(
        state,
        Engine.setup_person(state.store.engine, person),
        person,
        "local_setup",
        "person_setup"
      )

  def handle_call({:setup_device, person, key}, _from, state),
    do:
      setup(
        state,
        Engine.setup_device(state.store.engine, person, key),
        person,
        "local_setup",
        "device_setup"
      )

  def handle_call(
        {:setup_agent_fixture, person, id, key, senders},
        _from,
        %{agent_fixture_mode: true} = state
      ),
      do:
        setup(
          state,
          Engine.setup_agent_fixture(state.store.engine, person, id, key, senders),
          person,
          "local_fixture",
          "agent_fixture_setup"
        )

  def handle_call({:setup_agent_fixture, _, _, _, _}, _from, state),
    do: {:reply, {:error, :not_supported}, state}

  def handle_call({:revoke_agent_fixture, key}, _from, %{agent_fixture_mode: true} = state),
    do:
      setup(
        state,
        Engine.revoke_agent_fixture(state.store.engine, key),
        get_in(state.store.engine.agents, [key, :person]),
        "local_owner_fixture",
        "agent_fixture_revoke"
      )

  def handle_call({:revoke_agent_fixture, _}, _from, state),
    do: {:reply, {:error, :not_supported}, state}

  def handle_call({:fixture_agent_answer, key, frame}, _from, %{agent_fixture_mode: true} = state) do
    case Engine.fixture_agent_answer(state.store.engine, key, frame) do
      {:ok, next_engine, fields} ->
        person = get_in(state.store.engine.agents, [key, :person])

        case commit(state.store, next_engine, %{
               person: person,
               actor: key,
               action: "agent_fixture_answer"
             }) do
          {:ok, store} -> {:reply, {:ok, fields}, %{state | store: store}}
          {:error, _, _} -> {:reply, {:error, :storage_unavailable}, state}
        end

      {:error, reason, field, _terminal} ->
        {:reply, {:error, reason, field}, state}
    end
  end

  def handle_call({:fixture_agent_answer, _, _}, _from, state),
    do: {:reply, {:error, :not_supported}, state}

  def handle_call({:setup_sender, key, pattern}, _from, state) do
    person = person_of(state.store.engine, key)

    setup(
      state,
      Engine.setup_sender(state.store.engine, key, pattern),
      person,
      "local_setup",
      "sender_setup"
    )
  end

  def handle_call(
        {:grant_source_report, person, key, source, senders, delegation_ref},
        _from,
        %{source_fixture_mode: true} = state
      ) do
    setup(
      state,
      Engine.grant_source_report(
        state.store.engine,
        person,
        key,
        source,
        senders,
        delegation_ref
      ),
      person,
      "local_source_owner_fixture",
      "source_report_grant"
    )
  end

  def handle_call({:grant_source_report, _, _, _, _, _}, _from, state),
    do: {:reply, {:error, :not_supported}, state}

  def handle_call(
        {:revoke_source_report, person, key, source},
        _from,
        %{source_fixture_mode: true} = state
      ) do
    setup(
      state,
      Engine.revoke_source_report(state.store.engine, person, key, source),
      person,
      "local_source_owner_fixture",
      "source_report_revoke"
    )
  end

  def handle_call({:revoke_source_report, _, _, _}, _from, state),
    do: {:reply, {:error, :not_supported}, state}

  def handle_call(
        {:grant_enrollment_read_fixture, person, connector, grant_id, source, target, issuer},
        _from,
        %{enrollment_fixture_mode: true} = state
      ),
      do:
        setup(
          state,
          Engine.grant_enrollment_read(
            state.store.engine,
            person,
            connector,
            grant_id,
            source,
            target,
            issuer
          ),
          person,
          "local_owner_fixture",
          "enrollment_read_grant"
        )

  def handle_call({:grant_enrollment_read_fixture, _, _, _, _, _, _}, _from, state),
    do: {:reply, {:error, :not_supported}, state}

  def handle_call(
        {:revoke_enrollment_read_fixture, person, grant_id},
        _from,
        %{enrollment_fixture_mode: true} = state
      ),
      do:
        setup(
          state,
          Engine.revoke_enrollment_read(state.store.engine, person, grant_id),
          person,
          "local_owner_fixture",
          "enrollment_read_revoke"
        )

  def handle_call({:revoke_enrollment_read_fixture, _, _}, _from, state),
    do: {:reply, {:error, :not_supported}, state}

  def handle_call({:revoke_device, key}, _from, state) do
    person = person_of(state.store.engine, key)

    case Engine.revoke_device(state.store.engine, key) do
      {:ok, next_engine} ->
        next_engine = revoke_origin_sessions(state, next_engine, person, key)

        case commit(state.store, next_engine, %{
               person: person,
               actor: "local_setup",
               action: "device_revoke"
             }) do
          {:ok, store} ->
            next = %{state | store: store} |> remove_focus_device(key, person)
            {:reply, :ok, fence_sessions(next, key)}

          {:error, _, _} ->
            {:reply, {:error, :storage_unavailable}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:register_tls_listener, listener}, _from, state) when is_pid(listener) do
    if Map.has_key?(state.tls_listeners, listener) do
      {:reply, {:error, :already_registered}, state}
    else
      monitor = Process.monitor(listener)
      {:reply, :ok, %{state | tls_listeners: Map.put(state.tls_listeners, listener, monitor)}}
    end
  end

  def handle_call({:unregister_tls_listener, listener}, _from, state),
    do: {:reply, :ok, unregister_listener(state, listener)}

  def handle_call({:register_origin_listener, listener, port}, _from, state)
      when is_pid(listener) and is_integer(port) and port in 1..65_535 do
    if state.origin_listener do
      {:reply, {:error, :already_registered}, state}
    else
      monitor = Process.monitor(listener)
      {:reply, :ok, %{state | origin_listener: %{pid: listener, port: port, monitor: monitor}}}
    end
  end

  def handle_call({:unregister_origin_listener, listener}, _from, state) do
    {:reply, :ok, unregister_origin_listener_state(state, listener)}
  end

  def handle_call({:open_session, key, owner, socket, listener, transport}, _from, state) do
    admission =
      if is_nil(listener) or Map.has_key?(state.tls_listeners, listener),
        do: Engine.acknowledged_cursor(state.store.engine, key),
        else: {:error, :listener_stopped}

    case admission do
      {:ok, cursor} ->
        state = fence_sessions(state, key)
        id = make_ref()

        session = %{
          key: key,
          focus_opted_in: false,
          contiguous: cursor,
          stream_seq: nil,
          ready: false,
          conversation_seq: nil,
          conversation_contiguous: nil,
          conversation_ready: false,
          owner: owner,
          socket: socket,
          listener: listener,
          transport: transport,
          sync_mode: nil,
          snapshot_id: nil,
          replay_attach: nil,
          view_support: false,
          view_notice: nil,
          view_notice_due_at: nil,
          view_last_notice_at: nil,
          levels_support: false,
          levels_notice: false
        }

        next =
          %{state | sessions: Map.put(state.sessions, id, session)}
          |> set_focus_connected(key, true)

        {:reply, {:ok, id}, next}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call(
        {:open_browser_recipient, person, owner_key, owner_revision, browser_session_id,
         recipient, worker},
        _from,
        state
      ) do
    key = BrowserRecipient.key(person, browser_session_id, recipient)
    recipient_ref = {browser_session_id, recipient}

    with true <- is_pid(worker),
         true <- Message.uuid_v4?(recipient),
         true <- BrowserRecipient.valid_for?(key, person),
         {:ok, ^owner_revision} <-
           Engine.owner_revision(state.store.engine, person, owner_key) do
      state =
        case Map.get(state.browser_recipients, recipient_ref) do
          %{connection_id: old_id} when not is_nil(old_id) ->
            drop_browser_connection(state, old_id, :replaced)

          _ ->
            state
        end

      id = make_ref()

      session =
        case Map.get(state.browser_recipients, recipient_ref) do
          %{session: saved} -> saved
          _ -> empty_browser_session(key)
        end
        |> Map.merge(%{
          key: key,
          person: person,
          owner_key: owner_key,
          owner_revision: owner_revision,
          browser_session_id: browser_session_id,
          recipient: recipient,
          owner: worker,
          socket: nil,
          listener: nil,
          transport: :browser
        })

      recipients =
        Map.put(state.browser_recipients, recipient_ref, %{connection_id: id, session: session})

      {:reply, {:ok, id},
       %{state | sessions: Map.put(state.sessions, id, session), browser_recipients: recipients}}
    else
      _ -> {:reply, {:error, :forbidden}, state}
    end
  end

  def handle_call({:end_browser_session, browser_session_id}, _from, state) do
    state =
      state.sessions
      |> Enum.filter(fn {_id, session} ->
        session.transport == :browser and session.browser_session_id == browser_session_id
      end)
      |> Enum.reduce(state, fn {id, _session}, acc -> drop_session(acc, id) end)

    actor_keys =
      state.browser_recipients
      |> Enum.flat_map(fn
        {{^browser_session_id, _recipient}, %{session: %{key: key}}} -> [key]
        _ -> []
      end)

    actor_key_set = MapSet.new(actor_keys)

    active_uploads =
      Map.new(state.active_uploads, fn {lease, active} ->
        {_person, actor, _op_id, _sender, _message, _revision} = active.group

        {lease,
         %{
           active
           | canceled: active.canceled or MapSet.member?(actor_key_set, actor)
         }}
      end)

    state = %{state | active_uploads: active_uploads}

    case cleanup_browser_recipient_state(state, actor_keys) do
      {:ok, next} ->
        recipients =
          Map.reject(next.browser_recipients, fn {{session_id, _recipient}, _} ->
            session_id == browser_session_id
          end)

        {:reply, :ok, %{next | browser_recipients: recipients}}

      {:error, reason, next} ->
        recipients =
          Map.reject(next.browser_recipients, fn {{session_id, _recipient}, _} ->
            session_id == browser_session_id
          end)

        {:reply, {:error, reason}, %{next | browser_recipients: recipients}}
    end
  end

  def handle_call({:session_send, id, frames}, _from, state) do
    with {:ok, %{socket: socket, key: key, transport: transport}} <- active_session(state, id),
         true <- not is_nil(socket) and is_list(frames),
         :ok <-
           send_frames(
             socket,
             refresh_enrollment_frames(state.store.engine, key, frames),
             transport
           ) do
      {:reply, :ok, state}
    else
      _ ->
        {:reply, {:error, :fenced}, drop_session(state, id)}
    end
  end

  def handle_call({:close_session, id}, _from, state),
    do: {:reply, :ok, drop_session(state, id)}

  def handle_call({:session_request_line, id, raw}, from, state),
    do: session_request_line_call(state, id, raw, from)

  def handle_call(
        {:browser_session_request_line, browser_session_id, id, raw},
        from,
        state
      ) do
    case browser_connection(state, browser_session_id, id) do
      {:ok, _session} -> session_request_line_call(state, id, raw, from)
      {:error, :forbidden} -> {:reply, {:error, :session_ended}, drop_session(state, id)}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:session_drain, id}, _from, state),
    do: session_drain_call(state, id)

  def handle_call({:browser_session_drain, browser_session_id, id}, _from, state) do
    case browser_connection(state, browser_session_id, id) do
      {:ok, _session} -> session_drain_call(state, id)
      {:error, :forbidden} -> {:reply, {:error, :session_ended}, drop_session(state, id)}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:close_browser_connection, browser_session_id, id}, _from, state) do
    case Map.get(state.sessions, id) do
      %{transport: :browser, browser_session_id: ^browser_session_id} ->
        {:reply, :ok, drop_session(state, id)}

      _ ->
        {:reply, :ok, state}
    end
  end

  defp session_drain_call(state, id) do
    with {:ok, session} <- active_session(state, id),
         true <- session.ready,
         {:ok, events, high} <- Engine.events(state.store.engine, session.key, session.stream_seq),
         {:ok, conversation_events, conversation_high} <-
           drain_conversation(state.store.engine, session) do
      contiguous = if session.stream_seq <= session.contiguous, do: high, else: session.contiguous

      conversation_contiguous =
        if session.conversation_ready and
             session.conversation_seq <= session.conversation_contiguous,
           do: conversation_high,
           else: session.conversation_contiguous

      updated = %{
        session
        | stream_seq: high,
          contiguous: contiguous,
          conversation_seq: if(session.conversation_ready, do: conversation_high, else: nil),
          conversation_contiguous: conversation_contiguous
      }

      {view_notice, updated} = take_view_notice(updated, System.monotonic_time(:millisecond))
      {levels_notice, updated} = take_levels_notice(updated)
      next = put_session(state, id, updated)

      frames =
        Enum.map(events ++ conversation_events, &line/1) ++ view_notice ++ levels_notice

      {:reply, {:ok, frames}, next}
    else
      false ->
        case Map.get(state.sessions, id) do
          %{levels_notice: true} = session ->
            {notice, updated} = take_levels_notice(session)
            {:reply, {:ok, notice}, put_session(state, id, updated)}

          _ ->
            {:reply, {:error, :resync_required}, state}
        end

      {:error, :forbidden} ->
        {:reply, {:error, :forbidden}, drop_session(state, id)}

      error ->
        {:reply, error, state}
    end
  end

  defp discard_uploads_call(state, key, attrs) do
    with {:ok, person} <- Engine.authorized_person(state.store.engine, key),
         true <- valid_discard_attributes?(attrs) do
      scope = upload_request_scope(person, key, attrs)
      engine = state.store.engine
      references = Map.get(state.upload_index.references_by_person, person, MapSet.new())
      scoped_ids = Map.get(state.upload_index.ids_by_request_scope, scope, MapSet.new())

      ids =
        for id <- scoped_ids,
            not MapSet.member?(references, id),
            do: id

      committed? =
        Enum.any?(scoped_ids, &MapSet.member?(references, &1))

      active =
        Map.new(state.active_uploads, fn {lease, active} ->
          {lease,
           %{active | canceled: active.canceled or upload_request_scope(active.group) == scope}}
        end)

      case Store.delete_uploads(state.store, ids, %{
             person: person,
             actor: key,
             action: "upload_discard",
             operation_id: Diagnostics.operation_id(attrs)
           }) do
        {:ok, store} ->
          result = %{
            "state" => if(committed?, do: "committed", else: "discarded"),
            "discarded" => length(ids)
          }

          removed = removed_upload_ids(engine, store.engine, ids)
          index = remove_uploads_from_index(state.upload_index, engine, removed)

          {:reply, {:ok, result},
           %{state | store: store, upload_index: index, active_uploads: active}}

        {:error, reason, store} ->
          removed = removed_upload_ids(engine, store.engine, ids)
          index = remove_uploads_from_index(state.upload_index, engine, removed)

          {:reply, {:error, reason},
           %{state | store: store, upload_index: index, active_uploads: active}}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
      _ -> {:reply, {:error, :invalid_upload}, state}
    end
  end

  defp upload_prepare_call(state, key, attrs, size, hash, lease, context) do
    with {:ok, person} <- upload_authorized_person(state, key, context),
         true <- is_integer(size) and size >= 0,
         true <- state.upload_cap == :none or size <= state.upload_cap,
         true <- valid_upload_attributes?(attrs),
         :ok <- upload_lease_status(state, lease, upload_group(person, key, attrs)),
         true <- is_binary(hash) and Regex.match?(~r/\A[0-9a-f]{64}\z/, hash) do
      base = %{
        person: person,
        uploader: key,
        uploader_kind: upload_source(state, person, key, attrs, lease),
        op_id: attrs["op_id"],
        sender_id: attrs["sender_id"],
        message_id: attrs["message_id"],
        revision: attrs["revision"],
        name: attrs["name"],
        size: size,
        sha256: hash,
        last_activity_ms: System.system_time(:millisecond)
      }

      previous =
        state.upload_index.ids_by_group
        |> Map.get(upload_group(person, key, attrs), MapSet.new())
        |> Enum.find_value(fn id ->
          case Map.get(state.store.engine.uploads, id) do
            %{person: ^person, uploader: ^key, op_id: op_id, name: name} = upload
            when op_id == base.op_id and name == base.name ->
              {id, upload}

            _ ->
              nil
          end
        end)

      case previous do
        {_id, upload} ->
          if Map.drop(upload, [:id, :last_activity_ms, :uploader_kind]) ==
               Map.drop(base, [:last_activity_ms, :uploader_kind]) do
            {:reply, {:ok, :duplicate, upload_reply(upload, true)}, state}
          else
            {:reply, {:error, :upload_conflict}, state}
          end

        nil ->
          upload = Map.put(base, :id, ConversationFiles.new_upload_id())

          {:reply, {:ok, upload, state.store.upload_dir}, state}
      end
    else
      {:error, :upload_discarded} ->
        {:reply, {:error, :upload_discarded}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}

      false
      when is_integer(size) and is_integer(state.upload_cap) and
             size > state.upload_cap ->
        {:reply, {:error, :upload_cap_exceeded}, state}

      _ ->
        {:reply, {:error, :invalid_upload}, state}
    end
  end

  defp upload_commit_call(state, key, upload, published, lease, context) do
    result =
      with {:ok, person} <- upload_authorized_person(state, key, context),
           true <- person == upload.person and key == upload.uploader,
           :ok <- upload_lease_status(state, lease, upload_group(upload)),
           true <- state.upload_cap == :none or upload.size <= state.upload_cap do
        previous =
          state.upload_index.ids_by_group
          |> Map.get(upload_group(upload), MapSet.new())
          |> Enum.find_value(fn id ->
            case Map.get(state.store.engine.uploads, id) do
              %{person: ^person, uploader: ^key, op_id: op_id, name: name} = stored
              when op_id == upload.op_id and name == upload.name ->
                {id, stored}

              _ ->
                nil
            end
          end)

        case previous do
          {_id, stored} ->
            if Map.drop(stored, [:id, :last_activity_ms, :uploader_kind]) ==
                 Map.drop(upload, [:id, :last_activity_ms, :uploader_kind]) do
              {:reply, {:ok, upload_reply(stored, true)}, state}
            else
              {:reply, {:error, :upload_conflict}, state}
            end

          nil ->
            upload = %{upload | last_activity_ms: System.system_time(:millisecond)}

            case Store.put_published_upload(state.store, upload) do
              {:ok, store} ->
                index = add_upload_to_index(state.upload_index, upload)

                {:reply, {:ok, upload_reply(upload, false)},
                 %{state | store: store, upload_index: index}}

              {:error, reason} ->
                {:reply, {:error, reason}, state}
            end
        end
      else
        {:error, reason} -> {:reply, {:error, reason}, state}
        _ -> {:reply, {:error, :invalid_upload}, state}
      end

    # A canceled lease, competing duplicate, or failed metadata commit must not
    # leave newly published bytes addressable. The large rename ran in the
    # connection process; this unlink only removes an uncommitted target.
    case result do
      {:reply, {:ok, %{"duplicate" => false}}, _} ->
        result

      _ ->
        _ = File.rm(published)
        result
    end
  end

  defp session_request_line_call(state, id, raw, from) do
    with {:ok, session} <- active_session(state, id) do
      browser_type = browser_frame_type(raw)

      cond do
        session.transport == :browser and is_binary(browser_type) and
            browser_type not in @browser_frame_types ->
          {:reply, {:ok, [error_line(raw, "unknown_type")]}, state}

        true ->
          case JSON.decode_line(raw) do
            {:ok, %{"type" => "inbox_start"} = frame}
            when session.transport in [:websocket, :browser] ->
              inbox_start_call(state, id, session, frame, from)

            {:ok, %{"type" => "focus_report"} = frame} ->
              {reply, next} = focus_report_request(state, id, session, frame)
              {:reply, reply, next}

            _ ->
              {reply, next} = session_request(state, id, session, raw)
              {:reply, reply, next}
          end
      end
    else
      {:error, :forbidden} ->
        {:reply, {:error, :forbidden}, drop_session(state, id)}

      error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_info(:upload_sweep, state) do
    Process.send_after(self(), :upload_sweep, @upload_sweep_ms)

    case sweep_agent_uploads(state, System.system_time(:millisecond)) do
      {:ok, next, _count} ->
        {:noreply, next}

      {:error, reason, next} ->
        Diagnostics.failure(:upload_cleanup, :periodic_cleanup, reason, :unavailable)
        {:noreply, next}
    end
  end

  def handle_info(:refresh_default_sweep_index, state) do
    {:noreply, reschedule_default_sweep(state, Engine.default_index(state.store.engine))}
  end

  def handle_info(
        {:bounce_due, generation},
        %{bounce_deadline_generation: generation} = state
      ) do
    state = %{state | bounce_deadline_timer: nil}
    old_engine = state.store.engine
    now_ms = state.clock.()
    {next_engine, changed_keys} = BounceDecision.evaluate_due_with_changes(old_engine, now_ms)

    if changed_keys == [] do
      {:noreply, reschedule_bounce_sweep(state)}
    else
      case commit(state.store, next_engine, %{
             person: "system",
             actor: "bounce_decision",
             action: "bounce_due"
           }) do
        {:ok, store} ->
          next_state = %{state | store: store}

          next_state =
            record_advisory_feed_change(next_state, old_engine, next_engine, changed_keys)

          {:noreply, reschedule_bounce_sweep(next_state)}

        {:error, reason, _store} ->
          Diagnostics.failure(:bounce_schedule, :decision_commit, reason, :unavailable)
          {:noreply, schedule_bounce_retry(state)}

        other ->
          Diagnostics.failure(:bounce_schedule, :decision_commit, other, :unavailable)
          {:noreply, schedule_bounce_retry(state)}
      end
    end
  end

  def handle_info({:default_sweep, generation}, %{default_timer_generation: generation} = state) do
    state = %{state | default_timer: nil}
    old_engine = state.store.engine
    now_ms = state.clock.()

    due_index =
      state.default_index
      |> Enum.filter(fn {_key, at} -> is_integer(at) and at <= now_ms end)
      |> Map.new()

    next_engine = Engine.sweep_due_defaults(old_engine, now_ms, due_index)

    if next_engine == old_engine do
      if map_size(due_index) == 0,
        do: {:noreply, reschedule_default_sweep(state, state.default_index)},
        else: {:noreply, schedule_default_retry(state)}
    else
      case commit(state.store, next_engine, %{
             person: "system",
             actor: "default_answer",
             action: "default_answer_sweep"
           }) do
        {:ok, store} ->
          next_state = %{state | store: store}

          next_state =
            next_engine.origin_feeds
            |> Map.keys()
            |> Enum.reduce(next_state, fn target, current ->
              record_origin_feed_change(current, old_engine, next_engine, target)
            end)

          people = Map.keys(next_engine.persons)

          next_state =
            Enum.reduce(people, next_state, fn person, current ->
              if pending_signature(old_engine, person) != pending_signature(next_engine, person),
                do: pending_set_changed(current, person),
                else: current
            end)

          {:noreply, next_state}

        {:error, reason, next_store} ->
          Diagnostics.failure(:message_default, :default_sweep, reason, :unavailable)
          {:noreply, schedule_default_retry(%{state | store: next_store})}
      end
    end
  end

  def handle_info({:default_sweep, _stale_generation}, state), do: {:noreply, state}

  def handle_info({:prioritization_due, person, generation}, state) do
    case Map.get(state.priority_timers, person) do
      %{generation: ^generation} ->
        next = %{state | priority_timers: Map.delete(state.priority_timers, person)}

        if Map.has_key?(next.priority_jobs, person) do
          {:noreply, %{next | priority_queued: Map.put(next.priority_queued, person, generation)}}
        else
          {:noreply, start_priority_pass(next, person, generation)}
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:priority_retry, person, generation}, state) do
    case Map.get(state.priority_retry_timers, person) do
      %{generation: ^generation} ->
        state = %{state | priority_retry_timers: Map.delete(state.priority_retry_timers, person)}
        {:noreply, schedule_priority_pass(state, person)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:priority_deadline, person, due}, state) do
    case Map.get(state.priority_deadline_timers, person) do
      %{due: ^due} ->
        state = %{
          state
          | priority_deadline_timers: Map.delete(state.priority_deadline_timers, person)
        }

        {:noreply, schedule_priority_pass(state, person)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:prioritization_result, person, sequence, generation, result}, state) do
    case Map.get(state.priority_jobs, person) do
      %{sequence: ^sequence, generation: ^generation, monitor: monitor} = job ->
        Process.demonitor(monitor, [:flush])
        {:noreply, finish_priority_pass(state, person, job, result)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:focus_expire, device_key, generation}, state) do
    case Map.get(state.focus_expiry_generations, device_key) do
      ^generation ->
        state = %{state | focus_expiry_timers: Map.delete(state.focus_expiry_timers, device_key)}

        case Map.get(state.focus_clients, device_key) do
          %{connected: false, last_disconnected_at: disconnected_at} = focus
          when is_integer(disconnected_at) ->
            if state.clock.() - disconnected_at >= @focus_recent_ms do
              person =
                Map.get(focus, :person) || person_for_device(state.store.engine, device_key)

              next = delete_focus_client(state, device_key)
              {:noreply, if(person, do: notify_focus_streams(next, person), else: next)}
            else
              {:noreply, state}
            end

          _ ->
            {:noreply, state}
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor, :process, listener, reason}, state) do
    case Map.pop(state.inbox_build_monitors, monitor) do
      {nil, _monitors} ->
        priority_job =
          Enum.find(state.priority_jobs, fn {_person, job} -> job.monitor == monitor end)

        cond do
          priority_job ->
            {person, job} = priority_job
            {:noreply, finish_priority_pass(state, person, job, {:error, :unavailable})}

          active =
              Enum.find(state.active_uploads, fn {_lease, upload} ->
                upload.monitor == monitor
              end) ->
            {lease, upload} = active

            Diagnostics.failure(
              :origin_https,
              :upload_read,
              reason,
              :canceled,
              Diagnostics.operation_id(upload)
            )

            {:noreply, %{state | active_uploads: Map.delete(state.active_uploads, lease)}}

          match?(%{pid: ^listener, monitor: ^monitor}, state.origin_listener) ->
            Diagnostics.failure(:origin_https, :server_terminate, reason, :unavailable)
            {:noreply, %{state | origin_listener: nil}}

          Map.get(state.tls_listeners, listener) == monitor ->
            Diagnostics.failure(:device_relay, :server_terminate, reason, :unavailable)
            {:noreply, unregister_listener(state, listener)}

          match?(%{monitor: ^monitor}, Map.get(state.focus_streams, listener)) ->
            {:noreply, delete_focus_stream(state, listener)}

          true ->
            case Enum.find(state.origin_streams, fn {_, stream} ->
                   stream.worker == listener and stream.monitor == monitor
                 end) do
              {stream_key, _} ->
                Diagnostics.failure(:origin_https, :stream_detach, reason, :closed)
                {:noreply, detach_origin_stream(state, stream_key)}

              nil ->
                {:noreply, state}
            end
        end

      {build_key, monitors} ->
        state = %{state | inbox_build_monitors: monitors}
        {:noreply, fail_inbox_build(state, build_key, :snapshot_unavailable, false)}
    end
  end

  def handle_info({:inbox_snapshot_data, token, {:ok, counts}}, state) do
    case inbox_build_by_token(state, token) do
      {build_key, %{pid: pid} = build} ->
        send(pid, {:inbox_snapshot_continue, token})
        next = put_inbox_build(state, build_key, %{build | counts: counts})
        {:noreply, next}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info({:inbox_snapshot_data, token, {:error, reason}}, state) do
    case inbox_build_by_token(state, token) do
      {build_key, _build} ->
        {:noreply, fail_inbox_build(state, build_key, reason, true)}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info({:inbox_snapshot_chunk, token, kind, offset, values}, state) do
    case inbox_build_by_token(state, token) do
      {build_key, %{pid: pid, snapshot: snapshot}} ->
        case Store.save_inbox_snapshot_chunk(state.store, snapshot, kind, offset, values) do
          :ok ->
            send(pid, {:inbox_snapshot_continue, token})
            {:noreply, state}

          {:error, reason} ->
            send(pid, {:inbox_snapshot_abort, token, reason})
            {:noreply, fail_inbox_build(state, build_key, reason, true)}
        end

      nil ->
        {:noreply, state}
    end
  end

  def handle_info({:inbox_snapshot_done, token}, state) do
    case inbox_build_by_token(state, token) do
      {build_key, build} ->
        case Store.finish_inbox_snapshot(state.store, build.snapshot, build.counts) do
          :ok ->
            {:noreply, complete_inbox_build(state, build_key)}

          {:error, reason} ->
            {:noreply, fail_inbox_build(state, build_key, reason, true)}
        end

      nil ->
        {:noreply, state}
    end
  end

  defp detach_origin_stream(state, {person, origin, :origin} = key) do
    {stream, streams} = Map.pop(state.origin_streams, key)
    if stream, do: Process.demonitor(stream.monitor, [:flush])

    proposed = %{
      state
      | origin_streams: streams,
        origin_streams_by_identity:
          remove_origin_stream_index(state.origin_streams_by_identity, {person, origin}, key)
    }

    next = origin_availability_changes(state, proposed, person, origin)

    case Store.commit(state.store, next, audit(state.store.engine, origin, "origin_detach")) do
      {:ok, store} ->
        %{proposed | store: store}

      error ->
        Diagnostics.failure(:origin_https, :stream_detach, error, :unavailable)
        proposed
    end
  end

  defp detach_origin_stream(state, {person, origin, handle} = key) do
    {stream, streams} = Map.pop(state.origin_streams, key)
    if stream, do: Process.demonitor(stream.monitor, [:flush])

    streams_by_identity =
      remove_origin_stream_index(state.origin_streams_by_identity, {person, origin}, key)

    proposed =
      %{state | origin_streams: streams, origin_streams_by_identity: streams_by_identity}

    case Conversation.mark_detached(state.store.engine, person, origin, handle) do
      {:ok, next} ->
        proposed = %{proposed | store: %{state.store | engine: next}}

        previous = origin_session_status(state, state.store.engine, key)
        current = origin_session_status(proposed, next, key)

        next =
          case {previous, current} do
            {{:ok, %{"availability" => "gone"}}, {:ok, %{"availability" => "gone"}}} ->
              next

            {_, {:ok, status}} ->
              Engine.origin_availability(
                next,
                person,
                origin,
                handle,
                status["availability"],
                status["last_detached_at"]
              )

            _ ->
              next
          end

        case Store.commit(
               state.store,
               next,
               audit(state.store.engine, origin, "origin_detach")
             ) do
          {:ok, store} ->
            %{
              state
              | origin_streams: streams,
                origin_streams_by_identity: streams_by_identity,
                store: store
            }

          {:error, reason, _store} ->
            Diagnostics.failure(:origin_https, :stream_detach, reason, :unavailable)
            %{state | origin_streams: streams, origin_streams_by_identity: streams_by_identity}

          other ->
            Diagnostics.failure(:origin_https, :stream_detach, other, :unavailable)
            %{state | origin_streams: streams, origin_streams_by_identity: streams_by_identity}
        end

      {:error, reason} ->
        Diagnostics.failure(:origin_https, :stream_detach, reason, :unavailable)
        %{state | origin_streams: streams, origin_streams_by_identity: streams_by_identity}

      other ->
        Diagnostics.failure(:origin_https, :stream_detach, other, :unavailable)
        %{state | origin_streams: streams, origin_streams_by_identity: streams_by_identity}
    end
  end

  defp remove_origin_stream_index(index, identity, stream_key) do
    case Map.get(index, identity) do
      nil ->
        index

      streams ->
        remaining = MapSet.delete(streams, stream_key)

        if MapSet.size(remaining) == 0,
          do: Map.delete(index, identity),
          else: Map.put(index, identity, remaining)
    end
  end

  defp agent_request(state, _person, key, %{"v" => 1, "type" => "search"} = request) do
    case Engine.agent_search(state.store.engine, key, request) do
      {:ok, reply} -> {reply, state}
      {:error, reason} -> {%{"v" => 1, "error" => reason}, state}
    end
  end

  defp agent_request(state, _person, key, %{"v" => 1, "type" => "get"} = request) do
    case Engine.agent_get(state.store.engine, key, request) do
      {:ok, reply} -> {reply, state}
      {:error, reason} -> {%{"v" => 1, "error" => reason}, state}
    end
  end

  defp agent_request(state, person, key, %{"v" => 1, "type" => "answer"} = request) do
    case Engine.agent_answer(state.store.engine, key, request) do
      {:ok, next_engine, fields} ->
        target = origin_feed_target_for_person(next_engine, person, request)

        case commit(state.store, next_engine, %{
               person: person,
               actor: key,
               action: "agent_answer"
             }) do
          {:ok, store} ->
            next_state =
              record_origin_feed_change(
                %{state | store: store},
                state.store.engine,
                next_engine,
                target
              )

            next_state =
              if pending_signature(state.store.engine, person) !=
                   pending_signature(next_engine, person),
                 do: pending_set_changed(next_state, person),
                 else: next_state

            {%{"v" => 1, "type" => "answer_result", "ok" => true} |> Map.merge(fields),
             next_state}

          _ ->
            {%{"v" => 1, "error" => "storage_unavailable"}, state}
        end

      {:error, reason, field, terminal} ->
        reply = %{"v" => 1, "error" => reason}
        reply = if field, do: Map.put(reply, "field", field), else: reply
        reply = if terminal, do: Map.put(reply, "terminal", terminal), else: reply
        {reply, state}
    end
  end

  defp agent_request(state, _person, _key, _request),
    do: {%{"v" => 1, "error" => "invalid_request"}, state}

  defp active_session(state, id) do
    case Map.get(state.sessions, id) do
      nil ->
        {:error, :unknown_session}

      %{transport: :browser} = session ->
        with true <- BrowserRecipient.valid_for?(session.key, session.person),
             {:ok, revision} <-
               Engine.owner_revision(
                 state.store.engine,
                 session.person,
                 session.owner_key
               ),
             true <- revision == session.owner_revision do
          {:ok, session}
        else
          _ -> {:error, :forbidden}
        end

      session ->
        case Engine.authorized_person(state.store.engine, session.key) do
          {:ok, _} -> {:ok, session}
          error -> error
        end
    end
  end

  defp browser_connection(state, browser_session_id, id) do
    case Map.get(state.sessions, id) do
      %{transport: :browser, browser_session_id: ^browser_session_id} ->
        active_session(state, id)

      nil ->
        {:error, :unknown_session}

      _ ->
        {:error, :forbidden}
    end
  end

  defp browser_recipient_session(state, browser_session_id, recipient) do
    case Map.get(state.browser_recipients, {browser_session_id, recipient}) do
      %{session: %{transport: :browser} = session} ->
        with true <-
               session.key == BrowserRecipient.key(session.person, browser_session_id, recipient),
             true <- BrowserRecipient.valid_for?(session.key, session.person),
             {:ok, revision} <-
               Engine.owner_revision(state.store.engine, session.person, session.owner_key),
             true <- revision == session.owner_revision do
          {:ok, session}
        else
          _ -> {:error, :session_ended}
        end

      _ ->
        {:error, :forbidden}
    end
  end

  defp file_limits_reply(state, key) do
    case Engine.authorized_person(state.store.engine, key) do
      {:ok, _person} ->
        cap = if state.upload_cap == :none, do: "none", else: state.upload_cap

        {:reply,
         {:ok,
          %{
            "inline_decoded_bytes_per_entry" => ConversationFiles.inline_limit(),
            "upload_cap_bytes_per_file" => cap
          }}, state}

      _ ->
        {:reply, {:error, :forbidden}, state}
    end
  end

  defp begin_upload(state, person, key, attrs, _size, worker, browser_session_id) do
    with {:ok, store} <-
           touch_upload_group(
             state.store,
             state.upload_index,
             upload_group(person, key, attrs)
           ) do
      lease = make_ref()

      uploader_kind =
        Engine.upload_source(
          state.store.engine,
          person,
          key,
          attrs["sender_id"],
          attrs["message_id"]
        )
        |> Atom.to_string()

      monitor = Process.monitor(worker)

      active =
        Map.put(state.active_uploads, lease, %{
          group: upload_group(person, key, attrs),
          uploader_kind: uploader_kind,
          browser_session_id: browser_session_id,
          canceled: false,
          monitor: monitor
        })

      {:ok, lease, %{state | store: store, active_uploads: active}}
    end
  end

  defp upload_authorized_person(state, key, :device),
    do: Engine.authorized_person(state.store.engine, key)

  defp upload_authorized_person(state, key, {:browser, browser_session_id, recipient}) do
    case browser_recipient_session(state, browser_session_id, recipient) do
      {:ok, %{key: ^key, person: person}} -> {:ok, person}
      {:ok, _other} -> {:error, :forbidden}
      {:error, reason} -> {:error, reason}
    end
  end

  defp view_get(state, id, session, frame) do
    cond do
      frame["v"] != 1 or not is_integer(frame["v"]) ->
        {{:ok, [error_line(frame, "unsupported_version")]}, state}

      not is_binary(frame["ref"]) ->
        {{:ok, [error_line(frame, "invalid_field", "ref")]}, state}

      Enum.any?(Map.keys(frame), &(&1 not in ~w(v type ref known_view_rev))) ->
        {{:ok, [error_line(frame, "invalid_field")]}, state}

      Map.has_key?(frame, "known_view_rev") and not is_binary(frame["known_view_rev"]) ->
        {{:ok, [error_line(frame, "invalid_field", "known_view_rev")]}, state}

      true ->
        person = person_of(state.store.engine, session.key)
        state = ensure_priority_view(state, person)
        view = Map.fetch!(state.priority_views, person)
        updated = %{session | view_support: true}
        state = put_session(state, id, updated)

        reply =
          if frame["known_view_rev"] == view.view_rev do
            %{
              "v" => 1,
              "type" => "ok",
              "ref" => frame["ref"],
              "view_rev" => view.view_rev,
              "unchanged" => true
            }
          else
            %{
              "v" => 1,
              "type" => "ok",
              "ref" => frame["ref"],
              "view_rev" => view.view_rev,
              "algorithm" => view.algorithm,
              "basis" => view.basis,
              "entries" => view.entries,
              "replies" => view.replies,
              "list_items" => view.list_items
            }
            |> then(fn reply ->
              if Map.has_key?(view, :free_order_version),
                do: Map.put(reply, "free_order_version", view.free_order_version),
                else: reply
            end)
          end

        {{:ok, [line(reply)]}, state}
    end
  end

  defp session_request(state, id, session, raw) do
    case JSON.decode_line(raw) do
      {:ok, %{"type" => type} = frame}
      when type in [
             "inbox_start",
             "inbox_page",
             "inbox_reconcile",
             "inbox_receipts",
             "inbox_checkpoint_ack",
             "inbox_attach"
           ] ->
        inbox_session_request(state, id, session, frame)

      {:ok, %{"type" => type} = frame}
      when type in [
             "resync",
             "ack",
             "conversation_open",
             "conversation_ack",
             "view_get",
             "inbox_list",
             "conversation_page"
           ] ->
        special_request(state, id, session, frame)

      {:ok, %{"type" => type}} when type in ["levels_get", "levels_set"] ->
        levels_session_request(state, id, session, raw)

      {:ok, %{"type" => type} = frame} when type in ["forward", "answer_receipt"] ->
        {{:ok, [error_line(frame, "unknown_type")]}, state}

      _ ->
        case session_mutation_state(state, session, raw) do
          {:blocked, reason} ->
            {{:ok, [error_line(raw, reason)]}, state}

          :allowed ->
            {reply, next} = request(state, session.key, raw)
            {wrap_line(reply), next}
        end
    end
  end

  defp browser_frame_type(raw) do
    case JSON.decode_line(raw) do
      {:ok, %{"type" => type}} when is_binary(type) -> type
      _ -> nil
    end
  end

  defp empty_browser_session(key) do
    %{
      key: key,
      focus_opted_in: false,
      contiguous: 0,
      stream_seq: nil,
      ready: false,
      conversation_seq: nil,
      conversation_contiguous: nil,
      conversation_ready: false,
      owner: nil,
      socket: nil,
      listener: nil,
      transport: :browser,
      sync_mode: nil,
      snapshot_id: nil,
      replay_attach: nil,
      view_support: false,
      view_notice: nil,
      view_notice_due_at: nil,
      view_last_notice_at: nil,
      levels_support: false,
      levels_notice: false
    }
  end

  defp levels_session_request(state, id, session, raw) do
    session = %{session | levels_support: true}
    state = put_session(state, id, session)

    case session_mutation_state(state, session, raw) do
      {:blocked, reason} ->
        {{:ok, [error_line(raw, reason)]}, state}

      :allowed ->
        {reply, next} = request(state, session.key, raw)
        {wrap_line(reply), next}
    end
  end

  defp special_request(state, id, session, frame) do
    case envelope_error(frame) do
      nil ->
        case frame["type"] do
          "resync" when session.sync_mode == :paging ->
            {{:ok, [error_line(frame, "sync_mode_conflict")]}, state}

          "resync" ->
            legacy_session = %{session | sync_mode: :legacy}
            resync(put_session(state, id, legacy_session), id, legacy_session, frame)

          "ack" when session.sync_mode == :paging ->
            case paging_ack_state(state, session) do
              :ready -> ack(state, session, frame)
              {:blocked, reason} -> {{:ok, [error_line(frame, reason)]}, state}
            end

          "ack" ->
            case session_mutation_state(state, session, JSON.encode!(frame)) do
              {:blocked, reason} -> {{:ok, [error_line(frame, reason)]}, state}
              :allowed -> ack(state, session, frame)
            end

          "conversation_open" ->
            case frame["bounded"] do
              true -> conversation_open_bounded(state, id, session, frame)
              value when is_nil(value) or is_boolean(value) ->
                conversation_open(state, id, session, frame)

              _ ->
                {{:ok, [error_line(frame, "invalid_field", "bounded")]}, state}
            end

          "conversation_ack" ->
            conversation_ack(state, session, frame)

          "view_get" ->
            view_get(state, id, session, frame)

          "inbox_list" ->
            inbox_list(state, session, frame)

          "conversation_page" ->
            conversation_page(state, session, frame)
        end

      {reason, field} ->
        {{:ok, [error_line(frame, reason, field)]}, state}
    end
  end

  defp inbox_session_request(state, id, session, frame) do
    cond do
      session.transport not in [:websocket, :browser] ->
        {{:ok, [error_line(frame, "unknown_type")]}, state}

      session.sync_mode == :legacy ->
        {{:ok, [error_line(frame, "sync_mode_conflict")]}, state}

      true ->
        paging_session = %{session | sync_mode: :paging}
        state = put_session(state, id, paging_session)

        case frame["type"] do
          "inbox_start" -> inbox_start(state, id, paging_session, frame)
          "inbox_page" -> inbox_page(state, id, paging_session, frame)
          "inbox_reconcile" -> inbox_reconcile(state, id, paging_session, frame)
          "inbox_receipts" -> inbox_receipts(state, id, paging_session, frame)
          "inbox_checkpoint_ack" -> inbox_checkpoint_ack(state, id, paging_session, frame)
          "inbox_attach" -> inbox_attach(state, id, paging_session, frame)
        end
    end
  end

  defp inbox_start_call(state, id, session, frame, from) do
    cond do
      session.ready or session.sync_mode == :legacy ->
        {:reply, {:ok, [error_line(frame, "sync_mode_conflict")]}, state}

      true ->
        person = person_of(state.store.engine, session.key)
        state = ensure_priority_view(state, person)
        view_order = Enum.map(state.priority_views[person].entries, &Placement.identity/1)

        case InboxPaging.start_plan(
               state.store,
               state.store.engine,
               session.key,
               frame,
               view_order
             ) do
          {:ready, _snapshot} ->
            {reply, next} = inbox_start(state, id, session, frame)
            {:reply, reply, next}

          {:build, plan} ->
            begin_inbox_build(state, id, session, frame, from, plan)

          {:building, snapshot} ->
            key = inbox_build_key(snapshot)

            case Map.get(state.inbox_builds, key) do
              nil ->
                case Store.discard_inbox_snapshot(state.store, snapshot) do
                  :ok ->
                    inbox_start_call(state, id, session, frame, from)

                  {:error, reason} ->
                    {reply, next} = paging_error(state, frame, reason)
                    {:reply, reply, next}
                end

              build ->
                join_inbox_build(state, id, session, frame, from, key, build)
            end

          {:error, reason} ->
            {reply, next} = paging_error(state, frame, reason)
            {:reply, reply, next}
        end
    end
  end

  defp begin_inbox_build(state, id, session, frame, from, plan) do
    if session.snapshot_id do
      {:reply, {:ok, [error_line(frame, "sync_mode_conflict")]}, state}
    else
      begin_inbox_build_without_snapshot(state, id, session, frame, from, plan)
    end
  end

  defp begin_inbox_build_without_snapshot(state, id, session, frame, from, plan) do
    case Store.begin_inbox_snapshot(state.store, plan) do
      {:ok, snapshot} ->
        build_key = inbox_build_key(snapshot)
        token = make_ref()
        parent = self()
        engine = state.store.engine

        snapshot_test_gate =
          if state.enrollment_fixture_mode,
            do: Map.get(state, :inbox_snapshot_test_gate),
            else: nil

        {pid, monitor} =
          spawn_monitor(fn ->
            inbox_snapshot_worker(parent, token, engine, session.key, plan, snapshot_test_gate)
          end)

        waiter = {from, id, frame}

        build = %{
          token: token,
          pid: pid,
          monitor: monitor,
          snapshot: snapshot,
          counts: nil,
          waiters: [waiter]
        }

        updated = %{session | sync_mode: :paging, snapshot_id: snapshot.snapshot_id}

        next =
          state
          |> put_session(id, updated)
          |> put_inbox_build(build_key, build)
          |> put_inbox_build_monitor(monitor, build_key)

        {:noreply, next}

      {:error, reason} ->
        {reply, next} = paging_error(state, frame, reason)
        {:reply, reply, next}
    end
  end

  defp join_inbox_build(state, id, session, frame, from, build_key, build) do
    if session.snapshot_id && session.snapshot_id != build.snapshot.snapshot_id do
      {:reply, {:ok, [error_line(frame, "sync_mode_conflict")]}, state}
    else
      updated = %{session | sync_mode: :paging, snapshot_id: build.snapshot.snapshot_id}
      waiter = {from, id, frame}
      build = %{build | waiters: [waiter | build.waiters]}

      next =
        state
        |> put_session(id, updated)
        |> put_inbox_build(build_key, build)

      {:noreply, next}
    end
  end

  defp inbox_snapshot_worker(relay, token, engine, key, plan, test_gate) do
    relay_monitor = Process.monitor(relay)

    try do
      case await_inbox_snapshot_test_gate(test_gate, relay_monitor, token) do
        :ok ->
          case InboxPaging.snapshot_data(engine, key, plan) do
            {:ok, data} ->
              counts = %{
                items: length(data.items),
                receipts: length(data.receipts),
                operation_ids: length(data.operation_ids)
              }

              send(relay, {:inbox_snapshot_data, token, {:ok, counts}})

              with :ok <- await_inbox_snapshot_continue(relay_monitor, token),
                   :ok <- stream_snapshot_chunks(relay, relay_monitor, token, :items, data.items),
                   :ok <-
                     stream_snapshot_chunks(relay, relay_monitor, token, :receipts, data.receipts),
                   :ok <-
                     stream_snapshot_chunks(
                       relay,
                       relay_monitor,
                       token,
                       :operation_ids,
                       data.operation_ids
                     ) do
                send(relay, {:inbox_snapshot_done, token})
              else
                {:error, reason} -> send(relay, {:inbox_snapshot_data, token, {:error, reason}})
              end

            {:error, reason} ->
              send(relay, {:inbox_snapshot_data, token, {:error, reason}})
          end

        {:error, reason} ->
          send(relay, {:inbox_snapshot_data, token, {:error, reason}})
      end
    rescue
      _ -> send(relay, {:inbox_snapshot_data, token, {:error, :snapshot_unavailable}})
    catch
      _, _ -> send(relay, {:inbox_snapshot_data, token, {:error, :snapshot_unavailable}})
    after
      Process.demonitor(relay_monitor, [:flush])
    end
  end

  defp await_inbox_snapshot_test_gate(nil, _relay_monitor, _token), do: :ok

  defp await_inbox_snapshot_test_gate(test_gate, relay_monitor, token) when is_pid(test_gate) do
    send(test_gate, {:inbox_snapshot_worker_waiting, self(), token})

    receive do
      {:continue_inbox_snapshot, ^token} -> :ok
      {:DOWN, ^relay_monitor, :process, _relay, _reason} -> {:error, :relay_unavailable}
    end
  end

  defp stream_snapshot_chunks(relay, relay_monitor, token, kind, values) do
    values
    |> Stream.chunk_every(20)
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {chunk, page}, :ok ->
      send(relay, {:inbox_snapshot_chunk, token, kind, page * 20, chunk})

      case await_inbox_snapshot_continue(relay_monitor, token) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp await_inbox_snapshot_continue(relay_monitor, token) do
    receive do
      {:inbox_snapshot_continue, ^token} ->
        :ok

      {:inbox_snapshot_abort, ^token, reason} ->
        {:error, reason}

      {:DOWN, ^relay_monitor, :process, _relay, _reason} ->
        {:error, :relay_unavailable}
    end
  end

  defp complete_inbox_build(state, build_key) do
    case Map.pop(state.inbox_builds, build_key) do
      {nil, _builds} ->
        state

      {build, builds} ->
        Process.demonitor(build.monitor, [:flush])

        state = %{
          state
          | inbox_builds: builds,
            inbox_build_monitors: Map.delete(state.inbox_build_monitors, build.monitor)
        }

        Enum.reduce(build.waiters, state, fn {from, id, frame}, acc ->
          case Map.get(acc.sessions, id) do
            nil ->
              GenServer.reply(from, {:error, :forbidden})
              acc

            session ->
              {reply, next} = inbox_start(acc, id, session, frame)
              GenServer.reply(from, reply)
              next
          end
        end)
    end
  end

  defp fail_inbox_build(state, build_key, reason, notify_worker?) do
    case Map.pop(state.inbox_builds, build_key) do
      {nil, _builds} ->
        state

      {build, builds} ->
        if notify_worker?, do: send(build.pid, {:inbox_snapshot_abort, build.token, reason})
        Process.demonitor(build.monitor, [:flush])
        _ = Store.discard_inbox_snapshot(state.store, build.snapshot)

        state = %{
          state
          | inbox_builds: builds,
            inbox_build_monitors: Map.delete(state.inbox_build_monitors, build.monitor)
        }

        Enum.reduce(build.waiters, state, fn {from, id, frame}, acc ->
          acc = reset_building_session(acc, id, build.snapshot.snapshot_id)
          {reply, next} = paging_error(acc, frame, reason)
          GenServer.reply(from, reply)
          next
        end)
    end
  end

  defp reset_building_session(state, id, snapshot_id) do
    case Map.get(state.sessions, id) do
      %{snapshot_id: ^snapshot_id} = session ->
        put_session(state, id, %{session | sync_mode: nil, snapshot_id: nil})

      _ ->
        state
    end
  end

  defp inbox_build_by_token(state, token) do
    Enum.find_value(state.inbox_builds, fn {key, build} ->
      if build.token == token, do: {key, build}
    end)
  end

  defp inbox_build_key(snapshot),
    do: {snapshot.person_id, snapshot.device_id, snapshot.startup_id}

  defp put_inbox_build(state, key, build),
    do: %{state | inbox_builds: Map.put(state.inbox_builds, key, build)}

  defp put_inbox_build_monitor(state, monitor, key),
    do: %{state | inbox_build_monitors: Map.put(state.inbox_build_monitors, monitor, key)}

  defp inbox_start(state, id, session, frame) do
    cond do
      session.ready ->
        {{:ok, [error_line(frame, "sync_mode_conflict")]}, state}

      true ->
        case InboxPaging.start(state.store, state.store.engine, session.key, frame) do
          {:ok, reply, snapshot, _created?} ->
            if session.snapshot_id && session.snapshot_id != snapshot.snapshot_id do
              {{:ok, [error_line(frame, "sync_mode_conflict")]}, state}
            else
              updated = %{session | sync_mode: :paging, snapshot_id: snapshot.snapshot_id}
              {{:ok, [line(reply)]}, put_session(state, id, updated)}
            end

          {:error, reason} ->
            paging_error(state, frame, reason)
        end
    end
  end

  defp inbox_page(state, id, session, frame) do
    with :ok <- session_snapshot(session, frame),
         {:ok, reply, _snapshot} <-
           InboxPaging.page(state.store, state.store.engine, session.key, frame) do
      updated = %{session | sync_mode: :paging}
      {{:ok, [line(reply)]}, put_session(state, id, updated)}
    else
      {:error, reason} -> paging_error(state, frame, reason)
    end
  end

  defp inbox_reconcile(state, id, session, frame) do
    with :ok <- session_snapshot(session, frame) do
      case InboxPaging.reconcile(state.store, state.store.engine, session.key, frame) do
        {:ok, reply, _snapshot, store, applied} ->
          updated = %{session | sync_mode: :paging}
          next_state = reconciled_state(state, store, session.key, applied)
          {{:ok, [line(reply)]}, put_session(next_state, id, updated)}

        {:error, reason, store, applied} ->
          paging_error(reconciled_state(state, store, session.key, applied), frame, reason)

        {:error, reason} ->
          paging_error(state, frame, reason)
      end
    else
      {:error, reason} -> paging_error(state, frame, reason)
    end
  end

  # A reconciliation can now commit a No comment terminal. Preserve the same
  # origin-feed and pending-view notifications as an ordinary device request.
  defp reconciled_state(state, store, key, applied) do
    next = %{state | store: store}

    targets =
      applied
      |> Enum.map(&origin_feed_target(store.engine, key, &1))
      |> Enum.uniq()

    next =
      Enum.reduce(targets, next, fn target, current ->
        record_origin_feed_change(current, state.store.engine, store.engine, target)
      end)

    case Engine.authorized_person(state.store.engine, key) do
      {:ok, person} ->
        if pending_signature(state.store.engine, person) !=
             pending_signature(store.engine, person),
           do: pending_set_changed(next, person),
           else: next

      _ ->
        next
    end
  end

  defp inbox_receipts(state, id, session, frame) do
    with :ok <- session_snapshot(session, frame),
         {:ok, reply, _snapshot} <-
           InboxPaging.receipts(state.store, state.store.engine, session.key, frame) do
      updated = %{session | sync_mode: :paging}
      {{:ok, [line(reply)]}, put_session(state, id, updated)}
    else
      {:error, reason} -> paging_error(state, frame, reason)
    end
  end

  defp inbox_checkpoint_ack(state, id, session, frame) do
    with :ok <- session_snapshot(session, frame),
         {:ok, reply, store, _snapshot} <-
           InboxPaging.checkpoint_ack(state.store, state.store.engine, session.key, frame) do
      updated = %{session | sync_mode: :paging}
      {{:ok, [line(reply)]}, put_session(%{state | store: store}, id, updated)}
    else
      {:error, reason, store} -> paging_error(%{state | store: store}, frame, reason)
      {:error, reason} -> paging_error(state, frame, reason)
    end
  end

  defp inbox_attach(state, id, session, frame) do
    cond do
      session.snapshot_id && session.snapshot_id != frame["snapshot_id"] ->
        {{:ok, [error_line(frame, "invalid_snapshot")]}, state}

      frame["mode"] == "replay" && is_map(session.replay_attach) ->
        cached = session.replay_attach

        if cached.snapshot_id == frame["snapshot_id"] and cached.since == frame["since"] do
          {{:ok, replay_frames_with_ref(cached.frames, frame)}, state}
        else
          {{:ok, [error_line(frame, "sync_mode_conflict")]}, state}
        end

      true ->
        case InboxPaging.attach(state.store, state.store.engine, session.key, frame) do
          {:ok, %{reply: reply, events: events, barrier: barrier, since: since}, snapshot} ->
            frames =
              [line(reply)] ++
                Enum.map(events, &line/1) ++
                [line(%{"v" => 1, "type" => "resynced", "seq" => barrier})]

            updated = %{
              session
              | sync_mode: :paging,
                snapshot_id: snapshot.snapshot_id,
                contiguous: barrier,
                stream_seq: barrier,
                ready: true,
                replay_attach: %{
                  snapshot_id: snapshot.snapshot_id,
                  since: since,
                  frames: frames
                }
            }

            {{:ok, frames}, put_session(state, id, updated)}

          {:ok, reply, snapshot} when is_map(reply) ->
            updated = %{session | sync_mode: :paging, snapshot_id: snapshot.snapshot_id}
            {{:ok, [line(reply)]}, put_session(state, id, updated)}

          {:error, reason} ->
            paging_error(state, frame, reason)
        end
    end
  end

  defp session_snapshot(%{sync_mode: mode, snapshot_id: snapshot_id}, frame) do
    if mode == :paging and is_binary(snapshot_id) and frame["snapshot_id"] == snapshot_id,
      do: :ok,
      else: {:error, :invalid_snapshot}
  end

  defp replay_frames_with_ref([first | rest], frame) do
    case JSON.decode_line(first) do
      {:ok, reply} ->
        reply =
          if is_binary(frame["ref"]),
            do: Map.put(reply, "ref", frame["ref"]),
            else: Map.delete(reply, "ref")

        [line(reply) | rest]

      _ ->
        [first | rest]
    end
  end

  defp replay_frames_with_ref(frames, _frame), do: frames

  defp paging_ack_state(state, %{ready: true, snapshot_id: snapshot_id} = session)
       when is_binary(snapshot_id) do
    case Store.inbox_checkpoint_status(
           state.store,
           person_of(state.store.engine, session.key),
           session.key,
           snapshot_id
         ) do
      {:ok, %{status: "pending"}} -> {:blocked, "checkpoint_pending"}
      {:ok, %{status: "building"}} -> {:blocked, "checkpoint_pending"}
      {:ok, %{status: "accepted"}} -> :ready
      {:ok, %{status: "replaced"}} -> {:blocked, "snapshot_replaced"}
      {:error, reason} -> {:blocked, wire_reason(reason)}
    end
  end

  defp paging_ack_state(_state, _session), do: {:blocked, "checkpoint_pending"}

  defp session_mutation_state(state, session, raw) do
    case JSON.decode_line(raw) do
      {:ok, %{"type" => type}} when is_binary(type) ->
        if type in InboxPaging.mutation_types() do
          pending_mutation_state(state, session)
        else
          :allowed
        end

      _ ->
        :allowed
    end
  end

  defp pending_mutation_state(state, session) do
    if session.sync_mode == :paging and is_binary(session.snapshot_id) do
      case Store.inbox_checkpoint_status(
             state.store,
             person_of(state.store.engine, session.key),
             session.key,
             session.snapshot_id
           ) do
        {:ok, %{status: "pending"}} -> {:blocked, "checkpoint_pending"}
        {:ok, %{status: "building"}} -> {:blocked, "checkpoint_pending"}
        {:ok, %{status: status}} when status in ["accepted", "replaced"] -> :allowed
        {:error, reason} -> {:blocked, wire_reason(reason)}
        _ -> {:blocked, "snapshot_unavailable"}
      end
    else
      :allowed
    end
  end

  defp paging_error(state, frame, {:invalid_field, field}),
    do: {{:ok, [error_line(frame, "invalid_field", field)]}, state}

  defp paging_error(state, frame, reason),
    do: {{:ok, [error_line(frame, wire_reason(reason))]}, state}

  defp wire_reason(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp wire_reason({:invalid_field, _}), do: "invalid_field"
  defp wire_reason(_), do: "storage_unavailable"

  defp resync(state, id, session, frame) do
    case Engine.events(state.store.engine, session.key, frame["since"]) do
      {:ok, events, high} ->
        contiguous = if frame["since"] <= session.contiguous, do: high, else: session.contiguous

        updated = %{
          session
          | contiguous: contiguous,
            stream_seq: high,
            ready: true,
            sync_mode: :legacy
        }

        frames =
          [ok_line(frame, %{})] ++
            Enum.map(events, &line/1) ++ [line(%{"v" => 1, "type" => "resynced", "seq" => high})]

        {{:ok, frames}, put_session(state, id, updated)}

      {:error, reason} ->
        {{:ok, [error_line(frame, Atom.to_string(reason))]}, state}
    end
  end

  defp conversation_open(state, id, session, frame) do
    if session.ready do
      with {:ok, events, high} <-
             Engine.conversation_events(state.store.engine, session.key, frame["since"]),
           {:ok, cursor} <- Engine.conversation_cursor(state.store.engine, session.key),
           {:ok, read_positions} <- Engine.chat_read_positions(state.store.engine, session.key),
           {:ok, next_engine} <- Engine.open_conversation(state.store.engine, session.key),
           {:ok, store} <-
             commit(
               state.store,
               next_engine,
               audit(state.store.engine, session.key, "conversation_open")
             ) do
        contiguous = if frame["since"] <= cursor, do: high, else: cursor

        updated = %{
          session
          | conversation_ready: true,
            conversation_seq: high,
            conversation_contiguous: contiguous
        }

        endpoint =
          case state.origin_listener do
            %{port: port} -> %{"origin_https_port" => port}
            nil -> %{}
          end

        frames =
          [ok_line(frame, endpoint)] ++
            Enum.map(events, &line/1) ++
            [
              line(%{
                "v" => 1,
                "type" => "conversation_resynced",
                "conversation_seq" => high,
                "chat_read_positions" => read_positions
              })
            ]

        {{:ok, frames}, put_session(%{state | store: store}, id, updated)}
      else
        {:error, :storage_unavailable, _} ->
          {{:ok, [error_line(frame, "storage_unavailable")]}, state}

        {:error, reason} ->
          {{:ok, [error_line(frame, Atom.to_string(reason))]}, state}
      end
    else
      {{:ok, [error_line(frame, "resync_required")]}, state}
    end
  end

  defp conversation_open_bounded(state, id, session, frame) do
    if session.ready and session.sync_mode == :legacy do
      with {:ok, events, high} <-
             Engine.conversation_events(state.store.engine, session.key, frame["since"]),
           {:ok, cursor} <- Engine.conversation_cursor(state.store.engine, session.key),
           {:ok, next_engine} <- Engine.open_conversation(state.store.engine, session.key),
           {:ok, person} <- Engine.authorized_person(next_engine, session.key),
           %{view_rev: view_rev} <- Map.get(state.priority_views, person),
           {:ok, store} <-
             commit(
               state.store,
               next_engine,
               audit(state.store.engine, session.key, "conversation_open")
             ) do
        contiguous = if frame["since"] <= cursor, do: high, else: cursor

        updated = %{
          session
          | conversation_ready: true,
            conversation_seq: high,
            conversation_contiguous: contiguous,
            view_support: true
        }

        endpoint =
          case state.origin_listener do
            %{port: port} -> %{"origin_https_port" => port}
            nil -> %{}
          end

        frames =
          [ok_line(frame, endpoint)] ++
            Enum.map(events, &line/1) ++
            [
              line(%{
                "v" => 1,
                "type" => "conversation_resynced",
                "conversation_seq" => high,
                "view_rev" => view_rev
              })
            ]

        {{:ok, frames}, put_session(%{state | store: store}, id, updated)}
      else
        {:error, :storage_unavailable, _} ->
          {{:ok, [error_line(frame, "storage_unavailable")]}, state}

        {:error, reason} ->
          {{:ok, [error_line(frame, Atom.to_string(reason))]}, state}

        nil ->
          {{:ok, [error_line(frame, "view_unavailable")]}, state}

        _ ->
          {{:ok, [error_line(frame, "view_unavailable")]}, state}
      end
    else
      {{:ok, [error_line(frame, "resync_required")]}, state}
    end
  end

  defp inbox_list(state, session, frame) do
    with :ok <- route_b_fields(frame, ~w(v type ref limit cursor)),
         {:ok, limit} <- route_b_limit(frame, false),
         {:ok, cursor} <- route_b_cursor(frame),
         {:ok, person} <- Engine.authorized_person(state.store.engine, session.key),
         %{
           view_rev: view_rev,
           message_order: order,
           message_order_suffixes: order_suffixes,
           message_entries_by_identity: entries
         } <- Map.get(state.priority_views, person),
         {:ok, reply} <-
           Engine.route_b_inbox_page(
             state.store.engine,
             session.key,
             order,
             order_suffixes,
             entries,
             limit,
             cursor
           ) do
      {{:ok, [ok_line(frame, Map.put(reply, "view_rev", view_rev))]}, state}
    else
      {:error, :invalid_cursor} -> {{:ok, [error_line(frame, "invalid_cursor")]}, state}
      {:error, :forbidden} -> {{:ok, [error_line(frame, "forbidden")]}, state}
      {:error, {:invalid_field, field}} ->
        {{:ok, [error_line(frame, "invalid_field", field)]}, state}

      {:error, reason} ->
        {{:ok, [error_line(frame, Atom.to_string(reason))]}, state}

      nil ->
        {{:ok, [error_line(frame, "view_unavailable")]}, state}

      _ ->
        {{:ok, [error_line(frame, "view_unavailable")]}, state}
    end
  end

  defp conversation_page(state, session, frame) do
    with :ok <- route_b_fields(frame, ~w(v type ref sender_id message_id before limit)),
         :ok <- route_b_identity_fields(frame),
         {:ok, limit} <- route_b_limit(frame, true),
         {:ok, before} <- route_b_before(frame),
         {:ok, person} <- Engine.authorized_person(state.store.engine, session.key),
         %{view_rev: view_rev, message_entries_by_identity: entries} <-
           Map.get(state.priority_views, person),
         identity = {frame["sender_id"], frame["message_id"]},
         {:ok, reply} <-
           Engine.route_b_conversation_page(
             state.store.engine,
             session.key,
             identity,
             before,
             limit,
             Map.get(entries, identity)
           ) do
      {{:ok, [ok_line(frame, Map.put(reply, "view_rev", view_rev))]}, state}
    else
      {:error, :not_found} -> {{:ok, [error_line(frame, "not_found")]}, state}
      {:error, :forbidden} -> {{:ok, [error_line(frame, "forbidden")]}, state}
      {:error, {:invalid_field, field}} ->
        {{:ok, [error_line(frame, "invalid_field", field)]}, state}

      {:error, reason} ->
        {{:ok, [error_line(frame, Atom.to_string(reason))]}, state}

      nil ->
        {{:ok, [error_line(frame, "view_unavailable")]}, state}

      _ ->
        {{:ok, [error_line(frame, "view_unavailable")]}, state}
    end
  end

  defp route_b_fields(frame, allowed) do
    case Enum.find(Map.keys(frame), &(&1 not in allowed)) do
      nil -> :ok
      field -> {:error, {:invalid_field, field}}
    end
  end

  defp route_b_identity_fields(frame) do
    if is_binary(frame["sender_id"]) and is_binary(frame["message_id"]),
      do: :ok,
      else: {:error, {:invalid_field, if(is_binary(frame["sender_id"]), do: "message_id", else: "sender_id")}}
  end

  defp route_b_limit(frame, allow_zero?) do
    default = @route_b_default_limit
    minimum = if allow_zero?, do: 0, else: 1

    case Map.get(frame, "limit", default) do
      limit when is_integer(limit) and limit >= minimum ->
        {:ok, min(limit, @route_b_max_limit)}

      _ ->
        {:error, {:invalid_field, "limit"}}
    end
  end

  defp route_b_before(frame) do
    case Map.fetch(frame, "before") do
      :error -> {:ok, nil}
      {:ok, before} when is_integer(before) and before >= 0 -> {:ok, before}
      _ -> {:error, {:invalid_field, "before"}}
    end
  end

  defp route_b_cursor(frame) do
    case Map.fetch(frame, "cursor") do
      :error -> {:ok, nil}
      {:ok, cursor} when is_binary(cursor) -> decode_route_b_cursor(cursor)
      _ -> {:error, :invalid_cursor}
    end
  end

  defp decode_route_b_cursor(cursor) do
    with {:ok, json} <- Base.url_decode64(cursor, padding: false),
         {:ok, %{"sender_id" => sender, "message_id" => id}} <- JSON.decode_line(json <> "\n"),
         true <- is_binary(sender) and is_binary(id) do
      {:ok, {sender, id}}
    else
      _ -> {:error, :invalid_cursor}
    end
  end

  defp conversation_ack(state, session, frame) do
    if session.conversation_ready do
      case Engine.acknowledge_conversation(
             state.store.engine,
             session.key,
             frame["conversation_seq"],
             session.conversation_contiguous
           ) do
        {:ok, next_engine, fields} ->
          case commit(
                 state.store,
                 next_engine,
                 audit(state.store.engine, session.key, "conversation_ack")
               ) do
            {:ok, store} -> {{:ok, [ok_line(frame, fields)]}, %{state | store: store}}
            {:error, _, _} -> {{:ok, [error_line(frame, "storage_unavailable")]}, state}
          end

        {:error, reason} ->
          {{:ok, [error_line(frame, Atom.to_string(reason))]}, state}
      end
    else
      {{:ok, [error_line(frame, "conversation_open_required")]}, state}
    end
  end

  defp drain_conversation(engine, %{conversation_ready: true} = session),
    do: Engine.conversation_events(engine, session.key, session.conversation_seq)

  defp drain_conversation(_engine, _session), do: {:ok, [], nil}

  defp ack(state, session, frame) do
    case Engine.acknowledge(state.store.engine, session.key, frame["seq"], session.contiguous) do
      {:ok, next_engine, fields} ->
        audit = audit(state.store.engine, session.key, "cursor_ack")

        case commit(state.store, next_engine, audit) do
          {:ok, store} -> {{:ok, [ok_line(frame, fields)]}, %{state | store: store}}
          {:error, _, _} -> {{:ok, [error_line(frame, "storage_unavailable")]}, state}
        end

      {:error, reason} ->
        {{:ok, [error_line(frame, Atom.to_string(reason))]}, state}
    end
  end

  defp request(state, key, raw) do
    case Engine.authorized_person(state.store.engine, key) do
      {:ok, _person} ->
        cond do
          not state.push_enabled and push_request?(raw) ->
            {{:ok, error_line(raw, "forbidden")}, state}

          not state.source_fixture_mode and source_report_request?(raw) ->
            {{:ok, error_line(raw, "not_supported")}, state}

          true ->
            perform_request(state, key, raw)
        end

      _ ->
        {{:error, :forbidden}, state}
    end
  end

  defp source_report_request?(raw) do
    case JSON.decode_line(raw) do
      {:ok, %{"type" => type}} when type in ["source_bind", "source_result"] -> true
      _ -> false
    end
  end

  defp push_request?(raw) do
    case JSON.decode_line(raw) do
      {:ok, %{"type" => type}} when type in ["push_register", "push_remove", "push_status"] ->
        true

      _ ->
        false
    end
  end

  defp perform_request(state, key, raw) do
    {next_engine, reply} = Engine.request_line(state.store.engine, key, raw, state.clock.())
    next_engine = with_coverage_availability(state, key, next_engine)
    next_engine = with_initial_origin_availability(state, key, next_engine)

    operation_id =
      case JSON.decode_line(raw) do
        {:ok, request} when is_map(request) -> Diagnostics.operation_id(request)
        _ -> nil
      end

    targets =
      case JSON.decode_line(raw) do
        {:ok, request} when is_map(request) ->
          origin_feed_targets(state.store.engine, next_engine, key, request)

        _ ->
          []
      end

    finish_request(
      state,
      key,
      next_engine,
      reply,
      operation_id,
      targets,
      request_action(raw),
      raw
    )
  end

  defp perform_request_frame(state, key, frame) do
    {next_engine, reply} = Engine.request_frame(state.store.engine, key, frame, state.clock.())
    next_engine = with_coverage_availability(state, key, next_engine)
    next_engine = with_initial_origin_availability(state, key, next_engine)
    operation_id = Diagnostics.operation_id(frame)
    targets = origin_feed_targets(state.store.engine, next_engine, key, frame)

    finish_request(
      state,
      key,
      next_engine,
      reply,
      operation_id,
      targets,
      frame["type"] || "relay_request",
      frame
    )
  end

  defp finish_request(state, key, next_engine, reply, operation_id, targets, action, original) do
    if next_engine == state.store.engine do
      {reply, state}
    else
      case commit(
             state.store,
             next_engine,
             Map.put(
               audit(state.store.engine, key, action),
               :operation_id,
               operation_id
             )
           ) do
        {:ok, store} ->
          next_state = %{state | store: store}

          next_state =
            case Engine.authorized_person(next_engine, key) do
              {:ok, person} -> notify_coverage_change(next_state, state.store.engine, person, key)
              _ -> next_state
            end

          next_state =
            Enum.reduce(targets, next_state, fn target, current ->
              record_origin_feed_change(current, state.store.engine, next_engine, target)
            end)

          next_state =
            case Engine.authorized_person(state.store.engine, key) do
              {:ok, person} ->
                if pending_signature(state.store.engine, person) !=
                     pending_signature(next_engine, person),
                   do: pending_set_changed(next_state, person),
                   else: next_state

              _ ->
                next_state
            end

          next_state =
            case changed_levels_person(state.store.engine, next_engine, key) do
              {:ok, person} -> notify_levels_changed(next_state, person)
              :unchanged -> next_state
            end

          {reply, next_state}

        {:error, _, _} ->
          {{:ok, error_line(original, "storage_unavailable")}, state}
      end
    end
  end

  defp initialize_priority_views(state) do
    Enum.reduce(Map.keys(state.store.engine.persons), state, fn person, current ->
      current = ensure_priority_view(current, person)
      engine = current.store.engine

      if Placement.reassess(engine, person, nil) == :whole_set and
           JevClient.connection_ready?(engine.jev_endpoint, engine.jev_model) and
           Placement.ranking_items(engine, person) != [] do
        schedule_priority_pass(current, person)
      else
        current
      end
    end)
  end

  defp ensure_priority_view(state, person) do
    engine = state.store.engine
    choice = Placement.choice(engine, person)
    {items, reply_summary} = Placement.ranking_projection(engine, person)
    previous = Map.get(state.priority_views, person)

    view =
      cond do
        choice == "baseline" ->
          %{
            algorithm: "baseline",
            basis: "selected",
            entries: Placement.place(choice, items, [], nil)
          }

        not JevClient.connection_ready?(engine.jev_endpoint, engine.jev_model) ->
          fallback_view(items)

        match?(%{algorithm: "jev-set", basis: "selected"}, previous) ->
          %{
            algorithm: "jev-set",
            basis: "selected",
            entries:
              Placement.place(
                choice,
                items,
                Map.get(previous, :engine_entries, previous.entries),
                nil
              )
          }

        true ->
          fallback_view(items)
      end

    put_priority_view(state, person, view, reply_summary)
  end

  defp ensure_priority_view_if_missing(state, person) do
    if Map.has_key?(state.priority_views, person),
      do: state,
      else: ensure_priority_view(state, person)
  end

  defp fallback_view(items),
    do: %{
      algorithm: "jev-set",
      basis: "fallback",
      entries: Placement.place("baseline", items, [], nil)
    }

  defp put_priority_view(state, person, view, reply_summary) do
    previous = Map.get(state.priority_views, person)
    engine_entries = Map.get(view, :engine_entries, Map.get(view, :entries, []))

    next = Engine.retain_reply_scores(state.store.engine, person, engine_entries)

    case commit(state.store, next, %{
           person: person,
           actor: "server",
           action: "reply_group_scores"
         }) do
      {:ok, store} ->
        publish_priority_view(
          %{state | store: store},
          person,
          view,
          previous,
          engine_entries,
          reply_summary
        )

      {:error, _, _} ->
        state
    end
  end

  defp publish_priority_view(state, person, view, previous, engine_entries, reply_summary) do
    {replies, messages} = Enum.split_with(engine_entries, &Map.has_key?(&1, "conversation_seq"))

    shown =
      view
      |> Map.take([:algorithm, :basis, :reason])
      |> Map.put(:entries, Placement.grouped(messages))
      |> Map.put(:replies, Enum.sort_by(replies, & &1["conversation_seq"]))
      |> Map.put(
        :list_items,
        Placement.list_items(state.store.engine, person, engine_entries, reply_summary)
      )

    shown =
      if Placement.choice(state.store.engine, person) == "baseline" do
        entries_by_id = Map.new(messages, &{Placement.identity(&1), &1})

        ordered_messages =
          shown.list_items
          |> Enum.filter(&(&1["kind"] == "message"))
          |> Enum.flat_map(fn row ->
            case Map.get(entries_by_id, Placement.identity(row)) do
              nil -> []
              entry -> [entry]
            end
          end)

        shown |> Map.put(:entries, ordered_messages) |> Map.put(:free_order_version, 1)
      else
        shown
      end

    {message_order, message_entries_by_identity, message_order_suffixes} =
      Enum.reduce(Enum.reverse(shown.entries), {[], %{}, %{}}, fn entry,
                                                                {order, entries, suffixes} ->
        identity = Placement.identity(entry)
        next_order = [identity | order]

        {
          next_order,
          Map.put(entries, identity, entry),
          Map.put(suffixes, identity, next_order)
        }
      end)

    shown =
      shown
      |> Map.put(:message_order, message_order)
      |> Map.put(:message_entries_by_identity, message_entries_by_identity)
      |> Map.put(:message_order_suffixes, message_order_suffixes)

    comparable =
      Map.take(shown, [
        :algorithm,
        :basis,
        :reason,
        :entries,
        :replies,
        :list_items,
        :free_order_version
      ])

    message_order = shown.message_order
    message_entries_by_identity = shown.message_entries_by_identity

    if previous &&
         Map.take(previous, [
           :algorithm,
           :basis,
           :reason,
           :entries,
           :replies,
           :list_items,
           :free_order_version
         ]) ==
           comparable do
      Map.put(
        state,
        :priority_views,
        Map.put(
          state.priority_views,
          person,
          previous
          |> Map.put(:engine_entries, engine_entries)
          |> Map.put(:message_order, message_order)
          |> Map.put(:message_entries_by_identity, message_entries_by_identity)
          |> Map.put(:message_order_suffixes, message_order_suffixes)
        )
      )
    else
      counter = state.priority_revision_counter + 1
      view_rev = state.priority_revision_namespace <> ":" <> Integer.to_string(counter)

      view =
        comparable
        |> Map.put(:engine_entries, engine_entries)
        |> Map.put(:message_order, message_order)
        |> Map.put(:message_entries_by_identity, message_entries_by_identity)
        |> Map.put(:message_order_suffixes, message_order_suffixes)
        |> Map.put(:view_rev, view_rev)

      state
      |> Map.put(:priority_revision_counter, counter)
      |> Map.put(:priority_views, Map.put(state.priority_views, person, view))
      |> notify_priority_view_change(person, view_rev)
    end
  end

  defp notify_priority_view_change(state, person, view_rev) do
    now = System.monotonic_time(:millisecond)

    sessions =
      Map.new(state.sessions, fn {id, session} ->
        case Engine.authorized_person(state.store.engine, session.key) do
          {:ok, ^person} when session.view_support and session.ready ->
            due_at =
              case session.view_last_notice_at do
                nil -> now
                last when now - last >= 1_000 -> now
                last -> last + 1_000
              end

            {id, %{session | view_notice: view_rev, view_notice_due_at: due_at}}

          _ ->
            {id, session}
        end
      end)

    %{state | sessions: sessions}
  end

  defp changed_levels_person(old_engine, next_engine, key) do
    with {:ok, person} <- Engine.authorized_person(old_engine, key),
         {:ok, ^person} <- Engine.authorized_person(next_engine, key),
         true <-
           Map.get(old_engine.bounce_levels, person) != Map.get(next_engine.bounce_levels, person) do
      {:ok, person}
    else
      _ -> :unchanged
    end
  end

  defp notify_levels_changed(state, person) do
    sessions =
      Map.new(state.sessions, fn {id, session} ->
        if session.levels_support and
             Engine.authorized_person(state.store.engine, session.key) == {:ok, person} do
          {id, %{session | levels_notice: true}}
        else
          {id, session}
        end
      end)

    %{state | sessions: sessions}
  end

  defp take_view_notice(%{view_notice: nil} = session, _now), do: {[], session}

  defp take_view_notice(
         %{view_notice: view_rev, view_notice_due_at: due_at} = session,
         now
       )
       when is_integer(due_at) and due_at <= now do
    frame = line(%{"v" => 1, "type" => "view_changed", "view_rev" => view_rev})
    {[frame], %{session | view_notice: nil, view_notice_due_at: nil, view_last_notice_at: now}}
  end

  defp take_view_notice(session, _now), do: {[], session}

  defp take_levels_notice(%{levels_notice: true} = session) do
    {[line(%{"v" => 1, "type" => "levels_changed"})], %{session | levels_notice: false}}
  end

  defp take_levels_notice(session), do: {[], session}

  defp pending_signature(engine, person) do
    engine
    |> Placement.ranking_items(person)
    |> Enum.map(&{&1.identity, &1.revision, &1.message})
    |> Enum.sort_by(&elem(&1, 0))
  end

  defp pending_set_changed(state, person) do
    previous = Map.get(state.priority_views, person)
    state = ensure_priority_view(state, person)
    engine = state.store.engine

    previous_entries =
      if is_map(previous), do: Map.get(previous, :engine_entries, previous.entries), else: []

    if Placement.reassess(engine, person, previous_entries) == :whole_set and
         JevClient.connection_ready?(engine.jev_endpoint, engine.jev_model) and
         Placement.ranking_items(engine, person) != [] do
      schedule_priority_pass(state, person)
    else
      state |> clear_priority_cache(person) |> invalidate_priority_work(person)
    end
  end

  defp priority_settings_changed(state, person) do
    state = clear_priority_cache(state, person)
    previous = Map.get(state.priority_views, person)
    state = ensure_priority_view(state, person)
    engine = state.store.engine

    previous_entries =
      if is_map(previous), do: Map.get(previous, :engine_entries, previous.entries), else: []

    if Placement.reassess(engine, person, previous_entries) == :whole_set and
         JevClient.connection_ready?(engine.jev_endpoint, engine.jev_model) and
         Placement.ranking_items(engine, person) != [] do
      schedule_priority_pass(state, person)
    else
      invalidate_priority_work(state, person)
    end
  end

  defp invalidate_priority_work(state, person) do
    case Map.get(state.priority_timers, person) do
      %{ref: ref} -> Process.cancel_timer(ref)
      _ -> :ok
    end

    generation = Map.get(state.priority_generations, person, 0) + 1

    case Map.get(state.priority_retry_timers, person) do
      %{ref: ref} -> Process.cancel_timer(ref)
      _ -> :ok
    end

    case Map.get(state.priority_jobs, person) do
      %{pid: pid} -> Process.exit(pid, :kill)
      _ -> :ok
    end

    %{
      state
      | priority_generations: Map.put(state.priority_generations, person, generation),
        priority_timers: Map.delete(state.priority_timers, person),
        priority_retry_timers: Map.delete(state.priority_retry_timers, person),
        priority_queued: Map.delete(state.priority_queued, person)
    }
  end

  defp schedule_priority_pass(state, person) do
    state = invalidate_priority_work(state, person)
    generation = Map.fetch!(state.priority_generations, person)
    engine = state.store.engine

    if Placement.choice(engine, person) == "jev-set" and
         JevClient.connection_ready?(engine.jev_endpoint, engine.jev_model) and
         Placement.ranking_items(engine, person) != [] do
      ref =
        Process.send_after(
          self(),
          {:prioritization_due, person, generation},
          @prioritization_debounce_ms
        )

      %{
        state
        | priority_timers:
            Map.put(state.priority_timers, person, %{ref: ref, generation: generation})
      }
    else
      state
    end
  end

  defp start_priority_pass(state, person, generation) do
    engine = state.store.engine

    cond do
      Map.get(state.priority_generations, person, 0) != generation ->
        state

      Placement.choice(engine, person) != "jev-set" ->
        state

      Map.has_key?(state.priority_jobs, person) ->
        %{state | priority_queued: Map.put(state.priority_queued, person, generation)}

      not JevClient.connection_ready?(engine.jev_endpoint, engine.jev_model) ->
        state |> clear_priority_cache(person) |> set_fallback_priority_view(person)

      true ->
        items = Placement.ranking_items(engine, person)
        sequence = Map.get(state.priority_sequences, person, 0) + 1
        parent = self()
        cache = Map.get(state.priority_caches, person, %{})
        cache_epoch = Map.get(state.priority_cache_epochs, person, 0)
        now = state.clock.()
        evaluator = state.priority_evaluator

        {pid, monitor} =
          spawn_monitor(fn ->
            result =
              JevRanking.run(items, cache, engine.jev_endpoint, engine.jev_model, now, evaluator)

            send(parent, {:prioritization_result, person, sequence, generation, result})
          end)

        %{
          state
          | priority_sequences: Map.put(state.priority_sequences, person, sequence),
            priority_jobs:
              Map.put(state.priority_jobs, person, %{
                pid: pid,
                monitor: monitor,
                sequence: sequence,
                generation: generation,
                cache_epoch: cache_epoch,
                endpoint: engine.jev_endpoint,
                model: engine.jev_model
              })
        }
    end
  end

  defp finish_priority_pass(state, person, job, result) do
    state = %{state | priority_jobs: Map.delete(state.priority_jobs, person)}
    state = if is_map(result), do: log_priority_pass(state, result), else: state
    engine = state.store.engine
    generation = Map.get(state.priority_generations, person, 0)

    same_connection? =
      Placement.choice(engine, person) == "jev-set" and
        Map.get(job, :endpoint, engine.jev_endpoint) == engine.jev_endpoint and
        Map.get(job, :model, engine.jev_model) == engine.jev_model and
        Map.get(job, :cache_epoch, Map.get(state.priority_cache_epochs, person, 0)) ==
          Map.get(state.priority_cache_epochs, person, 0)

    ready? =
      same_connection? and JevClient.connection_ready?(engine.jev_endpoint, engine.jev_model)

    current? = generation == job.generation and ready?

    state =
      if current? do
        {items, reply_summary} = Placement.ranking_projection(engine, person)
        previous = Map.get(state.priority_views, person)

        case result do
          %{cache: cache, scores: scores, overflow: overflow} = pass ->
            state = %{state | priority_caches: Map.put(state.priority_caches, person, cache)}

            {state, score_commit_failed?} =
              if map_size(scores) > 0 do
                entries = Placement.place("jev-set", items, [], {:ranked, scores, overflow})

                put_scored_priority_view(state, person, items, entries, reply_summary)
              else
                {put_priority_view(state, person, fallback_view(items), reply_summary), false}
              end

            state =
              state
              |> schedule_priority_retry(
                person,
                if(score_commit_failed?, do: 1, else: pass.failed)
              )
              |> schedule_priority_deadline(person, pass.considered)

            if Enum.any?(pass.considered, fn item ->
                 JevRanking.deadline_class(item, pass.now) !=
                   JevRanking.deadline_class(item, state.clock.())
               end),
               do: schedule_priority_pass(state, person),
               else: state

          {:ok, scores} when is_map(scores) and map_size(scores) > 0 ->
            previous_entries =
              if is_map(previous), do: Map.get(previous, :engine_entries, []), else: []

            entries = Placement.place("jev-set", items, previous_entries, {:scored, scores})

            {scored_state, score_commit_failed?} =
              put_scored_priority_view(state, person, items, entries, reply_summary)

            if score_commit_failed?,
              do: schedule_priority_retry(scored_state, person, 1),
              else: scored_state

          _ ->
            state
            |> put_priority_view(person, fallback_view(items), reply_summary)
            |> schedule_priority_retry(person, 1)
        end
      else
        cond do
          ready? and is_map(result) and Map.has_key?(result, :cache) ->
            %{state | priority_caches: Map.put(state.priority_caches, person, result.cache)}

          generation == job.generation and Placement.choice(engine, person) == "jev-set" and
              not JevClient.connection_ready?(engine.jev_endpoint, engine.jev_model) ->
            state |> clear_priority_cache(person) |> set_fallback_priority_view(person)

          true ->
            state
        end
      end

    case Map.pop(state.priority_queued, person) do
      {nil, _remaining_queued} ->
        state

      {^generation, remaining_queued} when not is_map_key(state.priority_timers, person) ->
        start_priority_pass(%{state | priority_queued: remaining_queued}, person, generation)

      {_stale, remaining_queued} ->
        %{state | priority_queued: remaining_queued}
    end
  end

  defp put_scored_priority_view(state, person, items, entries, reply_summary) do
    old_engine = state.store.engine

    {next_engine, changed_keys} =
      BounceDecision.record_scores_with_changes(old_engine, person, entries, state.clock.())

    case persist_bounce_decision(
           state,
           old_engine,
           next_engine,
           changed_keys,
           "bounce_score_selection"
         ) do
      {:ok, next_state} ->
        next_state =
          put_priority_view(
            next_state,
            person,
            %{
              algorithm: "jev-set",
              basis: "selected",
              engine_entries: entries
            },
            reply_summary
          )
          |> record_applied_scores(person, items, entries)

        {next_state, false}

      {:error, reason} ->
        Diagnostics.failure(:bounce_schedule, :score_selection_commit, reason, :unavailable)

        {put_priority_view(state, person, fallback_view(items), reply_summary), true}
    end
  end

  defp persist_bounce_decision(state, old_engine, next_engine, _changed_keys, _action)
       when old_engine == next_engine,
       do: {:ok, state}

  defp persist_bounce_decision(state, old_engine, next_engine, changed_keys, action) do
    audit = %{person: "system", actor: "bounce_decision", action: action}

    case commit(state.store, next_engine, audit) do
      {:ok, store} ->
        next_state = %{state | store: store}

        next_state =
          next_state
          |> record_advisory_feed_change(old_engine, next_engine, changed_keys)
          |> reschedule_bounce_sweep()

        {:ok, next_state}

      {:error, reason, _store} ->
        {:error, reason}

      error ->
        {:error, error}
    end
  end

  defp record_applied_scores(%{applied_score_record: nil} = state, _person, _items, _entries),
    do: state

  defp record_applied_scores(state, person, items, entries) do
    # A failed durable history commit must not be logged as an applied score.
    case Map.get(state.priority_views, person) do
      %{engine_entries: ^entries} -> append_applied_scores(state, person, items, entries)
      _ -> state
    end
  end

  defp append_applied_scores(state, person, items, entries) do
    items = Map.new(items, &{&1.identity, &1})
    now = state.clock.()
    model = state.store.engine.jev_model
    previous = Map.get(state.priority_recorded_scores, person, %{})

    current =
      Map.new(
        for %{"score" => score} = entry <- entries,
            do: {Placement.identity(entry), {score, model}}
      )

    # Ranking passes merge reused cache entries with new results. Rebuilding a
    # view must not give an unchanged score a new record or timestamp.
    rows =
      for %{"score" => score} = entry <- entries,
          Map.get(previous, Placement.identity(entry)) != {score, model} do
        item = Map.fetch!(items, Placement.identity(entry))

        %{
          "time" => now,
          "person" => person,
          "sender_id" => entry["sender_id"],
          "message_id" => entry["message_id"],
          "title" => item.message["title"],
          "score" => score,
          "model" => model
        }
        |> then(fn row ->
          case entry do
            %{"conversation_seq" => seq} ->
              Map.merge(row, %{"kind" => "reply", "conversation_seq" => seq})

            _ ->
              row
          end
        end)
      end

    AppliedScoreRecord.record(state.applied_score_record, rows)
    %{state | priority_recorded_scores: Map.put(state.priority_recorded_scores, person, current)}
  end

  defp set_fallback_priority_view(state, person) do
    {items, reply_summary} = Placement.ranking_projection(state.store.engine, person)
    put_priority_view(state, person, fallback_view(items), reply_summary)
  end

  defp clear_priority_cache(state, person) do
    for timers <- [state.priority_deadline_timers, state.priority_retry_timers],
        %{ref: ref} <- [Map.get(timers, person)] do
      Process.cancel_timer(ref)
    end

    %{
      state
      | priority_caches: Map.delete(state.priority_caches, person),
        priority_cache_epochs: Map.update(state.priority_cache_epochs, person, 1, &(&1 + 1)),
        priority_failures: Map.delete(state.priority_failures, person),
        priority_retry_timers: Map.delete(state.priority_retry_timers, person),
        priority_deadline_timers: Map.delete(state.priority_deadline_timers, person)
    }
  end

  defp log_priority_pass(state, %{sent: sent} = pass) when sent > 0 do
    outcome =
      cond do
        pass.failed == 0 -> "ok"
        pass.failed == sent -> "failed"
        true -> "partial"
      end

    Logger.info(
      "event=harklight_priority_pass outcome=#{outcome} requests=#{sent - if(pass.finals_sent, do: 1, else: 0)} " <>
        "finals=#{if(pass.finals_sent, do: 1, else: 0)} considered=#{length(pass.considered)} " <>
        "clipped=#{length(pass.clipped)} bytes=#{pass.bytes} elapsed_ms=#{pass.elapsed_ms}"
    )

    state
  end

  defp log_priority_pass(state, _pass), do: state

  defp schedule_priority_retry(state, person, failed) do
    if failed > 0 do
      count = Map.get(state.priority_failures, person, 0) + 1
      delay = Enum.at([10_000, 30_000, 60_000, 120_000], count - 1, 300_000)
      generation = Map.get(state.priority_generations, person, 0)
      ref = Process.send_after(self(), {:priority_retry, person, generation}, delay)

      %{
        state
        | priority_failures: Map.put(state.priority_failures, person, count),
          priority_retry_timers:
            Map.put(state.priority_retry_timers, person, %{ref: ref, generation: generation})
      }
    else
      %{state | priority_failures: Map.delete(state.priority_failures, person)}
    end
  end

  defp schedule_priority_deadline(state, person, considered) do
    case Map.get(state.priority_deadline_timers, person) do
      %{ref: ref} -> Process.cancel_timer(ref)
      _ -> :ok
    end

    state = %{
      state
      | priority_deadline_timers: Map.delete(state.priority_deadline_timers, person)
    }

    now = state.clock.()

    case JevRanking.next_deadline(considered, now) do
      nil ->
        state

      due ->
        ref =
          Process.send_after(
            self(),
            {:priority_deadline, person, due},
            min(max(due - now + 1, 1), @max_timer_delay_ms)
          )

        %{
          state
          | priority_deadline_timers:
              Map.put(state.priority_deadline_timers, person, %{ref: ref, due: due})
        }
    end
  end

  defp priority_settings(state, person) do
    engine = state.store.engine
    view = Map.get(state.priority_views, person)

    %{
      person_id: person,
      algorithm_id: Placement.choice(engine, person),
      endpoint: engine.jev_endpoint || "",
      model: engine.jev_model || "",
      key_status: JevClient.key_status(),
      basis: if(is_map(view), do: Map.get(view, :basis), else: nil)
    }
  end

  defp save_priority_settings(
         state,
         person,
         key,
         configured_owner_person,
         algorithm,
         endpoint,
         model
       ) do
    cond do
      algorithm not in ["baseline", "jev-set"] ->
        {:error, :unknown_algorithm}

      state.deployment_mode != :self_hosted or person != configured_owner_person or
          not match?({:ok, ^person, _}, Engine.owner_identity(state.store.engine, key)) ->
        {:error, :forbidden}

      true ->
        with {:ok, endpoint} <- normalize_endpoint(endpoint),
             {:ok, model} <- normalize_model(model) do
          save_priority_settings_authorized(state, person, key, algorithm, endpoint, model)
        else
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp save_priority_settings_authorized(state, person, key, requested, endpoint, model) do
    engine = state.store.engine
    connection_changed? = engine.jev_endpoint != endpoint or engine.jev_model != model

    with {:ok, configured} <- Engine.set_jev_connection(engine, endpoint, model),
         {:ok, changed} <- Engine.set_algorithm_choice(configured, person, requested) do
      next_state =
        if changed == engine do
          state
        else
          case commit(state.store, changed, %{
                 person: person,
                 actor: key,
                 action: "prioritization_settings"
               }) do
            {:ok, store} ->
              next = %{state | store: store}

              affected_people =
                if connection_changed? do
                  changed.persons
                  |> Map.keys()
                  |> Enum.filter(&(Placement.choice(changed, &1) == "jev-set"))
                  |> then(&Enum.uniq([person | &1]))
                else
                  [person]
                end

              Enum.reduce(affected_people, next, fn affected_person, current ->
                current =
                  if connection_changed? and
                       match?(
                         %{basis: "selected"},
                         Map.get(current.priority_views, affected_person)
                       ) do
                    %{
                      current
                      | priority_views: Map.delete(current.priority_views, affected_person)
                    }
                  else
                    current
                  end

                priority_settings_changed(current, affected_person)
              end)

            {:error, _, _} ->
              nil
          end
        end

      if is_nil(next_state) do
        {:error, :storage_unavailable}
      else
        {:ok, next_state, priority_settings(next_state, person)}
      end
    end
  end

  defp set_hosted_algorithm(state, person, algorithm) do
    engine = state.store.engine

    cond do
      algorithm not in ["baseline", "jev-set"] ->
        {:error, :unknown_algorithm, state}

      state.deployment_mode != :saas or is_nil(Engine.hosted_account(engine, person)) ->
        {:error, :not_hosted, state}

      Placement.choice(engine, person) == algorithm ->
        {:ok, ensure_priority_view(state, person)}

      true ->
        case Engine.set_algorithm_choice(engine, person, algorithm) do
          {:ok, ^engine} ->
            {:ok, state}

          {:ok, changed} ->
            case commit(state.store, changed, %{
                   person: person,
                   actor: "hosted_component",
                   action: "hosted_algorithm"
                 }) do
              {:ok, store} -> {:ok, priority_settings_changed(%{state | store: store}, person)}
              {:error, _, _} -> {:error, :storage_unavailable, state}
            end

          {:error, :not_found} ->
            {:error, :not_hosted, state}

          {:error, reason} ->
            {:error, reason, state}
        end
    end
  end

  defp normalize_endpoint(value) when is_binary(value) do
    value = String.trim(value)

    cond do
      value == "" -> {:ok, nil}
      JevClient.valid_endpoint?(value) -> {:ok, value}
      true -> {:error, :invalid_endpoint}
    end
  end

  defp normalize_endpoint(_), do: {:error, :invalid_endpoint}

  defp normalize_model(value) when is_binary(value) do
    value = String.trim(value)

    cond do
      value == "" -> {:ok, nil}
      JevClient.valid_model?(value) -> {:ok, value}
      true -> {:error, :invalid_model}
    end
  end

  defp normalize_model(_), do: {:error, :invalid_model}

  defp covered_handles(engine, person, origin) do
    handles =
      for {{^person, ^origin, handle}, session} <- engine.origin_sessions,
          get_in(session, [:support, "origin_listener"]) == true,
          do: handle

    Enum.sort(handles)
  end

  defp origin_listening?(state, engine, {person, origin, _handle} = key) do
    Map.has_key?(state.origin_streams, key) or
      (Map.has_key?(state.origin_streams, {person, origin, :origin}) and
         get_in(engine.origin_sessions, [key, :support, "origin_listener"]) == true)
  end

  defp origin_session_status(state, engine, {person, origin, handle} = key) do
    Conversation.status(engine, person, origin, handle, origin_listening?(state, engine, key))
  end

  defp append_origin_availability_if_changed(engine, person, origin, handle, previous, current) do
    case current do
      {:ok, status} ->
        previous_availability =
          case previous do
            {:ok, prior} -> prior["availability"]
            _ -> nil
          end

        if previous_availability == status["availability"] do
          engine
        else
          Engine.origin_availability(
            engine,
            person,
            origin,
            handle,
            status["availability"],
            status["last_detached_at"]
          )
        end

      _ ->
        engine
    end
  end

  defp revoke_origin_sessions(state, engine, person, origin) do
    engine.origin_sessions
    |> Enum.filter(fn {{session_person, session_origin, _handle}, _session} ->
      session_person == person and session_origin == origin
    end)
    |> Enum.sort_by(fn {{_session_person, _session_origin, handle}, _session} -> handle end)
    |> Enum.reduce(engine, fn {{^person, ^origin, handle} = key, _session}, current ->
      previous = origin_session_status(state, state.store.engine, key)

      case Conversation.mark_gone(current, person, origin, handle) do
        {:ok, gone} ->
          current_status = origin_session_status(state, gone, key)

          append_origin_availability_if_changed(
            gone,
            person,
            origin,
            handle,
            previous,
            current_status
          )

        _ ->
          current
      end
    end)
  end

  defp origin_availability_changes(before, after_state, person, origin) do
    Enum.reduce(after_state.store.engine.origin_sessions, after_state.store.engine, fn
      {{^person, ^origin, handle} = key, _}, engine ->
        old = origin_session_status(before, before.store.engine, key)
        current = origin_session_status(after_state, engine, key)

        previous =
          case old do
            {:ok, status} -> status["availability"]
            _ -> nil
          end

        case current do
          {:ok, status} ->
            if is_nil(previous) or previous == status["availability"] do
              engine
            else
              engine =
                if status["availability"] == "not_listening" do
                  {:ok, detached} = Conversation.mark_detached(engine, person, origin, handle)
                  detached
                else
                  engine
                end

              case origin_session_status(after_state, engine, key) do
                {:ok, updated} ->
                  append_origin_availability_if_changed(
                    engine,
                    person,
                    origin,
                    handle,
                    old,
                    {:ok, updated}
                  )

                _ ->
                  engine
              end
            end

          _ ->
            engine
        end

      _, engine ->
        engine
    end)
  end

  defp with_coverage_availability(state, _key, next)
       when state.store.engine.origin_sessions == next.origin_sessions, do: next

  defp with_coverage_availability(state, key, next) do
    case Engine.authorized_person(next, key) do
      {:ok, person} ->
        origin_availability_changes(
          state,
          %{state | store: %{state.store | engine: next}},
          person,
          key
        )

      _ ->
        next
    end
  end

  defp requeue_covered(state, person, origin) do
    engine =
      Enum.reduce(
        covered_handles(state.store.engine, person, origin),
        state.store.engine,
        fn handle, engine ->
          BounceDecision.requeue_supported_session(engine, {person, origin, handle})
        end
      )

    %{state | store: %{state.store | engine: engine}} |> reschedule_bounce_sweep()
  end

  defp notify_coverage_change(state, old_engine, person, origin) do
    if covered_handles(old_engine, person, origin) !=
         covered_handles(state.store.engine, person, origin) do
      case state.origin_streams[{person, origin, :origin}] do
        %{worker: worker} -> send(worker, :origin_coverage_changed)
        _ -> :ok
      end

      requeue_covered(state, person, origin)
    else
      state
    end
  end

  defp with_initial_origin_availability(state, key, next_engine) do
    case Engine.authorized_person(next_engine, key) do
      {:ok, person} ->
        high = Map.get(next_engine.persons, person, 0)
        previous_high = Map.get(state.store.engine.persons, person, 0)

        case Map.get(next_engine.events, {person, high}) do
          %{kind: :delivery, key: message_key} when high > previous_high ->
            record = next_engine.messages[message_key]

            if is_binary(Map.get(record, :session_handle)) do
              handle = record.session_handle
              listening? = origin_listening?(state, next_engine, {person, key, handle})
              {:ok, current} = Engine.origin_status(next_engine, key, handle, listening?)

              Engine.origin_delivery_availability(
                next_engine,
                person,
                high,
                current["availability"],
                current["last_detached_at"]
              )
            else
              next_engine
            end

          _ ->
            next_engine
        end

      _ ->
        next_engine
    end
  end

  defp setup(state, {:ok, next_engine}, person, actor, action) do
    case commit(state.store, next_engine, %{person: person, actor: actor, action: action}) do
      {:ok, store} ->
        state = %{state | store: store}
        state = ensure_priority_view_if_missing(state, person)
        {:reply, :ok, state}

      {:error, _, _} -> {:reply, {:error, :storage_unavailable}, state}
    end
  end

  defp setup(state, {:error, reason}, _person, _actor, _action),
    do: {:reply, {:error, reason}, state}

  defp reschedule_default_sweep(state, index) do
    if is_reference(state.default_timer), do: Process.cancel_timer(state.default_timer)

    generation = state.default_timer_generation + 1

    state = %{
      state
      | default_index: index,
        default_timer: nil,
        default_timer_generation: generation
    }

    case Enum.min(index |> Map.values(), &<=/2, fn -> nil end) do
      at when is_integer(at) ->
        delay = min(max(at - state.clock.(), 0), @max_timer_delay_ms)
        timer = Process.send_after(self(), {:default_sweep, generation}, delay)
        %{state | default_timer: timer}

      _ ->
        state
    end
  end

  defp schedule_default_retry(state) do
    generation = state.default_timer_generation + 1
    timer = Process.send_after(self(), {:default_sweep, generation}, @default_sweep_retry_ms)
    %{state | default_timer: timer, default_timer_generation: generation}
  end

  defp reschedule_bounce_sweep(state) do
    if is_reference(state.bounce_deadline_timer),
      do: Process.cancel_timer(state.bounce_deadline_timer)

    generation = state.bounce_deadline_generation + 1

    state = %{
      state
      | bounce_deadline_timer: nil,
        bounce_deadline_generation: generation
    }

    case BounceDecision.next_deadline(state.store.engine) do
      due_at when is_integer(due_at) ->
        delay = min(max(due_at - state.clock.(), 0), @max_timer_delay_ms)
        timer = Process.send_after(self(), {:bounce_due, generation}, delay)
        %{state | bounce_deadline_timer: timer}

      _ ->
        state
    end
  end

  defp schedule_bounce_retry(state) do
    if is_reference(state.bounce_deadline_timer),
      do: Process.cancel_timer(state.bounce_deadline_timer)

    generation = state.bounce_deadline_generation + 1
    timer = Process.send_after(self(), {:bounce_due, generation}, @default_sweep_retry_ms)

    %{state | bounce_deadline_timer: timer, bounce_deadline_generation: generation}
  end

  defp refresh_stream_advisory_support(state, {person, origin, handle} = stream_key) do
    case Map.get(state.origin_streams, stream_key) do
      %{advisory_requested: requested} = stream ->
        enabled =
          requested and
            BounceDecision.advisory_supported?(state.store.engine, person, origin, handle)

        %{
          state
          | origin_streams:
              Map.put(state.origin_streams, stream_key, %{stream | advisory_v1: enabled})
        }

      _ ->
        state
    end
  end

  defp record_advisory_feed_change(state, old_engine, next_engine, changed_keys) do
    BounceDecision.advisory_feed_keys_for_changes(next_engine, changed_keys)
    |> Enum.reduce(state, fn {_person, _origin, handle} = key, current ->
      old_high = get_in(old_engine.advisory_feeds, [key, :high]) || 0

      case Map.get(next_engine.advisory_feeds, key) do
        %{high: high} when high > old_high ->
          case Map.get(current.origin_streams, key) do
            %{worker: worker, advisory_v1: true} ->
              send(worker, {:origin_advisory_changed, handle, high})

            _ ->
              :ok
          end

        _ ->
          :ok
      end

      current
    end)
  end

  # Origin-feed changes are announced only by the request paths that append
  # them. This keeps unrelated commits out of every attached origin stream.
  defp commit(store, next, audit) do
    result = Store.commit(store, next, audit)

    if match?({:ok, _}, result) and store.engine.messages != next.messages do
      send(self(), :refresh_default_sweep_index)
    end

    result
  end

  defp origin_feed_targets(
         old_engine,
         next_engine,
         key,
         %{"type" => "forward", "message" => %{"sender_id" => sender, "supersedes" => ids}} =
           request
       )
       when is_list(ids) do
    case origin_feed_target(next_engine, key, request) do
      {person, _origin} = own ->
        target_origins =
          Enum.flat_map(ids, fn id ->
            if is_binary(id) do
              case Map.get(old_engine.messages, {person, sender, id}) do
                %{origin: origin, terminal: nil} -> [{person, origin}]
                _ -> []
              end
            else
              []
            end
          end)

        Enum.uniq([own | target_origins])

      nil ->
        []
    end
  end

  defp origin_feed_targets(_old_engine, next_engine, key, request) do
    case origin_feed_target(next_engine, key, request) do
      nil -> []
      target -> [target]
    end
  end

  defp origin_feed_target(engine, key, request) do
    case Engine.authorized_person(engine, key) do
      {:ok, person} ->
        case Map.get(request, "type") do
          "forward" ->
            {person, key}

          "answer_no_comment" ->
            request_id = Map.get(request, "request_id")

            matches =
              for {{^person, _sender, ^request_id} = message_key, record} <- engine.messages,
                  do: {message_key, record}

            case matches do
              [{{^person, _sender, ^request_id}, %{origin: origin}}] -> {person, origin}
              _ -> nil
            end

          _ ->
            sender = Map.get(request, "sender_id")
            message_id = Map.get(request, "message_id")

            case Map.get(engine.messages, {person, sender, message_id}) do
              %{origin: origin} -> {person, origin}
              _ -> nil
            end
        end

      _ ->
        nil
    end
  end

  defp origin_feed_target_for_person(engine, person, request) do
    sender = Map.get(request, "sender_id")
    message_id = Map.get(request, "message_id")

    case Map.get(engine.messages, {person, sender, message_id}) do
      %{origin: origin} -> {person, origin}
      _ -> nil
    end
  end

  defp record_origin_feed_change(state, old_engine, next_engine, {person, origin}) do
    key = {person, origin}
    old_feed = Map.get(old_engine.origin_feeds, key, %{high: 0, entries: %{}})
    next_feed = Map.get(next_engine.origin_feeds, key, %{high: 0, entries: %{}})

    if next_feed.high > old_feed.high do
      new_ids =
        (old_feed.high + 1)..next_feed.high
        |> Enum.flat_map(fn seq ->
          next_feed.entries
          |> Map.fetch!(seq)
          |> Map.get("files", [])
          |> Enum.map(&Map.get(&1, "upload_id"))
          |> Enum.reject(&is_nil/1)
        end)
        |> MapSet.new()

      upload_index = add_upload_references(state.upload_index, person, new_ids)

      state.origin_streams_by_identity
      |> Map.get(key, MapSet.new())
      |> Enum.each(fn stream_key ->
        case Map.get(state.origin_streams, stream_key) do
          %{worker: worker} -> send(worker, {:origin_state_changed, next_feed.high})
          _ -> :ok
        end
      end)

      %{state | upload_index: upload_index}
    else
      state
    end
  end

  defp record_origin_feed_change(state, _old_engine, _next_engine, _target), do: state

  defp add_upload_references(index, person, ids) do
    if MapSet.size(ids) == 0 do
      index
    else
      references_by_person =
        Map.update(index.references_by_person, person, ids, &MapSet.union(&1, ids))

      %{
        index
        | references_by_person: references_by_person,
          unsubmitted_agent_upload_ids: MapSet.difference(index.unsubmitted_agent_upload_ids, ids)
      }
    end
  end

  defp audit(engine, key, action),
    do: %{person: person_of(engine, key), actor: key, action: action}

  defp person_of(engine, key) do
    case get_in(engine.devices, [key, :person]) do
      nil ->
        case BrowserRecipient.person(key) do
          {:ok, person} -> if(Map.has_key?(engine.persons, person), do: person, else: nil)
          _ -> nil
        end

      person ->
        person
    end
  end

  defp random_uuid_v4 do
    <<a::binary-size(6), b::8, c::8, d::8, e::binary-size(7)>> = :crypto.strong_rand_bytes(16)
    b = Bitwise.bor(Bitwise.band(b, 15), 64)
    d = Bitwise.bor(Bitwise.band(d, 63), 128)
    hex = Base.encode16(<<a::binary, b, c, d, e::binary>>, case: :lower)

    Enum.join(
      [
        String.slice(hex, 0, 8),
        String.slice(hex, 8, 4),
        String.slice(hex, 12, 4),
        String.slice(hex, 16, 4),
        String.slice(hex, 20, 12)
      ],
      "-"
    )
  end

  defp request_action(raw) do
    case JSON.decode_line(raw) do
      {:ok, %{"type" => type}}
      when type in [
             "forward",
             "answer",
             "lifecycle",
             "subscribe_remote",
             "source_bind",
             "source_result",
             "push_register",
             "push_remove"
           ] ->
        type

      _ ->
        "relay_request"
    end
  end

  defp envelope_error(frame) do
    cond do
      Map.has_key?(frame, "ref") and not is_binary(frame["ref"]) -> {"invalid_field", "ref"}
      frame["v"] != 1 or not is_integer(frame["v"]) -> {"unsupported_version", nil}
      true -> nil
    end
  end

  defp ok_line(frame, fields), do: line(Map.merge(base("ok", frame), fields))

  defp error_line(%{} = frame, reason, field) do
    base("error", frame)
    |> Map.put("reason", reason)
    |> then(fn result -> if field, do: Map.put(result, "field", field), else: result end)
    |> line()
  end

  defp error_line(%{} = frame, reason), do: error_line(frame, reason, nil)

  defp error_line(raw, reason) when is_binary(raw) do
    case JSON.decode_line(raw) do
      {:ok, %{} = frame} -> error_line(frame, reason)
      _ -> error_line(%{}, reason)
    end
  end

  defp base(type, frame) do
    result = %{"v" => 1, "type" => type}
    if is_binary(Map.get(frame, "ref")), do: Map.put(result, "ref", frame["ref"]), else: result
  end

  defp line(frame), do: JSON.encode!(frame) <> "\n"

  defp put_session(state, id, session) do
    state = %{state | sessions: Map.put(state.sessions, id, session)}

    if session.transport == :browser do
      recipient_ref = {session.browser_session_id, session.recipient}
      recipient = %{connection_id: id, session: session}

      %{
        state
        | browser_recipients: Map.put(state.browser_recipients, recipient_ref, recipient)
      }
    else
      state
    end
  end

  defp send_frames(socket, frames, transport) do
    Enum.reduce_while(frames, :ok, fn frame, _ ->
      bytes =
        if transport == :websocket,
          do: AttentionServer.WebSocketRelay.encode_text(frame),
          else: frame

      case :ssl.send(socket, bytes) do
        :ok -> {:cont, :ok}
        _ -> {:halt, {:error, :send_failed}}
      end
    end)
  end

  # TLS writes occur inside this GenServer. Re-evaluate each successful G8
  # observation immediately before its serialized send, so a queued target or
  # grant revocation wins when it committed before response release.
  defp refresh_enrollment_frames(engine, key, frames) do
    Enum.map(frames, fn line ->
      case JSON.decode_line(line) do
        {:ok, %{"type" => "ok", "proof" => %{"proof_version" => 1} = proof} = reply} ->
          request = %{
            "v" => 1,
            "type" => "enrollment_readback",
            "proof_version" => 1,
            "grant_id" => proof["grant_id"],
            "source_instance_id" => proof["source_instance_id"],
            "target" => proof["target"]
          }

          request =
            if Map.has_key?(reply, "ref"),
              do: Map.put(request, "ref", reply["ref"]),
              else: request

          case Engine.request_line(engine, key, JSON.encode!(request) <> "\n") do
            {^engine, {:ok, refreshed}} -> refreshed
            _ -> JSON.encode!(%{"v" => 1, "type" => "error", "reason" => "not_found"}) <> "\n"
          end

        _ ->
          line
      end
    end)
  end

  defp fence_sessions(state, key) do
    Enum.reduce(state.sessions, state, fn {id, session}, acc ->
      if session.key == key, do: drop_session(acc, id), else: acc
    end)
  end

  defp unregister_listener(state, listener) do
    case Map.pop(state.tls_listeners, listener) do
      {nil, _} ->
        state

      {monitor, listeners} ->
        Process.demonitor(monitor, [:flush])

        Enum.reduce(state.sessions, %{state | tls_listeners: listeners}, fn {id, session}, acc ->
          if session.listener == listener, do: drop_session(acc, id), else: acc
        end)
    end
  end

  defp unregister_origin_listener_state(state, listener) do
    case state.origin_listener do
      %{pid: ^listener, monitor: monitor} ->
        Process.demonitor(monitor, [:flush])
        %{state | origin_listener: nil}

      _ ->
        state
    end
  end

  defp focus_authorized_person(engine, key, permission) do
    with {:ok, person} <- Engine.authorized_person(engine, key),
         %{person: ^person, revoked: false} = device <- Map.get(engine.devices, key),
         true <- Map.get(device, focus_permission_field(permission), false) do
      {:ok, person}
    else
      _ -> {:error, :forbidden}
    end
  end

  defp focus_permission_field(:see), do: :see_focus
  defp focus_permission_field(:push), do: :push_view

  defp focus_list(state, person) do
    client_keys =
      state.focus_clients_by_person
      |> Map.get(person, MapSet.new())
      |> MapSet.to_list()

    focus_list(
      state.store.engine,
      Map.take(state.focus_clients, client_keys),
      person,
      state.clock.()
    )
  end

  defp focus_list(engine, focus_clients, person, now) do
    focus_clients
    |> Enum.filter(fn {key, focus} ->
      case Map.get(engine.devices, key) do
        %{person: ^person, revoked: false} ->
          focus.connected or
            (is_integer(focus.last_disconnected_at) and
               now - focus.last_disconnected_at < @focus_recent_ms)

        _ ->
          false
      end
    end)
    |> Enum.map(fn {key, focus} ->
      device = Map.fetch!(engine.devices, key)

      label =
        case Map.get(device, :label) do
          value when is_binary(value) and value != "" -> value
          _ -> "Device " <> String.slice(key, 0, 8)
        end

      %{
        "client_id" => key,
        "device_name" => label,
        "screen" => focus.screen,
        "reported_at" => focus.reported_at,
        "in_front" =>
          focus.connected and focus.screen in ["inbox", "message", "chat", "settings"],
        "connected" => focus.connected
      }
      |> maybe_focus_field(focus, :sender_id, "sender_id")
      |> maybe_focus_field(focus, :message_id, "message_id")
      |> maybe_focus_field(focus, :visible_event_id, "visible_event_id")
    end)
    |> Enum.sort_by(fn client -> {-client["reported_at"], client["client_id"]} end)
  end

  defp maybe_focus_field(client, focus, source, target) do
    case Map.fetch(focus, source) do
      {:ok, value} -> Map.put(client, target, value)
      :error -> client
    end
  end

  defp notify_focus_change(old_state, new_state, person) do
    if focus_list(old_state, person) == focus_list(new_state, person),
      do: new_state,
      else: notify_focus_streams(new_state, person)
  end

  defp notify_focus_streams(state, person) do
    data = %{"clients" => focus_list(state, person)}

    state.focus_streams_by_person
    |> Map.get(person, MapSet.new())
    |> Enum.each(fn worker ->
      case Map.get(state.focus_streams, worker) do
        nil -> :ok
        _stream -> send(worker, {:focus_change, data})
      end
    end)

    state
  end

  defp set_focus_connected(state, device_key, true) do
    case Map.get(state.focus_clients, device_key) do
      nil ->
        state

      focus ->
        next =
          state
          |> cancel_focus_expiry(device_key)
          |> then(fn current ->
            updated = %{focus | connected: true}
            put_focus_client(current, device_key, updated)
          end)

        person = Map.get(focus, :person) || person_for_device(state.store.engine, device_key)
        if person, do: notify_focus_change(state, next, person), else: next
    end
  end

  defp set_focus_connected(state, device_key, false) do
    case Map.get(state.focus_clients, device_key) do
      nil ->
        state

      %{connected: false} ->
        state

      focus ->
        disconnected_at = state.clock.()
        updated = %{focus | connected: false, last_disconnected_at: disconnected_at}

        next =
          put_focus_client(state, device_key, updated)
          |> schedule_focus_expiry(device_key, disconnected_at)

        person = Map.get(focus, :person) || person_for_device(state.store.engine, device_key)
        if person, do: notify_focus_change(state, next, person), else: next
    end
  end

  defp person_for_device(engine, device_key) do
    case Map.get(engine.devices, device_key) do
      %{person: person} -> person
      _ -> nil
    end
  end

  defp schedule_focus_expiry(state, device_key, disconnected_at) do
    state = cancel_focus_expiry(state, device_key)
    generation = Map.get(state.focus_expiry_generations, device_key, 0) + 1
    delay = max(disconnected_at + @focus_recent_ms - state.clock.(), 0)
    timer = Process.send_after(self(), {:focus_expire, device_key, generation}, delay)

    %{
      state
      | focus_expiry_timers: Map.put(state.focus_expiry_timers, device_key, timer),
        focus_expiry_generations: Map.put(state.focus_expiry_generations, device_key, generation)
    }
  end

  defp cancel_focus_expiry(state, device_key) do
    case Map.pop(state.focus_expiry_timers, device_key) do
      {nil, _} ->
        state

      {timer, timers} ->
        Process.cancel_timer(timer)
        %{state | focus_expiry_timers: timers}
    end
  end

  defp close_focus_streams(state, agent_key) do
    state.focus_streams
    |> Enum.filter(fn {_worker, stream} -> stream.key == agent_key end)
    |> Enum.reduce(state, fn {worker, stream}, current ->
      Process.demonitor(stream.monitor, [:flush])
      send(worker, :focus_forbidden)
      delete_focus_stream(current, worker)
    end)
  end

  defp put_focus_client(state, key, focus) do
    old_person =
      case Map.get(state.focus_clients, key) do
        %{person: person} -> person
        _ -> nil
      end

    new_person = Map.get(focus, :person)

    index =
      state.focus_clients_by_person
      |> remove_person_index(old_person, key)
      |> add_person_index(new_person, key)

    %{
      state
      | focus_clients: Map.put(state.focus_clients, key, focus),
        focus_clients_by_person: index
    }
  end

  defp delete_focus_client(state, key) do
    case Map.pop(state.focus_clients, key) do
      {nil, _clients} ->
        state

      {focus, clients} ->
        index = remove_person_index(state.focus_clients_by_person, Map.get(focus, :person), key)
        %{state | focus_clients: clients, focus_clients_by_person: index}
    end
  end

  defp put_focus_stream(state, worker, stream) do
    old_person =
      case Map.get(state.focus_streams, worker) do
        %{person: person} -> person
        _ -> nil
      end

    index =
      state.focus_streams_by_person
      |> remove_person_index(old_person, worker)
      |> add_person_index(stream.person, worker)

    %{
      state
      | focus_streams: Map.put(state.focus_streams, worker, stream),
        focus_streams_by_person: index
    }
  end

  defp delete_focus_stream(state, worker) do
    case Map.pop(state.focus_streams, worker) do
      {nil, _streams} ->
        state

      {stream, streams} ->
        index = remove_person_index(state.focus_streams_by_person, stream.person, worker)
        %{state | focus_streams: streams, focus_streams_by_person: index}
    end
  end

  defp add_person_index(index, nil, _key), do: index

  defp add_person_index(index, person, key),
    do: Map.update(index, person, MapSet.new([key]), &MapSet.put(&1, key))

  defp remove_person_index(index, nil, _key), do: index

  defp remove_person_index(index, person, key) do
    case Map.get(index, person) do
      nil ->
        index

      keys ->
        remaining = MapSet.delete(keys, key)

        if MapSet.size(remaining) == 0,
          do: Map.delete(index, person),
          else: Map.put(index, person, remaining)
    end
  end

  defp remove_focus_device(state, device_key, person) do
    focus = Map.get(state.focus_clients, device_key)

    person =
      person ||
        case focus do
          %{person: focus_person} -> focus_person
          _ -> person_for_device(state.store.engine, device_key)
        end

    state = cancel_focus_expiry(state, device_key)
    next = state |> delete_focus_client(device_key)
    next = %{next | held_shows: Map.delete(next.held_shows, device_key)}
    next = close_focus_streams(next, device_key)

    if person, do: notify_focus_streams(next, person), else: next
  end

  defp focus_report_request(state, id, session, frame) do
    case envelope_error(frame) || focus_report_error(frame) do
      {reason, field} ->
        {{:ok, [error_line(frame, reason, field)]}, state}

      nil ->
        now = state.clock.()
        key = session.key
        person = person_for_device(state.store.engine, key)
        reported = Map.take(frame, ~w(screen sender_id message_id visible_event_id))
        existing = Map.get(state.focus_clients, key)
        changed = is_nil(existing) or focus_fields(existing) != reported

        reported_at =
          cond do
            is_nil(existing) -> now
            changed and reported["screen"] in ["inbox", "message", "chat", "settings"] -> now
            true -> existing.reported_at
          end

        focus =
          reported
          |> Map.new(fn {name, value} -> {String.to_atom(name), value} end)
          |> Map.put(:reported_at, reported_at)
          |> Map.put(
            :last_disconnected_at,
            Map.get(existing || %{}, :last_disconnected_at)
          )
          |> Map.put(:person, person)
          |> Map.put(:connected, true)

        next =
          state
          |> put_focus_client(key, focus)
          |> put_session(id, %{session | focus_opted_in: true})
          |> cancel_focus_expiry(key)

        next = if person, do: notify_focus_change(state, next, person), else: next

        ack = ok_line(frame, %{})

        case Map.get(next.held_shows, key) do
          nil ->
            {{:ok, [ack]}, next}

          held ->
            show = held_show_frame(held, now)
            frames = [ack, line(show)]

            if session.socket do
              case send_frames(session.socket, frames, session.transport) do
                :ok ->
                  next = %{next | held_shows: Map.delete(next.held_shows, key)}
                  {{:ok, []}, next}

                _ ->
                  {{:error, :send_failed}, drop_session(next, id)}
              end
            else
              {{:ok, frames}, %{next | held_shows: Map.delete(next.held_shows, key)}}
            end
        end
    end
  end

  defp focus_fields(focus) do
    focus
    |> Map.take([:screen, :sender_id, :message_id, :visible_event_id])
    |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)
  end

  defp focus_report_error(frame) do
    screen = frame["screen"]
    pair_screen = screen in ["message", "chat"]

    cond do
      not is_binary(frame["ref"]) ->
        {"invalid_field", "ref"}

      screen not in ["inbox", "message", "chat", "settings", "signed_out", "not_in_front"] ->
        {"invalid_field", "screen"}

      pair_screen and not Map.has_key?(frame, "sender_id") ->
        {"invalid_field", "sender_id"}

      pair_screen and not Message.sender_id?(frame["sender_id"]) ->
        {"invalid_field", "sender_id"}

      pair_screen and not Map.has_key?(frame, "message_id") ->
        {"invalid_field", "message_id"}

      pair_screen and not Message.nonempty?(frame["message_id"]) ->
        {"invalid_field", "message_id"}

      not pair_screen and Map.has_key?(frame, "sender_id") ->
        {"invalid_field", "sender_id"}

      not pair_screen and Map.has_key?(frame, "message_id") ->
        {"invalid_field", "message_id"}

      Map.has_key?(frame, "visible_event_id") and screen != "chat" ->
        {"invalid_field", "visible_event_id"}

      Map.has_key?(frame, "visible_event_id") and
          not (is_binary(frame["visible_event_id"]) and
                   Regex.match?(~r/\A[0-9]+\z/, frame["visible_event_id"])) ->
        {"invalid_field", "visible_event_id"}

      true ->
        nil
    end
  end

  defp handle_view_show(state, key, request) do
    case focus_authorized_person(state.store.engine, key, :push) do
      {:ok, person} -> validate_and_send_show(state, person, key, request)
      error -> {error, state}
    end
  end

  defp validate_and_send_show(state, person, key, request) do
    cond do
      not Message.sender_id?(request["sender_id"]) ->
        {{:error, {:invalid_field, "sender_id"}}, state}

      not Message.nonempty?(request["message_id"]) ->
        {{:error, {:invalid_field, "message_id"}}, state}

      request["target"] not in ["message", "chat"] ->
        {{:error, {:invalid_field, "target"}}, state}

      Map.has_key?(request, "target_client") and Map.has_key?(request, "target_device_name") ->
        {{:error, {:invalid_field, "target_client"}}, state}

      Map.has_key?(request, "target_client") and
          not (is_binary(request["target_client"]) and
                   Regex.match?(~r/\A[0-9a-f]{64}\z/, request["target_client"])) ->
        {{:error, {:invalid_field, "target_client"}}, state}

      Map.has_key?(request, "target_device_name") and
          not Message.nonempty?(request["target_device_name"]) ->
        {{:error, {:invalid_field, "target_device_name"}}, state}

      not Map.has_key?(
        state.store.engine.messages,
        {person, request["sender_id"], request["message_id"]}
      ) ->
        {{:error, {:not_found, "message_id"}}, state}

      true ->
        clients = focus_list(state, person)

        target =
          cond do
            is_binary(request["target_client"]) ->
              Enum.find(clients, &(&1["client_id"] == request["target_client"]))

            is_binary(request["target_device_name"]) ->
              wanted = normalized_device_name(request["target_device_name"])
              Enum.find(clients, &(normalized_device_name(&1["device_name"]) == wanted))

            true ->
              List.first(clients)
          end

        case target do
          nil ->
            {{:error, {:not_found, "target_client"}}, state}

          client ->
            send_or_hold_show(state, key, client, request)
        end
    end
  end

  defp normalized_device_name(name),
    do: name |> String.replace("’", "'") |> String.downcase()

  defp send_or_hold_show(state, agent_key, client, request) do
    client_key = client["client_id"]

    show = %{
      "v" => 1,
      "type" => "view_show",
      "sender_id" => request["sender_id"],
      "message_id" => request["message_id"],
      "target" => request["target"]
    }

    live_session =
      Enum.find(state.sessions, fn {_id, session} ->
        session.key == client_key and session.focus_opted_in
      end)

    case live_session do
      {id, %{socket: socket, transport: transport}} when not is_nil(socket) ->
        case send_frames(socket, [line(show)], transport) do
          :ok ->
            {{:ok,
              %{
                "client_id" => client["client_id"],
                "device_name" => client["device_name"],
                "delivery" => "sent"
              }}, state}

          _ ->
            next = drop_session(state, id)
            hold_show(next, agent_key, client, show)
        end

      _ ->
        hold_show(state, agent_key, client, show)
    end
  end

  defp hold_show(state, _agent_key, client, show) do
    held = Map.put(show, "accepted_at", state.clock.())
    next = %{state | held_shows: Map.put(state.held_shows, client["client_id"], held)}

    {{:ok,
      %{
        "client_id" => client["client_id"],
        "device_name" => client["device_name"],
        "delivery" => "held"
      }}, next}
  end

  defp held_show_frame(held, now) do
    held
    |> Map.drop(["accepted_at"])
    |> Map.put("held_ms", max(now - held["accepted_at"], 0))
  end

  defp drop_session(state, id) do
    case Map.pop(state.sessions, id) do
      {nil, _} ->
        state

      {session, sessions} ->
        if session.owner, do: send(session.owner, {:attention_fenced, id})

        # Removing the session fences all serialized requests and writes.
        # TLS close can wait on the peer; keep that wait out of the shared Relay.
        if session.socket, do: spawn(fn -> :ssl.close(session.socket) end)

        state = %{state | sessions: sessions}

        if session.transport == :browser do
          recipient_ref = {session.browser_session_id, session.recipient}
          saved = %{connection_id: nil, session: %{session | owner: nil}}
          %{state | browser_recipients: Map.put(state.browser_recipients, recipient_ref, saved)}
        else
          set_focus_connected(state, session.key, false)
        end
    end
  end

  defp drop_browser_connection(state, id, :replaced) do
    case Map.pop(state.sessions, id) do
      {nil, _} ->
        state

      {session, sessions} ->
        if session.owner, do: send(session.owner, {:browser_connection_replaced, id})
        recipient_ref = {session.browser_session_id, session.recipient}
        saved = %{connection_id: nil, session: %{session | owner: nil}}

        %{
          state
          | sessions: sessions,
            browser_recipients: Map.put(state.browser_recipients, recipient_ref, saved)
        }
    end
  end

  defp cleanup_browser_recipient_state(state, actor_keys) do
    actor_keys = MapSet.new(actor_keys)

    state =
      state.inbox_builds
      |> Enum.filter(fn {_key, build} ->
        MapSet.member?(actor_keys, build.snapshot.device_id)
      end)
      |> Enum.reduce(state, fn {key, _build}, acc ->
        fail_inbox_build(acc, key, :forbidden, true)
      end)

    engine = discard_browser_actor_state(state.store.engine, actor_keys)

    case commit(state.store, engine, %{
           person: "system",
           actor: "browser_session",
           action: "browser_session_end"
         }) do
      {:ok, store} ->
        next = %{state | store: store}

        Enum.reduce_while(actor_keys, {:ok, next}, fn key, {:ok, acc} ->
          case BrowserRecipient.person(key) do
            {:ok, person} ->
              case Store.discard_inbox_snapshots_for_device(acc.store, person, key) do
                :ok -> {:cont, {:ok, acc}}
                {:error, reason} -> {:halt, {:error, reason, acc}}
              end

            _ ->
              {:cont, {:ok, acc}}
          end
        end)

      {:error, reason, store} ->
        {:error, reason, %{state | store: store}}
    end
  end

  defp browser_actor_keys(engine) do
    keys =
      Enum.map(engine.cursors, fn {{_person, key}, _seq} -> key end) ++
        Enum.map(engine.conversation_cursors, fn {{_person, key}, _seq} -> key end) ++
        Enum.map(engine.conversation_opened, fn {_person, key} -> key end) ++
        Enum.map(engine.receipts, fn {{_person, key, _op_id}, _receipt} -> key end) ++
        Enum.map(engine.uploads, fn {_id, upload} -> upload.uploader end)

    keys
    |> Enum.uniq()
    |> Enum.filter(&(BrowserRecipient.person(&1) != :error))
  end

  defp discard_browser_actor_state(engine, actor_keys) do
    actor_keys = MapSet.new(actor_keys)

    %{
      engine
      | cursors:
          Map.reject(engine.cursors, fn {{_person, actor}, _seq} ->
            MapSet.member?(actor_keys, actor)
          end),
        conversation_cursors:
          Map.reject(engine.conversation_cursors, fn {{_person, actor}, _seq} ->
            MapSet.member?(actor_keys, actor)
          end),
        conversation_opened:
          MapSet.filter(engine.conversation_opened, fn {_person, actor} ->
            not MapSet.member?(actor_keys, actor)
          end),
        receipts:
          Map.reject(engine.receipts, fn {{_person, actor, _op_id}, _receipt} ->
            MapSet.member?(actor_keys, actor)
          end)
    }
  end

  defp wrap_line({:ok, line}), do: {:ok, [line]}
  defp wrap_line(error), do: error

  defp upload_reply(upload, duplicate) do
    %{
      "upload_id" => upload.id,
      "name" => upload.name,
      "size" => upload.size,
      "sha256" => upload.sha256,
      "duplicate" => duplicate
    }
  end

  defp valid_upload_attributes?(attrs) when is_map(attrs) do
    Message.uuid_v4?(attrs["op_id"]) and
      Message.sender_id?(attrs["sender_id"]) and
      Message.nonempty?(attrs["message_id"]) and
      is_integer(attrs["revision"]) and attrs["revision"] > 0 and
      ConversationFiles.valid_name?(attrs["name"])
  end

  defp valid_upload_attributes?(_), do: false

  defp valid_discard_attributes?(attrs) when is_map(attrs) do
    Message.uuid_v4?(attrs["op_id"]) and
      Message.sender_id?(attrs["sender_id"]) and
      Message.nonempty?(attrs["message_id"]) and
      is_integer(attrs["revision"]) and attrs["revision"] > 0
  end

  defp valid_discard_attributes?(_), do: false

  defp upload_group(person, key, attrs) do
    {person, key, attrs["op_id"], attrs["sender_id"], attrs["message_id"], attrs["revision"]}
  end

  defp upload_group(upload) do
    {upload.person, upload.uploader, upload.op_id, upload.sender_id, upload.message_id,
     upload.revision}
  end

  defp upload_request_scope(person, key, attrs) do
    {person, key, attrs["sender_id"], attrs["message_id"], attrs["revision"]}
  end

  defp upload_request_scope(upload) when is_map(upload) do
    {upload.person, upload.uploader, upload.sender_id, upload.message_id, upload.revision}
  end

  defp upload_request_scope({person, key, _op_id, sender_id, message_id, revision}) do
    {person, key, sender_id, message_id, revision}
  end

  defp upload_lease_status(_state, nil, _group), do: :ok

  defp upload_lease_status(state, lease, group) do
    case Map.get(state.active_uploads, lease) do
      %{group: ^group, canceled: false} -> :ok
      %{group: ^group, canceled: true} -> {:error, :upload_discarded}
      _ -> {:error, :invalid_upload}
    end
  end

  defp touch_upload_group(store, upload_index, group) do
    engine = store.engine
    now_ms = System.system_time(:millisecond)
    ids = Map.get(upload_index.ids_by_group, group, MapSet.new())

    uploads =
      Enum.reduce(ids, engine.uploads, fn id, acc ->
        case Map.get(acc, id) do
          %{last_activity_ms: _} = upload
          when is_map(upload) ->
            if MapSet.member?(upload_index.unsubmitted_agent_upload_ids, id),
              do: Map.put(acc, id, %{upload | last_activity_ms: now_ms}),
              else: acc

          _ ->
            acc
        end
      end)

    case commit(store, %{engine | uploads: uploads}, %{
           person: elem(group, 0),
           actor: elem(group, 1),
           action: "upload_progress"
         }) do
      {:ok, updated} -> {:ok, updated}
      {:error, reason, _} -> {:error, reason}
    end
  end

  defp agent_upload?(engine, upload) do
    case Map.get(upload, :uploader_kind) do
      "agent" ->
        true

      "client" ->
        false

      _ ->
        Engine.upload_source(
          engine,
          upload.person,
          upload.uploader,
          upload.sender_id,
          upload.message_id
        ) == :agent
    end
  end

  defp sweep_agent_uploads(state, now_ms) do
    engine = state.store.engine

    active_groups =
      state.active_uploads
      |> Map.values()
      |> Enum.map(& &1.group)
      |> MapSet.new()

    groups =
      Enum.reduce(state.upload_index.unsubmitted_agent_upload_ids, %{}, fn id, groups ->
        case Map.get(engine.uploads, id) do
          %{last_activity_ms: last_activity_ms} = upload ->
            group = upload_group(upload)

            Map.update(groups, group, {[id], last_activity_ms}, fn {ids, latest} ->
              {[id | ids], max(latest, last_activity_ms)}
            end)

          _ ->
            groups
        end
      end)

    expired =
      for {group, {ids, latest}} <- groups,
          not MapSet.member?(active_groups, group),
          now_ms - latest >= @agent_upload_idle_ms,
          id <- ids,
          do: id

    if expired == [] do
      {:ok, state, 0}
    else
      case Store.delete_uploads(state.store, expired, %{
             person: "system",
             actor: "system",
             action: "upload_sweep"
           }) do
        {:ok, store} ->
          removed = removed_upload_ids(engine, store.engine, expired)
          index = remove_uploads_from_index(state.upload_index, engine, removed)
          {:ok, %{state | store: store, upload_index: index}, length(expired)}

        {:error, reason, store} ->
          removed = removed_upload_ids(engine, store.engine, expired)
          index = remove_uploads_from_index(state.upload_index, engine, removed)
          {:error, reason, %{state | store: store, upload_index: index}}
      end
    end
  end

  defp referenced_upload?(upload_index, person, id) do
    upload_index.references_by_person
    |> Map.get(person, MapSet.new())
    |> MapSet.member?(id)
  end

  defp fetched_file_for_person(state, person, id) do
    engine = state.store.engine

    with %{person: ^person} = upload <- Map.get(engine.uploads, id),
         true <- referenced_upload?(state.upload_index, person, id),
         {:ok, path} <- Store.get_upload(state.store, id) do
      {:ok, upload, path}
    else
      {:error, :storage_unavailable} -> {:error, :storage_unavailable}
      _ -> {:error, :not_found}
    end
  end

  defp upload_source(state, person, key, attrs, lease) do
    group = upload_group(person, key, attrs)

    case Map.get(state.active_uploads, lease) do
      %{group: ^group, uploader_kind: kind} ->
        kind

      _ ->
        state.store.engine
        |> Engine.upload_source(person, key, attrs["sender_id"], attrs["message_id"])
        |> Atom.to_string()
    end
  end

  defp build_upload_index(engine) do
    empty = %{
      references_by_person: %{},
      ids_by_group: %{},
      ids_by_request_scope: %{},
      unsubmitted_agent_upload_ids: MapSet.new()
    }

    references =
      Enum.reduce(engine.origin_feeds, empty, fn {{person, _origin}, feed}, index ->
        ids =
          Enum.reduce(feed.entries, MapSet.new(), fn {_seq, entry}, found ->
            Enum.reduce(Map.get(entry, "files", []), found, fn file, acc ->
              case Map.get(file, "upload_id") do
                id when is_binary(id) -> MapSet.put(acc, id)
                _ -> acc
              end
            end)
          end)

        add_upload_references(index, person, ids)
      end)

    Enum.reduce(engine.uploads, references, fn {_id, upload}, index ->
      add_upload_to_index(index, upload, engine)
    end)
  end

  defp add_upload_to_index(index, upload), do: add_upload_to_index(index, upload, nil)

  defp add_upload_to_index(index, upload, engine) do
    id = upload.id
    group = upload_group(upload)
    scope = upload_request_scope(upload)

    index = %{
      index
      | ids_by_group: put_index_id(index.ids_by_group, group, id),
        ids_by_request_scope: put_index_id(index.ids_by_request_scope, scope, id)
    }

    is_agent =
      case Map.get(upload, :uploader_kind) do
        "agent" -> true
        "client" -> false
        _ when is_map(engine) -> agent_upload?(engine, upload)
        _ -> false
      end

    if is_agent and not referenced_upload?(index, upload.person, id),
      do: %{
        index
        | unsubmitted_agent_upload_ids: MapSet.put(index.unsubmitted_agent_upload_ids, id)
      },
      else: index
  end

  defp remove_uploads_from_index(index, engine, ids) do
    Enum.reduce(ids, index, fn id, current ->
      case Map.get(engine.uploads, id) do
        nil ->
          current

        upload ->
          group = upload_group(upload)
          scope = upload_request_scope(upload)

          %{
            current
            | ids_by_group: remove_index_id(current.ids_by_group, group, id),
              ids_by_request_scope: remove_index_id(current.ids_by_request_scope, scope, id),
              unsubmitted_agent_upload_ids:
                MapSet.delete(current.unsubmitted_agent_upload_ids, id)
          }
      end
    end)
  end

  defp removed_upload_ids(old_engine, next_engine, ids) do
    Enum.filter(
      ids,
      &(Map.has_key?(old_engine.uploads, &1) and not Map.has_key?(next_engine.uploads, &1))
    )
  end

  defp put_index_id(index, key, id),
    do: Map.update(index, key, MapSet.new([id]), &MapSet.put(&1, id))

  defp remove_index_id(index, key, id) do
    case Map.get(index, key) do
      nil ->
        index

      ids ->
        remaining = MapSet.delete(ids, id)

        if MapSet.size(remaining) == 0,
          do: Map.delete(index, key),
          else: Map.put(index, key, remaining)
    end
  end
end
