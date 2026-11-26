defmodule AttentionServer.Relay do
  @moduledoc """
  Serialized relay core with an optional durable SQLite store.

  Authenticated client commands are applied directly to the engine. Devices
  reach the Relay through client API v2 (`ClientAPI.Listener`); the Relay
  keeps no device sessions.
  """

  use GenServer
  require Logger
  @default_sweep_retry_ms 1_000
  @max_timer_delay_ms 4_294_967_295
  @prioritization_debounce_ms 250
  @hosted_test_states ~w(no-devices device-waiting device-enrolled)
  @hosted_test_label "Test account"
  @hosted_test_device "Test device"

  alias AttentionServer.{
    AppliedScoreRecord,
    BrowserAuth,
    BrowserRecipient,
    BounceDecision,
    BouncePolicy,
    ChangeLog,
    Conversation,
    ConversationFiles,
    DashboardInventory,
    Diagnostics,
    DeviceIdentity,
    DeviceProof,
    Engine,
    Message,
    NativeSignIn,
    JevClient,
    JevRanking,
    Placement,
    RouteBIndex,
    Store
  }

  alias AttentionServer.JSON, warn: false

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def setup_person(server, person), do: GenServer.call(server, {:setup_person, person}, :infinity)

  def setup_device(server, person, key),
    do: GenServer.call(server, {:setup_device, person, key}, :infinity)

  @doc "Local-operator bootstrap from a public owner certificate; no network caller uses this API."
  def operator_bootstrap(server, certificate_der) do
    with {:ok, key} <- DeviceIdentity.from_certificate(certificate_der),
         do: GenServer.call(server, {:operator_bootstrap, key}, :infinity)
  end

  @doc "Local-operator owner recovery retains the person and retires the old owner key."
  def operator_recover(server, person, certificate_der) do
    with {:ok, key} <- DeviceIdentity.from_certificate(certificate_der),
         do: GenServer.call(server, {:operator_recover, person, key}, :infinity)
  end

  def owner_revision(server, person, verified_owner_key),
    do: GenServer.call(server, {:owner_revision, person, verified_owner_key}, :infinity)

  @doc "Read device metadata under current owner authority; only a configured self-host owner can see all persons."
  def dashboard_inventory(server, person, owner_key, configured_owner_person),
    do:
      GenServer.call(
        server,
        {:dashboard_inventory, person, owner_key, configured_owner_person},
        :infinity
      )

  def dashboard_prioritization(server, person, owner_key, configured_owner_person),
    do:
      GenServer.call(
        server,
        {:dashboard_prioritization, person, owner_key, configured_owner_person},
        :infinity
      )

  def dashboard_levels(server, person, owner_key),
    do: GenServer.call(server, {:dashboard_levels, person, owner_key}, :infinity)

  def save_dashboard_levels(server, person, owner_key, levels),
    do: GenServer.call(server, {:save_dashboard_levels, person, owner_key, levels}, :infinity)

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
           endpoint, model},
          :infinity
        )

  def owner_identity(server, verified_owner_key),
    do: GenServer.call(server, {:owner_identity, verified_owner_key}, :infinity)

  def agent_identity(server, verified_agent_key),
    do: GenServer.call(server, {:agent_identity, verified_agent_key}, :infinity)

  def focus_stream_attach(server, verified_agent_key, worker),
    do: GenServer.call(server, {:focus_stream_attach, verified_agent_key, worker}, :infinity)

  def upload_begin(server, verified_device_key, attributes, size),
    do: GenServer.call(server, {:upload_begin, verified_device_key, attributes, size}, :infinity)

  def upload_end(server, lease), do: GenServer.call(server, {:upload_end, lease}, :infinity)

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
    GenServer.call(server, {:fetch_file, verified_device_key, upload_id}, :infinity)
    |> verify_fetched_file()
  end

  @doc false
  def fetch_granted_agent_file(server, key, upload_id),
    do: GenServer.call(server, {:fetch_granted_agent_file, key, upload_id}, :infinity)

  def browser_fetch_file(server, person, owner_key, owner_revision, upload_id) do
    GenServer.call(
      server,
      {:browser_fetch_file, person, owner_key, owner_revision, upload_id},
      :infinity
    )
    |> verify_fetched_file()
  end

  defp verify_fetched_file({:ok, upload, path}) do
    case Store.verify_upload(path, upload) do
      :ok -> {:ok, upload, path}
      error -> error
    end
  end

  defp verify_fetched_file(error), do: error

  defp upload_file_outside_relay(server, key, attrs, path, size, hash, lease) do
    case GenServer.call(server, {:upload_prepare, key, attrs, size, hash, lease}, :infinity) do
      {:ok, :duplicate, reply} ->
        {:ok, reply}

      {:ok, upload, upload_dir} ->
        case Store.stage_upload(upload_dir, upload, path) do
          {:ok, staged} ->
            try do
              case Store.publish_staged_upload(upload_dir, upload, staged) do
                {:ok, published} ->
                  GenServer.call(
                    server,
                    {:upload_commit, key, upload, published, lease},
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

  @doc false
  def client_agent_answer(server, key, frame),
    do: GenServer.call(server, {:client_agent_answer, key, frame}, :infinity)

  def owner_register_agent(server, person, owner_key, id, agent_key),
    do:
      GenServer.call(server, {:owner_register_agent, person, owner_key, id, agent_key}, :infinity)

  def owner_replace_agent_grants(server, person, owner_key, agent_key, revision, read, respond),
    do:
      GenServer.call(
        server,
        {:owner_replace_agent_grants, person, owner_key, agent_key, revision, read, respond},
        :infinity
      )

  def owner_revoke_agent(server, person, owner_key, agent_key),
    do: GenServer.call(server, {:owner_revoke_agent, person, owner_key, agent_key}, :infinity)

  def owner_list_agents(server, person, owner_key),
    do: GenServer.call(server, {:owner_list_agents, person, owner_key}, :infinity)

  @doc "Release a bounded owner response in mutation order only while its TLS key/revision is current."
  def owner_send_line(server, person, verified_owner_key, revision, socket, line),
    do:
      GenServer.call(
        server,
        {:owner_send_line, person, verified_owner_key, revision, socket, line},
        :infinity
      )

  @doc "Pending-certificate submission seam for the separate narrow HTTPS listener."
  def pending_enroll(server, certificate_der, label),
    do: GenServer.call(server, {:pending_enroll, certificate_der, label}, :infinity)

  def pending_status(server, status_secret),
    do: GenServer.call(server, {:pending_status, status_secret}, :infinity)

  def pending_resume(server, status_secret, certificate_der),
    do: GenServer.call(server, {:pending_resume, status_secret, certificate_der}, :infinity)

  def pending_cancel(server, secret, code, expected_key, deadline),
    do: GenServer.call(server, {:pending_cancel, secret, code, expected_key, deadline}, :infinity)

  @doc "Key argument must come from TLS CertificateVerify, never a body-supplied identity."
  def device_remove(server, verified_key),
    do: GenServer.call(server, {:device_remove, verified_key}, :infinity)

  # Only cleanup TLS admission uses this retained identity; it grants no relay access.
  def removal_identity(server, key),
    do: GenServer.call(server, {:removal_identity, key}, :infinity)

  @doc "Issue a one-use browser credential under the verified current person's owner."
  def owner_browser_sign_in_create(server, person, verified_owner_key),
    do:
      GenServer.call(
        server,
        {:owner_browser_sign_in_create, person, verified_owner_key},
        :infinity
      )

  @doc "Authenticate a bearer once, returning its stored person-owner principal only after commit."
  def consume_browser_sign_in(server, token),
    do: GenServer.call(server, {:consume_browser_sign_in, token}, :infinity)

  def redeem_browser_fragment(server, token, session_hash),
    do: GenServer.call(server, {:redeem_browser_fragment, token, session_hash}, :infinity)

  def browser_session(server, hash),
    do: GenServer.call(server, {:browser_session, hash}, :infinity)

  def hosted_browser_grant(server, person, transaction, method),
    do: GenServer.call(server, {:hosted_browser_grant, person, transaction, method}, :infinity)

  def redeem_hosted_browser_grant(server, grant, transaction),
    do: GenServer.call(server, {:redeem_hosted_browser_grant, grant, transaction}, :infinity)

  def native_sign_in_methods(server),
    do: GenServer.call(server, :native_sign_in_methods, :infinity)

  def create_native_transaction(server, verifier_hash),
    do: GenServer.call(server, {:create_native_transaction, verifier_hash}, :infinity)

  def redeem_native_transaction(server, request),
    do: GenServer.call(server, {:redeem_native_transaction, request}, :infinity)

  def native_session(server, bearer),
    do: GenServer.call(server, {:native_session, bearer}, :infinity)

  def authorize_hosted_native_transaction(server, person, transaction, method),
    do: GenServer.call(server, {:authorize_hosted_native_transaction, person, transaction, method}, :infinity)

  def owner_invite_create(server, person, owner_key),
    do: GenServer.call(server, {:owner_invite_create, person, owner_key}, :infinity)

  def owner_invite_create(server, person, owner_key, request_id),
    do: GenServer.call(server, {:owner_invite_create, person, owner_key, request_id}, :infinity)

  def dashboard_origin(server), do: GenServer.call(server, :dashboard_origin, :infinity)

  @doc """
  Create, or return, the one person for a hosted account reference. The person's
  owner key is held by the server and never leaves it; the hosted side gets only
  the person identifier.
  """
  def hosted_account_create(server, account_ref),
    do: GenServer.call(server, {:hosted_account_create, account_ref}, :infinity)

  @doc """
  Issue a one-use browser sign-in for a hosted person under its server-held owner
  key, recording the readable label the dashboard shows for it. Refuses any
  person that no hosted account owns.
  """
  def hosted_sign_in_create(server, person, label),
    do: GenServer.call(server, {:hosted_sign_in_create, person, label}, :infinity)

  def hosted_set_algorithm(server, person, algorithm),
    do: GenServer.call(server, {:hosted_set_algorithm, person, algorithm}, :infinity)

  def hosted_delivery_state(server, person, target_state),
    do: GenServer.call(server, {:hosted_delivery_state, person, target_state}, :infinity)

  @doc """
  Put the designated test account's person into a named product state:
  `no-devices`, `device-waiting` or `device-enrolled`. Acts on no other person.
  """
  def hosted_test_reset(server, target_state),
    do: GenServer.call(server, {:hosted_test_reset, target_state}, 30_000)

  def invitation_resolve(server, token),
    do: GenServer.call(server, {:invitation_resolve, token}, :infinity)

  def invited_enroll(server, invite_token, certificate_der, label, status_secret),
    do:
      GenServer.call(
        server,
        {:invited_enroll, invite_token, certificate_der, label, status_secret},
        :infinity
      )

  def owner_pending_decline(server, person, owner_key, code, expected_key, deadline),
    do:
      GenServer.call(
        server,
        {:owner_pending_decline, person, owner_key, code, expected_key, deadline},
        :infinity
      )

  @doc "Owner-key arguments must come from a proved management TLS peer."
  def owner_pending_lookup(server, person, verified_owner_key, code),
    do:
      GenServer.call(server, {:owner_pending_lookup, person, verified_owner_key, code}, :infinity)

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
           expected_expires_at, sender_grants},
          :infinity
        )

  def owner_revoke_device(server, person, verified_owner_key, device_key),
    do:
      GenServer.call(
        server,
        {:owner_revoke_device, person, verified_owner_key, device_key},
        :infinity
      )

  def owner_grant_sender(server, person, verified_owner_key, device_key, pattern),
    do:
      GenServer.call(
        server,
        {:owner_grant_sender, person, verified_owner_key, device_key, pattern},
        :infinity
      )

  def owner_revoke_sender(server, person, verified_owner_key, device_key, pattern),
    do:
      GenServer.call(
        server,
        {:owner_revoke_sender, person, verified_owner_key, device_key, pattern},
        :infinity
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
           target},
          :infinity
        )

  def owner_revoke_enrollment_read(server, person, verified_owner_key, grant_id),
    do:
      GenServer.call(
        server,
        {:owner_revoke_enrollment_read, person, verified_owner_key, grant_id},
        :infinity
      )

  def owner_grant_maintainer(server, person, verified_owner_key, grant_id, grantee, scopes),
    do:
      GenServer.call(
        server,
        {:owner_grant_maintainer, person, verified_owner_key, grant_id, grantee, scopes},
        :infinity
      )

  def owner_revoke_maintainer(server, person, verified_owner_key, grant_id),
    do:
      GenServer.call(
        server,
        {:owner_revoke_maintainer, person, verified_owner_key, grant_id},
        :infinity
      )

  @doc "Local projection fixture only; does not expose agent service authority on the wire."
  def setup_agent_fixture(server, person, id, key, sender_ids),
    do: GenServer.call(server, {:setup_agent_fixture, person, id, key, sender_ids}, :infinity)

  def fixture_agent_answer(server, key, frame),
    do: GenServer.call(server, {:fixture_agent_answer, key, frame}, :infinity)

  def revoke_agent_fixture(server, key),
    do: GenServer.call(server, {:revoke_agent_fixture, key}, :infinity)

  @doc "Local operator/test provisioning of a certificate's public-key identity."
  def setup_device_certificate(server, person, certificate_der) do
    with {:ok, key} <- DeviceIdentity.from_certificate(certificate_der),
         :ok <- setup_device(server, person, key),
         do: {:ok, key}
  end

  def setup_sender(server, key, pattern),
    do: GenServer.call(server, {:setup_sender, key, pattern}, :infinity)

  def revoke_device(server, key), do: GenServer.call(server, {:revoke_device, key}, :infinity)

  @doc "Trusted local fixture seam for an externally recorded source-owner delegation."
  def grant_source_report(server, person, key, source, senders, delegation_ref),
    do:
      GenServer.call(
        server,
        {:grant_source_report, person, key, source, senders, delegation_ref},
        :infinity
      )

  def revoke_source_report(server, person, key, source),
    do: GenServer.call(server, {:revoke_source_report, person, key, source}, :infinity)

  @doc "Trusted local owner fixture for one exact enrollment disclosure grant."
  def grant_enrollment_read_fixture(server, person, connector, grant_id, source, target, issuer),
    do:
      GenServer.call(
        server,
        {:grant_enrollment_read_fixture, person, connector, grant_id, source, target, issuer},
        :infinity
      )

  def revoke_enrollment_read_fixture(server, person, grant_id),
    do: GenServer.call(server, {:revoke_enrollment_read_fixture, person, grant_id}, :infinity)

  if Mix.env() == :test do
    def request_line(server, verified_key, line),
      do: GenServer.call(server, {:request_line, verified_key, line}, :infinity)
  end

  @doc false
  def apply_client_command(server, key, command, sources),
    do: GenServer.call(server, {:client_command, key, command, sources}, :infinity)

  @doc false
  def push_server_id(server), do: GenServer.call(server, :push_server_id, :infinity)

  @doc false
  def claim_push(server, now_ms), do: GenServer.call(server, {:claim_push, now_ms}, :infinity)

  @doc false
  def enable_push(server), do: GenServer.call(server, :enable_push, :infinity)

  @doc false
  def finish_push(server, claim, outcome, now_ms),
    do: GenServer.call(server, {:finish_push, claim, outcome, now_ms}, :infinity)

  def acknowledged_cursor(server, verified_key),
    do: GenServer.call(server, {:cursor, verified_key}, :infinity)

  @doc "The current person for an enrolled, unrevoked device key (client API principal)."
  def device_person(server, verified_key),
    do: GenServer.call(server, {:device_person, verified_key}, :infinity)

  @doc "Whether a person's account is paused (`inactive_account`), for the client API."
  def account_paused?(server, person),
    do: GenServer.call(server, {:account_paused?, person}, :infinity)

  def end_browser_session(server, browser_session_id),
    do: GenServer.call(server, {:end_browser_session, browser_session_id}, :infinity)

  @impl true
  def init(opts) do
    case startup_store_with_cutoff(opts) do
      {:ok, store, cutoff} ->
        clock = Keyword.get(opts, :clock, fn -> System.system_time(:millisecond) end)

        case startup_cleanup(store, clock.(), cutoff) do
          {:ok, store} ->
            state = %{
              store: store,
              hosted_jev_connection: hosted_jev_connection(opts),
              feature_flags_path: Keyword.get(opts, :feature_flags_path),
              feature_flags_warned: false,
              clock: clock,
              default_index: Engine.default_index(store.engine),
              default_timer: nil,
              default_timer_generation: 0,
              bounce_deadline_timer: nil,
              bounce_deadline_generation: 0,
              upload_index: build_upload_index(store.engine),
              active_uploads: %{},
              socket_writes: %{},
              focus_clients: %{},
              focus_clients_by_person: %{},
              held_shows: %{},
              agent_show_receipts: %{},
              focus_streams: %{},
              focus_streams_by_person: %{},
              applied_score_record:
                AppliedScoreRecord.start(
                  Keyword.get(opts, :deployment_mode, :self_hosted),
                  Keyword.get(opts, :jev_score_record_path)
                ),
              client_rank_cache: %{},
              client_rank_retry: nil,
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
            state = initialize_client_ranks(state)
            state = reschedule_default_sweep(state, state.default_index)
            state = reschedule_bounce_sweep(state)
            schedule_change_log_prune(0)
            {:ok, AttentionServer.AgentAPI.FeedActivity.reset(state)}

          error ->
            Store.close(store)

            {:stop,
             startup_reason(opts, :store_initialization_failed, {:storage_unavailable, error})}
        end

      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp startup_store_with_cutoff(opts) do
    with {:ok, cutoff} <- AttentionServer.BouncePolicy.cutoff(),
         {:ok, store} <- open_startup_store(opts) do
      {:ok, store, cutoff}
    end
  end

  # The store writes the change log in every commit. It is turned on once,
  # here, before any startup commit; a store that cannot keep it (one with no
  # path) does not start.
  # Change-log rows older than the retention window are pruned daily; a cursor
  # older than the oldest retained row gets cursor_expired.
  @change_log_retention_ms 30 * 24 * 60 * 60 * 1_000
  @change_log_prune_interval_ms 24 * 60 * 60 * 1_000
  @client_rank_retry_ms 250

  defp schedule_change_log_prune(delay_ms),
    do: Process.send_after(self(), :change_log_prune, delay_ms)

  defp open_startup_store(opts) do
    case Store.open_relay(Keyword.get(opts, :store_path)) do
      {:ok, store} -> enable_change_log(Store.track_relay_writes(store), opts)
      {:error, reason} -> {:error, startup_reason(opts, :store_open_failed, reason)}
    end
  end

  defp enable_change_log(store, opts) do
    case Store.enable_change_log(store) do
      {:ok, store} ->
        {:ok, store}

      {:error, reason} ->
        Diagnostics.failure(:server_startup, :configuration, reason, :unavailable)
        Store.close(store)

        {:error,
         startup_reason(opts, :store_initialization_failed, {:storage_unavailable, reason})}
    end
  end

  # The local operator CLI needs the phase without exposing internal reasons.
  # Regular Relay callers keep their existing startup errors.
  defp startup_reason(opts, phase, reason)
       when reason == :storage_unavailable or
              (is_tuple(reason) and tuple_size(reason) == 2 and
                 elem(reason, 0) == :storage_unavailable) do
    if Keyword.get(opts, :operator_startup_errors, false), do: phase, else: reason
  end

  defp startup_reason(_opts, _phase, reason), do: reason

  defp startup_cleanup(store, now_ms, cutoff) do
    actor_keys = browser_actor_keys(store.engine)
    engine = discard_browser_actor_state(store.engine, actor_keys)

    with :ok <- Store.discard_building_inbox_snapshots(store),
         {:ok, store} <- commit_browser_cleanup(store, engine),
         {:ok, store} <- resume_bounces(store, now_ms, cutoff),
         :ok <- Store.discard_browser_inbox_snapshots(store) do
      {:ok, store}
    else
      {:error, reason, _store} -> {:error, reason}
      error -> error
    end
  end

  defp resume_bounces(store, now_ms, cutoff) do
    {engine, keys, counts} = BounceDecision.startup(store.engine, now_ms, cutoff)

    if engine == store.engine do
      {:ok, store}
    else
      audit = %{person: "system", actor: "bounce_decision", action: "resume_bounces"}

      with {:ok, engine} <- AttentionServer.AgentAPI.Advisories.stamp_decisions(store, engine, keys),
           {:ok, updated} <- commit(store, engine, audit) do
        if counts.backfilled > 0 do
          Logger.info(
            "event=harklight_bounce_backfill selections=#{counts.backfilled} overdue_full_bounces=#{counts.backfilled_full}"
          )
        end

        {:ok, updated}
      end
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
           AttentionServer.GuidedJoin.create(engine, person, owner, hash, config),
         {:ok, waiting, %{"approval_code" => code}} <-
           guided_pending(
             invited,
             hash,
             certificate,
             @hosted_test_device,
             status_hash,
             config,
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

  defp guided_pending(_, _, _, _, _, _, 0), do: {:error, :refused}

  defp guided_pending(engine, hash, cert, label, secret, config, attempts) do
    code = :crypto.strong_rand_bytes(7) |> Base.encode32(padding: false) |> binary_part(0, 10)

    case AttentionServer.GuidedJoin.redeem(engine, hash, cert, label, secret, code, config) do
      {:error, :collision} ->
        guided_pending(engine, hash, cert, label, secret, config, attempts - 1)

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

  defp generate_pending(_engine, _certificate, _label, 0),
    do: {:error, :storage_unavailable}

  defp generate_pending(engine, certificate, label, attempts) do
    secret = :crypto.strong_rand_bytes(32)
    secret_hash = :crypto.hash(:sha256, secret)

    # A key that already has a waiting plain request gets that same request and
    # approval code back; only its status secret changes.
    case Engine.pending_restart(engine, certificate, label, secret_hash) do
      {:ok, next, code} ->
        {:ok, next, code, secret}

      :none ->
        code = :crypto.strong_rand_bytes(7) |> Base.encode32(padding: false) |> binary_part(0, 10)

        case Engine.pending_enroll(engine, certificate, label, code, secret_hash) do
          {:ok, next} -> {:ok, next, code, secret}
          {:error, :collision} -> generate_pending(engine, certificate, label, attempts - 1)
          error -> error
        end
    end
  end

  @impl true
  def handle_call(request, from, state) do
    with_persistence_reads(state, fn ->
      case dispatch_call(request, from, state) do
        {:reply, {:socket_write, socket, authority, frames, completion}, next} ->
          next = rank_client_items(state, next)
          {:noreply, enqueue_socket_write(next, socket, authority, frames, completion, from)}

        {:reply, reply, next} ->
          {:reply, reply, fence_socket_writes(rank_client_items(state, next))}

        {:noreply, next} ->
          {:noreply, fence_socket_writes(rank_client_items(state, next))}
      end
    end)
  end

  defp with_persistence_reads(state, fun) do
    reader = fn committed_store, request, from ->
      committed_state = %{state | store: committed_store}
      # The persistence read set consists exclusively of callbacks that return
      # their input state unchanged. Reuse their authorization/projection logic.
      {:reply, reply, ^committed_state} = dispatch_call(request, from, committed_state)
      reply
    end

    AttentionServer.Store.Persistence.with_reads(state.store, reader, fun)
  end

  if Mix.env() == :test do
    defp dispatch_call({:request_line, key, raw}, _from, state) do
      {reply, next} = request(state, key, raw)
      {:reply, reply, next}
    end
  end

  defp dispatch_call(
         {:client_command, key, command, sources},
         _from,
         state
       ) do
    with {:ok, person} <- Engine.authorized_person(state.store.engine, key),
         :ok <- client_command_enabled(state, command),
         :ok <- verify_staged_reply_sources(state.store, person, sources) do
      {reply, next} = perform_command(state, key, command, sources)
      {:reply, reply, next}
    else
      error -> {:reply, error, state}
    end
  end

  defp dispatch_call(:push_server_id, _from, state),
    do: {:reply, state.store.engine.relay_instance_id, state}

  defp dispatch_call(:enable_push, _from, state),
    do: {:reply, :ok, %{state | push_enabled: true}}

  defp dispatch_call({:claim_push, now_ms}, _from, state) when is_integer(now_ms) do
    case Engine.claim_push(state.store.engine, now_ms) do
      {:ok, next_engine, claim} when next_engine == state.store.engine ->
        {:reply, {:ok, claim}, state}

      {:ok, next_engine, claim} ->
        with {:ok, claim} <- put_push_revision(state.store, claim),
             {:ok, store} <-
               commit(state.store, next_engine, %{
                 person: if(claim, do: claim.person, else: "system"),
                 actor: "apns_dispatcher",
                 action: "push_claim"
               }) do
          {:reply, {:ok, claim}, %{state | store: store}}
        else
          _ -> {:reply, {:error, :storage_unavailable}, state}
        end
    end
  end

  defp dispatch_call({:finish_push, claim, outcome, now_ms}, _from, state)
       when is_integer(now_ms) do
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

  defp dispatch_call({:cursor, key}, _from, state),
    do: {:reply, Engine.acknowledged_cursor(state.store.engine, key), state}

  defp dispatch_call({:device_person, key}, _from, state),
    do: {:reply, Engine.authorized_person(state.store.engine, key), state}

  defp dispatch_call({:account_paused?, person}, _from, state),
    do: {:reply, Engine.delivery_stopped?(state.store.engine, person), state}

  defp dispatch_call({:owner_revision, person, key}, _from, state),
    do: {:reply, Engine.owner_revision(state.store.engine, person, key), state}

  defp dispatch_call({:dashboard_inventory, person, key, configured_owner_person}, _from, state) do
    engine = state.store.engine

    result =
      if is_binary(configured_owner_person) and person == configured_owner_person do
        DashboardInventory.for_account_owner(engine, configured_owner_person, key)
      else
        DashboardInventory.for_owner(engine, person, key)
      end

    {:reply, result, state}
  end

  defp dispatch_call(
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

  defp dispatch_call({:dashboard_levels, person, key}, _from, state) do
    case Engine.owner_identity(state.store.engine, key) do
      {:ok, ^person, _revision} ->
        {:reply, {:ok, %{"levels" => BounceDecision.levels(state.store.engine, person)}}, state}

      _ ->
        {:reply, {:error, :forbidden}, state}
    end
  end

  defp dispatch_call({:save_dashboard_levels, person, key, rows}, _from, state) do
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
            next_state = %{state | store: store}

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

  defp dispatch_call(
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

  defp dispatch_call({:owner_identity, key}, _from, state),
    do: {:reply, Engine.owner_identity(state.store.engine, key), state}

  defp dispatch_call({:agent_identity, key}, _from, state),
    do: {:reply, Engine.agent_identity(state.store.engine, key), state}

  defp dispatch_call({:upload_begin, key, attrs, size}, {worker, _}, state) do
    with {:ok, person} <- Engine.authorized_person(state.store.engine, key),
         true <- valid_upload_attributes?(attrs),
         true <- is_integer(size) and size >= 0,
         true <- state.upload_cap == :none or size <= state.upload_cap do
      case begin_upload(state, person, key, attrs, worker) do
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

  defp dispatch_call({:upload_end, lease}, _from, state) do
    if active = Map.get(state.active_uploads, lease),
      do: Process.demonitor(active.monitor, [:flush])

    {:reply, :ok, %{state | active_uploads: Map.delete(state.active_uploads, lease)}}
  end

  defp dispatch_call({:upload_prepare, key, attrs, size, hash, lease}, _from, state),
    do: upload_prepare_call(state, key, attrs, size, hash, lease)

  defp dispatch_call({:upload_commit, key, upload, published, lease}, _from, state),
    do: upload_commit_call(state, key, upload, published, lease)

  defp dispatch_call({:fetch_file, key, id}, _from, state) do
    case Engine.authorized_person(state.store.engine, key) do
      {:ok, person} ->
        {:reply, fetched_file_for_person(state, person, id), state}

      _ ->
        {:reply, {:error, :not_found}, state}
    end
  end

  defp dispatch_call({:fetch_granted_agent_file, key, id}, _from, state) do
    case Engine.agent_identity(state.store.engine, key) do
      {:ok, person, _id, _revision} ->
        {:reply, fetched_file_for_person(state, person, id), state}

      _ ->
        {:reply, {:error, :not_found}, state}
    end
  end

  defp dispatch_call(
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

  defp dispatch_call({:origin_session_gone, key, handle, :agent_v2}, _from, state) do
    case Engine.origin_session_gone(state.store.engine, key, handle) do
      {:ok, next} ->
        {:ok, person} = Engine.authorized_person(next, key)
        scope = {person, key, handle}

        case commit(
               state.store,
               next,
               audit(state.store.engine, key, "origin_session_gone")
             ) do
          {:ok, store} ->
            next =
              AttentionServer.AgentAPI.FeedActivity.refresh_session(
                %{state | store: store},
                scope
              )

            {:reply, :ok, next}

          _ ->
            {:reply, {:error, :storage_unavailable}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  # Client API v2 modules run in the relay's serialized step, so a response and
  # the change-log position it reports come from one state.
  defp dispatch_call({:client_api, handler, request}, from, state) when is_atom(handler),
    do: handler.handle_call(request, from, state)

  defp dispatch_call({:focus_stream_attach, key, worker}, _from, state) when is_pid(worker) do
    case focus_device_person(state.store.engine, key) do
      {:ok, person} ->
        if existing = Map.get(state.focus_streams, worker) do
          Process.demonitor(existing.monitor, [:flush])
        end

        monitor = Process.monitor(worker)
        stream = %{key: key, person: person, monitor: monitor}
        next = put_focus_stream(state, worker, stream)
        {:reply, {:ok, focus_list(next, person)}, next}

      error ->
        {:reply, error, state}
    end
  end

  defp dispatch_call({:origin_ack2, key, feed_seq, declaration}, _from, state) do
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

  defp dispatch_call({:client_agent_answer, key, frame}, _from, state) do
    with {:ok, person, _id, _revision} <- Engine.agent_identity(state.store.engine, key),
         {:ok, next_engine, fields} <-
           Engine.client_agent_answer(state.store.engine, key, frame, state.clock.()) do
      case commit_agent_answer(state, next_engine, person, key, frame) do
        {:ok, next_state} -> {:reply, {:ok, fields}, next_state}
        :error -> {:reply, {:error, :storage_unavailable}, state}
      end
    else
      {:error, :forbidden} -> {:reply, {:error, :unauthorized}, state}
      error -> {:reply, error, state}
    end
  end

  defp dispatch_call({:owner_register_agent, person, owner_key, id, agent_key}, _from, state),
    do:
      setup(
        state,
        Engine.owner_register_agent(state.store.engine, person, owner_key, id, agent_key),
        person,
        owner_key,
        "owner_agent_register"
      )

  defp dispatch_call(
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

  defp dispatch_call({:owner_revoke_agent, person, owner_key, agent_key}, _from, state),
    do:
      setup(
        state,
        Engine.owner_revoke_agent(state.store.engine, person, owner_key, agent_key),
        person,
        owner_key,
        "owner_agent_revoke"
      )

  defp dispatch_call({:owner_list_agents, person, owner_key}, _from, state),
    do: {:reply, Engine.owner_list_agents(state.store.engine, person, owner_key), state}

  defp dispatch_call({:owner_send_line, person, key, revision, socket, line}, _from, state) do
    result =
      case Engine.owner_revision(state.store.engine, person, key) do
        {:ok, ^revision} ->
          {:socket_write, socket, {:owner, person, key, revision}, [line], :result}

        _ ->
          {:error, :forbidden}
      end

    {:reply, result, state}
  end

  defp dispatch_call({:pending_enroll, certificate_der, label}, _from, state) do
    case generate_pending(state.store.engine, certificate_der, label, 8) do
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

  defp dispatch_call({:owner_browser_sign_in_create, person, owner}, _from, state) do
    case issue_browser_sign_in(state, state.store.engine, person, owner, owner) do
      {:ok, response, next} -> {:reply, {:ok, response}, next}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp dispatch_call({:hosted_account_create, account_ref}, _from, state) do
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

          _ ->
            {:reply, {:error, :storage_unavailable}, state}
        end
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp dispatch_call({:hosted_sign_in_create, person, label}, _from, state) do
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

  defp dispatch_call({:hosted_set_algorithm, person, algorithm}, _from, state) do
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

  defp dispatch_call({:hosted_delivery_state, person, target_state}, _from, state) do
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

  defp dispatch_call({:hosted_test_reset, target_state}, _from, state) do
    engine = state.store.engine

    with true <- target_state in @hosted_test_states || {:error, :unknown_state},
         account_ref when is_binary(account_ref) <-
           state.hosted_test_account || {:error, :not_designated},
         %{person: person} <-
           Map.get(engine.hosted_accounts, account_ref) || {:error, :not_created},
         owner when is_binary(owner) <- Engine.hosted_owner_key(engine, person),
         {:ok, cleared, _removed_keys} <- Engine.hosted_test_reset(engine, person),
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

      result = %{state: target_state, person_id: person}
      result = if target_state == "device-waiting", do: Map.put(result, :code, code), else: result
      {:reply, {:ok, result}, next}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
      {:error, _, _} -> {:reply, {:error, :storage_unavailable}, state}
      _ -> {:reply, {:error, :not_created}, state}
    end
  end

  defp dispatch_call(:native_sign_in_methods, _from, state) do
    methods =
      case {state.store.auth_schema, state.deployment_mode} do
        {%{}, :saas} -> {:ok, ~w(google apple email_code)}
        {%{}, :self_hosted} -> {:ok, ~w(owner_code)}
        _ -> {:error, :disabled}
      end

    {:reply, methods, state}
  end

  defp dispatch_call({:create_native_transaction, verifier_hash}, _from, state) do
    result =
      case state.store do
        %{auth_schema: %{}, db: db} -> NativeSignIn.begin(db, verifier_hash, state.clock.())
        _ -> {:error, :disabled}
      end

    {:reply, result, state}
  end

  defp dispatch_call({:redeem_native_transaction, request}, _from, state) do
    case state.store do
      %{auth_schema: %{}, db: db, engine: engine} ->
        result =
          if Map.has_key?(request, "old_proof") do
            {:error, :invalid_request}
          else
            NativeSignIn.prepare_redemption(db, engine, request, state.clock.())
          end

        case result do
          {:ok, next, redemption, response} ->
            audit = %{
              person: redemption.person,
              actor: redemption.device,
              action: "native_sign_in",
              native_redemption: redemption
            }

            case commit(state.store, next, audit) do
              {:ok, store} -> {:reply, {:ok, response}, %{state | store: store}}
              _ -> {:reply, {:error, :storage_unavailable}, state}
            end

          {:ok, %{"person_id" => person, "device_id" => device} = response} ->
            case Engine.authorized_person(engine, device) do
              {:ok, ^person} -> {:reply, {:ok, response}, state}
              _ -> {:reply, {:error, :unauthorized}, state}
            end

          error ->
            {:reply, error, state}
        end

      _ ->
        {:reply, {:error, :disabled}, state}
    end
  end

  defp dispatch_call({:native_session, bearer}, _from, state) do
    result =
      with %{auth_schema: %{}, db: db, engine: engine} <- state.store,
           true <- DeviceProof.secret?(bearer),
           hash = DeviceProof.digest(bearer),
           {:ok, %{device_id: device, person_id: person} = session} <-
             NativeSignIn.lookup_session(db, hash),
           {:ok, ^person} <- Engine.authorized_person(engine, device) do
        {:ok, Map.put(session, :hash, hash)}
      else
        _ -> {:error, :unauthorized}
      end

    {:reply, result, state}
  end

  defp dispatch_call({:authorize_hosted_native_transaction, person, transaction, method}, _from, state) do
    result =
      with :saas <- state.deployment_mode,
           %{auth_schema: %{}, db: db, engine: engine} <- state.store,
           {:ok, _record} <- hosted_record(engine, person) do
        NativeSignIn.authorize_hosted(db, transaction, person, method, state.clock.())
      else
        :self_hosted -> {:error, :disabled}
        %Store{auth_schema: nil} -> {:error, :disabled}
        error -> error
      end

    {:reply, result, state}
  end

  defp dispatch_call({:hosted_browser_grant, person, transaction, method}, _from, state) do
    result =
      with :saas <- state.deployment_mode,
           %{auth_schema: %{}, db: db, engine: engine} <- state.store,
           {:ok, _record} <- hosted_record(engine, person),
           key when is_binary(key) <- Engine.hosted_owner_key(engine, person),
           {:ok, revision} <- Engine.owner_revision(engine, person, key) do
        BrowserAuth.issue_hosted(db, transaction, person, key, revision, method, state.clock.())
      else
        :self_hosted -> {:error, :disabled}
        %Store{auth_schema: nil} -> {:error, :disabled}
        _ -> {:error, :not_hosted}
      end

    {:reply, result, state}
  end

  defp dispatch_call({:redeem_hosted_browser_grant, grant, transaction}, _from, state) do
    result =
      case {state.deployment_mode, state.store} do
        {:saas, %{auth_schema: %{}, db: db, engine: engine}} ->
          BrowserAuth.redeem_hosted(db, engine, grant, transaction, state.clock.())

        _ ->
          {:error, :disabled}
      end

    {:reply, result, state}
  end

  defp dispatch_call({:browser_session, hash}, _from, state) do
    result =
      with %{auth_schema: %{}, db: db, engine: engine} <- state.store,
           {:ok, %{person: person, key: key, revision: revision} = session} <-
             BrowserAuth.session(db, hash),
           {:ok, ^revision} <- Engine.owner_revision(engine, person, key) do
        {:ok, with_hosted_label(session, :display_name, engine, person)}
      else
        _ -> {:error, :invalid_session}
      end

    {:reply, result, state}
  end

  defp dispatch_call({:consume_browser_sign_in, token}, _from, state),
    do: consume_browser_fragment(state, token, nil)

  defp dispatch_call({:redeem_browser_fragment, token, hash}, _from, state),
    do: consume_browser_fragment(state, token, hash)

  defp dispatch_call({:owner_invite_create, person, owner}, _from, state) do
    token = :crypto.strong_rand_bytes(32)
    hash = :crypto.hash(:sha256, token)

    case AttentionServer.GuidedJoin.create(
           state.store.engine,
           person,
           owner,
           hash,
           state.join_configuration
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

  defp dispatch_call(:dashboard_origin, _from, state),
    do: {:reply, state.dashboard_origin, state}

  defp dispatch_call({:invitation_resolve, token}, _from, state) do
    with {:ok, _hash} <- secret_hash(token) do
      result =
        case AttentionServer.GuidedJoin.resolve(
               state.store.engine,
               token,
               state.join_configuration
             ) do
          {:ok, %{"person_id" => person} = response} ->
            {:ok, with_hosted_label(response, "account_label", state.store.engine, person)}

          other ->
            other
        end

      {:reply, result, state}
    else
      _ -> {:reply, {:error, :unavailable}, state}
    end
  end

  defp dispatch_call({:owner_invite_create, person, owner, request_id}, _from, state) do
    result =
      AttentionServer.GuidedJoin.create_for_request(
        state.store.engine,
        person,
        owner,
        request_id,
        state.join_configuration
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

      error ->
        {:reply, error, state}
    end
  end

  defp dispatch_call({:invited_enroll, token, certificate, label, status_secret}, _from, state) do
    with {:ok, hash} <- secret_hash(token), {:ok, status_hash} <- secret_hash(status_secret) do
      case guided_pending(
             state.store.engine,
             hash,
             certificate,
             label,
             status_hash,
             state.join_configuration,
             8
           ) do
        {:ok, next, response} ->
          case commit(state.store, next, %{
                 person: next.join_invitations[hash].person,
                 actor: "invited_submitter",
                 action: "invited_pending_enrollment"
               }) do
            {:ok, committed} -> {:reply, {:ok, response}, %{state | store: committed}}
            _ -> {:reply, {:error, :storage_unavailable}, state}
          end

        error ->
          {:reply, error, state}
      end
    else
      _ -> {:reply, {:error, :refused}, state}
    end
  end

  defp dispatch_call({:pending_cancel, secret, code, key, deadline}, _from, state) do
    with {:ok, hash} <- secret_hash(secret) do
      case Engine.pending_cancel(state.store.engine, hash, code, key, deadline) do
        {:ok, next, reply} ->
          case commit(state.store, next, %{
                 person: "requester",
                 actor: "pending_submitter",
                 action: "pending_cancel"
               }) do
            {:ok, store} -> {:reply, {:ok, reply}, %{state | store: store}}
            _ -> {:reply, {:error, :storage_unavailable}, state}
          end

        error ->
          {:reply, error, state}
      end
    else
      _ -> {:reply, {:error, :not_found}, state}
    end
  end

  defp dispatch_call({:removal_identity, key}, _from, state) do
    reply =
      case Map.get(state.store.engine.devices, key) do
        %{person: person} -> {:ok, person}
        _ -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  defp dispatch_call({:device_remove, key}, _from, state) do
    case Engine.device_remove(state.store.engine, key) do
      {:ok, next, reply} ->
        next = revoke_origin_sessions(next, reply["person_id"], key)

        case commit_device_removal(
               state,
               next,
               reply["person_id"],
               key,
               key,
               "device_self_remove"
             ) do
          {:ok, next} ->
            next = remove_focus_device(next, key, reply["person_id"])
            {:reply, {:ok, reply}, next}

          {:error, _, next, :committed} ->
            next = remove_focus_device(next, key, reply["person_id"])
            {:reply, {:error, :storage_unavailable}, next}

          {:error, _, _, :not_committed} ->
            {:reply, {:error, :storage_unavailable}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  defp dispatch_call({:owner_pending_decline, person, owner, code, key, deadline}, _from, state) do
    case Engine.pending_decline(state.store.engine, person, owner, code, key, deadline) do
      {:ok, next} ->
        case commit(state.store, next, %{
               person: person,
               actor: owner,
               action: "owner_device_decline"
             }) do
          {:ok, store} -> {:reply, :ok, %{state | store: store}}
          _ -> {:reply, {:error, :storage_unavailable}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  defp dispatch_call({:pending_status, secret_hex}, _from, state) when is_binary(secret_hex) do
    with {:ok, secret} <- Base.decode16(secret_hex, case: :lower),
         true <- byte_size(secret) == 32 do
      hash = :crypto.hash(:sha256, secret)
      {:reply, Engine.pending_status(state.store.engine, hash), state}
    else
      _ -> {:reply, {:error, :not_found}, state}
    end
  end

  defp dispatch_call({:pending_status, _}, _from, state),
    do: {:reply, {:error, :not_found}, state}

  defp dispatch_call({:pending_resume, secret_hex, cert_der}, _from, state) do
    with {:ok, hash} <- secret_hash(secret_hex) do
      case Engine.pending_resume(state.store.engine, hash, cert_der) do
        {:ok, _engine, reply} -> {:reply, {:ok, reply}, state}
        error -> {:reply, error, state}
      end
    else
      _ -> {:reply, {:error, :not_found}, state}
    end
  end

  defp dispatch_call({:owner_pending_lookup, person, key, code}, _from, state),
    do: {:reply, Engine.pending_lookup(state.store.engine, person, key, code), state}

  defp dispatch_call(
         {:owner_pending_approve, person, key, code, expected_key, expected_expires_at, senders},
         _from,
         state
       ) do
    now_ms = System.system_time(:millisecond)

    case Engine.pending_approve(
           state.store.engine,
           person,
           key,
           code,
           expected_key,
           expected_expires_at,
           senders,
           now_ms
         ) do
      {:ok, next_engine, device_key} ->
        case commit(state.store, next_engine, %{
               person: person,
               actor: key,
               action: "owner_device_approve"
             }) do
          {:ok, store} ->
            next_state = ensure_priority_view_if_missing(%{state | store: store}, person)
            {:reply, {:ok, device_key}, next_state}

          {:error, _, _} ->
            {:reply, {:error, :storage_unavailable}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp dispatch_call({:owner_revoke_device, person, owner_key, device_key}, _from, state) do
    case Engine.owner_revoke_device(state.store.engine, person, owner_key, device_key) do
      {:ok, next_engine} ->
        next_engine = revoke_origin_sessions(next_engine, person, device_key)

        case commit_device_removal(
               state,
               next_engine,
               person,
               device_key,
               owner_key,
               "owner_device_revoke"
             ) do
          {:ok, next} ->
            next = remove_focus_device(next, device_key, person)
            {:reply, :ok, next}

          {:error, _, next, :committed} ->
            next = remove_focus_device(next, device_key, person)
            {:reply, {:error, :storage_unavailable}, next}

          {:error, _, _, :not_committed} ->
            {:reply, {:error, :storage_unavailable}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp dispatch_call({:owner_grant_sender, person, key, device, pattern}, _from, state),
    do:
      setup(
        state,
        Engine.owner_grant_sender(state.store.engine, person, key, device, pattern),
        person,
        key,
        "owner_sender_grant"
      )

  defp dispatch_call({:owner_revoke_sender, person, key, device, pattern}, _from, state),
    do:
      setup(
        state,
        Engine.owner_revoke_sender(state.store.engine, person, key, device, pattern),
        person,
        key,
        "owner_sender_revoke"
      )

  defp dispatch_call(
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

  defp dispatch_call({:owner_revoke_enrollment_read, person, key, grant_id}, _from, state),
    do:
      setup(
        state,
        Engine.owner_revoke_enrollment_read(state.store.engine, person, key, grant_id),
        person,
        key,
        "owner_enrollment_read_revoke"
      )

  defp dispatch_call(
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

  defp dispatch_call({:owner_revoke_maintainer, person, key, grant_id}, _from, state),
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

  defp dispatch_call({:operator_bootstrap, key}, _from, state) do
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

          {:error, _, _} ->
            {:reply, {:error, :storage_unavailable}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp dispatch_call({:operator_recover, person, key}, _from, state),
    do:
      setup(
        state,
        Engine.operator_recover(state.store.engine, person, key),
        person,
        "local_operator",
        "owner_recovery"
      )

  defp dispatch_call({:setup_person, person}, _from, state),
    do:
      setup(
        state,
        Engine.setup_person(state.store.engine, person),
        person,
        "local_setup",
        "person_setup"
      )

  defp dispatch_call({:setup_device, person, key}, _from, state),
    do:
      setup(
        state,
        Engine.setup_device(state.store.engine, person, key),
        person,
        "local_setup",
        "device_setup"
      )

  defp dispatch_call(
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

  defp dispatch_call({:setup_agent_fixture, _, _, _, _}, _from, state),
    do: {:reply, {:error, :not_supported}, state}

  defp dispatch_call({:revoke_agent_fixture, key}, _from, %{agent_fixture_mode: true} = state),
    do:
      setup(
        state,
        Engine.revoke_agent_fixture(state.store.engine, key),
        get_in(state.store.engine.agents, [key, :person]),
        "local_owner_fixture",
        "agent_fixture_revoke"
      )

  defp dispatch_call({:revoke_agent_fixture, _}, _from, state),
    do: {:reply, {:error, :not_supported}, state}

  defp dispatch_call(
         {:fixture_agent_answer, key, frame},
         _from,
         %{agent_fixture_mode: true} = state
       ) do
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

  defp dispatch_call({:fixture_agent_answer, _, _}, _from, state),
    do: {:reply, {:error, :not_supported}, state}

  defp dispatch_call({:setup_sender, key, pattern}, _from, state) do
    person = person_of(state.store.engine, key)

    setup(
      state,
      Engine.setup_sender(state.store.engine, key, pattern),
      person,
      "local_setup",
      "sender_setup"
    )
  end

  defp dispatch_call(
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

  defp dispatch_call({:grant_source_report, _, _, _, _, _}, _from, state),
    do: {:reply, {:error, :not_supported}, state}

  defp dispatch_call(
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

  defp dispatch_call({:revoke_source_report, _, _, _}, _from, state),
    do: {:reply, {:error, :not_supported}, state}

  defp dispatch_call(
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

  defp dispatch_call({:grant_enrollment_read_fixture, _, _, _, _, _, _}, _from, state),
    do: {:reply, {:error, :not_supported}, state}

  defp dispatch_call(
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

  defp dispatch_call({:revoke_enrollment_read_fixture, _, _}, _from, state),
    do: {:reply, {:error, :not_supported}, state}

  defp dispatch_call({:revoke_device, key}, _from, state) do
    person = person_of(state.store.engine, key)

    case Engine.revoke_device(state.store.engine, key) do
      {:ok, next_engine} ->
        next_engine = revoke_origin_sessions(next_engine, person, key)

        case commit_device_removal(
               state,
               next_engine,
               person,
               key,
               "local_setup",
               "device_revoke"
             ) do
          {:ok, next} ->
            next = remove_focus_device(next, key, person)
            {:reply, :ok, next}

          {:error, _, next, :committed} ->
            next = remove_focus_device(next, key, person)
            {:reply, {:error, :storage_unavailable}, next}

          {:error, _, _, :not_committed} ->
            {:reply, {:error, :storage_unavailable}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  # Signing a browser out closes its v2 feeds and ends its focus report.
  defp dispatch_call({:end_browser_session, browser_session_id}, _from, state) do
    result =
      case state.store do
        %{auth_schema: %{}, db: db} when is_binary(browser_session_id) ->
          BrowserAuth.remove_session(db, browser_session_id)
        _ -> :ok
      end

    case result do
      :ok ->
        AttentionServer.ChangeLog.notify_closing([{{:browser_session, browser_session_id}, :revoked}])
        {:reply, :ok, end_focus(state, {:browser, browser_session_id})}

      _ ->
        {:reply, {:error, :storage_unavailable}, state}
    end
  end

  # Each frame is released under current authority. A released write belongs
  # to that connection; its completion never occupies the shared core.
  defp dispatch_call({:socket_write_frame, socket, token, frame}, _from, state) do
    case Map.get(state.socket_writes, socket) do
      %{token: ^token, job: job} ->
        case authorize_socket_write(state, job.authority, frame) do
          {:ok, bytes} -> {:reply, {:ok, bytes}, state}
          error -> {:reply, error, state}
        end

      _ ->
        {:reply, {:error, :fenced}, state}
    end
  end

  defp upload_prepare_call(state, key, attrs, size, hash, lease) do
    with {:ok, person} <- Engine.authorized_person(state.store.engine, key),
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

      # Client API v2 (POST /v2/files) keeps the sent Content-Type. Only that
      # path sets this atom key; today's decoded-JSON attributes cannot.
      base = put_client_content_type(base, attrs)

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
          if Map.drop(upload, [:id, :last_activity_ms, :uploader_kind, :staged_file_id]) ==
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

  defp put_client_content_type(base, %{client_api_v2_content_type: type}) when is_binary(type),
    do: Map.put(base, :content_type, type)

  defp put_client_content_type(base, _attrs), do: base

  defp upload_commit_call(state, key, upload, published, lease) do
    result =
      with {:ok, person} <- Engine.authorized_person(state.store.engine, key),
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
            if Map.drop(stored, [:id, :last_activity_ms, :uploader_kind, :staged_file_id]) ==
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

  @impl true
  def handle_info(message, state) do
    with_persistence_reads(state, fn ->
      {:noreply, next} = dispatch_info(message, state)
      {:noreply, fence_socket_writes(rank_client_items(state, next))}
    end)
  end

  defp initialize_client_ranks(state) do
    persist_client_ranks(state, Map.keys(state.store.engine.persons))
  end

  # Client API v2 ranks are computed once per change, here, and stored with
  # the change log, so reads page from the stored order. A person is ranked
  # when a commit changed their client resources or their priority view
  # changed.
  defp rank_client_items(before, next) do
    views_changed =
      if before.priority_views === next.priority_views do
        []
      else
        (Map.keys(before.priority_views) ++ Map.keys(next.priority_views))
        |> Enum.uniq()
        |> Enum.reject(&(Map.get(before.priority_views, &1) === Map.get(next.priority_views, &1)))
      end

    # Failed ranks stay touched until the timer retries them. Reads and other
    # unrelated Relay traffic must not repeatedly retry a failing write.
    persons =
      next.store.change_log_touched
      |> MapSet.union(MapSet.new(views_changed))
      |> MapSet.difference(before.store.change_log_touched)

    persist_client_ranks(next, persons)
  end

  defp persist_client_ranks(state, persons) do
    state = Enum.reduce(persons, state, &store_client_ranks(&2, &1))

    if state.client_rank_retry == nil and MapSet.size(state.store.change_log_touched) > 0 do
      timer = Process.send_after(self(), :retry_client_ranks, @client_rank_retry_ms)
      %{state | client_rank_retry: timer}
    else
      state
    end
  end

  # The person's last stored ranking is kept here, so an unchanged re-rank is
  # compared in memory and writes nothing. A failure drops it, and the next
  # ranking compares against SQLite again.
  defp store_client_ranks(state, person) do
    store = state.store
    cache = Map.get(state.client_rank_cache, person)

    result =
      if Map.has_key?(store.engine.persons, person) do
        previous =
          case cache do
            %{units: units} -> {:ok, units}
            nil -> ChangeLog.rank_keys(store, person)
          end

        with {:ok, previous} <- previous do
          ranks =
            AttentionServer.ClientAPI.Resources.ranks(
              store.engine,
              person,
              Map.get(state.priority_views, person),
              previous
            )

          ChangeLog.put_ranks(store, person, ranks, cache)
        end
      else
        {:ok, nil, nil}
      end

    case result do
      {:ok, _head, next_cache} ->
        caches =
          if next_cache,
            do: Map.put(state.client_rank_cache, person, next_cache),
            else: Map.delete(state.client_rank_cache, person)

        store = %{store | change_log_touched: MapSet.delete(store.change_log_touched, person)}
        %{state | store: store, client_rank_cache: caches}

      error ->
        retry_client_rank(state, person, error)
    end
  rescue
    error -> retry_client_rank(state, person, error)
  end

  defp retry_client_rank(state, person, error) do
    Diagnostics.failure(:server_storage, :state_commit, error, :retry)

    store = %{
      state.store
      | change_log_touched: MapSet.put(state.store.change_log_touched, person)
    }

    %{state | store: store, client_rank_cache: Map.delete(state.client_rank_cache, person)}
  end

  defp dispatch_info(:retry_client_ranks, state) do
    {:noreply,
     persist_client_ranks(%{state | client_rank_retry: nil}, state.store.change_log_touched)}
  end

  defp dispatch_info({:socket_write_done, socket, token, result}, state) do
    case Map.get(state.socket_writes, socket) do
      %{token: ^token} = write ->
        Process.demonitor(write.monitor, [:flush])
        state = %{state | socket_writes: Map.delete(state.socket_writes, socket)}
        {reply, state} = finish_socket_write(state, write.job, result)
        GenServer.reply(write.job.from, reply)

        if result == :ok do
          {:noreply, start_queued_write(state, socket, write.queue)}
        else
          {:noreply, fail_queued_writes(state, write.queue, result)}
        end

      _ ->
        {:noreply, state}
    end
  end

  defp dispatch_info({:agent_feed_activity, action, scope}, state) do
    {:noreply, AttentionServer.AgentAPI.FeedActivity.handle_info(action, scope, state)}
  end

  defp dispatch_info(:change_log_prune, state) do
    before_ms = state.clock.() - @change_log_retention_ms

    case AttentionServer.ChangeLog.prune(state.store, before_ms) do
      {:ok, _deleted} -> :ok
      {:error, reason} -> Diagnostics.failure(:server_storage, :periodic_cleanup, reason, :retry)
    end

    # This runs at startup and daily. File IO stays in the worker, while
    # Relay reads and acknowledges the durable list of pending discards.
    relay = self()
    Task.start(fn -> AttentionServer.ClientAPI.Uploads.clean_discarded(relay) end)

    schedule_change_log_prune(@change_log_prune_interval_ms)

    receipts =
      Map.reject(state.agent_show_receipts, fn {_, receipt} -> receipt.at_ms < before_ms end)

    {:noreply, %{state | agent_show_receipts: receipts}}
  end

  defp dispatch_info(:refresh_bounce_sweep, state), do: {:noreply, reschedule_bounce_sweep(state)}

  defp dispatch_info(:refresh_default_sweep_index, state) do
    {:noreply, reschedule_default_sweep(state, Engine.default_index(state.store.engine))}
  end

  defp dispatch_info(
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
      case persist_bounce_decision(state, old_engine, next_engine, changed_keys, "bounce_due") do
        {:ok, next_state} ->
          people =
            for {person, _, _} = key <- changed_keys,
                BounceDecision.inbox_open?(old_engine.messages[key]),
                not BounceDecision.inbox_open?(next_engine.messages[key]),
                uniq: true,
                do: person

          {:noreply, Enum.reduce(people, next_state, &pending_set_changed(&2, &1))}

        {:error, reason} ->
          Diagnostics.failure(:bounce_schedule, :decision_commit, reason, :unavailable)
          {:noreply, schedule_bounce_retry(state)}
      end
    end
  end

  defp dispatch_info({:bounce_due, _stale_generation}, state), do: {:noreply, state}

  defp dispatch_info(
         {:default_sweep, generation},
         %{default_timer_generation: generation} = state
       ) do
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

  defp dispatch_info({:default_sweep, _stale_generation}, state), do: {:noreply, state}

  defp dispatch_info({:prioritization_due, person, generation}, state) do
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

  defp dispatch_info({:priority_retry, person, generation}, state) do
    case Map.get(state.priority_retry_timers, person) do
      %{generation: ^generation} ->
        state = %{state | priority_retry_timers: Map.delete(state.priority_retry_timers, person)}
        {:noreply, schedule_priority_pass(state, person)}

      _ ->
        {:noreply, state}
    end
  end

  defp dispatch_info({:priority_deadline, person, due}, state) do
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

  defp dispatch_info({:prioritization_result, person, sequence, generation, result}, state) do
    case Map.get(state.priority_jobs, person) do
      %{sequence: ^sequence, generation: ^generation, monitor: monitor} = job ->
        Process.demonitor(monitor, [:flush])
        {:noreply, finish_priority_pass(state, person, job, result)}

      _ ->
        {:noreply, state}
    end
  end

  defp dispatch_info({:DOWN, monitor, :process, pid, reason}, state) do
    priority_job =
      Enum.find(state.priority_jobs, fn {_person, job} -> job.monitor == monitor end)

    cond do
      write = Enum.find(state.socket_writes, fn {_, write} -> write.monitor == monitor end) ->
        {socket, _} = write
        {:noreply, cancel_socket_write(state, socket, {:error, :closed})}

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

      match?(%{monitor: ^monitor}, Map.get(state.focus_streams, pid)) ->
        {:noreply, delete_focus_stream(state, pid)}

      true ->
        {:noreply, state}
    end
  end

  # Client API v2 agent answers commit through the shared store boundary.
  defp commit_agent_answer(state, next_engine, person, key, frame) do
    target = origin_feed_target_for_person(next_engine, person, frame)

    case commit(state.store, next_engine, %{person: person, actor: key, action: "agent_answer"}) do
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

        {:ok, next_state}

      _ ->
        :error
    end
  end

  defp begin_upload(state, person, key, attrs, worker) do
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
        canceled: false,
        monitor: monitor
      })

    {:ok, lease, %{state | active_uploads: active}}
  end

  if Mix.env() == :test do
    defp request(state, key, raw) do
      case Engine.authorized_person(state.store.engine, key) do
        {:ok, _person} ->
          cond do
            not state.push_enabled and push_request?(raw) ->
              {{:ok, error_line(raw, "forbidden")}, state}

            not state.source_fixture_mode and source_report_request?(raw) ->
              {{:ok, error_line(raw, "not_supported")}, state}

            true ->
              perform_request_line(state, key, raw)
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

    defp perform_request_line(state, key, raw) do
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
  end

  @doc false
  def apply_agent_command(state, key, command, sources),
    do: perform_command(state, key, command, sources)

  @doc false
  def apply_account_retract(state, {person, _, _} = message_key, actor, frame) do
    engine = state.store.engine
    {next, reply} = Engine.request_account_retract(engine, actor, frame, state.clock.())
    origin = engine.messages[message_key].origin

    finish_request(
      state,
      actor,
      next,
      reply,
      Diagnostics.operation_id(frame),
      [{person, origin}],
      "conversation_withdraw",
      frame
    )
  end

  @doc false
  def apply_agent_file_use(state, key, seq, name, outcome, op_id) do
    case Engine.origin_file_report(state.store.engine, key, seq, name, outcome, op_id) do
      {:ok, engine, reply} ->
        case commit(state.store, engine, audit(state.store.engine, key, "origin_file_use")) do
          {:ok, store} -> {{:ok, reply}, %{state | store: store}}
          _ -> {{:error, :storage_unavailable}, state}
        end

      error ->
        {error, state}
    end
  end

  @doc false
  def apply_agent_advisory_ack(state, {person, _, _} = item, key, advisory_id) do
    case AttentionServer.AgentAPI.Advisories.acknowledge(
           state.store.engine,
           {person, key},
           item,
           advisory_id
         ) do
      {:ok, next, reply} when next == state.store.engine ->
        {{:ok, reply}, state}

      {:ok, next, reply} ->
        case commit(state.store, next, audit(state.store.engine, key, "origin_advisory_ack")) do
          {:ok, store} -> {{:ok, reply}, %{state | store: store}}
          _ -> {{:error, :storage_unavailable}, state}
        end

      error ->
        {error, state}
    end
  end

  @doc false
  def apply_agent_command(state, request), do: dispatch_call(request, nil, state)

  # Copying uploads takes place outside the Relay. Check the source handles
  # here so a completed discard cannot be followed by a reply using that handle.
  defp verify_staged_reply_sources(store, person, sources) do
    Enum.reduce_while(sources, :ok, fn {_upload_id, file_id}, :ok ->
      case AttentionServer.ClientAPI.Uploads.fetch(store, person, file_id) do
        {:ok, _} -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp client_command_enabled(%{push_enabled: false}, %{"type" => type})
       when type in ["push_register", "push_remove"],
       do: {:error, "forbidden", nil, nil}

  defp client_command_enabled(_state, _command), do: :ok

  defp perform_command(state, key, command, staged_sources) do
    {next_engine, reply} = Engine.request_command(state.store.engine, key, command, state.clock.())

    next_engine =
      if match?({:ok, _}, reply),
        do: record_staged_reply_sources(next_engine, staged_sources),
        else: next_engine

    next_engine = with_coverage_availability(state, key, next_engine)
    next_engine = initial_delivery_availability(state, key, next_engine, command)
    operation_id = Diagnostics.operation_id(command)
    targets = origin_feed_targets(state.store.engine, next_engine, key, command)

    finish_request(
      state,
      key,
      next_engine,
      reply,
      operation_id,
      targets,
      command["type"] || "relay_request",
      command
    )
  end

  defp refresh_new_agent_session(
         before,
         next,
         origin,
         %{:agent_api_v2 => true, "type" => "forward", "session_handle" => handle}
       )
       when is_binary(handle) do
    old = before.store.engine
    engine = next.store.engine

    with {:ok, person} <- Engine.authorized_person(engine, origin),
         scope = {person, origin, handle},
         true <-
           Conversation.reopened?(old, engine, scope) or
             not RouteBIndex.v2_session?(old.route_b_index, scope) do
      AttentionServer.AgentAPI.FeedActivity.refresh_session(next, scope)
    else
      _ -> next
    end
  end

  defp refresh_new_agent_session(_, next, _, _), do: next

  defp record_staged_reply_sources(engine, sources) when map_size(sources) == 0, do: engine

  defp record_staged_reply_sources(engine, sources) do
    Enum.reduce(sources, engine, fn {upload_id, file_id}, state ->
      upload = Map.fetch!(state.uploads, upload_id)
      Engine.put_row(state, :uploads, upload_id, Map.put_new(upload, :staged_file_id, file_id))
    end)
  end

  defp finish_request(state, key, next_engine, reply, operation_id, targets, action, original) do
    if next_engine == state.store.engine do
      {reply, state}
    else
      audit = Map.put(audit(state.store.engine, key, action), :operation_id, operation_id)

      case commit_request(state.store, next_engine, audit, original) do
        {:ok, store} ->
          next_state = %{state | store: store}

          next_state =
            case Engine.authorized_person(next_engine, key) do
              {:ok, person} -> notify_coverage_change(next_state, state.store.engine, person, key)
              _ -> next_state
            end

          next_state =
            if action == "message_open", do: reschedule_bounce_sweep(next_state), else: next_state

          next_state =
            Enum.reduce(targets, next_state, fn target, current ->
              record_origin_feed_change(current, state.store.engine, next_engine, target)
            end)

          next_state =
            case Engine.account_device_principal(state.store.engine, key) do
              {:ok, %{person: person}} ->
                if pending_signature(state.store.engine, person) !=
                     pending_signature(next_engine, person),
                   do: pending_set_changed(next_state, person),
                   else: next_state

              _ ->
                next_state
            end

          {reply, refresh_new_agent_session(state, next_state, key, original)}

        {:error, _, _} ->
          {storage_failure(original), state}
      end
    end
  end

  if Mix.env() == :test do
    defp storage_failure(raw) when is_binary(raw),
      do: {:ok, error_line(raw, "storage_unavailable")}
  end

  defp storage_failure(_command), do: {:error, "storage_unavailable", nil, nil}

  if Mix.env() == :test do
    defp commit_request(store, next, %{action: "message_open"} = audit, raw)
         when is_binary(raw) do
      case JSON.decode_line(raw) do
        {:ok, command} -> commit_request(store, next, audit, command)
        _ -> {:error, :storage_unavailable, store}
      end
    end
  end

  defp commit_request(store, next, %{action: "message_open", person: person} = audit, %{
         "sender_id" => sender,
         "message_id" => id
       }) do
    with {:ok, next} <-
           AttentionServer.AgentAPI.Advisories.stamp_decisions(store, next, [{person, sender, id}]) do
      commit(store, next, audit)
    else
      _ -> {:error, :storage_unavailable, store}
    end
  end

  defp commit_request(store, next, audit, _original), do: commit(store, next, audit)

  defp hosted_jev_connection(opts) do
    if Keyword.get(opts, :deployment_mode) == :saas do
      with true <- JevClient.key_configured?(),
           {:ok, endpoint} when is_binary(endpoint) <-
             normalize_endpoint(Keyword.get(opts, :jev_endpoint)),
           {:ok, model} when is_binary(model) <- normalize_model(Keyword.get(opts, :jev_model)) do
        {endpoint, model}
      else
        _ -> {nil, nil}
      end
    end
  end

  defp jev_connection(%{hosted_jev_connection: connection}) when is_tuple(connection),
    do: connection

  defp jev_connection(%{store: %{engine: engine}}),
    do: {engine.jev_endpoint, engine.jev_model}

  defp jev_ready?(state) do
    {endpoint, model} = jev_connection(state)
    JevClient.connection_ready?(endpoint, model)
  end

  defp priority_choice(%{deployment_mode: :saas} = state, person) do
    if match?({endpoint, _model} when is_binary(endpoint), state.hosted_jev_connection) and
         Engine.hosted_account(state.store.engine, person) do
      "jev-set"
    else
      "baseline"
    end
  end

  defp priority_choice(state, person), do: Placement.choice(state.store.engine, person)

  defp initialize_priority_views(state) do
    Enum.reduce(Map.keys(state.store.engine.persons), state, fn person, current ->
      current = ensure_priority_view(current, person)
      engine = current.store.engine

      if Placement.reassess(engine, person, nil, priority_choice(current, person)) == :whole_set and
           jev_ready?(current) and
           Placement.ranking_items(engine, person) != [] do
        schedule_priority_pass(current, person)
      else
        current
      end
    end)
  end

  defp ensure_priority_view(state, person) do
    engine = state.store.engine
    choice = priority_choice(state, person)
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

        not jev_ready?(state) ->
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

    {next, projected_entries} =
      Engine.retain_reply_scores(state.store.engine, person, engine_entries)

    audit = %{person: person, actor: "server", action: "reply_group_scores"}

    case Store.commit(state.store, next, audit, projected_entries) do
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

  # Display metadata is added only to public baseline/fallback entries. Internal
  # engine_entries and every score/bounce path remain unscored.
  defp urgency_display_priority(entry, engine, person) do
    message =
      case Map.get(engine.messages, {person, entry["sender_id"], entry["message_id"]}) do
        %{message: message} -> message
        _ -> nil
      end

    Map.put(entry, "priority", Message.effective_urgency(message))
  end

  defp publish_priority_view(state, person, view, previous, engine_entries, reply_summary) do
    engine = state.store.engine

    engine_entries =
      Enum.reject(engine_entries, fn entry ->
        key = {person, entry["sender_id"], entry["message_id"]}
        BounceDecision.fully_bounced?(engine.messages[key])
      end)

    reply_summary =
      Map.reject(reply_summary, fn {{sender, id}, _} ->
        BounceDecision.fully_bounced?(engine.messages[{person, sender, id}])
      end)

    {replies, messages} = Enum.split_with(engine_entries, &Map.has_key?(&1, "conversation_seq"))

    shown =
      view
      |> Map.take([:algorithm, :basis, :reason])
      |> Map.put(:entries, Placement.grouped(messages))
      |> Map.put(:replies, Enum.sort_by(replies, & &1["conversation_seq"]))
      |> Map.put(
        :list_items,
        Placement.list_items(
          state.store.engine,
          person,
          engine_entries,
          reply_summary,
          view.algorithm
        )
      )

    # Baseline entries follow the list order. Ranks read the entries through
    # message_entries_by_identity, where the first entry for an identity wins.
    shown =
      if view.algorithm == "baseline" do
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

        Map.put(shown, :entries, ordered_messages)
      else
        shown
      end

    shown =
      if view.algorithm == "baseline" or view.basis == "fallback" do
        decorate = fn entries ->
          Enum.map(entries, &urgency_display_priority(&1, state.store.engine, person))
        end

        shown |> Map.update!(:entries, decorate) |> Map.update!(:replies, decorate)
      else
        shown
      end

    # The first entry for an identity wins.
    message_entries_by_identity =
      shown.entries |> Enum.reverse() |> Map.new(&{Placement.identity(&1), &1})

    comparable = Map.take(shown, [:algorithm, :basis, :reason, :entries, :replies, :list_items])

    if previous &&
         Map.take(previous, [:algorithm, :basis, :reason, :entries, :replies, :list_items]) ==
           comparable do
      Map.put(
        state,
        :priority_views,
        Map.put(
          state.priority_views,
          person,
          previous
          |> Map.put(:engine_entries, engine_entries)
          |> Map.put(:message_entries_by_identity, message_entries_by_identity)
        )
      )
    else
      view =
        comparable
        |> Map.put(:engine_entries, engine_entries)
        |> Map.put(:message_entries_by_identity, message_entries_by_identity)

      index = AttentionServer.ClientAPI.Resources.index_reply_entries(view.replies)
      view = Map.put(view, :reply_entries_by_item, index)

      Map.put(state, :priority_views, Map.put(state.priority_views, person, view))
    end
  end

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

    if Placement.reassess(engine, person, previous_entries, priority_choice(state, person)) ==
         :whole_set and jev_ready?(state) and
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

    if Placement.reassess(engine, person, previous_entries, priority_choice(state, person)) ==
         :whole_set and jev_ready?(state) and
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

    if priority_choice(state, person) == "jev-set" and jev_ready?(state) and
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

      priority_choice(state, person) != "jev-set" ->
        state

      Map.has_key?(state.priority_jobs, person) ->
        %{state | priority_queued: Map.put(state.priority_queued, person, generation)}

      not jev_ready?(state) ->
        state |> clear_priority_cache(person) |> set_fallback_priority_view(person)

      true ->
        items = Placement.ranking_items(engine, person)
        sequence = Map.get(state.priority_sequences, person, 0) + 1
        parent = self()
        cache = Map.get(state.priority_caches, person, %{})
        cache_epoch = Map.get(state.priority_cache_epochs, person, 0)
        now = state.clock.()
        evaluator = state.priority_evaluator
        {endpoint, model} = jev_connection(state)

        {pid, monitor} =
          spawn_monitor(fn ->
            result =
              JevRanking.run(items, cache, endpoint, model, now, evaluator)

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
                endpoint: endpoint,
                model: model
              })
        }
    end
  end

  defp finish_priority_pass(state, person, job, result) do
    state = %{state | priority_jobs: Map.delete(state.priority_jobs, person)}
    state = if is_map(result), do: log_priority_pass(state, result), else: state
    engine = state.store.engine
    generation = Map.get(state.priority_generations, person, 0)

    {endpoint, model} = jev_connection(state)

    same_connection? =
      priority_choice(state, person) == "jev-set" and
        Map.get(job, :endpoint, endpoint) == endpoint and
        Map.get(job, :model, model) == model and
        Map.get(job, :cache_epoch, Map.get(state.priority_cache_epochs, person, 0)) ==
          Map.get(state.priority_cache_epochs, person, 0)

    ready? =
      same_connection? and jev_ready?(state)

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

          generation == job.generation and priority_choice(state, person) == "jev-set" and
              not jev_ready?(state) ->
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

  defp persist_bounce_decision(state, _old_engine, next_engine, changed_keys, action) do
    audit = %{person: "system", actor: "bounce_decision", action: action}

    with {:ok, next_engine} <-
           AttentionServer.AgentAPI.Advisories.stamp_decisions(
             state.store,
             next_engine,
             changed_keys
           ),
         {:ok, store} <- commit(state.store, next_engine, audit) do
      {:ok, reschedule_bounce_sweep(%{state | store: store})}
    else
      {:error, reason, _store} ->
        {:error, reason}

      {:error, _} = error ->
        error

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

      priority_choice(state, person) == algorithm ->
        {:ok, ensure_priority_view(state, person)}

      true ->
        {:error, :algorithm_managed_by_server, state}
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

  # No origin stream attaches to a protocol 0.1 session handle any more, so the
  # legacy status of a stored session never reports a listener. Client API v2
  # sessions report availability through FeedActivity.
  defp origin_session_status(engine, {person, origin, handle}) do
    Conversation.status(engine, person, origin, handle, false)
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

  defp commit_device_removal(state, next_engine, person, key, actor, action) do
    engine = state.store.engine
    references = Map.get(state.upload_index.references_by_person, person, MapSet.new())

    abandoned_ids =
      for {id, %{person: ^person, uploader: ^key}} <- engine.uploads,
          not MapSet.member?(references, id),
          do: id

    next_engine = Enum.reduce(abandoned_ids, next_engine, &Engine.delete_row(&2, :uploads, &1))
    audit = %{person: person, actor: actor, action: action}

    case commit(state.store, next_engine, audit) do
      {:ok, store} ->
        removed = removed_upload_ids(engine, store.engine, abandoned_ids)
        upload_index = remove_uploads_from_index(state.upload_index, engine, removed)

        active_uploads =
          Map.new(state.active_uploads, fn {lease, upload} ->
            case upload.group do
              {^person, ^key, _op_id, _sender, _message, _revision} ->
                {lease, %{upload | canceled: true}}

              _ ->
                {lease, upload}
            end
          end)

        next = %{state | store: store, upload_index: upload_index, active_uploads: active_uploads}

        case Store.delete_uploads(store, removed, audit) do
          {:ok, store} ->
            next =
              AttentionServer.AgentAPI.FeedActivity.refresh_origin(
                %{next | store: store},
                person,
                key
              )

            {:ok, next}

          {:error, reason, store} ->
            next =
              AttentionServer.AgentAPI.FeedActivity.refresh_origin(
                %{next | store: store},
                person,
                key
              )

            {:error, reason, next, :committed}
        end

      {:error, reason, _store} ->
        {:error, reason, state, :not_committed}
    end
  end

  defp revoke_origin_sessions(engine, person, origin) do
    index = engine.route_b_index

    Enum.reduce(RouteBIndex.origin_sessions(index, person, origin), engine, fn handle, current ->
      case Conversation.mark_gone(current, person, origin, handle) do
        {:ok, gone} -> gone
        _ -> current
      end
    end)
  end

  defp origin_availability_changes(before, after_state, person, origin) do
    Enum.reduce(after_state.store.engine.origin_sessions, after_state.store.engine, fn
      {{^person, ^origin, handle} = key, _}, engine ->
        old = origin_session_status(before.store.engine, key)
        current = origin_session_status(engine, key)

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

              case origin_session_status(engine, key) do
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

    audit = %{person: person, actor: origin, action: "bounce_requeue"}

    case commit(state.store, engine, audit) do
      {:ok, store} -> reschedule_bounce_sweep(%{state | store: store})
      {:error, _, _} -> state
    end
  end

  defp notify_coverage_change(state, old_engine, person, origin) do
    if covered_handles(old_engine, person, origin) !=
         covered_handles(state.store.engine, person, origin) do
      requeue_covered(state, person, origin)
    else
      state
    end
  end

  defp initial_delivery_availability(
         state,
         key,
         engine,
         %{"type" => "forward", "message" => message}
       )
       when is_map(message) do
    with {:ok, person} <- Engine.authorized_person(engine, key),
         item = {person, message["sender_id"], message["message_id"]},
         false <- Map.has_key?(state.store.engine.messages, item),
         %{agent_api_v2: true, origin: ^key, accepted_seq: seq, session_handle: handle}
         when is_binary(handle) <- Map.get(engine.messages, item) do
      activity = AttentionServer.AgentAPI.FeedActivity.activity(state)

      listening =
        AttentionServer.AgentAPI.FeedActivity.listening?(engine, activity, {person, key, handle})

      Engine.agent_feed_delivery_availability(engine, person, seq, listening)
    else
      _ -> with_initial_origin_availability(state, key, engine)
    end
  end

  defp initial_delivery_availability(state, key, engine, _frame),
    do: with_initial_origin_availability(state, key, engine)

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
              {:ok, current} = Engine.origin_status(next_engine, key, handle, false)

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

      {:error, _, _} ->
        {:reply, {:error, :storage_unavailable}, state}
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

  defp put_push_revision(_store, nil), do: {:ok, nil}

  defp put_push_revision(store, %{person: person, locator: locator} = claim)
       when is_map(locator) do
    # Relay serializes the claim with the writes that queued it. The committed
    # person head covers every coalesced change, including entry and count rows
    # that can follow the item row in the same write.
    case ChangeLog.head(store, person) do
      {:ok, rev} when rev > 0 -> {:ok, put_in(claim.locator["rev"], rev)}
      _ -> {:error, :storage_unavailable}
    end
  end

  defp put_push_revision(_store, _claim), do: {:error, :storage_unavailable}

  defp consume_browser_fragment(state, token, session_hash) do
    with {:ok, hash} <- AttentionServer.BrowserSignIn.token_hash(token),
         {:ok, next, principal} <- AttentionServer.BrowserSignIn.consume(state.store.engine, hash),
         {:ok, audit} <- browser_fragment_audit(state, principal, session_hash) do
      case commit(state.store, next, audit) do
        {:ok, committed} ->
          principal = with_hosted_label(principal, :display_name, next)
          principal = if Map.has_key?(audit, :browser_session), do: Map.put(principal, :persistent, true), else: principal
          {:reply, {:ok, principal}, %{state | store: committed}}

        _ ->
          {:reply, {:error, :storage_unavailable}, state}
      end
    else
      _ -> {:reply, {:error, :invalid_sign_in}, state}
    end
  end

  defp browser_fragment_audit(state, principal, hash) do
    audit = %{person: principal.person_id, actor: principal.owner_key, action: "browser_sign_in_consume"}

    case {state.store.auth_schema, state.deployment_mode, hash} do
      {nil, _, _} ->
        {:ok, audit}

      {%{}, :saas, hash} when is_binary(hash) and byte_size(hash) == 32 ->
        {:ok, Map.put(audit, :browser_session, {hash, principal.owner_revision, "hosted"})}

      _ ->
        {:error, :invalid_sign_in}
    end
  end

  defp commit(store, next, audit) do
    result = Store.commit(store, next, audit)

    if match?({:ok, _}, result) and store.engine.messages != next.messages do
      send(self(), :refresh_default_sweep_index)
    end

    if match?({:ok, _}, result) and
         BounceDecision.next_deadline(store.engine) != BounceDecision.next_deadline(next) do
      send(self(), :refresh_bounce_sweep)
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
    case get_in(engine.devices, [key, :person]) || get_in(engine.agents, [key, :person]) do
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

  if Mix.env() == :test do
    defp request_action(raw) do
      case JSON.decode_line(raw) do
        {:ok, %{"type" => type}}
        when type in [
               "forward",
               "answer",
               "lifecycle",
               "message_open",
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

    defp line(frame), do: JSON.encode_line(frame)
  end

  @doc false
  def agent_focus(state, person, key) do
    case focus_device_person(state.store.engine, key) do
      {:ok, ^person} -> {:ok, focus_list(state, person)}
      _ -> {:error, :forbidden}
    end
  end

  defp focus_device_person(engine, key) do
    case Map.get(engine.devices, key) do
      %{person: person, revoked: false} -> {:ok, person}
      _ -> {:error, :forbidden}
    end
  end

  defp focus_list(state, person) do
    client_keys =
      state.focus_clients_by_person
      |> Map.get(person, MapSet.new())
      |> MapSet.to_list()

    focus_list(
      state.store.engine,
      Map.take(state.focus_clients, client_keys),
      person
    )
  end

  defp focus_list(engine, focus_clients, person) do
    focus_clients
    |> Enum.flat_map(fn {key, focus} ->
      with true <- focus.connected,
           {id, label, kind} <- focus_client_identity(engine, person, key, focus) do
        client =
          %{
            "client_id" => id,
            "device_name" => label,
            "screen" => focus.screen,
            "reported_at" => focus.reported_at,
            "in_front" => focus.screen in ["inbox", "fyi", "message", "chat", "settings"],
            "connected" => true
          }
          |> maybe_focus_field(focus, :sender_id, "sender_id")
          |> maybe_focus_field(focus, :message_id, "message_id")
          |> maybe_focus_field(focus, :visible_event_id, "visible_event_id")

        [if(kind == :browser, do: Map.put(client, "kind", "browser"), else: client)]
      else
        _ -> []
      end
    end)
    |> Enum.sort_by(fn client -> {-client["reported_at"], client["client_id"]} end)
  end

  defp focus_client_identity(engine, person, {:browser, session_id}, %{
         browser: %{owner: owner, revision: revision, name: label},
         person: person
       }) do
    if Engine.owner_revision(engine, person, owner) == {:ok, revision} do
      id = "browser-" <> Base.url_encode64(:crypto.hash(:sha256, session_id), padding: false)
      {id, label, :browser}
    end
  end

  defp focus_client_identity(engine, person, key, _focus) do
    case Map.get(engine.devices, key) do
      %{person: ^person, revoked: false} = device ->
        label = Map.get(device, :label)

        label =
          if is_binary(label) and label != "",
            do: label,
            else: "Device " <> String.slice(key, 0, 8)

        {key, label, :device}

      _ ->
        nil
    end
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

  defp end_focus(state, key) do
    existing = Map.get(state.focus_clients, key, %{})
    person = existing[:person]
    AttentionServer.ClientAPI.Focus.cancel_lapse(existing[:client_api_token])
    next = delete_focus_client(state, key)
    if person, do: notify_focus_change(state, next, person), else: next
  end

  defp person_for_device(engine, device_key) do
    case Map.get(engine.devices, device_key) do
      %{person: person} -> person
      _ -> nil
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

    next = state |> delete_focus_client(device_key)
    next = %{next | held_shows: Map.delete(next.held_shows, device_key)}
    next = close_focus_streams(next, device_key)

    if person, do: notify_focus_streams(next, person), else: next
  end

  @doc false
  # Client API v2 focus (contract rulings round 11): today's focus report,
  # carried by PUT /v2/devices/me/focus instead of a session, and kept in
  # memory only, as today. Called only by ClientAPI.Focus inside the relay
  # step. A report applies today's reported_at rule and focus notices, and
  # takes the device's held show; a lapse of its ttl, or signed_out, ends
  # focus as today's disconnect does.
  # Returns {held_show | nil, state}.
  def client_api_focus(state, {:report, key, person, browser, reported, token}) do
    now = state.clock.()
    existing = Map.get(state.focus_clients, key)
    changed = is_nil(existing) or focus_fields(existing) != reported

    reported_at =
      cond do
        is_nil(existing) -> now
        changed and reported["screen"] in ["inbox", "fyi", "message", "chat", "settings"] -> now
        true -> existing.reported_at
      end

    focus =
      reported
      |> Map.new(fn {name, value} -> {String.to_existing_atom(name), value} end)
      |> Map.merge(%{reported_at: reported_at, person: person, connected: true, browser: browser})
      |> Map.put(:client_api_token, token)

    next = put_focus_client(state, key, focus)
    next = if person, do: notify_focus_change(state, next, person), else: next

    case Map.pop(next.held_shows, key) do
      {nil, _shows} -> {nil, next}
      {held, shows} -> {held, %{next | held_shows: shows}}
    end
  end

  def client_api_focus(state, {:signed_out, key}), do: {nil, end_focus(state, key)}

  def client_api_focus(state, {:lapse, key, token}) do
    case Map.get(state.focus_clients, key) do
      %{client_api_token: ^token} -> {nil, end_focus(state, key)}
      _ -> {nil, state}
    end
  end

  defp focus_fields(focus) do
    focus
    |> Map.take([:screen, :sender_id, :message_id, :visible_event_id])
    |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)
  end

  @doc false
  def apply_agent_show(state, key, request, show_id),
    do: handle_view_show(state, key, Map.put(request, :show_id, show_id))

  defp handle_view_show(state, key, request) do
    case focus_device_person(state.store.engine, key) do
      {:ok, person} -> validate_and_send_show(state, person, request)
      error -> {error, state}
    end
  end

  defp validate_and_send_show(state, person, request) do
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
        clients = Enum.reject(focus_list(state, person), &(&1["kind"] == "browser"))

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
            hold_show(state, client, request)
        end
    end
  end

  defp normalized_device_name(name),
    do: name |> String.replace("’", "'") |> String.downcase()

  defp hold_show(state, client, request) do
    held = %{
      "item_id" => request["item_id"],
      "target" => request["target"],
      "show_id" => Map.fetch!(request, :show_id),
      "accepted_at" => state.clock.()
    }

    next = %{state | held_shows: Map.put(state.held_shows, client["client_id"], held)}

    {{:ok,
      %{
        "client_id" => client["client_id"],
        "device_name" => client["device_name"],
        "delivery" => "held"
      }}, next}
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

    cursors =
      Map.reject(engine.cursors, fn {{_, actor}, _} -> MapSet.member?(actor_keys, actor) end)

    conversation_cursors =
      Map.reject(engine.conversation_cursors, fn {{_, actor}, _} ->
        MapSet.member?(actor_keys, actor)
      end)

    opened =
      MapSet.filter(engine.conversation_opened, fn {_, actor} ->
        not MapSet.member?(actor_keys, actor)
      end)

    receipts =
      Map.reject(engine.receipts, fn {{_, actor, _}, _} -> MapSet.member?(actor_keys, actor) end)

    engine
    |> Engine.replace_rows(:cursors, cursors)
    |> Engine.replace_rows(:conversation_cursors, conversation_cursors)
    |> Engine.replace_members(:conversation_opened, opened)
    |> Engine.replace_rows(:receipts, receipts)
  end

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
    Message.uuid?(attrs["op_id"]) and
      Message.sender_id?(attrs["sender_id"]) and
      Message.nonempty?(attrs["message_id"]) and
      is_integer(attrs["revision"]) and attrs["revision"] > 0 and
      ConversationFiles.valid_name?(attrs["name"])
  end

  defp valid_upload_attributes?(_), do: false

  defp upload_group(person, key, attrs) do
    {person, key, attrs["op_id"], attrs["sender_id"], attrs["message_id"], attrs["revision"]}
  end

  defp upload_group(upload) do
    {upload.person, upload.uploader, upload.op_id, upload.sender_id, upload.message_id,
     upload.revision}
  end

  defp upload_lease_status(_state, nil, _group), do: :ok

  defp upload_lease_status(state, lease, group) do
    case Map.get(state.active_uploads, lease) do
      %{group: ^group, canceled: false} -> :ok
      %{group: ^group, canceled: true} -> {:error, :upload_discarded}
      _ -> {:error, :invalid_upload}
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

  defp referenced_upload?(upload_index, person, id) do
    upload_index.references_by_person
    |> Map.get(person, MapSet.new())
    |> MapSet.member?(id)
  end

  @doc false
  def staged_upload_attached?(state, person, file_id) do
    state.upload_index.references_by_person
    |> Map.get(person, MapSet.new())
    |> Enum.any?(fn id ->
      match?(%{staged_file_id: ^file_id}, Map.get(state.store.engine.uploads, id))
    end)
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
    index = %{index | ids_by_group: put_index_id(index.ids_by_group, group, id)}

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

          %{
            current
            | ids_by_group: remove_index_id(current.ids_by_group, group, id),
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

  defp enqueue_socket_write(state, socket, authority, frames, completion, from) do
    job = %{authority: authority, frames: frames, completion: completion, from: from}

    case Map.get(state.socket_writes, socket) do
      nil ->
        start_socket_write(state, socket, job, :queue.new())

      write ->
        put_in(state.socket_writes[socket], %{write | queue: :queue.in(job, write.queue)})
    end
    |> fence_socket_writes()
  end

  defp start_socket_write(state, socket, job, queue) do
    token = make_ref()

    {pid, monitor} =
      AttentionServer.SessionWriter.start(self(), elem(job.from, 0), socket, token, job.frames)

    write = %{pid: pid, monitor: monitor, token: token, job: job, queue: queue}
    put_in(state.socket_writes[socket], write)
  end

  defp start_queued_write(state, socket, queue) do
    case :queue.out(queue) do
      {{:value, job}, rest} -> start_socket_write(state, socket, job, rest)
      {:empty, _} -> state
    end
  end

  defp authorize_socket_write(state, {:owner, person, key, revision}, frame) do
    case Engine.owner_revision(state.store.engine, person, key) do
      {:ok, ^revision} -> {:ok, {:raw, frame}}
      _ -> {:error, :forbidden}
    end
  end

  defp socket_authorized?(state, authority),
    do: match?({:ok, _}, authorize_socket_write(state, authority, ""))

  defp fence_socket_writes(state) do
    Enum.reduce(state.socket_writes, state, fn {socket, write}, acc ->
      if socket_authorized?(acc, write.job.authority),
        do: acc,
        else: cancel_socket_write(acc, socket, {:error, :forbidden})
    end)
  end

  defp cancel_socket_write(state, nil, _result), do: state

  defp cancel_socket_write(state, socket, result) do
    case Map.pop(state.socket_writes, socket) do
      {nil, _} ->
        state

      {write, writes} ->
        Process.demonitor(write.monitor, [:flush])
        Process.exit(write.pid, :kill)
        state = %{state | socket_writes: writes}
        {reply, state} = finish_socket_write(state, write.job, result)
        GenServer.reply(write.job.from, reply)
        fail_queued_writes(state, write.queue, result)
    end
  end

  defp fail_queued_writes(state, queue, result) do
    Enum.reduce(:queue.to_list(queue), state, fn job, acc ->
      {reply, acc} = finish_socket_write(acc, job, result)
      GenServer.reply(job.from, reply)
      acc
    end)
  end

  defp finish_socket_write(state, %{completion: :result}, result), do: {result, state}
end
