defmodule AttentionServer.Migration.SourceSchema do
  @moduledoc false

  # Reviewed FORMAT32 source columns. New columns need an explicit migration
  # disposition; silently dropping a new durable field is never acceptable.
  @tables %{
    "agent_change_log_meta" => ~w(person_id agent_key head floor seed_rev),
    "agent_change_log_state" => ~w(id store_version),
    "agent_changes" => ~w(person_id agent_key resource_id rev session at_ms),
    "agent_item_advisory_changes" => ~w(person_id agent_key resource_id rev session at_ms),
    "client_change_log_epoch" => ~w(id epoch),
    "client_change_log_meta" => ~w(person_id head floor seed_rev),
    "client_change_log_state" => ~w(id store_version rank_projection_version),
    "client_changes" => ~w(person_id rev kind resource_id at_ms),
    "client_discarded_files" => ~w(file_id),
    "client_idempotency" => ~w(person_id idempotency_key digest status body at_ms),
    "client_rank_keys" => ~w(person_id resource_id rank_key),
    "client_rank_state" => ~w(person_id),
    "client_ranks" => ~w(person_id item_id rank_key rank_json),
    "client_resource_created" => ~w(person_id resource_id created_at_ms),
    "client_resource_revs" => ~w(person_id resource_id rev),
    "client_staged_file_owners" => ~w(file_id owner_key),
    "client_staged_files" => ~w(file_id person_id name content_type size sha256 at_ms),
    "harklight_app_receipts" =>
      ~w(person_id idempotency_key digest status body at_ms original_response),
    "harklight_app_reply_intents" => ~w(person_id idempotency_key request_digest intent_digest),
    "harklight_auth_schema" => ~w(id minimum_generation device_certificates_disabled),
    "harklight_browser_grants" =>
      ~w(grant_hash transaction_id person_id owner_key owner_revision method expires_ms),
    "harklight_browser_sessions" => ~w(session_hash person_id owner_key owner_revision issuer),
    "harklight_device_sessions" =>
      ~w(session_hash device_id person_id public_key signed_in_at_ms pending),
    "harklight_migration_grants" =>
      ~w(token_hash device_id expires_ms session_hash public_key label),
    "harklight_migration_roster" => ~w(device_id person_id phase),
    "harklight_native_transactions" =>
      ~w(transaction_id verifier_hash expires_ms person_id method authorized_until_ms redeemed_device redeemed_session redeemed_key redeemed_label redeemed_old_proof old_device_result),
    "harklight_owner_codes" => ~w(code_hash person_id owner_key owner_revision expires_ms),
    "harklight_passkey_challenges" =>
      ~w(reference purpose browser_hash native_reference challenge expires_ms),
    "harklight_passkeys" => ~w(credential_id person_id owner_key owner_revision public_key),
    "relay_advisories" => ~w(person origin session seq item advisory_id),
    "relay_audit" => ~w(id person_id actor_id action at_ms),
    "relay_authority" => ~w(owner key),
    "relay_data_migrations" => ~w(name),
    "relay_device_progress" => ~w(id snapshot_version version payload checksum),
    "relay_entries" => ~w(person seq item kind author event_id folds_into fold_event_id),
    "relay_feed" =>
      ~w(person origin seq session item kind author actionable baseline receipt acted baseline_attempt),
    "relay_inbox_batches" =>
      ~w(person_id device_id snapshot_id batch_id input_digest result result_checksum),
    "relay_inbox_item_pages" => ~w(person_id device_id snapshot_id start_offset payload checksum),
    "relay_inbox_operation_receipts" => ~w(person_id device_id snapshot_id op_id),
    "relay_inbox_receipt_pages" =>
      ~w(person_id device_id snapshot_id start_offset next_offset result result_checksum),
    "relay_inbox_receipt_sequences" =>
      ~w(person_id device_id snapshot_id start_offset payload checksum),
    "relay_inbox_snapshots" =>
      ~w(person_id device_id startup_id snapshot_id since_seq high_water status item_count receipt_count operation_count cursor_secret receipt_next_offset receipts_complete),
    "relay_index_state" => ~w(id index_version liveness_version generation),
    "relay_items" =>
      ~w(person seq sender message_id state origin session agent_v2 source inherits_from last_entry last_reply read_position chat_started availability_entry discussed origin_revision bounce_pending listening live),
    "relay_large_data_refs" => ~w(path marker id size sha256),
    "relay_record_generation" => ~w(id generation),
    "relay_records" => ~w(key payload checksum account),
    "relay_sessions" => ~w(person origin session gone origin_listener),
    "relay_state" => ~w(id version payload checksum),
    "relay_upload_refs" => ~w(upload_id key),
    "sqlite_sequence" => ~w(name seq)
  }

  # Explicit FORMAT32 record families, independently versioned from Engine so
  # adding a new durable field cannot silently widen migration admission.
  @scalars ~w(relay_instance_id agent_cursor_key browser_sign_in_clock_high_water pending_clock_high_water jev_endpoint jev_model)a
  @members ~w(retired_owner_keys conversation_opened opened_revisions)a
  @rows ~w(persons people_profiles owners pending_enrollments pending_history join_invitations browser_sign_ins
    maintainer_grants devices agents sender_grants messages groups hosted_group_refs shared_messages
    shared_recipients shared_entries shared_seen shared_answers events free_order_highs conversation_highs
    conversation_events chat_read_positions reply_group_history conversation_cursors receipts origin_sessions
    item_availability origin_receipts origin_actions uploads source_report_grants enrollment_read_grants
    subscriptions cursors hosted_accounts hosted_organizations local_organizations delivery_states
    algorithm_choices alert_choices push_bindings bounce_levels bounce_revisions bounce_waiting_routes)a

  def tables, do: @tables
  def record_families, do: %{field: @scalars, member: @members, row: @rows}

  def valid_record?({:field, field}, _), do: field in @scalars
  def valid_record?({:row, field, _key}, _), do: field in @rows
  def valid_record?({:member, field, _key}, true), do: field in @members

  def valid_record?({:origin_feed, {person, origin}, part}, value)
      when is_binary(person) and is_binary(origin), do: feed_part?(:origin_feed, part, value)

  def valid_record?({:advisory, {person, origin, handle}, part}, value)
      when is_binary(person) and is_binary(origin) and is_binary(handle),
      do: feed_part?(:advisory, part, value)

  def valid_record?(_, _), do: false

  defp feed_part?(_, :high, high), do: is_integer(high) and high >= 0
  defp feed_part?(_, {:entry, seq}, _), do: is_integer(seq) and seq > 0
  defp feed_part?(:origin_feed, {:delivery_ack, seq}, _), do: is_integer(seq) and seq > 0
  defp feed_part?(:advisory, {:receipt, id}, true), do: is_binary(id)
  defp feed_part?(_, _, _), do: false
end
