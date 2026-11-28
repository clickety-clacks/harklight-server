defmodule AttentionServer.Migration.SourceTables do
  @moduledoc false
  alias AttentionServer.Migration.SourceSchema

  # FORMAT32 durable SQL identities. These are source keys, never row ordinals.
  @keys %{
    "agent_change_log_meta" => ~w(person_id agent_key),
    "agent_changes" => ~w(person_id agent_key resource_id),
    "agent_item_advisory_changes" => ~w(person_id agent_key resource_id),
    "client_change_log_epoch" => ~w(id),
    "client_change_log_meta" => ~w(person_id),
    "client_changes" => ~w(person_id rev),
    "client_discarded_files" => ~w(file_id),
    "client_idempotency" => ~w(person_id idempotency_key),
    "client_rank_keys" => ~w(person_id resource_id),
    "client_rank_state" => ~w(person_id),
    "client_ranks" => ~w(person_id item_id),
    "client_resource_created" => ~w(person_id resource_id),
    "client_resource_revs" => ~w(person_id resource_id),
    "client_staged_file_owners" => ~w(file_id),
    "client_staged_files" => ~w(file_id),
    "harklight_app_receipts" => ~w(person_id idempotency_key),
    "harklight_app_reply_intents" => ~w(person_id idempotency_key),
    "harklight_auth_schema" => ~w(id),
    "harklight_browser_grants" => ~w(grant_hash),
    "harklight_browser_sessions" => ~w(session_hash),
    "harklight_device_sessions" => ~w(session_hash),
    "harklight_migration_grants" => ~w(token_hash),
    "harklight_migration_roster" => ~w(device_id),
    "harklight_native_transactions" => ~w(transaction_id),
    "harklight_owner_codes" => ~w(code_hash),
    "harklight_passkey_challenges" => ~w(reference),
    "harklight_passkeys" => ~w(credential_id),
    "relay_audit" => ~w(id),
    "relay_data_migrations" => ~w(name),
    "relay_device_progress" => ~w(id),
    "relay_inbox_batches" => ~w(person_id device_id snapshot_id batch_id),
    "relay_inbox_item_pages" => ~w(person_id device_id snapshot_id start_offset),
    "relay_inbox_operation_receipts" => ~w(person_id device_id snapshot_id op_id),
    "relay_inbox_receipt_pages" => ~w(person_id device_id snapshot_id start_offset),
    "relay_inbox_receipt_sequences" => ~w(person_id device_id snapshot_id start_offset),
    "relay_inbox_snapshots" => ~w(person_id device_id startup_id),
    "sqlite_sequence" => ~w(name)
  }
  @derived ~w(relay_advisories relay_authority relay_entries relay_feed relay_index_state relay_items relay_sessions relay_upload_refs)
  @source_only ~w(agent_change_log_state client_change_log_state relay_large_data_refs relay_record_generation relay_records relay_state)

  def disposition(table) do
    cond do
      Map.has_key?(@keys, table) -> {:durable, Map.fetch!(@keys, table)}
      table in @derived -> :derived
      table in @source_only -> :source_only
      true -> {:error, :unsupported_source_table}
    end
  end

  def fact(table, values) when is_list(values) do
    with {:durable, keys} <- disposition(table),
         {:ok, columns} <- Map.fetch(SourceSchema.tables(), table),
         true <- length(columns) == length(values) do
      row = Map.new(Enum.zip(columns, values))
      key = Enum.map(keys, &Map.fetch!(row, &1))
      if Enum.any?(key, &is_nil/1),
        do: {:error, :missing_source_table_key},
        else: {:ok, {"sqlite", table, key}, row}
    else
      _ -> {:error, :unsupported_source_table_row}
    end
  end

  def fact(_, _), do: {:error, :unsupported_source_table_row}
end
