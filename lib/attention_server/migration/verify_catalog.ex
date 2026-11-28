defmodule AttentionServer.Migration.VerifyCatalog do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, ImportSession, ImportStatus, SourceSchema, SourceTables}
  alias AttentionServer.Repository.Postgres.Schema

  # Coverage boundary for the final report, not a replacement for field/byte
  # comparisons. Unknown relations/families require an explicit disposition.
  @tables ~w(advisory_feed_entries advisory_feed_receipts allocation_highs answers auth_sessions
    baseline_snapshot_items baseline_snapshots compatibility_facts conversation_entries conversation_entry_uploads
    conversations group_invitations group_members groups handoff_receipts
    hosted_accounts import_checkpoints import_state jobs large_text_chunks
    large_texts message_recipients message_revisions message_uploads messages
    operation_receipts organization_members organizations origin_delivery_acks origin_feed_entries
    origin_sessions people principals read_positions schema_migrations
    sender_grants server_identity stream_changes stream_heads stream_resource_revisions
    upload_chunks uploads)
  @derived ~w(device_subscription_heads device_subscription_senders ordinary_revision_files
    ordinary_item_files ordinary_item_availability shared_read_access shared_revision_scope shared_revision_files)
  @empty ~w(handoff_receipts jobs baseline_snapshots baseline_snapshot_items)

  def run(db, identity, bundle) do
    with {:ok, manifest, summary} <- Bundle.verify(bundle),
         true <- identity.source == Base.decode16!(summary.sha256, case: :lower),
         {:ok, %{"phase" => phase}} when phase in ["loading", "validated"] <- ImportStatus.read(db, identity),
         :ok <- relations(db),
         :ok <- versions(db),
         :ok <- families(db),
         {:ok, counts} <- table_counts(db),
         true <- Enum.all?(@empty, &(Map.fetch!(counts, &1) == 0)),
         {:ok, ^manifest, ^summary} <- Bundle.verify(bundle) do
      {:ok, %{tables: counts, source_sha256: summary.sha256}}
    else
      false -> {:error, :catalog_identity_or_empty_table_mismatch}
      {:error, _} = error -> error
      _ -> {:error, :catalog_import_not_staged}
    end
  end

  defp relations(db) do
    expected = Enum.map(@tables, &[&1, "r"])
    case ImportSession.query(db, """
      SELECT c.relname::text,c.relkind::text FROM pg_class c
      JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname='harklight' AND c.relkind IN ('r','p','v','m','f')
      ORDER BY c.relname
      """, []) do
      {:ok, %{rows: ^expected}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, :catalog_relation_coverage_mismatch}
    end
  end

  defp versions(db) do
    expected = Enum.map(1..Schema.version(), &[&1])
    case ImportSession.query(db, "SELECT version FROM harklight.schema_migrations ORDER BY version", []) do
      {:ok, %{rows: ^expected}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, :catalog_schema_version_mismatch}
    end
  end

  defp families(db) do
    fields = SourceSchema.record_families() |> Map.values() |> List.flatten() |> Enum.map(&Atom.to_string/1)
    tables = for {table, _} <- SourceSchema.tables(), match?({:durable, _}, SourceTables.disposition(table)), do: table
    allowed = Enum.uniq(fields ++ tables ++ ["origin_feeds", "advisory_feeds" | @derived])
    # Dynamic path families are closed field-by-field and by total reference
    # count in VerifyLargeText. This check only admits their exact naming form.
    case ImportSession.query(db, """
      SELECT 1 FROM harklight.compatibility_facts
      WHERE NOT (family=ANY($1::text[])) AND family !~ '^large-text-paths/[0-9a-f]{64}$'
      LIMIT 1
      """, [allowed]) do
      {:ok, %{rows: []}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, :catalog_fact_family_uncovered}
    end
  end

  defp table_counts(db) do
    Enum.reduce_while(@tables, {:ok, %{}}, fn table, {:ok, counts} ->
      case ImportSession.query(db, "SELECT count(*) FROM harklight." <> table, []) do
        {:ok, %{rows: [[count]]}} -> {:cont, {:ok, Map.put(counts, table, count)}}
        error -> {:halt, error}
      end
    end)
  end
end
