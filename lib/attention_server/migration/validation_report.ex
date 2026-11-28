defmodule AttentionServer.Migration.ValidationReport do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, ImportReceipt, ImportSession, ImportStatus, VerifyMaterial, VerifyMessageUploads}
  alias AttentionServer.Repository.Postgres.{Attributes, Schema}

  # A fixed complete comparison set: callers cannot omit a slow or failed check.
  # This is read-only evidence, not permission to activate or a lifecycle update.
  @checks [
    {"catalog", AttentionServer.Migration.VerifyCatalog},
    {"source_facts", AttentionServer.Migration.VerifyFacts},
    {"accounts", AttentionServer.Migration.VerifyAccounts},
    {"organizations", AttentionServer.Migration.VerifyOrganizations},
    {"authority", AttentionServer.Migration.VerifyAuthority},
    {"auth_sessions", AttentionServer.Migration.VerifyAuthSessions},
    {"subscriptions", AttentionServer.Migration.VerifySubscriptions},
    {"stream_positions", AttentionServer.Migration.VerifyStreamPositions},
    {"stream_heads", AttentionServer.Migration.VerifyStreamHeads},
    {"receipts", AttentionServer.Migration.VerifyReceipts},
    {"feeds", AttentionServer.Migration.VerifyFeeds},
    {"delivery_acks", AttentionServer.Migration.VerifyDeliveryAcks},
    {"ordinary_messages", AttentionServer.Migration.VerifyMessages},
    {"ordinary_recipients", AttentionServer.Migration.VerifyRecipientState},
    {"ordinary_conversations", AttentionServer.Migration.VerifyConversations},
    {"shared_messages", AttentionServer.Migration.VerifySharedMessages},
    {"shared_conversations", AttentionServer.Migration.VerifySharedConversations},
    {"uploads", AttentionServer.Migration.VerifyUploads},
    {"large_text", AttentionServer.Migration.VerifyLargeText},
    {"ordinary_entry_uploads", AttentionServer.Migration.VerifyEntryUploads},
    {"shared_entry_uploads", AttentionServer.Migration.VerifySharedEntryUploads},
    {"ordinary_file_facts", AttentionServer.Migration.VerifyFileFacts},
    {"ordinary_availability", AttentionServer.Migration.VerifyAvailability},
    {"shared_reader_facts", AttentionServer.Migration.VerifySharedReaderFacts}
  ]

  def run(connection, identity, bundle, material_root) do
    with {:ok, manifest, summary} <- Bundle.verify(bundle),
         {:ok, material} <- VerifyMaterial.run(bundle, identity, material_root) do
      Postgrex.transaction(connection, fn db ->
        result = with {:ok, _} <- ImportSession.query(db,
                        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY", []),
                      {:ok, %{"phase" => phase} = before} when phase in ["loading", "validated"] <- ImportStatus.read(db, identity),
                      {:ok, comparisons} <- comparisons(db, identity, bundle),
                      {:ok, ordinary} <- VerifyMessageUploads.run(db, identity, bundle, :ordinary),
                      {:ok, shared} <- VerifyMessageUploads.run(db, identity, bundle, :shared),
                      {:ok, ^before} <- ImportStatus.read(db, identity),
                      {:ok, ^material} <- VerifyMaterial.run(bundle, identity, material_root),
                      {:ok, ^manifest, ^summary} <- Bundle.verify(bundle) do
          checks = comparisons |> Map.put("ordinary_message_uploads", ordinary) |> Map.put("shared_message_uploads", shared)
          report = %{"report_version" => 1, "scope" => "staged_import_comparisons",
            "identity" => ImportReceipt.document(identity), "schema_version" => Schema.version(),
            "comparisons" => normalize(checks),
            "material" => Map.take(material, ["manifest_sha256", "files"])}
          digest = :crypto.hash(:sha256, Attributes.key(report)) |> Base.encode16(case: :lower)
          {:ok, %{"report" => report, "sha256" => digest, "digest_encoding" => "deterministic-etf-v2",
            "material" => material}}
        else
          {:error, _} = error -> error
          _ -> {:error, :validation_report_state_changed}
        end
        case result do
          {:ok, report} -> report
          {:error, reason} -> Postgrex.rollback(db, reason)
        end
      end, timeout: 300_000)
    end
  end

  defp comparisons(db, identity, bundle) do
    Enum.reduce_while(@checks, {:ok, %{}}, fn {name, module}, {:ok, acc} ->
      case module.run(db, identity, bundle) do
        {:ok, counts} -> {:cont, {:ok, Map.put(acc, name, counts)}}
        {:error, reason} -> {:halt, {:error, {:validation_comparison_failed, name, reason}}}
      end
    end)
  end

  defp normalize(map) when is_map(map),
    do: Map.new(map, fn {key, value} -> {to_string(key), normalize(value)} end)
  defp normalize(list) when is_list(list), do: Enum.map(list, &normalize/1)
  defp normalize(value) when is_atom(value) and value not in [true, false, nil], do: Atom.to_string(value)
  defp normalize(value), do: value
end
