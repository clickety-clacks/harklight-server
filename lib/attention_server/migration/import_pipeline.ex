defmodule AttentionServer.Migration.ImportPipeline do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Connection, ImportMaterial, ImportReceipt, ImportSession, ImportStatus, PrepareSchema, ValidateImport}

  @stages [AttentionServer.Migration.ImportOrganizations,
    AttentionServer.Migration.ImportTables,
    AttentionServer.Migration.ImportAuth,
    AttentionServer.Migration.ImportStreams,
    AttentionServer.Migration.ImportReceipts,
    AttentionServer.Migration.ImportFeeds,
    AttentionServer.Migration.ImportRecipients,
    AttentionServer.Migration.ImportSubscriptions,
    AttentionServer.Migration.ImportAnswers,
    AttentionServer.Migration.ImportUploads,
    AttentionServer.Migration.ImportEntryUploads,
    AttentionServer.Migration.ImportItemFacts,
    AttentionServer.Migration.ImportSharedReaderFacts,
    AttentionServer.Migration.ImportSharedEntryUploads]

  # Stable beside the private receipt, never the original state directory.
  def material_root(receipt), do: Path.expand(receipt) <> ".material"

  def run(env, bundle, receipt) do
    with {:ok, identity} <- ImportReceipt.open(receipt, bundle),
         {:ok, options} <- Connection.options(env) do
      Connection.with_options(options, fn db ->
        with {:ok, %{"phase" => phase}} <- ImportStatus.read(db, identity) do
          case phase do
            "validated" ->
              # Recover a lost validate response without reopening checkpoint
              # writes or repairing material in an already validated import.
              with {:ok, validated} <- ValidateImport.run(db, identity, bundle, material_root(receipt)),
                   {:ok, status} <- ImportStatus.read(db, identity) do
                {:ok, status |> Map.put("command", "import") |> Map.put("material", validated["material"])}
              end
            "active" -> {:error, :import_already_active}
            phase when phase in ["unclaimed", "loading"] -> load(db, options, identity, bundle, receipt)
          end
        end
      end)
    end
  end

  defp load(db, options, identity, bundle, receipt) do
    with {:ok, material} <- ImportMaterial.run(bundle, identity, material_root(receipt)),
         {:ok, _} <- PrepareSchema.run(options, identity),
         {:ok, _} <- ImportSession.claim(db, identity),
         :ok <- stages(db, identity, bundle),
         {:ok, _, summary} <- Bundle.verify(bundle),
         true <- identity.source == Base.decode16!(summary.sha256, case: :lower),
         {:ok, %{"phase" => "loading"} = status} <- ImportStatus.read(db, identity) do
      {:ok, status |> Map.put("command", "import") |> Map.put("material", material)}
    else
      false -> {:error, :import_source_mismatch}
      {:error, _} = error -> error
      _ -> {:error, :import_pipeline_incomplete}
    end
  end

  defp stages(db, identity, bundle) do
    Enum.reduce_while(@stages, :ok, fn module, :ok ->
      case module.run(db, identity, bundle) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end
end
