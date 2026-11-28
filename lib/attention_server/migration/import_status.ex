defmodule AttentionServer.Migration.ImportStatus do
  @moduledoc false
  alias AttentionServer.Migration.{ImportReceipt, ImportSession}

  def read(db, %{id: id, source: source, schema: schema} = identity) do
    with {:ok, %{rows: [[exists]]}} <- ImportSession.query(db,
           "SELECT to_regclass('harklight.import_state') IS NOT NULL", []) do
      if exists do
        case ImportSession.query(db, """
               SELECT import_id,source_sha256,schema_sha256,phase,manifest_sha256,
                      (SELECT count(*) FROM harklight.import_checkpoints c WHERE c.import_id=s.import_id),
                      (SELECT coalesce(sum(last_ordinal),0)::bigint FROM harklight.import_checkpoints c WHERE c.import_id=s.import_id)
               FROM harklight.import_state s WHERE singleton=true
               """, []) do
          {:ok, %{rows: []}} -> {:ok, result(identity, "unclaimed", nil, 0, 0)}
          {:ok, %{rows: [[^id, ^source, ^schema, phase, manifest, families, rows]]}} ->
            {:ok, result(identity, phase, manifest, families, rows)}
          {:ok, _} -> {:error, :import_identity_mismatch}
          error -> error
        end
      else
        {:ok, result(identity, "unclaimed", nil, 0, 0)}
      end
    end
  end

  defp result(identity, phase, manifest, families, rows) do
    Map.merge(ImportReceipt.document(identity), %{"command" => "status", "phase" => phase,
      "validation_sha256" => if(is_nil(manifest), do: nil, else: Base.encode16(manifest, case: :lower)),
      "checkpoint_families" => families, "checkpoint_rows" => rows})
  end
end
