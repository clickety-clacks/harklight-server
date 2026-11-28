defmodule AttentionServer.Migration.VerifySubscriptions do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportSession, ImportStatus, ImportSubscriptions}
  alias AttentionServer.Repository.Postgres.Attributes

  def run(db, identity, bundle) do
    with {:ok, manifest, summary} <- Bundle.verify(bundle),
         true <- identity.source == Base.decode16!(summary.sha256, case: :lower),
         {:ok, %{"phase" => phase}} when phase in ["loading", "validated"] <- ImportStatus.read(db, identity),
         {:ok, raw_count} <- Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, 0, fn
           {{:row, :subscriptions, _} = address, value}, n ->
             with :ok <- fact(db, "subscriptions", address, value), do: {:ok, n + 1}
           _, n -> {:ok, n}
         end),
         :ok <- count(db, "subscriptions", raw_count),
         {:ok, counts} <- Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records,
           %{heads: 0, senders: 0}, fn
             {{:row, :devices, device}, principal}, acc -> device(db, device, principal, acc)
             _, acc -> {:ok, acc}
           end),
         :ok <- count(db, "device_subscription_heads", counts.heads),
         :ok <- count(db, "device_subscription_senders", counts.senders),
         {:ok, ^manifest, ^summary} <- Bundle.verify(bundle) do
      {:ok, counts}
    else
      false -> {:error, :import_source_mismatch}
      {:error, _} = error -> error
      _ -> {:error, :subscription_verification_requires_staged_import}
    end
  end

  # Raw subscriptions are checked directly against all source frames first,
  # including rows for retired devices. This lookup cannot invent a missing row.
  defp device(db, device, %{person: person} = principal, counts) do
    with {:ok, source} <- source(db, person, device),
         {:ok, projection} <- ImportSubscriptions.projection(source, principal) do
      case projection do
        :absent -> {:ok, counts}
        {revision, senders} ->
          with :ok <- fact(db, "device_subscription_heads", {person, device}, revision),
               :ok <- each(senders, fn sender ->
                 fact(db, "device_subscription_senders", {person, device, revision, sender}, true)
               end) do
            {:ok, %{heads: counts.heads + 1, senders: counts.senders + length(senders)}}
          end
      end
    end
  end

  defp source(db, person, device) do
    case ImportSession.query(db,
      "SELECT body FROM harklight.compatibility_facts WHERE family='subscriptions' AND address=$1",
      [Attributes.key({:row, :subscriptions, {person, device}})]) do
      {:ok, %{rows: []}} -> {:ok, nil}
      {:ok, %{rows: [[body]]}} -> Attributes.decode(body)
      {:error, _} = error -> error
      _ -> {:error, :invalid_device_subscription}
    end
  end

  defp fact(db, family, address, value) do
    expected = Attributes.encode(value)
    case ImportSession.query(db,
      "SELECT body FROM harklight.compatibility_facts WHERE family=$1 AND address=$2",
      [family, Attributes.key(address)]) do
      {:ok, %{rows: [[^expected]]}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, {:subscription_fact_mismatch, family,
        Base.encode16(:crypto.hash(:sha256, Attributes.key(address)), case: :lower)}}
    end
  end

  defp count(db, family, n) do
    case ImportSession.query(db,
      "SELECT count(*) FROM harklight.compatibility_facts WHERE family=$1", [family]) do
      {:ok, %{rows: [[^n]]}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, {:subscription_count_mismatch, family}}
    end
  end

  defp each(rows, fun), do: Enum.reduce_while(rows, :ok, fn row, :ok ->
    case fun.(row) do
      :ok -> {:cont, :ok}
      error -> {:halt, error}
    end
  end)
end
