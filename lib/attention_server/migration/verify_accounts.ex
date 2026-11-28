defmodule AttentionServer.Migration.VerifyAccounts do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportSession, VerifyFacts, VerifyAllocationBounds}
  alias AttentionServer.Repository.Postgres.Attributes

  @settings [:algorithm_choices, :alert_choices, :bounce_levels]

  def run(db, identity, bundle) do
    with {:ok, _} <- VerifyFacts.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, counts} <- Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records,
           %{people: 0, highs: 0}, fn row, acc -> verify(db, row, acc) end),
         :ok <- count(db, "people", counts.people),
         :ok <- count(db, "allocation_highs", counts.highs),
         :ok <- VerifyAllocationBounds.run(db, Path.join(bundle, "records.frames"), manifest.data.records) do
      {:ok, counts}
    end
  end

  defp verify(db, {{:row, :persons, person}, value}, acc) do
    with :ok <- person(db, person),
         :ok <- high(db, person, "", "", "accepted", value) do
      {:ok, %{people: acc.people + 1, highs: acc.highs + 1}}
    end
  end
  defp verify(db, {{:row, field, person}, value}, acc)
       when field in [:free_order_highs, :conversation_highs] do
    kind = if field == :free_order_highs, do: "free_order", else: "conversation"
    with :ok <- high(db, person, "", "", kind, value), do: {:ok, %{acc | highs: acc.highs + 1}}
  end
  defp verify(db, {{:origin_feed, {person, origin}, :high}, value}, acc) do
    with :ok <- high(db, person, origin, "", "origin_feed", value), do: {:ok, %{acc | highs: acc.highs + 1}}
  end
  defp verify(db, {{:advisory, {person, origin, handle}, :high}, value}, acc) do
    with :ok <- high(db, person, origin, handle, "advisory_feed", value), do: {:ok, %{acc | highs: acc.highs + 1}}
  end
  defp verify(_, _, acc), do: {:ok, acc}

  # At most five source fields per person; no complete person map in memory.
  # VerifyFacts has already checked these raw typed bodies against the bundle.
  defp person(db, person) do
    fields = [:people_profiles, :delivery_states | @settings]
    addresses = Enum.map(fields, &Attributes.key({:row, &1, person}))
    with {:ok, %{rows: rows}} <- ImportSession.query(db,
           "SELECT family,body FROM harklight.compatibility_facts WHERE family=ANY($1::text[]) AND address=ANY($2::bytea[])",
           [Enum.map(fields, &Atom.to_string/1), addresses]),
         {:ok, values} <- decode(rows),
         profile = (case Map.fetch(values, "people_profiles") do
           :error -> %{}
           {:ok, value} -> Attributes.encode(value)
         end),
         settings = for(field <- @settings, Map.has_key?(values, Atom.to_string(field)), into: %{},
           do: {field, Map.fetch!(values, Atom.to_string(field))}),
         settings_body = if(map_size(settings) == 0, do: %{}, else: Attributes.encode(settings)),
         expected = [Map.get(values, "delivery_states"), profile, settings_body],
         {:ok, %{rows: [^expected]}} <- ImportSession.query(db,
           "SELECT delivery_state,profile,settings FROM harklight.people WHERE id=$1", [person]) do
      :ok
    else
      {:error, _} = error -> error
      _ -> mismatch(:person, person)
    end
  end

  defp decode(rows) do
    Enum.reduce_while(rows, {:ok, %{}}, fn [field, body], {:ok, acc} ->
      case Attributes.decode(body) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, field, value)}}
        error -> {:halt, error}
      end
    end)
  end

  defp high(db, person, origin, handle, kind, value) do
    case ImportSession.query(db,
      "SELECT high FROM harklight.allocation_highs WHERE person_id=$1 AND origin_key=$2 AND session_handle=$3 AND kind=$4",
      [person, origin, handle, kind]) do
      {:ok, %{rows: [[^value]]}} -> :ok
      {:error, _} = error -> error
      _ -> mismatch(:allocation_high, {person, origin, handle, kind})
    end
  end
  defp count(db, table, expected) do
    case ImportSession.query(db, "SELECT count(*) FROM harklight." <> table, []) do
      {:ok, %{rows: [[^expected]]}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, {:account_count_mismatch, table}}
    end
  end
  defp mismatch(field, key), do: {:error, {:account_state_mismatch, field,
    Base.encode16(:crypto.hash(:sha256, Attributes.key(key)), case: :lower)}}
end
