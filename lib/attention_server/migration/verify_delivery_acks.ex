defmodule AttentionServer.Migration.VerifyDeliveryAcks do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportLargeText, ImportSession, VerifyFeeds}
  alias AttentionServer.Repository.Postgres.Attributes

  def run(db, identity, bundle) do
    # VerifyFeeds includes exact raw fact closure. Only after that check may the
    # retained facts below act as a bounded lookup index for the source journal.
    with {:ok, _} <- VerifyFeeds.run(db, identity, bundle),
         {:ok, manifest, summary} <- Bundle.verify(bundle),
         {:ok, count} <- Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, 0, fn
           {{:origin_feed, {person, origin}, {:delivery_ack, position}}, stages}, n ->
             with :ok <- check(db, person, origin, position, stages), do: {:ok, n + map_size(stages)}
           _, n -> {:ok, n}
         end),
         {:ok, ^manifest, ^summary} <- Bundle.verify(bundle) do
      {:ok, %{journal_pointers: count}}
    end
  end

  # Internal component seam: run/3 establishes exact raw-source equality first.
  @doc false
  def check(db, person, origin, position, stages) do
    with :ok <- supported(stages),
         {:ok, %{} = feed} <- fact(db, "origin_feeds", {:origin_feed, {person, origin}, {:entry, position}}),
         true <- feed["feed_seq"] == position,
         sender when is_binary(sender) <- feed["sender_id"],
         external when is_binary(external) <- feed["message_id"],
         key = {person, sender, external},
         {:ok, %{} = message} <- fact(db, "messages", {:row, :messages, key}),
         true <- Map.get(message, :origin) == origin and Map.get(message, :session_handle) == feed["session_handle"] do
      Enum.reduce_while(stages, :ok, fn {stage, seq}, :ok ->
        case journal(db, key, feed, stage, seq) do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end
      end)
    else
      {:error, _} = error -> error
      _ -> {:error, :delivery_ack_source_reference_mismatch}
    end
  end

  defp supported(%{ack1: first} = stages) when is_integer(first) and first > 0 do
    if Enum.all?(stages, fn
      {:ack1, ^first} -> true
      {:ack2, second} -> is_integer(second) and second > first
      _ -> false
    end), do: :ok, else: {:error, :unsupported_delivery_ack}
  end
  defp supported(_), do: {:error, :unsupported_delivery_ack}

  defp journal(db, {person, sender, external} = key, feed, stage, seq) do
    address = {:row, :conversation_events, {person, seq}}
    with {:ok, %{kind: :conversation, key: ^key, payload: payload} = event} <- fact(db, "conversation_events", address),
         true <- payload["kind"] == "delivery_ack" and payload["stage"] == Atom.to_string(stage),
         true <- Enum.all?(~w(event_id op_id terminal_seq session_handle request_revision), fn field ->
           Map.fetch(payload, field) == Map.fetch(feed, field)
         end),
         {:ok, projected} <- ImportLargeText.project(db, address, event),
         expected = [seq, payload["request_revision"], "delivery_ack", Attributes.encode(projected)],
         {:ok, %{rows: [^expected]}} <- ImportSession.query(db, """
           SELECT e.position,e.request_revision,e.kind,e.body
           FROM harklight.conversation_entries e
           JOIN harklight.conversations c ON c.id=e.conversation_id
           JOIN harklight.messages m ON m.id=c.message_id
           WHERE m.kind='ordinary' AND m.person_id=$1 AND m.sender_id=$2 AND m.message_id=$3 AND e.id=$4
           """, [person, sender, external, Integer.to_string(seq)]) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :delivery_ack_journal_mismatch}
    end
  end

  defp fact(db, family, address) do
    case ImportSession.query(db,
      "SELECT body FROM harklight.compatibility_facts WHERE family=$1 AND address=$2", [family, Attributes.key(address)]) do
      {:ok, %{rows: [[body]]}} -> Attributes.decode(body)
      {:error, _} = error -> error
      _ -> {:error, :delivery_ack_source_reference_mismatch}
    end
  end
end
