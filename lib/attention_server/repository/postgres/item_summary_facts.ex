defmodule AttentionServer.Repository.Postgres.ItemSummaryFacts do
  @moduledoc false

  alias AttentionServer.{ChangeLog, JSON}
  alias AttentionServer.AgentAPI.FeedActivity
  alias AttentionServer.ClientAPI.Resources
  alias AttentionServer.Repository.Postgres.{Attributes, Items, Repo, Transaction}

  # Internal facts for a future complete item response. The caller authorizes
  # the recipient and reads in one snapshot. Missing conversation summaries
  # and unavailable legacy availability projections are explicit, not empty.
  def read(person, "i" <> _ = item) do
    if Repo.in_transaction?() do
      with {:ok, row} <- recipient(person, item),
           {:ok, attributes} <- Items.recipient_attributes(row.attributes),
           {:ok, seen} <- seen(attributes, row.seen_revision, row.raw_revision) do
        facts(person, item, row, seen)
      end
    else
      {:error, :transaction_required}
    end
  end

  def read(_person, _item), do: {:error, :not_found}

  defp recipient(person, item) do
    case Transaction.query(
           """
           SELECT message.sender_id, message.message_id, message.session_handle, message.revision,
                  recipient.attributes, recipient.seen_revision,
                  coalesce(stream.head, identity.cursor_epoch),
                  coalesce(stream.seed_revision, identity.cursor_epoch),
                  conversation.attributes, read_position.position
           FROM harklight.message_recipients AS recipient
           JOIN harklight.messages AS message
             ON message.id = recipient.message_id AND message.person_id = recipient.person_id
           CROSS JOIN harklight.server_identity AS identity
           LEFT JOIN harklight.stream_heads AS stream
             ON stream.person_id = recipient.person_id AND stream.origin_key = ''
           LEFT JOIN harklight.conversations AS conversation ON conversation.message_id = message.id
           LEFT JOIN harklight.read_positions AS read_position
             ON read_position.conversation_id = conversation.id AND read_position.person_id = recipient.person_id
           WHERE recipient.person_id = $1 AND recipient.item_id = $2
           """,
           [person, item]
         ) do
      {:ok, %{rows: [[sender, external_id, handle, raw, attrs, seen, head, seed, chat, read_through]]}} ->
        {:ok, %{
          key: {person, sender, external_id}, handle: handle, raw_revision: raw, attributes: attrs,
          seen_revision: seen, head: head, seed: seed, chat: chat, read_through: read_through
        }}

      {:ok, %{rows: []}} -> {:error, :not_found}
      {:error, _} -> {:error, :storage_unavailable}
    end
  end

  defp seen(%{substantive_revision: substantive}, seen, raw)
       when is_integer(substantive) and substantive > 0 and substantive <= raw and
              (is_nil(seen) or (is_integer(seen) and seen > 0 and seen <= substantive)),
       do: {:ok, seen == substantive}

  defp seen(_attrs, _seen, _raw), do: {:error, :storage_unavailable}

  defp facts(person, item, row, seen) do
    addresses = [
      person,
      ChangeLog.item_resource(row.key),
      Attributes.key(person),
      Attributes.key({person, item}),
      Attributes.key({:row, :item_availability, row.key})
    ]

    with {:ok, %{rows: [[revision, true, rank, availability]]}} <-
           Transaction.query(
             """
             SELECT (SELECT revision FROM harklight.stream_resource_revisions
                       WHERE person_id = $1 AND origin_key = '' AND resource_kind = 'item' AND resource_id = $2),
                    EXISTS(SELECT 1 FROM harklight.compatibility_facts
                       WHERE family = 'client_rank_state' AND address = $3),
                    (SELECT body FROM harklight.compatibility_facts
                       WHERE family = 'client_ranks' AND address = $4),
                    (SELECT body FROM harklight.compatibility_facts
                       WHERE family = 'item_availability' AND address = $5)
             """,
             addresses
           ),
         {:ok, rank} <- rank(rank, person, item),
         {:ok, chat} <- chat(row, rank["_reply"]),
         {:ok, availability} <- availability(row.handle, availability) do
      {:ok, %{
        rank: rank, seen: seen, head: row.head, resource_revision: revision || max(row.seed, 1),
        request_revision: row.raw_revision, chat: chat, availability: availability
      }}
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  defp rank(stored, person, item) do
    with {:ok, [^person, ^item, key, json]} when is_binary(key) and is_binary(json) <- Attributes.decode(stored),
         {:ok, %{"key" => ^key} = rank} <- JSON.decode_line(json <> "\n") do
      {:ok, rank}
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  defp chat(%{chat: unprojected}, _reply) when unprojected in [nil, %{}], do: {:ok, :unprojected}

  defp chat(row, reply) do
    with {:ok, %{chat_started: started, latest_origin_reply_seq: last, unread_replies: unread} = summary}
         when is_boolean(started) and (is_nil(last) or (is_integer(last) and last > 0)) and
                is_integer(unread) and unread >= 0 <- Attributes.decode(row.chat) do
      {:ok, Resources.chat_payload(row.handle, Map.put(summary, :read_through, row.read_through || 0), reply)}
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  defp availability(nil, _stored), do: {:ok, nil}
  defp availability(_handle, nil), do: {:ok, :unprojected}

  defp availability(_handle, stored) do
    case Attributes.decode(stored) do
      {:ok, {status, stamp}} when status in ~w(listening not_listening gone unsupported) ->
        {:ok, FeedActivity.render_committed(status, stamp)}

      _ ->
        {:error, :storage_unavailable}
    end
  end
end
