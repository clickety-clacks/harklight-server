defmodule AttentionServer.Repository.Postgres.Items do
  @moduledoc false

  alias AttentionServer.BounceDecision
  alias AttentionServer.Repository.Postgres.{Attributes, Transaction}

  @max_revision 9_223_372_036_854_775_807

  def recipient_item(person, item_id) do
    with {:ok, key, record} <- fetch(person, item_id, nil),
         true <- BounceDecision.person_visible?(record) do
      {:ok, key, record}
    else
      false -> {:error, :not_found}
      error -> error
    end
  end

  def agent_item({person, origin}, item_id) when is_binary(origin) and origin != "" do
    fetch(person, item_id, origin)
  end

  def agent_item(_scope, _item_id), do: {:error, :not_found}

  # The caller rechecks authority and keeps both indexed reads in the same
  # committed snapshot. A revision never needs the current message's history.
  def recipient_revision(person, "i" <> _ = item, revision) when revision in 1..@max_revision do
    case Transaction.query(
           """
           SELECT recipient.attributes, revision.body, coalesce(stream.head, identity.cursor_epoch)
           FROM harklight.message_recipients AS recipient
           JOIN harklight.messages AS message
             ON message.id = recipient.message_id AND message.person_id = recipient.person_id
             AND message.kind = 'ordinary'
           JOIN harklight.message_revisions AS revision
             ON revision.message_id = message.id AND revision.revision = $3
             AND revision.revision <= message.revision
           CROSS JOIN harklight.server_identity AS identity
           LEFT JOIN harklight.stream_heads AS stream
             ON stream.person_id = recipient.person_id AND stream.origin_key = ''
           WHERE recipient.person_id = $1 AND recipient.item_id = $2
           """,
           [person, item, revision]
         ) do
      {:ok, %{rows: [[attributes, body, head]]}} ->
        with :ok <- revision_visibility(attributes),
             {:ok, message} when is_map(message) and not is_struct(message) <- Attributes.decode(body),
             {:ok, files} <- revision_files(person, item, revision) do
          {:ok, %{"rev" => head, "revision" => %{
            "request_revision" => revision, "message" => message, "files" => files
          }}}
        else
          {:error, :not_found} = error -> error
          _ -> {:error, :storage_unavailable}
        end

      {:ok, %{rows: []}} -> {:error, :not_found}
      {:error, _} -> {:error, :storage_unavailable}
    end
  end

  def recipient_revision(_person, _item, _revision), do: {:error, :not_found}

  defp revision_visibility(attributes) do
    case Attributes.decode(attributes) do
      {:ok, %{visible: true}} -> :ok
      {:ok, %{visible: false}} -> {:error, :not_found}
      _ -> {:error, :storage_unavailable}
    end
  end

  defp revision_files(person, item, revision) do
    address = Attributes.key({person, item, revision})

    with {:ok, %{rows: [[stored]]}} <-
           Transaction.query(
             "SELECT body FROM harklight.compatibility_facts WHERE family = 'ordinary_revision_files' AND address = $1",
             [address]
           ),
         {:ok, files} when is_list(files) <- Attributes.decode(stored) do
      {:ok, files}
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  defp fetch(person, "i" <> _ = item_id, origin) do
    case Transaction.query(
           """
           SELECT message.sender_id, message.message_id, message.origin_key,
                  message.session_handle, message.revision, message.accepted_position,
                  message.free_order, message.accepted_at_ms, message.terminal,
                  message.attributes, revision.body
           FROM harklight.message_recipients AS recipient
           JOIN harklight.messages AS message ON message.id = recipient.message_id AND message.kind = 'ordinary'
           JOIN harklight.message_revisions AS revision
             ON revision.message_id = message.id AND revision.revision = message.revision
           WHERE recipient.person_id = $1 AND recipient.item_id = $2
             AND message.person_id = $1 AND ($3::text IS NULL OR message.origin_key = $3)
           """,
           [person, item_id, origin]
         ) do
      {:ok, %{rows: [row]}} -> decode_row(person, row)
      {:ok, %{rows: []}} -> {:error, :not_found}
      {:error, _} = error -> error
    end
  end

  defp fetch(_person, _item_id, _origin), do: {:error, :not_found}

  defp decode_row(person, [
         sender,
         message_id,
         origin,
         session,
         revision,
         position,
         order,
         accepted_at,
         terminal,
         attributes,
         body
       ]) do
    with {:ok, attributes} when is_map(attributes) <- decode_attributes(attributes),
         {:ok, message} when is_map(message) <- Attributes.decode(body),
         {:ok, terminal} <- decode_terminal(terminal) do
      record =
        Map.merge(attributes, %{
          message: message,
          origin: origin,
          session_handle: session,
          revision: revision,
          accepted_seq: position,
          free_order: order,
          accepted_at: accepted_at,
          terminal: terminal
        })

      {:ok, {person, sender, message_id}, record}
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  defp decode_attributes(value) when is_map(value) and map_size(value) == 0, do: {:ok, value}
  defp decode_attributes(value), do: Attributes.decode(value)

  defp decode_terminal(nil), do: {:ok, nil}
  defp decode_terminal(value), do: Attributes.decode(value)
end
