defmodule AttentionServer.Repository.Postgres.SharedItems do
  @moduledoc false

  alias AttentionServer.{LargeData, SharedMessages}
  alias AttentionServer.Repository.Postgres.{Attributes, Transaction}

  # Only the recipient row and its small visibility facts are needed to
  # authorize a historical read. The normalized group reference may be gone.
  def revision_scope(person, "shared_" <> _ = item, groups?) do
    case Transaction.query(
           """
           SELECT message.id, message.revision, scope.body, stream.head,
                  coalesce(resource.revision, nullif(stream.seed_revision, 0))
           FROM harklight.message_recipients AS recipient
           JOIN harklight.messages AS message ON message.id = recipient.message_id AND message.kind = 'shared'
           LEFT JOIN harklight.compatibility_facts AS scope
             ON scope.family = 'shared_revision_scope' AND scope.address = $3
           LEFT JOIN harklight.stream_heads AS stream
             ON stream.person_id = recipient.person_id AND stream.origin_key = ''
           LEFT JOIN harklight.stream_resource_revisions AS resource
             ON resource.person_id = stream.person_id AND resource.origin_key = stream.origin_key
             AND resource.resource_kind = 'item' AND resource.resource_id = 'shared-item:' || message.id
           WHERE recipient.person_id = $1 AND recipient.item_id = $2
           """,
           [person, item, Attributes.key({person, item})]
         ) do
      {:ok, %{rows: [[id, current, encoded, head, resource_rev]]}} ->
        with {:ok, limit} <- revision_limit(encoded, current, groups?),
             :ok <- committed_position(head, resource_rev) do
          {:ok, %{id: id, limit: limit, head: head}}
        end

      {:ok, %{rows: []}} -> {:error, :not_found}
      {:error, _} -> {:error, :storage_unavailable}
    end
  end

  def revision_scope(_person, _item, _groups?), do: {:error, :not_found}

  def revision(%{id: id, limit: limit, head: head}, revision)
      when is_integer(revision) and revision > 0 and revision <= limit do
    with {:ok, %{rows: [[body, stored_files]]}} <-
           Transaction.query(
             """
             SELECT revision.body, files.body FROM harklight.message_revisions AS revision
             JOIN harklight.compatibility_facts AS files
               ON files.family = 'shared_revision_files' AND files.address = $3
             WHERE revision.message_id = $1 AND revision.revision = $2
             """,
             [id, revision, Attributes.key({id, revision})]
           ),
         {:ok, message} when is_map(message) and not is_struct(message) <- Attributes.decode(body),
         false <- LargeData.contains_ref?(message),
         {:ok, files} when is_list(files) <- Attributes.decode(stored_files),
         true <- Enum.all?(files, &is_map/1) do
      payload = SharedMessages.revision(%{revision: revision, message: message, files: files})
      {:ok, %{"rev" => head, "revision" => payload}}
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  def revision(_scope, _revision), do: {:error, :not_found}

  defp revision_limit(encoded, current, groups?) do
    case Attributes.decode(encoded) do
      {:ok, %{group?: true}} when not groups? -> {:error, :not_found}
      {:ok, %{group?: group?, frozen_revision: nil}} when is_boolean(group?) -> {:ok, current}
      {:ok, %{group?: group?, frozen_revision: rev}}
      when is_boolean(group?) and is_integer(rev) and rev > 0 and rev <= current -> {:ok, rev}
      _ -> {:error, :storage_unavailable}
    end
  end

  defp committed_position(nil, _rev), do: {:error, :storage_unavailable}
  defp committed_position(_head, nil), do: {:error, :not_found}
  defp committed_position(head, rev) when rev > 0 and rev <= head, do: :ok
  defp committed_position(_head, _rev), do: {:error, :storage_unavailable}
end
