defmodule AttentionServer.ClientAPI.SharedAttachments do
  @moduledoc false

  alias AttentionServer.{ConversationFiles, Engine, Store}
  alias AttentionServer.ClientAPI.{Reads, Uploads}

  # Fetch staging metadata under the serialized sender identity, but transfer
  # the bytes in the connection process. A lost response can safely replay the
  # JSON receipt without using the staging handles again.
  def prepare(store, scope, ids, message_id, sender, op_id, now),
    do: prepare(store, scope, ids, %{message_id: message_id, sender: sender, op_id: op_id,
      now: now, revision: 1, kind: "agent"})

  def prepare(store, {person, actor}, ids, attrs) do
    Enum.reduce_while(ids, {:ok, []}, fn id, {:ok, files} ->
      case fetch_staged(store, {person, actor}, id, attrs.kind) do
        {:ok, file} ->
          metadata = %{
            id: ConversationFiles.new_upload_id(), person: person, uploader: actor,
            uploader_kind: attrs.kind, op_id: attrs.op_id, sender_id: attrs.sender,
            message_id: attrs.message_id, revision: attrs.revision,
            name: file.name, size: file.size, sha256: file.sha256,
            last_activity_ms: attrs.now, content_type: file.content_type, staged_file_id: id
          }

          {:cont, {:ok, [{metadata, file.path} | files]}}

        {:error, :not_found} ->
          {:halt, {:error, :invalid_field, "file_ids"}}

        error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, files} ->
        files = Enum.reverse(files)
        names = Enum.map(files, fn {metadata, _} -> metadata.name end)

        if Enum.uniq(names) == names, do: {:ok, files}, else: {:error, :invalid_field, "file_ids"}

      error -> error
    end
  end

  defp fetch_staged(store, {person, _}, id, "client"), do: Uploads.fetch(store, person, id)
  defp fetch_staged(store, scope, id, "agent"), do: Uploads.fetch_owned(store, scope, id)

  def stage_and_apply(dir, files, apply) do
    with {:ok, staged} <- stage(dir, files) do
      try do
        apply.(staged)
      after
        discard(staged)
      end
    end
  end

  def commit(store, transition, audit, []), do: Store.commit(store, transition, audit)

  def commit(store, transition, audit, staged) do
    with {:ok, published} <- publish(store, staged) do
      case Store.commit(store, attach(transition, published), audit) do
        {:ok, _} = result ->
          result

        error ->
          discard(published)
          error
      end
    end
  end

  def validate_ids(ids) when is_list(ids) do
    if Enum.uniq(ids) == ids and Enum.all?(ids, &is_binary/1),
      do: :ok,
      else: {:error, :invalid_field, "file_ids"}
  end

  def validate_ids(_ids), do: {:error, :invalid_field, "file_ids"}

  def stage(dir, files) do
    Enum.reduce_while(files, {:ok, []}, fn {metadata, source}, {:ok, staged} ->
      case Store.stage_upload(dir, metadata, source) do
        {:ok, path} -> {:cont, {:ok, [{metadata, path} | staged]}}
        error ->
          discard(staged)
          {:halt, error}
      end
    end)
    |> case do
      {:ok, staged} -> {:ok, Enum.reverse(staged)}
      error -> error
    end
  end

  def publish(store, staged) do
    Enum.reduce_while(staged, {:ok, []}, fn {metadata, path}, {:ok, published} ->
      case Store.publish_staged_upload(store.upload_dir, metadata, path) do
        {:ok, target} -> {:cont, {:ok, [{metadata, target} | published]}}
        error ->
          discard(published)
          {:halt, error}
      end
    end)
  end

  def discard(files), do: Enum.each(files, fn {_, path} -> File.rm(path) end)

  def attach(engine, files) do
    Enum.reduce(files, engine, fn {metadata, _}, next ->
      Engine.put_row(next, :uploads, metadata.id, metadata)
    end)
  end

  def references(staged) do
    Enum.map(staged, fn {metadata, _} ->
      %{
        "upload_id" => metadata.id,
        "file_id" => "shared-file_" <> metadata.id,
        "name" => metadata.name, "size" => metadata.size,
        "sha256" => metadata.sha256, "content_type" => metadata.content_type
      }
    end)
  end

  def wire(files), do: Enum.map(files, &Map.delete(&1, "upload_id"))

  def download(relay, principal, "shared-file_" <> upload_id) do
    GenServer.call(relay, {:client_api, __MODULE__, {principal, upload_id}}, :infinity)
  end

  @doc false
  def handle_call({who, upload_id}, _from, state) do
    engine = state.store.engine

    result =
      with {:ok, principal} <- Reads.client_principal(engine, who),
           true <- Reads.shared_readable?(state, principal.person) || {:error, :unsupported},
           %{message_id: message_id} = upload <- engine.uploads[upload_id],
           %{} = record <- engine.shared_messages[message_id],
           {:ok, high, cutoff} <- allowed_history(engine, principal, record),
           true <- visible_upload?(engine, record, upload, high, cutoff) || {:error, :not_found},
           {:ok, path} <- Store.get_upload(state.store, upload_id) do
        {:ok, %{path: path, size: upload.size, sha256: upload.sha256,
                name: upload.name, content_type: upload.content_type}}
      else
        nil -> {:error, :not_found}
        error -> error
      end

    {:reply, result, state}
  end

  defp allowed_history(engine, %{kind: kind, person: person, key: actor}, record)
       when kind in [:device, :browser] do
    with %{sender_person: ^person, sender_key: ^actor} <- record,
         true <- Engine.shared_sender_allowed?(engine, person, actor, record.sender_id) do
      {:ok, Map.get(record, :sender_frozen_revision, record.revision), Map.get(record, :sender_frozen_entry_high)}
    else
      _ ->
        case Map.get(engine.shared_recipients, {record.id, person}) do
          %{frozen: %{revision: revision, entry_high: high}} -> {:ok, revision, high}
          %{} -> {:ok, record.revision, nil}
          nil -> {:error, :not_found}
        end
    end
  end

  defp allowed_history(_engine, _principal, _record), do: {:error, :not_found}

  defp visible_upload?(engine, record, %{shared_entry_id: entry_id} = upload, high, cutoff) do
    case engine.shared_entries[entry_id] do
      %{shared_id: id, position: position, request_revision: revision, files: files}
      when id == record.id and revision <= high and (is_nil(cutoff) or position <= cutoff) ->
        retained?(files, upload.id)

      _ ->
        false
    end
  end

  defp visible_upload?(_engine, record, upload, high, _cutoff) do
    upload.revision <= high and retained?(from_revision(record, upload.revision), upload.id)
  end

  defp retained?(files, upload_id), do: Enum.any?(files, &(&1["upload_id"] == upload_id))

  def entry_upload_ids(nil), do: []
  def entry_upload_ids(entry), do: Enum.map(Map.get(entry, :files, []), & &1["upload_id"])

  def from_revision(record, revision) do
    if record.revision == revision do
      Map.get(record, :files, [])
    else
      get_in(record, [:history, revision, :files]) || []
    end
  end

  def upload_ids(nil), do: []
  def upload_ids(record) do
    current = Map.get(record, :files, [])
    historical = for {_, version} <- Map.get(record, :history, %{}), file <- Map.get(version, :files, []), do: file

    MapSet.new(current ++ historical, & &1["upload_id"])
  end
end
