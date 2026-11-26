defmodule AttentionServer.ClientAPI.SharedAttachments do
  @moduledoc false

  alias AttentionServer.{ConversationFiles, Engine, SharedMessages, Store}
  alias AttentionServer.ClientAPI.{Reads, Uploads}

  # Fetch staging metadata under the serialized sender identity, but transfer
  # the bytes in the connection process. A lost response can safely replay the
  # JSON receipt without using the staging handles again.
  def prepare(store, {person, actor}, ids, message_id, sender, op_id, now) do
    Enum.reduce_while(ids, {:ok, []}, fn id, {:ok, files} ->
      case Uploads.fetch_owned(store, {person, actor}, id) do
        {:ok, file} ->
          metadata = %{
            id: ConversationFiles.new_upload_id(), person: person, uploader: actor,
            uploader_kind: "agent", op_id: op_id, sender_id: sender, message_id: message_id,
            revision: 1, name: file.name, size: file.size, sha256: file.sha256,
            last_activity_ms: now, content_type: file.content_type, staged_file_id: id
          }

          {:cont, {:ok, [{metadata, file.path} | files]}}

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
           {:ok, high} <- allowed_revision(engine, principal, record),
           true <- visible_upload?(record, upload, high) || {:error, :not_found},
           {:ok, path} <- Store.get_upload(state.store, upload_id) do
        {:ok, %{path: path, size: upload.size, sha256: upload.sha256,
                name: upload.name, content_type: upload.content_type}}
      else
        nil -> {:error, :not_found}
        error -> error
      end

    {:reply, result, state}
  end

  defp allowed_revision(engine, %{kind: kind, person: person, key: actor}, record)
       when kind in [:device, :browser] do
    with %{sender_person: ^person, sender_key: ^actor} <- record,
         true <- Engine.shared_sender_allowed?(engine, person, actor, record.sender_id) do
      {:ok, Map.get(record, :sender_frozen_revision, record.revision)}
    else
      _ ->
        case Map.get(engine.shared_recipients, {record.id, person}) do
          %{frozen: %{revision: revision}} -> {:ok, revision}
          %{} -> {:ok, record.revision}
          nil -> {:error, :not_found}
        end
    end
  end

  defp allowed_revision(_engine, _principal, _record), do: {:error, :not_found}

  defp visible_upload?(record, upload, high) do
    upload.revision <= high and
      Enum.any?(from_revision(record, upload.revision), &(&1["upload_id"] == upload.id))
  end

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
