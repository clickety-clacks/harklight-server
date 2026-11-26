defmodule AttentionServer.ClientAPI.Uploads do
  @moduledoc """
  Files sent with `POST /v2/files`, held until a reply uses them
  (docs/client-api-v2.md §5).

  Today's uploads belong to one message: their operation, sender, message and
  revision are fixed when the upload starts. `POST /v2/files` names no
  message, so its bytes are staged here, with their name and `Content-Type`,
  under the person. A reply that names their `file_id`s then uploads them to
  its message through today's upload path, exactly as today's clients do.

  Staged files live beside the store's uploads and are kept for the 30-day
  retention window. Call these from the Relay process.
  """

  alias AttentionServer.ClientAPI.Idempotency
  alias AttentionServer.Diagnostics
  alias Exqlite.Sqlite3

  @retention_ms 30 * 24 * 60 * 60 * 1000
  @dir "client-api-v2-staged"

  @schema [
    """
    CREATE TABLE IF NOT EXISTS client_staged_files (
      file_id TEXT PRIMARY KEY NOT NULL,
      person_id TEXT NOT NULL,
      name TEXT NOT NULL,
      content_type TEXT NOT NULL,
      size INTEGER NOT NULL,
      sha256 TEXT NOT NULL,
      at_ms INTEGER NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS client_staged_files_at ON client_staged_files (at_ms)",
    """
    CREATE TABLE IF NOT EXISTS client_staged_file_owners (
      file_id TEXT PRIMARY KEY NOT NULL REFERENCES client_staged_files(file_id) ON DELETE CASCADE,
      owner_key TEXT NOT NULL
    )
    """,
    "CREATE TABLE IF NOT EXISTS client_discarded_files (file_id TEXT PRIMARY KEY NOT NULL)"
  ]

  @type staged :: %{
          file_id: String.t(),
          name: String.t(),
          content_type: String.t(),
          size: non_neg_integer(),
          sha256: String.t(),
          path: String.t()
        }

  @doc """
  The directory staged bytes are written to, created if needed. A request
  body is first written to a temporary file there, so staging is a rename.
  """
  @spec dir(struct()) :: {:ok, String.t()} | {:error, :disabled | :storage_unavailable}
  def dir(%AttentionServer.Store{change_log: true, upload_dir: upload_dir})
      when is_binary(upload_dir) do
    path = Path.join(upload_dir, @dir)

    case File.mkdir_p(path) do
      :ok -> {:ok, path}
      _ -> {:error, :storage_unavailable}
    end
  end

  def dir(%AttentionServer.Store{}), do: {:error, :disabled}

  @doc "Moves a fully written temporary file into place and records it."
  @spec stage(struct(), String.t(), map(), String.t()) ::
          {:ok, String.t()} | {:error, term()}
  def stage(store, person, meta, tmp_path, owner \\ nil, capture \\ nil)

  def stage(
        %AttentionServer.Store{change_log: true, db: db} = store,
        person,
        meta,
        tmp_path,
        owner,
        capture
      ) do
    file_id = "fs" <> Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
    now = System.system_time(:millisecond)

    with {:ok, dir} <- dir(store),
         :ok <- ensure(db),
         :ok <- prune(db, dir, now - @retention_ms),
         path = Path.join(dir, file_id),
         :ok <- File.rename(tmp_path, path) do
      attrs = [file_id, person, meta.name, meta.content_type, meta.size, meta.sha256, now]

      case record_staged_file(db, file_id, attrs, owner, capture) do
        :ok ->
          {:ok, file_id}

        {:error, _} ->
          _ = File.rm(path)
          {:error, :storage_unavailable}
      end
    else
      {:error, :disabled} = error -> error
      _ -> {:error, :storage_unavailable}
    end
  end

  def stage(%AttentionServer.Store{}, _person, _meta, _tmp_path, _owner, _capture), do: {:error, :disabled}

  defp record_staged_file(db, file_id, attrs, owner, capture) do
    with :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE") do
      with :ok <-
             run(
               db,
               "INSERT INTO client_staged_files (file_id, person_id, name, content_type, size, sha256, at_ms) " <>
                 "VALUES (?, ?, ?, ?, ?, ?, ?)",
               attrs
             ),
           :ok <- remember_owner(db, file_id, owner),
           :ok <- capture_upload(capture, file_id),
           :ok <- Sqlite3.execute(db, "COMMIT") do
        :ok
      else
        error ->
          _ = Sqlite3.execute(db, "ROLLBACK")
          error
      end
    end
  end

  defp capture_upload(nil, _file_id), do: :ok
  defp capture_upload(capture, file_id), do: capture.(file_id)

  @doc "The person's staged file, if it exists and is retained."
  @spec fetch(struct(), String.t(), String.t()) ::
          {:ok, staged()} | {:error, :not_found | :disabled | :storage_unavailable}
  def fetch(%AttentionServer.Store{change_log: true, db: db} = store, person, "fs" <> _ = file_id) do
    with {:ok, dir} <- dir(store),
         :ok <- ensure(db),
         {:ok, row} <-
           one(
             db,
             "SELECT name, content_type, size, sha256 FROM client_staged_files " <>
               "WHERE file_id = ? AND person_id = ? AND at_ms >= ?",
             [file_id, person, System.system_time(:millisecond) - @retention_ms]
           ) do
      path = Path.join(dir, file_id)

      case row do
        [name, content_type, size, sha256] ->
          if File.regular?(path),
            do:
              {:ok,
               %{
                 file_id: file_id,
                 name: name,
                 content_type: content_type,
                 size: size,
                 sha256: sha256,
                 path: path
               }},
            else: {:error, :storage_unavailable}

        nil ->
          {:error, :not_found}
      end
    else
      {:error, :disabled} = error -> error
      _ -> {:error, :storage_unavailable}
    end
  end

  def fetch(%AttentionServer.Store{change_log: true}, _person, _file_id), do: {:error, :not_found}
  def fetch(%AttentionServer.Store{}, _person, _file_id), do: {:error, :disabled}

  @doc false
  def fetch_owned(store, {person, owner}, file_id) do
    with {:ok, file} <- fetch(store, person, file_id),
         {:ok, [^owner]} <-
           one(store.db, "SELECT owner_key FROM client_staged_file_owners WHERE file_id = ?", [
             file_id
           ]) do
      {:ok, file}
    else
      {:ok, _} -> {:error, :not_found}
      error -> error
    end
  end

  @doc false
  def verify_discard_owner(%AttentionServer.Store{change_log: true, db: db}, id, owner) do
    with :ok <- ensure(db) do
      case one(db, "SELECT owner_key FROM client_staged_file_owners WHERE file_id = ?", [id]) do
        # Handles staged before ownership tracking are still account-scoped.
        {:ok, nil} -> :ok
        {:ok, [^owner]} -> :ok
        {:ok, _} -> {:error, :not_found}
        _ -> {:error, :storage_unavailable}
      end
    end
  end

  def verify_discard_owner(_store, _id, _owner), do: {:error, :disabled}

  defp remember_owner(_db, _id, nil), do: :ok

  defp remember_owner(db, id, owner),
    do:
      run(db, "INSERT INTO client_staged_file_owners (file_id, owner_key) VALUES (?, ?)", [
        id,
        owner
      ])

  @doc "Forget an unattached file and its bytes, storing any replay receipt with the deletion."
  def discard(%AttentionServer.Store{change_log: true, db: db} = store, person, file_id, key, digest) do
    with :ok <- ensure(db),
         {:ok, [1]} <-
           one(
             db,
             "SELECT 1 FROM client_staged_files WHERE file_id = ? AND person_id = ? AND at_ms >= ?",
             [file_id, person, System.system_time(:millisecond) - @retention_ms]
           ),
         :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
         :ok <- commit_discard(store, db, person, file_id, key, digest) do
      :ok
    else
      {:ok, nil} -> {:error, :not_found}
      {:error, :disabled} = error -> error
      _ -> {:error, :storage_unavailable}
    end
  end

  def discard(%AttentionServer.Store{}, _person, _file_id, _key, _digest),
    do: {:error, :disabled}

  defp commit_discard(store, db, person, file_id, key, digest) do
    with :ok <- run(db, "DELETE FROM client_staged_file_owners WHERE file_id = ?", [file_id]),
         :ok <-
           run(db, "DELETE FROM client_staged_files WHERE file_id = ? AND person_id = ?", [
             file_id,
             person
           ]),
         :ok <- run(db, "INSERT INTO client_discarded_files (file_id) VALUES (?)", [file_id]),
         :ok <- save_discard_receipt(store, person, key, digest),
         :ok <- Sqlite3.execute(db, "COMMIT") do
      :ok
    else
      error ->
        _ = Sqlite3.execute(db, "ROLLBACK")
        error
    end
  end

  defp save_discard_receipt(_store, _person, nil, _digest), do: :ok

  defp save_discard_receipt(store, person, key, digest),
    do: Idempotency.save(store, person, key, digest, 204, %{"discarded_file" => true})

  @doc "Unlink committed discards outside Relay, retaining failed cleanup for another attempt."
  def clean_discarded(relay, file_id \\ nil) do
    case GenServer.call(relay, {:client_api, __MODULE__, {:discarded, file_id}}, :infinity) do
      {:ok, dir, rows} ->
        removed =
          for [id] <- rows, unlink_discarded(Path.join(dir, id)) == :ok, do: id

        if removed != [] do
          GenServer.call(relay, {:client_api, __MODULE__, {:discard_cleaned, removed}}, :infinity)
        end

      {:error, reason} ->
        Diagnostics.failure(:server_storage, :upload_delete, reason, :unavailable)
    end
  catch
    :exit, _reason -> :ok
  end

  @doc false
  def handle_call({:discarded, file_id}, _from, %{store: store} = state) do
    reply =
      with %AttentionServer.Store{change_log: true, db: db, upload_dir: dir} when is_binary(dir) <-
             store,
           {:ok, rows} <- pending_discards(db, file_id) do
        {:ok, Path.join(dir, @dir), rows}
      else
        {:error, _} = error -> error
        _ -> {:ok, nil, []}
      end

    {:reply, reply, state}
  end

  def handle_call({:discard_cleaned, ids}, _from, %{store: store} = state) do
    Enum.each(ids, fn id ->
      case run(store.db, "DELETE FROM client_discarded_files WHERE file_id = ?", [id]) do
        :ok -> :ok
        {:error, reason} -> Diagnostics.failure(:server_storage, :upload_delete, reason, :retry)
      end
    end)

    {:reply, :ok, state}
  end

  # The table is lazy, like the staged files themselves. Startup cleanup
  # need not initialize upload storage on a server that has never used it.
  defp pending_discards(db, nil) do
    with {:ok, [1]} <-
           one(db, "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", [
             "client_discarded_files"
           ]) do
      all(db, "SELECT file_id FROM client_discarded_files", [])
    else
      {:ok, nil} -> {:ok, []}
      error -> error
    end
  end

  defp pending_discards(db, file_id),
    do: all(db, "SELECT file_id FROM client_discarded_files WHERE file_id = ?", [file_id])

  # Attached copies have their own download IDs and paths.
  defp unlink_discarded(path) do
    case File.rm(path) do
      :ok ->
        :ok

      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        Diagnostics.failure(:server_storage, :upload_delete, reason, :retry)
        {:error, reason}
    end
  end

  defp prune(db, dir, before_ms) do
    with {:ok, rows} <-
           all(db, "SELECT file_id FROM client_staged_files WHERE at_ms < ?", [before_ms]) do
      Enum.each(rows, fn [file_id] -> File.rm(Path.join(dir, file_id)) end)

      with :ok <-
             run(
               db,
               "DELETE FROM client_staged_file_owners WHERE file_id IN (SELECT file_id FROM client_staged_files WHERE at_ms < ?)",
               [before_ms]
             ) do
        run(db, "DELETE FROM client_staged_files WHERE at_ms < ?", [before_ms])
      end
    end
  end

  defp ensure(db) do
    Enum.reduce_while(@schema, :ok, fn sql, :ok ->
      case Sqlite3.execute(db, sql) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp one(db, sql, params) do
    statement(db, sql, params, fn stmt ->
      case Sqlite3.step(db, stmt) do
        {:row, row} -> {:ok, row}
        :done -> {:ok, nil}
        error -> error
      end
    end)
  end

  defp all(db, sql, params), do: statement(db, sql, params, &collect(db, &1, []))

  defp collect(db, stmt, acc) do
    case Sqlite3.step(db, stmt) do
      {:row, row} -> collect(db, stmt, [row | acc])
      :done -> {:ok, Enum.reverse(acc)}
      error -> error
    end
  end

  defp run(db, sql, params) do
    case statement(db, sql, params, &Sqlite3.step(db, &1)) do
      :done -> :ok
      error -> error
    end
  end

  defp statement(db, sql, params, fun) do
    with {:ok, stmt} <- Sqlite3.prepare(db, sql) do
      result =
        case Sqlite3.bind(stmt, params) do
          :ok -> fun.(stmt)
          error -> error
        end

      _ = Sqlite3.release(db, stmt)
      result
    end
  end
end
