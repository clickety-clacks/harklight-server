defmodule AttentionServer.Migration.VerifySharedReaderFacts do
  @moduledoc false
  alias AttentionServer.SharedMessages
  alias AttentionServer.Migration.{Bundle, Frames, ImportSession, ImportStatus}
  alias AttentionServer.Repository.Postgres.Attributes
  alias AttentionServer.Store.RecordTable
  alias Exqlite.Sqlite3

  @families ["shared_read_access", "shared_revision_scope", "shared_revision_files"]

  def run(db, identity, bundle) do
    with {:ok, manifest, summary} <- Bundle.verify(bundle),
         true <- identity.source == Base.decode16!(summary.sha256, case: :lower),
         {:ok, %{"phase" => phase}} when phase in ["loading", "validated"] <- ImportStatus.read(db, identity),
         {:ok, counts} <- source_index(Path.join(bundle, "records.frames"), manifest.data.records,
           fn source -> compare(db, source, "", <<>>, Map.new(@families, &{&1, 0})) end),
         :ok <- each(counts, fn {family, count} -> count(db, family, count) end),
         {:ok, ^manifest, ^summary} <- Bundle.verify(bundle) do
      {:ok, counts}
    else
      false -> {:error, :import_source_mismatch}
      {:error, _} = error -> error
      _ -> {:error, :shared_reader_verification_requires_staged_import}
    end
  end

  # Expected facts are built from sealed source frames, never from destination
  # authority or recipient projections. TEMP storage and keyset pages bound the
  # working set even when a person has many retained shared messages.
  defp source_index(path, summary, fun) do
    with {:ok, source} <- Sqlite3.open(":memory:") do
      try do
        with :ok <- Sqlite3.execute(source, """
               PRAGMA temp_store=FILE;
               PRAGMA temp.cache_size=-2048;
               CREATE TEMP TABLE expected(family TEXT,address BLOB,body BLOB NOT NULL,PRIMARY KEY(family,address)) WITHOUT ROWID;
               CREATE TEMP TABLE message_groups(address BLOB PRIMARY KEY,group_present INTEGER NOT NULL) WITHOUT ROWID;
               """),
             {:ok, :ok} <- Frames.fold(path, summary, :ok, fn row, :ok ->
               with :ok <- base(source, row), do: {:ok, :ok}
             end),
             {:ok, :ok} <- Frames.fold(path, summary, :ok, fn row, :ok ->
               with :ok <- derive(source, row), do: {:ok, :ok}
             end) do
          fun.(source)
        end
      after
        Sqlite3.close(source)
      end
    end
  end

  defp base(source, {{:row, :persons, person}, _}),
    do: put(source, "shared_read_access", person, false)
  defp base(source, {{:row, :shared_messages, id}, record}),
    do: RecordTable.run(source, "INSERT INTO temp.message_groups VALUES(?,?)",
      [{:blob, Attributes.key(id)}, if(Map.has_key?(record, :group_id), do: 1, else: 0)])
  defp base(_, _), do: :ok

  defp derive(source, {{:row, :local_organizations, _}, %MapSet{} = members}),
    do: each(members, &put(source, "shared_read_access", &1, true))
  defp derive(source, {{:row, :hosted_organizations, _}, %{members: members}}),
    do: each(members, &put(source, "shared_read_access", &1, true))
  defp derive(source, {{:row, :shared_messages, id}, record}) do
    versions = [record.revision | Map.keys(Map.get(record, :history, %{}))] |> Enum.uniq()
    with :ok <- if(Map.has_key?(record, :sender_frozen_revision),
           do: put(source, "shared_read_access", record.sender_person, true), else: :ok) do
      each(versions, fn revision ->
        files = record |> SharedMessages.historical_record(revision) |> Map.get(:files, [])
        put(source, "shared_revision_files", {id, revision}, files)
      end)
    end
  end
  defp derive(source, {{:row, :shared_recipients, {id, person}}, recipient}) do
    with {:ok, [[group]]} <- RecordTable.select(source,
           "SELECT group_present FROM temp.message_groups WHERE address=?", [{:blob, Attributes.key(id)}]),
         :ok <- put(source, "shared_revision_scope", {person, recipient.item_id},
           %{group?: group == 1, frozen_revision: get_in(recipient, [:frozen, :revision])}),
         :ok <- if(Map.has_key?(recipient, :frozen),
           do: put(source, "shared_read_access", person, true), else: :ok) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :shared_reader_source_reference_missing}
    end
  end
  defp derive(_, _), do: :ok

  defp put(source, family, key, value), do: RecordTable.run(source,
    "INSERT INTO temp.expected VALUES(?,?,?) ON CONFLICT(family,address) DO UPDATE SET body=excluded.body",
    [family, {:blob, Attributes.key(key)}, {:blob, Attributes.key(Attributes.encode(value))}])

  defp compare(db, source, family, address, counts) do
    with {:ok, rows} <- RecordTable.select(source, """
           SELECT family,address,body FROM temp.expected
           WHERE family>? OR (family=? AND address>?) ORDER BY family,address LIMIT 256
           """, [family, family, {:blob, address}]),
         :ok <- each(rows, fn [f, a, body] -> fact(db, f, a, :erlang.binary_to_term(body, [:safe])) end) do
      case rows do
        [] -> {:ok, counts}
        _ ->
          counts = Enum.reduce(rows, counts, fn [f, _, _], acc -> Map.update!(acc, f, &(&1 + 1)) end)
          [f, a, _] = List.last(rows)
          compare(db, source, f, a, counts)
      end
    end
  end

  defp fact(db, family, address, expected) do
    case ImportSession.query(db,
      "SELECT body FROM harklight.compatibility_facts WHERE family=$1 AND address=$2", [family, address]) do
      {:ok, %{rows: [[^expected]]}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, {:shared_reader_fact_mismatch, family,
        Base.encode16(:crypto.hash(:sha256, address), case: :lower)}}
    end
  end
  defp count(db, family, expected) do
    case ImportSession.query(db,
      "SELECT count(*) FROM harklight.compatibility_facts WHERE family=$1", [family]) do
      {:ok, %{rows: [[^expected]]}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, {:shared_reader_count_mismatch, family}}
    end
  end
  defp each(rows, fun), do: Enum.reduce_while(rows, :ok, fn row, :ok ->
    case fun.(row) do
      :ok -> {:cont, :ok}
      error -> {:halt, error}
    end
  end)
end
