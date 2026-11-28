defmodule AttentionServer.Migration.VerifyAvailability do
  @moduledoc false
  alias AttentionServer.{AgentAPI.FeedActivity, ClientAPI.Resources}
  alias AttentionServer.Migration.{Bundle, Frames, ImportSession, ImportStatus}
  alias AttentionServer.Repository.Postgres.Attributes
  alias AttentionServer.Store.RecordTable
  alias Exqlite.Sqlite3

  # The query seam supports deterministic isolated source-change tests only.
  def run(db, identity, bundle, query \\ &ImportSession.query/3) do
    with {:ok, manifest, summary} <- Bundle.verify(bundle),
         true <- identity.source == Base.decode16!(summary.sha256, case: :lower),
         {:ok, %{"phase" => phase}} when phase in ["loading", "validated"] <- ImportStatus.read(db, identity),
         {:ok, n} <- with_source(Path.join(bundle, "records.frames"), manifest.data.records, fn source ->
           Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, 0, fn
             {{:row, :messages, {person, _, _} = key}, %{session_handle: handle} = record}, n
                 when is_binary(handle) ->
               with {:ok, value} <- projection(source, key, record),
                    :ok <- compare(db, {person, Resources.item_id(record)}, value, query), do: {:ok, n + 1}
             _, n -> {:ok, n}
           end)
         end),
         {:ok, %{rows: [[^n]]}} <- query.(db,
           "SELECT count(*) FROM harklight.compatibility_facts WHERE family='ordinary_item_availability'", []),
         {:ok, ^manifest, ^summary} <- Bundle.verify(bundle) do
      {:ok, %{availability: n}}
    else
      false -> {:error, :import_source_mismatch}
      {:error, _} = error -> error
      _ -> {:error, :availability_count_or_identity_mismatch}
    end
  end

  # A connection-local, file-backed index built only from sealed frames. Neither
  # the source SQLite database nor any destination row is used as its oracle.
  # A bounded SQLite cache avoids keeping all source sessions/events on BEAM.
  @doc false
  def with_source(path, summary, fun) do
    with {:ok, db} <- Sqlite3.open(":memory:") do
      try do
        with :ok <- Sqlite3.execute(db, """
               PRAGMA temp_store=FILE;
               PRAGMA temp.cache_size=-2048;
               CREATE TEMP TABLE source_values(address BLOB PRIMARY KEY,body BLOB NOT NULL) WITHOUT ROWID;
               CREATE TEMP TABLE latest_availability(address BLOB PRIMARY KEY,position INTEGER NOT NULL,body BLOB NOT NULL) WITHOUT ROWID;
               """),
             {:ok, :ok} <- Frames.fold(path, summary, :ok, fn row, :ok ->
               with :ok <- index(db, row), do: {:ok, :ok}
             end) do
          fun.(db)
        end
      after
        Sqlite3.close(db)
      end
    end
  end

  defp index(db, {{:row, family, _} = address, value})
       when family in [:origin_sessions, :item_availability, :events] do
    RecordTable.run(db, "INSERT INTO temp.source_values VALUES(?,?)",
      [{:blob, Attributes.key(address)}, {:blob, Attributes.key(value)}])
  end
  defp index(db, {{:row, :conversation_events, {person, seq}},
       %{key: {person, _, _} = key, payload: %{"kind" => "availability"} = payload}}) do
    RecordTable.run(db, """
      INSERT INTO temp.latest_availability VALUES(?,?,?)
      ON CONFLICT(address) DO UPDATE SET position=excluded.position,body=excluded.body
      WHERE excluded.position>position
      """, [{:blob, Attributes.key(key)}, seq, {:blob, Attributes.key(payload)}])
  end
  defp index(_, _), do: :ok

  @doc false
  def projection(source, {person, _, _} = key, record) do
    with {:ok, session} <- value(source, :source_values, {:row, :origin_sessions, {person, record.origin, record.session_handle}}),
         {:ok, raw} <- value(source, :source_values, {:row, :item_availability, key}),
         {:ok, latest} <- value(source, :latest_availability, key),
         {:ok, accepted} <- value(source, :source_values, {:row, :events, {person, record.accepted_seq}}) do
      legacy = latest || (accepted && Map.get(accepted, :payload)) || %{}
      {:ok, FeedActivity.committed_availability(record, session, raw, legacy)}
    end
  end

  defp value(db, table, key) when table in [:source_values, :latest_availability] do
    case RecordTable.select(db, "SELECT body FROM temp." <> Atom.to_string(table) <> " WHERE address=?",
      [{:blob, Attributes.key(key)}]) do
      {:ok, []} -> {:ok, nil}
      {:ok, [[body]]} -> {:ok, :erlang.binary_to_term(body, [:safe])}
      {:error, _} = error -> error
      _ -> {:error, :availability_source_mismatch}
    end
  end

  defp compare(db, key, value, query) do
    expected = Attributes.encode(value)
    case query.(db,
      "SELECT body FROM harklight.compatibility_facts WHERE family='ordinary_item_availability' AND address=$1",
      [Attributes.key(key)]) do
      {:ok, %{rows: [[^expected]]}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, {:availability_fact_mismatch,
        Base.encode16(:crypto.hash(:sha256, Attributes.key(key)), case: :lower)}}
    end
  end
end
