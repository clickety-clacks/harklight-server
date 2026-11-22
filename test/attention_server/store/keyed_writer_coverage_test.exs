defmodule AttentionServer.Store.KeyedWriterCoverageTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias AttentionServer.{Engine, Relay, Store}
  alias AttentionServer.Store.RecordCodec
  alias Exqlite.Sqlite3

  @moduletag :tmp_dir
  @op "00000000-0000-4000-8000-000000000020"
  @second_upload "00000000-0000-4000-8000-000000000021"
  @bytes <<1, 2, 3, 5, 8>>
  @audit %{person: "A", actor: "origin", action: "keyed_writer_probe"}

  # Every public Store function, classified by what it may write on a keyed
  # store. A new public function fails the first test until it is classified,
  # and an engine writer fails the second test until it has a refusal probe.
  #
  # :keyed_writer  - changes engine state through the keyed commit; must
  #                  succeed on a keyed store and survive a keyed reopen.
  # :keyed_refused - changes engine state but has no keyed form; must refuse
  #                  on a keyed store and leave every table and file unchanged.
  # :side_table    - writes only mode-neutral tables that a keyed reopen reads
  #                  back unchanged (startup cleanup of the 0.1 inbox snapshot
  #                  paging rows, change log setup, which conversion advances
  #                  to the keyed generation).
  # :files         - stages or prunes upload files against engine metadata.
  # :read / :lifecycle - no durable write of engine state.
  @classification %{
    {:open, 1} => :lifecycle,
    {:open, 2} => :lifecycle,
    {:close, 1} => :lifecycle,
    {:enable_change_log, 1} => :side_table,
    {:commit, 3} => :keyed_writer,
    {:commit, 4} => :keyed_writer,
    {:put_published_upload, 2} => :keyed_writer,
    {:delete_uploads, 3} => :keyed_writer,
    {:stage_upload, 3} => :files,
    {:publish_staged_upload, 3} => :files,
    {:prune_untracked_uploads, 1} => :files,
    {:get_upload, 2} => :read,
    {:verify_upload, 2} => :read,
    {:verify_retained_uploads, 1} => :read,
    {:discard_building_inbox_snapshots, 1} => :side_table,
    {:discard_browser_inbox_snapshots, 1} => :side_table
  }

  setup %{tmp_dir: directory} do
    path = Path.join(directory, "state.sqlite3")
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    source = Path.join(directory, "source.bin")
    File.write!(source, @bytes)
    size = byte_size(@bytes)
    digest = Base.encode16(:crypto.hash(:sha256, @bytes), case: :lower)

    attributes = %{
      "op_id" => @op,
      "sender_id" => "app:test",
      "message_id" => "item",
      "revision" => 1,
      "name" => "payload.bin"
    }

    assert {:ok, lease} = Relay.upload_begin(relay, "origin", attributes, size)

    assert {:ok, %{"upload_id" => id}} =
             Relay.upload_file(relay, "origin", attributes, source, size, digest, lease)

    assert :ok = Relay.upload_end(relay, lease)
    assert :ok = GenServer.stop(relay)

    assert {:ok, keyed} = Store.open(path, :keyed_cutover)
    assert keyed.mode == :keyed
    assert :ok = Store.close(keyed)
    %{path: path, id: id}
  end

  test "every public Store function is classified for keyed mode" do
    exported = Store.__info__(:functions) -- [__struct__: 0, __struct__: 1]
    assert Enum.sort(exported) == Enum.sort(Map.keys(@classification))
  end

  test "every keyed-refused writer refuses a keyed store and leaves its state unchanged", %{
    path: path
  } do
    writers = for {fun, :keyed_refused} <- @classification, do: fun
    probes = refusal_probes()
    assert Enum.sort(writers) == Enum.sort(Map.keys(probes))

    for {{name, arity} = fun, probe} <- probes do
      assert {:ok, store} = Store.open(path, :keyed)
      before = fingerprint(store)

      result = probe.(store)

      assert match?({:error, :storage_unavailable}, result) or
               match?({:error, :storage_unavailable, _}, result),
             "#{name}/#{arity} did not refuse a keyed store: #{inspect(result)}"

      assert fingerprint(store) == before, "#{inspect(fun)} wrote to a keyed store"
      assert :ok = Store.close(store)

      assert {:ok, reopened} = Store.open(path, :keyed)
      assert reopened.engine == store.engine
      assert :ok = Store.close(reopened)
    end
  end

  test "every keyed writer commits on a keyed store and survives a keyed reopen", %{
    path: path,
    id: id
  } do
    writers = for {fun, :keyed_writer} <- @classification, do: fun
    probes = writer_probes(id)
    assert Enum.sort(writers) == Enum.sort(Map.keys(probes))

    # Deletion runs last because the upload probe copies the fixture upload.
    for {{name, arity}, probe} <- Enum.sort_by(probes, &(elem(elem(&1, 0), 0) == :delete_uploads)) do
      assert {:ok, store} = Store.open(path, :keyed)
      before = fingerprint(store)

      assert {:ok, committed} = probe.(store), "#{name}/#{arity} did not commit"
      assert committed.mode == :keyed
      assert committed.version == store.version + 1
      refute fingerprint(committed) == before, "#{name}/#{arity} wrote nothing"
      assert :ok = Store.close(committed)

      assert {:ok, reopened} = Store.open(path, :keyed)
      assert reopened.version == committed.version
      assert records(reopened.engine) == records(committed.engine)
      assert :ok = Store.close(reopened)
    end
  end

  test "a keyed commit without touched addresses is refused", %{path: path} do
    assert {:ok, store} = Store.open(path, :keyed)
    before = fingerprint(store)
    assert {:error, :storage_unavailable, ^store} = Store.commit(store, proposal(store), @audit)
    assert fingerprint(store) == before
    assert :ok = Store.close(store)
  end

  test "a keyed commit appends change log rows that a keyed reopen keeps", %{path: path} do
    assert {:ok, store} = Store.open(path, :keyed)
    assert {:ok, store} = Store.enable_change_log(store)
    assert {:ok, [[epoch]]} = epoch(store.db)

    frame = %{
      "v" => 1,
      "type" => "forward",
      "op_id" => "00000000-0000-4000-8000-000000000030",
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => "logged",
        "created_at" => 1,
        "title" => "Logged"
      }
    }

    {next, _reply} =
      Engine.request_line(store.engine, "origin", AttentionServer.JSON.encode_line(frame), 1_000)

    assert {:ok, committed} = Store.commit(store, touched(store, next), @audit)
    assert {:ok, [[count]]} = query(committed.db, "SELECT count(*) FROM client_changes")
    assert count > 0

    assert {:ok, [[version]]} =
             query(committed.db, "SELECT store_version FROM client_change_log_state WHERE id = 1")

    assert version == committed.version
    assert :ok = Store.close(committed)

    assert {:ok, reopened} = Store.open(path, :keyed)
    assert {:ok, reopened} = Store.enable_change_log(reopened)
    assert {:ok, [[^epoch]]} = epoch(reopened.db)
    assert {:ok, [[^count]]} = query(reopened.db, "SELECT count(*) FROM client_changes")
    assert :ok = Store.close(reopened)
  end

  defp epoch(db) do
    case query(db, "SELECT epoch FROM client_change_log_epoch WHERE id = 1") do
      {:ok, []} -> {:ok, [[0]]}
      other -> other
    end
  end

  test "a keyed commit that omits a changed record is refused and writes nothing", %{path: path} do
    assert {:ok, store} = Store.open(path, :keyed)
    {next, touched} = touched(store, forward(store, "omitted"))
    assert MapSet.size(touched) > 1
    before = fingerprint(store)

    log =
      capture_log(fn ->
        for omitted <- touched do
          partial = MapSet.delete(touched, omitted)

          assert {:error, :storage_unavailable, ^store} =
                   Store.commit(store, {next, partial}, @audit)

          assert fingerprint(store) == before
        end
      end)

    assert log =~ "cause=untouched_change"
    assert :ok = Store.close(store)
    assert {:ok, reopened} = Store.open(path, :keyed)
    assert records(reopened.engine) == records(store.engine)
    assert :ok = Store.close(reopened)
  end

  test "a keyed commit with an empty touched set and a real change is refused", %{path: path} do
    assert {:ok, store} = Store.open(path, :keyed)
    {next, _touched} = touched(store, proposal(store))
    before = fingerprint(store)

    capture_log(fn ->
      assert {:error, :storage_unavailable, ^store} =
               Store.commit(store, {next, MapSet.new()}, @audit)
    end)

    assert fingerprint(store) == before
    assert {:ok, ^store} = Store.commit(store, {store.engine, MapSet.new()}, @audit)
    assert :ok = Store.close(store)
  end

  test "a keyed commit may over-report touched records", %{path: path, id: id} do
    assert {:ok, store} = Store.open(path, :keyed)
    {next, touched} = touched(store, proposal(store))
    extra = MapSet.put(touched, {:row, :uploads, id})
    assert {:ok, committed} = Store.commit(store, {next, extra}, @audit)
    assert :ok = Store.close(committed)

    assert {:ok, reopened} = Store.open(path, :keyed)
    assert records(reopened.engine) == records(committed.engine)
    assert Map.has_key?(reopened.engine.uploads, id)
    assert :ok = Store.close(reopened)
  end

  test "a keyed upload deletion removes its file after the commit", %{path: path, id: id} do
    assert {:ok, store} = Store.open(path, :keyed)
    file = Path.join(store.upload_dir, id)
    assert File.exists?(file)
    assert {:ok, committed} = Store.delete_uploads(store, [id], @audit)
    refute File.exists?(file)
    refute Map.has_key?(committed.engine.uploads, id)
    assert :ok = Store.close(committed)

    assert {:ok, reopened} = Store.open(path, :keyed)
    refute Map.has_key?(reopened.engine.uploads, id)
    assert :ok = Store.close(reopened)
  end

  defp refusal_probes, do: %{}

  defp writer_probes(id) do
    %{
      {:commit, 3} => fn store -> Store.commit(store, touched(store, proposal(store)), @audit) end,
      {:commit, 4} => fn store ->
        Store.commit(store, touched(store, proposal(store)), @audit, [])
      end,
      {:put_published_upload, 2} => fn store ->
        metadata = %{Map.fetch!(store.engine.uploads, id) | id: @second_upload}
        Store.put_published_upload(store, metadata)
      end,
      {:delete_uploads, 3} => fn store -> Store.delete_uploads(store, [id], @audit) end
    }
  end

  defp proposal(store) do
    assert {:ok, engine} = Engine.setup_person(store.engine, "B#{store.version}")
    engine
  end

  defp forward(store, id) do
    frame = %{
      "v" => 1,
      "type" => "forward",
      "op_id" => "00000000-0000-4000-8000-000000000040",
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => id,
        "created_at" => 1,
        "title" => id
      }
    }

    {next, {:ok, _}} =
      Engine.request_line(store.engine, "origin", AttentionServer.JSON.encode_line(frame), 1_000)

    next
  end

  defp records(engine) do
    Map.new(RecordCodec.encode(engine), fn {key, payload, _checksum} -> {key, payload} end)
  end

  # Test oracle: every record whose encoding differs is a touched address.
  defp touched(store, next) do
    old = records(store.engine)
    new = records(next)

    addresses =
      MapSet.union(MapSet.new(Map.keys(old)), MapSet.new(Map.keys(new)))
      |> Enum.reject(&(Map.fetch(old, &1) === Map.fetch(new, &1)))
      |> MapSet.new(&elem(:erlang.binary_to_term(&1), 1))

    {next, addresses}
  end

  defp fingerprint(store) do
    {:ok, tables} =
      query(store.db, "SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name")

    rows =
      for [table] <- tables do
        {:ok, rows} = query(store.db, ~s(SELECT * FROM "#{table}" ORDER BY 1))
        {table, Enum.sort(rows)}
      end

    files =
      for name <- Enum.sort(File.ls!(store.upload_dir)) do
        {name, File.read!(Path.join(store.upload_dir, name))}
      end

    :crypto.hash(:sha256, :erlang.term_to_binary({rows, files}))
  end

  defp query(db, sql) do
    {:ok, statement} = Sqlite3.prepare(db, sql)

    try do
      Sqlite3.fetch_all(db, statement)
    after
      Sqlite3.release(db, statement)
    end
  end
end
