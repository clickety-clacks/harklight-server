if System.get_env("HARKLIGHT_POSTGRES_TEST_URL") do
  defmodule AttentionServer.Repository.PostgresTransactionTest do
    use ExUnit.Case, async: false

    alias AttentionServer.Repository.Postgres.{Operations, Pool, Repo, Transaction}

    setup_all do
      config = [url: System.fetch_env!("HARKLIGHT_POSTGRES_TEST_URL")]
      assert {:error, :postgres_disabled} = Pool.start_link(config)
      Application.put_env(:attention_server, :postgres_repository, true)
      on_exit(fn -> Application.delete_env(:attention_server, :postgres_repository) end)
      assert {:error, :postgres_not_active} = Pool.start_link(config)

      start_supervised!({Repo, config})
      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.server_identity VALUES (true, 'fixture', $1, 1)",
                 [:binary.copy(<<1>>, 32)]
               )
      assert {:error, :postgres_not_active} = Transaction.verify_ready()
      assert {:ok, _} =
               Transaction.query(
                 """
                 INSERT INTO harklight.import_state
                   (import_id, source_sha256, schema_sha256, phase)
                 VALUES ('fixture', $1, $2, 'loading')
                 """,
                 [:binary.copy(<<2>>, 32), :binary.copy(<<3>>, 32)]
               )
      assert {:error, :postgres_not_active} = Transaction.verify_ready()
      assert {:ok, _} =
               Transaction.query(
                 """
                 UPDATE harklight.import_state
                 SET phase = 'validated', manifest_sha256 = $1
                 WHERE import_id = 'fixture' AND phase = 'loading'
                 """,
                 [:binary.copy(<<4>>, 32)]
               )
      assert {:error, :postgres_not_active} = Transaction.verify_ready()
      assert {:ok, _} =
               Transaction.query(
                 "UPDATE harklight.import_state SET phase = 'active' WHERE phase = 'validated'",
                 []
               )
      stop_supervised!(Repo)
      start_supervised!({Pool, config})
      assert :ok = Transaction.verify_ready()

      for person <- ["synthetic-one", "synthetic-two", "retry-person", "lock-a", "lock-b", "receipt-person"] do
        assert {:ok, _} =
                 Transaction.query("INSERT INTO harklight.people (id) VALUES ($1)", [person])
      end

      :ok
    end

    test "committed pages preserve person and Agent position and retention contracts" do
      alias AttentionServer.Repository.Postgres.Streams

      person = "synthetic-page"
      scope = {person, ""}
      agent = {person, "synthetic-agent"}

      assert {:ok, _} = Transaction.query("INSERT INTO harklight.people (id) VALUES ($1)", [person])

      a = %{change("item-one") | session_handle: "session-one", created_at_ms: 10}
      b = %{change("shared-message:two") | session_handle: "session-two", created_at_ms: 20}
      c = %{change("item-three") | session_handle: "session-one", created_at_ms: 30}

      assert {:ok, 3} =
               Transaction.run([scope, agent], fn heads ->
                 with {:ok, 2} <- Transaction.append(heads, scope, [a, c]) do
                   Transaction.append(heads, agent, [a, b, c])
                 end
               end)

      assert {:ok, [%{rev: 1}], true, 1} = Streams.person_page(person, 0, 1)
      assert {:ok, [%{rev: 2}], false, 2} = Streams.person_page(person, 1, 10)
      assert {:ok, [], false, 2} = Streams.person_page(person, 2, 10)
      assert {:error, :cursor_expired} = Streams.person_page(person, 3, 10)
      assert {:ok, [%{rev: 1}, %{rev: 3}], false, 3} = Streams.agent_page(agent, 0, 10, nil, false)
      assert {:ok, [%{rev: 2}], false, 3} = Streams.agent_page(agent, 0, 10, "session-two", true)
      assert {:ok, [], false, 3} = Streams.agent_page(agent, 0, 10, "absent", true)

      assert {:ok, 1} = Streams.prune(scope, 20)
      assert {:ok, 2} = Streams.prune(agent, 30)
      assert {:error, :cursor_expired} = Streams.person_page(person, 0, 10)
      assert {:error, :cursor_expired} = Streams.agent_page(agent, 1, 10, nil, true)
      assert {:ok, [%{rev: 3}], false, 3} = Streams.agent_page(agent, 2, 10, nil, true)
    end

    test "unrelated streams commit independently and same-stream revisions follow commit order" do
      observer = self()
      scope = {"synthetic-one", ""}
      unrelated = {"synthetic-two", ""}

      first =
        Task.async(fn ->
          Transaction.run([scope], fn heads ->
            with {:ok, 1} <- Transaction.append(heads, scope, [change("first")]) do
              send(observer, :first_appended)
              receive do
                :commit_first -> {:ok, 1}
              after
                4_000 -> {:error, :test_timeout}
              end
            end
          end)
        end)

      assert_receive :first_appended, 2_000

      second =
        Task.async(fn ->
          Transaction.run([scope], &Transaction.append(&1, scope, [change("second")]))
        end)

      assert {:ok, 1} =
               Transaction.run([unrelated], &Transaction.append(&1, unrelated, [change("third")]))

      assert {:ok, %{rows: []}} =
               Transaction.query(
                 "SELECT revision FROM harklight.stream_changes WHERE person_id = $1",
                 [elem(scope, 0)]
               )

      send(first.pid, :commit_first)
      assert {:ok, 1} = Task.await(first)
      assert {:ok, 2} = Task.await(second)

      assert {:ok, %{rows: [[1, "first"], [2, "second"]]}} =
               Transaction.query(
                 """
                 SELECT revision, resource_id FROM harklight.stream_changes
                 WHERE person_id = $1 ORDER BY revision
                 """,
                 [elem(scope, 0)]
               )
    end

    test "ordinary item facts are scoped to the recipient or enrolled origin" do
      alias AttentionServer.Repository.{AgentItems, RecipientItems}
      alias AttentionServer.Repository.Postgres.Attributes

      person = "synthetic-item-owner"
      origin = "synthetic-item-origin"
      body = %{"title" => "Synthetic item"}
      facts = Attributes.encode(%{field_warnings: []})

      assert {:ok, _} = Transaction.query("INSERT INTO harklight.people (id) VALUES ($1)", [person])
      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.principals (key, person_id, kind, revision) VALUES ($1, $2, 'device', 1)",
                 [origin, person]
               )
      assert {:ok, _} =
               Transaction.query(
                 """
                 INSERT INTO harklight.messages
                   (id, person_id, origin_key, sender_id, message_id, revision, accepted_at_ms,
                    accepted_position, free_order, attributes)
                 VALUES ('fixture-item', $1, $2, 'app:test', 'fixture-message', 1, 20, 7, 8, $3)
                 """,
                 [person, origin, facts]
               )
      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.message_revisions VALUES ('fixture-item', 1, 20, $1)",
                 [Attributes.encode(body)]
               )
      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.message_recipients (message_id, person_id, item_id) VALUES ('fixture-item', $1, 'i7')",
                 [person]
               )

      assert {:ok, {^person, "app:test", "fixture-message"} = key,
              %{message: ^body, revision: 1, accepted_seq: 7, free_order: 8} = record} =
               RecipientItems.fetch(:postgres, person, "i7")

      assert {:ok, ^key, ^record} = AgentItems.owned_item(:postgres, {person, origin}, "i7")
      assert {:error, :not_found} = AgentItems.owned_item(:postgres, {person, "other"}, "i7")
      assert {:error, :not_found} = RecipientItems.fetch(:postgres, "other", "i7")

      assert {:ok, _} =
               Transaction.query(
                 "UPDATE harklight.messages SET attributes = $1 WHERE id = 'fixture-item'",
                 [Attributes.encode(%{"full_bounced_at_ms" => 99})]
               )

      assert {:error, :not_found} = RecipientItems.fetch(:postgres, person, "i7")
      assert {:ok, ^key, _} = AgentItems.owned_item(:postgres, {person, origin}, "i7")
    end

    test "an attachment is served only after its complete chunk sequence and digest verify" do
      alias AttentionServer.Repository.Postgres.Uploads

      person = "synthetic-upload-person"
      owner = "synthetic-upload-owner"
      ref = {person, owner, "synthetic-attachment"}
      full = :binary.copy(<<42>>, Uploads.chunk_bytes())
      tail = <<0, 1, 255>>
      size = byte_size(full) + byte_size(tail)
      digest = :crypto.hash(:sha256, [full, tail])

      assert {:ok, _} = Transaction.query("INSERT INTO harklight.people (id) VALUES ($1)", [person])
      assert {:error, :upload_cap_exceeded} =
               Uploads.begin_upload(
                 person, owner, "synthetic-attachment", size, digest, %{},
                 upload_limits(size - 1, size)
               )
      assert {:ok, "synthetic-attachment"} =
               Uploads.begin_upload(
                 person, owner, "synthetic-attachment", size, digest, %{}, upload_limits(size, size)
               )
      assert {:error, :not_found} = Uploads.read_chunk(person, "synthetic-attachment", 0)
      assert {:error, :invalid_chunk} = Uploads.put_chunk(ref, 0, tail)
      assert {:error, :not_found} =
               Uploads.put_chunk({person, "other-owner", "synthetic-attachment"}, 0, full)

      assert {:ok, :stored} = Uploads.put_chunk(ref, 0, full)
      assert {:ok, :stored} = Uploads.put_chunk(ref, 0, full)
      assert {:error, :upload_conflict} =
               Uploads.put_chunk(ref, 0, :binary.copy(<<43>>, byte_size(full)))
      assert {:error, :incomplete_upload} = Uploads.complete(ref, 10)
      assert {:ok, :stored} = Uploads.put_chunk(ref, 1, tail)
      assert {:ok, "synthetic-attachment"} = Uploads.complete(ref, 10)
      assert {:ok, "synthetic-attachment"} = Uploads.complete(ref, 11)
      assert {:ok, ^full} = Uploads.read_chunk(person, "synthetic-attachment", 0)
      assert {:ok, ^tail} = Uploads.read_chunk(person, "synthetic-attachment", 1)
      assert {:error, :not_found} = Uploads.read_chunk("other-person", "synthetic-attachment", 0)
      assert {:error, :upload_completed} = Uploads.put_chunk(ref, 1, tail)

      assert {:ok, "corrupt-attachment"} =
               Uploads.begin_upload(
                 person, owner, "corrupt-attachment", 3, digest, %{}, upload_limits(:none, size + 3)
               )
      corrupt = {person, owner, "corrupt-attachment"}
      assert {:ok, :stored} = Uploads.put_chunk(corrupt, 0, tail)
      assert {:error, :upload_digest_mismatch} = Uploads.complete(corrupt, 10)
      assert {:error, :not_found} = Uploads.read_chunk(person, "corrupt-attachment", 0)

      empty_digest = :crypto.hash(:sha256, <<>>)
      assert {:ok, "empty-attachment"} =
               Uploads.begin_upload(
                 person, owner, "empty-attachment", 0, empty_digest, %{}, upload_limits(:none, size + 3)
               )
      assert {:ok, "empty-attachment"} = Uploads.complete({person, owner, "empty-attachment"}, 10)
    end

    test "a committed upload reservation can be replayed after its response is lost" do
      alias AttentionServer.Repository.Postgres.Uploads

      person = "synthetic-upload-replay"
      owner = "synthetic-owner"
      id = "resumable-upload"
      bytes = <<1, 2, 3>>
      digest = :crypto.hash(:sha256, bytes)
      metadata = %{filename: "fixture.bin"}
      observer = self()
      assert {:ok, _} = Transaction.query("INSERT INTO harklight.people VALUES ($1)", [person])

      {writer, monitor} =
        spawn_monitor(fn ->
          result = Uploads.begin_upload(person, owner, id, 3, digest, metadata, upload_limits(3, 3))
          send(observer, {:reservation_committed, result})
          receive do: (:response_delivered -> :ok)
        end)

      assert_receive {:reservation_committed, {:ok, ^id}}, 2_000
      Process.exit(writer, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^writer, :killed}, 2_000

      assert {:ok, ^id} =
               Uploads.begin_upload(person, owner, id, 3, digest, metadata, upload_limits(2, 2))
      assert {:error, :upload_conflict} =
               Uploads.begin_upload(person, owner, id, 3, digest, %{}, upload_limits(3, 3))
      assert {:error, :upload_conflict} =
               Uploads.begin_upload(person, "other-owner", id, 3, digest, metadata, upload_limits(3, 3))
      assert {:ok, :stored} = Uploads.put_chunk({person, owner, id}, 0, bytes)
      assert {:ok, ^id} = Uploads.complete({person, owner, id}, 11)
      assert {:ok, ^bytes} = Uploads.read_chunk(person, id, 0)
    end

    test "parallel reservations share the account cap across upload owners" do
      alias AttentionServer.Repository.Postgres.Uploads

      person = "synthetic-upload-quota"
      bytes = <<1, 2, 3>>
      digest = :crypto.hash(:sha256, bytes)
      observer = self()
      assert {:ok, _} = Transaction.query("INSERT INTO harklight.people VALUES ($1)", [person])

      blocker =
        Task.async(fn ->
          Transaction.run([], fn _ ->
            assert {:ok, %{num_rows: 1}} =
                     Transaction.query(
                       "SELECT id FROM harklight.people WHERE id = $1 FOR NO KEY UPDATE",
                       [person]
                     )

            send(observer, :account_locked)
            receive do: (:release_account -> {:ok, :released})
          end)
        end)

      assert_receive :account_locked, 2_000

      requests =
        for id <- ["quota-one", "quota-two"] do
          Task.async(fn ->
            Uploads.begin_upload(person, id, id, 3, digest, %{}, upload_limits(3, 5))
          end)
        end

      await_admission_waits(100)
      send(blocker.pid, :release_account)
      assert {:ok, :released} = Task.await(blocker)
      assert [{:error, :upload_cap_exceeded}, {:ok, winner}] =
               requests |> Enum.map(&Task.await/1) |> Enum.sort()

      assert {:ok, :stored} = Uploads.put_chunk({person, winner, winner}, 0, bytes)
      assert {:ok, 0} = Uploads.cleanup(person, 99)
      assert {:ok, 1} = Uploads.cleanup(person, 100)
      assert {:ok, %{rows: [[0]]}} =
               Transaction.query("SELECT count(*) FROM harklight.upload_chunks WHERE upload_id = $1", [winner])
      assert {:ok, "replacement"} =
               Uploads.begin_upload(person, "owner", "replacement", 3, digest, %{}, upload_limits(3, 5))
    end

    test "cleanup skips attachment links in flight and only retires expired orphans" do
      alias AttentionServer.Repository.Postgres.Uploads

      person = "synthetic-upload-cleanup"
      owner = "synthetic-upload-writer"
      target = {person, owner, "linked-upload"}
      orphan = {person, owner, "orphan-upload"}
      message = "synthetic-attachment-message"
      observer = self()
      assert {:ok, _} = Transaction.query("INSERT INTO harklight.people VALUES ($1)", [person])
      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.principals (key, person_id, kind, revision) VALUES ($1, $2, 'device', 1)",
                 [owner, person]
               )
      assert {:ok, _} =
               Transaction.query(
                 """
                 INSERT INTO harklight.messages
                   (id, person_id, origin_key, sender_id, message_id, revision, accepted_at_ms)
                 VALUES ($1, $2, $3, 'app:fixture', 'message', 1, 10)
                 """,
                 [message, person, owner]
               )
      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.message_revisions VALUES ($1, 1, 10, '{}')",
                 [message]
               )

      for {_person, _owner, id} <- [target, orphan] do
        assert {:ok, ^id} =
                 Uploads.begin_upload(
                   person, owner, id, 0, :crypto.hash(:sha256, <<>>), %{}, upload_limits(0, 0)
                 )
      end

      assert {:error, :transaction_required} = Uploads.link(target, message, 1, 0, %{})
      assert {:error, :incomplete_upload} =
               Transaction.run([], fn _ -> Uploads.link(target, message, 1, 0, %{}) end)
      assert {:ok, "linked-upload"} = Uploads.complete(target, 10)

      linker =
        Task.async(fn ->
          Transaction.run([], fn _ ->
            assert {:ok, "linked-upload"} = Uploads.link(target, message, 1, 0, %{})
            send(observer, :attachment_linked)
            receive do: (:commit_link -> {:ok, :linked})
          end)
        end)

      assert_receive :attachment_linked, 2_000
      assert {:ok, 1} = Uploads.cleanup(person, 100)
      send(linker.pid, :commit_link)
      assert {:ok, :linked} = Task.await(linker)
      assert {:ok, 0} = Uploads.cleanup(person, 100)
      assert {:error, :not_found} =
               Transaction.run([], fn _ -> Uploads.link(orphan, message, 1, 1, %{}) end)
      assert {:ok, %{rows: [["linked-upload"]]}} =
               Transaction.query("SELECT upload_id FROM harklight.message_uploads WHERE message_id = $1", [message])
    end

    defp upload_limits(file, account) do
      %{file_limit: file, account_limit: account, expires_at_ms: 100}
    end

    defp await_admission_waits(0), do: flunk("both admissions did not reach the account lock")

    defp await_admission_waits(remaining) do
      assert {:ok, %{rows: [[count]]}} =
               Transaction.query(
                 """
                 SELECT count(*) FROM pg_stat_activity
                 WHERE wait_event_type = 'Lock'
                   AND query LIKE 'SELECT id FROM harklight.people%FOR NO KEY UPDATE'
                 """,
                 []
               )

      if count < 2 do
        Process.sleep(10)
        await_admission_waits(remaining - 1)
      end
    end

    test "reversed multi-stream inputs wait on the same first lock" do
      left = {"lock-a", ""}
      right = {"lock-b", ""}
      observer = self()

      assert {:ok, _} = Transaction.run([left, right], fn heads -> {:ok, heads} end)

      blocker =
        Task.async(fn ->
          Transaction.run([left], fn _heads ->
            send(observer, :left_locked)
            receive do: (:release_left -> {:ok, :released})
          end)
        end)

      assert_receive :left_locked, 2_000

      reversed =
        Task.async(fn ->
          Transaction.run([right, left], fn heads ->
            assert Map.keys(heads) |> Enum.sort() == [left, right]
            {:ok, :reversed}
          end)
        end)

      await_stream_lock_wait(100)

      # A caller using the original reverse order would already hold right.
      assert {:ok, :right_available} =
               Transaction.run([], fn _ ->
                 assert {:ok, %{rows: [[0]]}} =
                          Transaction.query(
                            """
                            SELECT head FROM harklight.stream_heads
                            WHERE person_id = $1 AND origin_key = $2 FOR UPDATE NOWAIT
                            """,
                            Tuple.to_list(right)
                          )

                 {:ok, :right_available}
               end)

      forward =
        Task.async(fn ->
          Transaction.run([left, right], fn heads ->
            assert Map.keys(heads) |> Enum.sort() == [left, right]
            {:ok, :forward}
          end)
        end)

      send(blocker.pid, :release_left)
      assert {:ok, :released} = Task.await(blocker)
      assert {:ok, :reversed} = Task.await(reversed)
      assert {:ok, :forward} = Task.await(forward)
    end

    defp await_stream_lock_wait(0), do: flunk("request did not reach the held stream lock")

    defp await_stream_lock_wait(remaining) do
      assert {:ok, %{rows: [[waiting?]]}} =
               Transaction.query(
                 """
                 SELECT EXISTS (
                   SELECT 1 FROM pg_stat_activity
                   WHERE wait_event_type = 'Lock'
                     AND query LIKE '%harklight.stream_heads%'
                 )
                 """,
                 []
               )

      unless waiting? do
        Process.sleep(10)
        await_stream_lock_wait(remaining - 1)
      end
    end

    test "known transaction aborts retry without carrying previous writes forward" do
      scope = {"retry-person", ""}
      attempt = make_ref()

      operation = "stable-operation"

      assert {:ok, {^operation, 1}} =
               Transaction.run([scope], fn heads ->
                 assert {:ok, 1} = Transaction.append(heads, scope, [change(operation)])

                 if Process.put(attempt, true) do
                   {:ok, {operation, 1}}
                 else
                   force_abort("40001")
                 end
               end)

      # Dispatch only after committed success. Both attempts return database
      # work to this caller, and no callback invokes the dispatch.
      dispatch = fn identity ->
        assert {:ok, %{rows: [[1]]}} =
                 Transaction.query(
                   "SELECT head FROM harklight.stream_heads WHERE person_id = $1",
                   [elem(scope, 0)]
                 )

        send(self(), {:dispatched, identity})
      end

      dispatch.(operation)
      assert_receive {:dispatched, ^operation}
      refute_received {:dispatched, _}

      exhaustion = make_ref()
      assert {:error, :storage_unavailable} =
               Transaction.run([scope], fn heads ->
                 Process.put(exhaustion, Process.get(exhaustion, 0) + 1)
                 assert {:ok, 2} = Transaction.append(heads, scope, [change("discard")])
                 force_abort("40P01")
               end)

      assert Process.delete(exhaustion) == 3
      refute_received {:dispatched, _}

      assert {:ok, %{rows: [[1, "stable-operation"]]}} =
               Transaction.query(
                 """
                 SELECT revision, resource_id FROM harklight.stream_changes
                 WHERE person_id = $1 ORDER BY revision
                 """,
                 [elem(scope, 0)]
               )
    end

    defp force_abort("40001") do
      Transaction.query("DO $$ BEGIN RAISE EXCEPTION USING ERRCODE = '40001'; END $$", [])
    end

    defp force_abort("40P01") do
      Transaction.query("DO $$ BEGIN RAISE EXCEPTION USING ERRCODE = '40P01'; END $$", [])
    end

    test "a retry keeps the exact original outcome and rollback releases the operation" do
      scope = {"receipt-person", ""}
      operation = {"receipt-person", "", "app", "fixture-operation"}
      body = %{"entry" => %{"id" => "e1", "rev" => 1}}

      assert {:error, :synthetic_failure} =
               Transaction.run([scope], fn heads ->
                 Operations.execute(operation, "digest", 1, nil, fn ->
                   assert {:ok, 1} = Transaction.append(heads, scope, [change("rolled-back")])
                   {:error, :synthetic_failure}
                 end)
               end)

      assert {:ok, {200, ^body, false}} =
               Transaction.run([scope], fn heads ->
                 Operations.execute(operation, "digest", 1, nil, fn ->
                   assert {:ok, 1} = Transaction.append(heads, scope, [change("accepted")])
                   {:ok, 200, body}
                 end)
               end)

      assert {:ok, {200, ^body, true}} =
               Transaction.run([scope], fn _heads ->
                 Operations.execute(operation, "digest", 2, nil, fn ->
                   flunk("retained operation ran again")
                 end)
               end)

      assert {:error, :idempotency_conflict} =
               Transaction.run([scope], fn _heads ->
                 Operations.execute(operation, "different", 2, nil, fn ->
                   flunk("conflicting operation ran")
                 end)
               end)
    end

    defp change(id) do
      %{kind: "item", resource_id: id, session_handle: nil, created_at_ms: 1, first_seen_at_ms: 1}
    end
  end
end
