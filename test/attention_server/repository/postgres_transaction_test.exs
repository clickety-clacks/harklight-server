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

    test "a concurrent retention pass keeps page bounds and rows in one snapshot" do
      alias AttentionServer.Repository.Postgres.Streams

      person = "synthetic-retention"
      scope = {person, ""}
      observer = self()

      assert {:ok, _} = Transaction.query("INSERT INTO harklight.people (id) VALUES ($1)", [person])
      assert {:ok, 1} = Transaction.run([scope], &Transaction.append(&1, scope, [change("old")]))

      # An imported high can be beyond every retained change.
      assert {:ok, _} =
               Transaction.query(
                 "UPDATE harklight.stream_heads SET head = 9 WHERE person_id = $1 AND origin_key = ''",
                 [person]
               )

      blocker =
        Task.async(fn ->
          Transaction.run([], fn _ ->
            assert {:ok, %{rows: [[1]]}} =
                     Transaction.query(
                       """
                       SELECT revision FROM harklight.stream_changes
                       WHERE person_id = $1 AND origin_key = '' AND revision = 1 FOR UPDATE
                       """,
                       [person]
                     )

            send(observer, :row_locked)
            receive do: (:release_row -> {:ok, :released})
          end)
        end)

      assert_receive :row_locked, 2_000
      prune = Task.async(fn -> Streams.prune(scope, 50) end)
      await_lock_wait("stream_changes", 100)

      assert {:ok, [%{rev: 1}], false, 1} = Streams.person_page(person, 0, 10)
      assert {:ok, [], false, 8} = Streams.person_page(person, 8, 10)

      send(blocker.pid, :release_row)
      assert {:ok, :released} = Task.await(blocker)
      assert {:ok, 1} = Task.await(prune)
      assert {:ok, 1} = Streams.prune(scope, 50)
      assert {:error, :cursor_expired} = Streams.person_page(person, 0, 10)
      assert {:ok, [], false, 8} = Streams.person_page(person, 8, 10)
      assert {:error, :cursor_expired} = Streams.person_page(person, 10, 10)
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

      await_lock_wait("stream_heads", 100)

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

    defp await_lock_wait(table, 0), do: flunk("request did not wait for a lock on #{table}")

    defp await_lock_wait(table, remaining) do
      assert {:ok, %{rows: [[waiting?]]}} =
               Transaction.query(
                 """
                 SELECT EXISTS (
                   SELECT 1 FROM pg_stat_activity
                   WHERE wait_event_type = 'Lock'
                     AND query LIKE $1
                 )
                 """,
                 ["%harklight.#{table}%"]
               )

      unless waiting? do
        Process.sleep(10)
        await_lock_wait(table, remaining - 1)
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

    test "concurrent same-key requests replay the winner and a new key runs independently" do
      key = {"receipt-person", "", "app", "concurrent-operation"}
      observer = self()

      first =
        Task.async(fn ->
          Transaction.run([], fn _ ->
            Operations.execute(key, "same-digest", 1, nil, fn ->
              send(observer, :operation_reserved)
              receive do: (:finish_operation -> {:ok, 200, %{"result" => "first"}})
            end)
          end)
        end)

      assert_receive :operation_reserved, 2_000

      other =
        Task.async(fn ->
          send(observer, :second_started)

          Transaction.run([], fn _ ->
            Operations.execute(key, "same-digest", 2, nil, fn ->
              flunk("same key executed twice")
            end)
          end)
        end)

      assert_receive :second_started, 2_000
      await_lock_wait("operation_receipts", 100)
      send(first.pid, :finish_operation)
      assert {:ok, {200, %{"result" => "first"}, false}} = Task.await(first)
      assert {:ok, {200, %{"result" => "first"}, true}} = Task.await(other)

      assert {:ok, {200, %{"result" => "new"}, false}} =
               Transaction.run([], fn _ ->
                 Operations.execute(put_elem(key, 3, "different-operation"), "same-digest", 3, nil, fn ->
                   {:ok, 200, %{"result" => "new"}}
                 end)
               end)
    end

    test "a new database connection replays the exact result after the writer dies following COMMIT" do
      observer = self()
      key = {"lost-response-person", "", "app", "lost-response-operation"}
      scope = {"lost-response-person", ""}
      body = %{"entry" => %{"id" => "stable-entry", "rev" => 7}}

      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.people (id) VALUES ($1)", [elem(scope, 0)])

      {writer, monitor} =
        spawn_monitor(fn ->
          {:ok, {200, ^body, false}} =
            Transaction.run([scope], fn heads ->
              Operations.execute(key, "digest", 1, nil, fn ->
                assert {:ok, _} = Transaction.append(heads, scope, [change("lost-response")])
                {:ok, 200, body}
              end)
            end)

          send(observer, {:committed, self()})
          receive do: (:deliver_response -> send(observer, {:response, body}))
        end)

      assert_receive {:committed, ^writer}, 2_000
      Process.exit(writer, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^writer, :killed}, 2_000
      refute_received {:response, _}

      config = [name: nil, pool_size: 1, url: System.fetch_env!("HARKLIGHT_POSTGRES_TEST_URL")]
      fresh = start_supervised!({Repo, config}, id: :fresh_retry_pool)
      previous = Repo.put_dynamic_repo(fresh)

      try do
        assert {:ok, {200, ^body, true}} =
                 Transaction.run([scope], fn _ ->
                   Operations.execute(key, "digest", 2, nil, fn ->
                     flunk("lost response replay repeated the write")
                   end)
                 end)

        assert {:ok, %{rows: [[1]]}} =
                 Transaction.query(
                   "SELECT count(*) FROM harklight.stream_changes WHERE person_id = $1 AND resource_id = $2",
                   [elem(scope, 0), "lost-response"]
                 )
      after
        Repo.put_dynamic_repo(previous)
      end
    end

    defp change(id) do
      %{kind: "item", resource_id: id, session_handle: nil, created_at_ms: 1, first_seen_at_ms: 1}
    end
  end
end
