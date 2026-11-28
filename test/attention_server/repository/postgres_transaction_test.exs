if System.get_env("HARKLIGHT_POSTGRES_TEST_URL") do
  defmodule AttentionServer.Repository.PostgresTransactionTest do
    use ExUnit.Case, async: false

    alias AttentionServer.Repository.Postgres.{Attributes, Jobs, Operations, Pool, Repo, Transaction}

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

    test "a stopped provider leaves its job recoverable after the lease expires" do
      insert_job("push", "synthetic-push", 10)

      assert {:ok, %{attempts: 1, body: %{item_id: "synthetic-item"}} = first} =
               Jobs.claim("push", 10, 20, "first-claim")

      assert {:ok, nil} = Jobs.claim("push", 19, 30, "unused-claim")
      assert {:ok, %{attempts: 2} = recovered} = Jobs.claim("push", 20, 30, "recovered")
      assert {:error, :claim_lost} = Jobs.complete(first, 21)
      assert {:ok, :recorded} = Jobs.retry_at(recovered, 21, 25)
      assert {:ok, nil} = Jobs.claim("push", 24, 40, "too-early")
      assert {:ok, %{attempts: 3} = final} = Jobs.claim("push", 25, 40, "final-claim")
      assert {:ok, :recorded} = Jobs.complete(final, 26)
      assert {:ok, nil} = Jobs.claim("push", 40, 50, "done")
    end

    test "a late provider success can complete the current claim before reclaim" do
      insert_job("push", "late-success", 10)

      assert {:ok, %{id: "late-success"} = claim} = Jobs.claim("push", 10, 20, "slow-provider")
      assert {:ok, :recorded} = Jobs.complete(claim, 20)
      assert {:ok, nil} = Jobs.claim("push", 20, 30, "no-redelivery")
      assert {{20, 1}, nil} = job_outcome("push", "late-success")
    end

    test "an expired final attempt is terminal and the next eligible job progresses" do
      insert_job("reminder", "expired-final", 10, 4)
      insert_job("reminder", "next-reminder", 20)

      assert {:ok, %{id: "expired-final", attempts: 5} = claim} =
               Jobs.claim("reminder", 10, 20, "final-attempt")

      assert {:ok, nil} = Jobs.claim("reminder", 19, 30, "before-expiry")

      assert {:ok, %{id: "next-reminder", attempts: 1} = next} =
               Jobs.claim("reminder", 20, 30, "other-job")

      assert {:error, :claim_lost} = Jobs.complete(claim, 21)
      assert {:ok, :recorded} = Jobs.complete(next, 21)
      assert {:ok, nil} = Jobs.claim("reminder", 40, 50, "no-reclaim")

      assert {{20, 5}, %{attempts: 5, at_ms: 20, reason: :lease_expired}} =
               job_outcome("reminder", "expired-final")
    end

    test "retrying a failed final attempt records terminal exhaustion" do
      insert_job("reminder", "failed-final", 10, 4)

      assert {:ok, %{id: "failed-final", attempts: 5} = claim} =
               Jobs.claim("reminder", 10, 20, "final-claim")

      assert {:ok, :exhausted} = Jobs.retry_at(claim, 12, 15)
      assert {:ok, nil} = Jobs.claim("reminder", 15, 20, "no-retry")

      assert {{12, 5}, %{attempts: 5, at_ms: 12, reason: :retry_exhausted}} =
               job_outcome("reminder", "failed-final")
    end

    defp insert_job(kind, id, ready_at_ms, attempts \\ 0) do
      assert {:ok, _} =
               Transaction.query(
                 """
                 INSERT INTO harklight.jobs (kind, id, person_id, ready_at_ms, attempts, body)
                 VALUES ($1, $2, 'receipt-person', $3, $4, $5)
                 """,
                 [kind, id, ready_at_ms, attempts, Attributes.encode(%{item_id: "synthetic-item"})]
               )
    end

    defp job_outcome(kind, id) do
      assert {:ok, %{rows: [[completed_at_ms, attempts, encoded]]}} =
               Transaction.query(
                 """
                 SELECT completed_at_ms, attempts, fact.body
                 FROM harklight.jobs
                 LEFT JOIN harklight.compatibility_facts AS fact
                   ON fact.family = 'job_exhaustion' AND fact.address = $3
                 WHERE kind = $1 AND id = $2 AND claim_token IS NULL AND claim_until_ms IS NULL
                 """,
                 [kind, id, Attributes.key({kind, id})]
               )

      {:ok, outcome} = if is_nil(encoded), do: {:ok, nil}, else: Attributes.decode(encoded)
      {{completed_at_ms, attempts}, outcome}
    end

    defp change(id) do
      %{kind: "item", resource_id: id, session_handle: nil, created_at_ms: 1, first_seen_at_ms: 1}
    end
  end
end
