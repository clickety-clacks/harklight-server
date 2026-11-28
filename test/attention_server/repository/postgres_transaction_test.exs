if System.get_env("HARKLIGHT_POSTGRES_TEST_URL") do
  defmodule AttentionServer.Repository.PostgresTransactionTest do
    use ExUnit.Case, async: false

    alias AttentionServer.Repository.Postgres.{Pool, Repo, Transaction}

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

      for person <- ["synthetic-one", "synthetic-two", "retry-person"] do
        assert {:ok, _} =
                 Transaction.query("INSERT INTO harklight.people (id) VALUES ($1)", [person])
      end

      :ok
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

    test "known transaction aborts retry without carrying previous writes forward" do
      scope = {"retry-person", ""}
      attempt = make_ref()

      assert {:ok, 1} =
               Transaction.run([scope], fn heads ->
                 assert {:ok, 1} = Transaction.append(heads, scope, [change("stable-operation")])

                 if Process.put(attempt, true) do
                   {:ok, 1}
                 else
                   force_abort("40001")
                 end
               end)

      assert {:error, %Postgrex.Error{postgres: %{code: :deadlock_detected}}} =
               Transaction.run([scope], fn heads ->
                 send(self(), :retry_attempt)
                 assert {:ok, 2} = Transaction.append(heads, scope, [change("discard")])
                 force_abort("40P01")
               end)

      for _ <- 1..3, do: assert_received(:retry_attempt)
      refute_received :retry_attempt

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

    defp change(id) do
      %{kind: "item", resource_id: id, session_handle: nil, created_at_ms: 1, first_seen_at_ms: 1}
    end
  end
end
