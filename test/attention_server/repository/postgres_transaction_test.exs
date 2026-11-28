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

    @tag :tmp_dir
    test "a FORMAT32 browser cookie keeps its principal after projection to PostgreSQL", %{tmp_dir: dir} do
      alias AttentionServer.DashboardBrowserSession
      alias AttentionServer.ClientAPI.{Auth, Router}
      alias AttentionServer.Repository.{CommittedRead, Postgres.Identities}

      {relay, browser, backend, cookie, principal} = browser_fixture(dir)
      %{person: person, key: key, owner_revision: revision, session_id: hash} = principal
      conn = browser_request(:get, "/v2/me", cookie)

      assert {:ok, ^principal} = Auth.web(conn, Identities, :postgres)
      # Exercise the browser authentication seam in the real router. Both
      # requests still use the FORMAT32 relay for the /me projection.
      sqlite_opts = Router.init(relay: relay, auth: {:browser, backend, nil})
      postgres_opts = Router.init(relay: relay, auth: {:browser, Identities, nil})
      assert %{status: 200, resp_body: body} = Router.call(conn, sqlite_opts)
      assert %{status: 200, resp_body: ^body} = Router.call(conn, postgres_opts)

      assert {:ok, ^person, ^revision} = CommittedRead.execute(:postgres, {:owner_identity, key})
      assert {:ok, ^revision} = CommittedRead.execute(:postgres, {:owner_revision, person, key})
      assert {:error, :forbidden} =
               CommittedRead.execute(:postgres, {:owner_revision, "other-person", key})

      assert {:ok, _} =
               Transaction.query("UPDATE harklight.principals SET revision = revision + 1 WHERE key = $1", [key])
      assert {:error, :unauthorized} = Auth.web(conn, Identities, :postgres)
      assert {:ok, _} =
               Transaction.query("UPDATE harklight.principals SET revision = $2, revoked = true WHERE key = $1", [
                 key,
                 revision
               ])
      assert {:error, :forbidden} = CommittedRead.execute(:postgres, {:owner_identity, key})
      assert {:error, :unauthorized} = Auth.web(conn, Identities, :postgres)
      assert %{status: 401} = Router.call(conn, postgres_opts)
      assert {:ok, _} =
               Transaction.query("UPDATE harklight.principals SET revoked = false WHERE key = $1", [key])

      assert :ok = DashboardBrowserSession.end_session(browser, cookie)
      assert {:ok, _} = Transaction.query("DELETE FROM harklight.auth_sessions WHERE token_hash = $1", [hash])
      assert {:error, :unauthorized} = Auth.web(conn, backend, relay)
      assert {:error, :unauthorized} = Auth.web(conn, Identities, :postgres)
    end

    @tag :tmp_dir
    test "the isolated PostgreSQL router serves the same browser settings and ETag", %{tmp_dir: dir} do
      alias AttentionServer.{JSON, Relay}
      alias AttentionServer.ClientAPI.{Router, Writes}
      alias AttentionServer.Repository.Postgres.{Attributes, Identities}

      {relay, _browser, backend, cookie, principal} = browser_fixture(dir)
      %{person: person, key: key} = principal
      sqlite = Router.init(relay: relay, auth: {:browser, backend, nil})
      postgres = Router.init(relay: nil, repository: :postgres, auth: {:browser, Identities, nil})
      conn = browser_request(:get, "/v2/settings", cookie)

      assert %{status: 200, resp_body: initial} = original = Router.call(conn, sqlite)
      assert %{status: 200, resp_body: ^initial} = Router.call(conn, postgres)
      assert [tag] = Plug.Conn.get_resp_header(original, "etag")

      assert {:ok, 200, _, false} = Writes.alerts(relay, principal, "all", tag)
      assert %{status: 200, resp_body: body} = updated = Router.call(conn, sqlite)
      assert {:ok, %{"rev" => head, "settings" => %{"rev" => rev, "alerts" => "all"}}} =
               JSON.decode_line(body <> "\n")
      assert [updated_tag] = Plug.Conn.get_resp_header(updated, "etag")
      observer = self()

      writer =
        Task.async(fn ->
          Transaction.run([], fn _ ->
            assert {:ok, _} =
                     Transaction.query("UPDATE harklight.people SET settings = $2 WHERE id = $1", [
                       person,
                       Attributes.encode(%{alert_choices: "all"})
                     ])
            assert {:ok, _} =
                     Transaction.query(
                       "INSERT INTO harklight.stream_heads (person_id, origin_key, head) VALUES ($1, '', $2)",
                       [person, head]
                     )
            assert {:ok, _} =
                     Transaction.query(
                       """
                       INSERT INTO harklight.stream_resource_revisions
                         (person_id, origin_key, resource_kind, resource_id, revision)
                       VALUES ($1, '', 'settings', 'settings', $2)
                       """,
                       [person, rev]
                     )

            send(observer, :settings_staged)
            receive do: (:commit_settings -> {:ok, :committed})
          end)
        end)

      assert_receive :settings_staged, 2_000
      assert %{status: 200, resp_body: ^initial} = Router.call(conn, postgres)
      send(writer.pid, :commit_settings)
      assert {:ok, :committed} = Task.await(writer)
      assert %{status: 200, resp_body: ^body} = pg_response = Router.call(conn, postgres)
      assert [^updated_tag] = Plug.Conn.get_resp_header(pg_response, "etag")
      conditional = Plug.Conn.put_req_header(conn, "if-none-match", updated_tag)
      assert %{status: 304, resp_body: ""} = Router.call(conditional, sqlite)
      assert %{status: 304, resp_body: ""} = Router.call(conditional, postgres)

      assert {:ok, _} = Relay.hosted_delivery_state(relay, person, "STOPPED")
      assert {:ok, _} =
               Transaction.query("UPDATE harklight.people SET delivery_state = 'STOPPED' WHERE id = $1", [person])
      assert %{status: 403, assigns: %{client_api_error: "account_paused"}} = Router.call(conn, sqlite)
      assert %{status: 403, assigns: %{client_api_error: "account_paused"}} = Router.call(conn, postgres)

      assert {:ok, _} =
               Transaction.query("UPDATE harklight.principals SET revoked = true WHERE key = $1", [key])
      assert %{status: 401} = Router.call(conn, postgres)
      assert %{status: 401} = Router.call(Plug.Test.conn(:get, "/v2/settings"), postgres)
      assert %{status: 404} = Router.call(browser_request(:get, "/v2/inbox", cookie), postgres)
      assert %{status: 404} = Router.call(browser_request(:put, "/v2/settings/priority-levels", cookie), postgres)
    end

    @tag :tmp_dir
    test "browser alerts writes preserve FORMAT32 preconditions, replay, and committed projection", %{tmp_dir: dir} do
      alias AttentionServer.JSON
      alias AttentionServer.ClientAPI.{Idempotency, Router}
      alias AttentionServer.Repository.Postgres.{Attributes, Identities}
      alias Exqlite.Sqlite3

      {relay, _browser, backend, cookie, principal} = browser_fixture(dir)
      origin = "https://account.example.test"
      sqlite = Router.init(relay: relay, auth: {:browser, backend, origin})
      postgres = Router.init(relay: nil, repository: :postgres, auth: {:browser, Identities, origin})
      settings_conn = browser_request(:get, "/v2/settings", cookie)
      key = "00000000-0000-4000-8000-000000000099"
      assert %{status: 200} = initial = Router.call(settings_conn, sqlite)
      assert [tag] = Plug.Conn.get_resp_header(initial, "etag")

      for opts <- [sqlite, postgres] do
        assert %{status: 412, assigns: %{client_api_error: "precondition_failed"}} =
                 cookie |> alerts_request("all", ~s("stale"), key, origin) |> Router.call(opts)
        assert %{status: 400, assigns: %{client_api_error: "invalid_field"}} =
                 cookie |> alerts_request("invalid", tag, key, origin) |> Router.call(opts)
      end

      assert %{status: 200, resp_body: accepted} =
               cookie |> alerts_request("all", tag, key, origin) |> Router.call(sqlite)
      assert %{status: 200, resp_body: ^accepted} = pg =
               cookie |> alerts_request("all", tag, key, origin) |> Router.call(postgres)
      assert [] = Plug.Conn.get_resp_header(pg, "idempotent-replayed")
      assert {:ok, %{"rev" => 1, "settings" => %{"alerts" => "all"}}} = JSON.decode_line(accepted <> "\n")

      assert %{status: 409, assigns: %{client_api_error: "idempotency_conflict"}} =
               cookie |> alerts_request("important", "*", key, origin) |> Router.call(postgres)
      assert %{status: 200, resp_body: current} =
               cookie |> alerts_request("important", "*", nil, origin) |> Router.call(sqlite)
      assert %{status: 200, resp_body: ^current} =
               cookie |> alerts_request("important", "*", nil, origin) |> Router.call(postgres)

      for opts <- [sqlite, postgres] do
        assert %{status: 200, resp_body: ^current} = replayed =
                 cookie |> alerts_request("all", tag, key, origin) |> Router.call(opts)
        assert ["true"] = Plug.Conn.get_resp_header(replayed, "idempotent-replayed")
        assert %{status: 403} =
                 cookie |> alerts_request("all", "*", nil, "https://another.example.test") |> Router.call(opts)
      end

      assert {:ok, %{rows: [[2, 2]]}} =
               Transaction.query(
                 """
                 SELECT head, revision FROM harklight.stream_heads AS stream
                 JOIN harklight.stream_resource_revisions AS resource USING (person_id, origin_key)
                 WHERE person_id = $1 AND origin_key = '' AND resource_id = 'settings'
                 """,
                 [principal.person]
               )

      # A retained app receipt can reserve the same person-wide key. Its
      # original response takes precedence over a browser's recent marker.
      assert {:ok, original_body} = JSON.decode_line(accepted <> "\n")
      digest = Idempotency.digest("PUT", ["settings", "alerts"], %{"alerts" => "all", "if_match" => tag})
      {:ok, db} = Sqlite3.open(Path.join(dir, "relay.sqlite3"))
      assert :ok = Idempotency.save_retained(db, principal.person, key, digest, 200, original_body)
      :ok = Sqlite3.close(db)
      assert {:ok, _} =
               Transaction.query(
                 """
                 INSERT INTO harklight.operation_receipts
                   (person_id, origin_key, namespace, operation_key, digest, status, response,
                    created_at_ms, original_response)
                 VALUES ($1, '', 'harklight_app_receipts', $2, $3, 200, $4, 1, true)
                 """,
                 [principal.person, key, Base.decode16!(digest, case: :lower), Attributes.encode(original_body)]
               )
      for opts <- [sqlite, postgres] do
        assert %{status: 200, resp_body: ^accepted} = replayed =
                 cookie |> alerts_request("all", tag, key, origin) |> Router.call(opts)
        assert ["true"] = Plug.Conn.get_resp_header(replayed, "idempotent-replayed")
      end
    end

    @tag :tmp_dir
    test "failed final receipt update rolls back browser alerts and its stream append", %{tmp_dir: dir} do
      alias AttentionServer.ClientAPI.Router
      alias AttentionServer.Repository.Postgres.{Attributes, Identities}

      {_relay, _browser, _backend, cookie, principal} = browser_fixture(dir)
      origin = "https://account.example.test"
      postgres = Router.init(relay: nil, repository: :postgres, auth: {:browser, Identities, origin})
      settings = browser_request(:get, "/v2/settings", cookie)
      uuid = "00000000-0000-4000-8000-000000000098"
      assert %{status: 200, resp_body: initial} = Router.call(settings, postgres)

      assert {:ok, _} =
               Transaction.query(
                 """
                 ALTER TABLE harklight.operation_receipts ADD CONSTRAINT fixture_alert_receipt
                 CHECK (operation_key <> '00000000-0000-4000-8000-000000000098' OR status <> 200)
                 """,
                 []
               )

      assert %{status: 503} =
               cookie |> alerts_request("all", "*", uuid, origin) |> Router.call(postgres)
      assert %{status: 200, resp_body: ^initial} = Router.call(settings, postgres)
      assert {:ok, %{rows: []}} =
               Transaction.query("SELECT head FROM harklight.stream_heads WHERE person_id = $1", [principal.person])
      assert {:ok, %{rows: []}} =
               Transaction.query("SELECT status FROM harklight.operation_receipts WHERE operation_key = $1", [uuid])
      actor = AttentionServer.ClientAPI.Writes.browser_actor_key(principal.person, principal.session_id)
      address = Attributes.key({:row, :receipts, {principal.person, actor, uuid}})
      assert {:ok, %{rows: []}} =
               Transaction.query("SELECT body FROM harklight.compatibility_facts WHERE address = $1", [address])

      assert {:ok, _} =
               Transaction.query("ALTER TABLE harklight.operation_receipts DROP CONSTRAINT fixture_alert_receipt", [])
      assert %{status: 200} =
               cookie |> alerts_request("all", "*", uuid, origin) |> Router.call(postgres)
    end

    defp alerts_request(cookie, value, if_match, key, origin) do
      conn =
        Plug.Test.conn(:put, "/v2/settings/alerts", AttentionServer.JSON.encode!(%{"alerts" => value}))
        |> Plug.Test.put_req_cookie("__Host-harklight", cookie)
        |> Plug.Conn.put_req_header("origin", origin)
        |> Plug.Conn.put_req_header("if-match", if_match)

      if key, do: Plug.Conn.put_req_header(conn, "idempotency-key", key), else: conn
    end

    defp browser_fixture(dir) do
      alias AttentionServer.{DashboardBackend, DashboardBrowserSession, Relay, Store.AuthSchema}
      alias AttentionServer.ClientAPI.Auth
      alias Exqlite.Sqlite3

      path = Path.join(dir, "relay.sqlite3")
      opts = [
        store_path: path,
        deployment_mode: :saas,
        dashboard_origin: "https://account.example.test"
      ]

      {:ok, relay} = Relay.start_link(opts)
      {:ok, person} = Relay.hosted_account_create(relay, "browser-fixture")
      :ok = GenServer.stop(relay)

      {:ok, db} = Sqlite3.open(path)
      assert {:ok, _} = AuthSchema.initialize(db)
      :ok = Sqlite3.close(db)

      relay = start_supervised!({Relay, opts})
      browser = start_supervised!({DashboardBrowserSession, relay: relay})
      backend = {DashboardBackend, %DashboardBackend{relay: relay, sessions: browser}}
      assert {:ok, grant} = Relay.hosted_browser_grant(relay, person, "fixture-browser", "google")
      assert {:ok, %{cookie: cookie}} = Relay.redeem_hosted_browser_grant(relay, grant, "fixture-browser")
      conn = browser_request(:get, "/v2/me", cookie)

      assert {:ok, %{person: ^person, key: key, owner_revision: revision, session_id: hash} = principal} =
               Auth.web(conn, backend, relay)
      assert {:ok, _} = Transaction.query("INSERT INTO harklight.people (id) VALUES ($1)", [person])
      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.principals (key, person_id, kind, revision) VALUES ($1, $2, 'owner', $3)",
                 [key, person, revision]
               )
      assert {:ok, _} =
               Transaction.query(
                 """
                 INSERT INTO harklight.auth_sessions
                   (token_hash, person_id, principal_key, kind, principal_revision)
                 VALUES ($1, $2, $3, 'browser', $4)
                 """,
                 [hash, person, key, revision]
               )

      {relay, browser, backend, cookie, principal}
    end

    defp browser_request(method, path, cookie) do
      method
      |> Plug.Test.conn(path)
      |> Plug.Test.put_req_cookie("__Host-harklight", cookie)
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

    test "append waits for retention to release the stream head and keeps the imported high" do
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

      head_blocker =
        Task.async(fn ->
          Transaction.run([scope], fn _ ->
            send(observer, :head_locked)
            receive do: (:release_head -> {:ok, :released})
          end)
        end)

      assert_receive :head_locked, 2_000
      prune = Task.async(fn -> Streams.prune(scope, 50) end)
      await_lock_wait("SELECT head FROM harklight.stream_heads", 100)
      send(head_blocker.pid, :release_head)
      assert {:ok, :released} = Task.await(head_blocker)
      await_lock_wait("DELETE FROM harklight.stream_changes", 100)

      new_change = %{change("new") | created_at_ms: 100}

      append =
        Task.async(fn ->
          Transaction.run([scope], &Transaction.append(&1, scope, [new_change]))
        end)

      await_lock_wait("SELECT head FROM harklight.stream_heads", 100)

      assert {:ok, [%{rev: 1}], false, 1} = Streams.person_page(person, 0, 10)
      assert {:ok, [], false, 8} = Streams.person_page(person, 8, 10)

      send(blocker.pid, :release_row)
      assert {:ok, :released} = Task.await(blocker)
      assert {:ok, 1} = Task.await(prune)
      assert {:ok, 10} = Task.await(append)
      assert {:ok, 1} = Streams.prune(scope, 50)
      assert {:error, :cursor_expired} = Streams.person_page(person, 0, 10)
      assert {:ok, [%{rev: 10}], false, 10} = Streams.person_page(person, 8, 10)
      assert {:error, :cursor_expired} = Streams.person_page(person, 11, 10)
    end

    test "person item removals replace the prior feed row under the same revision key" do
      alias AttentionServer.Repository.Postgres.Streams

      person = "synthetic-item-removed"
      scope = {person, ""}
      id = AttentionServer.ChangeLog.item_resource({person, "app:fixture", "removed"})
      original = change(id)
      removed = %{original | kind: "item_removed", created_at_ms: 2}
      assert {:ok, _} = Transaction.query("INSERT INTO harklight.people (id) VALUES ($1)", [person])
      assert {:ok, 1} = Transaction.run([scope], &Transaction.append(&1, scope, [original]))
      assert {:ok, 2} = Transaction.run([scope], &Transaction.append(&1, scope, [removed]))
      assert {:ok, [%{rev: 2, kind: "item_removed", resource_id: ^id}], false, 2} =
               Streams.person_page(person, 0, 10)
      assert {:ok, %{rows: [["item", 2]]}} =
               Transaction.query(
                 "SELECT resource_kind, revision FROM harklight.stream_resource_revisions WHERE person_id = $1",
                 [person]
               )
    end

    @tag :tmp_dir
    test "PostgreSQL certificate authority follows FORMAT32 migration and sender rules", %{tmp_dir: dir} do
      alias AttentionServer.{DeviceIdentity, Relay, TLSFixtures}
      alias AttentionServer.ClientAPI.Auth
      alias AttentionServer.Store.{AuthSchema, RecordTable}
      alias Exqlite.Sqlite3

      certs = TLSFixtures.create!()
      on_exit(fn -> File.rm_rf!(certs) end)
      person = "synthetic-cert-account"
      agent_id = "00000000-0000-4000-8000-000000000001"
      sender_pattern = "app:synthetic"
      path = Path.join(dir, "certificates.sqlite3")
      opts = [store_path: path, agent_fixture_mode: true]
      relay = start_supervised!({Relay, opts})
      assert :ok = Relay.setup_person(relay, person)
      assert {:ok, _} = Transaction.query("INSERT INTO harklight.people (id) VALUES ($1)", [person])

      credentials =
        for {name, kind} <- [{"a1", :device}, {"a2", :device}, {"b1", :device}, {"unknown", :agent}],
            into: %{} do
          der = TLSFixtures.certificate_der!(certs, name)
          assert {:ok, key} = DeviceIdentity.from_certificate(der)

          result =
            case kind do
              :device -> Relay.setup_device(relay, person, key)
              :agent -> Relay.setup_agent_fixture(relay, person, agent_id, key, [sender_pattern])
            end

          assert :ok = result

          assert {:ok, _} =
                   Transaction.query(
                     "INSERT INTO harklight.principals (key, person_id, kind, revision) VALUES ($1, $2, $3, 1)",
                     [key, person, to_string(kind)]
                   )

          conn =
            Plug.Test.conn(:get, "/v2/me")
            |> Plug.Test.put_peer_data(%{address: {127, 0, 0, 1}, port: 1, ssl_cert: der})

          {name, {key, conn}}
        end

      {legacy, _} = credentials["a1"]
      {pending, _} = credentials["a2"]
      {sender, _} = credentials["b1"]
      {agent, _} = credentials["unknown"]

      check = fn relay, mode, expected ->
        for {name, kind} <- expected do
          {key, conn} = Map.fetch!(credentials, name)
          actual = Auth.device(conn, relay)

          case kind do
            :denied -> assert {:error, :unauthorized} = actual
            kind -> assert {:ok, %{key: ^key, person: ^person, kind: ^kind}} = actual
          end

          assert Auth.device(conn, {:postgres, mode}) == actual
        end
      end

      admitted = [{"a1", :device}, {"a2", :device}, {"b1", :device}, {"unknown", :agent}]
      check.(relay, :self_hosted, admitted)
      stop_supervised!(Relay)

      {:ok, db} = Sqlite3.open(path)
      assert {:ok, %{minimum_generation: generation}} = AuthSchema.initialize(db)
      put_certificate_fact("harklight_auth_schema", 1, [1, generation, 0])

      for {key, phase} <- [{legacy, "legacy"}, {pending, "pending"}] do
        row = [key, person, phase]
        assert :ok = RecordTable.run(db, "INSERT INTO harklight_migration_roster VALUES (?, ?, ?)", row)
        put_certificate_fact("harklight_migration_roster", key, row)
      end
      put_certificate_session(db, person, pending, :binary.copy(<<41>>, 32), true)

      :ok = Sqlite3.close(db)
      relay = start_supervised!({Relay, opts})
      check.(relay, :self_hosted, [{"b1", :denied}])
      assert :ok = Relay.setup_sender(relay, sender, sender_pattern)
      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.sender_grants VALUES ($1, $2)", [
                 sender,
                 sender_pattern
               ])
      check.(relay, :self_hosted, admitted)
      stop_supervised!(Relay)

      relay = start_supervised!({Relay, Keyword.put(opts, :deployment_mode, :saas)})
      check.(relay, :saas, [{"a1", :denied}, {"a2", :denied}, {"b1", :device}, {"unknown", :agent}])
      stop_supervised!(Relay)

      {:ok, db} = Sqlite3.open(path)
      put_certificate_session(db, person, sender, :binary.copy(<<42>>, 32), false)
      assert :ok = RecordTable.run(db, "UPDATE harklight_migration_roster SET phase = 'activated' WHERE device_id = ?", [pending])
      put_certificate_fact("harklight_migration_roster", pending, [pending, person, "activated"])
      :ok = Sqlite3.close(db)

      relay = start_supervised!({Relay, opts})
      check.(relay, :self_hosted, [{"a1", :device}, {"a2", :denied}, {"b1", :denied}, {"unknown", :agent}])
      stop_supervised!(Relay)

      {:ok, db} = Sqlite3.open(path)
      assert :ok = RecordTable.run(db, "UPDATE harklight_auth_schema SET device_certificates_disabled = 1", [])
      put_certificate_fact("harklight_auth_schema", 1, [1, generation, 1])
      :ok = Sqlite3.close(db)

      relay = start_supervised!({Relay, opts})
      check.(relay, :self_hosted, [{"a1", :denied}, {"unknown", :agent}])
      assert :ok = Relay.revoke_agent_fixture(relay, agent)
      assert {:ok, _} =
               Transaction.query("UPDATE harklight.principals SET revoked = true WHERE key = $1", [agent])
      check.(relay, :self_hosted, [{"unknown", :denied}])
    end

    defp put_certificate_session(db, person, device, hash, pending?) do
      alias AttentionServer.NativeSignIn
      alias AttentionServer.Repository.Postgres.Attributes

      assert :ok =
               NativeSignIn.add_session(db, Base.encode16(hash, case: :lower), device, person, "fixture-proof", 10, pending?)
      assert {:ok, _} =
               Transaction.query(
                 """
                 INSERT INTO harklight.auth_sessions
                   (token_hash, person_id, principal_key, kind, principal_revision, attributes)
                 VALUES ($1, $2, $3, 'native', 1, $4)
                 """,
                 [hash, person, device, Attributes.encode(%{pending: pending?})]
               )
    end

    defp put_certificate_fact(family, key, row) do
      alias AttentionServer.Repository.Postgres.Attributes

      assert {:ok, _} =
               Transaction.query(
                 """
                 INSERT INTO harklight.compatibility_facts (family, address, body) VALUES ($1, $2, $3)
                 ON CONFLICT (family, address) DO UPDATE SET body = excluded.body
                 """,
                 [family, Attributes.key(key), Attributes.encode(row)]
               )
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

      await_lock_wait("harklight.stream_heads", 100)

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

    defp await_lock_wait(statement, 0), do: flunk("request did not wait on #{statement}")

    defp await_lock_wait(statement, remaining) do
      assert {:ok, %{rows: [[waiting?]]}} =
               Transaction.query(
                 """
                 SELECT EXISTS (
                   SELECT 1 FROM pg_stat_activity
                   WHERE wait_event_type = 'Lock'
                     AND query LIKE $1
                 )
                 """,
                 ["%#{statement}%"]
               )

      unless waiting? do
        Process.sleep(10)
        await_lock_wait(statement, remaining - 1)
      end
    end

    @tag :tmp_dir
    test "FORMAT32 native bearers retain pending state and device authority in PostgreSQL", %{tmp_dir: dir} do
      alias AttentionServer.{BrowserAuth, DeviceProof, JSON, NativeSignIn, Relay, Store.AuthSchema}
      alias AttentionServer.ClientAPI.{Auth, Router, Writes}
      alias AttentionServer.Repository.Postgres.Attributes
      alias Exqlite.Sqlite3

      person = "synthetic-native-account"
      active = "synthetic-active-device"
      pending = "synthetic-pending-device"
      path = Path.join(dir, "native.sqlite3")
      opts = [store_path: path, push_enabled: true]
      relay = start_supervised!({Relay, opts})
      assert :ok = Relay.setup_person(relay, person)
      assert :ok = Relay.setup_device(relay, person, active)
      assert :ok = Relay.setup_device(relay, person, pending)
      stop_supervised!(Relay)

      {:ok, db} = Sqlite3.open(path)
      assert {:ok, _} = AuthSchema.initialize(db)
      assert {:ok, _} = Transaction.query("INSERT INTO harklight.people (id) VALUES ($1)", [person])

      requests =
        for {device, pending?} <- [{active, false}, {pending, true}], into: %{} do
          bearer = BrowserAuth.new_secret()
          hash = DeviceProof.digest(bearer)
          assert :ok = NativeSignIn.add_session(db, hash, device, person, "synthetic-proof-key", 10, pending?)
          assert {:ok, _} =
                   Transaction.query(
                     "INSERT INTO harklight.principals (key, person_id, kind, revision) VALUES ($1, $2, 'device', 2)",
                     [device, person]
                   )
          assert {:ok, _} =
                   Transaction.query(
                     """
                     INSERT INTO harklight.auth_sessions
                       (token_hash, person_id, principal_key, kind, principal_revision, attributes)
                     VALUES ($1, $2, $3, 'native', 1, $4)
                     """,
                     [Base.decode16!(hash, case: :lower), person, device, Attributes.encode(%{pending: pending?})]
                   )

          {device, {"Bearer " <> bearer, hash, pending?}}
        end

      :ok = Sqlite3.close(db)
      relay = start_supervised!({Relay, opts})
      router = Router.init(relay: relay, auth: {:browser, nil, nil})
      postgres = Router.init(relay: nil, repository: :postgres, auth: {:browser, nil, nil})

      for {device, {header, hash, pending?}} <- requests do
        conn = Plug.Test.conn(:get, "/v2/me") |> Plug.Conn.put_req_header("authorization", header)
        assert {:ok, %{person: ^person, key: ^device, session_id: ^hash, pending: ^pending?} = principal} =
                 Auth.web(conn, nil, relay)
        assert {:ok, ^principal} = Auth.web(conn, nil, :postgres)
        assert %{status: 200} = Router.call(conn, router)

        settings = Plug.Test.conn(:get, "/v2/settings") |> Plug.Conn.put_req_header("authorization", header)
        assert %{status: 200, resp_body: body} = response = Router.call(settings, router)
        assert %{status: 200, resp_body: ^body} = pg_response = Router.call(settings, postgres)
        assert Plug.Conn.get_resp_header(response, "etag") == Plug.Conn.get_resp_header(pg_response, "etag")

        both = Plug.Test.put_req_cookie(conn, "__Host-harklight", "fixture-cookie")
        assert {:error, :unauthorized} = Auth.web(both, nil, relay)
        assert {:error, :unauthorized} = Auth.web(both, nil, :postgres)
      end

      {pending_header, _, _} = Map.fetch!(requests, pending)
      pending_write =
        Plug.Test.conn(:put, "/v2/settings/alerts")
        |> Plug.Conn.put_req_header("authorization", pending_header)
      assert %{status: 403, assigns: %{client_api_error: "forbidden"}} = Router.call(pending_write, router)

      {active_header, old_hash, _} = Map.fetch!(requests, active)
      old = Plug.Test.conn(:get, "/v2/me") |> Plug.Conn.put_req_header("authorization", active_header)
      assert {:ok, principal} = Auth.web(old, nil, relay)
      assert {:ok, 200, _, false} =
               Writes.push(relay, principal, String.duplicate("a", 64), "production", 1)

      settings = Plug.Test.conn(:get, "/v2/settings") |> Plug.Conn.put_req_header("authorization", active_header)
      assert %{status: 200, resp_body: body} = response = Router.call(settings, router)
      assert {:ok, %{"rev" => head, "settings" => %{"rev" => rev, "push" => %{"revision" => 1}}}} =
               JSON.decode_line(body <> "\n")
      binding = Map.fetch!(:sys.get_state(relay).store.engine.push_bindings, {person, active})
      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.compatibility_facts (family, address, body) VALUES ('push_bindings', $1, $2)",
                 [Attributes.key({:row, :push_bindings, {person, active}}), Attributes.encode(binding)]
               )
      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.stream_heads (person_id, origin_key, head) VALUES ($1, '', $2)",
                 [person, head]
               )
      assert {:ok, _} =
               Transaction.query(
                 """
                 INSERT INTO harklight.stream_resource_revisions
                   (person_id, origin_key, resource_kind, resource_id, revision)
                 VALUES ($1, '', 'settings', 'settings', $2)
                 """,
                 [person, rev]
               )
      assert %{status: 200, resp_body: ^body} = pg_response = Router.call(settings, postgres)
      assert Plug.Conn.get_resp_header(response, "etag") == Plug.Conn.get_resp_header(pg_response, "etag")

      assert :ok = Relay.revoke_device(relay, active)
      assert {:ok, _} =
               Transaction.query("UPDATE harklight.principals SET revoked = true WHERE key = $1", [active])
      assert {:error, :unauthorized} = Auth.web(old, nil, relay)
      assert {:error, :unauthorized} = Auth.web(old, nil, :postgres)
      assert %{status: 401} = Router.call(settings, postgres)

      assert {:ok, _} =
               Transaction.query(
                 "UPDATE harklight.auth_sessions SET kind = 'browser' WHERE token_hash = $1",
                 [Base.decode16!(old_hash, case: :lower)]
               )
      assert {:ok, _} =
               Transaction.query("UPDATE harklight.principals SET revoked = false WHERE key = $1", [active])
      assert {:error, :unauthorized} = Auth.web(old, nil, :postgres)
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
      await_lock_wait("harklight.operation_receipts", 100)
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
