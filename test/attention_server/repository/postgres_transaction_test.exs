if System.get_env("HARKLIGHT_POSTGRES_TEST_URL") do
  defmodule AttentionServer.Repository.PostgresTransactionTest do
    use ExUnit.Case, async: false

    alias AttentionServer.Repository.Postgres.{Attributes, Jobs, Operations, Pool, Repo, Transaction}

    defmodule LargeTextTransportFixture do
      alias AttentionServer.ClientAPI.{Auth, Errors, Projection}
      alias AttentionServer.Repository.RecipientItems
      alias AttentionServer.Repository.Postgres.{Identities, Settings, Transaction}

      def init(opts), do: opts

      def call(%Plug.Conn{path_info: ["v2", "items", item]} = conn, observer) do
        result =
          Transaction.snapshot(fn ->
            with {:ok, %{kind: :browser, person: person}} <- Auth.web(conn, Identities, :postgres),
                 :ok <- Settings.check_active(person),
                 {:ok, _, record} <- RecipientItems.fetch(:postgres, person, item) do
              {:ok, %{"message" => record.message}}
            end
          end)

        response = respond(conn, result)
        send(observer, :large_text_response_finished)
        response
      end

      defp respond(conn, result) do
        case result do
          {:ok, body} ->
            case Projection.capture(fn -> Errors.send_streamed_json(conn, 200, body) end) do
              {:error, %Projection{} = fault} -> Errors.projection_error(conn, fault)
              response -> response
            end

          {:error, reason} ->
            token =
              case Errors.token_for_reason(reason) do
                {:ok, token} -> token
                :error -> "storage_unavailable"
              end

            Errors.send_error(conn, token)
        end
      end
    end

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

    test "shared large text verifies actual multi-chunk bytes before bounded reads" do
      alias AttentionServer.Repository.Postgres.LargeText

      chunk = LargeText.chunk_bytes()
      parts = [:binary.copy("a", chunk), :binary.copy("b", chunk), "final UTF-8 text ☃"]
      [first, middle, last] = parts
      hash = :crypto.hash(:sha256, parts) |> Base.encode16(case: :lower)
      size = 2 * chunk + byte_size(last)
      assert size > 2 * 1024 * 1024
      assert {:ok, ref} = LargeText.reference(hash, size)
      assert {:ok, wrong_size} = LargeText.reference(hash, size + 1)

      assert :ok = LargeText.stage(ref)
      assert {:error, :large_text_conflict} = LargeText.stage(wrong_size)
      assert :ok = LargeText.put_chunk(ref, 0, first)
      assert :ok = LargeText.put_chunk(ref, 0, first)
      assert {:error, :large_text_conflict} = LargeText.put_chunk(ref, 0, middle)
      assert {:error, :invalid_large_text_chunk} = LargeText.put_chunk(ref, 1, last)
      assert {:error, :not_found} = LargeText.read_chunk(ref, 0)
      assert {:error, :incomplete_large_text} = LargeText.complete(ref)
      assert :ok = LargeText.put_chunk(ref, 1, middle)
      assert :ok = LargeText.put_chunk(ref, 2, last)
      assert :ok = LargeText.complete(ref)
      assert :ok = LargeText.complete(ref)
      assert :ok = LargeText.stage(ref)
      assert :ok = LargeText.put_chunk(ref, 2, last)

      observer = self()
      query = fn query, [_, ^size, number, true] = params ->
        send(observer, {:chunk_query, number})
        Transaction.query(query, params)
      end

      assert {:ok, ^middle} = LargeText.read_chunk(ref, 1, query)
      assert_receive {:chunk_query, 1}
      refute_received {:chunk_query, _}
      assert {:ok, ^last} = LargeText.read_chunk(ref, 2)
      assert {:error, :not_found} = LargeText.read_chunk(ref, 3)
      assert {:error, :not_found} = LargeText.read_chunk(wrong_size, 0)
    end

    test "matching length with the wrong digest remains unpublished" do
      alias AttentionServer.Repository.Postgres.LargeText

      hash = :crypto.hash(:sha256, "expected") |> Base.encode16(case: :lower)
      assert {:ok, ref} = LargeText.reference(hash, 8)
      assert :ok = LargeText.stage(ref)
      assert :ok = LargeText.put_chunk(ref, 0, "replaced")
      assert {:error, :large_text_checksum_mismatch} = LargeText.complete(ref)
      assert {:error, :not_found} = LargeText.read_chunk(ref, 0)
    end

    @tag :tmp_dir
    test "authorized ordinary history streams PostgreSQL text with FORMAT32 wire bytes", %{tmp_dir: dir} do
      alias AttentionServer.{JSON, LargeData}
      alias AttentionServer.Repository.RecipientItems
      alias AttentionServer.Repository.Postgres.LargeText

      size = LargeText.chunk_bytes()
      # Split multibyte UTF-8 across both database chunk boundaries.
      parts = [
        :binary.copy("a", size - 1) <> <<0xF0>>,
        <<0x9F, 0x92, 0xA1>> <> :binary.copy("b", size - 4) <> <<0xC3>>,
        <<0xA9, 0x22, 0x5C, 0x0A, 0x09, 0x01>>
      ]

      digest = :crypto.hash(:sha256, parts)
      length = IO.iodata_length(parts)
      assert length > 2 * 1024 * 1024
      hex = Base.encode16(digest, case: :lower)
      assert {:ok, reference} = LargeText.reference(hex, length)
      assert :ok = LargeText.stage(reference)

      for {part, index} <- Enum.with_index(parts) do
        assert :ok = LargeText.put_chunk(reference, index, part)
      end

      person = "synthetic-large-text-person"
      origin = "synthetic-large-text-origin"
      body = %{"title" => "Historical text", "body" => reference}
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
                   (id, person_id, origin_key, sender_id, message_id, revision, accepted_at_ms, accepted_position)
                 VALUES ('large-text-item', $1, $2, 'app:fixture', 'historical', 1, 20, 9)
                 """,
                 [person, origin]
               )

      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.message_revisions VALUES ('large-text-item', 1, 20, $1)",
                 [Attributes.encode(body)]
               )

      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.message_recipients (message_id, person_id, item_id) VALUES ('large-text-item', $1, 'i9')",
                 [person]
               )

      assert {:error, :not_found} = RecipientItems.fetch(:postgres, "another-person", "i9")
      assert {:ok, {^person, "app:fixture", "historical"}, %{message: ^body}} =
               RecipientItems.fetch(:postgres, person, "i9")

      wire = %{"message" => body}
      assert_raise ArgumentError, "large text unavailable", fn ->
        wire |> JSON.encode_stream() |> Stream.run()
      end

      assert :ok = LargeText.complete(reference)
      path = Path.join(dir, "format32-body")
      File.write!(path, parts)
      source = %LargeData.Ref{id: hex, sha256: hex, size: length, path: path}
      legacy_wire = %{"message" => %{body | "body" => source}}
      assert streamed_signature(wire) == streamed_signature(legacy_wire)

      # Synthetic out-of-band corruption keeps the length and publication
      # marker, but must abort encoding before it closes the JSON string.
      assert {:ok, _} =
               Transaction.query(
                 "UPDATE harklight.large_text_chunks SET bytes = $2 WHERE sha256 = $1 AND chunk_number = 2",
                 [digest, <<0xA9, 0x22, 0x5C, 0x0A, 0x09, 0x02>>]
               )

      assert_raise ArgumentError, "large text checksum mismatch", fn ->
        wire |> JSON.encode_stream() |> Stream.run()
      end
    end

    defp streamed_signature(wire) do
      {hash, length} =
        wire
        |> AttentionServer.JSON.encode_stream()
        |> Enum.reduce({:crypto.hash_init(:sha256), 0}, fn segment, {hash, length} ->
          assert byte_size(segment) <= 6 * 64 * 1024
          {:crypto.hash_update(hash, segment), length + byte_size(segment)}
        end)

      {:crypto.hash_final(hash), length}
    end

    @tag :tmp_dir
    test "isolated HTTP sends two retained messages sharing one FORMAT32 large value", %{tmp_dir: dir} do
      alias AttentionServer.{BrowserAuth, JSON, LargeData, Relay}
      alias AttentionServer.ClientAPI.Reads
      alias AttentionServer.Repository.Postgres.LargeText

      {relay, _browser, _backend, cookie, principal} = browser_fixture(dir)
      person = principal.person
      origin = "synthetic-transport-origin"
      assert :ok = Relay.setup_device(relay, person, origin)
      assert :ok = Relay.setup_sender(relay, origin, "app:fixture")
      chunk = LargeText.chunk_bytes()
      text = :binary.copy("x", chunk - 1) <> "💡" <> :binary.copy("y", chunk) <> "\"\\\n"
      assert byte_size(text) > 2 * 1024 * 1024

      source =
        for number <- [1, 2] do
          id = "retained-text-#{number}"
          frame = %{
            "v" => 1,
            "type" => "forward",
            "op_id" => "00000000-0000-4000-8000-00000000081#{number}",
            "session_handle" => "10000000-0000-4000-8000-000000000081",
            "message" => %{
              "sender_id" => "app:fixture",
              "message_id" => id,
              "created_at" => 1,
              "title" => "Historical #{number}",
              "body" => text
            }
          }

          assert {:ok, reply} = Relay.request_line(relay, origin, JSON.encode!(frame) <> "\n")
          assert {:ok, %{"type" => "ok", "seq" => ^number}} = JSON.decode_line(reply)
          item = "i#{number}"
          assert {:ok, %{"item" => %{"message" => %{"body" => %LargeData.Ref{}} = message}}} =
                   Reads.item(relay, principal, item)
          {id, item, number, message}
        end

      [{_, _, _, %{"body" => source_ref}}, {_, _, _, %{"body" => shared_ref}}] = source
      assert source_ref == shared_ref
      assert :ok = LargeData.verify(source_ref)
      assert {:ok, ref} = LargeText.reference(source_ref.sha256, source_ref.size)
      assert :ok = LargeText.stage(ref)
      for {bytes, ordinal} <- source_ref.path |> File.stream!([], chunk) |> Enum.with_index() do
        assert :ok = LargeText.put_chunk(ref, ordinal, bytes)
      end
      assert :ok = LargeText.complete(ref)

      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.principals (key, person_id, kind, revision) VALUES ($1, $2, 'device', 1)",
                 [origin, person]
               )

      for {id, item, number, message} <- source do
        assert {:ok, _} =
                 Transaction.query(
                   """
                   INSERT INTO harklight.messages
                     (id, person_id, origin_key, sender_id, message_id, revision, accepted_at_ms, accepted_position)
                   VALUES ($1, $2, $3, 'app:fixture', $1, 1, 1, $4)
                   """,
                   [id, person, origin, number]
                 )
        assert {:ok, _} =
                 Transaction.query(
                   "INSERT INTO harklight.message_revisions VALUES ($1, 1, 1, $2)",
                   [id, Attributes.encode(%{message | "body" => ref})]
                 )
        assert {:ok, _} =
                 Transaction.query(
                   "INSERT INTO harklight.message_recipients (message_id, person_id, item_id) VALUES ($1, $2, $3)",
                   [id, person, item]
                 )
      end

      other_cookie = "synthetic-other-valid-session"
      assert {:ok, _} = Transaction.query("INSERT INTO harklight.people (id) VALUES ('transport-other')", [])
      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.principals (key, person_id, kind, revision) VALUES ('transport-other-owner', 'transport-other', 'owner', 1)",
                 []
               )
      assert {:ok, _} =
               Transaction.query(
                 """
                 INSERT INTO harklight.auth_sessions
                   (token_hash, person_id, principal_key, kind, principal_revision)
                 VALUES ($1, 'transport-other', 'transport-other-owner', 'browser', 1)
                 """,
                 [BrowserAuth.hash(other_cookie)]
               )

      assert {:ok, _} = Application.ensure_all_started(:inets)
      listener = start_supervised!({Bandit, plug: {LargeTextTransportFixture, self()}, ip: {127, 0, 0, 1}, port: 0})
      assert {:ok, {_, port}} = ThousandIsland.listener_info(listener)
      spools = Path.join(System.tmp_dir!(), "harklight-json-response-*")
      existing_spools = Path.wildcard(spools)
      for {_, item, _, message} <- source do
        assert {200, headers, body} = request_retained_text(port, item, cookie)
        assert {~c"content-type", ~c"application/json; charset=utf-8"} in headers
        assert :crypto.hash(:sha256, body) ==
                 :crypto.hash(:sha256, JSON.encode!(%{"message" => message}))
      end
      assert {404, _, denied} = request_retained_text(port, "i1", other_cookie)
      assert {:ok, %{"error" => "not_found"}} = JSON.decode_line(denied <> "\n")

      digest = Base.decode16!(ref.sha256, case: :lower)
      assert {:ok, _} =
               Transaction.query("DELETE FROM harklight.large_text_chunks WHERE sha256 = $1 AND chunk_number = 2", [digest])
      assert {500, _, error} = request_retained_text(port, "i1", cookie)
      assert {:ok, %{"error" => "internal_error"}} = JSON.decode_line(error <> "\n")
      assert Path.wildcard(spools) == existing_spools
    end

    defp request_retained_text(port, item, cookie) do
      url = String.to_charlist("http://127.0.0.1:#{port}/v2/items/#{item}")
      headers = [{~c"cookie", String.to_charlist("__Host-harklight=" <> cookie)}]
      assert {:ok, {{_, status, _}, headers, body}} =
               :httpc.request(:get, {url, headers}, [timeout: 15_000], [body_format: :binary])
      assert_receive :large_text_response_finished, 2_000
      {status, headers, body}
    end

    @tag :tmp_dir
    test "ordinary PostgreSQL revision preserves historical body, file refs, head and ETag", %{tmp_dir: dir} do
      alias AttentionServer.{JSON, LargeData, Relay}
      alias AttentionServer.ClientAPI.{Context, Reads, Resources, Router}
      alias AttentionServer.Repository.RecipientItems
      alias AttentionServer.Repository.Postgres.LargeText

      {relay, _browser, backend, cookie, principal} = browser_fixture(dir)
      %{person: person, key: owner} = principal
      origin = "synthetic-revision-origin"
      assert :ok = Relay.setup_device(relay, person, origin)
      assert :ok = Relay.setup_sender(relay, origin, "app:fixture")

      long = :binary.copy("x", 2 * 1024 * 1024 + 1) <> " 💡"
      message = %{"sender_id" => "app:fixture", "message_id" => "history", "created_at" => 1, "title" => "First", "body" => long}
      first = %{
        "type" => "forward",
        "op_id" => "00000000-0000-4000-8000-000000000091",
        "session_handle" => "10000000-0000-4000-8000-000000000091",
        "message" => message,
        "files" => [%{"name" => "history.bin", "data_base64" => Base.encode64(<<0, 255>>)}]
      }
      later = %{
        "type" => "conversation_revise",
        "op_id" => "00000000-0000-4000-8000-000000000092",
        "sender_id" => "app:fixture",
        "message_id" => "history",
        "base_revision" => 1,
        "message" => %{message | "title" => "Second", "body" => "Current"}
      }

      for frame <- [first, later] do
        assert {:ok, reply} = Relay.request_line(relay, origin, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")
        assert {:ok, %{"type" => "ok"}} = JSON.decode_line(reply)
      end

      path = "/v2/items/i1/revisions/1"
      opts = Router.init(relay: relay, auth: {:browser, backend, nil})
      request = browser_request(:get, path, cookie)
      assert %{status: 200, resp_body: expected} = original = Router.call(request, opts)
      assert [etag] = Plug.Conn.get_resp_header(original, "etag")
      assert {:ok, %{"rev" => head, "revision" => %{"files" => files}}} = JSON.decode_line(expected <> "\n")
      assert [%{"file_id" => "fi1.1.0", "name" => "history.bin", "size" => 2}] = files

      engine = :sys.get_state(relay).store.engine
      key = {person, "app:fixture", "history"}
      record = Map.fetch!(engine.messages, key)
      assert record.revision == 2
      assert %LargeData.Ref{} = ref = record.revisions[1]["body"]
      assert Resources.revision_file_refs(1, 1, record.files_by_revision[1], engine.uploads) == files
      assert {:ok, pg_ref} = LargeText.reference(ref.sha256, ref.size)
      assert :ok = LargeText.stage(pg_ref)
      for {bytes, ordinal} <- ref.path |> File.stream!([], LargeText.chunk_bytes()) |> Enum.with_index() do
        assert :ok = LargeText.put_chunk(pg_ref, ordinal, bytes)
      end
      assert :ok = LargeText.complete(pg_ref)

      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.principals (key, person_id, kind, revision) VALUES ($1, $2, 'device', 1)",
                 [origin, person]
               )
      assert {:ok, _} =
               Transaction.query(
                 """
                 INSERT INTO harklight.messages
                   (id, person_id, origin_key, sender_id, message_id, revision, accepted_at_ms, accepted_position)
                 VALUES ('revision-item', $1, $2, 'app:fixture', 'history', 2, 1, 1)
                 """,
                 [person, origin]
               )

      for {revision, body} <- [{1, %{record.revisions[1] | "body" => pg_ref}}, {2, record.revisions[2]}] do
        assert {:ok, _} =
                 Transaction.query(
                   "INSERT INTO harklight.message_revisions VALUES ('revision-item', $1, 1, $2)",
                   [revision, Attributes.encode(body)]
                 )
      end

      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.message_recipients (message_id, person_id, item_id, attributes) VALUES ('revision-item', $1, 'i1', $2)",
                 [person, Attributes.encode(%{visible: true})]
               )
      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.stream_heads (person_id, origin_key, head) VALUES ($1, '', $2)", [person, head])

      read = fn conn, n ->
        Reads.get_postgres_revision(conn, %Context{
          principal: nil, relay: nil, request_id: nil, params: %{item_id: "i1", n: n}
        })
      end

      assert %{status: 503} = read.(request, "1")
      address = Attributes.key({person, "i1", 1})
      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.compatibility_facts VALUES ('ordinary_revision_files', $1, $2)", [
                 address, Attributes.encode(files)
               ])

      assert %{status: 200, resp_body: actual} = response = read.(request, "1")
      assert :crypto.hash(:sha256, actual) == :crypto.hash(:sha256, expected)
      assert [^etag] = Plug.Conn.get_resp_header(response, "etag")
      assert ["private, no-cache"] = Plug.Conn.get_resp_header(response, "cache-control")
      conditional = Plug.Conn.put_req_header(request, "if-none-match", "W/" <> etag)
      assert %{status: 304, resp_body: ""} = read.(conditional, "1")
      for n <- ["x", "0", "3", "9223372036854775808"], do: assert(%{status: 404} = read.(request, n))
      assert {:error, :not_found} = RecipientItems.revision(:postgres, "other-person", "i1", 1)
      assert %{status: 503} = read.(request, "2")

      assert {:ok, _} =
               Transaction.query("UPDATE harklight.message_recipients SET attributes = $2 WHERE person_id = $1", [
                 person, Attributes.encode(%{visible: false})
               ])
      assert %{status: 404} = read.(conditional, "1")
      assert {:ok, _} = Transaction.query("UPDATE harklight.principals SET revoked = true WHERE key = $1", [owner])
      assert %{status: 401} = read.(conditional, "1")
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
      assert %{status: 401} = Router.call(browser_request(:put, "/v2/settings/priority-levels", cookie), postgres)
      assert %{status: 404} = Router.call(browser_request(:put, "/v2/devices/me/push", cookie), postgres)
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
      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.compatibility_facts (family, address, body) VALUES ('harklight_app_receipts', $1, $2)",
                 [
                   Attributes.key({principal.person, key}),
                   Attributes.encode([principal.person, key, digest, 200, accepted, 1, 1])
                 ]
               )
      for opts <- [sqlite, postgres] do
        assert %{status: 200, resp_body: ^accepted} = replayed =
                 cookie |> alerts_request("all", tag, key, origin) |> Router.call(opts)
        assert ["true"] = Plug.Conn.get_resp_header(replayed, "idempotent-replayed")
      end
    end

    @tag :tmp_dir
    test "staged browser settings feed matches FORMAT32 cursor and wire behavior", %{tmp_dir: dir} do
      alias AttentionServer.{ChangeLog, JSON}
      alias AttentionServer.ClientAPI.{Context, Feed, Router}
      alias AttentionServer.Repository.Postgres.{Identities, Streams}

      {relay, _, backend, cookie, principal} = browser_fixture(dir)
      origin = "https://account.example.test"
      sqlite = Router.init(relay: relay, auth: {:browser, backend, origin})
      postgres = Router.init(relay: nil, repository: :postgres, auth: {:browser, Identities, origin})
      ctx = %Context{relay: nil, request_id: nil, principal: nil}
      read = &Feed.postgres_changes(&1, ctx)
      scope = {principal.person, ""}
      commit = alerts_request(cookie, "all", "*", nil, origin)
      assert %{status: 200, resp_body: body} = Router.call(commit, sqlite)
      assert %{status: 200, resp_body: ^body} = Router.call(commit, postgres)
      assert {:ok, %{"rev" => head}} = JSON.decode_line(body <> "\n")
      assert head > 0

      store = :sys.get_state(relay).store
      assert :ok = ChangeLog.ranks_available(store, principal.person)
      assert {:ok, _} = Transaction.query(
        "INSERT INTO harklight.compatibility_facts VALUES ('client_rank_state', $1, $2)",
        [Attributes.key(principal.person), Attributes.encode([principal.person])]
      )

      batch = browser_request(:get, "/v2/changes?after=#{head + 1}&limit=1", cookie)
      fetch = Plug.Conn.put_req_header(batch, "last-event-id", "0")
      assert %{status: 200, resp_body: changes} = Router.call(fetch, sqlite)
      assert %{status: 200, resp_body: ^changes} = read.(fetch)
      assert {:ok, %{"rev" => ^head, "more" => false, "changes" => [
        %{"rev" => ^head, "kind" => "settings", "data" => %{"alerts" => "all", "rev" => ^head}}
      ]}} = JSON.decode_line(changes <> "\n")

      huge = browser_request(:get, "/v2/changes?after=9223372036854775808", cookie)
      invalid = Plug.Conn.put_req_header(batch, "last-event-id", "invalid")
      for request <- [batch, huge], run <- [&Router.call(&1, sqlite), read] do
        assert %{status: 410, assigns: %{client_api_error: "cursor_expired"}} = run.(request)
      end
      for run <- [&Router.call(&1, sqlite), read] do
        assert %{status: 400, resp_body: error} = run.(invalid)
        assert {:ok, %{"field" => "last_event_id", "error" => "invalid_field"}} = JSON.decode_line(error <> "\n")
      end

      assert %{status: 401} = read.(Plug.Test.conn(:get, "/v2/changes"))
      assert %{status: 404} = Router.call(fetch, postgres)
      assert %{status: 404} = read.(Plug.Conn.put_req_header(fetch, "accept", "text/event-stream"))

      cutoff = System.system_time(:millisecond) + 1_000
      assert {:ok, 1} = ChangeLog.prune(store, cutoff)
      assert {:ok, ^head} = Streams.prune(scope, cutoff)
      after_floor = browser_request(:get, "/v2/changes?after=#{head}", cookie)
      assert %{status: 200, resp_body: empty} = Router.call(after_floor, sqlite)
      assert %{status: 200, resp_body: ^empty} = read.(after_floor)
      assert {:ok, %{"rev" => ^head, "changes" => [], "more" => false}} = JSON.decode_line(empty <> "\n")
      assert %{status: 410} = Router.call(fetch, sqlite)
      assert %{status: 410} = read.(fetch)

      next = head + 1
      assert {:ok, ^next} = Transaction.run([scope], &Transaction.append(&1, scope, [change("item:synthetic")]))
      assert %{status: 503, resp_body: error} = read.(after_floor)
      assert {:ok, %{"error" => "storage_unavailable"} = unavailable} = JSON.decode_line(error <> "\n")
      refute Map.has_key?(unavailable, "rev")
      assert {:ok, [%{rev: ^next}], false, ^next} = Streams.person_page(principal.person, head, 1)
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

    @tag :tmp_dir
    test "native bearer alerts keep FORMAT32 authority, retained bytes and rollback", %{tmp_dir: dir} do
      alias AttentionServer.{BrowserAuth, DeviceProof, JSON, Relay, Store.AuthSchema}
      alias AttentionServer.ClientAPI.{Auth, Idempotency, Router, Writes}
      alias AttentionServer.Repository.Command
      alias AttentionServer.Store.RecordTable
      alias Exqlite.Sqlite3

      person = "synthetic-native-alerts"
      other = "synthetic-native-alerts-other"
      active = "synthetic-alerts-active"
      pending = "synthetic-alerts-pending"
      outside = "synthetic-alerts-outside"
      path = Path.join(dir, "relay.sqlite3")
      opts = [store_path: path, push_enabled: true]
      relay = start_supervised!({Relay, opts})

      for account <- [person, other] do
        assert :ok = Relay.setup_person(relay, account)
        assert {:ok, _} = Transaction.query("INSERT INTO harklight.people (id) VALUES ($1)", [account])
      end

      devices = [{person, active, false}, {person, pending, true}, {other, outside, false}]

      for {account, device, _} <- devices do
        assert :ok = Relay.setup_device(relay, account, device)
        assert {:ok, _} = Transaction.query(
          "INSERT INTO harklight.principals (key, person_id, kind, revision) VALUES ($1, $2, 'device', 1)",
          [device, account]
        )
      end

      stop_supervised!(Relay)
      {:ok, db} = Sqlite3.open(path)
      assert {:ok, _} = AuthSchema.initialize(db)

      headers =
        Map.new(devices, fn {account, device, pending?} ->
          bearer = BrowserAuth.new_secret()
          hash = bearer |> DeviceProof.digest() |> Base.decode16!(case: :lower)
          put_certificate_session(db, account, device, hash, pending?)
          {device, "Bearer " <> bearer}
        end)

      :ok = Sqlite3.close(db)
      relay = start_supervised!({Relay, opts})
      origin = "https://account.example.test"
      sqlite = Router.init(relay: relay, auth: {:browser, nil, origin})
      postgres = Router.init(relay: nil, repository: :postgres, auth: {:browser, nil, origin})
      native = Plug.Test.conn(:get, "/v2/settings") |> Plug.Conn.put_req_header("authorization", headers[active])
      assert {:ok, principal} = Auth.web(native, nil, relay)
      assert {:ok, 200, _, false} = Writes.push(relay, principal, String.duplicate("b", 64), "production", 1)
      store = :sys.get_state(relay).store
      binding = Map.fetch!(store.engine.push_bindings, {person, active})
      assert {:ok, _} = Transaction.query(
        "INSERT INTO harklight.compatibility_facts VALUES ('push_bindings', $1, $2)",
        [Attributes.key({:row, :push_bindings, {person, active}}), Attributes.encode(binding)]
      )

      assert %{status: 200, resp_body: initial} = initial_response = Router.call(native, sqlite)
      assert {:ok, %{"rev" => head, "settings" => %{"rev" => rev, "push" => %{"revision" => 1}}}} =
               JSON.decode_line(initial <> "\n")
      assert {:ok, _} = Transaction.query(
        "INSERT INTO harklight.stream_heads (person_id, origin_key, head) VALUES ($1, '', $2)", [person, head]
      )
      assert {:ok, _} = Transaction.query(
        "INSERT INTO harklight.stream_resource_revisions VALUES ($1, '', 'settings', 'settings', $2, NULL)", [person, rev]
      )
      assert %{status: 200, resp_body: ^initial} = Router.call(native, postgres)
      assert [tag] = Plug.Conn.get_resp_header(initial_response, "etag")

      key = "00000000-0000-4000-8000-000000000083"
      failed_key = "00000000-0000-4000-8000-000000000084"
      write = fn device, value, match, key -> native_alerts_request(headers[device], value, match, key) end

      for router <- [sqlite, postgres] do
        assert %{status: 403, assigns: %{client_api_error: "forbidden"}} =
                 Router.call(write.(pending, "all", "*", key), router)
        assert %{status: 412} = Router.call(write.(active, "all", ~s("stale"), key), router)
      end

      assert {:error, :unauthorized} =
               Command.execute(:postgres, %{principal | person: other}, {:alerts, "all", "*"}, key, 100)
      assert {:error, :forbidden} =
               Command.execute(:postgres, %{principal | key: pending, session_id: native_session_hash(headers[pending]), pending: false},
                 {:alerts, "all", "*"}, key, 100)

      request = write.(active, "all", tag, key) |> Plug.Conn.put_req_header("origin", "https://other.example.test")
      assert %{status: 200, resp_body: accepted} = Router.call(request, sqlite)
      assert %{status: 200, resp_body: ^accepted} = committed = Router.call(request, postgres)
      assert [] = Plug.Conn.get_resp_header(committed, "idempotent-replayed")
      assert {:ok, %{"settings" => %{"alerts" => "all", "push" => %{"revision" => 1}}}} = JSON.decode_line(accepted <> "\n")
      assert {:ok, %{body: ^accepted, original_response: true}} = Idempotency.lookup_retained(store.db, person, key)
      assert {:ok, %{rows: [["harklight_app_receipts", true, nil]]}} = Transaction.query(
        "SELECT namespace, original_response, expires_at_ms FROM harklight.operation_receipts WHERE person_id = $1 AND operation_key = $2",
        [person, key]
      )

      assert %{status: 200, resp_body: current} = Router.call(write.(active, "important", "*", nil), sqlite)
      assert %{status: 200, resp_body: ^current} = Router.call(write.(active, "important", "*", nil), postgres)

      compact_key = "00000000-0000-4000-8000-000000000085"
      evidence = %{"settings" => true}
      digest = Idempotency.digest("PUT", ["settings", "alerts"], %{"alerts" => "all", "if_match" => tag})
      assert :ok = Idempotency.save(store, person, compact_key, digest, 200, evidence)
      assert {:ok, _} = Transaction.query("""
      INSERT INTO harklight.operation_receipts
        (person_id, origin_key, namespace, operation_key, digest, status, response, created_at_ms, expires_at_ms, original_response)
      VALUES ($1, '', 'client_idempotency', $2, $3, 200, $4, $5, $6, false)
      """, [person, compact_key, Base.decode16!(digest, case: :lower), Attributes.encode(evidence), 1,
        System.system_time(:millisecond) + Idempotency.retention_ms()])

      for router <- [sqlite, postgres] do
        assert %{status: 200, resp_body: ^accepted} = replay = Router.call(request, router)
        assert ["true"] = Plug.Conn.get_resp_header(replay, "idempotent-replayed")
        assert %{status: 200, resp_body: ^current} = compact = Router.call(write.(active, "all", tag, compact_key), router)
        assert ["true"] = Plug.Conn.get_resp_header(compact, "idempotent-replayed")
        assert %{status: 409} = Router.call(write.(active, "important", "*", key), router)
        assert %{status: 200, resp_body: body} = Router.call(write.(outside, "important", "*", key), router)
        assert {:ok, %{"settings" => %{"alerts" => "important", "push" => %{"state" => "unconfigured"}}}} =
                 JSON.decode_line(body <> "\n")
      end

      {:ok, failure_db} = Sqlite3.open(path)
      assert :ok = Sqlite3.execute(failure_db, """
      CREATE TRIGGER reject_native_alert_receipt BEFORE INSERT ON harklight_app_receipts
      WHEN NEW.idempotency_key = '#{failed_key}'
      BEGIN SELECT RAISE(ABORT, 'fixture_failure'); END
      """)
      assert {:ok, _} = Transaction.query("""
      ALTER TABLE harklight.operation_receipts ADD CONSTRAINT reject_native_alert_receipt
      CHECK (operation_key <> '#{failed_key}' OR status <> 200)
      """, [])

      failure = write.(active, "all", "*", failed_key)
      for router <- [sqlite, postgres] do
        assert %{status: 503} = Router.call(failure, router)
        assert %{status: 200, resp_body: ^current} = Router.call(native, router)
      end

      assert {:ok, []} = RecordTable.select(failure_db, "SELECT status FROM harklight_app_receipts WHERE idempotency_key = ?", [failed_key])
      assert {:ok, %{rows: []}} =
               Transaction.query("SELECT status FROM harklight.operation_receipts WHERE operation_key = $1", [failed_key])
      assert {:ok, %{rows: []}} = Transaction.query(
        "SELECT body FROM harklight.compatibility_facts WHERE address IN ($1, $2)",
        [Attributes.key({person, failed_key}), Attributes.key({:row, :receipts, {person, active, failed_key}})]
      )
      assert {:ok, changes} = RecordTable.select(failure_db, """
      SELECT rev, kind, resource_id FROM client_changes WHERE person_id = ? AND rev > ? ORDER BY rev
      """, [person, head])
      # Two commits advance the head twice and coalesce the resource feed.
      assert changes == [[head + 2, "settings", "settings"]]
      assert {:ok, %{rows: ^changes}} = Transaction.query("""
      SELECT revision, resource_kind, resource_id FROM harklight.stream_changes
      WHERE person_id = $1 AND origin_key = '' AND revision > $2 ORDER BY revision
      """, [person, head])

      assert :ok = Sqlite3.execute(failure_db, "DROP TRIGGER reject_native_alert_receipt")
      assert :ok = Sqlite3.close(failure_db)
      assert {:ok, _} = Transaction.query("ALTER TABLE harklight.operation_receipts DROP CONSTRAINT reject_native_alert_receipt", [])
      assert %{status: 200, resp_body: retried} = Router.call(failure, sqlite)
      assert %{status: 200, resp_body: ^retried} = Router.call(failure, postgres)

      assert :ok = Relay.revoke_device(relay, active)
      assert {:ok, _} = Transaction.query("UPDATE harklight.principals SET revoked = true WHERE key = $1", [active])
      for router <- [sqlite, postgres], do: assert(%{status: 401} = Router.call(request, router))
      assert {:error, :unauthorized} = Command.execute(:postgres, principal, {:alerts, "all", "*"}, key, 100)
    end

    defp native_alerts_request(header, value, if_match, key) do
      conn =
        Plug.Test.conn(:put, "/v2/settings/alerts", AttentionServer.JSON.encode!(%{"alerts" => value}))
        |> Plug.Conn.put_req_header("authorization", header)
        |> Plug.Conn.put_req_header("if-match", if_match)

      if key, do: Plug.Conn.put_req_header(conn, "idempotency-key", key), else: conn
    end

    defp native_session_hash("Bearer " <> bearer), do: AttentionServer.DeviceProof.digest(bearer)

    @tag :tmp_dir
    test "owner level saves preserve generation boundaries and roll back with their audit", %{tmp_dir: dir} do
      alias AttentionServer.Relay
      alias AttentionServer.Repository.Postgres.Attributes

      {relay, _browser, _backend, _cookie, owner} = browser_fixture(dir)
      %{person: person, key: key} = owner
      {reader, head, accepted, conversations} = seed_level_history(relay, owner)

      assert Relay.dashboard_levels(:postgres, person, key) == Relay.dashboard_levels(relay, person, key)

      rows = [
        %{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil},
        %{"name" => "Follow up", "lower_bound" => 0.6, "bounce_after_days" => 1.5}
      ]

      for backend <- [relay, :postgres] do
        assert {:error, :forbidden} = Relay.save_dashboard_levels(backend, "synthetic-other", key, rows)
        assert {:error, :forbidden} = Relay.save_dashboard_levels(backend, person, reader, [])
        assert {:error, :invalid_levels} = Relay.save_dashboard_levels(backend, person, key, [])
      end

      # The dashboard owner operation still works for a paused Hosted account.
      assert {:ok, _} = Relay.hosted_delivery_state(relay, person, "STOPPED")
      assert {:ok, _} =
               Transaction.query("UPDATE harklight.people SET delivery_state = 'STOPPED' WHERE id = $1", [person])

      assert {:ok, %{"levels" => ^rows}} = Relay.save_dashboard_levels(relay, person, key, rows)
      assert {:ok, %{"levels" => ^rows}} = Relay.save_dashboard_levels(:postgres, person, key, rows)
      {configuration, revision} = owner_settings_snapshot(person)
      assert revision == head + 1
      assert %{after_delivery_seq: ^accepted, after_conversation_seq: ^conversations, configured_at_ms: first_time} =
               configuration
      original = :sys.get_state(relay).store.engine.bounce_levels[person]
      assert Map.delete(configuration, :configured_at_ms) == Map.delete(original, :configured_at_ms)
      assert {:ok, %{"levels" => ^rows}} = Relay.save_dashboard_levels(:postgres, person, key, rows)
      assert {^configuration, ^revision} = owner_settings_snapshot(person)

      audit = Attributes.key({person, revision})
      assert {:ok, %{rows: [[encoded]]}} =
               Transaction.query(
                 "SELECT body FROM harklight.compatibility_facts WHERE family = 'runtime_audit' AND address = $1",
                 [audit]
               )
      assert {:ok, %{actor_id: ^key, person_id: ^person, action: "dashboard_levels", at_ms: ^first_time}} =
               Attributes.decode(encoded)

      changed = put_in(rows, [Access.at(1), "name"], "Follow up soon")
      conflict = Attributes.key({person, revision + 1})
      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.compatibility_facts VALUES ('runtime_audit', $1, '{}')", [conflict])
      assert {:error, :storage_unavailable} = Relay.save_dashboard_levels(:postgres, person, key, changed)
      assert {^configuration, ^revision} = owner_settings_snapshot(person)
      assert {:ok, _} =
               Transaction.query("DELETE FROM harklight.compatibility_facts WHERE family = 'runtime_audit' AND address = $1", [conflict])

      assert {:ok, %{"levels" => ^changed}} = Relay.save_dashboard_levels(relay, person, key, changed)
      assert {:ok, %{"levels" => ^changed}} = Relay.save_dashboard_levels(:postgres, person, key, changed)
      assert {%{configured_at_ms: ^first_time, after_delivery_seq: ^accepted, after_conversation_seq: ^conversations}, next} =
               owner_settings_snapshot(person)
      assert next == revision + 1

      assert {:ok, _} =
               Transaction.query("UPDATE harklight.principals SET revoked = true WHERE key = $1", [key])
      assert {:error, :forbidden} = Relay.save_dashboard_levels(:postgres, person, key, changed)
      assert {:error, :forbidden} = Relay.dashboard_levels(:postgres, person, key)
    end

    @tag :tmp_dir
    test "browser priority levels preserve FORMAT32 settings, generations and receipts", %{tmp_dir: dir} do
      alias AttentionServer.ClientAPI.{Router, Writes}
      alias AttentionServer.Repository.Postgres.{Attributes, Identities}

      {relay, _browser, backend, cookie, principal} = browser_fixture(dir)
      %{person: person, session_id: session} = principal
      {_reader, head, accepted, conversations} = seed_level_history(relay, principal)
      origin = "https://account.example.test"
      sqlite = Router.init(relay: relay, auth: {:browser, backend, origin})
      postgres = Router.init(relay: nil, repository: :postgres, auth: {:browser, Identities, origin})
      settings = browser_request(:get, "/v2/settings", cookie)
      rows = [
        %{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil},
        %{"name" => "Follow up", "lower_bound" => 0.6, "bounce_after_days" => 1.5}
      ]

      first = "00000000-0000-4000-8000-000000000096"
      second = "00000000-0000-4000-8000-000000000097"
      put = fn target, levels, tag, key ->
        settings_request(cookie, "priority-levels", %{"levels" => levels}, tag, key, origin)
        |> Router.call(target)
      end

      assert %{status: 200, resp_body: initial} = response = Router.call(settings, sqlite)
      assert [tag] = Plug.Conn.get_resp_header(response, "etag")
      assert %{status: 200, resp_body: ^initial} = response = Router.call(settings, postgres)
      assert [^tag] = Plug.Conn.get_resp_header(response, "etag")

      for target <- [sqlite, postgres] do
        assert %{status: 412} = put.(target, [], ~s("stale"), first)
        assert %{status: 400, assigns: %{client_api_error: "invalid_field"}} = put.(target, [], tag, first)
        assert %{status: 403} =
                 settings_request(cookie, "priority-levels", %{"levels" => rows}, tag, first, "https://other.example.test")
                 |> Router.call(target)
      end

      assert %{status: 200, resp_body: body} = put.(sqlite, rows, tag, first)
      assert %{status: 200, resp_body: ^body} = response = put.(postgres, rows, tag, first)
      assert [] = Plug.Conn.get_resp_header(response, "idempotent-replayed")
      {configuration, revision} = owner_settings_snapshot(person)
      assert revision == head + 1
      assert %{after_delivery_seq: ^accepted, after_conversation_seq: ^conversations} = configuration
      assert %{} = format32 = :sys.get_state(relay).store.engine.bounce_levels[person]
      assert Map.delete(configuration, :configured_at_ms) == Map.delete(format32, :configured_at_ms)

      actor = Writes.browser_actor_key(person, session)
      receipt_address = Attributes.key({:row, :receipts, {person, actor, first}})
      assert {:ok, %{rows: [[receipt]]}} =
               Transaction.query("SELECT body FROM harklight.compatibility_facts WHERE family = 'receipts' AND address = $1", [receipt_address])
      assert {:ok, domain_receipt} = Attributes.decode(receipt)
      assert domain_receipt == :sys.get_state(relay).store.engine.receipts[{person, actor, first}]

      assert %{status: 200, resp_body: ^body} = put.(postgres, rows, "*", nil)
      assert {^configuration, ^revision} = owner_settings_snapshot(person)

      changed = put_in(rows, [Access.at(1), "name"], "Follow up soon")
      assert %{status: 409} = put.(postgres, changed, "*", first)
      assert {:ok, _} =
               Transaction.query(
                 """
                 ALTER TABLE harklight.operation_receipts ADD CONSTRAINT fixture_level_receipt
                 CHECK (operation_key <> '00000000-0000-4000-8000-000000000097' OR status <> 200)
                 """,
                 []
               )
      assert %{status: 503} = put.(postgres, changed, "*", second)
      assert {^configuration, ^revision} = owner_settings_snapshot(person)
      assert {:ok, %{rows: []}} =
               Transaction.query("SELECT status FROM harklight.operation_receipts WHERE person_id = $1 AND operation_key = $2", [person, second])
      second_address = Attributes.key({:row, :receipts, {person, actor, second}})
      assert {:ok, %{rows: []}} =
               Transaction.query("SELECT body FROM harklight.compatibility_facts WHERE family = 'receipts' AND address = $1", [second_address])
      assert {:ok, _} = Transaction.query("ALTER TABLE harklight.operation_receipts DROP CONSTRAINT fixture_level_receipt", [])

      assert %{status: 200, resp_body: current} = put.(sqlite, changed, "*", second)
      assert %{status: 200, resp_body: ^current} = put.(postgres, changed, "*", second)
      {updated, next_revision} = owner_settings_snapshot(person)
      assert next_revision == revision + 1
      assert Map.delete(updated, :levels) == Map.delete(configuration, :levels)

      for target <- [sqlite, postgres] do
        assert %{status: 200, resp_body: ^current} = response = put.(target, rows, tag, first)
        assert ["true"] = Plug.Conn.get_resp_header(response, "idempotent-replayed")
      end
    end

    defp seed_level_history(relay, %{person: person} = owner) do
      alias AttentionServer.{JSON, Relay}
      alias AttentionServer.ClientAPI.{Reads, Writes}

      origin = "synthetic-level-origin"
      reader = "synthetic-level-reader"
      assert :ok = Relay.setup_device(relay, person, origin)
      assert :ok = Relay.setup_device(relay, person, reader)
      assert :ok = Relay.setup_sender(relay, origin, "app:fixture")
      message = %{"title" => "Fixture settings history", "created_at" => 10, "sender_id" => "app:fixture", "message_id" => "levels"}
      frame = %{
        "v" => 1, "type" => "forward", "op_id" => "00000000-0000-4000-8000-000000000012",
        "session_handle" => "10000000-0000-4000-8000-000000000001", "message" => message
      }
      assert {:ok, reply} = Relay.request_line(relay, origin, JSON.encode!(frame) <> "\n")
      assert {:ok, %{"type" => "ok"}} = JSON.decode_line(reply)
      assert {:ok, %{"items" => [%{"item_id" => item}]}} = Reads.inbox(relay, reader)
      assert {:ok, 200, _, false} =
               Writes.reply(relay, reader, item, "Fixture reply", [], "00000000-0000-4000-8000-000000000013")

      engine = :sys.get_state(relay).store.engine
      accepted = Map.fetch!(engine.persons, person)
      conversations = Map.fetch!(engine.conversation_highs, person)
      assert accepted > 0 and conversations > 0
      assert {:ok, %{"rev" => head}} = Reads.settings(relay, owner)
      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.stream_heads (person_id, origin_key, head) VALUES ($1, '', $2)",
                 [person, head]
               )
      for {kind, high} <- [{"accepted", accepted}, {"conversation", conversations}] do
        assert {:ok, _} =
                 Transaction.query(
                   "INSERT INTO harklight.allocation_highs VALUES ($1, '', '', $2, $3)",
                   [person, kind, high]
                 )
      end

      {reader, head, accepted, conversations}
    end

    defp owner_settings_snapshot(person) do
      alias AttentionServer.Repository.Postgres.Attributes

      assert {:ok, %{rows: [[attributes, revision]]}} =
               Transaction.query(
                 """
                 SELECT person.settings, stream.head
                 FROM harklight.people AS person JOIN harklight.stream_heads AS stream ON person.id = stream.person_id
                 WHERE person.id = $1 AND stream.origin_key = ''
                 """,
                 [person]
               )
      assert {:ok, %{bounce_levels: levels}} = Attributes.decode(attributes)
      {levels, revision}
    end

    defp alerts_request(cookie, value, if_match, key, origin) do
      settings_request(cookie, "alerts", %{"alerts" => value}, if_match, key, origin)
    end

    defp settings_request(cookie, route, body, if_match, key, origin) do
      conn =
        Plug.Test.conn(:put, "/v2/settings/" <> route, AttentionServer.JSON.encode!(body))
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

    @tag :tmp_dir
    test "instance prioritization preserves FORMAT32 settings and rolls back on audit failure", %{tmp_dir: dir} do
      alias AttentionServer.{DeviceIdentity, Relay, TLSFixtures}
      alias AttentionServer.Repository.Postgres.{Attributes, InstanceSettings}

      certs = TLSFixtures.create!()
      on_exit(fn -> File.rm_rf!(certs) end)
      certificate = TLSFixtures.certificate_der!(certs, "a1")
      assert {:ok, key} = DeviceIdentity.from_certificate(certificate)
      opts = [store_path: Path.join(dir, "instance.sqlite3")]
      relay = start_supervised!({Relay, opts})
      assert {:ok, person} = Relay.operator_bootstrap(relay, certificate)
      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.people (id, settings) VALUES ($1, $2)", [
                 person, Attributes.encode(%{alert_choices: "all"})
               ])
      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.principals (key, person_id, kind, revision) VALUES ($1, $2, 'owner', 1)",
                 [key, person]
               )

      authority = %{person: person, key: key, configured_owner_person: person, deployment_mode: :self_hosted}
      other = %{authority | configured_owner_person: "another-person"}
      compare = fn auth, {algorithm, endpoint, model} = values, expected ->
        assert Relay.save_dashboard_prioritization(
                 relay, auth.person, auth.key, auth.configured_owner_person, algorithm, endpoint, model
               ) == expected
        assert InstanceSettings.save(auth, values, 100) == expected
      end

      compare.(other, {"unknown", nil, nil}, {:error, :unknown_algorithm})
      compare.(other, {"baseline", nil, nil}, {:error, :forbidden})
      compare.(%{authority | key: "wrong-owner"}, {"baseline", "", ""}, {:error, :forbidden})
      compare.(authority, {"baseline", "http://fixture.example.test", "fixture"}, {:error, :invalid_endpoint})
      compare.(authority, {"baseline", "", nil}, {:error, :invalid_model})

      endpoint = "https://fixture.example.test/compose"
      model = "fixture-model"
      assert {:ok, expected} =
               Relay.save_dashboard_prioritization(relay, person, key, person, "jev-set", " " <> endpoint, model <> " ")
      durable = Map.take(expected, [:person_id, :algorithm_id, :endpoint, :model])
      assert durable == %{person_id: person, algorithm_id: "jev-set", endpoint: endpoint, model: model}
      assert {:ok, ^durable} = InstanceSettings.save(authority, {"jev-set", " " <> endpoint, model <> " "}, 100)
      {attrs, scalars, audits} = instance_settings_snapshot(person)
      assert attrs == %{algorithm_choices: "jev-set", alert_choices: "all"}
      assert scalars == %{"jev_endpoint" => endpoint, "jev_model" => model}
      assert {:ok, ^durable} = InstanceSettings.save(authority, {"jev-set", endpoint, model}, 101)
      assert {^attrs, ^scalars, ^audits} = instance_settings_snapshot(person)

      assert {:ok, _} =
               Transaction.query(
                 "ALTER TABLE harklight.compatibility_facts ADD CONSTRAINT fixture_audit CHECK (family <> 'runtime_audit') NOT VALID",
                 []
               )
      try do
        assert {:error, :storage_unavailable} = InstanceSettings.save(authority, {"baseline", "", ""}, 102)
        assert {^attrs, ^scalars, ^audits} = instance_settings_snapshot(person)
      after
        assert {:ok, _} =
                 Transaction.query("ALTER TABLE harklight.compatibility_facts DROP CONSTRAINT fixture_audit", [])
      end

      assert {:ok, cleared} = Relay.save_dashboard_prioritization(relay, person, key, person, "baseline", "", "")
      assert {:ok, persisted} = InstanceSettings.save(authority, {"baseline", "", ""}, 103)
      assert persisted == Map.take(cleared, [:person_id, :algorithm_id, :endpoint, :model])
      assert {%{algorithm_choices: "baseline", alert_choices: "all"}, %{"jev_endpoint" => nil, "jev_model" => nil}, count} =
               instance_settings_snapshot(person)
      assert count == audits + 1
      assert {:ok, %{rows: []}} =
               Transaction.query("SELECT head FROM harklight.stream_heads WHERE person_id = $1", [person])

      stop_supervised!(Relay)
      relay = start_supervised!({Relay, Keyword.put(opts, :deployment_mode, :saas)})
      assert {:error, :forbidden} = Relay.save_dashboard_prioritization(relay, person, key, person, "baseline", "", "")
      assert {:error, :forbidden} = InstanceSettings.save(%{authority | deployment_mode: :saas}, {"baseline", "", ""}, 104)
    end

    defp instance_settings_snapshot(person) do
      alias AttentionServer.Repository.Postgres.Attributes

      assert {:ok, %{rows: [[encoded]]}} =
               Transaction.query("SELECT settings FROM harklight.people WHERE id = $1", [person])
      assert {:ok, attrs} = Attributes.decode(encoded)
      assert {:ok, %{rows: rows}} =
               Transaction.query("SELECT family, body FROM harklight.compatibility_facts WHERE family IN ('jev_endpoint', 'jev_model')", [])
      facts = Map.new(rows, fn [family, body] ->
        assert {:ok, value} = Attributes.decode(body)
        {family, value}
      end)
      assert {:ok, %{rows: [[count]]}} =
               Transaction.query("SELECT count(*) FROM harklight.compatibility_facts WHERE family = 'runtime_audit'", [])
      {attrs, facts, count}
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

    @tag :tmp_dir
    test "bounded ordinary summary facts match retained FORMAT32 ranks and chat", %{tmp_dir: dir} do
      alias AttentionServer.{ChangeLog, JSON, Relay}
      alias AttentionServer.ClientAPI.{Reads, Writes}
      alias AttentionServer.Repository.RecipientItems
      alias AttentionServer.Store.RecordTable

      {relay, _browser, _backend, _cookie, principal} = browser_fixture(dir)
      person = principal.person
      origin = "synthetic-summary-origin"
      handle = "10000000-0000-4000-8000-000000000085"
      key = {person, "app:fixture", "summary"}
      assert :ok = Relay.setup_device(relay, person, origin)
      assert :ok = Relay.setup_sender(relay, origin, "app:fixture")
      forward = %{
        "v" => 1, "type" => "forward", "op_id" => "00000000-0000-4000-8000-000000000051",
        "session_handle" => handle,
        "message" => %{"sender_id" => "app:fixture", "message_id" => "summary", "created_at" => 1, "title" => "Summary"}
      }

      assert {:ok, reply} = Relay.request_line(relay, origin, JSON.encode!(forward) <> "\n")
      assert {:ok, %{"type" => "ok"}} = JSON.decode_line(reply)
      assert {:ok, 200, _, false} =
               Writes.reply(relay, principal, "i1", "Synthetic discussion", [], "00000000-0000-4000-8000-000000000052")

      state = :sys.get_state(relay)
      entries = state.store.engine.origin_feeds[{person, origin}].entries
      turn = Enum.find_value(entries, fn
        {seq, %{"kind" => "discussion", "author" => "person"}} -> seq
        _ -> nil
      end)
      assert is_integer(turn)
      agent_reply = %{
        "v" => 1, "type" => "conversation_reply", "op_id" => "00000000-0000-4000-8000-000000000053",
        "sender_id" => "app:fixture", "message_id" => "summary", "request_revision" => 1,
        "in_reply_to" => turn, "text" => "Synthetic response"
      }
      assert {:ok, reply} = Relay.request_line(relay, origin, JSON.encode!(agent_reply) <> "\n")
      assert {:ok, %{"type" => "ok"}} = JSON.decode_line(reply)
      assert {:ok, 200, _, false} = Writes.seen(relay, principal, "i1", "00000000-0000-4000-8000-000000000054")

      assert {:ok, %{"rev" => head, "item" => item}} = Reads.item(relay, principal, "i1")
      assert %{"seen" => true, "chat" => %{"started" => true, "unread_replies" => 1}} = item
      state = :sys.get_state(relay)
      assert {:ok, [[rank_key, rank_json]]} =
               RecordTable.select(state.store.db, "SELECT rank_key, rank_json FROM client_ranks WHERE person_id = ? AND item_id = 'i1'", [person])
      assert {:ok, rank} = JSON.decode_line(rank_json <> "\n")

      assert {:ok, _} =
               Transaction.query(
                 "INSERT INTO harklight.principals (key, person_id, kind, revision) VALUES ($1, $2, 'device', 1)",
                 [origin, person]
               )
      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.origin_sessions (person_id, origin_key, handle) VALUES ($1, $2, $3)", [
                 person, origin, handle
               ])
      assert {:ok, _} =
               Transaction.query(
                 """
                 INSERT INTO harklight.messages
                   (id, person_id, origin_key, session_handle, sender_id, message_id, revision, accepted_at_ms, accepted_position)
                 VALUES ('summary-item', $1, $2, $3, 'app:fixture', 'summary', 1, 1, 1)
                 """,
                 [person, origin, handle]
               )
      attrs = Attributes.encode(%{visible: true, substantive_revision: 1})
      assert {:ok, _} =
               Transaction.query(
                 """
                 INSERT INTO harklight.message_recipients (message_id, person_id, item_id, seen_revision, attributes)
                 VALUES ('summary-item', $1, 'i1', 1, $2)
                 """,
                 [person, attrs]
               )
      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.stream_heads (person_id, origin_key, head, seed_revision) VALUES ($1, '', $2, 1)", [
                 person, head
               ])
      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.stream_resource_revisions VALUES ($1, '', 'item', $2, $3, NULL)", [
                 person, ChangeLog.item_resource(key), item["rev"]
               ])
      put_summary_fact("client_rank_state", person, [person])
      put_summary_fact("client_ranks", {person, "i1"}, [person, "i1", rank_key, rank_json])

      read = fn p -> Transaction.snapshot(fn -> RecipientItems.summary_facts(:postgres, p, "i1") end) end
      assert {:error, :transaction_required} = RecipientItems.summary_facts(:postgres, person, "i1")
      assert {:error, :not_found} = read.("other-person")
      assert {:ok, %{rank: ^rank, head: ^head, chat: :unprojected, availability: :unprojected, seen: true}} = read.(person)

      summary = state.store.engine.route_b_index.conversation_summary_by_item[key]
      chat_attrs = Attributes.encode(%{
        chat_started: summary.chat_started, latest_origin_reply_seq: summary.latest_origin_reply_seq, unread_replies: 1
      })
      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.conversations VALUES ('summary-item', 'summary-item', $1)", [chat_attrs])
      stamp = item["availability"]["last_detached_at_ms"]
      put_summary_fact("item_availability", {:row, :item_availability, key}, {item["availability"]["state"], stamp})

      assert {:ok, actual} = read.(person)
      assert actual.resource_revision == item["rev"]
      assert Map.drop(actual.rank, ["_reply", "_thread"]) == item["rank"]
      assert actual.chat == item["chat"]
      assert actual.availability == item["availability"]

      assert {:ok, _} = Transaction.query("UPDATE harklight.messages SET revision = 2 WHERE id = 'summary-item'", [])
      assert {:ok, %{request_revision: 2, seen: true}} = read.(person)
      assert {:ok, _} =
               Transaction.query("UPDATE harklight.message_recipients SET attributes = $1 WHERE message_id = 'summary-item'", [
                 Attributes.encode(%{visible: true, substantive_revision: 2})
               ])
      assert {:ok, %{request_revision: 2, seen: false}} = read.(person)

      assert {:ok, _} =
               Transaction.query("DELETE FROM harklight.compatibility_facts WHERE family = 'client_rank_state' AND address = $1", [
                 Attributes.key(person)
               ])
      assert {:error, :storage_unavailable} = read.(person)
    end

    defp put_summary_fact(family, address, value) do
      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.compatibility_facts VALUES ($1, $2, $3)", [
                 family, Attributes.key(address), Attributes.encode(value)
               ])
    end

    @tag :tmp_dir
    test "ordinary PostgreSQL item preserves the full FORMAT32 body and inherited UI/1 file IDs", %{tmp_dir: dir} do
      alias AttentionServer.{ChangeLog, JSON, Relay}
      alias AttentionServer.ClientAPI.{Context, Reads, Resources, Router, Writes}
      alias AttentionServer.Repository.Postgres.Identities
      alias AttentionServer.Store.RecordTable

      {relay, _browser, backend, cookie, principal} = browser_fixture(dir)
      person = principal.person
      origin = "synthetic-item-complete-origin"
      session = "10000000-0000-4000-8000-000000000092"
      key = {person, "app:fixture", "complete"}
      assert :ok = Relay.setup_device(relay, person, origin)
      assert :ok = Relay.setup_sender(relay, origin, "app:fixture")

      image = %{"kind" => "image", "x" => 0, "y" => 0, "width" => 100, "height" => 100, "file" => "plan.png"}
      action = %{"id" => "blue", "label" => "Blue", "kind" => "value", "value" => "blue", "illustration" => "plan"}
      screen = %{
        "schema" => "attention-client.ui/1",
        "requires" => ~w(scene/1 result/1 illustration/1 response/1 image/1),
        "illustrations" => [%{"id" => "plan", "label" => "Plan", "width" => 100, "height" => 100, "primitives" => [image]}],
        "inputs" => [%{"id" => "choice", "label" => "Choose", "kind" => "response", "actions" => [action]}],
        "open" => %{"kind" => "input", "id" => "choice", "layout" => "cards"},
        "resolved" => %{"kind" => "text", "text" => "Recorded", "style" => "body"},
        "submission" => %{"label" => "Send", "result" => %{"kind" => "input", "id" => "choice"}}
      }
      message = %{
        "sender_id" => "app:fixture", "message_id" => "complete", "created_at" => 1,
        "title" => "First", "message_type" => "request", "answer_ui" => screen
      }

      request = fn payload ->
        assert {:ok, reply} = Relay.request_line(relay, origin, JSON.encode!(Map.put(payload, "v", 1)) <> "\n")
        assert {:ok, %{"type" => "ok"}} = JSON.decode_line(reply)
      end

      request.(%{
        "type" => "forward", "op_id" => "00000000-0000-4000-8000-000000000061",
        "session_handle" => session, "message" => message,
        "files" => Enum.map(["unused.png", "plan.png"], &%{"name" => &1, "data_base64" => "AAEC"})
      })
      request.(%{
        "type" => "conversation_revise", "op_id" => "00000000-0000-4000-8000-000000000062",
        "sender_id" => "app:fixture", "message_id" => "complete", "base_revision" => 1,
        "message" => %{message | "title" => "Current"},
        "files" => [%{"name" => "current.bin", "data_base64" => "AQI="}]
      })
      assert {:ok, 200, _, false} =
               Writes.answer(relay, principal, "i1", 2, {:result, %{"choice" => "blue"}}, "00000000-0000-4000-8000-000000000063")

      source_state = :sys.get_state(relay)
      terminal =
        source_state.store.engine.origin_feeds[{person, origin}].entries
        |> Map.values()
        |> Enum.find(&(&1["kind"] == "terminal"))
      assert %{"feed_seq" => terminal_seq} = terminal
      assert {:ok, _} =
               GenServer.call(relay, {:origin_ack2, origin, terminal_seq,
                 %{"op_id" => "00000000-0000-4000-8000-000000000064", "outcome" => "handed"}})
      request.(%{
        "type" => "conversation_result", "op_id" => "00000000-0000-4000-8000-000000000065",
        "sender_id" => "app:fixture", "message_id" => "complete", "in_reply_to" => terminal_seq, "status" => "applied"
      })

      path = "/v2/items/i1"
      conn = browser_request(:get, path, cookie)
      format32 = Router.init(relay: relay, auth: {:browser, backend, nil})
      assert %{status: 200, resp_body: expected} = response = Router.call(conn, format32)
      assert [tag] = Plug.Conn.get_resp_header(response, "etag")
      assert {:ok, %{"rev" => head, "item" => item}} = JSON.decode_line(expected <> "\n")
      assert %{
        "request_revision" => 2,
        "files" => [%{"file_id" => "fi1.2.0", "name" => "current.bin"}, %{"file_id" => "fi1.1.1", "name" => "plan.png"}],
        "terminal" => %{"answer" => %{"kind" => "result", "value" => %{"choice" => "blue"}}},
        "result" => %{"status" => "applied"}
      } = item

      state = :sys.get_state(relay)
      record = state.store.engine.messages[key]
      files = Resources.item_file_refs(record, state.store.engine.uploads)
      assert files == item["files"]
      assert {:ok, [[rank_key, rank_json]]} =
               RecordTable.select(state.store.db, "SELECT rank_key, rank_json FROM client_ranks WHERE person_id = ? AND item_id = 'i1'", [person])

      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.principals (key, person_id, kind, revision) VALUES ($1, $2, 'device', 1)", [origin, person])
      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.origin_sessions (person_id, origin_key, handle) VALUES ($1, $2, $3)", [person, origin, session])
      assert {:ok, _} =
               Transaction.query(
                 """
                 INSERT INTO harklight.messages
                   (id, person_id, origin_key, session_handle, sender_id, message_id, revision,
                    accepted_at_ms, accepted_position, free_order, terminal)
                 VALUES ('complete-item', $1, $2, $3, 'app:fixture', 'complete', 2, $4, 1, $5, $6)
                 """,
                 [person, origin, session, record.accepted_at, Map.get(record, :free_order), Attributes.encode(record.terminal)]
               )
      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.message_revisions VALUES ('complete-item', 2, $1, $2)", [
                 record.revision_accepted_at, Attributes.encode(record.message)
               ])

      bounce = state.store.engine.bounce_revisions[key]
      substantive = Map.get(bounce.raw_to_substantive, 2, bounce.current_substantive_revision)
      metadata = %{visible: true, substantive_revision: substantive, origin_result: record.origin_result}
      seen = if item["seen"], do: substantive
      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.message_recipients VALUES ('complete-item', $1, 'i1', $2, $3)", [
                 person, seen, Attributes.encode(metadata)
               ])
      assert {:ok, _} = Transaction.query("INSERT INTO harklight.stream_heads (person_id, origin_key, head) VALUES ($1, '', $2)", [person, head])
      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.stream_resource_revisions VALUES ($1, '', 'item', $2, $3, NULL)", [
                 person, ChangeLog.item_resource(key), item["rev"]
               ])
      put_summary_fact("client_rank_state", person, [person])
      put_summary_fact("client_ranks", {person, "i1"}, [person, "i1", rank_key, rank_json])

      ctx = %Context{principal: nil, relay: nil, request_id: nil, params: %{item_id: "i1"}}
      assert %{status: 503} = Reads.get_postgres_item(conn, ctx)
      chat = %{
        chat_started: item["chat"]["started"],
        latest_origin_reply_seq: nil,
        unread_replies: item["chat"]["unread_replies"]
      }
      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.conversations VALUES ('complete-item', 'complete-item', $1)", [Attributes.encode(chat)])
      availability = item["availability"]
      put_summary_fact("ordinary_item_availability", {person, "i1"}, {
        availability["state"], availability["last_detached_at_ms"]
      })
      assert %{status: 503} = Reads.get_postgres_item(conn, ctx)
      put_summary_fact("ordinary_item_files", {person, "i1", 2}, files)

      assert %{status: 200, resp_body: ^expected} = actual = Reads.get_postgres_item(conn, ctx)
      assert [^tag] = Plug.Conn.get_resp_header(actual, "etag")
      conditional = Plug.Conn.put_req_header(conn, "if-none-match", tag)
      assert %{status: 304, resp_body: ""} = Reads.get_postgres_item(conditional, ctx)
      pg = Router.init(relay: nil, repository: :postgres, auth: {:browser, Identities, nil})
      assert %{status: 404} = Router.call(conn, pg)

      assert {:ok, _} = Transaction.query("""
        INSERT INTO harklight.messages (id, person_id, origin_key, sender_id, message_id, kind, revision, accepted_at_ms)
        VALUES ('complete', $1, $2, 'app:fixture', 'complete', 'shared', 2, 1)
        """, [person, origin])
      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.message_revisions VALUES ('complete', 2, 1, $1)", [
                 Attributes.encode(record.message)
               ])
      assert {:ok, _} =
               Transaction.query("INSERT INTO harklight.message_recipients VALUES ('complete', $1, 'i99', $2, $3)", [
                 person, seen, Attributes.encode(metadata)
               ])
      snapshot = fn -> AttentionServer.Repository.RecipientItems.summary_facts(:postgres, person, "i99") end
      assert {:error, :not_found} = Transaction.snapshot(snapshot)
      assert {:error, :storage_unavailable} =
               AttentionServer.Repository.Postgres.Items.current_recipient(person, "i99", 2)
      assert %{status: 404} = Reads.get_postgres_item(conn, %{ctx | params: %{item_id: "i99"}})
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
