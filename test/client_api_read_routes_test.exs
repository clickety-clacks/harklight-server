defmodule AttentionServer.ClientAPIReadRoutesTest do
  # The reads mounted on the client API router: query parameters, ETag and
  # If-None-Match, error envelopes and file responses.
  use ExUnit.Case, async: false

  import Plug.Test
  import Plug.Conn

  alias AttentionServer.{JSON, Relay, TLSFixtures}
  alias AttentionServer.ClientAPI.{Context, Principal, Reads, Router, Uploads}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @bytes :binary.copy(<<1, 2, 3, 4>>, 50)

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir =
      Path.join(System.tmp_dir!(), "harklight-read-routes-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    flags = Path.join(dir, "flags.json")
    File.write!(flags, ~s({"client_api_v2":true}))

    store_path = Path.join(dir, "state.sqlite3")

    assert {:ok, relay} =
             Relay.start_link(
               store_path: store_path,
               feature_flags_path: flags,
               agent_fixture_mode: true
             )

    on_exit(fn -> if Process.alive?(relay), do: GenServer.stop(relay) end)

    der = TLSFixtures.certificate_der!(certs, "a1")
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert {:ok, reader} = Relay.setup_device_certificate(relay, "A", der)
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    for {id, n} <- [{"one", 2}, {"two", 3}, {"three", 4}] do
      frame = %{
        "type" => "forward",
        "op_id" => op(n),
        "session_handle" => @handle,
        "message" => %{
          "sender_id" => "app:test",
          "message_id" => id,
          "created_at" => n,
          "title" => id
        }
      }

      frame =
        if id == "one",
          do:
            Map.put(frame, "files", [%{"name" => "a.bin", "data_base64" => Base.encode64(@bytes)}]),
          else: frame

      assert {:ok, line} =
               Relay.request_line(relay, "origin", JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

      assert {:ok, %{"type" => "ok"}} = JSON.decode_line(line)
    end

    %{relay: relay, reader: reader, der: der, flags: flags, store_path: store_path}
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp ctx(%{relay: relay, reader: reader}, params \\ %{}) do
    %Context{
      principal: %Principal{person: "A", kind: :device, key: reader},
      relay: relay,
      request_id: "req-1",
      params: params
    }
  end

  defp get(path, headers \\ []) do
    Enum.reduce(headers, conn(:get, path), fn {k, v}, conn -> put_req_header(conn, k, v) end)
    |> assign(:client_api_request_id, "req-1")
  end

  defp body(conn) do
    assert {:ok, decoded} = JSON.decode_line(conn.resp_body <> "\n")
    decoded
  end

  defp etag(conn), do: conn |> get_resp_header("etag") |> List.first()

  defp file_request(relay, der, conn) do
    conn
    |> put_peer_data(%{address: {127, 0, 0, 1}, port: 1, ssl_cert: der})
    |> Router.call(Router.init(relay: relay))
  end

  defp stage_file(relay, der, key) do
    conn =
      conn(:post, "/v2/files", @bytes)
      |> put_req_header("idempotency-key", key)
      |> put_req_header("content-length", Integer.to_string(byte_size(@bytes)))
      |> put_req_header("content-disposition", "attachment; filename*=UTF-8''discard.bin")
      |> then(&file_request(relay, der, &1))

    assert conn.status == 201
    body(conn)["file_id"]
  end

  defp restart_relay(%{relay: relay, store_path: store_path, flags: flags}) do
    GenServer.stop(relay)

    assert {:ok, relay} =
             Relay.start_link(
               store_path: store_path,
               feature_flags_path: flags,
               agent_fixture_mode: true
             )

    on_exit(fn -> if Process.alive?(relay), do: GenServer.stop(relay) end)
    relay
  end

  test "discard removes the staged row and bytes, and replays by person and key", ctx do
    %{relay: relay, der: der, certs: certs} = ctx
    handle = stage_file(relay, der, op(100))
    other_handle = stage_file(relay, der, op(101))
    store = :sys.get_state(relay).store
    assert {:ok, %{path: staged_path}} = Uploads.fetch(store, "A", handle)
    delete = conn(:delete, "/v2/files/" <> handle) |> put_req_header("idempotency-key", op(102))
    fetched = file_request(relay, der, conn(:get, "/v2/files/" <> handle))
    assert fetched.status == 404
    assert body(fetched)["error"] == "not_found"

    stranger = TLSFixtures.certificate_der!(certs, "b1")
    assert :ok = Relay.setup_person(relay, "B")
    assert {:ok, _} = Relay.setup_device_certificate(relay, "B", stranger)
    refused = file_request(relay, stranger, delete)
    assert refused.status == 404
    assert body(refused)["error"] == "not_found"
    assert File.regular?(staged_path)

    first = file_request(relay, der, delete)
    assert first.status == 204
    assert first.resp_body == ""
    assert get_resp_header(first, "idempotent-replayed") == []
    assert {:error, :not_found} = Uploads.fetch(store, "A", handle)
    refute File.exists?(staged_path)

    replay = file_request(relay, der, delete)
    assert replay.status == 204
    assert replay.resp_body == ""
    assert get_resp_header(replay, "idempotent-replayed") == ["true"]

    conflict =
      conn(:delete, "/v2/files/" <> other_handle)
      |> put_req_header("idempotency-key", op(102))
      |> then(&file_request(relay, der, &1))

    assert conflict.status == 409
    assert body(conflict)["error"] == "idempotency_conflict"
    assert {:ok, _} = Uploads.fetch(store, "A", other_handle)

    absent = file_request(relay, der, conn(:delete, "/v2/files/" <> handle))
    assert absent.status == 404
    assert body(absent)["error"] == "not_found"

    invalid =
      conn(:delete, "/v2/files/" <> other_handle)
      |> put_req_header("idempotency-key", "invalid")
      |> then(&file_request(relay, der, &1))

    assert invalid.status == 400
    assert body(invalid)["field"] == "Idempotency-Key"
  end

  test "discard refuses an attached upload without affecting the downloadable file", ctx do
    %{relay: relay, reader: reader, der: der} = ctx
    handle = stage_file(relay, der, op(110))
    twin = stage_file(relay, der, op(111))
    {:ok, %{"items" => items}} = Reads.inbox(relay, reader)
    item = Enum.find(items, &(&1["message"]["message_id"] == "two"))

    reply =
      conn(
        :post,
        "/v2/items/" <> item["item_id"] <> "/replies",
        JSON.encode!(%{"text" => "With a file", "file_ids" => [handle]})
      )
      |> put_req_header("content-type", "application/json")
      |> put_req_header("idempotency-key", op(112))
      |> then(&file_request(relay, der, &1))

    assert reply.status == 200
    assert [%{"file_id" => download_id}] = body(reply)["entry"]["files"]
    refute download_id == handle
    relay = restart_relay(ctx)

    refused = file_request(relay, der, conn(:delete, "/v2/files/" <> handle))
    assert refused.status == 409
    assert body(refused)["error"] == "already_closed"

    discarded = file_request(relay, der, conn(:delete, "/v2/files/" <> twin))
    assert discarded.status == 204
    downloaded = file_request(relay, der, conn(:get, "/v2/files/" <> download_id))
    assert downloaded.status == 200
    assert downloaded.resp_body == @bytes
  end

  test "discard cleanup retries after unlink failure on replay and after restart", ctx do
    %{relay: relay, der: der} = ctx
    store = :sys.get_state(relay).store

    [{replay_path, replay_request}, {boot_path, _}] =
      for n <- [120, 122] do
        id = stage_file(relay, der, op(n))
        assert {:ok, %{path: path}} = Uploads.fetch(store, "A", id)
        File.rename!(path, path <> ".fixture")
        File.mkdir!(path)

        req = conn(:delete, "/v2/files/" <> id) |> put_req_header("idempotency-key", op(n + 1))
        assert file_request(relay, der, req).status == 204
        assert {:error, :not_found} = Uploads.fetch(store, "A", id)
        assert File.dir?(path)

        File.rmdir!(path)
        File.rename!(path <> ".fixture", path)
        {path, req}
      end

    replay = file_request(relay, der, replay_request)
    assert get_resp_header(replay, "idempotent-replayed") == ["true"]
    refute File.exists?(replay_path)
    assert File.regular?(boot_path)

    restart_relay(ctx)

    assert Enum.reduce_while(1..100, false, fn _, _ ->
             if File.exists?(boot_path) do
               Process.sleep(20)
               {:cont, false}
             else
               {:halt, true}
             end
           end)
  end

  test "a read answers 200 with an ETag, and 304 when the client already has it", ctx do
    first = Reads.get_inbox(get("/v2/inbox?limit=2&unknown=1"), ctx(ctx))
    assert first.status == 200
    assert get_resp_header(first, "cache-control") == ["private, no-cache"]
    assert %{"items" => [_, _], "next_page" => page} = body(first)
    tag = etag(first)
    assert tag == Reads.etag(body(first))

    for header <- [tag, "W/" <> tag, ~s("other", #{tag}), "*"] do
      again = Reads.get_inbox(get("/v2/inbox?limit=2", [{"if-none-match", header}]), ctx(ctx))
      assert again.status == 304
      assert again.resp_body == ""
      assert etag(again) == tag
    end

    changed =
      Reads.get_inbox(get("/v2/inbox?limit=2", [{"if-none-match", ~s("stale")}]), ctx(ctx))

    assert changed.status == 200

    rest = Reads.get_inbox(get("/v2/inbox?limit=2&page=" <> page), ctx(ctx))
    assert %{"items" => [_]} = body(rest)
    refute etag(rest) == tag
  end

  test "bad parameters answer the contract's error envelope", ctx do
    bad = Reads.get_inbox(get("/v2/inbox?limit=0"), ctx(ctx))
    assert bad.status == 400

    assert body(bad) == %{
             "error" => "invalid_field",
             "field" => "limit",
             "retryable" => false,
             "request_id" => "req-1"
           }

    listed = Reads.get_inbox(get("/v2/inbox?limit[]=2"), ctx(ctx))
    assert listed.status == 400

    missing = Reads.get_item(get("/v2/items/i999"), ctx(ctx, %{item_id: "i999"}))
    assert missing.status == 404
    assert body(missing)["error"] == "not_found"

    revoked = %{ctx(ctx) | principal: %Principal{person: "A", kind: :device, key: "gone"}}
    assert Reads.get_settings(get("/v2/settings"), revoked).status == 401
  end

  test "items, entries, revisions and settings read through their routes", ctx do
    %{"items" => items} = body(Reads.get_inbox(get("/v2/inbox"), ctx(ctx)))
    one = Enum.find(items, &(&1["message"]["message_id"] == "one"))

    item = Reads.get_item(get("/v2/items/x"), ctx(ctx, %{item_id: one["item_id"]}))
    assert %{"item" => %{"item_id" => id}} = body(item)
    assert id == one["item_id"]

    entries =
      Reads.get_entries(get("/v2/items/x/entries?limit=5"), ctx(ctx, %{item_id: id}))

    assert %{"entries" => [], "older" => false} = body(entries)

    revision =
      Reads.get_revision(get("/v2/items/x/revisions/1"), ctx(ctx, %{item_id: id, n: "1"}))

    assert %{"revision" => %{"request_revision" => 1}} = body(revision)

    assert Reads.get_revision(get("/v2/items/x/revisions/2"), ctx(ctx, %{item_id: id, n: "2"})).status ==
             404

    settings = Reads.get_settings(get("/v2/settings"), ctx(ctx))
    assert etag(settings) == Reads.etag(body(settings)["settings"])

    unchanged =
      Reads.get_settings(get("/v2/settings", [{"if-none-match", etag(settings)}]), ctx(ctx))

    assert unchanged.status == 304
  end

  test "a file answers its bytes with its type and sha256 ETag", ctx do
    %{"items" => items} = body(Reads.get_inbox(get("/v2/inbox"), ctx(ctx)))
    [ref] = Enum.find(items, &(&1["message"]["message_id"] == "one"))["files"]
    path = "/v2/files/" <> ref["file_id"]

    file = Reads.get_file(get(path), ctx(ctx, %{file_id: ref["file_id"]}))
    assert file.status == 200
    assert file.resp_body == @bytes
    assert get_resp_header(file, "content-type") == ["application/octet-stream"]
    assert etag(file) == ~s("#{ref["sha256"]}")

    cached =
      Reads.get_file(
        get(path, [{"if-none-match", etag(file)}]),
        ctx(ctx, %{file_id: ref["file_id"]})
      )

    assert cached.status == 304
    assert cached.resp_body == ""

    missing = Reads.get_file(get("/v2/files/fx"), ctx(ctx, %{file_id: "fx"}))
    assert missing.status == 404
  end

  test "the router serves the reads to an enrolled device", ctx do
    opts = Router.init(relay: ctx.relay)

    conn =
      conn(:get, "/v2/inbox?limit=1")
      |> put_peer_data(%{address: {127, 0, 0, 1}, port: 1, ssl_cert: ctx.der})
      |> Router.call(opts)

    assert conn.status == 200
    assert [_request_id] = get_resp_header(conn, "request-id")
    assert %{"items" => [%{"item_id" => _}], "next_page" => _, "rev" => rev} = body(conn)
    assert rev > 0

    stranger =
      conn(:get, "/v2/inbox")
      |> put_peer_data(%{address: {127, 0, 0, 1}, port: 1, ssl_cert: "not a certificate"})
      |> Router.call(opts)

    assert stranger.status == 401
  end

  test "me reports the account, the operator's client features and the published limits", ctx do
    me = Reads.get_me(get("/v2/me"), ctx(ctx))
    assert me.status == 200
    body = body(me)

    defs =
      AttentionServer.ClientAPISchema.contract_defs(
        Path.expand("../docs/client-api-v2.md", __DIR__)
      )

    assert AttentionServer.ClientAPISchema.errors(defs, "Me", body) == []

    assert body["account"] == %{"state" => "active"}

    assert body["features"] == %{
             "server_matched_push_routing" => false,
             "priority_level_editing" => false
           }

    assert body["limits"]["answer_ui_limits"] == AttentionServer.UI1.published_limits()

    assert body["limits"]["inline_decoded_bytes_per_entry"] ==
             AttentionServer.ConversationFiles.inline_limit()

    assert body["limits"]["retention_days"] == 30

    assert body["limits"]["upload_cap_bytes_per_file"] == "none" or
             is_integer(body["limits"]["upload_cap_bytes_per_file"])

    File.write!(
      ctx.flags,
      ~s({"client_api_v2":true,"priority_level_editing":true,"progress_inbox":true})
    )

    assert %{"features" => features} = body(Reads.get_me(get("/v2/me"), ctx(ctx)))
    assert features == %{"server_matched_push_routing" => false, "priority_level_editing" => true}

    :sys.replace_state(ctx.relay, fn state ->
      engine = state.store.engine
      stopped = %{engine | delivery_states: Map.put(engine.delivery_states, "A", "STOPPED")}
      %{state | store: %{state.store | engine: stopped}}
    end)

    assert %{"account" => %{"state" => "paused"}} = body(Reads.get_me(get("/v2/me"), ctx(ctx)))
  end

  test "an ETag from before a change no longer matches, so the client gets the new body", ctx do
    first = Reads.get_inbox(get("/v2/inbox"), ctx(ctx))
    old_tag = etag(first)

    frame = %{
      "type" => "forward",
      "op_id" => op(30),
      "session_handle" => @handle,
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => "four",
        "created_at" => 30,
        "title" => "four"
      }
    }

    assert {:ok, line} =
             Relay.request_line(ctx.relay, "origin", JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(line)

    after_change = Reads.get_inbox(get("/v2/inbox", [{"if-none-match", old_tag}]), ctx(ctx))
    assert after_change.status == 200
    refute etag(after_change) == old_tag
    assert body(after_change)["rev"] > body(first)["rev"]
    assert length(body(after_change)["items"]) == length(body(first)["items"]) + 1

    settings = Reads.get_settings(get("/v2/settings"), ctx(ctx))
    # Settings did not change, so its ETag still matches.
    again = Reads.get_settings(get("/v2/settings", [{"if-none-match", etag(settings)}]), ctx(ctx))
    assert again.status == 304
  end

  test "a browser gets a file as an attachment of octet-stream; a device gets its type", ctx do
    %{"items" => items} = body(Reads.get_inbox(get("/v2/inbox"), ctx(ctx)))
    [ref] = Enum.find(items, &(&1["message"]["message_id"] == "one"))["files"]

    :sys.replace_state(ctx.relay, fn state ->
      engine = state.store.engine
      owners = Map.put(engine.owners, "A", %{key: "owner-a", revision: 2})
      %{state | store: %{state.store | engine: %{engine | owners: owners}}}
    end)

    browser = %Context{
      principal: %Principal{
        person: "A",
        kind: :browser,
        key: "owner-a",
        owner_revision: 2,
        session_id: :crypto.strong_rand_bytes(32)
      },
      relay: ctx.relay,
      request_id: "req-1",
      params: %{file_id: ref["file_id"]}
    }

    file = Reads.get_file(get("/v2/files/x"), browser)
    assert file.status == 200
    assert file.resp_body == @bytes
    assert get_resp_header(file, "content-type") == ["application/octet-stream"]
    assert get_resp_header(file, "content-disposition") == ["attachment; filename*=UTF-8''a.bin"]
    assert get_resp_header(file, "etag") == [~s("#{ref["sha256"]}")]

    device = Reads.get_file(get("/v2/files/x"), ctx(ctx, %{file_id: ref["file_id"]}))
    assert get_resp_header(device, "content-disposition") == []
    assert get_resp_header(device, "content-type") == ["application/octet-stream"]
  end

  test "a device gets its uploaded content type in a file reference and download", ctx do
    opts = Router.init(relay: ctx.relay)

    request = fn conn ->
      conn
      |> put_peer_data(%{address: {127, 0, 0, 1}, port: 1, ssl_cert: ctx.der})
      |> Router.call(opts)
    end

    inbox = request.(conn(:get, "/v2/inbox"))
    assert inbox.status == 200
    item = Enum.find(body(inbox)["items"], &(&1["message"]["message_id"] == "one"))
    item_path = "/v2/items/#{item["item_id"]}"

    upload =
      conn(:post, "/v2/files", @bytes)
      |> put_req_header("content-type", "image/png")
      |> put_req_header("content-length", Integer.to_string(byte_size(@bytes)))
      |> put_req_header("content-disposition", "attachment; filename*=UTF-8''pic.png")
      |> put_req_header("idempotency-key", op(20))
      |> request.()

    assert upload.status == 201
    assert %{"file_id" => uploaded} = body(upload)

    reply =
      conn(
        :post,
        item_path <> "/replies",
        JSON.encode!(%{"text" => "Picture", "file_ids" => [uploaded]})
      )
      |> put_req_header("content-type", "application/json")
      |> put_req_header("idempotency-key", op(21))
      |> request.()

    assert reply.status == 200

    assert %{"entry" => %{"files" => [%{"file_id" => served, "content_type" => "image/png"}]}} =
             body(reply)

    entries = request.(conn(:get, item_path <> "/entries"))
    assert entries.status == 200

    assert %{
             "entries" => [%{"files" => [%{"file_id" => ^served, "content_type" => "image/png"}]}]
           } =
             body(entries)

    download = request.(conn(:get, "/v2/files/#{served}"))
    assert download.status == 200
    assert download.resp_body == @bytes
    assert get_resp_header(download, "content-type") == ["image/png"]
    assert get_resp_header(download, "content-disposition") == []
  end
end
