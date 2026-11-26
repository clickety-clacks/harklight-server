defmodule AttentionServer.ClientAPISharedSendTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{ClientAPIFeedClient, JSON, Relay, Server, TLSFixtures}

  @moduletag :tmp_dir
  @key "10000000-0000-4000-8000-000000000001"
  @another_key "10000000-0000-4000-8000-000000000002"

  setup %{tmp_dir: dir} do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)

    server_opts = [
      store_path: Path.join(dir, "relay.sqlite3"),
      certfile: Path.join(certs, "server.pem"),
      keyfile: Path.join(certs, "server.key"),
      cacertfile: Path.join(certs, "ca.pem"),
      port: 0,
      enrollment_enabled: false,
      ip: {127, 0, 0, 1}
    ]

    %{certs: certs, server_opts: server_opts}
  end

  test "shared send binds one organization, commits once and replays the original result", ctx do
    server = start_supervised!({Server, [shared_messages: true] ++ ctx.server_opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    body = body([first, second, first])

    assert {400, %{"error" => "unsupported"}, _} =
             send_request(ctx, server, @key, body, false)

    assert :ok = Relay.replace_hosted_organization(relay, "org-a", [sender, first, second])
    assert :ok = Relay.replace_hosted_organization(relay, "org-b", [sender, first, second])

    assert {:ok, %{"principal" => %{"profile" => %{"handle" => "sam"}}}} =
             set_profile(relay, first, "Sam", "sam")

    assert {:error, :invalid_field, "handle"} =
             set_profile(relay, second, "Other Sam", "sam")

    headers = [{"harklight-features", "shared_messages"}]
    assert {200, %{"features" => %{"shared_messages" => true}}, _} =
             get_request(ctx, server, "a2", "/v2/me", [])

    assert {200, %{"items" => [], "rev" => baseline, "continuation" => continuation}, _} =
             get_request(ctx, server, "a2", "/v2/inbox", headers)

    baseline_path = "/v2/agent/delivery-baseline"

    assert {200, %{"rev" => agent_baseline, "continuation" => agent_continuation,
                  "shared_events" => true, "next_page" => shared_page}, _} =
             get_request(ctx, server, "a1", baseline_path, headers)

    assert {200, %{"rev" => ^agent_baseline, "changes" => []}, _} =
             get_request(ctx, server, "a1", baseline_path <> "?page=" <> shared_page, headers)

    assert {410, %{"error" => "cursor_expired"}, _} =
             get_request(ctx, server, "a1", baseline_path <> "?page=" <> shared_page, [])

    assert {200, %{"rev" => ^agent_baseline, "answers" => "all"} = legacy, _} =
             get_request(ctx, server, "a1", baseline_path, [])

    refute Map.has_key?(legacy, "shared_events")
    refute Map.has_key?(legacy, "continuation")

    assert {409, %{"error" => "organization_required", "organization_ids" => ids}, _} =
             send_request(ctx, server, @key, body, true)

    assert [organization, _] = ids
    selected = put_in(body, ["recipients", "organization_id"], organization)

    assert {200, committed, _} = send_request(ctx, server, @key, selected, true)
    assert %{
             "rev" => 1,
             "message" => %{"item_id" => id, "shared" => %{"organization_id" => ^organization}},
             "recipients" => [
               %{"person_id" => ^first, "status" => "reached"},
               %{"person_id" => ^second, "status" => "reached"}
             ]
           } = committed

    item_id = :sys.get_state(relay).store.engine.shared_recipients[{id, first}].item_id
    path = "/v2/items/" <> item_id
    headers = [{"harklight-features", "shared_messages"}]

    assert {200, %{"item" => %{"item_id" => ^item_id, "shared" => %{"shared_id" => ^id}}}, _} =
             get_request(ctx, server, "a2", path, headers)

    assert {200, %{"item" => %{"shared" => %{"members" => roster}}}, _} =
             get_request(ctx, server, "a2", path, headers)

    assert Enum.any?(roster, &match?(%{"person" => %{"id" => ^first, "handle" => "sam"}}, &1))

    assert {404, _, _} = get_request(ctx, server, "a2", path, [])
    assert {404, _, _} = get_request(ctx, server, "b1", path, headers)

    assert {200, %{"revision" => %{"request_revision" => 1}}, _} =
             get_request(ctx, server, "a2", path <> "/revisions/1", headers)

    assert {200, %{"entries" => [], "older" => false}, _} =
             get_request(ctx, server, "a2", path <> "/entries", headers)

    assert {200, %{"message" => %{"item_id" => ^id}}, _} =
             get_request(ctx, server, "a1", "/v2/agent/messages/" <> id, headers)

    feed_path = "/v2/changes?after=#{baseline}"
    feed_headers = [{"harklight-continuation", continuation} | headers]

    assert {200, %{"changes" => changes}, _} =
             get_request(ctx, server, "a2", feed_path, feed_headers)

    assert Enum.any?(changes, &match?(%{"kind" => "item", "data" => %{"item_id" => ^item_id}}, &1))
    assert {410, %{"error" => "cursor_expired"}, _} =
             get_request(ctx, server, "b1", feed_path, feed_headers)

    assert {200, %{"items" => [%{"item_id" => ^item_id}]}, _} =
             get_request(ctx, server, "a2", "/v2/inbox", headers)

    assert {200, %{"items" => []}, _} = get_request(ctx, server, "a2", "/v2/inbox", [])

    agent_feed = "/v2/agent/changes?after=#{agent_baseline}"
    agent_headers = [{"harklight-continuation", agent_continuation} | headers]

    assert {200, %{"changes" => [%{"kind" => "message", "data" => %{"item_id" => ^id}}]}, _} =
             get_request(ctx, server, "a1", agent_feed, agent_headers)

    assert {200, %{"changes" => [], "rev" => 1}, _} =
             get_request(ctx, server, "a1", agent_feed, [])

    assert {410, %{"error" => "cursor_expired"}, _} =
             get_request(ctx, server, "a1", agent_feed, headers)

    {ref, _} =
      Enum.find(:sys.get_state(relay).store.engine.hosted_organizations, fn {_, org} ->
        org.id == organization
      end)

    assert {200, %{"rev" => other_before}, _} =
             get_request(ctx, server, "b1", "/v2/inbox", headers)

    assert :ok = Relay.replace_hosted_organization(relay, ref, [sender, second])

    assert {200, %{"item" => %{"rank" => %{"section" => "resolved"},
                              "shared" => %{"chat_writable" => false, "can_answer" => false}}}, _} =
             get_request(ctx, server, "a2", path, headers)

    assert {200, %{"rev" => ^other_before}, _} =
             get_request(ctx, server, "b1", "/v2/inbox", headers)

    assert :ok = Relay.replace_hosted_organization(relay, ref, [sender, first, second])

    assert {200, %{"item" => %{"shared" => %{"chat_writable" => false}}}, _} =
             get_request(ctx, server, "a2", path, headers)

    assert :ok = Relay.delete_hosted_organization(relay, "org-a")
    assert :ok = Relay.delete_hosted_organization(relay, "org-b")
    assert {200, ^committed, headers} = send_request(ctx, server, @key, selected, true)
    assert {"idempotent-replayed", "true"} in headers
    assert {409, %{"error" => "idempotency_conflict"}, _} =
             send_request(ctx, server, @key, body, true)

    engine = :sys.get_state(relay).store.engine
    assert Map.keys(engine.shared_messages) == [id]
    assert Enum.sort(Map.keys(engine.shared_recipients)) == Enum.sort([{id, first}, {id, second}])
  end

  test "cross-organization refusal has no outcomes and creates no shared rows", ctx do
    server = start_supervised!({Server, [shared_messages: true] ++ ctx.server_opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    assert :ok = Relay.replace_hosted_organization(relay, "org-a", [sender, first])
    assert :ok = Relay.replace_hosted_organization(relay, "org-b", [sender, second])

    assert {403, %{"error" => "recipient_refused"} = error, _} =
             send_request(ctx, server, @another_key, body([first, second]), true)
    refute Map.has_key?(error, "recipients")
    assert :sys.get_state(relay).store.engine.shared_messages == %{}
    assert :sys.get_state(relay).store.engine.shared_recipients == %{}
  end

  test "only the sender can revise, close or retract, and frozen history keeps its cutoff", ctx do
    server = start_supervised!({Server, [shared_messages: true] ++ ctx.server_opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, first, second])

    assert {200, %{"message" => %{"item_id" => id}}, _} =
             send_request(ctx, server, @key, body([first, second]), true)

    engine = :sys.get_state(relay).store.engine
    frozen_path = "/v2/items/" <> engine.shared_recipients[{id, first}].item_id
    active_path = "/v2/items/" <> engine.shared_recipients[{id, second}].item_id
    authority = [{"harklight-features", "shared_messages"}]
    sender_path = "/v2/agent/messages/" <> id
    patch = %{"message" => %{"title" => "Revised", "created_at" => 2}}

    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, second])

    assert {404, _, _} =
             write_request(ctx, server, "b1", "PUT", sender_path, @another_key, patch,
               [{"if-match", "1"} | authority])

    assert {412, %{"error" => "precondition_failed"}, _} =
             write_request(ctx, server, "a1", "PUT", sender_path, @another_key, patch,
               [{"if-match", "3"} | authority])

    revision_headers = [{"if-match", ~s("1")} | authority]

    assert {200, %{"message" => %{"revision" => 2}} = revised, _} =
             write_request(ctx, server, "a1", "PUT", sender_path, @another_key, patch, revision_headers)

    assert {200, ^revised, replay} =
             write_request(ctx, server, "a1", "PUT", sender_path, @another_key, patch, revision_headers)

    assert {"idempotent-replayed", "true"} in replay

    assert {200, %{"revision" => %{"message" => %{"title" => "Launch"}}}, _} =
             get_request(ctx, server, "b1", active_path <> "/revisions/1", authority)

    assert {404, _, _} = get_request(ctx, server, "a2", frozen_path <> "/revisions/2", authority)

    close_key = "10000000-0000-4000-8000-000000000003"
    retract_key = "10000000-0000-4000-8000-000000000004"

    assert {200, %{"message" => %{"shared" => %{"chat_open" => false}}}, _} =
             write_request(ctx, server, "a1", "POST", sender_path <> "/chat/close", close_key, %{}, authority)

    assert {200, %{"message" => %{"terminal" => %{"state" => "retracted"}}}, _} =
             write_request(ctx, server, "a1", "POST", sender_path <> "/retract", retract_key, %{}, authority)

    assert {200, %{"item" => %{"terminal" => nil, "request_revision" => 1}}, _} =
             get_request(ctx, server, "a2", frozen_path, authority)

    assert {200, %{"item" => %{"terminal" => %{"state" => "retracted"}, "request_revision" => 2}}, _} =
             get_request(ctx, server, "b1", active_path, authority)
  end

  test "shared Chat is replayable and recoverable with a stable speaker and receipt", ctx do
    server = start_supervised!({Server, [shared_messages: true] ++ ctx.server_opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, first, second])
    headers = [{"harklight-features", "shared_messages"}]
    baseline_path = "/v2/agent/delivery-baseline"

    assert {200, %{"rev" => before, "continuation" => continuation}, _} =
             get_request(ctx, server, "a1", baseline_path, headers)

    assert {200, %{"message" => %{"item_id" => id}}, _} =
             send_request(ctx, server, @key, body([first, second]), true)

    engine = :sys.get_state(relay).store.engine
    first_path = "/v2/items/" <> engine.shared_recipients[{id, first}].item_id
    second_path = "/v2/items/" <> engine.shared_recipients[{id, second}].item_id
    sender_path = "/v2/agent/messages/" <> id
    text = %{"text" => "Thursday works."}

    assert {200, %{"entry" => %{"entry_id" => own_id, "shared_entry_id" => turn}} = accepted, _} =
             write_request(ctx, server, "a2", "POST", first_path <> "/replies", @another_key, text, headers)

    assert {200, ^accepted, replay_headers} =
             write_request(ctx, server, "a2", "POST", first_path <> "/replies", @another_key, text, headers)

    assert {"idempotent-replayed", "true"} in replay_headers

    assert {200, %{"entries" => [%{"entry_id" => local_id, "shared_entry_id" => ^turn,
                                   "speaker" => %{"kind" => "person", "id" => ^first}}]}, _} =
             get_request(ctx, server, "b1", second_path <> "/entries", headers)

    assert own_id != local_id

    assert {200, %{"item" => %{"chat" => %{"unread_replies" => 1, "last_origin_reply" => nil}}}, _} =
             get_request(ctx, server, "b1", second_path, headers)

    assert {200, %{"item" => %{"chat" => %{"unread_replies" => 0, "read_position" => ^local_id}}}, _} =
             write_request(ctx, server, "b1", "PUT", second_path <> "/read-position", @another_key,
               %{"entry_id" => local_id}, headers)

    receipt_key = "10000000-0000-4000-8000-000000000009"
    receipt_path = sender_path <> "/entries/" <> turn <> "/receipt"
    receipt = %{"outcome" => "handed"}

    assert {409, %{"error" => "not_answered"}, _} =
             write_request(ctx, server, "a1", "POST", receipt_path, receipt_key, receipt, headers)

    feed_headers = [{"harklight-continuation", continuation} | headers]
    feed_path = "/v2/agent/changes?after=#{before}"

    assert {200, %{"changes" => changes}, _} = get_request(ctx, server, "a1", feed_path, feed_headers)
    assert Enum.any?(changes, &match?(%{"kind" => "entry", "data" => %{"entry_id" => ^turn}}, &1))

    assert {200, %{"shared_events" => true, "next_page" => page}, _} =
             get_request(ctx, server, "a1", baseline_path, headers)

    assert {200, %{"changes" => [%{"kind" => "entry", "data" => %{"entry_id" => ^turn,
                         "request_revision" => 1, "speaker" => %{"id" => ^first}}}]}, _} =
             get_request(ctx, server, "a1", baseline_path <> "?page=" <> page, headers)

    assert {200, %{"status" => "recorded", "stage_outcome" => "handed"} = recorded, _} =
             write_request(ctx, server, "a1", "POST", receipt_path, receipt_key, receipt, headers)

    assert {200, ^recorded, headers_replay} =
             write_request(ctx, server, "a1", "POST", receipt_path, receipt_key, receipt, headers)
    assert {"idempotent-replayed", "true"} in headers_replay

    assert {200, %{"changes" => []}, _} =
             get_request(ctx, server, "a1", baseline_path <> "?page=" <> page, headers)

    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, second])
    reply_key = "10000000-0000-4000-8000-000000000010"

    assert {200, %{"entry" => %{"entry_id" => sender_turn, "speaker" => %{"kind" => "sender"}}}, _} =
             write_request(ctx, server, "a1", "POST", sender_path <> "/replies", reply_key,
               %{"text" => "Thanks", "notify_person_id" => second}, headers)

    assert {200, %{"entries" => [%{"shared_entry_id" => ^turn}]}, _} =
             get_request(ctx, server, "a2", first_path <> "/entries", headers)

    assert {200, %{"entries" => [%{"shared_entry_id" => ^turn}, %{"shared_entry_id" => ^sender_turn}]}, _} =
             get_request(ctx, server, "b1", second_path <> "/entries", headers)

    assert {200, %{"changes" => []}, _} = get_request(ctx, server, "a1", feed_path, [])
  end

  test "shared attachments have one durable copy and retain the frozen recipient access", ctx do
    server = start_supervised!({Server, [shared_messages: true] ++ ctx.server_opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, first, second])
    opt_in = [{"harklight-features", "shared_messages"}]
    bytes = "shared attachment contents"
    headers = [
      {"content-type", "text/plain"},
      {"content-disposition", "attachment; filename*=UTF-8''scope.txt"},
      {"idempotency-key", @another_key}
    ]

    assert %{status: 201, body: staged} = raw_request(ctx, server, "a1", "POST", "/v2/files", headers, bytes)
    assert {:ok, %{"file_id" => handle}} = JSON.decode_line(staged <> "\n")
    body = Map.put(body([first]), "file_ids", [handle])

    assert {200, %{"message" => %{"item_id" => id, "files" => [%{"file_id" => file_id, "name" => "scope.txt"}]}}, _} =
             send_request(ctx, server, @key, body, true)

    store = :sys.get_state(relay).store
    assert map_size(store.engine.uploads) == 1
    [upload] = Map.values(store.engine.uploads)
    assert upload.staged_file_id == handle

    path = "/v2/files/" <> file_id
    assert %{status: 200, body: ^bytes} = raw_request(ctx, server, "a2", "GET", path, opt_in, nil)
    assert %{status: 404} = raw_request(ctx, server, "a2", "GET", path, [], nil)
    assert %{status: 404} = raw_request(ctx, server, "b1", "GET", path, opt_in, nil)
    assert %{status: 200, body: ^bytes} = raw_request(ctx, server, "a1", "GET", path, opt_in, nil)
    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, second])
    revision = %{"message" => %{"title" => "Clear attachment", "created_at" => 2}, "file_ids" => []}
    revision_key = "10000000-0000-4000-8000-000000000006"
    assert {200, %{"message" => %{"files" => [], "revision" => 2}}, _} =
             write_request(ctx, server, "a1", "PUT", "/v2/agent/messages/" <> id, revision_key,
               revision, [{"if-match", "1"} | opt_in])

    assert %{status: 200, body: ^bytes} = raw_request(ctx, server, "a2", "GET", path, opt_in, nil)
    assert %{status: 404} = raw_request(ctx, server, "b1", "GET", path, opt_in, nil)
  end

  test "Seen is per person and revision, with one attributed Agent event", ctx do
    server = start_supervised!({Server, [shared_messages: true] ++ ctx.server_opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, first, second])
    headers = [{"harklight-features", "shared_messages"}]

    assert {200, %{"rev" => before, "continuation" => continuation}, _} =
             get_request(ctx, server, "a1", "/v2/agent/delivery-baseline", headers)

    assert {200, %{"message" => %{"item_id" => id}}, _} =
             send_request(ctx, server, @key, body([first, second]), true)

    engine = :sys.get_state(relay).store.engine
    first_path = "/v2/items/" <> engine.shared_recipients[{id, first}].item_id
    second_path = "/v2/items/" <> engine.shared_recipients[{id, second}].item_id

    assert {409, %{"error" => "stale_request"}, _} =
             write_request(ctx, server, "a2", "POST", first_path <> "/seen", @another_key,
               %{"request_revision" => 2}, headers)

    seen = %{"request_revision" => 1}
    assert {200, %{"item" => %{"seen" => true}}, _} =
             write_request(ctx, server, "a2", "POST", first_path <> "/seen", @another_key, seen, headers)

    retry_key = "10000000-0000-4000-8000-000000000003"
    assert {200, _, _} =
             write_request(ctx, server, "a2", "POST", first_path <> "/seen", retry_key, seen, headers)

    assert {200, %{"item" => %{"seen" => false, "shared" => %{"members" => members}}}, _} =
             get_request(ctx, server, "b1", second_path, headers)

    assert Enum.any?(members, &match?(%{"person" => %{"id" => ^first}, "seen_revision" => 1}, &1))
    assert {200, %{"message" => %{"seen" => true}}, _} =
             get_request(ctx, server, "a1", "/v2/agent/messages/" <> id, headers)

    assert {200, %{"changes" => changes}, _} =
             get_request(ctx, server, "a1", "/v2/agent/changes?after=#{before}",
               [{"harklight-continuation", continuation} | headers])

    assert [%{"data" => %{"request_revision" => 1, "speaker" => %{"id" => ^first}}}] =
             Enum.filter(changes, &(&1["kind"] == "seen"))
  end

  test "the default-off setting refuses the path while preserving legacy sends", ctx do
    server = start_supervised!({Server, ctx.server_opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, first, second])

    body = body([first, second])
    assert {400, %{"error" => "unsupported"}, _} = send_request(ctx, server, @key, body, true)
    assert {200, %{"message" => message}, _} =
             send_request(ctx, server, @key, Map.delete(body, "recipients"), false)
    refute Map.has_key?(message, "shared")
  end

  defp set_profile(relay, person, name, handle) do
    engine = :sys.get_state(relay).store.engine
    key = AttentionServer.Engine.hosted_owner_key(engine, person)
    {:ok, revision} = AttentionServer.Engine.owner_revision(engine, person, key)
    principal = %{kind: :browser, person: person, key: key,
                  owner_revision: revision, session_id: "fixture-browser"}
    body = %{"display_name" => name, "handle" => handle}

    GenServer.call(relay, {:client_api, AttentionServer.ClientAPI.PeopleProfiles, {principal, body}})
  end

  defp people(ctx, relay) do
    assert {:ok, sender} = Relay.hosted_account_create(relay, "account-sender")
    assert {:ok, first} = Relay.hosted_account_create(relay, "account-first")
    assert {:ok, second} = Relay.hosted_account_create(relay, "account-second")
    for {person, name} <- [{sender, "a1"}, {first, "a2"}, {second, "b1"}] do
      assert {:ok, key} =
               Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(ctx.certs, name))

      if person == sender, do: assert(:ok = Relay.setup_sender(relay, key, "app:test"))
    end

    {sender, first, second}
  end

  defp body(recipients) do
    %{
      "sender" => "app:test",
      "message" => %{"title" => "Launch", "created_at" => 1},
      "recipients" => %{"kind" => "people", "people" => recipients}
    }
  end

  defp raw_request(ctx, server, device, method, path, headers, body) do
    conn = ClientAPIFeedClient.connect(Server.port(server), ctx.certs, device)
    assert {:ok, conn, ref} = Mint.HTTP.request(conn, method, path, headers, body)
    {conn, response} = ClientAPIFeedClient.collect(conn, ref)
    Mint.HTTP.close(conn)
    response
  end

  defp get_request(ctx, server, device, path, headers) do
    conn = ClientAPIFeedClient.connect(Server.port(server), ctx.certs, device)
    assert {:ok, conn, ref} = Mint.HTTP.request(conn, "GET", path, headers, nil)
    {conn, response} = ClientAPIFeedClient.collect(conn, ref)
    Mint.HTTP.close(conn)
    assert {:ok, decoded} = JSON.decode_line(response.body <> "\n")
    {response.status, decoded, response.headers}
  end

  defp send_request(ctx, server, key, body, shared?) do
    headers = if shared?, do: [{"harklight-features", "shared_messages"}], else: []
    write_request(ctx, server, "a1", "POST", "/v2/agent/messages", key, body, headers)
  end

  defp write_request(ctx, server, device, method, path, key, body, headers) do
    headers = [{"content-type", "application/json"}, {"idempotency-key", key} | headers]
    conn = ClientAPIFeedClient.connect(Server.port(server), ctx.certs, device)
    assert {:ok, conn, ref} = Mint.HTTP.request(conn, method, path, headers, JSON.encode!(body))
    {conn, response} = ClientAPIFeedClient.collect(conn, ref)
    Mint.HTTP.close(conn)
    assert {:ok, decoded} = JSON.decode_line(response.body <> "\n")
    {response.status, decoded, response.headers}
  end
end
