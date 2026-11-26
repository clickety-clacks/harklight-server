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

  test "people manage accepted group membership and Agents only see their own joined group IDs", ctx do
    server = start_supervised!({Server, [shared_messages: true, groups: true] ++ ctx.server_opts})
    relay = Server.relay(server)
    [manager, invitee, outsider] =
      for {owner, device} <- [{"expired", "a1"}, {"future", "a2"}, {"unknown", "b1"}] do
        {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(ctx.certs, owner))
        {:ok, _key} =
          Relay.setup_device_certificate(relay, person, TLSFixtures.certificate_der!(ctx.certs, device))
        person
      end

    options = [{"harklight-features", "groups, shared_messages"}]
    assert {404, _, _} = get_request(ctx, server, "a1", "/v2/groups", [])
    assert {200, %{"groups" => [], "invitations" => []}, _} =
             get_request(ctx, server, "a1", "/v2/groups", options)

    assert {400, %{"error" => "invalid_field", "field" => "name"}, _} =
             write_request(ctx, server, "a1", "POST", "/v2/groups", op_key(84), %{"name" => " "}, options)
    create = %{"name" => "Planning"}
    assert {200, %{"group" => %{"id" => group_id, "manager" => %{"id" => ^manager}, "member" => true}}, _} =
             write_request(ctx, server, "a1", "POST", "/v2/groups", op_key(40), create, options)
    group = "/v2/groups/" <> group_id
    assert {400, %{"error" => "invalid_field", "field" => "muted"}, _} =
             write_request(ctx, server, "a1", "PUT", group <> "/mute", op_key(85), %{"muted" => "yes"}, options)

    assert {200, %{"features" => %{"shared_messages" => true, "groups" => true}}, _} =
             get_request(ctx, server, "a1", "/v2/me", [])
    shared_only = [{"harklight-features", "shared_messages"}]
    assert {200, %{"rev" => before}, _} =
             get_request(ctx, server, "a1", "/v2/inbox", options)
    assert {200, %{"continuation" => shared_token}, _} =
             get_request(ctx, server, "a1", "/v2/inbox", shared_only)
    assert {410, %{"error" => "cursor_expired"}, _} =
             get_request(ctx, server, "a1", "/v2/changes?after=#{before}",
               [{"harklight-continuation", shared_token} | options])
    sse = [{"accept", "text/event-stream"} | options]
    event_path = "/v2/changes?after=#{before}&features=shared_messages%2Cgroups&continuation=#{shared_token}"
    assert {410, %{"error" => "cursor_expired"}, _} = get_request(ctx, server, "a1", event_path, sse)
    assert {400, %{"error" => "invalid_field", "field" => "features"}, _} =
             get_request(ctx, server, "a1", event_path <> "&features=shared_messages%2Cgroups", sse)

    assert {:ok, actor} =
             AttentionServer.DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(ctx.certs, "b1"))
    assert :ok = Relay.setup_sender(relay, actor, "app:group")
    assert :ok = Relay.enable_push(relay)
    registration = %{"token" => String.duplicate("ab", 32), "environment" => "production",
                     "revision" => 1, "features" => ["shared_messages", "groups"]}
    push_path = "/v2/devices/me/push"
    assert {200, _, _} = write_request(ctx, server, "a1", "PUT", push_path, op_key(73), registration, [])
    named_only = %{registration | "token" => String.duplicate("cd", 32), "features" => ["shared_messages"]}
    assert {200, _, _} = write_request(ctx, server, "a2", "PUT", push_path, op_key(74), named_only, [])
    assert {200, %{"rev" => before, "continuation" => token}, _} =
             get_request(ctx, server, "a1", "/v2/inbox", options)
    assert {200, %{"continuation" => shared_token}, _} =
             get_request(ctx, server, "a1", "/v2/inbox", shared_only)

    bytes = "Shared group file"
    upload_headers = [
      {"content-type", "text/plain"},
      {"content-disposition", "attachment; filename*=UTF-8''group.txt"},
      {"idempotency-key", op_key(50)}
    ]
    assert %{status: 201, body: staged} =
             raw_request(ctx, server, "b1", "POST", "/v2/files", upload_headers, bytes)
    assert {:ok, %{"file_id" => handle}} = JSON.decode_line(staged <> "\n")
    send = %{"sender" => "app:group", "message" => %{"title" => "Shared planning", "created_at" => 1},
             "file_ids" => [handle], "recipients" => %{"kind" => "group", "name" => "planning"}}
    assert {404, %{"error" => "not_found"}, _} =
             write_request(ctx, server, "b1", "POST", "/v2/agent/messages", op_key(47), send, shared_only)
    assert {200, %{"message" => %{"item_id" => room, "shared" => %{"group" => %{"id" => ^group_id}},
                                  "files" => [%{"file_id" => file_id}]},
                   "recipients" => %{"group_id" => ^group_id, "kind" => "group"}} = sent, _} =
             write_request(ctx, server, "b1", "POST", "/v2/agent/messages", op_key(47), send, options)
    sender_path = "/v2/agent/messages/" <> room
    now = System.system_time(:millisecond) + 5_000
    assert {:ok, %{person: ^manager, badge: 1, presentation: :alert} = group_alert} = Relay.claim_push(relay, now)
    assert :ok = Relay.finish_push(relay, group_alert, %{state: "provider_accepted"}, now)

    baseline_path = "/v2/agent/delivery-baseline"
    assert {200, %{"shared_events" => true, "next_page" => group_page}, _} =
             get_request(ctx, server, "b1", baseline_path, options)
    assert {410, %{"error" => "cursor_expired"}, _} =
             get_request(ctx, server, "b1", baseline_path <> "?page=" <> group_page, shared_only)
    assert {404, _, _} = get_request(ctx, server, "b1", sender_path, shared_only)
    download = "/v2/files/" <> file_id
    assert %{status: 404} = raw_request(ctx, server, "a1", "GET", download, shared_only, nil)
    assert %{status: 404} = raw_request(ctx, server, "a2", "GET", download, options, nil)
    assert %{status: 200, body: ^bytes} = raw_request(ctx, server, "a1", "GET", download, options, nil)
    assert {200, %{"entry" => _}, _} =
             write_request(ctx, server, "b1", "POST", sender_path <> "/replies", op_key(48),
               %{"text" => "Before the invitation."}, options)
    assert {:ok, %{person: ^manager, locator: %{"target" => "chat"}} = chat_alert} = Relay.claim_push(relay, now)
    assert {200, _, _} =
             write_request(ctx, server, "a1", "PUT", group <> "/mute", op_key(75), %{"muted" => true}, options)
    assert :skip = Relay.refresh_push_claim(relay, chat_alert)
    assert :ok = Relay.finish_push(relay, chat_alert, %{state: "suppressed"}, now)
    assert {200, _, _} =
             write_request(ctx, server, "a1", "PUT", group <> "/mute", op_key(76), %{"muted" => false}, options)
    assert {:ok, nil} = Relay.claim_push(relay, now)

    assert {200, %{"items" => [], "counts" => %{"unresolved" => 0}}, _} =
             get_request(ctx, server, "a1", "/v2/inbox", shared_only)
    assert {200, %{"items" => [%{"item_id" => manager_item}], "counts" => %{"unresolved" => 1}}, _} =
             get_request(ctx, server, "a1", "/v2/inbox", options)
    assert {404, _, _} = get_request(ctx, server, "a1", "/v2/items/" <> manager_item, shared_only)
    assert {200, %{"changes" => shared_changes}, _} =
             get_request(ctx, server, "a1", "/v2/changes?after=#{before}",
               [{"harklight-continuation", shared_token} | shared_only])
    assert Enum.all?(shared_changes, &match?(%{"kind" => "settings"}, &1))
    assert {200, %{"changes" => group_changes}, _} =
             get_request(ctx, server, "a1", "/v2/changes?after=#{before}",
               [{"harklight-continuation", token} | options])
    assert Enum.any?(group_changes, &match?(%{"kind" => "entry"}, &1))

    assert {200, %{"invitation" => %{"id" => invitation}}, _} =
             write_request(ctx, server, "a1", "POST", group <> "/invitations", op_key(41),
               %{"person_id" => invitee}, options)
    accept = "/v2/group-invitations/" <> invitation <> "/accept"
    assert {404, _, _} = write_request(ctx, server, "b1", "POST", accept, op_key(42), %{}, options)
    assert {200, %{"groups" => [], "invitations" => [%{"id" => ^invitation}]}, _} =
             get_request(ctx, server, "a2", "/v2/groups", options)
    assert {200, %{"groups" => []}, _} = get_request(ctx, server, "a2", "/v2/agent/groups", options)

    assert {200, %{"group" => %{"id" => ^group_id, "member" => true}} = accepted, _} =
             write_request(ctx, server, "a2", "POST", accept, op_key(43), %{}, options)
    assert {200, ^accepted, replay_headers} =
             write_request(ctx, server, "a2", "POST", accept, op_key(43), %{}, options)
    assert {"idempotent-replayed", "true"} in replay_headers
    assert {200, ^sent, send_replay} =
             write_request(ctx, server, "b1", "POST", "/v2/agent/messages", op_key(47), send, options)
    assert {"idempotent-replayed", "true"} in send_replay
    assert {200, %{"items" => [%{"item_id" => invitee_item}]}, _} =
             get_request(ctx, server, "a2", "/v2/inbox", options)
    assert {200, %{"entries" => [%{"text" => "Before the invitation."}]}, _} =
             get_request(ctx, server, "a2", "/v2/items/" <> invitee_item <> "/entries", options)
    assert %{status: 200, body: ^bytes} = raw_request(ctx, server, "a2", "GET", download, options, nil)

    assert {200, %{"entry" => %{"shared_entry_id" => turn}}, _} =
             write_request(ctx, server, "a2", "POST", "/v2/items/" <> invitee_item <> "/replies",
               op_key(51), %{"text" => "New member reply."}, options)
    assert {200, %{"next_page" => named_page}, _} = get_request(ctx, server, "b1", baseline_path, shared_only)
    assert {200, %{"changes" => []}, _} =
             get_request(ctx, server, "b1", baseline_path <> "?page=" <> named_page, shared_only)
    assert {200, %{"next_page" => new_page}, _} = get_request(ctx, server, "b1", baseline_path, options)
    assert {200, %{"changes" => [%{"kind" => "entry", "data" => %{"entry_id" => ^turn}}]}, _} =
             get_request(ctx, server, "b1", baseline_path <> "?page=" <> new_page, options)

    assert {200, %{"groups" => [%{"id" => ^group_id} = discovery]}, _} =
             get_request(ctx, server, "a2", "/v2/agent/groups", options)
    refute Map.has_key?(discovery, "members")
    assert {200, %{"groups" => []}, _} = get_request(ctx, server, "b1", "/v2/agent/groups", options)
    assert outsider != invitee

    assert {404, _, _} =
             write_request(ctx, server, "a2", "PATCH", group, op_key(44), %{"name" => "New name"}, options)
    assert {200, %{"group" => %{"muted" => true}}, _} =
             write_request(ctx, server, "a2", "PUT", group <> "/mute", op_key(45), %{"muted" => true}, options)
    assert {200, %{"groups" => [%{"muted" => false}]}, _} =
             get_request(ctx, server, "a1", "/v2/groups", options)

    assert {200, %{"rev" => _}, _} =
             write_request(ctx, server, "a1", "POST", group <> "/leave", op_key(46), %{}, options)
    assert {200, %{"groups" => [%{"member" => false, "members" => [%{"id" => ^invitee}]}]}, _} =
             get_request(ctx, server, "a1", "/v2/groups", options)
    assert {200, %{"groups" => []}, _} = get_request(ctx, server, "a1", "/v2/agent/groups", options)
    assert {200, %{"entry" => _}, _} =
             write_request(ctx, server, "b1", "POST", sender_path <> "/replies", op_key(49),
               %{"text" => "After departure."}, options)
    assert {200, %{"entries" => [%{"text" => "Before the invitation."}, %{"text" => "New member reply."}]}, _} =
             get_request(ctx, server, "a1", "/v2/items/" <> manager_item <> "/entries", options)
    assert {200, %{"entries" => [%{}, %{}, %{}]}, _} =
             get_request(ctx, server, "a2", "/v2/items/" <> invitee_item <> "/entries", options)

    assert {200, %{"rev" => prior, "continuation" => agent_token}, _} =
             get_request(ctx, server, "b1", baseline_path, options)
    assert {200, %{"continuation" => agent_shared_token}, _} =
             get_request(ctx, server, "b1", baseline_path, shared_only)
    next_send = send |> Map.delete("file_ids") |> put_in(["message", "title"], "Another group request")
    assert {200, %{"message" => %{"item_id" => second_room}}, _} =
             write_request(ctx, server, "b1", "POST", "/v2/agent/messages", op_key(77), next_send, options)
    engine = :sys.get_state(relay).store.engine
    assert is_nil(AttentionServer.SharedReminders.next_deadline(engine))
    due = engine.shared_recipients[{second_room, invitee}].reminder.selection["due_at_ms"]
    state = :sys.replace_state(relay, &%{&1 | clock: fn -> due end})
    send(relay, {:bounce_due, state.bounce_deadline_generation})
    _ = :sys.get_state(relay)

    assert {200, %{"changes" => changes}, _} =
             get_request(ctx, server, "b1", "/v2/agent/changes?after=#{prior}",
               [{"harklight-continuation", agent_token} | options])
    assert [%{"data" => %{"item_id" => ^second_room, "advisory" => %{"people" => [%{"id" => ^invitee}]}}}] =
             Enum.filter(changes, &(&1["kind"] == "advisory"))
    assert {200, %{"changes" => []}, _} =
             get_request(ctx, server, "b1", "/v2/agent/changes?after=#{prior}",
               [{"harklight-continuation", agent_shared_token} | shared_only])
  end

  test "revoked sender grant blocks shared receipt replay with or without staged files", ctx do
    server = start_supervised!({Server, [shared_messages: true] ++ ctx.server_opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, first, second])
    plain = body([first])
    handle = stage_attachment(ctx, server, "a1", 80, "message.txt", "fixture")
    with_file = Map.put(plain, "file_ids", [handle])
    assert {200, _, _} = send_request(ctx, server, @key, plain, true)
    assert {200, _, _} = send_request(ctx, server, @another_key, with_file, true)

    owner = AttentionServer.Engine.hosted_owner_key(:sys.get_state(relay).store.engine, sender)
    assert {:ok, device} =
             AttentionServer.DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(ctx.certs, "a1"))
    assert :ok = Relay.owner_revoke_sender(relay, sender, owner, device, "app:test")
    assert {:ok, ^sender} = Relay.device_person(relay, device)
    assert {403, %{"error" => "forbidden"}, _} = send_request(ctx, server, @key, plain, true)
    assert {403, %{"error" => "forbidden"}, _} = send_request(ctx, server, @another_key, with_file, true)
  end

  test "retained terminal uses the same retraction and clear projection after departure", ctx do
    server = start_supervised!({Server, [shared_messages: true] ++ ctx.server_opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, first, second])
    headers = [{"harklight-features", "shared_messages"}]
    recipients = body([first, second])
    assert {200, %{"message" => %{"item_id" => retracted}}, _} =
             send_request(ctx, server, @key, recipients, true)
    {first_path, _} = recipient_paths(relay, retracted, first, second)
    answer = %{"request_revision" => 1, "comment" => "Done"}
    assert {200, _, _} = write_request(ctx, server, "a2", "POST", first_path <> "/answer", op_key(81), answer, headers)
    assert {200, _, _} =
             write_request(ctx, server, "a1", "POST", "/v2/agent/messages/" <> retracted <> "/retract", op_key(82), %{}, headers)

    assert {200, %{"message" => %{"item_id" => cleared}}, _} =
             send_request(ctx, server, @another_key, Map.put(recipients, "answer_policy", %{"kind" => "everyone"}), true)
    {clear_path, _} = recipient_paths(relay, cleared, first, second)
    assert {200, _, _} =
             write_request(ctx, server, "a1", "POST", "/v2/agent/messages/" <> cleared <> "/lifecycle",
               op_key(83), %{"action" => "cleared"}, headers)
    assert {200, %{"item" => %{"terminal" => %{"state" => "retracted"} = retracted_terminal}}, _} =
             get_request(ctx, server, "a2", first_path, headers)
    assert {200, %{"item" => %{"terminal" => %{"state" => "cleared", "by_this_person" => false} = cleared_terminal}}, _} =
             get_request(ctx, server, "a2", clear_path, headers)

    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, second])
    assert {200, %{"item" => %{"terminal" => ^retracted_terminal}}, _} =
             get_request(ctx, server, "a2", first_path, headers)
    assert {200, %{"item" => %{"terminal" => ^cleared_terminal}}, _} =
             get_request(ctx, server, "a2", clear_path, headers)
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

  test "revision and Chat attachments survive replay and use separate departure fences", ctx do
    server = start_supervised!({Server, [shared_messages: true] ++ ctx.server_opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, first, second])
    opt_in = [{"harklight-features", "shared_messages"}]

    assert {200, %{"message" => %{"item_id" => id}}, _} =
             send_request(ctx, server, @key, body([first, second]), true)

    sender_path = "/v2/agent/messages/" <> id
    revision_bytes = "new revision attachment"
    handle = stage_attachment(ctx, server, "a1", 20, "revision.txt", revision_bytes)
    revision = %{"message" => %{"title" => "Updated", "created_at" => 2}, "file_ids" => [handle]}
    revise_headers = [{"if-match", "1"} | opt_in]
    assert {200, %{"message" => %{"revision" => 2, "files" => [%{"file_id" => file_id}]}}
                = revision_result, _} =
             write_request(ctx, server, "a1", "PUT", sender_path, op_key(21), revision, revise_headers)
    assert {200, ^revision_result, replay_headers} =
             write_request(ctx, server, "a1", "PUT", sender_path, op_key(21), revision, revise_headers)
    assert {"idempotent-replayed", "true"} in replay_headers
    assert %{status: 200, body: ^revision_bytes} =
             raw_request(ctx, server, "a2", "GET", "/v2/files/" <> file_id, opt_in, nil)

    {first_path, second_path} = recipient_paths(relay, id, first, second)
    person_bytes = "first chat attachment"
    person_handle = stage_attachment(ctx, server, "a2", 22, "prior.txt", person_bytes)
    reply_body = %{"text" => "Before departure", "file_ids" => [person_handle]}
    assert {200, %{"entry" => %{"files" => [%{"file_id" => prior_id}]}} = reply_result, _} =
             write_request(ctx, server, "a2", "POST", first_path <> "/replies", op_key(23), reply_body, opt_in)
    assert {200, ^reply_result, headers} =
             write_request(ctx, server, "a2", "POST", first_path <> "/replies", op_key(23), reply_body, opt_in)
    assert {"idempotent-replayed", "true"} in headers
    assert %{status: 200, body: ^person_bytes} =
             raw_request(ctx, server, "a1", "GET", "/v2/files/" <> prior_id, opt_in, nil)

    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, second])
    later_bytes = "second chat attachment"
    later_handle = stage_attachment(ctx, server, "a1", 24, "later.txt", later_bytes)
    later_body = %{"text" => "After departure", "file_ids" => [later_handle]}
    assert {200, %{"entry" => %{"request_revision" => 2, "files" => [%{"file_id" => later_id}]}}, _} =
             write_request(ctx, server, "a1", "POST", sender_path <> "/replies", op_key(25), later_body, opt_in)

    assert %{status: 200, body: ^person_bytes} =
             raw_request(ctx, server, "a2", "GET", "/v2/files/" <> prior_id, opt_in, nil)
    assert %{status: 404} = raw_request(ctx, server, "a2", "GET", "/v2/files/" <> later_id, opt_in, nil)
    assert %{status: 404} = raw_request(ctx, server, "b1", "GET", "/v2/files/" <> later_id, [], nil)
    assert %{status: 200, body: ^later_bytes} =
             raw_request(ctx, server, "b1", "GET", "/v2/files/" <> later_id, opt_in, nil)
    assert {200, _, _} = get_request(ctx, server, "b1", second_path, opt_in)

    assert map_size(:sys.get_state(relay).store.engine.uploads) == 3
    assert :ok = stop_supervised(Server)
    restarted = start_supervised!({Server, [shared_messages: true] ++ ctx.server_opts})
    assert %{status: 200, body: ^later_bytes} =
             raw_request(ctx, restarted, "b1", "GET", "/v2/files/" <> later_id, opt_in, nil)
    assert %{status: 404} =
             raw_request(ctx, restarted, "a2", "GET", "/v2/files/" <> later_id, opt_in, nil)
  end

  defp stage_attachment(ctx, server, device, key, name, bytes) do
    headers = [
      {"content-type", "text/plain"},
      {"content-disposition", "attachment; filename*=UTF-8''" <> name},
      {"idempotency-key", op_key(key)}
    ]
    assert %{status: 201, body: staged} = raw_request(ctx, server, device, "POST", "/v2/files", headers, bytes)
    assert {:ok, %{"file_id" => handle}} = JSON.decode_line(staged <> "\n")
    handle
  end

  test "push capability belongs to one binding revision, including downgrade and token rebind", ctx do
    server = start_supervised!({Server, [shared_messages: true] ++ ctx.server_opts})
    relay = Server.relay(server)
    {sender, first, _second} = people(ctx, relay)
    assert :ok = Relay.enable_push(relay)
    assert {:ok, sibling} =
             Relay.setup_device_certificate(relay, first, TLSFixtures.certificate_der!(ctx.certs, "unknown"))
    assert {:ok, opted_in} =
             AttentionServer.DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(ctx.certs, "a2"))

    path = "/v2/devices/me/push"
    token = String.duplicate("ab", 32)
    registration = %{"token" => token, "environment" => "production", "revision" => 1}
    feature_registration = Map.put(registration, "features", ["shared_messages"])
    assert {400, %{"error" => "unsupported"}, _} =
             write_request(ctx, server, "a2", "PUT", path, op_key(30), feature_registration, [])

    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, first])
    assert {200, shared_result, _} =
             write_request(ctx, server, "a2", "PUT", path, op_key(30), feature_registration, [])
    assert {200, ^shared_result, replay_headers} =
             write_request(ctx, server, "a2", "PUT", path, op_key(30), feature_registration, [])
    assert {"idempotent-replayed", "true"} in replay_headers
    assert {409, %{"error" => "idempotency_conflict"}, _} =
             write_request(ctx, server, "a2", "PUT", path, op_key(30), registration, [])
    assert {412, %{"error" => "precondition_failed"}, _} =
             write_request(ctx, server, "a2", "PUT", path, op_key(31), registration, [])

    legacy = %{registration | "token" => String.duplicate("cd", 32)}
    assert {200, _, _} = write_request(ctx, server, "unknown", "PUT", path, op_key(32), legacy, [])
    bindings = :sys.get_state(relay).store.engine.push_bindings
    assert bindings[{first, opted_in}].shared_messages
    refute bindings[{first, sibling}][:shared_messages]

    assert {200, _, _} = send_request(ctx, server, @key, body([first]), true)

    assert :ok = Relay.setup_sender(relay, opted_in, "app:individual")
    individual = %{
      "sender" => "app:individual",
      "message" => %{"title" => "Individual request", "created_at" => 1}
    }
    assert {200, _, _} =
             write_request(ctx, server, "a2", "POST", "/v2/agent/messages", op_key(36), individual, [])

    now = System.system_time(:millisecond) + 1_000
    claims = for _ <- 1..2 do
      assert {:ok, %{device: device, badge: badge} = claim} = Relay.claim_push(relay, now)
      assert :ok = Relay.finish_push(relay, claim, %{state: "provider_accepted"}, now)
      {device, badge}
    end
    assert Map.new(claims) == %{opted_in => 2, sibling => 1}

    assert {200, _, _} =
             write_request(ctx, server, "a2", "PUT", path, op_key(33), %{registration | "revision" => 2}, [])
    refute :sys.get_state(relay).store.engine.push_bindings[{first, opted_in}][:shared_messages]

    assert {200, _, _} =
             write_request(ctx, server, "a2", "PUT", path, op_key(34), %{feature_registration | "revision" => 3}, [])
    assert {200, _, _} =
             write_request(ctx, server, "unknown", "PUT", path, op_key(35), %{registration | "revision" => 2}, [])

    bindings = :sys.get_state(relay).store.engine.push_bindings
    refute bindings[{first, opted_in}].active
    refute bindings[{first, opted_in}][:shared_messages]
    assert bindings[{first, sibling}].active
    refute bindings[{first, sibling}][:shared_messages]
  end

  test "shared pushes survive restart and drop claimed alerts on departure", ctx do
    opts = [shared_messages: true] ++ ctx.server_opts
    server = start_supervised!({Server, opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, first, second])
    assert :ok = Relay.enable_push(relay)
    headers = [{"harklight-features", "shared_messages"}]
    path = "/v2/devices/me/push"
    registration = %{"token" => String.duplicate("ab", 32), "environment" => "production",
                     "revision" => 1, "features" => ["shared_messages"]}
    assert {200, _, _} =
             write_request(ctx, server, "a2", "PUT", path, op_key(52), registration, [])
    legacy = registration |> Map.delete("features") |> Map.put("token", String.duplicate("cd", 32))
    assert {200, _, _} =
             write_request(ctx, server, "b1", "PUT", path, op_key(53), legacy, [])

    assert {200, %{"message" => %{"item_id" => room}}, _} =
             send_request(ctx, server, @key, body([first, second]), true)
    assert :ok = stop_supervised(Server)
    server = start_supervised!({Server, opts})
    relay = Server.relay(server)
    {first_path, second_path} = recipient_paths(relay, room, first, second)
    first_item = List.last(String.split(first_path, "/"))

    now = System.system_time(:millisecond) + 1_000
    assert {:ok, %{stream: :shared, badge: 1, person: ^first, locator: %{"item_id" => ^first_item}} = request} =
             Relay.claim_push(relay, now)
    assert {:ok, %{locator: %{"item_id" => ^first_item}, badge: 1}} = Relay.refresh_push_claim(relay, request)
    assert :ok = Relay.finish_push(relay, request, %{state: "provider_accepted"}, now)
    assert {:ok, nil} = Relay.claim_push(relay, now)

    assert {200, _, _} =
             write_request(ctx, server, "b1", "POST", second_path <> "/replies", op_key(54),
               %{"text" => "Shared reply"}, headers)
    assert {:ok, %{stream: :shared, badge: 1, locator: %{"target" => "chat", "item_id" => ^first_item}} = chat} =
             Relay.claim_push(relay, now)
    assert {:ok, %{locator: %{"target" => "chat", "item_id" => ^first_item}}} = Relay.refresh_push_claim(relay, chat)
    assert %{"attention" => %{"locator" => %{"target" => "chat"}}} = AttentionServer.APNS.request_payload(chat)

    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, second])
    assert :skip = Relay.refresh_push_claim(relay, chat)
    assert :ok = Relay.finish_push(relay, chat, %{state: "suppressed"}, now)
    assert_departure_badge(relay, first, now)
  end

  test "departure clears a badge even when the original alert is already accepted", ctx do
    server = start_supervised!({Server, [shared_messages: true] ++ ctx.server_opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, first, second])
    assert :ok = Relay.enable_push(relay)
    binding = %{"token" => String.duplicate("ab", 32), "environment" => "production",
                "revision" => 1, "features" => ["shared_messages"]}
    assert {200, _, _} =
             write_request(ctx, server, "a2", "PUT", "/v2/devices/me/push", op_key(86), binding, [])
    assert {200, _, _} = send_request(ctx, server, @key, body([first]), true)
    now = System.system_time(:millisecond) + 1_000
    assert {:ok, %{badge: 1, presentation: :alert} = request} = Relay.claim_push(relay, now)
    assert :ok = Relay.finish_push(relay, request, %{state: "provider_accepted"}, now)
    assert {:ok, nil} = Relay.claim_push(relay, now)

    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, second])
    assert_departure_badge(relay, first, now)
  end

  defp assert_departure_badge(relay, person, now) do
    assert {:ok, %{stream: :shared, person: ^person, badge: 0, presentation: :background,
                   locator: nil} = refresh} = Relay.claim_push(relay, now)
    assert {:ok, current} = Relay.refresh_push_claim(relay, refresh)
    assert %{"aps" => %{"badge" => 0}, "attention" => data} = AttentionServer.APNS.request_payload(current)
    refute Map.has_key?(data, "locator")
    assert :ok = Relay.finish_push(relay, refresh, %{state: "provider_accepted"}, now)
    assert {:ok, nil} = Relay.claim_push(relay, now)
  end

  test "a new cutoff suppresses persisted reminders after Server restart", ctx do
    opts = [shared_messages: true] ++ ctx.server_opts
    server = start_supervised!({Server, opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, first, second])
    assert {200, %{"message" => %{"item_id" => room}}, _} =
             send_request(ctx, server, @key, body([first]), true)
    engine = :sys.get_state(relay).store.engine
    %{"created_at_ms" => accepted} = engine.shared_messages[room]
    due = AttentionServer.SharedReminders.next_deadline(engine)
    assert is_integer(due)
    set_cutoff(accepted + 1)
    assert :ok = stop_supervised(Server)

    restarted = start_supervised!({Server, opts})
    relay = Server.relay(restarted)
    assert :sys.get_state(relay).bounce_deadline_timer == nil
    state = :sys.replace_state(relay, &%{&1 | clock: fn -> due end})
    send(relay, {:bounce_due, state.bounce_deadline_generation})
    engine = :sys.get_state(relay).store.engine
    refute Map.has_key?(engine.shared_recipients[{room, first}].reminder, :outcome)
    refute Map.has_key?(engine.shared_messages[room], :shared_advisories)
  end

  defp set_cutoff(at) do
    env = "ATTENTION_SERVER_BOUNCE_NOT_BEFORE"
    previous = System.get_env(env)
    on_exit(fn -> if previous, do: System.put_env(env, previous), else: System.delete_env(env) end)
    System.put_env(env, at |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601())
  end

  test "due reminders group unseen people and preserve the separate opened reminder", ctx do
    server = start_supervised!({Server, [shared_messages: true] ++ ctx.server_opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, first, second])
    headers = [{"harklight-features", "shared_messages"}]
    baseline = "/v2/agent/delivery-baseline"
    assert {200, %{"rev" => prior, "continuation" => token}, _} =
             get_request(ctx, server, "a1", baseline, headers)
    assert {200, %{"message" => %{"item_id" => room}}, _} =
             send_request(ctx, server, @key, body([sender, first, second]), true)
    {first_path, second_path} = recipient_paths(relay, room, first, second)
    assert {200, _, _} =
             write_request(ctx, server, "a2", "POST", first_path <> "/seen", op_key(62),
               %{"request_revision" => 1}, headers)
    engine = :sys.get_state(relay).store.engine
    due = AttentionServer.SharedReminders.next_deadline(engine)
    assert is_integer(due)
    state = :sys.replace_state(relay, &%{&1 | clock: fn -> due end})
    send(relay, {:bounce_due, state.bounce_deadline_generation})
    state = :sys.get_state(relay)
    assert AttentionServer.SharedReminders.next_deadline(state.store.engine) == nil

    sender_path = "/v2/agent/messages/" <> room
    assert {200, %{"message" => %{"advisories" => advisories}}, _} =
             get_request(ctx, server, "a1", sender_path, headers)
    by_reason = Map.new(advisories, &{&1["advisory_type"], &1})
    assert %{"people" => [%{"id" => ^first}]} = by_reason["soft_bounce"]
    assert %{"people" => unseen, "advisory_id" => advisory_id} = by_reason["unseen_interval_elapsed"]
    assert MapSet.new(unseen, & &1["id"]) == MapSet.new([sender, second])
    assert {200, %{"item" => %{"rank" => %{"section" => "resolved"}}}, _} =
             get_request(ctx, server, "b1", second_path, headers)

    assert {200, %{"changes" => changes}, _} =
             get_request(ctx, server, "a1", "/v2/agent/changes?after=#{prior}",
               [{"harklight-continuation", token} | headers])
    assert Enum.count(changes, &(&1["kind"] == "advisory")) == 2
    ack = %{"advisory_id" => advisory_id}
    assert {200, %{"advisory_id" => ^advisory_id, "rev" => _, "delivered" => true} = acknowledged, _} =
             write_request(ctx, server, "a1", "POST", sender_path <> "/advisories/ack", op_key(63), ack, headers)
    assert {200, ^acknowledged, replay} =
             write_request(ctx, server, "a1", "POST", sender_path <> "/advisories/ack", op_key(63), ack, headers)
    assert {"idempotent-replayed", "true"} in replay
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

  test "one first answer settles the request, survives replay and leaves Chat open", ctx do
    server = start_supervised!({Server, [shared_messages: true] ++ ctx.server_opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, first, second])
    headers = [{"harklight-features", "shared_messages"}]
    baseline_path = "/v2/agent/delivery-baseline"
    assert {200, %{"rev" => before, "continuation" => token}, _} =
             get_request(ctx, server, "a1", baseline_path, headers)

    assert {200, %{"message" => %{"item_id" => id}}, _} =
             send_request(ctx, server, @key, body([first, second]), true)

    {first_path, second_path} = recipient_paths(relay, id, first, second)
    body = %{"request_revision" => 1, "comment" => "Thursday"}

    replies =
      [{first, "a2", first_path}, {second, "b1", second_path}]
      |> Task.async_stream(fn {person, device, path} ->
        {person, device, path,
         write_request(ctx, server, device, "POST", path <> "/answer", @another_key, body, headers)}
      end)
      |> Enum.map(fn {:ok, result} -> result end)

    assert {answerer, device, path, {200, saved, _}} = Enum.find(replies, &(elem(elem(&1, 3), 0) == 200))
    assert %{"item" => %{"terminal" => %{"answer_id" => answer_id, "by_this_person" => true},
                         "shared" => %{"chat_writable" => true}}} = saved
    assert {_, other_device, other_path,
            {409, %{"error" => "already_answered",
                    "terminal" => %{"answer_id" => ^answer_id, "by_this_person" => false,
                                    "speaker" => %{"id" => ^answerer}}}, _}} =
             Enum.find(replies, &(elem(elem(&1, 3), 0) == 409))

    assert {200, ^saved, response_headers} =
             write_request(ctx, server, device, "POST", path <> "/answer", @another_key, body, headers)
    assert {"idempotent-replayed", "true"} in response_headers

    chat_key = op_key(3)
    assert {200, %{"entry" => %{"shared_entry_id" => after_answer}}, _} =
             write_request(ctx, server, other_device, "POST", other_path <> "/replies",
               chat_key, %{"text" => "Can we confirm?"}, headers)

    assert {200, %{"changes" => changes}, _} =
             get_request(ctx, server, "a1", "/v2/agent/changes?after=#{before}",
               [{"harklight-continuation", token} | headers])
    assert [%{"data" => %{"answer_id" => ^answer_id, "request_revision" => 1,
                         "terminal" => %{"speaker" => %{"id" => ^answerer}}}}] =
             Enum.filter(changes, &(&1["kind"] == "answer"))
    assert Enum.any?(changes, &match?(%{"kind" => "entry", "data" => %{"entry_id" => ^after_answer}}, &1))

    assert {200, %{"next_page" => page}, _} = get_request(ctx, server, "a1", baseline_path, headers)
    assert {200, %{"changes" => [%{"kind" => "answer", "data" => %{"answer_id" => ^answer_id}},
                                %{"kind" => "entry", "data" => %{"entry_id" => ^after_answer}}]}, _} =
             get_request(ctx, server, "a1", baseline_path <> "?page=" <> page, headers)

    receipt = "/v2/agent/messages/" <> id <> "/answers/" <> answer_id <> "/receipt"
    handoff = %{"outcome" => "handed"}
    assert {200, %{"status" => "recorded", "stage_outcome" => "handed"} = acknowledged, _} =
             write_request(ctx, server, "a1", "POST", receipt, op_key(4), handoff, headers)
    assert {200, ^acknowledged, replay_headers} =
             write_request(ctx, server, "a1", "POST", receipt, op_key(4), handoff, headers)
    assert {"idempotent-replayed", "true"} in replay_headers
  end

  test "named and everyone policies preserve decision authority and personal answers", ctx do
    server = start_supervised!({Server, [shared_messages: true] ++ ctx.server_opts})
    relay = Server.relay(server)
    {sender, first, second} = people(ctx, relay)
    assert :ok = Relay.replace_hosted_organization(relay, "org", [sender, first, second])
    headers = [{"harklight-features", "shared_messages"}]

    named = Map.put(body([first, second]), "answer_policy", %{"kind" => "named", "people" => [first]})
    assert {200, %{"message" => %{"item_id" => named_id}}, _} =
             send_request(ctx, server, @key, named, true)
    {named_first, named_second} = recipient_paths(relay, named_id, first, second)

    answer = %{"request_revision" => 1, "comment" => "Accepted"}
    assert {403, %{"error" => "forbidden"}, _} =
             write_request(ctx, server, "b1", "POST", named_second <> "/answer", @another_key, answer, headers)
    assert {200, %{"item" => %{"terminal" => %{"speaker" => %{"id" => ^first}}}}, _} =
             write_request(ctx, server, "a2", "POST", named_first <> "/answer", @another_key, answer, headers)

    everyone = Map.put(body([first, second]), "answer_policy", %{"kind" => "everyone"})
    assert {200, %{"message" => %{"item_id" => everyone_id}}, _} =
             send_request(ctx, server, op_key(3), everyone, true)
    {everyone_first, everyone_second} = recipient_paths(relay, everyone_id, first, second)

    assert {200, %{"item" => %{"terminal" => %{"by_this_person" => true}}}, _} =
             write_request(ctx, server, "a2", "POST", everyone_first <> "/answer", op_key(4), answer, headers)
    assert {200, %{"item" => %{"terminal" => nil,
                              "shared" => %{"can_answer" => true, "answers" => [%{"person_id" => ^first}]}}}, _} =
             get_request(ctx, server, "b1", everyone_second, headers)

    clear_path = "/v2/agent/messages/" <> everyone_id <> "/lifecycle"
    assert {200, %{"message" => %{"terminal" => %{"state" => "cleared"}}}, _} =
             write_request(ctx, server, "a1", "POST", clear_path, op_key(5), %{"action" => "cleared"}, headers)
    assert {200, %{"item" => %{"terminal" => %{"state" => "answered"}}}, _} =
             get_request(ctx, server, "a2", everyone_first, headers)
    assert {200, %{"item" => %{"terminal" => %{"state" => "cleared", "by_this_person" => false},
                              "shared" => %{"chat_writable" => true}}}, _} =
             get_request(ctx, server, "b1", everyone_second, headers)
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

  defp recipient_paths(relay, id, first, second) do
    rows = :sys.get_state(relay).store.engine.shared_recipients
    {"/v2/items/" <> rows[{id, first}].item_id, "/v2/items/" <> rows[{id, second}].item_id}
  end

  defp op_key(number) do
    "10000000-0000-4000-8000-" <> String.pad_leading(to_string(number), 12, "0")
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
