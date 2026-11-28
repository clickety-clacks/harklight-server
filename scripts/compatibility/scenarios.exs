defmodule AttentionServer.CompatibilityScenarios do
  @moduledoc """
  Storage-independent /v2 scenarios. Provision through Relay APIs; observe through
  Router/Plug. The manifest retains assigned IDs and full resource fields for
  same-history verification after restart, upgrade, or migration.
  """
  alias AttentionServer.{JSON, Relay, TLSFixtures}
  alias AttentionServer.ClientAPI.Router
  @session "20000000-0000-4000-8000-000000000001"
  @features [{"harklight-features", "shared_messages,groups"}]
  @attachment "Synthetic attachment: preserved bytes.\n"

  def fixture(root) do
    :ok = File.mkdir(root)
    certs = TLSFixtures.create!()
    File.cp_r!(certs, Path.join(root, "certs"))
    File.rm_rf!(certs)
    open_fixture(root, System.system_time(:millisecond))
  end

  def open_fixture(root, now) do
    {:ok, clock} = Agent.start_link(fn -> now end)

    %{
      root: root,
      clock: clock,
      ders:
        Map.new([agent: "a1", phone: "a2", tablet: "unknown", other: "b1"], fn {role, name} ->
          {role, TLSFixtures.certificate_der!(Path.join(root, "certs"), name)}
        end)
    }
  end

  def start(fixture, opts \\ []) do
    Relay.start_link(
      Keyword.merge(
        [
          store_path: Path.join(fixture.root, "relay.sqlite3"),
          agent_fixture_mode: true,
          shared_messages: true,
          groups: true,
          clock: fn -> Agent.get(fixture.clock, & &1) end
        ],
        opts
      )
    )
  end

  def provision(relay, fixture) do
    {:ok, person} =
      Relay.operator_bootstrap(
        relay,
        TLSFixtures.certificate_der!(Path.join(fixture.root, "certs"), "expired")
      )

    {:ok, other} =
      Relay.operator_bootstrap(
        relay,
        TLSFixtures.certificate_der!(Path.join(fixture.root, "certs"), "future")
      )

    for {role, der} <- fixture.ders do
      account = if role == :other, do: other, else: person
      {:ok, key} = Relay.setup_device_certificate(relay, account, der)
      if role == :agent, do: :ok = Relay.setup_sender(relay, key, "app:compatibility")
    end

    %{person: person, other: other}
  end

  def run(relay, fixture) do
    %{person: person, other: other} = provision(relay, fixture)
    ctx = {relay, fixture}

    group =
      write(ctx, :phone, :post, "/v2/groups", %{"name" => "Synthetic planning"}, 1, @features)[
        "group"
      ]

    invitation =
      write(
        ctx,
        :phone,
        :post,
        "/v2/groups/#{group["id"]}/invitations",
        %{"person_id" => other},
        2,
        @features
      )["invitation"]

    write(
      ctx,
      :other,
      :post,
      "/v2/group-invitations/#{invitation["id"]}/accept",
      %{},
      3,
      @features
    )

    upload =
      request(ctx, :agent, :post, "/v2/files", @attachment, [
        {"content-type", "text/plain"},
        {"content-length", to_string(byte_size(@attachment))},
        {"content-disposition", "attachment; filename*=UTF-8''synthetic.txt"},
        {"idempotency-key", op(10)}
      ])
      |> body!(201)

    send_body = %{
      "sender" => "app:compatibility",
      "session" => @session,
      "message" => %{"title" => "Original question", "created_at" => 10},
      "file_ids" => [upload["file_id"]]
    }

    sent = write(ctx, :agent, :post, "/v2/agent/messages", send_body, 11)
    item = sent["message"]["item_id"]
    check!(is_binary(item), "send assigned an item ID")

    retry =
      request(ctx, :agent, :post, "/v2/agent/messages", JSON.encode!(send_body), json_headers(11))

    check!(body!(retry, 200)["message"]["item_id"] == item, "send retry kept the ID")

    check!(
      Plug.Conn.get_resp_header(retry, "idempotent-replayed") == ["true"],
      "send retry marked replayed"
    )

    for n <- 12..14 do
      write(
        ctx,
        :agent,
        :post,
        "/v2/agent/messages",
        %{
          "sender" => "app:compatibility",
          "session" => @session,
          "message" => %{"title" => "History #{n}", "created_at" => n}
        },
        n
      )
    end

    baseline = get(ctx, :phone, "/v2/inbox?limit=200")
    saved_cursor = baseline["rev"]

    revised =
      write(
        ctx,
        :agent,
        :put,
        "/v2/agent/messages/#{item}",
        %{
          "message" => %{"title" => "Revised question", "created_at" => 10},
          "file_ids" => [upload["file_id"]]
        },
        20,
        [{"if-match", "1"}]
      )

    check!(revised["message"]["revision"] == 2, "revision incremented")

    person_reply =
      write(ctx, :phone, :post, "/v2/items/#{item}/replies", %{"text" => "Please explain"}, 21)

    entry = person_reply["entry"]["entry_id"]
    check!(is_binary(entry), "reply assigned an entry ID")

    agent_feed =
      get(ctx, :agent, "/v2/agent/changes?after=0&session=#{@session}", [
        {"harklight-ack2", "true"}
      ])

    check!(
      Enum.any?(agent_feed["changes"], &(get_in(&1, ["data", "entry_id"]) == entry)),
      "agent feed includes reply"
    )

    receipt_path = "/v2/agent/messages/#{item}/entries/#{entry}/receipt"
    receipt_body = %{"outcome" => "handed", "agent" => "Synthetic agent"}
    receipt = write(ctx, :agent, :post, receipt_path, receipt_body, 22)

    check!(
      receipt["status"] == "recorded" and receipt["stage_outcome"] == "handed",
      "handoff recorded"
    )

    write(
      ctx,
      :agent,
      :post,
      "/v2/agent/messages/#{item}/replies",
      %{"text" => "Synthetic explanation", "in_reply_to" => entry},
      23
    )

    entries = get(ctx, :phone, "/v2/items/#{item}/entries")["entries"]
    handed = Enum.find(entries, &(&1["entry_id"] == entry))
    check!(handed["delivery"]["outcome"] == "handed", "handoff outcome visible to person")
    agent_entry = Enum.find(entries, &(&1["author"] == "origin" and &1["kind"] == "reply"))

    check!(
      is_map(agent_entry) and agent_entry["text"] == "Synthetic explanation",
      "agent reply present"
    )

    write(
      ctx,
      :tablet,
      :put,
      "/v2/items/#{item}/read-position",
      %{"entry_id" => agent_entry["entry_id"]},
      24
    )

    write(
      ctx,
      :phone,
      :post,
      "/v2/items/#{item}/answer",
      %{"request_revision" => 2, "comment" => "Approved synthetically"},
      25
    )

    # End the synthetic session before taking durable snapshots. Active feed
    # availability is intentionally transient and resets on a node restart.
    write(ctx, :agent, :post, "/v2/agent/sessions/#{@session}/end", %{}, 26)
    current = get(ctx, :phone, "/v2/items/#{item}")["item"]
    check!(current["message"]["title"] == "Revised question", "revised content retained")

    check!(
      current["chat"]["read_position"] == agent_entry["entry_id"],
      "read position shared across devices"
    )

    check!(
      current["terminal"]["state"] == "answered" and
        current["terminal"]["answer"]["text"] == "Approved synthetically",
      "answer retained"
    )

    final = get(ctx, :phone, "/v2/inbox?limit=200")
    paginated = pages(ctx, "/v2/inbox?limit=1", [], 0)
    check!(paginated == final["items"], "paginated baseline equals full baseline")
    catchup = get(ctx, :tablet, "/v2/changes?after=#{saved_cursor}&limit=1000")
    check!(length(catchup["changes"]) > 0, "saved cursor catches up on another device")

    resumed =
      get(ctx, :phone, "/v2/changes?after=0&limit=1000", [
        {"last-event-id", to_string(saved_cursor)}
      ])

    check!(resumed == catchup, "Last-Event-ID resumes the saved cursor")

    future =
      request(ctx, :phone, :get, "/v2/changes?after=#{final["rev"] + 1}", "", []) |> body!(410)

    check!(future["error"] == "cursor_expired", "future cursor is 410")
    file = hd(revised["message"]["files"])["file_id"]
    downloaded = request(ctx, :tablet, :get, "/v2/files/#{file}", "", [])

    check!(
      downloaded.status == 200 and downloaded.resp_body == @attachment,
      "attachment bytes retained"
    )

    observations =
      for {role, path, headers} <- [
            {:phone, "/v2/me", []},
            {:other, "/v2/me", []},
            {:phone, "/v2/groups", @features},
            {:other, "/v2/groups", @features},
            {:phone, "/v2/inbox?limit=200", []},
            {:tablet, "/v2/inbox?limit=200", []},
            {:phone, "/v2/items/#{item}", []},
            {:phone, "/v2/items/#{item}/entries", []},
            {:agent, "/v2/agent/messages/#{item}", []},
            {:phone, "/v2/items/#{item}/revisions/1", []},
            {:tablet, "/v2/changes?after=#{saved_cursor}&limit=1000", []}
          ] do
        %{
          "role" => to_string(role),
          "path" => path,
          "headers" => Enum.map(headers, &Tuple.to_list/1),
          "body" => get(ctx, role, path, headers)
        }
      end

    # Capture retry projections only after the history reaches its final state.
    # The original requests/keys are retained and must remain replayable by a candidate.
    send_replay = write(ctx, :agent, :post, "/v2/agent/messages", send_body, 11)
    receipt_replay = write(ctx, :agent, :post, receipt_path, receipt_body, 22)

    %{
      "schema" => 2,
      "seed" => "compatibility-v2",
      "clock_ms" => Agent.get(fixture.clock, & &1),
      "people" => [person, other],
      "organization_id" => group["organization_id"],
      "group_id" => group["id"],
      "session" => @session,
      "item_id" => item,
      "entry_id" => entry,
      "saved_cursor" => saved_cursor,
      "baseline_before" => baseline,
      "baseline_final" => final,
      "catchup" => catchup,
      "expected" => %{
        "title" => "Revised question",
        "reply" => "Please explain",
        "agent_reply" => "Synthetic explanation",
        "answer" => "Approved synthetically",
        "handoff" => "handed"
      },
      "observations" => observations,
      "attachment" => %{
        "file_id" => file,
        "bytes" => @attachment,
        "sha256" => Base.encode16(:crypto.hash(:sha256, @attachment), case: :lower)
      },
      "send_retry" => %{
        "path" => "/v2/agent/messages",
        "body" => send_body,
        "key" => op(11),
        "response" => send_replay
      },
      "retry" => %{
        "path" => receipt_path,
        "body" => receipt_body,
        "key" => op(22),
        "committed_response" => receipt,
        "response" => receipt_replay
      }
    }
  end

  def verify(relay, fixture, manifest) do
    check!(
      manifest["schema"] == 2,
      "verification requires schema 2; seed a fresh baseline with this fixture version"
    )

    ctx = {relay, fixture}

    for row <- manifest["observations"] do
      role = role!(row["role"])
      headers = Enum.map(row["headers"], &List.to_tuple/1)
      actual = get(ctx, role, row["path"], headers)

      check!(
        actual == row["body"],
        "manifest differs at #{row["role"]} #{row["path"]}: #{inspect(differences(row["body"], actual))}"
      )
    end

    file = manifest["attachment"]
    conn = request(ctx, :tablet, :get, "/v2/files/#{file["file_id"]}", "", [])
    check!(conn.status == 200 and conn.resp_body == file["bytes"], "manifest attachment differs")
    # All behavior gates run on the candidate, not just on the seeding version.
    ordered = pages(ctx, "/v2/inbox?limit=1", [], 0)
    check!(ordered == manifest["baseline_final"]["items"], "candidate paginated baseline differs")

    resumed =
      get(ctx, :phone, "/v2/changes?after=0&limit=1000", [
        {"last-event-id", to_string(manifest["saved_cursor"])}
      ])

    check!(resumed == manifest["catchup"], "candidate Last-Event-ID resume differs")

    for {name, retry} <- [{"send", manifest["send_retry"]}, {"receipt", manifest["retry"]}] do
      verify_retry(ctx, name, retry)
    end

    :ok
  end

  defp verify_retry(ctx, name, retry) do
    conn =
      request(ctx, :agent, :post, retry["path"], JSON.encode!(retry["body"]), [
        {"content-type", "application/json"},
        {"idempotency-key", retry["key"]}
      ])

    actual = body!(conn, 200)
    expected = retry["response"]
    # Only the envelope feed position is a current projection. The full nested
    # message/entry identity, content, resource revision and receipt outcome must
    # match the recorded final history. No recursive field filtering is allowed.
    changes = differences(Map.delete(expected, "rev"), Map.delete(actual, "rev"))
    check!(changes == [], "#{name} retry response differs: #{inspect(changes)}")

    check!(
      is_integer(actual["rev"]) and actual["rev"] >= expected["rev"],
      "#{name} retry cursor regressed"
    )

    check!(
      Plug.Conn.get_resp_header(conn, "idempotent-replayed") == ["true"],
      "#{name} replay lost"
    )
  end

  # Invoke after continuity verification, on disposable synthetic history only.
  # This exercises the real scheduled cleanup through the injected clock, not SQL.
  def expire(relay, fixture, manifest) do
    Agent.update(fixture.clock, &(&1 + 31 * 24 * 60 * 60 * 1_000))
    send(relay, :change_log_prune)
    ctx = {relay, fixture}

    body =
      request(ctx, :phone, :get, "/v2/changes?after=#{manifest["saved_cursor"]}", "", [])
      |> body!(410)

    check!(body["error"] == "cursor_expired", "retained-old cursor expires with 410")
    :ok
  end

  def differences(expected, actual, path \\ "$")
  def differences(expected, actual, _) when expected == actual, do: []

  def differences(expected, actual, path) when is_map(expected) and is_map(actual) do
    (Map.keys(expected) ++ Map.keys(actual))
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.flat_map(fn key ->
      differences(
        Map.get(expected, key, :missing),
        Map.get(actual, key, :missing),
        path <> "." <> to_string(key)
      )
    end)
  end

  def differences(expected, actual, path)
      when is_list(expected) and is_list(actual) and length(expected) == length(actual) do
    Enum.zip(expected, actual)
    |> Enum.with_index()
    |> Enum.flat_map(fn {{left, right}, n} -> differences(left, right, path <> "[#{n}]") end)
  end

  def differences(expected, actual, path), do: [%{path: path, expected: expected, actual: actual}]

  defp pages(ctx, path, acc, n) when n < 20 do
    body = get(ctx, :phone, path)
    acc = acc ++ body["items"]

    case body["next_page"] do
      nil -> acc
      page -> pages(ctx, "/v2/inbox?limit=1&page=#{URI.encode_www_form(page)}", acc, n + 1)
    end
  end

  defp pages(_, _, _, _), do: raise("pagination did not terminate")
  defp role!("agent"), do: :agent
  defp role!("phone"), do: :phone
  defp role!("tablet"), do: :tablet
  defp role!("other"), do: :other
  defp op(n), do: "30000000-0000-4000-8000-" <> String.pad_leading(to_string(n), 12, "0")
  defp json_headers(n), do: [{"content-type", "application/json"}, {"idempotency-key", op(n)}]

  defp write(ctx, role, method, path, body, n, headers \\ []),
    do:
      request(ctx, role, method, path, JSON.encode!(body), json_headers(n) ++ headers)
      |> body!(200)

  defp get(ctx, role, path, headers \\ []),
    do: request(ctx, role, :get, path, "", headers) |> body!(200)

  defp request({relay, fixture}, role, method, path, body, headers) do
    conn = Plug.Test.conn(method, path, body)
    conn = Enum.reduce(headers, conn, fn {k, v}, c -> Plug.Conn.put_req_header(c, k, v) end)

    conn
    |> Plug.Test.put_peer_data(%{
      address: {127, 0, 0, 1},
      port: 1,
      ssl_cert: Map.fetch!(fixture.ders, role)
    })
    |> Router.call(Router.init(relay: relay))
    |> filter_response(fixture)
  end

  # A fixture-only response filter lets negative tests simulate a broken
  # candidate route without altering production code or persisted synthetic data.
  defp filter_response(conn, %{response_filter: filter}), do: filter.(conn)
  defp filter_response(conn, _fixture), do: conn

  defp body!(conn, status) do
    check!(
      conn.status == status,
      "#{conn.method} #{conn.request_path}: expected #{status}, got #{conn.status}: #{conn.resp_body}"
    )

    {:ok, body} = JSON.decode_line(conn.resp_body <> "\n")
    body
  end

  defp check!(true, _), do: :ok
  defp check!(_, message), do: raise("compatibility: " <> message)
end
