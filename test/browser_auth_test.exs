defmodule AttentionServer.BrowserAuthTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{DashboardBrowserSession, HostedRequests, JSON, Relay}
  alias AttentionServer.Store.AuthSchema
  alias Exqlite.Sqlite3

  @moduletag :tmp_dir
  @session_token "fixture-hosted-session-token"
  @origin "https://account.example.test"

  setup %{tmp_dir: dir} do
    path = Path.join(dir, "relay.sqlite3")
    opts = [store_path: path, deployment_mode: :saas, dashboard_origin: @origin]
    {:ok, relay} = Relay.start_link(opts)
    {:ok, person} = Relay.hosted_account_create(relay, "fixture-account")
    :ok = GenServer.stop(relay)

    {:ok, db} = Sqlite3.open(path)
    assert {:ok, %{minimum_generation: 1}} = AuthSchema.initialize(db)
    :ok = Sqlite3.close(db)
    %{opts: opts, person: person}
  end

  test "hosted browser grant is transaction-bound, consumed once, and signs out after restart", c do
    {:ok, relay} = Relay.start_link(c.opts)

    issue = %{
      "purpose" => "browser",
      "person_id" => c.person,
      "transaction" => "current-transaction",
      "method" => "apple"
    }

    assert request(relay, "/session-grants", issue, "other").status == 401
    issued = request(relay, "/session-grants", issue)
    assert issued.status == 200
    assert {:ok, %{"grant" => grant}} = JSON.decode_line(issued.resp_body <> "\n")
    redeem = %{"grant" => grant, "transaction" => "current-transaction"}

    assert request(relay, "/session-grants/redeem", %{redeem | "transaction" => "other"}).status ==
             401

    redeemed = request(relay, "/session-grants/redeem", redeem)
    assert redeemed.status == 200
    assert {:ok, %{"method" => "apple"}} = JSON.decode_line(redeemed.resp_body <> "\n")
    assert [header] = Plug.Conn.get_resp_header(redeemed, "set-cookie")
    assert header =~ "; path=/"
    assert header =~ "; secure"
    assert header =~ "; HttpOnly"
    assert header =~ "; SameSite=Lax"
    assert header =~ "; expires="
    refute header =~ "; domain="
    assert ["__Host-harklight=" <> cookie | _] = String.split(header, ";")
    assert request(relay, "/session-grants/redeem", redeem).status == 401
    :ok = GenServer.stop(relay)

    {:ok, restarted} = Relay.start_link(c.opts)
    {:ok, browser} = DashboardBrowserSession.start_link(relay: restarted)
    assert {:ok, %{person_id: person}} = DashboardBrowserSession.resolve(browser, cookie)
    assert person == c.person
    assert :ok = DashboardBrowserSession.end_session(browser, cookie)
    assert {:error, :invalid_session} = DashboardBrowserSession.resolve(browser, cookie)
    :ok = GenServer.stop(browser)
    :ok = GenServer.stop(restarted)

    {:ok, reopened} = Relay.start_link(c.opts)
    {:ok, browser} = DashboardBrowserSession.start_link(relay: reopened)
    assert {:error, :invalid_session} = DashboardBrowserSession.resolve(browser, cookie)
  end

  test "the existing Hosted handoff becomes a durable session", c do
    {:ok, relay} = Relay.start_link(c.opts)
    assert {:ok, url} = Relay.hosted_sign_in_create(relay, c.person, "Fixture")
    token = URI.parse(url).fragment
    {:ok, browser} = DashboardBrowserSession.start_link(relay: relay)
    assert {:ok, cookie, :persistent} = DashboardBrowserSession.redeem(browser, token)
    assert {:error, :invalid_link} = DashboardBrowserSession.redeem(browser, token)
    :ok = GenServer.stop(browser)
    :ok = GenServer.stop(relay)

    {:ok, restarted} = Relay.start_link(c.opts)
    {:ok, browser} = DashboardBrowserSession.start_link(relay: restarted)
    assert {:ok, %{person_id: person}} = DashboardBrowserSession.resolve(browser, cookie)
    assert person == c.person
  end

  defp request(relay, path, body, token \\ @session_token) do
    Plug.Test.conn(:post, path, JSON.encode!(body))
    |> Plug.Conn.put_req_header("authorization", "Bearer " <> token)
    |> HostedRequests.call(HostedRequests.init(relay: relay, session_token: @session_token))
  end
end
