defmodule AttentionServer.DashboardBrowserSessionTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    DashboardBrowserSession,
    DashboardReadHTTP,
    DeviceIdentity,
    JSON,
    Relay,
    TLSFixtures
  }

  setup do
    certs = TLSFixtures.create!()
    {:ok, relay} = Relay.start_link()
    owner_cert = TLSFixtures.certificate_der!(certs, "a1")
    replacement = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, person} = Relay.operator_bootstrap(relay, owner_cert)
    {:ok, key} = DeviceIdentity.from_certificate(owner_cert)
    {:ok, revision} = Relay.owner_revision(relay, person, key)
    {:ok, one_use} = Agent.start_link(fn -> MapSet.new(["hbs1_approved"]) end)

    consume = fn token ->
      Agent.get_and_update(one_use, fn available ->
        if MapSet.member?(available, token) do
          {{:ok,
            %{person_id: person, owner_key: key, owner_revision: revision, scope: :person_owner}},
           MapSet.delete(available, token)}
        else
          {{:error, :invalid_link}, available}
        end
      end)
    end

    {:ok, browser} =
      DashboardBrowserSession.start_link(
        relay: relay,
        consume: consume
      )

    on_exit(fn -> File.rm_rf!(certs) end)

    %{
      relay: relay,
      browser: browser,
      certs: certs,
      person: person,
      key: key,
      replacement: replacement
    }
  end

  defmodule SignOutBackend do
    def resolve_session(browser, cookie), do: DashboardBrowserSession.resolve(browser, cookie)
    def end_session(browser, cookie), do: DashboardBrowserSession.end_session(browser, cookie)
  end

  for state <- [:valid, :missing, :revoked] do
    @tag sign_out_state: state
    test "hosted sign-out with #{state} cookie checks the form and completes logout",
         c do
      assert {:ok, cookie, nil} = DashboardBrowserSession.redeem(c.browser, "hbs1_approved")
      state = c.sign_out_state

      if state == :revoked,
        do: assert(:ok = Relay.operator_recover(c.relay, c.person, c.replacement))

      supplied_cookie = if state == :missing, do: nil, else: cookie
      origin = "https://account.example.test"

      opts =
        AttentionServer.DashboardReadHTTP.init(
          backend: {SignOutBackend, c.browser},
          skill_path: __ENV__.file,
          hosted: true,
          expected_origin: origin,
          hosted_addresses: %{
            sign_in: origin <> "/account/sign-in",
            sign_out: origin <> "/account/sign-out"
          }
        )

      post = fn opts, body, request_origin, content_type ->
        conn = %{Plug.Test.conn(:post, "/sign-out", body) | scheme: :https, port: 443}

        conn =
          if supplied_cookie,
            do: Plug.Conn.put_req_header(conn, "cookie", "__Host-harklight=#{supplied_cookie}"),
            else: conn

        conn
        |> Plug.Conn.put_req_header("origin", request_origin)
        |> Plug.Conn.put_req_header("content-type", content_type)
        |> AttentionServer.DashboardReadHTTP.call(opts)
      end

      form_type = "application/x-www-form-urlencoded"

      for {body, request_origin, content_type} <- [
            {"confirm=wrong", origin, form_type},
            {"", origin, form_type},
            {"confirm=sign_out&extra=value", origin, form_type},
            {"confirm=sign_out", "https://other.example.test", form_type},
            {"confirm=sign_out", origin, "application/json"}
          ] do
        refused = post.(opts, body, request_origin, content_type)
        assert refused.status == 403
        assert Plug.Conn.get_resp_header(refused, "location") == []
        refute Map.has_key?(refused.resp_cookies, "__Host-harklight")
      end

      if state == :valid,
        do: assert({:ok, _} = DashboardBrowserSession.resolve(c.browser, cookie))

      plain =
        Plug.Test.conn(:post, "/sign-out", "confirm=sign_out")
        |> Plug.Conn.put_req_header("origin", origin)
        |> Plug.Conn.put_req_header("content-type", form_type)
        |> Map.put(:scheme, :https)
        |> Map.put(:port, 443)
        |> AttentionServer.DashboardReadHTTP.call(%{opts | hosted: false})

      assert plain.status == 303
      assert Plug.Conn.get_resp_header(plain, "location") == ["/sign-in"]

      response = post.(opts, "confirm=sign_out", origin, form_type)
      assert response.status == 303
      assert Plug.Conn.get_resp_header(response, "location") == [origin <> "/account/sign-out"]

      assert %{"__Host-harklight" => %{max_age: 0, secure: true, http_only: true}} =
               response.resp_cookies

      if state == :missing do
        # A request without a cookie cannot revoke some other browser's session.
        assert {:ok, _} = DashboardBrowserSession.resolve(c.browser, cookie)
      else
        assert {:error, :invalid_session} = DashboardBrowserSession.resolve(c.browser, cookie)
      end
    end
  end

  test "a committed one-use owner principal becomes a cookie, not a reusable link", c do
    assert {:ok, cookie, _} = DashboardBrowserSession.redeem(c.browser, "hbs1_approved")
    assert {:error, :invalid_link} = DashboardBrowserSession.redeem(c.browser, "hbs1_approved")

    assert {:ok, %{person_id: person, owner_key: key}} =
             DashboardBrowserSession.resolve(c.browser, cookie)

    assert person == c.person and key == c.key

    assert {:error, :invalid_session} =
             DashboardBrowserSession.resolve(c.browser, "hbs1_approved")
  end

  test "self-hosted profile returns an empty label for a signed-in principal without one", c do
    assert {:ok, cookie, nil} = DashboardBrowserSession.redeem(c.browser, "hbs1_approved")
    assert {:ok, principal} = DashboardBrowserSession.resolve(c.browser, cookie)
    assert principal.person_id == c.person
    refute Map.has_key?(principal, :display_name)

    opts =
      DashboardReadHTTP.init(
        backend: {SignOutBackend, c.browser},
        skill_path: __ENV__.file
      )

    response =
      Plug.Test.conn(:get, "/api/profile")
      |> Plug.Conn.put_req_header("cookie", "__Host-harklight=#{cookie}")
      |> Map.put(:scheme, :https)
      |> Map.put(:port, 443)
      |> DashboardReadHTTP.call(opts)

    assert response.status == 200

    assert Plug.Conn.get_resp_header(response, "content-type") == [
             "application/json; charset=utf-8"
           ]

    assert {:ok, %{"display_name" => ""}} = JSON.decode_line(response.resp_body <> "\n")
  end

  test "owner recovery fences an existing browser cookie", c do
    assert {:ok, cookie, _} = DashboardBrowserSession.redeem(c.browser, "hbs1_approved")
    assert :ok = Relay.operator_recover(c.relay, c.person, c.replacement)
    assert {:error, :invalid_session} = DashboardBrowserSession.resolve(c.browser, cookie)
  end

  test "sign-out ends every connection under that browser session", c do
    assert {:ok, cookie, nil} = DashboardBrowserSession.redeem(c.browser, "hbs1_approved")

    assert {:ok, first} =
             DashboardBrowserSession.open_browser_connection(
               c.browser,
               cookie,
               "00000000-0000-4000-8000-000000000101",
               self()
             )

    assert {:ok, second} =
             DashboardBrowserSession.open_browser_connection(
               c.browser,
               cookie,
               "00000000-0000-4000-8000-000000000102",
               self()
             )

    assert :ok = DashboardBrowserSession.end_session(c.browser, cookie)

    assert_receive {:attention_fenced, first_id}
    assert_receive {:attention_fenced, second_id}
    assert MapSet.new([first.session_id, second.session_id]) == MapSet.new([first_id, second_id])
    assert {:error, :invalid_session} = DashboardBrowserSession.resolve(c.browser, cookie)
  end

  test "owner recovery removes the old session when its authority changes", c do
    assert {:ok, cookie, nil} = DashboardBrowserSession.redeem(c.browser, "hbs1_approved")
    assert {:ok, _} = DashboardBrowserSession.resolve(c.browser, cookie)
    assert map_size(:sys.get_state(c.browser).sessions) == 1

    assert :ok = Relay.operator_recover(c.relay, c.person, c.replacement)
    assert {:error, :invalid_session} = DashboardBrowserSession.resolve(c.browser, cookie)
    assert map_size(:sys.get_state(c.browser).sessions) == 0
  end

  test "stale owner session is removed without limiting another person's session", c do
    other_cert = TLSFixtures.certificate_der!(c.certs, "b1")
    {:ok, other_person} = Relay.operator_bootstrap(c.relay, other_cert)
    {:ok, other_key} = DeviceIdentity.from_certificate(other_cert)
    {:ok, other_revision} = Relay.owner_revision(c.relay, other_person, other_key)
    {:ok, own_revision} = Relay.owner_revision(c.relay, c.person, c.key)

    bindings = %{
      "first" => %{
        person_id: c.person,
        owner_key: c.key,
        owner_revision: own_revision,
        scope: :person_owner
      },
      "second" => %{
        person_id: other_person,
        owner_key: other_key,
        owner_revision: other_revision,
        scope: :person_owner
      }
    }

    {:ok, tokens} = Agent.start_link(fn -> bindings end)

    consume = fn token ->
      Agent.get_and_update(tokens, fn available ->
        {binding, rest} = Map.pop(available, token)
        {if(binding, do: {:ok, binding}, else: {:error, :invalid_link}), rest}
      end)
    end

    {:ok, limited} =
      DashboardBrowserSession.start_link(relay: c.relay, consume: consume, session_limit: 1)

    assert {:ok, first_cookie, _} = DashboardBrowserSession.redeem(limited, "first")
    assert :ok = Relay.operator_recover(c.relay, c.person, c.replacement)
    assert {:ok, second_cookie, _} = DashboardBrowserSession.redeem(limited, "second")
    assert {:error, :invalid_session} = DashboardBrowserSession.resolve(limited, first_cookie)

    assert {:ok, %{person_id: ^other_person}} =
             DashboardBrowserSession.resolve(limited, second_cookie)
  end
end
