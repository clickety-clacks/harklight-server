defmodule AttentionServer.DashboardBrowserSessionTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DashboardBrowserSession, DeviceIdentity, Relay, TLSFixtures}

  setup do
    certs = TLSFixtures.create!()
    {:ok, relay} = Relay.start_link()
    owner_cert = TLSFixtures.certificate_der!(certs, "a1")
    replacement = TLSFixtures.certificate_der!(certs, "a2")
    {:ok, person} = Relay.operator_bootstrap(relay, owner_cert)
    {:ok, key} = DeviceIdentity.from_certificate(owner_cert)
    {:ok, revision} = Relay.owner_revision(relay, person, key)
    {:ok, clock} = Agent.start_link(fn -> 1_000 end)
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
        consume: consume,
        now: fn -> Agent.get(clock, & &1) end
      )

    on_exit(fn -> File.rm_rf!(certs) end)

    %{
      relay: relay,
      browser: browser,
      certs: certs,
      clock: clock,
      person: person,
      key: key,
      replacement: replacement
    }
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

  test "owner recovery fences an existing browser cookie", c do
    assert {:ok, cookie, _} = DashboardBrowserSession.redeem(c.browser, "hbs1_approved")
    assert :ok = Relay.operator_recover(c.relay, c.person, c.replacement)
    assert {:error, :invalid_session} = DashboardBrowserSession.resolve(c.browser, cookie)
  end

  test "expired cookie stays expired after clock rollback", c do
    assert {:ok, cookie, deadline} = DashboardBrowserSession.redeem(c.browser, "hbs1_approved")
    Agent.update(c.clock, fn _ -> deadline end)
    assert {:error, :invalid_session} = DashboardBrowserSession.resolve(c.browser, cookie)
    Agent.update(c.clock, fn _ -> 1_000 end)
    assert {:error, :invalid_session} = DashboardBrowserSession.resolve(c.browser, cookie)
  end

  test "recovered owner's stale cookie cannot exhaust another person's session capacity", c do
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
