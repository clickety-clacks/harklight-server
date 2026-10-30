defmodule AttentionServer.DashboardViewTest do
  use ExUnit.Case, async: true

  alias AttentionServer.DashboardView

  test "an enrolled device is not presented as online" do
    html =
      DashboardView.devices("Mike", [%{label: "iPad", person_name: "Mike", revoked: false}], [])

    assert html =~ "iPad</td><td>Mike</td><td>Enrolled</td><td>Unknown"
    refute html =~ "Connected now"
    refute html =~ "Last seen"
  end

  test "revoked devices and pending requests stay distinct and untrusted labels are escaped" do
    html =
      DashboardView.devices(
        "Mike",
        [%{label: "Old <phone>", person_name: "Mike", revoked: true}],
        [%{label: "New <phone>", person_name: "Mike", code: "K7PQ3MZ2DW"}]
      )

    assert html =~ "Old &lt;phone&gt;</td><td>Mike</td><td>Revoked</td><td>Unknown"
    assert html =~ "New &lt;phone&gt;"
    assert html =~ "Waiting for approval"
    refute html =~ "<phone>"
  end

  test "only an active device the owner can revoke has a remove action" do
    owned_key = String.duplicate("a", 64)
    other_key = String.duplicate("b", 64)
    revoked_key = String.duplicate("c", 64)

    html =
      DashboardView.devices(
        "Mike",
        [
          %{device_id: owned_key, can_revoke: true, label: "Mike's tablet", person_name: "Mike"},
          %{device_id: other_key, can_revoke: false, label: "Other phone", person_name: "Dana"},
          %{
            device_id: revoked_key,
            can_revoke: false,
            label: "Old phone",
            person_name: "Mike",
            revoked: true
          }
        ],
        []
      )

    assert html =~ ~s(href="/devices/remove/#{owned_key}")
    refute html =~ ~s(href="/devices/remove/#{other_key}")
    refute html =~ ~s(href="/devices/remove/#{revoked_key}")
  end

  test "device removal confirmation names the device, fingerprint and permanent effect" do
    key = String.duplicate("d", 64)

    html =
      DashboardView.remove_device_confirmation("Mike", %{device_id: key, label: "Mike's tablet"})

    assert html =~ "Device: <strong>Mike&#39;s tablet</strong>"
    assert html =~ "Key fingerprint: <code class=\"fingerprint\">#{key}</code>"
    assert html =~ "Removal is permanent."
    assert html =~ "you must enroll it again"
    assert html =~ ~s(<form action="/devices/remove/#{key}" method="post">)
  end

  test "empty device list offers one direct invitation action and links to components" do
    request_id = "123e4567-e89b-42d3-a456-426614174000"
    html = DashboardView.devices("Dana", [], [], request_id)

    assert html =~ "No devices yet"
    assert html =~ "Create a person-bound invitation"
    assert html =~ ~s(<form action="/devices/invitation/new" method="post">)
    assert html =~ ~s(name="request_id" value="#{request_id}")
    assert html =~ ~s(<button class="button" type="submit">Create a new invitation</button>)
    refute html =~ ~s(<form action="/devices/add" method="post">)
    assert html =~ "Another invitation could leave an earlier link active"
    assert html =~ ~s(href="/components")
    assert html =~ ~s(<div class="account">Dana</div>)
  end

  test "retained request identity offers recovery through the ordinary Create action" do
    html = DashboardView.devices("Dana", [], [])

    assert html =~ ~s(<form action="/devices/add" method="post">)
    refute html =~ ~s(<form action="/devices/invitation/new" method="post">)
  end

  test "the components page shows the packaged skill version and no invented app link" do
    html = DashboardView.components("Dana", {:ok, "0.1.0"}, {:ok, "0.2.0"})

    assert html =~ "v0.1.0"
    assert html =~ ~s(<p class="status version">v0.1.0</p>)
    assert html =~ ".status.version{text-transform:none;letter-spacing:normal"

    assert html =~ ~s(href="/agent-skills/SKILL.md")
    assert html =~ ~s(<div class="account">Dana</div>)

    assert html =~ ~s(<section><h2>App</h2><p class="status">Not available yet</p></section>)
  end

  test "the components page presents a wrapped, copyable install command" do
    html = DashboardView.components("Dana", {:ok, "0.1.0"}, {:ok, "0.2.0"})

    assert html =~
             ~s(<pre id="install-command"><code>brew install <span class="brew-namespace">clickety-clacks</span>/<wbr>tap/<wbr>harklight-agent</code></pre>)

    assert [command_markup] =
             Regex.run(~r{<pre id="install-command"><code>(.*?)</code></pre>}s, html,
               capture: :all_but_first
             )

    assert Regex.replace(~r{<[^>]+>}, command_markup, "") ==
             "brew install clickety-clacks/tap/harklight-agent"

    assert html =~ ".install-command code .brew-namespace{white-space:nowrap}"

    assert html =~
             ~s(<button class="button" id="copy-install-command" type="button" aria-controls="install-command">Copy install command</button>)

    assert html =~ ~s(<script src="/components.js" defer></script>)
  end

  test "the components page links to the published CLI release" do
    html = DashboardView.components("Dana", {:ok, "0.1.0"}, {:ok, "0.2.0"})

    assert html =~
             ~s(href="https://github.com/clickety-clacks/harklight-agent/releases/latest")

    refute html =~ "Homebrew formula"
  end

  test "the components page documents both shipped skills with their own version and link" do
    html = DashboardView.components("Dana", {:ok, "0.1.0"}, {:ok, "0.2.0"})

    assert html =~ "Agent skill: harklight-agent</h2>"
    assert html =~ "v0.1.0"
    assert html =~ ~s(href="/agent-skills/SKILL.md")

    assert html =~ "Agent skill: harklight-agent-saas</h2>"
    assert html =~ "v0.2.0"
    assert html =~ ~s(href="/agent-skills/harklight-agent-saas/SKILL.md")
    assert html =~ ~s(href="/agent-skills/core.md")
  end

  test "the components page tolerates a skill version lookup failure for either skill independently" do
    html = DashboardView.components("Dana", :error, {:ok, "0.2.0"})

    refute html =~ "v0.1.0"
    assert html =~ "v0.2.0"

    html = DashboardView.components("Dana", {:ok, "0.1.0"}, :error)

    assert html =~ "v0.1.0"
    refute html =~ "v0.2.0"
  end

  test "the components page tolerates both skill version lookups failing" do
    html = DashboardView.components("Dana", :error, :error)

    refute html =~ ~s(<h2>Agent skill: harklight-agent</h2><p class="status">v)
    refute html =~ ~s(<h2>Agent skill: harklight-agent-saas</h2><p class="status">v)
  end

  test "uncertain invitation failure offers same-request retry without a new invitation action" do
    html = DashboardView.invitation_failure("Dana", "The server could not create an invitation")

    assert html =~ ~s(<form action="/devices/add" method="post">)
    refute html =~ ~s(<form action="/devices/invitation/new" method="post">)
  end
end
