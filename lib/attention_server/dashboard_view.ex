defmodule AttentionServer.DashboardView do
  @moduledoc """
  Render the person-scoped device page for an already authenticated browser session.

  The caller supplies only records the signed-in person may inspect. This module
  does not decide account access or infer presence from enrollment.
  """

  @spec devices(String.t(), list(map()), list(map()), String.t() | nil, keyword()) :: String.t()
  def devices(person_name, devices, waiting, new_request_id \\ nil, chrome \\ [])
      when is_list(devices) and is_list(waiting) do
    rows =
      devices
      |> Enum.map(&device_row/1)
      |> Enum.join()

    pending =
      waiting
      |> Enum.map(&waiting_row/1)
      |> Enum.join()

    body =
      if devices == [] and waiting == [] do
        ~s(<section class="empty"><h2>No devices yet</h2><p>Create a person-bound invitation, then approve the exact request when it appears here.</p></section>)
      else
        ~s(<section><h2>Waiting for approval</h2><ul class="waiting">#{pending}</ul></section>) <>
          ~s(<section><h2>Devices</h2><table><thead><tr><th>Device</th><th>Person</th><th>Status</th><th>Connection</th></tr></thead><tbody>#{rows}</tbody></table><p class="note">Enrolled means the device's key was approved. It does not say whether the device is reachable. Connection is unknown until the server has a supported presence reading.</p></section>)
      end

    invitation_action =
      if is_binary(new_request_id) do
        ~s(<p>This browser cannot recover an earlier invitation because its request identity is missing. Another invitation could leave an earlier link active. Create a new invitation only if needed.</p>) <>
          new_invitation_form(new_request_id, "Create a new invitation")
      else
        ~s(<p>If the last response was lost, Create invitation retrieves that same link while it is unused and active.</p>) <>
          invitation_form("Create invitation")
      end

    layout(
      person_name,
      "Devices",
      ~s(<header class="page-head"><div><h1>Devices</h1><p>Devices approved to receive requests.</p></div>#{invitation_action}</header>) <>
        body,
      chrome
    )
  end

  @spec components(
          String.t(),
          {:ok, String.t()} | :error,
          {:ok, String.t()} | :error,
          keyword()
        ) :: String.t()
  def components(person_name, skill_version, skill_version_send_listen, chrome \\ []) do
    app_section =
      ~s(<section><h2>App</h2><p class="status">Not available yet</p></section>)

    cli_section =
      ~s(<section><h2>Command-line agent</h2><pre><code>brew install clickety-clacks/tap/harklight-agent</code></pre><p><a href="https://github.com/clickety-clacks/harklight-agent/releases/latest">Prebuilt releases</a></p></section>)

    skill_section =
      skill_section("harklight-agent", skill_version, "/agent-skills/SKILL.md")

    skill_section_send_listen =
      skill_section(
        "harklight-agent-send-listen",
        skill_version_send_listen,
        "/agent-skills/harklight-agent-send-listen/SKILL.md"
      )

    body =
      ~s(<header class="page-head"><div><h1>Components</h1></div></header>) <>
        app_section <> cli_section <> skill_section <> skill_section_send_listen

    layout(person_name, "Components", body, chrome)
  end

  defp skill_section(name, skill_version, path) do
    version_line =
      case skill_version do
        {:ok, version} -> ~s(<p class="status">v#{escape(version)}</p>)
        :error -> ""
      end

    ~s(<section><h2>Agent skill: #{escape(name)}</h2>#{version_line}<p><a href="#{escape(path)}">Read the #{escape(name)} skill</a></p></section>)
  end

  def invitation_failure(person, message, action \\ :retry, new_request_id \\ nil, chrome \\ []) do
    action_form =
      case action do
        :retry -> invitation_form("Try again")
        :new -> new_invitation_form(new_request_id)
        :none -> ""
      end

    body =
      ~s(<h1>Invitation unavailable</h1><section><p>#{escape(message)}</p>#{action_form}</section>) <>
        ~s(<p><a href="/devices">Back to devices</a></p>)

    layout(person, "Invitation unavailable", body, chrome)
  end

  def invitation(person, invitation, new_request_id, chrome \\ []) do
    link = escape(invitation["join_url"] || "")
    tappable = escape(invitation["tappable_url"] || "")
    deadline = escape(time(invitation["expires_at"]))
    server = escape(invitation["server_name"] || "unknown server")
    enrollment = escape(invitation["enrollment_url"] || "unknown enrollment endpoint")

    body =
      ~s(<h1>Invitation ready</h1>) <>
        ~s(<section><p>Destination: <strong>#{server}</strong> &mdash; #{enrollment}</p><p>Account: <strong>#{escape(person)}</strong></p><p>Expires: <strong>#{deadline}</strong></p><label for="tappable-link">Tappable invitation link</label><input id="tappable-link" readonly value="#{tappable}" style="width:100%"><p><button class="button" id="share-invitation" type="button">Share invitation</button> <button class="button" id="copy-invitation" type="button">Copy tappable link</button></p><label for="join-link">Full invitation for Paste</label><input id="join-link" readonly value="#{link}" style="width:100%"><p><button class="button" id="copy-full-invitation" type="button">Copy full link</button></p><p id="share-status" role="status" aria-live="polite"></p></section>) <>
        ~s(#{new_invitation_form(new_request_id)}<p><a href="/devices">Back to devices</a></p><script src="/invitation-share.js" defer></script>)

    layout(person, "Invitation ready", body, chrome)
  end

  def review_pending(person, code, request, chrome \\ []) do
    key = request[:key]
    deadline = request[:expires_at]
    label = escape(request[:label] || "New device")
    escaped_code = escape(code)
    escaped_key = escape(key)

    body =
      ~s(<h1>Approve this device?</h1>) <>
        ~s(<section><p>Device: #{label}</p><p>Joins as: #{escape(person)}</p><p>Code on device: <strong>#{escaped_code}</strong></p><p>Closes: #{escape(time(deadline))}</p><details><summary>Exact device key</summary><code>#{escaped_key}</code></details></section>) <>
        ~s(<section>) <>
        action_form(code, "approve", key, deadline, "Approve this exact device") <>
        action_form(code, "decline", key, deadline, "Decline") <>
        ~s(</section><p><a href="/devices">Back to devices</a></p>)

    layout(person, "Review device", body, chrome)
  end

  defp action_form(code, action, key, deadline, label) do
    ~s(<form action="/devices/waiting/#{escape(code)}/#{action}" method="post"><input type="hidden" name="key" value="#{escape(key)}"><input type="hidden" name="deadline" value="#{escape(deadline)}"><button class="button" type="submit">#{escape(label)}</button></form>)
  end

  defp invitation_form(label) do
    ~s(<form action="/devices/add" method="post"><input type="hidden" name="confirm" value="invite"><button class="button" type="submit">#{escape(label)}</button></form>)
  end

  defp new_invitation_form(request_id, label \\ "Start a new invitation") do
    ~s(<form action="/devices/invitation/new" method="post"><input type="hidden" name="confirm" value="new_invitation"><input type="hidden" name="request_id" value="#{escape(request_id)}"><button class="button" type="submit">#{escape(label)}</button></form>)
  end

  defp device_row(device) do
    label = escape(device[:label] || device["label"] || "Unnamed device")
    person = escape(device[:person_name] || device["person_name"] || "")
    status = if device[:revoked] || device["revoked"], do: "Revoked", else: "Enrolled"

    ~s(<tr><td>#{label}</td><td>#{person}</td><td>#{status}</td><td>Unknown</td></tr>)
  end

  defp waiting_row(item) do
    label = escape(item[:label] || item["label"] || "Unnamed device")
    person = escape(item[:person_name] || item["person_name"] || "")
    code = escape(item[:code] || item["code"] || "")

    review =
      if item[:can_review],
        do: ~s(<a href="/devices/waiting/#{code}">Review exact request</a>),
        else: "Current person owner approval required"

    ~s(<li><span>#{label} · #{person}</span><span>Code on device: #{code}</span><span>#{review}</span></li>)
  end

  defp time(ms) when is_integer(ms),
    do: ms |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()

  defp time(_), do: "unknown time"

  # `chrome` adds hosted-only navigation: `:account_url` links the hosted
  # account page and `:sign_out` shows the Sign out control. Without it the
  # page is exactly the self-hosted page.
  defp layout(person_name, title, content, chrome) do
    account_link =
      case Keyword.get(chrome, :account_url) do
        url when is_binary(url) -> ~s(<a href="#{escape(url)}">Account</a>)
        _ -> ""
      end

    sign_out =
      if Keyword.get(chrome, :sign_out) == true,
        do:
          ~s(<form action="/sign-out" method="post"><input type="hidden" name="confirm" value="sign_out"><button class="button" type="submit">Sign out</button></form>),
        else: ""

    ~s(<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>#{escape(title)} · Harklight</title><style>#{css()}</style></head><body><div class="shell"><aside><strong>Harklight</strong><nav><a href="/devices">Devices</a><a href="/components">Components</a>#{account_link}</nav><div class="account">#{escape(person_name)}#{sign_out}</div></aside><main>#{content}</main></div></body></html>)
  end

  defp css do
    "body{margin:0;background:#f5f6f8;color:#202632;font:16px system-ui,-apple-system,sans-serif}.shell{display:flex;min-height:100vh}aside{width:210px;background:white;border-right:1px solid #dce1e7;padding:28px;display:flex;flex-direction:column;gap:32px}nav{display:grid;gap:12px}a{color:#1265b0;text-decoration:none}.account{margin-top:auto;color:#5f6874;font-size:14px}main{width:min(100%,900px);padding:40px}.page-head{display:flex;justify-content:space-between;align-items:center;gap:20px}h1{margin:0}h2{font-size:18px}.page-head p,.note{color:#5f6874}.status{display:inline-block;color:#5f6874;font-size:14px;text-transform:uppercase;letter-spacing:.04em;margin:0}.button{background:#1465b5;color:white;border:0;border-radius:20px;padding:10px 18px;white-space:nowrap;font:inherit;cursor:pointer}section{background:white;border:1px solid #dce1e7;border-radius:14px;padding:20px;margin-top:24px}.empty{max-width:480px}.waiting{list-style:none;padding:0}.waiting li{display:flex;gap:20px;justify-content:space-between;border-top:1px solid #e7e9ed;padding:12px 0}table{width:100%;border-collapse:collapse;text-align:left}th,td{padding:12px;border-bottom:1px solid #e7e9ed}th{color:#5f6874;font-size:14px}@media(max-width:680px){.shell{display:block}aside{width:auto;gap:12px;padding:16px}nav{display:flex;gap:20px}.account{margin:0}main{padding:20px}.page-head,.waiting li{display:block}.button{display:inline-block;margin-top:10px}}"
  end

  defp escape(value) do
    value
    |> to_string()
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
    |> String.replace("'", "&#39;")
  end
end
