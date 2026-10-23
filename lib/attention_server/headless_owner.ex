defmodule AttentionServer.HeadlessOwner do
  @moduledoc """
  Authenticated one-request owner tool protocol for device and agent management.
  The CLI displays exact target identity before explicit approval or replacement.
  """

  alias AttentionServer.HeadlessTLS

  def browser_sign_in(endpoint, certificate, key, person),
    do: request(endpoint, certificate, key, person, %{"type" => "browser_sign_in_create"})

  def invite(endpoint, certificate, key, person),
    do: request(endpoint, certificate, key, person, %{"type" => "invite_create"})

  def invite(endpoint, certificate, key, person, request_id),
    do:
      request(endpoint, certificate, key, person, %{
        "type" => "invite_create",
        "request_id" => request_id
      })

  def decline(endpoint, certificate, key, person, code, expected_key, deadline),
    do:
      request(endpoint, certificate, key, person, %{
        "type" => "pending_decline",
        "approval_code" => code,
        "expected_device_id" => expected_key,
        "expected_expires_at" => deadline
      })

  def lookup(endpoint, certificate, key, person, code),
    do:
      request(endpoint, certificate, key, person, %{
        "type" => "pending_lookup",
        "approval_code" => code
      })

  def approve(
        endpoint,
        certificate,
        key,
        person,
        code,
        expected_key,
        expected_expires_at,
        senders
      ),
      do:
        request(endpoint, certificate, key, person, %{
          "type" => "pending_approve",
          "approval_code" => code,
          "expected_device_id" => expected_key,
          "expected_expires_at" => expected_expires_at,
          "sender_grants" => senders
        })

  def revoke_device(endpoint, certificate, key, person, device),
    do:
      request(endpoint, certificate, key, person, %{
        "type" => "device_revoke",
        "device_id" => device
      })

  def grant_sender(endpoint, certificate, key, person, device, sender),
    do:
      request(endpoint, certificate, key, person, %{
        "type" => "sender_grant",
        "device_id" => device,
        "sender_id" => sender
      })

  def revoke_sender(endpoint, certificate, key, person, device, sender),
    do:
      request(endpoint, certificate, key, person, %{
        "type" => "sender_revoke",
        "device_id" => device,
        "sender_id" => sender
      })

  def grant_read(endpoint, certificate, key, person, connector, grant, source, target),
    do:
      request(endpoint, certificate, key, person, %{
        "type" => "enrollment_read_grant",
        "connector_device_id" => connector,
        "grant_id" => grant,
        "source_instance_id" => source,
        "target" => target
      })

  def revoke_read(endpoint, certificate, key, person, grant),
    do:
      request(endpoint, certificate, key, person, %{
        "type" => "enrollment_read_revoke",
        "grant_id" => grant
      })

  def grant_maintainer(endpoint, certificate, key, person, grant, grantee, scopes),
    do:
      request(endpoint, certificate, key, person, %{
        "type" => "maintainer_grant",
        "grant_id" => grant,
        "grantee_person_id" => grantee,
        "scopes" => scopes
      })

  def revoke_maintainer(endpoint, certificate, key, person, grant),
    do:
      request(endpoint, certificate, key, person, %{
        "type" => "maintainer_revoke",
        "grant_id" => grant
      })

  def register_agent(endpoint, certificate, key, person, id, fingerprint),
    do:
      request(endpoint, certificate, key, person, %{
        "type" => "agent_register",
        "agent_id" => id,
        "key_fingerprint" => fingerprint
      })

  def replace_agent_grants(
        endpoint,
        certificate,
        key,
        person,
        fingerprint,
        revision,
        read,
        respond
      ),
      do:
        request(endpoint, certificate, key, person, %{
          "type" => "agent_grants_replace",
          "key_fingerprint" => fingerprint,
          "expected_revision" => revision,
          "read_grants" => read,
          "respond_grants" => respond
        })

  def revoke_agent(endpoint, certificate, key, person, fingerprint),
    do:
      request(endpoint, certificate, key, person, %{
        "type" => "agent_revoke",
        "key_fingerprint" => fingerprint
      })

  def list_agents(endpoint, certificate, key, person),
    do: request(endpoint, certificate, key, person, %{"type" => "agent_list"})

  defp request(endpoint, certificate, key, person, fields) do
    frame = Map.merge(fields, %{"v" => 1, "person_id" => person})

    case HeadlessTLS.owner_request(endpoint, certificate, key, frame) do
      {:ok, %{"ok" => true} = response} -> {:ok, response}
      {:ok, %{"error" => reason}} -> {:error, reason}
      {:ok, _} -> {:error, :invalid_server_response}
      error -> error
    end
  end
end
