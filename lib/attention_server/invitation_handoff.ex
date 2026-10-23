defmodule AttentionServer.InvitationHandoff do
  @moduledoc """
  Direct invitation result for an explicitly authorized owner or owner-agent.

  The share URL is sensitive product output, never a generic log message.
  Inspection omits it; only `encode_result/1` explicitly releases it to the
  caller's dedicated handoff channel. No browser, clipboard or file retrieval
  is required. A nonsecret request ID recovers the same owner action.
  """

  alias AttentionServer.{GuidedJoin, HeadlessOwner, JSON, Message}

  @derive {Inspect, except: [:share_url, :full_url]}
  defstruct [
    :request_id,
    :state,
    :account,
    :destination,
    :expires_at,
    :share_url,
    :full_url,
    :error_code,
    :recovery
  ]

  def create(endpoint, certificate_path, key_path, person, request_id) do
    if Message.uuid_v4?(person) and Message.uuid_v4?(request_id) do
      case HeadlessOwner.invite(endpoint, certificate_path, key_path, person, request_id) do
        {:ok, %{"invitation" => invitation} = reply} ->
          ready(invitation, reply["tappable_url"], person, request_id)
        {:error, reason} -> failure(reason, person, request_id)
        _ -> failure(:unknown, person, request_id)
      end
    else
      %__MODULE__{
        state: "refused",
        error_code: "invalid_request",
        recovery:
          "Supply the intended person UUID and a stable request UUID before creating an invitation."
      }
    end
  end

  defp ready(
         %{"person_id" => person, "join_url" => url, "expires_at" => deadline} = invitation,
         tappable,
         person,
         request_id
       )
       when is_binary(url) and is_integer(deadline) and deadline > 0 do
    configuration =
      Map.take(invitation, ~w(server_name enrollment_url relay_url ca_pem ca_sha256))

    if GuidedJoin.valid_configuration?(configuration) and
         url == GuidedJoin.link(Map.delete(invitation, "join_url")) do
      share_url = if valid_tappable?(tappable, invitation), do: tappable, else: url

      %__MODULE__{
        state: "ready",
        request_id: request_id,
        account: person,
        destination: Map.take(configuration, ~w(server_name enrollment_url relay_url)),
        expires_at: deadline,
        share_url: share_url,
        full_url: url,
        recovery:
          "Share this invitation only with the joining person. An exact owner approval is still required."
      }
    else
      failure(:unknown, person, request_id)
    end
  end

  defp ready(_, _, person, request_id), do: failure(:unknown, person, request_id)

  defp valid_tappable?(url, %{"invite_token" => token, "server_name" => server})
       when is_binary(url) do
    case URI.parse(url) do
      %URI{scheme: "https", query: nil, fragment: ^token, path: "/join", host: host}
      when host == server -> byte_size(url) < 512
      _ -> false
    end
  end

  defp valid_tappable?(_, _), do: false

  defp failure(reason, person, request_id) when reason in ["expired", "used"] do
    %__MODULE__{
      state: reason,
      request_id: request_id,
      account: person,
      error_code: reason,
      recovery:
        "This invitation cannot be reused. Start a deliberate new invitation action with a new request UUID."
    }
  end

  defp failure(reason, person, request_id)
       when reason in ["not_found", "invalid_request", "request_conflict"] do
    %__MODULE__{
      state: "refused",
      request_id: request_id,
      account: person,
      error_code: reason,
      recovery:
        "Check current owner authority, the intended person and configured destination; do not infer an invitation or broader grants."
    }
  end

  defp failure("capacity", person, request_id) do
    %__MODULE__{
      state: "retryable",
      request_id: request_id,
      account: person,
      error_code: "capacity",
      recovery:
        "Invitation capacity is unavailable. Retry this same request UUID after capacity is available."
    }
  end

  defp failure(_, person, request_id) do
    %__MODULE__{
      state: "unconfirmed",
      request_id: request_id,
      account: person,
      error_code: "result_unavailable",
      recovery:
        "Creation is not confirmed. Retry the same request UUID after checking the connection; do not create a replacement action to recover a lost reply."
    }
  end

  def encode_result(%__MODULE__{} = result) do
    %{
      "ok" => result.state == "ready",
      "state" => result.state,
      "request_id" => result.request_id,
      "account" => result.account,
      "destination" => result.destination,
      "expires_at" => result.expires_at,
      "share_url" => result.share_url,
      "full_url" => result.full_url,
      "error_code" => result.error_code,
      "recovery" => result.recovery
    }
    |> JSON.encode!()
  end
end
