defmodule AttentionServer.GuidedJoin do
  @moduledoc """
  Person-bound, owner-created invitations. Only hashes of redemption and status
  secrets enter durable state. An invitation authorizes a pending request, never
  enrollment or sender rights; approval remains a separate current-owner act.
  """

  alias AttentionServer.{DeviceIdentity, Diagnostics, Engine}

  def configuration(name, enroll_port, relay_port, ca_pem) do
    with true <- is_binary(name) and Regex.match?(~r/\A[a-zA-Z0-9][a-zA-Z0-9.-]{0,252}\z/, name),
         true <- is_integer(enroll_port) and enroll_port in 1..65_535,
         true <- is_integer(relay_port) and relay_port in 1..65_535,
         true <- is_binary(ca_pem) and byte_size(ca_pem) <= 16 * 1024,
         [{:Certificate, der, :not_encrypted}] <- :public_key.pem_decode(ca_pem),
         true <- match?({:OTPCertificate, _, _, _}, :public_key.pkix_decode_cert(der, :otp)) do
      {:ok,
       %{
         "server_name" => name,
         "enrollment_url" => "https://#{name}:#{enroll_port}/enroll",
         "relay_url" => "wss://#{name}:#{relay_port}/relay",
         "ca_pem" => :public_key.pem_encode([{:Certificate, der, :not_encrypted}]),
         "ca_sha256" => Base.encode16(:crypto.hash(:sha256, der), case: :lower)
       }}
    else
      _ -> {:error, :invalid_join_configuration}
    end
  rescue
    error ->
      Diagnostics.failure(:server_startup, :configuration, error, :rejected)
      {:error, :invalid_join_configuration}
  end

  @doc "Fragment-only invitation: payload is not sent in an HTTP request or used as a resolver."
  def link(invitation) do
    "harklight://join#" <>
      Base.url_encode64(AttentionServer.JSON.encode!(invitation), padding: false)
  end

  def create(engine, person, owner, token_hash, configuration) do
    with {:ok, revision} <- Engine.owner_revision(engine, person, owner),
         true <- valid_configuration?(configuration),
         true <- hash?(token_hash) do
      engine = compact(engine)

      cond do
        Map.has_key?(engine.join_invitations, token_hash) ->
          {:error, :collision}

        true ->
          invite = %{
            person: person,
            owner_key: owner,
            owner_revision: revision,
            configuration: configuration,
            expires_at: nil,
            retain_until: nil,
            code: nil,
            key: nil,
            secret_hash: nil,
            request_id: nil
          }

          {:ok,
           %{
             engine
             | join_invitations: Map.put(engine.join_invitations, token_hash, invite)
           }, invite}
      end
    else
      {:error, :forbidden} -> {:error, :forbidden}
      _ -> {:error, :unavailable}
    end
  end

  @doc "Recover one owner action after a lost response without another active invitation."
  def create_for_request(engine, person, owner, request_id, configuration) do
    with {:ok, revision} <- Engine.owner_revision(engine, person, owner),
         true <- AttentionServer.Message.uuid_v4?(request_id),
         true <- valid_configuration?(configuration) do
      existing =
        Enum.find_value(engine.join_invitations, fn {_, item} ->
          if item.person == person and item.owner_key == owner and
               Map.get(item, :request_id) == request_id,
             do: item
        end)

      cond do
        existing &&
            (existing.owner_revision != revision or existing.configuration != configuration) ->
          {:error, :request_conflict}

        existing && not is_nil(existing.code) ->
          {:error, :used}

        existing ->
          {:ok, engine, existing, request_token(engine, existing, request_id)}

        true ->
          engine = compact(engine)

          binding = %{
            person: person,
            owner_key: owner,
            owner_revision: revision,
            expires_at: nil
          }

          token = request_token(engine, binding, request_id)
          hash = :crypto.hash(:sha256, token)

          with {:ok, next, invitation} <- create(engine, person, owner, hash, configuration) do
            invitation = %{invitation | request_id: request_id}
            next = %{next | join_invitations: Map.put(next.join_invitations, hash, invitation)}
            {:ok, next, invitation, token}
          end
      end
    else
      {:error, :forbidden} -> {:error, :forbidden}
      _ -> {:error, :invalid_request}
    end
  end

  # The existing durable random server key is domain-separated from its cursor
  # purpose. Only an authenticated owner operation can recover the bearer.
  # The stored invitation's own expiry field (nil on current invitations, a
  # deadline on older stored ones) stays in the binding so an existing request
  # keeps recovering the same link.
  defp request_token(engine, item, request_id) do
    binding =
      :erlang.term_to_binary(
        {item.person, item.owner_key, item.owner_revision, item.expires_at, request_id}
      )

    :crypto.mac(:hmac, :sha256, engine.agent_cursor_key, [
      "harklight-owner-invitation-v1",
      <<0>>,
      binding
    ])
  end

  def invitation_response(invite, token) do
    response =
      Map.merge(invite.configuration, %{
        "v" => 1,
        "person_id" => invite.person,
        "invite_token" => Base.encode16(token, case: :lower),
        "expires_at" => invite.expires_at
      })

    Map.put(response, "join_url", link(response))
  end

  @doc "A short HTTPS handoff keeps the bearer in the fragment, outside HTTP GETs."
  def bridge_link(origin, token)
      when is_binary(origin) and is_binary(token) and byte_size(token) == 64 do
    origin <> "/join#" <> token
  end

  @doc "Resolve an existing bearer for the joining person without issuing or using it."
  def resolve(engine, token, configuration) do
    with true <- is_binary(token) and Regex.match?(~r/\A[0-9a-f]{64}\z/, token),
         {:ok, raw} <- Base.decode16(token, case: :lower),
         invite when is_map(invite) <-
           Map.get(engine.join_invitations, :crypto.hash(:sha256, raw)),
         true <- invite.configuration == configuration,
         {:ok, revision} <- Engine.owner_revision(engine, invite.person, invite.owner_key),
         true <- revision == invite.owner_revision do
      if is_nil(invite.code),
        do: {:ok, invitation_response(invite, raw)},
        else: {:error, :used}
    else
      _ -> {:error, :unavailable}
    end
  end

  def redeem(engine, hash, certificate, label, secret_hash, code, configuration) do
    with true <- hash?(hash) and hash?(secret_hash),
         {:ok, key} <- DeviceIdentity.from_certificate(certificate),
         invite when is_map(invite) <- Map.get(engine.join_invitations, hash),
         true <- invite.configuration == configuration,
         {:ok, revision} <- Engine.owner_revision(engine, invite.person, invite.owner_key),
         true <- revision == invite.owner_revision do
      if is_nil(invite.code) do
        consume(engine, hash, invite, certificate, label, secret_hash, code)
      else
        retry(engine, invite, key, secret_hash)
      end
    else
      _ -> {:error, :refused}
    end
  end

  defp consume(engine, hash, invite, certificate, label, secret_hash, code) do
    with {:ok, pending} <- Engine.pending_enroll(engine, certificate, label, code, secret_hash) do
      item = Map.fetch!(pending.pending_enrollments, code)
      bound = item |> Map.put(:person, invite.person) |> Map.put(:joined, true)

      consumed = %{
        invite
        | code: code,
          key: item.key,
          secret_hash: secret_hash,
          retain_until: nil
      }

      next = %{
        pending
        | pending_enrollments: Map.put(pending.pending_enrollments, code, bound),
          join_invitations: Map.put(pending.join_invitations, hash, consumed)
      }

      {:ok, next, response(next, code, item)}
    end
  end

  defp retry(engine, invite, key, secret_hash) do
    if invite.key != key or invite.secret_hash != secret_hash do
      {:error, :used}
    else
      item =
        Map.get(engine.pending_enrollments, invite.code) ||
          Map.get(engine.pending_history, invite.code)

      if is_nil(item) or item.state == :expired,
        do: {:error, :expired},
        else: {:ok, engine, response(engine, invite.code, item)}
    end
  end

  defp response(engine, code, item) do
    {:ok, status} = Engine.pending_status(engine, item.secret_hash)

    Map.merge(status, %{
      "approval_code" => code,
      "expires_at" => item.expires_at,
      "status_expires_at" => item.status_expires_at
    })
  end

  @doc "Drop invitations whose owner authority has since changed."
  def compact(engine),
    do: %{
      engine
      | join_invitations:
          Map.reject(engine.join_invitations, fn {_, invite} ->
            Engine.owner_revision(engine, invite.person, invite.owner_key) !=
              {:ok, invite.owner_revision}
          end)
    }

  def valid_configuration?(
        %{
          "server_name" => name,
          "enrollment_url" => enrollment,
          "relay_url" => relay,
          "ca_pem" => ca,
          "ca_sha256" => pin
        } = config
      ) do
    with %URI{port: ep} <- URI.parse(enrollment),
         %URI{port: rp} <- URI.parse(relay),
         {:ok, expected} <- configuration(name, ep, rp, ca) do
      config == expected and is_binary(pin)
    else
      _ -> false
    end
  rescue
    error ->
      Diagnostics.failure(:server_storage, :store_open, error, :unavailable)
      false
  end

  def valid_configuration?(_), do: false

  def valid_state?(engine) do
    is_map(engine.join_invitations) and
      Enum.all?(engine.join_invitations, fn {hash, invite} ->
        hash?(hash) and map_size(invite) == 10 and Map.has_key?(engine.persons, invite.person) and
          key?(invite.owner_key) and is_integer(invite.owner_revision) and
          invite.owner_revision > 0 and valid_configuration?(invite.configuration) and
          (is_nil(invite.expires_at) or (is_integer(invite.expires_at) and invite.expires_at > 0)) and
          (is_nil(invite.retain_until) or is_integer(invite.retain_until)) and
          (is_nil(invite.request_id) or AttentionServer.Message.uuid_v4?(invite.request_id)) and
          valid_binding?(engine, invite)
      end)
  end

  defp valid_binding?(_engine, %{code: nil, key: nil, secret_hash: nil}), do: true

  defp valid_binding?(engine, i) do
    item = Map.get(engine.pending_enrollments, i.code) || Map.get(engine.pending_history, i.code)

    is_binary(i.code) and Regex.match?(~r/\A[A-Z2-7]{10}\z/, i.code) and key?(i.key) and
      hash?(i.secret_hash) and is_map(item) and item.key == i.key and
      item.secret_hash == i.secret_hash and
      (item.state == :expired or item.person == i.person)
  end

  defp hash?(hash), do: is_binary(hash) and byte_size(hash) == 32
  defp key?(key), do: is_binary(key) and Regex.match?(~r/\A[0-9a-f]{64}\z/, key)
end
