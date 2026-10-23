defmodule AttentionServer.GuidedJoin do
  @moduledoc """
  Person-bound, owner-created invitations. Only hashes of redemption and status
  secrets enter durable state. An invitation authorizes a pending request, never
  enrollment or sender rights; approval remains a separate current-owner act.
  """

  alias AttentionServer.{DeviceIdentity, Diagnostics, Engine}

  @invite_ms 30 * 60 * 1_000
  @retention_ms 24 * 60 * 60 * 1_000
  @limit 1_024

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

  def create(engine, person, owner, token_hash, configuration, now) do
    with {:ok, revision} <- Engine.owner_revision(engine, person, owner),
         true <- valid_configuration?(configuration),
         true <- hash?(token_hash),
         true <- is_integer(now) and now >= engine.pending_clock_high_water do
      engine = compact(engine, now)

      cond do
        map_size(engine.join_invitations) >= @limit ->
          {:error, :capacity}

        Map.has_key?(engine.join_invitations, token_hash) ->
          {:error, :collision}

        true ->
          invite = %{
            person: person,
            owner_key: owner,
            owner_revision: revision,
            configuration: configuration,
            expires_at: now + @invite_ms,
            retain_until: now + @invite_ms + @retention_ms,
            code: nil,
            key: nil,
            secret_hash: nil,
            request_id: nil
          }

          {:ok,
           %{
             engine
             | join_invitations: Map.put(engine.join_invitations, token_hash, invite),
               pending_clock_high_water: now
           }, invite}
      end
    else
      {:error, :forbidden} -> {:error, :forbidden}
      _ -> {:error, :unavailable}
    end
  end

  @doc "Recover one owner action after a lost response without another active invitation."
  def create_for_request(engine, person, owner, request_id, configuration, now) do
    with {:ok, revision} <- Engine.owner_revision(engine, person, owner),
         true <- AttentionServer.Message.uuid_v4?(request_id),
         true <- valid_configuration?(configuration),
         true <- is_integer(now) and now >= engine.pending_clock_high_water do
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

        existing && now >= existing.expires_at ->
          observed = %{
            engine
            | pending_clock_high_water: max(engine.pending_clock_high_water, existing.expires_at)
          }

          {:error, :expired, observed}

        existing ->
          {:ok, engine, existing, request_token(engine, existing, request_id)}

        true ->
          binding = %{
            person: person,
            owner_key: owner,
            owner_revision: revision,
            expires_at: now + @invite_ms
          }

          token = request_token(engine, binding, request_id)
          hash = :crypto.hash(:sha256, token)

          with {:ok, next, invitation} <- create(engine, person, owner, hash, configuration, now) do
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
  # Including the original deadline prevents a reclaimed request ID from ever
  # reactivating an older shared link.
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
  def resolve(engine, token, configuration, now) do
    with true <- is_binary(token) and Regex.match?(~r/\A[0-9a-f]{64}\z/, token),
         {:ok, raw} <- Base.decode16(token, case: :lower),
         invite when is_map(invite) <-
           Map.get(engine.join_invitations, :crypto.hash(:sha256, raw)),
         true <- invite.configuration == configuration,
         {:ok, revision} <- Engine.owner_revision(engine, invite.person, invite.owner_key),
         true <- revision == invite.owner_revision,
         true <- is_integer(now) and now >= engine.pending_clock_high_water do
      cond do
        not is_nil(invite.code) -> {:error, :used}
        now >= invite.expires_at -> {:error, :expired}
        true -> {:ok, invitation_response(invite, raw)}
      end
    else
      _ -> {:error, :unavailable}
    end
  end

  def redeem(engine, hash, certificate, label, secret_hash, code, configuration, now) do
    with true <- hash?(hash) and hash?(secret_hash),
         {:ok, key} <- DeviceIdentity.from_certificate(certificate),
         invite when is_map(invite) <- Map.get(engine.join_invitations, hash),
         true <- invite.configuration == configuration,
         {:ok, revision} <- Engine.owner_revision(engine, invite.person, invite.owner_key),
         true <- revision == invite.owner_revision,
         true <- is_integer(now) and now >= engine.pending_clock_high_water do
      if is_nil(invite.code) do
        consume(engine, hash, invite, certificate, label, secret_hash, code, now)
      else
        retry(engine, invite, key, secret_hash, now)
      end
    else
      _ -> {:error, :refused}
    end
  end

  defp consume(engine, hash, invite, certificate, label, secret_hash, code, now) do
    if now >= invite.expires_at do
      {:error, :expired}
    else
      with {:ok, pending} <-
             Engine.pending_enroll(engine, certificate, label, code, secret_hash, now) do
        item = Map.fetch!(pending.pending_enrollments, code)
        bound = item |> Map.put(:person, invite.person) |> Map.put(:joined, true)

        consumed = %{
          invite
          | code: code,
            key: item.key,
            secret_hash: secret_hash,
            retain_until: item.status_expires_at
        }

        next = %{
          pending
          | pending_enrollments: Map.put(pending.pending_enrollments, code, bound),
            join_invitations: Map.put(pending.join_invitations, hash, consumed)
        }

        {:ok, next, response(next, code, item)}
      end
    end
  end

  defp retry(engine, invite, key, secret_hash, now) do
    cond do
      invite.key != key or invite.secret_hash != secret_hash ->
        {:error, :used}

      true ->
        item =
          Map.get(engine.pending_enrollments, invite.code) ||
            Map.get(engine.pending_history, invite.code)

        cond do
          is_nil(item) or now >= item.status_expires_at ->
            {:error, :expired}

          item.state == :expired or (item.state == :pending and now >= item.expires_at) ->
            {:error, :expired}

          true ->
            {:ok, engine, response(engine, invite.code, item)}
        end
    end
  end

  defp response(engine, code, item) do
    {:ok, status} =
      Engine.pending_status(engine, item.secret_hash, engine.pending_clock_high_water)

    Map.merge(status, %{
      "approval_code" => code,
      "expires_at" => item.expires_at,
      "status_expires_at" => item.status_expires_at
    })
  end

  def observe_time(engine, hash, now) do
    case Map.get(engine.join_invitations, hash) do
      nil ->
        engine

      %{code: nil, expires_at: deadline} when now >= deadline ->
        %{engine | pending_clock_high_water: max(engine.pending_clock_high_water, deadline)}

      %{code: code} when is_binary(code) ->
        Engine.pending_observe_time(engine, {:code, code}, now)

      _ ->
        engine
    end
  end

  def compact(engine, now),
    do: %{
      engine
      | join_invitations:
          Map.reject(engine.join_invitations, fn {_, i} ->
            i.retain_until <= now
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
    is_map(engine.join_invitations) and map_size(engine.join_invitations) <= @limit and
      Enum.all?(engine.join_invitations, fn {hash, i} ->
        hash?(hash) and map_size(i) == 10 and Map.has_key?(engine.persons, i.person) and
          key?(i.owner_key) and is_integer(i.owner_revision) and i.owner_revision > 0 and
          valid_configuration?(i.configuration) and is_integer(i.expires_at) and i.expires_at > 0 and
          is_integer(i.retain_until) and i.retain_until > i.expires_at and
          (is_nil(i.request_id) or AttentionServer.Message.uuid_v4?(i.request_id)) and
          valid_binding?(engine, i)
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
