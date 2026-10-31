defmodule AttentionServer.APNS do
  @moduledoc false

  alias AttentionServer.{Diagnostics, JSON}
  require Logger

  @sandbox_host "api.sandbox.push.apple.com"
  @production_host "api.push.apple.com"
  @port 443
  @topic "co.clicketyclacks.Harklight"
  @profile_hosts %{
    "harklight-sandbox" => @sandbox_host,
    "harklight-production" => @production_host
  }
  @safe_reasons ~w(BadDeviceToken ExpiredProviderToken ExpiredToken Unregistered)
  @key_id_pattern ~r/\A[A-Z0-9]{10}\z/
  @team_id_pattern ~r/\A[A-Z0-9]{10}\z/
  @p256_oid {1, 2, 840, 10045, 3, 1, 7}
  @alert_payload %{
    "aps" => %{
      "alert" => %{"title" => "Harklight", "body" => "New attention is ready."},
      "content-available" => 1
    },
    "attention" => %{"v" => 1, "type" => "sync"}
  }
  @background_payload %{
    "aps" => %{"content-available" => 1},
    "attention" => %{"v" => 1, "type" => "sync"}
  }
  @max_payload_bytes 4_096
  @neutral_sender_label "Sender"
  @response_limit 16_384
  @connect_timeout 5_000
  @request_timeout 10_000

  def prepare!(opts) when is_list(opts) do
    team_id = Keyword.fetch!(opts, :team_id)
    topic = Keyword.get(opts, :topic, @topic)

    profiles =
      Keyword.get(opts, :profiles, %{
        "harklight-sandbox" => Keyword.take(opts, [:key_path, :key_id])
      })

    unless is_binary(team_id) and Regex.match?(@team_id_pattern, team_id) and topic == @topic and
             valid_profile_options?(profiles) do
      Diagnostics.failure(:apns_dispatch, :configuration, :invalid_configuration, :unavailable)
      raise ArgumentError, "invalid APNs configuration"
    end

    prepared_profiles =
      Map.new(profiles, fn {profile_id, profile_opts} ->
        {profile_id, prepare_profile!(profile_opts, team_id, topic)}
      end)

    if Map.has_key?(prepared_profiles, "harklight-production") and
         prepared_profiles["harklight-production"].key_id ==
           prepared_profiles["harklight-sandbox"].key_id do
      Diagnostics.failure(:apns_dispatch, :configuration, :invalid_configuration, :unavailable)
      raise ArgumentError, "invalid APNs configuration"
    end

    %{
      profiles: prepared_profiles,
      endpoint: Keyword.get(opts, :endpoint)
    }
  end

  defp valid_profile_options?(profiles) when is_map(profiles) do
    Map.has_key?(profiles, "harklight-sandbox") and
      Enum.all?(profiles, fn
        {profile_id, opts} when profile_id in ["harklight-sandbox", "harklight-production"] ->
          is_list(opts) and Keyword.keyword?(opts) and
            is_binary(Keyword.get(opts, :key_path)) and
            String.starts_with?(Keyword.get(opts, :key_path), "/") and
            is_binary(Keyword.get(opts, :key_id)) and
            Regex.match?(@key_id_pattern, Keyword.get(opts, :key_id))

        _ ->
          false
      end)
  end

  defp valid_profile_options?(_), do: false

  defp prepare_profile!(opts, team_id, topic) do
    %{
      key_id: Keyword.fetch!(opts, :key_id),
      team_id: team_id,
      topic: topic,
      private_key: read_private_key!(Keyword.fetch!(opts, :key_path))
    }
  end

  def profile_id?(profile_id), do: Map.has_key?(@profile_hosts, profile_id)

  def safe_reason?(reason), do: reason in @safe_reasons

  def send(%{profiles: profiles} = config, claim, cached_tokens) do
    now_ms = System.system_time(:millisecond)

    if claim.deadline_at <= now_ms do
      Diagnostics.failure(:apns_dispatch, :push_request, :expired, :closed, claim.claim_id)
      {outcome("expired", nil, nil, nil, 0, nil), cached_tokens}
    else
      profile_id = Map.get(claim, :profile_id)

      case Map.fetch(profiles, profile_id) do
        {:ok, profile_config} ->
          send_with_profile(config, profile_config, profile_id, claim, cached_tokens, now_ms)

        :error ->
          Diagnostics.failure(
            :apns_dispatch,
            :configuration,
            :invalid_configuration,
            :unavailable,
            claim.claim_id
          )

          {outcome("blocked_configuration", nil, nil, nil, 0, nil), cached_tokens}
      end
    end
  rescue
    error ->
      Diagnostics.failure(
        :apns_dispatch,
        :push_request,
        error,
        :unavailable,
        Map.get(claim, :claim_id)
      )

      {outcome("blocked_configuration", nil, nil, uuid_v4(), 0, nil), cached_tokens}
  end

  defp send_with_profile(config, profile_config, profile_id, claim, cached_tokens, now_ms) do
    cached_tokens = if is_map(cached_tokens), do: cached_tokens, else: %{}
    cached_profile_token = Map.get(cached_tokens, profile_id)

    {token, refreshed_profile_token} =
      cached_token(profile_config, cached_profile_token, div(now_ms, 1_000))

    next_cached_tokens = Map.put(cached_tokens, profile_id, refreshed_profile_token)
    request_config = Map.put(profile_config, :endpoint, config.endpoint)
    payload = request_payload(claim)

    case request(request_config, claim, token, payload) do
      {:ok, 403, _headers, body} ->
        if apns_reason(body) == "ExpiredProviderToken" do
          issued_at = div(System.system_time(:millisecond), 1_000)
          fresh = provider_token(profile_config, issued_at)

          case request(request_config, claim, fresh, payload) do
            {:ok, status, headers, response_body} ->
              result = response_outcome(status, headers, response_body, claim, now_ms)
              profile_token = %{token: fresh, issued_at: issued_at}

              {report_response(result, claim), Map.put(cached_tokens, profile_id, profile_token)}

            {:error, reason} ->
              Diagnostics.failure(
                :apns_dispatch,
                :push_request,
                reason,
                :retry,
                claim.claim_id
              )

              profile_token = %{token: fresh, issued_at: issued_at}

              {outcome("retry", nil, nil, uuid_v4(), 0, nil),
               Map.put(cached_tokens, profile_id, profile_token)}
          end
        else
          {response_outcome(403, [], body, claim, now_ms), next_cached_tokens}
        end

      {:ok, status, headers, body} ->
        {report_response(response_outcome(status, headers, body, claim, now_ms), claim),
         next_cached_tokens}

      {:error, reason} ->
        Diagnostics.failure(
          :apns_dispatch,
          :push_request,
          reason,
          :retry,
          claim.claim_id
        )

        {outcome("retry", nil, nil, uuid_v4(), 0, nil), next_cached_tokens}
    end
  end

  defp report_response(%{state: "provider_accepted"} = result, _claim), do: result

  defp report_response(%{state: "retry", reason: reason} = result, claim) do
    cause = if is_binary(reason), do: reason, else: Diagnostics.provider_cause(result.http_status)
    Diagnostics.failure(:apns_dispatch, :push_request, cause, :retry, claim.claim_id)
    result
  end

  defp report_response(%{reason: reason} = result, claim) do
    cause = if is_binary(reason), do: reason, else: Diagnostics.provider_cause(result.http_status)
    Diagnostics.failure(:apns_dispatch, :push_request, cause, :rejected, claim.claim_id)
    result
  end

  defp cached_token(_config, %{token: token, issued_at: issued_at} = cached, now_s)
       when is_binary(token) and is_integer(issued_at) and now_s >= issued_at and
              now_s - issued_at < 25 * 60,
       do: {token, cached}

  defp cached_token(config, _cached, now_s) do
    token = provider_token(config, now_s)
    {token, %{token: token, issued_at: now_s}}
  end

  defp provider_token(config, issued_at) do
    header = %{"alg" => "ES256", "kid" => config.key_id}
    claims = %{"iss" => config.team_id, "iat" => issued_at}
    unsigned = base64url(JSON.encode!(header)) <> "." <> base64url(JSON.encode!(claims))
    der_signature = :public_key.sign(unsigned, :sha256, config.private_key)
    {r, s} = :public_key.der_decode(:"ECDSA-Sig-Value", der_signature) |> signature_pair()
    raw_signature = <<r::unsigned-big-integer-size(256), s::unsigned-big-integer-size(256)>>
    unsigned <> "." <> base64url(raw_signature)
  end

  defp signature_pair({:"ECDSA-Sig-Value", r, s}), do: {r, s}
  defp signature_pair({r, s}), do: {r, s}

  defp request(config, claim, token, payload) do
    endpoint = endpoint(config, Map.get(claim, :profile_id))
    request_id = uuid_v4()
    path = "/3/device/" <> claim.address

    # Apple ignores the badge on background pushes, so a badge-carrying sync
    # with no alert text goes as a silent alert-type push.
    {push_type, priority, collapse_id} =
      case {claim.presentation, is_integer(Map.get(claim, :badge))} do
        {:alert, _} -> {"alert", "10", "attention-sync"}
        {:background, true} -> {"alert", "10", "attention-terminal-sync"}
        {:background, false} -> {"background", "5", "attention-terminal-sync"}
      end

    headers = [
      {"authorization", "bearer " <> token},
      {"content-type", "application/json"},
      {"apns-topic", config.topic},
      {"apns-push-type", push_type},
      {"apns-priority", priority},
      {"apns-collapse-id", collapse_id},
      {"apns-id", request_id},
      {"apns-expiration", Integer.to_string(div(claim.deadline_at, 1_000))}
    ]

    with {:ok, conn} <-
           Mint.HTTP2.connect(:https, endpoint.address, endpoint.port,
             hostname: endpoint.hostname,
             timeout: @connect_timeout,
             mode: :passive,
             transport_opts: [verify: :verify_peer, cacerts: endpoint.cacerts]
           ),
         {:ok, conn, ref} <-
           Mint.HTTP2.request(conn, "POST", path, headers, JSON.encode!(payload)),
         {:ok, conn, status, response_headers, body} <- receive_response(conn, ref) do
      _ = Mint.HTTP2.close(conn)
      {:ok, status, response_headers, body}
    else
      {:error, conn, reason} ->
        _ = Mint.HTTP2.close(conn)
        {:error, reason}

      {:error, reason} ->
        {:error, reason}
    end
  rescue
    error -> {:error, error}
  end

  defp request_payload(%{presentation: :background} = claim),
    do: put_badge(@background_payload, Map.get(claim, :badge))

  defp request_payload(
         %{
           presentation: :alert,
           alert: %{"title" => title},
           grouped_alert_names: names
         } = claim
       )
       when is_list(names) do
    locator = Map.get(claim, :locator)
    locator = if valid_locator?(locator), do: locator, else: nil

    case fit_grouped_alert_payload(title, names, locator, Map.get(claim, :badge)) do
      {:ok, payload} ->
        payload

      :error ->
        raise ArgumentError, "APNs grouped alert could not fit the payload limit"
    end
  end

  defp request_payload(%{presentation: :alert, alert: %{"title" => title} = alert} = claim) do
    body = Map.get(alert, "body")
    preview_truncated? = Map.get(alert, "body_truncated", false)
    preserve_title? = Map.get(claim, :preserve_alert_title, false)
    preserve_body? = Map.get(claim, :preserve_alert_body, false)
    locator = Map.get(claim, :locator)
    locator = if valid_locator?(locator), do: locator, else: nil
    badge = Map.get(claim, :badge)

    case fit_alert_with_optional_locator(
           title,
           body,
           preview_truncated?,
           locator,
           badge,
           preserve_title?,
           preserve_body?
         ) do
      {:ok, payload} ->
        payload

      :error when preserve_title? ->
        case fit_alert_with_optional_locator(
               @neutral_sender_label,
               body,
               preview_truncated?,
               locator,
               badge,
               false,
               false
             ) do
          {:ok, payload} ->
            payload

          :error ->
            raise ArgumentError, "APNs alert text could not fit the payload limit"
        end

      :error ->
        raise ArgumentError, "APNs alert text could not fit the payload limit"
    end
  end

  defp request_payload(claim), do: put_badge(@alert_payload, Map.get(claim, :badge))

  defp fit_alert_with_optional_locator(
         title,
         body,
         preview_truncated?,
         locator,
         badge,
         preserve_title?,
         preserve_body?
       ) do
    case fit_alert_payload(
           title,
           body,
           preview_truncated?,
           locator,
           badge,
           preserve_title?,
           preserve_body?
         ) do
      {:ok, payload} ->
        {:ok, payload}

      :error when not is_nil(locator) ->
        Logger.warning("event=harklight_apns_tap_locator outcome=omitted_payload_limit")

        fit_alert_payload(
          title,
          body,
          preview_truncated?,
          nil,
          badge,
          preserve_title?,
          preserve_body?
        )

      :error ->
        :error
    end
  end

  defp fit_grouped_alert_payload(title, names, locator, badge) do
    candidates =
      [
        {fit_grouped_names(title, names, locator, badge), not is_nil(locator)},
        {fit_grouped_names(title, names, nil, badge), false}
      ]
      |> Enum.uniq_by(fn {result, has_locator?} -> {result, has_locator?} end)
      |> Enum.filter(fn {result, _has_locator?} -> match?({:ok, _, _}, result) end)

    case Enum.max_by(
           candidates,
           fn {{:ok, _payload, shown_count}, has_locator?} ->
             {shown_count, has_locator?}
           end,
           fn -> nil end
         ) do
      {{:ok, payload, _shown_count}, has_locator?} ->
        if not is_nil(locator) and not has_locator? do
          Logger.warning("event=harklight_apns_tap_locator outcome=omitted_payload_limit")
        end

        {:ok, payload}

      nil ->
        case fit_grouped_names("Harklight", names, nil, badge) do
          {:ok, payload, _shown_count} -> {:ok, payload}
          :error -> :error
        end
    end
  end

  defp fit_grouped_names(title, names, locator, badge) do
    total_names = length(names)
    initial_body = grouped_alert_body([], total_names)
    initial_payload = alert_payload(title, initial_body, locator, badge)

    if payload_size(initial_payload) > @max_payload_bytes do
      :error
    else
      {shown_reversed, payload} =
        Enum.reduce_while(names, {[], initial_payload}, fn name, {shown_reversed, _payload} ->
          shown = Enum.reverse([name | shown_reversed])
          remaining = total_names - length(shown)
          body = grouped_alert_body(shown, remaining)
          candidate = alert_payload(title, body, locator, badge)

          if payload_size(candidate) <= @max_payload_bytes do
            {:cont, {[name | shown_reversed], candidate}}
          else
            {:halt,
             {shown_reversed,
              alert_payload(
                title,
                grouped_alert_body(
                  Enum.reverse(shown_reversed),
                  total_names - length(shown_reversed)
                ),
                locator,
                badge
              )}}
          end
        end)

      {:ok, payload, length(shown_reversed)}
    end
  end

  defp grouped_alert_body([], remaining), do: "#{remaining} senders"
  defp grouped_alert_body(names, 0), do: Enum.join(names, ", ")

  defp grouped_alert_body(names, remaining) do
    Enum.join(names, ", ") <> ", and #{remaining} more"
  end

  defp fit_alert_payload(
         title,
         body,
         preview_truncated?,
         locator,
         badge,
         preserve_title?,
         preserve_body?
       ) do
    payload = alert_payload(title, body, locator, badge)

    if payload_size(payload) <= @max_payload_bytes do
      {:ok, payload}
    else
      body_fit =
        if is_binary(body) and not preserve_body?,
          do: fit_body(title, body, preview_truncated?, locator, badge),
          else: :error

      case body_fit do
        {:ok, bounded_body} ->
          {:ok, alert_payload(title, bounded_body, locator, badge)}

        :error ->
          title_fit =
            if preserve_title?, do: :error, else: fit_title(title, body, locator, badge)

          case title_fit do
            {:ok, bounded_title} ->
              {:ok, alert_payload(bounded_title, body, locator, badge)}

            :error ->
              fallback_body =
                if is_binary(body) and not preserve_body?, do: "…", else: body

              fallback_title_fit =
                if preserve_title?,
                  do: :error,
                  else: fit_title(title, fallback_body, locator, badge)

              case fallback_title_fit do
                {:ok, bounded_title} ->
                  payload = alert_payload(bounded_title, fallback_body, locator, badge)
                  if payload_size(payload) <= @max_payload_bytes, do: {:ok, payload}, else: :error

                :error ->
                  :error
              end
          end
      end
    end
  end

  defp alert_payload(title, body, locator, badge) do
    attention = %{"v" => 1, "type" => "sync"}
    attention = if locator, do: Map.put(attention, "locator", locator), else: attention
    alert = %{"title" => title}
    alert = if is_binary(body), do: Map.put(alert, "body", body), else: alert

    put_badge(
      %{
        "aps" => %{"alert" => alert, "content-available" => 1},
        "attention" => attention
      },
      badge
    )
  end

  # The standard APNs badge field carries the recipient's Inbox count. Claims
  # without a count (older callers) leave the badge untouched.
  defp put_badge(payload, badge) when is_integer(badge) and badge >= 0,
    do: put_in(payload, ["aps", "badge"], badge)

  defp put_badge(payload, _badge), do: payload

  defp fit_body(title, body, preview_truncated?, locator, badge) do
    body_graphemes = String.graphemes(body)

    retained_graphemes =
      if preview_truncated? and List.last(body_graphemes) == "…",
        do: Enum.drop(body_graphemes, -1),
        else: body_graphemes

    full_body = if preview_truncated?, do: body, else: nil

    largest_fitting_prefix(length(retained_graphemes), fn count ->
      candidate =
        cond do
          count == length(retained_graphemes) and not is_nil(full_body) -> full_body
          count == length(retained_graphemes) -> body
          true -> (Enum.take(retained_graphemes, count) ++ ["…"]) |> Enum.join()
        end

      if payload_size(alert_payload(title, candidate, locator, badge)) <= @max_payload_bytes,
        do: {:ok, candidate},
        else: :too_large
    end)
  end

  defp fit_title(title, body, locator, badge) do
    title_graphemes = String.graphemes(title)

    case largest_fitting_prefix(length(title_graphemes), fn count ->
           candidate = (Enum.take(title_graphemes, count) ++ ["…"]) |> Enum.join()

           if payload_size(alert_payload(candidate, body, locator, badge)) <= @max_payload_bytes,
             do: {:ok, candidate},
             else: :too_large
         end) do
      {:ok, candidate} -> {:ok, candidate}
      :error -> :error
    end
  end

  defp largest_fitting_prefix(maximum, candidate) do
    case candidate.(0) do
      {:ok, value} -> {:ok, search_prefix(0, maximum, candidate, value)}
      :too_large -> :error
    end
  end

  defp search_prefix(low, high, _candidate, best) when low >= high, do: best

  defp search_prefix(low, high, candidate, best) do
    count = div(low + high + 1, 2)

    case candidate.(count) do
      {:ok, value} -> search_prefix(count, high, candidate, value)
      :too_large -> search_prefix(low, count - 1, candidate, best)
    end
  end

  defp payload_size(payload), do: payload |> JSON.encode!() |> byte_size()

  defp valid_locator?(
         %{
           "v" => 1,
           "binding_id" => binding_id,
           "person_id" => person_id,
           "device_id" => device_id,
           "sender_id" => sender_id,
           "message_id" => message_id,
           "acceptance_seq" => acceptance_seq
         } = locator
       ) do
    map_size(locator) == 7 and AttentionServer.Message.uuid_v4?(binding_id) and
      Enum.all?([person_id, device_id, sender_id, message_id], &valid_locator_id?/1) and
      is_integer(acceptance_seq) and acceptance_seq > 0
  end

  defp valid_locator?(_), do: false

  defp valid_locator_id?(value),
    do: is_binary(value) and byte_size(value) > 0 and String.valid?(value)

  defp endpoint(%{endpoint: endpoint}, profile_id) when is_map(endpoint) do
    selected =
      if valid_endpoint?(endpoint) do
        endpoint
      else
        Map.get(endpoint, profile_id)
      end

    if valid_endpoint?(selected) do
      selected
    else
      raise ArgumentError, "invalid APNs endpoint for profile"
    end
  end

  defp endpoint(_config, profile_id) do
    case Map.fetch(@profile_hosts, profile_id) do
      {:ok, host} ->
        %{address: host, hostname: host, port: @port, cacerts: :public_key.cacerts_get()}

      :error ->
        raise ArgumentError, "invalid APNs profile"
    end
  end

  defp valid_endpoint?(%{address: _address, hostname: _hostname, port: _port, cacerts: cacerts})
       when is_list(cacerts),
       do: true

  defp valid_endpoint?(_), do: false

  defp receive_response(conn, ref) do
    deadline = System.monotonic_time(:millisecond) + @request_timeout
    receive_response(conn, ref, nil, [], <<>>, false, deadline)
  end

  defp receive_response(conn, ref, status, headers, body, too_large, deadline) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    case Mint.HTTP2.recv(conn, 0, timeout) do
      {:ok, next, events} ->
        {status, headers, body, too_large, done?} =
          Enum.reduce(events, {status, headers, body, too_large, false}, fn event,
                                                                            {s, h, b, large, done} ->
            case event do
              {:status, ^ref, value} ->
                {value, h, b, large, done}

              {:headers, ^ref, value} ->
                {s, value, b, large, done}

              {:data, ^ref, value} when byte_size(b) + byte_size(value) <= @response_limit ->
                {s, h, b <> value, large, done}

              {:data, ^ref, _value} ->
                {s, h, b, true, done}

              {:done, ^ref} ->
                {s, h, b, large, true}

              _ ->
                {s, h, b, large, done}
            end
          end)

        if done? do
          if is_integer(status) and not too_large,
            do: {:ok, next, status, headers, body},
            else: {:error, next, :invalid_response}
        else
          receive_response(next, ref, status, headers, body, too_large, deadline)
        end

      {:error, next, error, _responses} ->
        {:error, next, error}

      _ ->
        {:error, conn, :invalid_response}
    end
  end

  defp response_outcome(200, _headers, _body, _claim, _now_ms),
    do: outcome("provider_accepted", 200, nil, nil, 0, nil)

  defp response_outcome(410, _headers, body, claim, _now_ms) do
    case {apns_reason(body), response_timestamp(body)} do
      {reason, timestamp}
      when reason in ["Unregistered", "ExpiredToken"] and
             is_integer(timestamp) and timestamp >= claim.registered_at ->
        outcome("endpoint_invalid", 410, reason, nil, 0, timestamp)

      {reason, _timestamp} when reason in ["Unregistered", "ExpiredToken"] ->
        outcome("blocked_configuration", 410, reason, nil, 0, nil)

      {reason, _timestamp} ->
        outcome("blocked_configuration", 410, reason, nil, 0, nil)
    end
  end

  defp response_outcome(400, _headers, body, claim, _now_ms) do
    case apns_reason(body) do
      "BadDeviceToken" ->
        outcome("endpoint_invalid", 400, "BadDeviceToken", nil, 0, claim.registered_at)

      reason ->
        outcome("blocked_configuration", 400, reason, nil, 0, nil)
    end
  end

  defp response_outcome(status, headers, body, _claim, now_ms)
       when status == 429 or status in 500..599 do
    outcome("retry", status, apns_reason(body), nil, retry_after_ms(headers, now_ms), nil)
  end

  defp response_outcome(status, _headers, body, _claim, _now_ms),
    do: outcome("blocked_configuration", status, apns_reason(body), nil, 0, nil)

  defp apns_reason(body) do
    case JSON.decode_line(body <> "\n") do
      {:ok, %{"reason" => reason}} when is_binary(reason) ->
        if safe_reason?(reason), do: reason, else: nil

      _ ->
        nil
    end
  end

  defp response_timestamp(body) do
    case JSON.decode_line(body <> "\n") do
      {:ok, %{"timestamp" => timestamp}} when is_integer(timestamp) and timestamp >= 0 ->
        timestamp

      _ ->
        nil
    end
  end

  defp retry_after_ms(headers, _now_ms) do
    case Enum.find_value(headers, fn {key, value} ->
           if String.downcase(key) == "retry-after", do: value
         end) do
      value when is_binary(value) ->
        case Integer.parse(value) do
          {seconds, ""} when seconds >= 0 and seconds <= 86_400 -> seconds * 1_000
          _ -> 0
        end

      _ ->
        0
    end
  end

  defp outcome(state, http_status, reason, request_id, retry_after_ms, invalidated_at_ms) do
    %{
      state: state,
      http_status: http_status,
      reason: reason,
      request_id: request_id,
      retry_after_ms: retry_after_ms,
      invalidated_at_ms: invalidated_at_ms
    }
  end

  defp read_private_key!(path) do
    with {:ok, stat} <- File.lstat(path),
         true <- stat.type == :regular and Bitwise.band(stat.mode, 0o077) == 0,
         true <- stat.size in 1..8_192,
         {:ok, pem} <- File.read(path),
         [{type, _der, :not_encrypted} = entry] when type in [:PrivateKeyInfo, :ECPrivateKey] <-
           :public_key.pem_decode(pem),
         {:ok, key} <- decode_private_key(type, entry) do
      key
    else
      {:error, reason} ->
        Diagnostics.failure(:apns_dispatch, :configuration, reason, :unavailable)
        raise ArgumentError, "unavailable protected APNs key"

      _ ->
        Diagnostics.failure(
          :apns_dispatch,
          :configuration,
          :invalid_configuration,
          :unavailable
        )

        raise ArgumentError, "unavailable protected APNs key"
    end
  rescue
    error ->
      Diagnostics.failure(:apns_dispatch, :configuration, error, :unavailable)
      raise ArgumentError, "unavailable protected APNs key"
  end

  defp decode_private_key(_type, entry) do
    case :public_key.pem_entry_decode(entry) do
      {:ECPrivateKey, _version, private, {:namedCurve, @p256_oid}, _public, _attributes} = key
      when is_binary(private) and byte_size(private) == 32 ->
        {:ok, key}

      _ ->
        {:error, :invalid_key}
    end
  end

  defp base64url(value), do: Base.url_encode64(value, padding: false)

  defp uuid_v4 do
    <<a::binary-size(6), b::8, c::8, d::8, e::binary-size(7)>> = :crypto.strong_rand_bytes(16)
    b = Bitwise.bor(Bitwise.band(b, 15), 64)
    d = Bitwise.bor(Bitwise.band(d, 63), 128)
    hex = Base.encode16(<<a::binary, b, c, d, e::binary>>, case: :lower)

    Enum.join(
      [
        String.slice(hex, 0, 8),
        String.slice(hex, 8, 4),
        String.slice(hex, 12, 4),
        String.slice(hex, 16, 4),
        String.slice(hex, 20, 12)
      ],
      "-"
    )
  end
end
