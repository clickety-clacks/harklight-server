defmodule AttentionServer.Diagnostics do
  @moduledoc false

  require Logger

  alias AttentionServer.Message

  @operations ~w(device_relay origin_https enrollment_https owner_tls agent_https agent_tls apns_dispatch upload_cleanup dashboard_http server_storage server_startup)a
  @phases ~w(listener_start listener_address accept worker_start socket_control server_terminate tls_handshake peer_certificate identity authentication session_setup relay_boundary websocket_upgrade request_read request_dispatch response_write response_drain stream_attach stream_detach stream_status stream_write stream_read upload_begin upload_read upload_commit file_fetch file_read file_cleanup push_claim push_request push_finish configuration startup_cleanup periodic_cleanup state_commit upload_stage upload_publish upload_verify upload_delete store_open store_close)a
  @outcomes ~w(failed retry retry_100ms closed rejected response_unknown outcome_unknown unavailable canceled)a

  @causes %{
    timeout: "timeout",
    etimedout: "network_timeout",
    closed: "closed",
    econnrefused: "connection_refused",
    econnreset: "connection_reset",
    eaddrinuse: "address_in_use",
    eafnosupport: "address_family_unsupported",
    eprotonosupport: "protocol_unsupported",
    ehostunreach: "host_unreachable",
    enetunreach: "network_unreachable",
    enotconn: "not_connected",
    eacces: "access_denied",
    enoent: "not_found",
    enospc: "no_space",
    enomem: "no_memory",
    ebadf: "bad_descriptor",
    einval: "invalid_argument",
    eexist: "already_exists",
    eperm: "permission_denied",
    erofs: "read_only_filesystem",
    system_limit: "system_limit",
    file_exists: "already_exists",
    busy: "busy",
    shutdown: "shutdown",
    canceled: "canceled",
    cancelled: "canceled",
    normal: "normal",
    certificate_required: "client_certificate_missing",
    no_peercert: "client_certificate_missing",
    unknown_ca: "untrusted_certificate",
    bad_certificate: "client_certificate_rejected",
    unsupported_certificate: "client_certificate_unsupported",
    handshake_failure: "tls_handshake_failed",
    protocol_version: "tls_version_mismatch",
    insufficient_security: "tls_security_rejected",
    bad_record_mac: "tls_record_rejected",
    unexpected_message: "tls_unexpected_message",
    decode_error: "tls_decode_error",
    internal_error: "tls_internal_error",
    bad_upgrade: "websocket_upgrade_rejected",
    bad_frame: "protocol_frame_rejected",
    invalid_json: "invalid_json",
    storage_unavailable: "storage_unavailable",
    incomplete_upload: "incomplete_upload",
    upload_conflict: "upload_conflict",
    upload_discarded: "upload_discarded",
    upload_integrity_mismatch: "upload_integrity_mismatch",
    invalid_state: "invalid_state",
    invalid_certificate: "invalid_certificate",
    invalid_request: "invalid_request",
    invalid_configuration: "invalid_configuration",
    missing_configuration: "missing_configuration",
    process_unavailable: "process_unavailable",
    forbidden: "forbidden",
    not_found: "not_found",
    expired: "expired",
    used: "used",
    capacity: "capacity",
    refused: "refused",
    stale_request: "stale_request",
    request_conflict: "request_conflict",
    clock_unavailable: "clock_unavailable",
    not_supported: "not_supported",
    invalid_session: "invalid_session",
    invalid_link: "invalid_link",
    bridge_unavailable: "bridge_unavailable",
    key_pending: "key_pending",
    key_in_use: "key_in_use",
    relay: "missing_relay_configuration",
    certfile: "missing_certificate_configuration",
    keyfile: "missing_key_configuration",
    cacertfile: "missing_ca_configuration",
    upload_cap_exceeded: "upload_cap_exceeded",
    method_not_allowed: "method_not_allowed",
    too_large: "too_large",
    provider_retry_response: "provider_retry_response",
    provider_rejected_response: "provider_rejected_response",
    provider_bad_request: "provider_bad_request",
    provider_authentication_rejected: "provider_authentication_rejected",
    provider_endpoint_gone: "provider_endpoint_gone",
    provider_rate_limited: "provider_rate_limited",
    provider_server_error: "provider_server_error",
    resync_required: "resync_required",
    conversation_open_required: "conversation_open_required"
  }

  @string_causes Map.merge(
                   Map.new(@causes, fn {reason, classification} ->
                     {Atom.to_string(reason), classification}
                   end),
                   %{
                     "BadDeviceToken" => "apns_bad_device_token",
                     "ExpiredProviderToken" => "apns_expired_provider_token",
                     "ExpiredToken" => "apns_expired_token",
                     "Unregistered" => "apns_unregistered",
                     "unsupported_version" => "invalid_request",
                     "invalid_field" => "invalid_request",
                     "invalid_upload" => "invalid_request",
                     "bad_request" => "invalid_request",
                     "bad_frame" => "protocol_frame_rejected",
                     "protocol" => "protocol_frame_rejected"
                   }
                 )

  @exceptions %{
    ArgumentError => "argument_error",
    BadArityError => "bad_arity",
    BadBooleanError => "bad_boolean",
    BadMapError => "bad_map",
    FunctionClauseError => "function_clause",
    KeyError => "key_error",
    MatchError => "match_error",
    Protocol.UndefinedError => "protocol_undefined",
    RuntimeError => "runtime_error",
    UndefinedFunctionError => "undefined_function"
  }

  @doc false
  def failure(operation, phase, reason, outcome \\ :failed, operation_id \\ nil)
      when operation in @operations and phase in @phases and outcome in @outcomes do
    cause = cause_class(reason)
    op_field = if Message.uuid_v4?(operation_id), do: " operation_id=#{operation_id}", else: ""

    Logger.warning(
      "event=harklight_failure operation=#{operation} phase=#{phase} cause=#{cause} outcome=#{outcome}#{op_field}"
    )

    :ok
  end

  @doc false
  def operation_id(%{} = request) do
    id =
      Map.get(request, "op_id") || Map.get(request, :op_id) || Map.get(request, "request_id") ||
        Map.get(request, :request_id)

    if Message.uuid_v4?(id), do: id, else: nil
  end

  def operation_id(_), do: nil

  @doc false
  def response_error(operation, phase, %{"type" => "error", "reason" => reason}, operation_id)
      when is_binary(reason) do
    log_response_error(operation, phase, reason, operation_id)
  end

  def response_error(operation, phase, %{"error" => reason}, operation_id)
      when is_binary(reason) do
    log_response_error(operation, phase, reason, operation_id)
  end

  def response_error(_operation, _phase, _response, _operation_id), do: :ok

  defp log_response_error(operation, phase, reason, operation_id) do
    outcome =
      if reason in ["unavailable", "storage_unavailable", "indeterminate_submission"],
        do: :outcome_unknown,
        else: :rejected

    failure(operation, phase, reason, outcome, operation_id)
  end

  @doc false
  def provider_cause(400), do: :provider_bad_request
  def provider_cause(403), do: :provider_authentication_rejected
  def provider_cause(410), do: :provider_endpoint_gone
  def provider_cause(429), do: :provider_rate_limited

  def provider_cause(status) when is_integer(status) and status in 500..599,
    do: :provider_server_error

  def provider_cause(_), do: :provider_rejected_response

  @doc false
  def result(operation, phase, result, outcome \\ :failed, operation_id \\ nil)

  def result(operation, phase, {:error, reason} = result, :automatic, operation_id) do
    failure(operation, phase, reason, failure_outcome(phase, reason), operation_id)
    result
  end

  def result(operation, phase, {:error, reason} = result, outcome, operation_id) do
    failure(operation, phase, reason, outcome, operation_id)
    result
  end

  def result(_operation, _phase, result, _outcome, _operation_id), do: result

  @doc false
  def cause_class({:tls_alert, {reason, _description}}), do: tls_cause(reason)
  def cause_class({:tls_alert, reason}), do: tls_cause(reason)
  def cause_class({:timeout, _context}), do: "timeout"
  def cause_class({:noproc, _context}), do: "process_unavailable"
  def cause_class({:nodedown, _context}), do: "process_unavailable"
  def cause_class({:error, _connection, reason}), do: cause_class(reason)
  def cause_class({:error, reason}), do: cause_class(reason)
  def cause_class({:shutdown, reason}), do: cause_class(reason)

  def cause_class(%{__struct__: Mint.TransportError, reason: reason}), do: cause_class(reason)
  def cause_class(%{__struct__: Mint.HTTPError, reason: reason}), do: cause_class(reason)

  def cause_class(%{__exception__: true, __struct__: module}) do
    Map.get(@exceptions, module, "exception_unknown")
  end

  def cause_class(reason) when is_binary(reason), do: Map.get(@string_causes, reason, "unknown")
  def cause_class(reason) when is_atom(reason), do: Map.get(@causes, reason, "unknown")
  def cause_class(_), do: "unknown"

  defp failure_outcome(phase, reason)
       when reason in [:storage_unavailable, :timeout, :closed] and
              phase in [
                :request_dispatch,
                :response_write,
                :state_commit,
                :upload_commit,
                :push_finish
              ],
       do: :outcome_unknown

  defp failure_outcome(_phase, reason) when reason in [:timeout, :closed], do: :closed
  defp failure_outcome(_phase, _reason), do: :rejected

  defp tls_cause(reason) when is_atom(reason) do
    case cause_class(reason) do
      "unknown" -> "tls_alert_unknown"
      value -> value
    end
  end

  defp tls_cause(_), do: "tls_alert_unknown"
end
