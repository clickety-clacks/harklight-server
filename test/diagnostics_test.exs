defmodule AttentionServer.DiagnosticsTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AttentionServer.Diagnostics

  test "records bounded failure context, keeps original operation identity, and omits unsafe reason text" do
    operation_id = "00000000-0000-4000-8000-000000000001"
    secret_reason = "bad certificate for PRIVATE-KEY-MATERIAL"

    log =
      capture_log(fn ->
        Diagnostics.failure(
          :origin_https,
          :response_write,
          {:tls_alert, {:unknown_ca, secret_reason}},
          :outcome_unknown,
          operation_id
        )
      end)

    assert log =~ "event=harklight_failure"
    assert log =~ "operation=origin_https"
    assert log =~ "phase=response_write"
    assert log =~ "cause=untrusted_certificate"
    assert log =~ "outcome=outcome_unknown"
    assert log =~ "operation_id=#{operation_id}"
    refute log =~ secret_reason
    refute log =~ "PRIVATE-KEY-MATERIAL"
  end

  test "unknown causes remain unknown and invalid request identities are omitted" do
    log =
      capture_log(fn ->
        Diagnostics.failure(
          :agent_https,
          :request_dispatch,
          {:arbitrary, "private text"},
          :failed,
          "not-an-op-id"
        )
      end)

    assert log =~ "cause=unknown"
    refute log =~ "private text"
    refute log =~ "operation_id="
  end

  test "structured operation refusal keeps correlation and distinguishes uncertain storage" do
    operation_id = "00000000-0000-4000-8000-000000000002"

    log =
      capture_log(fn ->
        Diagnostics.response_error(
          :agent_https,
          :request_dispatch,
          %{"type" => "error", "reason" => "storage_unavailable"},
          operation_id
        )
      end)

    assert log =~ "operation=agent_https"
    assert log =~ "phase=request_dispatch"
    assert log =~ "cause=storage_unavailable"
    assert log =~ "outcome=outcome_unknown"
    assert log =~ "operation_id=#{operation_id}"
  end
end
