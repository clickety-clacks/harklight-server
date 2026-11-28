defmodule AttentionServer.Migration.AvailabilityVerificationTest do
  use ExUnit.Case, async: true
  alias AttentionServer.Migration.{Frames, VerifyAvailability}

  test "source precedence and latest journal position are independent of frame order" do
    key = {"person", "sender", "message"}
    record = %{origin: "origin", session_handle: "session", accepted_seq: 7, agent_api_v2: true}
    session = {{:row, :origin_sessions, {"person", "origin", "session"}},
      %{"client_availability" => %{7 => {"session-state", 20}}}}
    raw = {{:row, :item_availability, key}, {"item-state", 30}}
    accepted = {{:row, :events, {"person", 7}}, %{payload: %{"availability" => "accepted-state", "last_detached_at" => 10}}}
    latest = {{:row, :conversation_events, {"person", 9}},
      %{key: key, payload: %{"kind" => "availability", "availability" => "latest-state", "last_detached_at" => 90}}}
    earlier = {{:row, :conversation_events, {"person", 3}},
      %{key: key, payload: %{"kind" => "availability", "availability" => "earlier-state", "last_detached_at" => 3}}}
    other = {{:row, :conversation_events, {"other", 999}},
      %{key: {"other", "sender", "message"}, payload: %{"kind" => "availability", "availability" => "wrong-scope"}}}
    for {rows, expected} <- [
      {[latest, raw, earlier, accepted, session, other], {"item-state", 30}},
      {[latest, earlier, accepted, session, other], {"session-state", 20}},
      {[latest, earlier, accepted, other], {"latest-state", 90}},
      {[earlier, latest, accepted, other], {"latest-state", 90}},
      {[accepted, other], {"accepted-state", 10}},
      {[other], {"not_listening", nil}}
    ] do
      root = Path.join(System.tmp_dir!(), "availability-source-#{System.unique_integer([:positive])}")
      File.mkdir!(root)
      File.chmod!(root, 0o700)
      try do
        path = Path.join(root, "records.frames")
        assert {:ok, summary} = Frames.write(path, rows)
        before = File.read!(path)
        assert {:ok, ^expected} = VerifyAvailability.with_source(path, summary,
          &VerifyAvailability.projection(&1, key, record))
        assert File.read!(path) == before
      after
        File.rm_rf!(root)
      end
    end
  end
end
