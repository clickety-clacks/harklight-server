defmodule AttentionServer.LargeDataRevisionSubstanceTest do
  # A revision that resends identical large content is not a substantive
  # change, whatever form the stored and requested bytes take.
  use ExUnit.Case, async: false

  alias AttentionServer.{JSON, Relay, TLSFixtures}

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "large-substance-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    relay =
      start_link_supervised!(
        {Relay, store_path: Path.join(dir, "relay.sqlite3"), enrollment_fixture_mode: true},
        id: make_ref(),
        restart: :temporary
      )

    :ok = Relay.setup_person(relay, "A")

    {:ok, key} =
      Relay.setup_device_certificate(relay, "A", TLSFixtures.certificate_der!(certs, "a1"))

    :ok = Relay.setup_sender(relay, key, "app:test")
    %{relay: relay, key: key}
  end

  defp op(n), do: "00000000-0000-4000-8000-" <> String.pad_leading("#{n}", 12, "0")

  defp request(relay, key, payload) do
    frame = JSON.encode!(Map.put_new(payload, "v", 1)) <> "\n"
    {:ok, line} = Relay.request_line(relay, key, frame)
    {:ok, response} = JSON.decode_line(line)
    response
  end

  defp message(body) do
    %{
      "sender_id" => "app:test",
      "message_id" => "big",
      "created_at" => 1,
      "title" => "Big",
      "body" => body,
      "message_type" => "fyi",
      "answer_ui" => %{
        "schema" => "attention-client.actions/1",
        "actions" => [
          %{
            "id" => "seen",
            "label" => "Seen",
            "kind" => "value",
            "value" => %{"acknowledgment" => "seen"}
          }
        ]
      }
    }
  end

  defp substantive_revision(relay) do
    engine = :sys.get_state(relay).store.engine

    [state] =
      for {key, state} <- engine.bounce_revisions,
          "big" in Tuple.to_list(key),
          do: state

    state.current_substantive_revision
  end

  test "identical large content keeps the substantive revision; new content advances it",
       %{relay: relay, key: key} do
    body = Base.encode64(:binary.copy(<<7>>, 1_700_000))

    assert %{"type" => "ok"} =
             request(relay, key, %{
               "type" => "forward",
               "op_id" => op(1),
               "session_handle" => op(9_000),
               "message" => message(body)
             })

    assert substantive_revision(relay) == 1

    assert %{"type" => "ok", "revision" => 2} =
             request(relay, key, %{
               "type" => "conversation_revise",
               "op_id" => op(2),
               "sender_id" => "app:test",
               "message_id" => "big",
               "base_revision" => 1,
               "message" => message(body)
             })

    assert substantive_revision(relay) == 1

    assert %{"type" => "ok", "revision" => 3} =
             request(relay, key, %{
               "type" => "conversation_revise",
               "op_id" => op(3),
               "sender_id" => "app:test",
               "message_id" => "big",
               "base_revision" => 2,
               "message" => message(Base.encode64(:binary.copy(<<8>>, 1_700_000)))
             })

    assert substantive_revision(relay) == 2
  end
end
