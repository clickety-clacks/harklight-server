defmodule AttentionServer.UI1ConformanceTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{JSON, Message, UI1}

  @corpus_path Path.join(__DIR__, "fixtures/attention-client-ui1-conformance-v3.json")
  @corpus_sha256 "c616ceac8904ae4d12d66894a31971eeab8111d7d91bbe771489b49e6566f040"
  @app_corpus_source_revision "b45fca200a7c219e8d316dc7e299c5a24050b258"
  @app_grammar_revision "f936fa378381f33f6a53e20c81c95ff8e0b7e420"
  @duplicates_key :__attention_server_duplicate_keys__
  @moduletag app_corpus_source_revision: @app_corpus_source_revision,
             app_grammar_revision: @app_grammar_revision

  defp decode_corpus!(raw) do
    options =
      Map.new(
        float: fn token -> %JSON.DecimalToken{raw: token} end,
        null: nil,
        object_start: fn _ -> %{} end,
        object_push: &put_with_duplicate_marker/3,
        object_finish: fn object, old_acc -> {object, old_acc} end
      )

    case :json.decode(raw, nil, options) do
      {value, nil, rest} when is_binary(rest) ->
        assert String.trim(rest) == ""
        value

      other ->
        flunk("could not decode app UI/1 corpus: #{inspect(other)}")
    end
  end

  defp put_with_duplicate_marker(key, value, object) do
    if Map.has_key?(object, key) do
      Map.update(object, @duplicates_key, [key], fn keys -> Enum.uniq(keys ++ [key]) end)
    else
      Map.put(object, key, value)
    end
  end

  test "Server UI/1 admission matches app-authored corpus verdicts and reject paths" do
    raw = File.read!(@corpus_path)
    assert :crypto.hash(:sha256, raw) |> Base.encode16(case: :lower) == @corpus_sha256

    corpus = decode_corpus!(raw)
    cases = corpus["cases"]
    assert length(cases) == 198
    assert Enum.count(cases, &(&1["verdict"] == "accept")) == 5
    assert Enum.count(cases, &(&1["verdict"] == "reject")) == 193

    mismatches =
      Enum.flat_map(cases, fn test_case ->
        document = Map.get(test_case, "sent_document", test_case["document"])
        files = test_case["delivered_file_names"]

        validation =
          try do
            UI1.validate(document, files)
          rescue
            error -> {:raised, Exception.format(:error, error, __STACKTRACE__)}
          end

        validator_mismatches =
          case {test_case["verdict"], validation} do
            {"accept", :ok} ->
              []

            {"accept", other} ->
              ["#{test_case["id"]}: accepted by app but Server returned #{inspect(other)}"]

            {"reject", {:error, path, reason}} ->
              expected_path = test_case["fault"]["location"]
              detail = "#{path}: #{reason}"

              if String.contains?(detail, expected_path),
                do: [],
                else: ["#{test_case["id"]}: fault at #{expected_path} not named in #{detail}"]

            {"reject", other} ->
              ["#{test_case["id"]}: rejected by app but Server returned #{inspect(other)}"]
          end

        admission_mismatches =
          if is_map(document) and not is_struct(document) and
               Map.get(document, "schema") == "attention-client.ui/1" do
            admission =
              try do
                Message.validate_ui1_admission(%{"answer_ui" => document}, files)
              rescue
                error -> {:raised, Exception.format(:error, error, __STACKTRACE__)}
              end

            case {test_case["verdict"], admission} do
              {"accept", :ok} ->
                []

              {"accept", other} ->
                ["#{test_case["id"]}: direct Server admission returned #{inspect(other)}"]

              {"reject", {:error, {:typed_message, "request_answer_ui_invalid", reason}}} ->
                expected_path = test_case["fault"]["location"]

                if String.contains?(reason, expected_path),
                  do: [],
                  else: [
                    "#{test_case["id"]}: direct admission did not name #{expected_path} in #{reason}"
                  ]

              {"reject", other} ->
                ["#{test_case["id"]}: direct Server admission returned #{inspect(other)}"]
            end
          else
            []
          end

        validator_mismatches ++ admission_mismatches
      end)

    assert mismatches == [], Enum.join(mismatches, "\n")
  end

  test "UI/1 admission requires an input only for typed requests" do
    valid_without_inputs = %{
      "schema" => "attention-client.ui/1",
      "requires" => ["scene/1", "result/1"],
      "illustrations" => [],
      "inputs" => [],
      "open" => %{"kind" => "text", "text" => "Open", "style" => "body"},
      "resolved" => %{"kind" => "text", "text" => "Done", "style" => "body"},
      "submission" => %{"label" => "Send", "result" => %{"kind" => "literal", "value" => true}}
    }

    no_input_message = %{
      "message_type" => "request",
      "answer_ui" => valid_without_inputs
    }

    assert {:error, {:typed_message, "request_needs_answer_controls", reason}} =
             Message.validate_ui1_admission(no_input_message, [])

    assert reason =~ "at least one input"
    assert :ok = Message.validate_ui1_admission(Map.delete(no_input_message, "message_type"), [])
  end

  test "V2 checks the Server-reencoded delivered document, including minus zero" do
    raw = File.read!(@corpus_path)
    corpus = decode_corpus!(raw)
    edge = Enum.find(corpus["cases"], &(&1["id"] == "accept-server-reencoded-minus-zero-edge"))
    sent = edge["sent_document"]
    delivered = edge["document"]

    assert UI1.validate(sent, edge["delivered_file_names"]) == :ok
    assert UI1.validate(delivered, edge["delivered_file_names"]) == :ok
    assert byte_size(JSON.encode!(delivered)) == 65_536
    assert byte_size(JSON.encode!(sent)) == 65_536
  end
end
