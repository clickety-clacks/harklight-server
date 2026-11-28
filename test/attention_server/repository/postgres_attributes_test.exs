defmodule AttentionServer.Repository.PostgresAttributesTest do
  use ExUnit.Case, async: true
  alias AttentionServer.JSON.DecimalToken
  alias AttentionServer.Repository.Postgres.Attributes

  test "typed attributes round-trip through JSON without changing keys or decimal lexemes" do
    value = %{
      {:person, "synthetic"} => MapSet.new([{:reply, 7}, <<0, 255>>]),
      :decimal => %DecimalToken{raw: "1.2300"},
      :pending => nil
    }

    envelope = value |> Attributes.encode() |> Jason.encode!() |> Jason.decode!()
    assert {:ok, decoded} = Attributes.decode(envelope)
    assert decoded === value
    assert Attributes.key(value) == Attributes.key(Map.new(Enum.reverse(Map.to_list(value))))
    assert {:error, :invalid_attributes} = Attributes.decode(%{envelope | "data" => "%%%"})
  end

  test "a cold VM decodes shared facts without seeding the modules in that VM" do
    encoded =
      Attributes.encode(%{
        {:origin, "synthetic"} => %{
          hosted_ref: {"external-org", "external-group"},
          reminder: %{outcome: :unseen_interval_elapsed},
          cursor: %DecimalToken{raw: "1.2300"}
        }
      })

    script = """
    false = :code.is_loaded(AttentionServer.SharedReminders)
    false = :code.is_loaded(AttentionServer.Groups)
    envelope = %{"encoding" => "erlang-etf-v1", "data" => System.fetch_env!("HARKLIGHT_TYPED_FIXTURE")}
    {:ok, fact} = AttentionServer.Repository.Postgres.Attributes.decode(envelope)
    ^envelope = AttentionServer.Repository.Postgres.Attributes.encode(fact)
    """

    {output, status} =
      System.cmd("mix", ["run", "--no-start", "--no-compile", "-e", script],
        env: [
          {"MIX_ENV", "test"},
          {"ERL_FLAGS", "+S 2:2"},
          {"ATTENTION_SERVER_ENABLE", "0"},
          {"HARKLIGHT_TYPED_FIXTURE", encoded["data"]}
        ],
        stderr_to_stdout: true
      )

    assert status == 0, output
  end

end
