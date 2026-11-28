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
end
