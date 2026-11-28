defmodule AttentionServer.Repository.Postgres.Attributes do
  @moduledoc false

  alias AttentionServer.Store.StoredTerms

  @encoding "erlang-etf-v1"
  @options [:deterministic, {:minor_version, 2}]

  # One already-decoded durable fact per envelope. Keep exact Elixir terms
  # without JSON key coercion or numeric roundtrips.
  def encode(value), do: %{"encoding" => @encoding, "data" => Base.encode64(key(value))}

  def key(value), do: :erlang.term_to_binary(value, @options)

  def decode(%{"encoding" => @encoding, "data" => data} = envelope)
      when map_size(envelope) == 2 and is_binary(data) do
    with {:ok, payload} <- Base.decode64(data) do
      StoredTerms.load_atoms()
      {:ok, :erlang.binary_to_term(payload, [:safe])}
    else
      _ -> {:error, :invalid_attributes}
    end
  rescue
    ArgumentError -> {:error, :invalid_attributes}
  end

  def decode(_), do: {:error, :invalid_attributes}
end
