defmodule AttentionServer.Repository.Postgres.Attributes do
  @moduledoc false

  alias AttentionServer.Store.StoredTerms
  alias AttentionServer.Repository.Postgres.LargeText.Ref

  @encoding "erlang-etf-v1"
  @options [:deterministic, {:minor_version, 2}]

  # One already-decoded durable fact per envelope. Keep exact Elixir terms
  # without JSON key coercion or numeric roundtrips.
  def encode(value), do: %{"encoding" => @encoding, "data" => Base.encode64(key(value))}

  def key(value), do: :erlang.term_to_binary(value, @options)

  def decode(%{"encoding" => @encoding, "data" => data} = envelope)
      when map_size(envelope) == 2 and is_binary(data) do
    with {:ok, payload} <- Base.decode64(data) do
      :ok = StoredTerms.load_keyed_atoms()
      Code.ensure_loaded!(Ref)

      case :erlang.binary_to_term(payload, [:safe, :used]) do
        {value, used} when used == byte_size(payload) -> {:ok, value}
        _ -> {:error, :invalid_attributes}
      end
    else
      _ -> {:error, :invalid_attributes}
    end
  rescue
    ArgumentError -> {:error, :invalid_attributes}
  end

  def decode(_), do: {:error, :invalid_attributes}
end
