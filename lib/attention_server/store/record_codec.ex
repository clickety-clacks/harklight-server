defmodule AttentionServer.Store.RecordCodec do
  @moduledoc false

  alias AttentionServer.Engine

  @derived [:advisory_id_index, :open_default_index, :route_b_index]

  @fields Map.new(Map.drop(Map.from_struct(%Engine{}), @derived), fn
            {field, %MapSet{}} -> {field, :member}
            {field, value} when is_map(value) -> {field, :row}
            {field, _value} -> {field, :field}
          end)
  @required MapSet.new(for {field, :field} <- @fields, do: field)

  # Enumerate at boot or migration. Routine writes only call encode_row/2
  # for records named by the transition.
  def encode(%Engine{} = engine) do
    Stream.flat_map(@fields, fn {field, kind} ->
      value = Map.fetch!(engine, field)

      case kind do
        :field -> [encode_row({:field, field}, value)]
        :row -> Stream.map(value, fn {key, row} -> encode_row({:row, field, key}, row) end)
        :member -> Stream.map(value, &encode_row({:member, field, &1}, true))
      end
    end)
  end

  def encode_row(address, value) do
    key = :erlang.term_to_binary(address, [:deterministic])
    payload = :erlang.term_to_binary(value, [:compressed])
    {key, payload, checksum(key, payload)}
  end

  def decode(records) do
    case Enum.reduce_while(records, {%Engine{}, @required}, &load_record/2) do
      {engine, required} when is_struct(required, MapSet) ->
        if MapSet.size(required) == 0, do: {:ok, engine}, else: {:error, :invalid_state}

      :invalid ->
        {:error, :invalid_state}
    end
  end

  defp load_record({key, payload, hash}, {engine, required})
       when is_binary(key) and is_binary(payload) and is_binary(hash) do
    with true <- hash == checksum(key, payload),
         address <- :erlang.binary_to_term(key, [:safe]),
         value <- :erlang.binary_to_term(payload, [:safe]),
         {:ok, next, needed} <- insert(engine, required, address, value) do
      {:cont, {next, needed}}
    else
      _ -> {:halt, :invalid}
    end
  rescue
    ArgumentError -> {:halt, :invalid}
  end

  defp load_record(_, _), do: {:halt, :invalid}

  defp insert(engine, required, {:field, field}, value) do
    if MapSet.member?(required, field) do
      {:ok, Map.replace!(engine, field, value), MapSet.delete(required, field)}
    end
  end

  defp insert(engine, required, {:row, field, key}, value) do
    if @fields[field] == :row do
      rows = Map.fetch!(engine, field)

      unless Map.has_key?(rows, key) do
        {:ok, Map.replace!(engine, field, Map.put(rows, key, value)), required}
      end
    end
  end

  defp insert(engine, required, {:member, field, key}, true) do
    if @fields[field] == :member do
      members = Map.fetch!(engine, field)

      unless MapSet.member?(members, key) do
        {:ok, Map.replace!(engine, field, MapSet.put(members, key)), required}
      end
    end
  end

  defp insert(_, _, _, _), do: :invalid

  defp checksum(key, payload), do: :crypto.hash(:sha256, [key, payload])
end
