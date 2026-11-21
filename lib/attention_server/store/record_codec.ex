defmodule AttentionServer.Store.RecordCodec do
  @moduledoc false

  alias AttentionServer.{Engine, Store.StoredTerms}

  @record_version :relay_record_v1
  @term_options [:deterministic, {:minor_version, 2}]
  @derived [:advisory_id_index, :open_default_index, :route_b_index]

  @fields Map.new(Map.drop(Map.from_struct(%Engine{}), [:advisory_feeds | @derived]), fn
            {field, %MapSet{}} -> {field, :member}
            {field, value} when is_map(value) -> {field, :row}
            {field, _value} -> {field, :field}
          end)
  @required MapSet.new(for {field, :field} <- @fields, do: field)

  # Enumerate at boot or migration. Routine writes only call encode_row/2
  # for records named by the transition.
  def encode(%Engine{} = engine) do
    fields =
      Stream.flat_map(@fields, fn {field, kind} ->
        value = Map.fetch!(engine, field)

        case kind do
          :field -> [encode_row({:field, field}, value)]
          :row -> Stream.map(value, fn {key, row} -> encode_row({:row, field, key}, row) end)
          :member -> Stream.map(value, &encode_row({:member, field, &1}, true))
        end
      end)

    advisories =
      Stream.flat_map(engine.advisory_feeds, fn {scope, feed} ->
        entries =
          Stream.map(feed.entries, fn {seq, entry} ->
            encode_row({:advisory, scope, {:entry, seq}}, entry)
          end)

        receipts =
          Stream.map(feed.receipts, &encode_row({:advisory, scope, {:receipt, &1}}, true))

        Stream.concat([[encode_row({:advisory, scope, :high}, feed.high)], entries, receipts])
      end)

    Stream.concat(fields, advisories)
  end

  def encode_key(address), do: :erlang.term_to_binary({@record_version, address}, @term_options)

  def encode_row(address, value) do
    key = encode_key(address)
    payload = :erlang.term_to_binary(value, [:compressed | @term_options])
    {key, payload, checksum(key, payload)}
  end

  # Persist this identity with the keyed store. Reopen on a different OTP
  # major requires rekeying before point writes are allowed.
  def identity, do: {@record_version, List.to_string(:erlang.system_info(:otp_release))}

  def decode(records) do
    :ok = StoredTerms.load_atoms()

    case Enum.reduce_while(records, {%Engine{}, @required}, &load_record/2) do
      {engine, required} when is_struct(required, MapSet) ->
        if MapSet.size(required) == 0 and
             Enum.all?(engine.advisory_feeds, fn {_scope, feed} -> not is_nil(feed.high) end) do
          {:ok, engine}
        else
          {:error, :invalid_state}
        end

      :invalid ->
        {:error, :invalid_state}
    end
  end

  defp load_record({key, payload, hash}, {engine, required})
       when is_binary(key) and is_binary(payload) and is_binary(hash) do
    with true <- hash == checksum(key, payload),
         {@record_version, address} <- :erlang.binary_to_term(key, [:safe]),
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

  defp insert(engine, required, {:advisory, scope, fact}, value) do
    feed =
      Map.get(engine.advisory_feeds, scope, %{high: nil, entries: %{}, receipts: MapSet.new()})

    with {:ok, updated} <- insert_advisory(feed, fact, value) do
      {:ok, %{engine | advisory_feeds: Map.put(engine.advisory_feeds, scope, updated)}, required}
    end
  end

  defp insert(_, _, _, _), do: :invalid

  defp insert_advisory(%{high: nil} = feed, :high, high) when is_integer(high) and high >= 0,
    do: {:ok, %{feed | high: high}}

  defp insert_advisory(feed, {:entry, seq}, entry) do
    unless Map.has_key?(feed.entries, seq) do
      {:ok, %{feed | entries: Map.put(feed.entries, seq, entry)}}
    end
  end

  defp insert_advisory(feed, {:receipt, id}, true) do
    unless MapSet.member?(feed.receipts, id) do
      {:ok, %{feed | receipts: MapSet.put(feed.receipts, id)}}
    end
  end

  defp insert_advisory(_, _, _), do: :invalid

  defp checksum(key, payload), do: :crypto.hash(:sha256, [key, payload])
end
