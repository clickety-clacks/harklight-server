defmodule AttentionServer.Store.RecordCodec do
  @moduledoc false

  alias AttentionServer.{Engine, Store.StoredTerms}

  @record_version :relay_record_v1
  @term_options [:deterministic, {:minor_version, 2}]
  @bounce_caches [:bounce_discussed_messages, :bounce_origin_entries]
  @derived [
             :advisory_id_index,
             :bounce_deadline_index,
             :open_default_index,
             :route_b_index,
             :touched_keys
           ] ++ @bounce_caches

  @fields Map.new(
            Map.drop(Map.from_struct(%Engine{}), [:advisory_feeds, :origin_feeds | @derived]),
            fn
              {field, %MapSet{}} -> {field, :member}
              {field, value} when is_map(value) -> {field, :row}
              {field, _value} -> {field, :field}
            end
          )
  @required MapSet.new(for {field, :field} <- @fields, do: field)

  def derived_fields, do: @derived

  # Enumerate at boot or migration. Routine writes only call encode_row/2
  # for records named by the transition.
  def encode(%Engine{} = engine, prepare \\ &Function.identity/1) do
    encode = fn address, value -> encode_row(address, prepare.(value)) end

    fields =
      Stream.flat_map(@fields, fn {field, kind} ->
        value = Map.fetch!(engine, field)

        case kind do
          :field -> [encode.({:field, field}, value)]
          :row -> Stream.map(value, fn {key, row} -> encode.({:row, field, key}, row) end)
          :member -> Stream.map(value, &encode.({:member, field, &1}, true))
        end
      end)

    advisories =
      Stream.flat_map(engine.advisory_feeds, fn {scope, feed} ->
        entries =
          Stream.map(feed.entries, fn {seq, entry} ->
            encode.({:advisory, scope, {:entry, seq}}, entry)
          end)

        receipts =
          Stream.map(feed.receipts, &encode.({:advisory, scope, {:receipt, &1}}, true))

        Stream.concat([[encode.({:advisory, scope, :high}, feed.high)], entries, receipts])
      end)

    origins =
      Stream.flat_map(engine.origin_feeds, fn {scope, feed} ->
        entries =
          Stream.map(feed.entries, fn {seq, entry} ->
            encode.({:origin_feed, scope, {:entry, seq}}, entry)
          end)

        acks =
          Stream.map(Map.get(feed, :delivery_acks, %{}), fn {seq, ack} ->
            encode.({:origin_feed, scope, {:delivery_ack, seq}}, ack)
          end)

        Stream.concat([[encode.({:origin_feed, scope, :high}, feed.high)], entries, acks])
      end)

    Stream.concat([fields, advisories, origins])
  end

  def lookup(engine, {:field, field}), do: Map.fetch(engine, field)
  def lookup(engine, {:row, field, key}), do: Map.fetch(Map.fetch!(engine, field), key)

  def lookup(engine, {:member, field, key}) do
    if MapSet.member?(Map.fetch!(engine, field), key), do: {:ok, true}, else: :error
  end

  def lookup(engine, {:origin_feed, scope, fact}),
    do: feed_record(Map.get(engine.origin_feeds, scope, %{}), fact)

  def lookup(engine, {:advisory, scope, fact}),
    do: feed_record(Map.get(engine.advisory_feeds, scope, %{}), fact)

  defp feed_record(feed, :high), do: Map.fetch(feed, :high)
  defp feed_record(feed, {:entry, seq}), do: Map.fetch(Map.get(feed, :entries, %{}), seq)

  defp feed_record(feed, {:delivery_ack, seq}),
    do: Map.fetch(Map.get(feed, :delivery_acks, %{}), seq)

  defp feed_record(feed, {:receipt, id}) do
    if MapSet.member?(Map.get(feed, :receipts, MapSet.new()), id), do: {:ok, true}, else: :error
  end

  def encode_key(address), do: :erlang.term_to_binary({@record_version, address}, @term_options)

  def decode_key(key) when is_binary(key) do
    case :erlang.binary_to_term(key, [:safe]) do
      {@record_version, address} -> {:ok, address}
      _ -> {:error, :invalid_state}
    end
  rescue
    ArgumentError -> {:error, :invalid_state}
  end

  def account?({:field, _}), do: true
  def account?({:origin_feed, _, :high}), do: true
  def account?({:member, _, _}), do: true

  def account?({:row, field, _}) do
    field in [
      :persons, :people_profiles, :owners, :pending_enrollments, :pending_history, :join_invitations,
      :browser_sign_ins, :maintainer_grants, :devices, :agents, :sender_grants,
      :source_report_grants, :enrollment_read_grants, :free_order_highs, :conversation_highs,
      :conversation_cursors, :subscriptions, :cursors, :hosted_accounts, :hosted_organizations,
      :local_organizations, :groups, :delivery_states,
      :algorithm_choices, :alert_choices, :push_bindings, :bounce_levels
    ]
  end

  def account?(_), do: false

  def encode_row(address, value) do
    key = encode_key(address)
    payload = :erlang.term_to_binary(value, [:compressed | @term_options])
    {key, payload, checksum(key, payload)}
  end

  # Persist this identity with the keyed store. Reopen on a different OTP
  # major requires rekeying before point writes are allowed.
  def identity, do: {@record_version, List.to_string(:erlang.system_info(:otp_release))}

  def decode_row({key, payload, hash})
      when is_binary(key) and is_binary(payload) and is_binary(hash) do
    with true <- hash == checksum(key, payload),
         {:ok, address} <- decode_key(key) do
      {:ok, address, :erlang.binary_to_term(payload, [:safe])}
    else
      _ -> {:error, :invalid_state}
    end
  rescue
    ArgumentError -> {:error, :invalid_state}
  end

  def decode_row(_), do: {:error, :invalid_state}

  def decode(records) do
    :ok = StoredTerms.load_atoms()

    case Enum.reduce_while(records, {%Engine{}, @required}, &load_record/2) do
      {engine, required} when is_struct(required, MapSet) ->
        if MapSet.size(required) == 0 and
             complete_feeds?(engine.advisory_feeds) and complete_feeds?(engine.origin_feeds) do
          {:ok, engine}
        else
          {:error, :invalid_state}
        end

      :invalid ->
        {:error, :invalid_state}
    end
  end

  def matches_row?(engine, row, prepare \\ &Function.identity/1) do
    with {:ok, address, actual} <- decode_row(row),
         {:ok, value} <- lookup(engine, address) do
      actual === prepare.(value)
    else
      _ -> false
    end
  rescue
    ArgumentError -> false
    KeyError -> false
  end

  defp complete_feeds?(feeds),
    do: Enum.all?(feeds, fn {_scope, feed} -> not is_nil(feed.high) end)

  defp load_record(row, {engine, required}) do
    with {:ok, address, value} <- decode_row(row),
         {:ok, next, needed} <- insert(engine, required, address, value) do
      {:cont, {next, needed}}
    else
      _ -> {:halt, :invalid}
    end
  rescue
    ArgumentError -> {:halt, :invalid}
  end

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

  # Earlier keyed rehearsal builds persisted these derived memberships.
  defp insert(engine, required, {:member, field, _key}, true) when field in @bounce_caches,
    do: {:ok, engine, required}

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

  defp insert(engine, required, {:origin_feed, scope, fact}, value) do
    feed = Map.get(engine.origin_feeds, scope, %{high: nil, entries: %{}})

    with {:ok, updated} <- insert_origin(feed, fact, value) do
      {:ok, %{engine | origin_feeds: Map.put(engine.origin_feeds, scope, updated)}, required}
    end
  end

  defp insert(_, _, _, _), do: :invalid

  defp insert_origin(feed, {:delivery_ack, seq}, ack) do
    acks = Map.get(feed, :delivery_acks, %{})

    unless Map.has_key?(acks, seq) do
      {:ok, Map.put(feed, :delivery_acks, Map.put(acks, seq, ack))}
    end
  end

  defp insert_origin(feed, :high, value), do: insert_advisory(feed, :high, value)

  defp insert_origin(feed, {:entry, _} = fact, value),
    do: insert_advisory(feed, fact, value)

  defp insert_origin(_, _, _), do: :invalid

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
