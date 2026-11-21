defmodule AttentionServer.Store.KeyedValidation do
  @moduledoc false

  alias AttentionServer.{Conversation, LargeData, Store.RecordCodec}

  @scalar_fields [
    :agent_cursor_key,
    :browser_sign_in_clock_high_water,
    :jev_endpoint,
    :jev_model,
    :pending_clock_high_water,
    :relay_instance_id
  ]

  @single_keys [
    :agents,
    :algorithm_choices,
    :bounce_levels,
    :browser_sign_ins,
    :conversation_highs,
    :delivery_states,
    :devices,
    :enrollment_read_grants,
    :free_order_highs,
    :hosted_accounts,
    :join_invitations,
    :maintainer_grants,
    :owners,
    :pending_enrollments,
    :pending_history,
    :persons,
    :uploads
  ]

  @pair_keys [:conversation_cursors, :cursors, :push_bindings, :sender_grants, :subscriptions]
  @ordered_keys [:events, :conversation_events]
  @item_keys [:bounce_revisions, :chat_read_positions, :messages, :reply_group_history]
  @actor_keys [:origin_sessions, :receipts, :source_report_grants]
  @origin_seq_keys [:origin_actions, :origin_receipts]

  # A new persisted field must gain an explicit address and validation rule.
  def preflight(old, next, %MapSet{} = touched) do
    checked(fn ->
      if Enum.all?(touched, &valid_fact_shape?(next, &1)) and origin_feeds?(old, next, touched),
        do: :ok,
        else: {:error, :invalid_fact}
    end)
  end

  # Keep exceptions from invalid proposed facts inside validation, before any
  # file, SQL, or notification work can begin.
  def checked(validate) do
    validate.()
  rescue
    _ -> {:error, :invalid_fact}
  end

  defp valid_fact_shape?(next, address) do
    known_address?(address) and
      case RecordCodec.lookup(next, address) do
        {:ok, value} -> valid_fact_value?(next, address, value) and LargeData.valid_refs?(value)
        :error -> not match?({:field, _}, address)
      end
  end

  defp known_address?({:field, field}), do: field in @scalar_fields
  defp known_address?({:row, field, key}) when field in @single_keys, do: is_binary(key)

  defp known_address?({:row, field, {person, actor}}) when field in @pair_keys,
    do: is_binary(person) and is_binary(actor)

  defp known_address?({:row, field, {person, seq}}) when field in @ordered_keys,
    do: is_binary(person) and positive_integer?(seq)

  defp known_address?({:row, field, key}) when field in @item_keys or field in @actor_keys,
    do: binary_triple?(key)

  defp known_address?({:row, :bounce_waiting_routes, {person, origin, handle}}),
    do: is_binary(person) and is_binary(origin) and (is_nil(handle) or is_binary(handle))

  defp known_address?({:row, field, {person, origin, seq}}) when field in @origin_seq_keys,
    do: is_binary(person) and is_binary(origin) and positive_integer?(seq)

  defp known_address?({:member, :retired_owner_keys, key}), do: is_binary(key)

  defp known_address?({:member, :conversation_opened, {person, actor}}),
    do: is_binary(person) and is_binary(actor)

  defp known_address?({:member, :opened_revisions, {person, sender, item, revision}}),
    do: binary_triple?({person, sender, item}) and positive_integer?(revision)

  defp known_address?({:origin_feed, {person, origin}, part}) do
    is_binary(person) and is_binary(origin) and
      case part do
        :high -> true
        {:entry, seq} -> positive_integer?(seq)
        {:delivery_ack, seq} -> positive_integer?(seq)
        _ -> false
      end
  end

  defp known_address?({:advisory, scope, part}) do
    binary_triple?(scope) and
      case part do
        :high -> true
        {:entry, seq} -> positive_integer?(seq)
        {:receipt, id} -> is_binary(id)
        _ -> false
      end
  end

  defp known_address?(_), do: false

  defp positive_integer?(value), do: is_integer(value) and value > 0
  defp nonnegative_integer?(value), do: is_integer(value) and value >= 0

  defp binary_triple?({a, b, c}), do: is_binary(a) and is_binary(b) and is_binary(c)
  defp binary_triple?(_), do: false

  defp valid_fact_value?(_engine, {:field, field}, value)
       when field in [:browser_sign_in_clock_high_water, :pending_clock_high_water],
       do: nonnegative_integer?(value)

  defp valid_fact_value?(_engine, {:row, field, _}, value)
       when field in [:persons, :conversation_highs],
       do: nonnegative_integer?(value)

  defp valid_fact_value?(engine, {:row, :origin_actions, key}, value),
    do: Conversation.valid_origin_action?(engine, key, value)

  defp valid_fact_value?(_engine, _address, _value), do: true

  defp origin_feeds?(old, next, touched) do
    # Existing entries are immutable. Given contiguous old history, only the
    # changed positions and the number of new rows are needed to prove coverage.
    counts =
      Enum.reduce(touched, %{}, fn
        {:origin_feed, scope, :high}, counts ->
          Map.put_new(counts, scope, 0)

        {:origin_feed, scope, {:entry, seq}}, counts ->
          appended =
            if is_nil(get_in(old.origin_feeds, [scope, :entries, seq])), do: 1, else: 0

          Map.update(counts, scope, appended, &(&1 + appended))

        {:origin_feed, scope, {:delivery_ack, _}}, counts ->
          Map.put_new(counts, scope, 0)

        _, counts ->
          counts
      end)

    Enum.all?(counts, fn {{person, origin} = scope, appended} ->
      old_high = get_in(old.origin_feeds, [scope, :high]) || 0

      case Map.get(next.origin_feeds, scope) do
        %{high: high, entries: entries} ->
          get_in(next.devices, [origin, :person]) == person and
            nonnegative_integer?(high) and is_map(entries) and map_size(entries) == high and
            high == old_high + appended and
            ((Map.has_key?(old.origin_feeds, scope) and high == old_high) or
               MapSet.member?(touched, {:origin_feed, scope, :high}))

        _ ->
          false
      end
    end) and Enum.all?(touched, &valid_origin_change?(old, next, &1))
  end

  defp valid_origin_change?(old, next, {:origin_feed, scope, {:entry, seq}}) do
    entry = get_in(next.origin_feeds, [scope, :entries, seq])

    case get_in(old.origin_feeds, [scope, :entries, seq]) do
      nil ->
        Conversation.valid_origin_entry?(seq, get_in(next.origin_feeds, [scope, :high]), entry)

      prior ->
        prior === entry
    end
  end

  defp valid_origin_change?(_old, next, {:origin_feed, scope, {:delivery_ack, seq}}) do
    Conversation.valid_delivery_ack?(next, scope, seq)
  end

  defp valid_origin_change?(_old, _next, _address), do: true

  def source_index(messages) do
    Enum.reduce(messages, %{}, fn {key, record}, index ->
      case source_key(record) do
        nil -> index
        source -> Map.put(index, source, key)
      end
    end)
  end

  def source_changes(index, old_messages, next_messages, changed_items) do
    cleared =
      Enum.reduce(changed_items, index, fn key, current ->
        case source_key(Map.get(old_messages, key)) do
          nil -> current
          source -> Map.delete(current, source)
        end
      end)

    Enum.reduce_while(changed_items, {:ok, cleared}, fn key, {:ok, current} ->
      case source_key(Map.get(next_messages, key)) do
        nil ->
          {:cont, {:ok, current}}

        source ->
          if Map.has_key?(current, source),
            do: {:halt, {:error, :source_binding_conflict}},
            else: {:cont, {:ok, Map.put(current, source, key)}}
      end
    end)
  end

  defp source_key(%{source_binding: binding}) when is_map(binding) do
    {binding["source_instance_id"], binding["request_id"], binding["request_revision"]}
  end

  defp source_key(_), do: nil

  @authority_rows [
    :messages,
    :uploads,
    :receipts,
    :sender_grants,
    :source_report_grants,
    :subscriptions,
    :push_bindings,
    :cursors,
    :conversation_cursors,
    :origin_sessions,
    :origin_feeds,
    :owners,
    :devices,
    :agents,
    :pending_enrollments,
    :pending_history,
    :browser_sign_ins,
    :join_invitations,
    :hosted_accounts,
    :delivery_states,
    :algorithm_choices,
    :free_order_highs,
    :bounce_levels,
    :maintainer_grants,
    :enrollment_read_grants
  ]

  def authority_index(engine) do
    index =
      for field <- @authority_rows,
          key <- Map.keys(Map.fetch!(engine, field)),
          reduce: %{} do
        index -> edit_authority_index(index, engine, {:row, field, key}, :add)
      end

    Enum.reduce(engine.conversation_opened, index, fn key, acc ->
      edit_authority_index(acc, engine, {:member, :conversation_opened, key}, :add)
    end)
  end

  def scoped_rows(index, field, scope),
    do: Map.get(index, {:scope, field, scope}, MapSet.new())

  def authority_changes(index, old, next, touched) do
    updated =
      Enum.reduce(touched, index, fn address, acc ->
        address = authority_address(address)

        acc
        |> edit_authority_index(old, address, :remove)
        |> edit_authority_index(next, address, :add)
      end)

    affected =
      Enum.reduce(touched, MapSet.new(), fn
        {:row, :persons, person}, keys ->
          if Map.has_key?(old.persons, person) != Map.has_key?(next.persons, person) or
               Map.get(next.persons, person, 0) < Map.get(old.persons, person, 0),
             do: MapSet.union(keys, Map.get(updated, {:person, person}, MapSet.new())),
             else: keys

        {:row, :conversation_highs, person}, keys ->
          if Map.get(next.conversation_highs, person, 0) <
               Map.get(old.conversation_highs, person, 0),
             do: MapSet.union(keys, Map.get(updated, {:person, person}, MapSet.new())),
             else: keys

        {:row, field, actor}, keys when field in [:devices, :agents] ->
          before = owner_identity(Map.get(Map.fetch!(old, field), actor))
          after_identity = owner_identity(Map.get(Map.fetch!(next, field), actor))

          if before == after_identity,
            do: keys,
            else: MapSet.union(keys, Map.get(updated, {:actor, actor}, MapSet.new()))

        {:row, field, code}, keys when field in [:pending_enrollments, :pending_history] ->
          MapSet.union(keys, scoped_rows(updated, :invitation_code, code))

        _, keys ->
          keys
      end)

    {updated, affected}
  end

  def unique_authority?(index, engine, touched) do
    Enum.all?(touched, fn address ->
      Enum.all?(authority_owners(engine, address), fn
        {:exclusive, _, _} = owner -> MapSet.size(Map.fetch!(index, owner)) == 1
        _ -> true
      end)
    end)
  end

  defp owner_identity(nil), do: nil
  defp owner_identity(record), do: Map.take(record, [:person, :id])

  defp edit_authority_index(index, engine, address, operation) do
    Enum.reduce(authority_owners(engine, address), index, fn owner, acc ->
      rows = Map.get(acc, owner, MapSet.new())

      next =
        if operation == :add, do: MapSet.put(rows, address), else: MapSet.delete(rows, address)

      if MapSet.size(next) == 0, do: Map.delete(acc, owner), else: Map.put(acc, owner, next)
    end)
  end

  defp authority_owners(engine, {:row, :messages, {person, _, _} = key}) do
    case Map.get(engine.messages, key) do
      nil ->
        []

      record ->
        actors = [record.origin, get_in(record, [:terminal, :actor])] |> Enum.reject(&is_nil/1)
        [{:person, person} | Enum.map(actors, &{:actor, &1})]
    end
  end

  defp authority_owners(engine, {:row, :uploads, id}) do
    case Map.get(engine.uploads, id) do
      nil ->
        []

      upload ->
        [
          {:person, upload.person},
          {:actor, upload.uploader},
          {:scope, :uploads, {upload.person, upload.uploader}}
        ]
    end
  end

  defp authority_owners(engine, {:row, :receipts, {person, actor, _} = key}) do
    if Map.has_key?(engine.receipts, key), do: [{:person, person}, {:actor, actor}], else: []
  end

  defp authority_owners(engine, {:row, :owners, person}) do
    case Map.get(engine.owners, person) do
      %{key: key} -> [{:person, person}, {:actor, key}, {:exclusive, :owner_key, key}]
      _ -> []
    end
  end

  defp authority_owners(engine, {:row, field, person})
       when field in [:delivery_states, :algorithm_choices, :free_order_highs, :bounce_levels] do
    if Map.has_key?(Map.fetch!(engine, field), person), do: [{:person, person}], else: []
  end

  defp authority_owners(engine, {:row, :maintainer_grants, id}) do
    case Map.get(engine.maintainer_grants, id) do
      %{target_person: target, grantee_person: grantee} -> [{:person, target}, {:person, grantee}]
      _ -> []
    end
  end

  defp authority_owners(engine, {:row, :enrollment_read_grants, id}) do
    case Map.get(engine.enrollment_read_grants, id) do
      %{person: person, connector: key, target: %{"key_fingerprint" => target}} ->
        [{:person, person}, {:actor, key}, {:actor, target}]

      _ ->
        []
    end
  end

  defp authority_owners(engine, {:row, :hosted_accounts, ref}) do
    case Map.get(engine.hosted_accounts, ref) do
      %{person: person} -> [{:person, person}]
      _ -> []
    end
  end

  defp authority_owners(engine, {:row, :devices, key}) do
    case Map.get(engine.devices, key) do
      %{person: person} -> [{:person, person}]
      _ -> []
    end
  end

  defp authority_owners(engine, {:row, :agents, key}) do
    case Map.get(engine.agents, key) do
      %{person: person, id: id} -> [{:person, person}, {:exclusive, :agent_id, {person, id}}]
      _ -> []
    end
  end

  defp authority_owners(engine, {:row, field, code})
       when field in [:pending_enrollments, :pending_history] do
    record = Map.get(Map.fetch!(engine, field), code)

    owners =
      case record do
        %{state: :approved, key: key, person: person} -> [{:person, person}, {:actor, key}]
        %{person: person} when is_binary(person) -> [{:person, person}]
        _ -> []
      end

    case {field, record} do
      {:pending_enrollments, %{secret_hash: hash}} -> [{:exclusive, field, hash} | owners]
      _ -> owners
    end
  end

  defp authority_owners(engine, {:row, field, token})
       when field in [:browser_sign_ins, :join_invitations] do
    case Map.get(Map.fetch!(engine, field), token) do
      %{person: person, code: code} when is_binary(code) ->
        [{:person, person}, {:scope, field, person}, {:scope, :invitation_code, code}]

      %{person: person} ->
        [{:person, person}, {:scope, field, person}]

      _ ->
        []
    end
  end

  defp authority_owners(engine, {:row, field, {person, actor} = key})
       when field in [
              :sender_grants,
              :subscriptions,
              :push_bindings,
              :cursors,
              :conversation_cursors,
              :origin_feeds
            ] do
    row_owners(engine, field, key, person, actor)
  end

  defp authority_owners(engine, {:row, :source_report_grants, {person, actor, _} = key}) do
    row_owners(engine, :source_report_grants, key, person, actor)
  end

  defp authority_owners(engine, {:row, :origin_sessions, {person, actor, _} = key}) do
    if Map.has_key?(engine.origin_sessions, key),
      do: [{:person, person}, {:actor, actor}, {:scope, :origin_sessions, {person, actor}}],
      else: []
  end

  defp authority_owners(engine, {:member, :conversation_opened, {person, actor} = key}) do
    if MapSet.member?(engine.conversation_opened, key),
      do: [{:person, person}, {:actor, actor}],
      else: []
  end

  defp authority_owners(_engine, _address), do: []

  defp authority_address({:origin_feed, scope, _}), do: {:row, :origin_feeds, scope}
  defp authority_address(address), do: address

  defp row_owners(engine, field, key, person, actor) do
    if Map.has_key?(Map.fetch!(engine, field), key),
      do: [{:person, person}, {:actor, actor}],
      else: []
  end

  def upload_index(engine) do
    for {scope, feed} <- engine.origin_feeds, {seq, entry} <- feed.entries, reduce: %{} do
      index -> add_upload_links(index, {scope, seq}, entry)
    end
  end

  def upload_changes(index, old, next, touched) do
    {index, changed, uploads} =
      Enum.reduce(touched, {index, MapSet.new(), []}, fn
        {:origin_feed, scope, {:entry, seq}}, {index, changed, uploads} ->
          key = {scope, seq}
          before_entry = get_in(old.origin_feeds, [scope, :entries, seq])
          after_entry = get_in(next.origin_feeds, [scope, :entries, seq])

          index =
            Enum.reduce(upload_ids(before_entry), index, fn id, acc ->
              owners = MapSet.delete(Map.get(acc, id, MapSet.new()), key)
              if MapSet.size(owners) == 0, do: Map.delete(acc, id), else: Map.put(acc, id, owners)
            end)

          {add_upload_links(index, key, after_entry), MapSet.put(changed, key), uploads}

        {:row, :uploads, id}, {index, changed, uploads} ->
          {index, changed, [id | uploads]}

        _, acc ->
          acc
      end)

    related = Enum.reduce(uploads, changed, &MapSet.union(Map.get(index, &1, MapSet.new()), &2))

    entries =
      for {{person, _} = scope, seq} <- related,
          entry = get_in(next.origin_feeds, [scope, :entries, seq]),
          do: {person, entry}

    {:ok, index, entries}
  end

  defp add_upload_links(index, key, entry) do
    Enum.reduce(upload_ids(entry), index, fn id, acc ->
      Map.update(acc, id, MapSet.new([key]), &MapSet.put(&1, key))
    end)
  end

  defp upload_ids(nil), do: []

  defp upload_ids(entry) do
    for %{"tier" => "upload", "upload_id" => id} <- Map.get(entry, "files", []), do: id
  end

  # The committed Engine satisfied the full validator. Each changed sequence
  # must still be inside its person's high-water mark, and the live row count
  # must change by exactly the same amount as that high-water mark.
  def sequences(old, next, touched) do
    touched = MapSet.new(touched)

    with {:ok, deltas} <- sequence_deltas(old, next, touched),
         true <-
           Enum.all?(deltas, fn {{highs, person}, delta} ->
             old_high = Map.get(Map.fetch!(old, highs), person, 0)
             next_high = Map.get(Map.fetch!(next, highs), person, 0)

             is_integer(next_high) and next_high >= 0 and
               delta == next_high - old_high and
               (old_high == next_high or MapSet.member?(touched, {:row, highs, person})) and
               (next_high == 0 or Map.has_key?(next.persons, person))
           end) do
      :ok
    else
      _ -> {:error, :invalid_sequence}
    end
  end

  defp sequence_deltas(old, next, touched) do
    Enum.reduce_while(touched, {:ok, %{}}, fn
      {:row, field, person}, {:ok, deltas} when field in [:persons, :conversation_highs] ->
        {:cont, {:ok, Map.put_new(deltas, {field, person}, 0)}}

      {:row, field, {person, seq} = key}, {:ok, deltas}
      when field in [:events, :conversation_events] ->
        highs = if field == :events, do: :persons, else: :conversation_highs
        old_event = Map.fetch(Map.fetch!(old, field), key)
        new_event = Map.fetch(Map.fetch!(next, field), key)
        high = Map.get(Map.fetch!(next, highs), person, 0)

        if valid_event?(new_event, next, field, person, seq, high) do
          delta = present(new_event) - present(old_event)
          {:cont, {:ok, Map.update(deltas, {highs, person}, delta, &(&1 + delta))}}
        else
          {:halt, {:error, :invalid_sequence}}
        end

      _, acc ->
        {:cont, acc}
    end)
  end

  defp valid_event?(:error, _next, _field, _person, seq, high),
    do: is_integer(seq) and is_integer(high) and seq > high

  defp valid_event?(
         {:ok, %{key: {person, _, _} = key, kind: kind}},
         next,
         field,
         person,
         seq,
         high
       ) do
    is_integer(seq) and seq > 0 and is_integer(high) and seq <= high and
      valid_kind?(field, kind) and Map.has_key?(next.messages, key)
  end

  defp valid_event?(_, _, _, _, _, _), do: false

  defp valid_kind?(:events, kind), do: kind in [:delivery, :lifecycle]
  defp valid_kind?(:conversation_events, kind), do: kind == :conversation

  defp present({:ok, _}), do: 1
  defp present(:error), do: 0
end
