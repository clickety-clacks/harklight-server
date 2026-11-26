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
    :alert_choices,
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
  @item_keys [:bounce_revisions, :chat_read_positions, :item_availability, :messages, :reply_group_history]
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
        prior === entry or Conversation.answer_baseline_mark?(prior, entry)
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

  def source_key(%{source_binding: binding}) when is_map(binding) do
    {binding["source_instance_id"], binding["request_id"], binding["request_revision"]}
  end

  def source_key(_), do: nil

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
    :alert_choices,
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

  defp authority_owners(engine, {:row, :origin_feeds, scope}),
    do: authority_owners(engine, {:origin_feed, scope, :high})

  defp authority_owners(engine, address) do
    case RecordCodec.lookup(engine, address) do
      {:ok, value} -> record_authority(address, value)
      :error -> []
    end
  end

  def record_authority({:row, :messages, {person, _, _}}, record) do
    actors = [record.origin, get_in(record, [:terminal, :actor])] |> Enum.reject(&is_nil/1)
    [{:person, person} | Enum.map(actors, &{:actor, &1})]
  end

  def record_authority({:row, :uploads, _}, upload) do
    [
      {:person, upload.person},
      {:actor, upload.uploader},
      {:scope, :uploads, {upload.person, upload.uploader}}
    ]
  end

  def record_authority({:row, :maintainer_grants, _}, %{target_person: target, grantee_person: grantee}),
    do: [{:person, target}, {:person, grantee}]

  def record_authority(
        {:row, :enrollment_read_grants, _},
        %{person: person, connector: key, target: %{"key_fingerprint" => target}}
      ),
      do: [{:person, person}, {:actor, key}, {:actor, target}]

  def record_authority({:row, field, _}, %{person: person})
      when field in [:hosted_accounts, :devices, :agents],
      do: [{:person, person}]

  def record_authority({:row, field, _}, %{state: :approved, key: key, person: person})
      when field in [:pending_enrollments, :pending_history],
      do: [{:person, person}, {:actor, key}]

  def record_authority({:row, field, _}, %{person: person})
      when field in [:pending_enrollments, :pending_history] and is_binary(person),
      do: [{:person, person}]

  def record_authority({:row, field, _}, %{person: person, code: code})
      when field in [:browser_sign_ins, :join_invitations] and is_binary(code),
      do: [{:person, person}, {:scope, field, person}, {:scope, :invitation_code, code}]

  def record_authority({:row, field, _}, %{person: person})
      when field in [:browser_sign_ins, :join_invitations],
      do: [{:person, person}, {:scope, field, person}]

  def record_authority({:row, field, person}, _)
      when field in [
             :owners,
             :delivery_states,
             :algorithm_choices,
             :alert_choices,
             :free_order_highs,
             :bounce_levels
           ],
      do: [{:person, person}]

  def record_authority({:row, field, {person, actor}}, _)
      when field in [:sender_grants, :subscriptions, :push_bindings, :cursors, :conversation_cursors],
      do: [{:person, person}, {:actor, actor}]

  def record_authority({:row, field, {person, actor, _}}, _)
      when field in [:receipts, :source_report_grants],
      do: [{:person, person}, {:actor, actor}]

  def record_authority({:row, :origin_sessions, {person, actor, _}}, _),
    do: [{:person, person}, {:actor, actor}, {:scope, :origin_sessions, {person, actor}}]

  def record_authority({:origin_feed, {person, actor}, :high}, _),
    do: [{:person, person}, {:actor, actor}]

  def record_authority({:member, :conversation_opened, {person, actor}}, true),
    do: [{:person, person}, {:actor, actor}]

  def record_authority(_, _), do: []

  defp authority_address({:origin_feed, scope, _}), do: {:row, :origin_feeds, scope}
  defp authority_address(address), do: address

  # Values the full validator requires to be carried by one row at most, as
  # {namespace, value} => addresses carrying it (inventory rows in brackets):
  #   :principal_key  one key space over device, agent, owner and retired
  #                   owner keys, and owner keys among owners (#6, #7, #8, #39)
  #   :pending_secret secret hashes across pending requests and history (#10, #13)
  #   :hosted_person  one hosted account per person (#19)
  #   :agent_id       agents by {person, id} (#40)
  #   :push_address   active push addresses (#24)
  # Devices, agents and retired keys are each keyed by the key itself, so one
  # carrier per principal key is exactly the pairwise checks of #6-#8 and #39.
  @unique_rows [
    :owners,
    :devices,
    :agents,
    :pending_enrollments,
    :pending_history,
    :hosted_accounts,
    :push_bindings
  ]

  def unique_index(engine) do
    index =
      for field <- @unique_rows, key <- Map.keys(Map.fetch!(engine, field)), reduce: %{} do
        index -> edit_unique_index(index, engine, {:row, field, key}, :add)
      end

    Enum.reduce(engine.retired_owner_keys, index, fn key, index ->
      edit_unique_index(index, engine, {:member, :retired_owner_keys, key}, :add)
    end)
  end

  def unique_rows(index, namespace, value),
    do: Map.get(index, {namespace, value}, MapSet.new())

  # The updated index and the first {namespace, value} a touched address now
  # shares with another row, or nil. The committed state had no shared value,
  # so every conflict includes a touched address carrying the value.
  def unique_changes(index, old, next, touched) do
    updated =
      Enum.reduce(touched, index, fn address, acc ->
        acc
        |> edit_unique_index(old, address, :remove)
        |> edit_unique_index(next, address, :add)
      end)

    conflict =
      Enum.find_value(touched, fn address ->
        Enum.find(unique_values(next, address), &(MapSet.size(Map.get(updated, &1)) > 1))
      end)

    {updated, conflict}
  end

  defp edit_unique_index(index, engine, address, operation) do
    Enum.reduce(unique_values(engine, address), index, fn value, acc ->
      rows = Map.get(acc, value, MapSet.new())

      next =
        if operation == :add, do: MapSet.put(rows, address), else: MapSet.delete(rows, address)

      if MapSet.size(next) == 0, do: Map.delete(acc, value), else: Map.put(acc, value, next)
    end)
  end

  # The patterns match the full validator's projections; a record they do not
  # match fails its own row check.
  defp unique_values(engine, {:row, :owners, person}) do
    case Map.get(engine.owners, person) do
      %{key: key} -> [{:principal_key, key}]
      _ -> []
    end
  end

  defp unique_values(engine, {:row, :devices, key}),
    do: if(Map.has_key?(engine.devices, key), do: [{:principal_key, key}], else: [])

  defp unique_values(engine, {:row, :agents, key}) do
    case Map.fetch(engine.agents, key) do
      {:ok, %{person: person, id: id}} -> [{:principal_key, key}, {:agent_id, {person, id}}]
      {:ok, _} -> [{:principal_key, key}]
      :error -> []
    end
  end

  defp unique_values(engine, {:member, :retired_owner_keys, key}),
    do:
      if(MapSet.member?(engine.retired_owner_keys, key),
        do: [{:principal_key, key}],
        else: []
      )

  defp unique_values(engine, {:row, field, code})
       when field in [:pending_enrollments, :pending_history] do
    case Map.get(Map.fetch!(engine, field), code) do
      %{secret_hash: hash} -> [{:pending_secret, hash}]
      _ -> []
    end
  end

  defp unique_values(engine, {:row, :hosted_accounts, ref}) do
    case Map.get(engine.hosted_accounts, ref) do
      %{person: person} -> [{:hosted_person, person}]
      _ -> []
    end
  end

  defp unique_values(engine, {:row, :push_bindings, key}) do
    case Map.get(engine.push_bindings, key) do
      %{active: true, address: address} -> [{:push_address, address}]
      _ -> []
    end
  end

  defp unique_values(_engine, _address), do: []

  def upload_index(engine) do
    index =
      for {scope, feed} <- engine.origin_feeds, {seq, entry} <- feed.entries, reduce: %{} do
        index ->
          add_upload_links(index, {:origin_feed, scope, {:entry, seq}}, entry_upload_ids(entry))
      end

    Enum.reduce(engine.messages, index, fn {key, record}, index ->
      add_upload_links(index, {:row, :messages, key}, revision_upload_ids(record))
    end)
  end

  def upload_changes(index, old, next, touched) do
    {index, changed, uploads} =
      Enum.reduce(touched, {index, MapSet.new(), []}, fn
        {:origin_feed, scope, {:entry, seq}} = address, {index, changed, uploads} ->
          before_ids = entry_upload_ids(get_in(old.origin_feeds, [scope, :entries, seq]))
          after_ids = entry_upload_ids(get_in(next.origin_feeds, [scope, :entries, seq]))

          {update_upload_links(index, address, before_ids, after_ids),
           MapSet.put(changed, address), uploads}

        {:row, :messages, key} = address, {index, changed, uploads} ->
          before_ids = revision_upload_ids(Map.get(old.messages, key))
          after_ids = revision_upload_ids(Map.get(next.messages, key))

          {update_upload_links(index, address, before_ids, after_ids),
           MapSet.put(changed, address), uploads}

        {:row, :uploads, id}, {index, changed, uploads} ->
          {index, changed, [id | uploads]}

        _, acc ->
          acc
      end)

    related = Enum.reduce(uploads, changed, &MapSet.union(Map.get(index, &1, MapSet.new()), &2))
    {:ok, index, related}
  end

  defp update_upload_links(index, address, before_ids, after_ids) do
    index =
      Enum.reduce(before_ids, index, fn id, index ->
        owners = MapSet.delete(Map.get(index, id, MapSet.new()), address)
        if MapSet.size(owners) == 0, do: Map.delete(index, id), else: Map.put(index, id, owners)
      end)

    add_upload_links(index, address, after_ids)
  end

  defp add_upload_links(index, address, ids) do
    Enum.reduce(ids, index, fn id, index ->
      Map.update(index, id, MapSet.new([address]), &MapSet.put(&1, address))
    end)
  end

  def record_uploads({:origin_feed, _, {:entry, _}}, entry), do: entry_upload_ids(entry)
  def record_uploads({:row, :messages, _}, record), do: revision_upload_ids(record)
  def record_uploads(_, _), do: []

  defp entry_upload_ids(nil), do: []

  defp entry_upload_ids(entry) do
    for %{"tier" => "upload", "upload_id" => id} <- Map.get(entry, "files", []), do: id
  end

  defp revision_upload_ids(nil), do: MapSet.new()

  defp revision_upload_ids(record) do
    for {_revision, files} <- Map.get(record, :files_by_revision, %{}),
        %{"tier" => "upload", "upload_id" => id} <- files,
        into: MapSet.new(),
        do: id
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

  # A record that changed without a touched address would be acknowledged in
  # memory but never written, so a keyed commit refuses it before writing.
  # Unchanged values share their terms with the current engine, so this is
  # an identity scan of the persisted fields. Over-reporting is allowed.
  def untouched_change(old, next, touched) do
    old
    |> Map.from_struct()
    |> Map.drop([:advisory_feeds, :origin_feeds | RecordCodec.derived_fields()])
    |> Enum.reduce_while(:ok, fn {field, before}, :ok ->
      case untouched_field_change(field, before, Map.fetch!(next, field), touched) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
    |> then(fn
      :ok ->
        with :ok <-
               untouched_feed_change(:origin_feed, old.origin_feeds, next.origin_feeds, touched) do
          untouched_feed_change(:advisory, old.advisory_feeds, next.advisory_feeds, touched)
        end

      error ->
        error
    end)
  end

  defp untouched_field_change(_field, same, same, _touched), do: :ok

  defp untouched_field_change(field, %MapSet{} = before, %MapSet{} = after_set, touched) do
    before
    |> MapSet.symmetric_difference(after_set)
    |> first_untouched(&{:member, field, &1}, touched)
  end

  defp untouched_field_change(field, before, after_rows, touched)
       when is_map(before) and is_map(after_rows) do
    changed_keys(before, after_rows) |> first_untouched(&{:row, field, &1}, touched)
  end

  defp untouched_field_change(field, _before, _after, touched),
    do: first_untouched([field], &{:field, &1}, touched)

  defp untouched_feed_change(_kind, same, same, _touched), do: :ok

  defp untouched_feed_change(kind, before, after_feeds, touched) do
    before
    |> Map.keys()
    |> Enum.concat(Map.keys(after_feeds))
    |> Enum.uniq()
    |> Enum.reduce_while(:ok, fn scope, :ok ->
      case untouched_scope_change(
             kind,
             scope,
             Map.get(before, scope),
             Map.get(after_feeds, scope),
             touched
           ) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp untouched_scope_change(_kind, _scope, same, same, _touched), do: :ok

  defp untouched_scope_change(kind, scope, before, after_feed, touched) do
    before = before || %{}
    after_feed = after_feed || %{}

    facts =
      [{:high, Map.get(before, :high), Map.get(after_feed, :high)}] ++
        collection(kind, :entries, :entry, before, after_feed) ++
        if(kind == :origin_feed,
          do: collection(kind, :delivery_acks, :delivery_ack, before, after_feed),
          else: receipts(before, after_feed)
        )

    Enum.reduce_while(facts, :ok, fn
      {_fact, same, same}, :ok -> {:cont, :ok}
      {fact, _, _}, :ok ->
        case first_untouched([fact], &{kind, scope, &1}, touched) do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end
    end)
  end

  defp collection(_kind, field, tag, before, after_feed) do
    old = Map.get(before, field, %{})
    new = Map.get(after_feed, field, %{})

    for key <- changed_keys(old, new),
        do: {{tag, key}, Map.get(old, key), Map.get(new, key, :removed)}
  end

  defp receipts(before, after_feed) do
    old = Map.get(before, :receipts, MapSet.new())
    new = Map.get(after_feed, :receipts, MapSet.new())

    for id <- MapSet.symmetric_difference(old, new),
        do: {{:receipt, id}, MapSet.member?(old, id), MapSet.member?(new, id)}
  end

  defp changed_keys(same, same), do: []

  defp changed_keys(before, after_rows) do
    added_or_changed =
      for {key, value} <- after_rows, Map.fetch(before, key) !== {:ok, value}, do: key

    removed = for {key, _} <- before, not Map.has_key?(after_rows, key), do: key
    added_or_changed ++ removed
  end

  defp first_untouched(keys, address, touched) do
    case Enum.find(keys, &(not MapSet.member?(touched, address.(&1)))) do
      nil -> :ok
      key -> {:error, {:untouched_change, address.(key)}}
    end
  end
end
