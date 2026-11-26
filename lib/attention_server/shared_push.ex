defmodule AttentionServer.SharedPush do
  @moduledoc false

  alias AttentionServer.{ConversationFiles, Engine, Message, SharedAnswers, SharedMessages, SharedReminders}

  def pending(binding), do: Map.get(binding, :pending_shared, %{})
  def pending?(binding), do: map_size(pending(binding)) > 0

  def valid?(binding) do
    work = pending(binding)

    is_map(work) and
      (map_size(work) == 0 or (binding.active and Map.get(binding, :shared_messages, false))) and
      Enum.all?(work, fn
        {id, {stamp, :badge}} -> is_binary(id) and is_binary(stamp)
        {id, {stamp, {:request, revision}}} ->
          is_binary(id) and is_binary(stamp) and is_integer(revision) and revision > 0
        {id, {stamp, {:chat, entry}}} ->
          is_binary(id) and is_binary(stamp) and is_binary(entry)
        _ -> false
      end)
  end

  def enqueue(old, engine, touched, audit) do
    changes = changes(old, engine, touched, audit)

    if map_size(changes) == 0 do
      {engine, touched}
    else
      {next, :ok, more} =
        Engine.with_touches(engine, fn state ->
          {update_bindings(state, changes), :ok}
        end)

      {%{next | touched_keys: engine.touched_keys}, MapSet.union(touched, more)}
    end
  end

  defp changes(old, engine, touched, audit) do
    changed =
      Enum.reduce(touched, %{}, fn
        {:row, :shared_recipients, {id, person} = key}, changes ->
          case {old.shared_recipients[key], engine.shared_recipients[key]} do
            {_, %{frozen: _}} -> Map.put(changes, {person, id}, :remove)
            {nil, %{}} -> request_hint(changes, engine, person, id)
            {%{frozen: _}, %{}} -> request_hint(changes, engine, person, id)
            _ -> changes
          end

        {:row, :shared_messages, id}, changes ->
          message_changes(changes, old.shared_messages[id], engine, id)

        _, changes ->
          changes
      end)

    case audit do
      %{shared_chat: %{operation: :reply, message_id: id, entry_id: entry}} ->
        chat_changes(changed, engine, id, engine.shared_entries[entry])

      %{shared_chat: %{operation: :answer, message_id: id}} ->
        badge_changes(changed, engine, id)

      %{shared_chat: %{operation: :read_position, person: person, message_id: id}} ->
        Map.put_new(changed, {person, id}, :badge)

      %{shared_reminder_recipients: recipients} ->
        Enum.reduce(recipients, changed, fn {id, person}, acc ->
          Map.put_new(acc, {person, id}, :badge)
        end)

      %{shared_default_answers: ids} ->
        Enum.reduce(ids, changed, &badge_changes(&2, engine, engine.shared_answers[&1].shared_id))

      _ ->
        changed
    end
  end

  defp message_changes(changes, before, engine, id) do
    case {before, engine.shared_messages[id]} do
      {%{revision: previous}, %{revision: current} = record} when current > previous ->
        if AttentionServer.BounceDecision.progress_only?(
             before.message, Map.get(before, :files, []), record.message, Map.get(record, :files, [])
           ) do
          changes
        else
          Enum.reduce(SharedMessages.live_recipients(engine, id), changes,
            &request_hint(&2, engine, &1, id))
        end

      {%{} = old, %{terminal: terminal}} when not is_map_key(old, :terminal) and not is_nil(terminal) ->
        badge_changes(changes, engine, id)

      _ ->
        changes
    end
  end

  defp request_hint(changes, engine, person, id) do
    record = engine.shared_messages[id]
    recipient = engine.shared_recipients[{id, person}]

    hint =
      if alertable?(engine, record, recipient, person),
        do: {:request, record.revision},
        else: :badge

    Map.put(changes, {person, id}, hint)
  end

  defp badge_changes(changes, engine, id) do
    Enum.reduce(SharedMessages.live_recipients(engine, id), changes, fn person, acc ->
      Map.put_new(acc, {person, id}, :badge)
    end)
  end

  defp chat_changes(changes, engine, id, entry) do
    target = Map.get(entry, :notify_person_id)

    Enum.reduce(SharedMessages.live_recipients(engine, id), changes, fn person, acc ->
      cond do
        entry.author == {:person, person} -> acc
        is_nil(target) or target == person -> Map.put(acc, {person, id}, {:chat, entry.id})
        true -> Map.put_new(acc, {person, id}, :badge)
      end
    end)
  end

  defp update_bindings(engine, changes) do
    by_person = Enum.group_by(changes, fn {{person, _}, _} -> person end)
    now = System.system_time(:millisecond)
    stamp = ConversationFiles.new_upload_id()

    Enum.reduce(engine.push_bindings, engine, fn
      {{person, _} = key, %{active: true, shared_messages: true} = binding}, next ->
        work =
          Enum.reduce(Map.get(by_person, person, []), pending(binding), fn
            {{_, id}, :remove}, work -> Map.delete(work, id)
            {{_, id}, hint}, work ->
              if deliverable?(engine, person, binding, id),
                do: put_hint(work, id, stamp, hint),
                else: Map.delete(work, id)
          end)

        updated = replace_pending(binding, work, now)
        Engine.put_row(next, :push_bindings, key, updated)

      _, next ->
        next
    end)
  end

  defp put_hint(work, id, stamp, :badge) do
    case work[id] do
      {_, {:chat, entry}} -> Map.put(work, id, {stamp, {:chat, entry}})
      _ -> Map.put(work, id, {stamp, :badge})
    end
  end

  defp put_hint(work, id, stamp, hint), do: Map.put(work, id, {stamp, hint})

  defp replace_pending(binding, work, now) do
    if work == pending(binding) do
      binding
    else
      binding = if map_size(work) == 0, do: Map.delete(binding, :pending_shared), else: Map.put(binding, :pending_shared, work)

      cond do
        binding.state in ["sending", "blocked_configuration"] -> binding
        map_size(work) > 0 ->
          %{binding | state: "queued", next_at: binding.next_at || now,
                      deadline_at: binding.deadline_at || now}
        binding.pending_high > 0 or binding.pending_conversation_high > 0 -> binding
        true -> %{binding | state: "ready", next_at: nil, deadline_at: nil}
      end
    end
  end

  def accepted(binding, claim) do
    work =
      Enum.reduce(claim.shared_work, pending(binding), fn {id, hint}, work ->
        if work[id] == hint, do: Map.delete(work, id), else: work
      end)

    if map_size(work) == 0, do: Map.delete(binding, :pending_shared), else: Map.put(binding, :pending_shared, work)
  end

  def target(engine, person, binding, work) do
    work
    |> Enum.flat_map(fn {id, {_, hint}} ->
      with true <- deliverable?(engine, person, binding, id),
           record = engine.shared_messages[id],
           recipient = engine.shared_recipients[{id, person}],
           {:ok, alert} <- current_alert(engine, record, recipient, person, hint) do
        [alert]
      else
        _ -> []
      end
    end)
    |> Enum.max_by(&{&1.at, &1.item_id}, fn -> nil end)
  end

  def deliverable?(engine, person, binding, id) do
    with %{organization_id: organization} = record <- engine.shared_messages[id],
         true <- binding_visible?(binding, record),
         %{} = recipient <- engine.shared_recipients[{id, person}],
         false <- Map.has_key?(recipient, :frozen),
         true <- MapSet.member?(SharedMessages.organizations(engine, person), organization) do
      not muted?(engine, record, person)
    else
      _ -> false
    end
  end

  def binding_visible?(%{groups: true}, %{group_id: _}), do: true
  def binding_visible?(_binding, %{group_id: _}), do: false
  def binding_visible?(_binding, %{}), do: true
  def binding_visible?(_binding, _record), do: false

  defp muted?(engine, %{group_id: group}, person) do
    get_in(Map.get(engine, :groups, %{}), [group, :members, person]) == true
  end

  defp muted?(_engine, _record, _person), do: false

  defp current_alert(engine, %{revision: revision} = record, recipient, person, {:request, revision}) do
    if recipient.seen_revision != revision and alertable?(engine, record, recipient, person) do
      text = record.message["body"] || record.message["title"]
      title = record.message["display_name"] || record.sender_id
      at = Map.get(record, :revision_accepted_at_ms, record["created_at_ms"] || 0)
      {:ok, %{item_id: recipient.item_id, at: at, title: title, text: text, chat?: false}}
    end
  end

  defp current_alert(_engine, %{terminal: %{"state" => "retracted"}}, _recipient, _person, {:chat, _}), do: nil

  defp current_alert(engine, record, recipient, person, {:chat, entry_id}) do
    case engine.shared_entries[entry_id] do
      %{shared_id: id, author: author, position: position} = entry
      when id == record.id and author != {:person, person} ->
        if position > Map.get(recipient, :read_position, 0) do
          title = entry.speaker["display_name"] || record.sender_id
          {:ok, %{item_id: recipient.item_id, at: entry.at_ms, title: title, text: entry.text, chat?: true}}
        end

      _ ->
        nil
    end
  end

  defp current_alert(_engine, _record, _recipient, _person, _hint), do: nil

  defp alertable?(engine, record, recipient, person) do
    message = record.message

    not Message.progress_message?(message) and not SharedReminders.bounced?(recipient) and not Map.has_key?(record, :terminal) and
      (SharedAnswers.available?(engine, record, recipient, person) or
         (is_nil(record.answer_policy) and
            (Engine.alert_choice(engine, person) == "all" or Message.effective_urgency(message) in ["high", "now"])))
  end

end
