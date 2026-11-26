defmodule AttentionServer.SharedReminders do
  @moduledoc false

  alias AttentionServer.{BounceDecision, BouncePolicy, ConversationFiles, Engine, SharedAnswers, SharedMessages}
  alias AttentionServer.ClientAPI.PeopleProfiles

  def schedule(old, engine, touched, now) do
    keys =
      Enum.reduce(touched, MapSet.new(), fn
        {:row, :shared_recipients, {id, person} = key}, keys ->
          case {old.shared_recipients[key], engine.shared_recipients[key]} do
            {nil, %{}} -> MapSet.put(keys, {id, person})
            {%{frozen: _}, recipient} when is_map(recipient) and not is_map_key(recipient, :frozen) ->
              MapSet.put(keys, key)
            _ -> keys
          end

        {:row, :shared_messages, id}, keys ->
          case {old.shared_messages[id], engine.shared_messages[id]} do
            {%{revision: previous} = before, %{revision: current} = after_record} when current > previous ->
              if progress_only?(before, after_record) do
                keys
              else
                Enum.reduce(SharedMessages.live_recipients(engine, id), keys,
                  &MapSet.put(&2, {id, &1}))
              end

            _ -> keys
          end

        _, keys -> keys
      end)

    if MapSet.size(keys) == 0 do
      {engine, touched}
    else
      {next, :ok, more} =
        Engine.with_touches(engine, fn state ->
          {Enum.reduce(keys, state, &schedule_recipient(&2, &1, now)), :ok}
        end)

      {%{next | touched_keys: engine.touched_keys}, MapSet.union(touched, more)}
    end
  end

  defp progress_only?(before, after_record) do
    BounceDecision.progress_only?(before.message, Map.get(before, :files, []),
      after_record.message, Map.get(after_record, :files, []))
  end

  defp schedule_recipient(engine, {id, person} = key, now) do
    record = engine.shared_messages[id]
    recipient = engine.shared_recipients[key]

    authoritative_at = SharedMessages.substantive_accepted_at(record) || now
    personal_start = max(now, authoritative_at)

    updated =
      with true <- eligible?(engine, record, recipient, person),
           true <- BouncePolicy.eligible_at?(authoritative_at),
           %{"decision" => "pending"} = selection <-
             BounceDecision.shared_selection(engine, person, record.message, personal_start) do
        Map.put(recipient, :reminder, %{revision: record.revision, accepted_at_ms: authoritative_at, selection: selection})
      else
        _ -> Map.delete(recipient, :reminder)
      end

    Engine.put_row(engine, :shared_recipients, key, updated)
  end

  def next_deadline(engine, groups? \\ false) do
    engine
    |> outstanding(groups?)
    |> Enum.map(fn {_, _, _, _, reminder} -> reminder.selection["due_at_ms"] end)
    |> Enum.min(fn -> nil end)
  end

  def evaluate_due(engine, now, groups? \\ false) do
    due =
      engine
      |> outstanding(groups?)
      |> Enum.filter(fn {_, _, _, _, reminder} -> reminder.selection["due_at_ms"] <= now end)

    {next, groups, changed} =
      Enum.reduce(due, {engine, %{}, []}, fn {id, person, record, recipient, reminder},
                                           {next, groups, changed} ->
        reason = reason(engine, record, recipient, reminder, person)
        updated = Map.put(recipient, :reminder, Map.put(reminder, :outcome, reason))
        next = Engine.put_row(next, :shared_recipients, {id, person}, updated)

        groups =
          if reason == :suppressed do
            groups
          else
            Map.update(groups, {id, reminder.revision, reason}, [{person, reminder}],
              &[{person, reminder} | &1])
          end

        {next, groups, [{id, person} | changed]}
      end)

    Enum.reduce(groups, {next, [], changed}, fn {{id, revision, reason}, people}, {next, advisories, changed} ->
      record = next.shared_messages[id]

      if Map.has_key?(record, :sender_frozen_revision) do
        {next, advisories, changed}
      else
        advisory = build_advisory(next, revision, reason, people, now)
        notices = Map.put(Map.get(record, :shared_advisories, %{}), advisory["advisory_id"], advisory)
        next = Engine.put_row(next, :shared_messages, id, Map.put(record, :shared_advisories, notices))
        {next, [{id, advisory["advisory_id"]} | advisories], changed}
      end
    end)
  end

  def advisory_projection(engine, record) do
    record
    |> Map.get(:shared_advisories, %{})
    |> Map.values()
    |> Enum.sort_by(&{&1["request_revision"], &1["evaluated_at_ms"], &1["advisory_id"]})
    |> Enum.map(&wire(engine, record, &1))
  end

  def wire(engine, record, advisory) do
    revision = advisory["request_revision"]

    active? =
      not Map.has_key?(record, :sender_frozen_revision) and
        Enum.any?(advisory["people"], fn %{"id" => person} ->
          recipient = engine.shared_recipients[{record.id, person}]
          with %{reminder: %{revision: ^revision} = reminder} <- recipient,
               true <- eligible?(engine, record, recipient, person) do
            Atom.to_string(reason(engine, record, recipient, reminder, person)) == advisory["advisory_type"]
          else
            _ -> false
          end
        end)

    advisory |> Map.put("moot", not active?) |> Map.put_new("decided_at_rev", 0)
  end

  def acknowledge(engine, record, id) when is_binary(id) do
    with %{} = advisory <- get_in(record, [:shared_advisories, id]) do
      projection = wire(engine, record, advisory)
      result =
        %{"advisory_id" => id, "already_delivered" => projection["delivered"],
          "delivered" => projection["delivered"], "moot" => projection["moot"]}

      if projection["moot"] or projection["delivered"] do
        {:ok, engine, result}
      else
        record = put_in(record[:shared_advisories][id]["delivered"], true)
        {:ok, Engine.put_row(engine, :shared_messages, record.id, record), %{result | "delivered" => true}}
      end
    else
      nil -> {:error, :not_found}
    end
  end

  def acknowledge(_engine, _record, _id), do: {:error, :invalid_field, "advisory_id"}

  def valid_recipient?(recipient, revision) do
    case Map.get(recipient, :reminder) do
      nil ->
        true

      %{revision: recorded, accepted_at_ms: accepted, selection: %{"due_at_ms" => due}} = reminder ->
        is_integer(recorded) and recorded in 1..revision and is_integer(accepted) and
          is_integer(due) and due >= accepted and
          Map.get(reminder, :outcome) in [nil, :suppressed, :soft_bounce, :unseen_interval_elapsed]

      _ ->
        false
    end
  end

  def bounced?(recipient), do: match?(%{reminder: %{outcome: :unseen_interval_elapsed}}, recipient)

  defp outstanding(engine, groups?) do
    case BouncePolicy.cutoff() do
      {:ok, cutoff} ->
        for {{id, person}, %{reminder: reminder} = recipient} <- engine.shared_recipients,
            not Map.has_key?(reminder, :outcome),
            record = engine.shared_messages[id],
            groups? or not Map.has_key?(record, :group_id),
            accepted = SharedMessages.substantive_accepted_at(record) || reminder.accepted_at_ms,
            BouncePolicy.eligible_at?(accepted, cutoff),
            eligible?(engine, record, recipient, person),
            do: {id, person, record, recipient, reminder}

      {:error, _} ->
        []
    end
  end

  defp eligible?(engine, record, recipient, person) do
    not Map.has_key?(recipient, :frozen) and not Map.has_key?(record, :terminal) and
      MapSet.member?(SharedMessages.organizations(engine, person), record.organization_id) and
      (SharedAnswers.available?(engine, record, recipient, person) or
        (is_nil(record.answer_policy) and recipient.seen_revision != record.revision))
  end

  defp reason(engine, record, recipient, reminder, person) do
    discussed? =
      Enum.any?(SharedMessages.entries(engine, record), fn entry ->
        entry.author == {:person, person} and entry.request_revision >= reminder.revision
      end)

    cond do
      discussed? -> :suppressed
      recipient.seen_revision == record.revision -> :soft_bounce
      true -> :unseen_interval_elapsed
    end
  end

  defp build_advisory(engine, revision, reason, people, now) do
    people = Enum.sort_by(people, &elem(&1, 0))
    {_, first} = Enum.min_by(people, fn {_, reminder} -> reminder.selection["due_at_ms"] end)

    first.selection
    |> Map.take(["due_at_ms", "bounce_after_days", "level_name"])
    |> Map.merge(%{
      "advisory_id" => ConversationFiles.new_upload_id(),
      "advisory_type" => Atom.to_string(reason),
      "author" => "server",
      "request_revision" => revision,
      "accepted_at_ms" => first.accepted_at_ms,
      "evaluated_at_ms" => now,
      "text" => advisory_text(reason),
      "delivered" => false,
      "people" => Enum.map(people, fn {person, _} -> PeopleProfiles.speaker(engine, person) end)
    })
  end

  defp advisory_text(:soft_bounce), do: "These people viewed this revision but have yet to act."
  defp advisory_text(:unseen_interval_elapsed), do: "These people had not opened this revision when the reminder interval elapsed."
end
