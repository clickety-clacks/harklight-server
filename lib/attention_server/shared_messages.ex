defmodule AttentionServer.SharedMessages do
  @moduledoc false

  alias AttentionServer.{BounceDecision, Engine, SharedAnswers}
  alias AttentionServer.ClientAPI.{PeopleProfiles, SharedAttachments, SharedCapability}

  def organizations(%Engine{} = engine, person) do
    hosted =
      for %{id: id, members: members} <- Map.values(engine.hosted_organizations),
          MapSet.member?(members, person),
          do: id

    local = Map.get(engine.local_organizations, :default, MapSet.new())

    if MapSet.member?(local, person),
      do: MapSet.new([engine.relay_instance_id | hosted]),
      else: MapSet.new(hosted)
  end

  def select_organization(%Engine{} = engine, sender_person, recipients, selected) do
    sender_organizations = organizations(engine, sender_person)

    if MapSet.size(sender_organizations) == 0 do
      {:error, :unsupported}
    else
      common =
        Enum.reduce(recipients, sender_organizations, fn person, eligible ->
          MapSet.intersection(eligible, organizations(engine, person))
        end)

      choose_organization(common, selected)
    end
  end

  defp choose_organization(common, selected) when is_binary(selected) do
    if MapSet.member?(common, selected),
      do: {:ok, selected},
      else: {:error, :recipient_refused}
  end

  defp choose_organization(common, nil) do
    case Enum.sort(common) do
      [] -> {:error, :recipient_refused}
      [id] -> {:ok, id}
      ids -> {:error, :organization_required, ids}
    end
  end

  # The surrounding sender transaction checks the enrolled key and sender ID,
  # supplies its stable operation-derived ID, and commits the receipt with these
  # rows. This transition never copies message content into recipient state.
  def create(%Engine{} = engine, attrs, recipients, selected) do
    people = Enum.uniq(recipients)

    with {:ok, organization} <- select_organization(engine, attrs.sender_person, people, selected),
         outcomes <- Enum.map(people, &outcome(engine, &1)),
         reached = for({person, :reached} <- outcomes, do: person),
         :ok <- admissible_recipients(reached, people, attrs.answer_policy, outcomes),
         false <- Map.has_key?(engine.shared_messages, attrs.id) do
      record = Map.merge(attrs, %{organization_id: organization, revision: 1, chat_open: true})
      next = Engine.put_row(engine, :shared_messages, attrs.id, record)

      next =
        Enum.reduce(reached, next, fn person, acc ->
          row = %{item_id: local_item_id(attrs.id, person), seen_revision: nil}
          Engine.put_row(acc, :shared_recipients, {attrs.id, person}, row)
        end)

      {:ok, next, record, outcomes}
    else
      true -> {:error, :idempotency_conflict}
      error -> error
    end
  end

  defp outcome(engine, person) do
    if Engine.delivery_stopped?(engine, person),
      do: {person, :unreachable},
      else: {person, :reached}
  end

  defp admissible_recipients([], _people, _policy, outcomes), do: {:error, :no_recipients, outcomes}

  defp admissible_recipients(reached, people, {:named, deciders}, outcomes) do
    case Enum.all?(deciders, &(&1 in people)) do
      false -> {:error, :invalid_deciders}
      true ->
        if Enum.all?(deciders, &(&1 in reached)),
          do: :ok,
          else: {:error, :decider_unreachable, outcomes}
    end
  end

  defp admissible_recipients(_reached, _people, _policy, _outcomes), do: :ok

  defp local_item_id(shared_id, person) do
    digest = :crypto.hash(:sha256, :erlang.term_to_binary({shared_id, person}))
    "shared_" <> Base.url_encode64(digest, padding: false)
  end

  def seen(%Engine{} = engine, record, person, recipient, revision, now) do
    cond do
      revision != record.revision ->
        {:error, :stale_request}

      recipient.seen_revision == revision ->
        {:ok, engine, nil}

      true ->
        id = stable_fact_id("seen_", {record.id, person, revision})
        event = %{
          id: id, shared_id: record.id, person: person, request_revision: revision,
          at_ms: now, speaker: PeopleProfiles.speaker(engine, person)
        }

        row = Map.merge(recipient, %{seen_revision: revision, seen_at_ms: now})

        next =
          engine
          |> Engine.put_row(:shared_recipients, {record.id, person}, row)
          |> Engine.put_row(:shared_seen, id, event)

        {:ok, next, id}
    end
  end

  def seen_event(event) do
    %{
      "item_id" => event.shared_id, "event_id" => event.id,
      "request_revision" => event.request_revision, "at_ms" => event.at_ms,
      "speaker" => event.speaker
    }
  end

  def valid_seen?(%Engine{} = engine, id, %{
        id: id, shared_id: shared_id, person: person, request_revision: revision,
        at_ms: at, speaker: %{"kind" => "person", "id" => person}
      }) do
    case engine.shared_messages[shared_id] do
      %{revision: current} ->
        is_binary(id) and revision in 1..current and is_integer(at) and
          Map.has_key?(engine.shared_recipients, {shared_id, person})
      nil -> false
    end
  end

  def valid_seen?(_, _, _), do: false

  defp stable_fact_id(prefix, value) do
    hash = :crypto.hash(:sha256, :erlang.term_to_binary(value))
    prefix <> Base.url_encode64(hash, padding: false)
  end

  def revise(%Engine{} = engine, record, revision, message, files, now) do
    with :ok <- writable_sender(record),
         true <- not Map.has_key?(record, :answer_id) and not Map.has_key?(record, :terminal) ||
                   {:error, :already_closed},
         true <- record.revision == revision || {:error, :precondition_failed} do
      history = Map.get(record, :history, %{})
      previous = Map.take(record, [:revision, :message]) |> Map.put(:files, Map.get(record, :files, []))
      updated =
        record
        |> Map.put(:history, Map.put(history, revision, previous))
        |> Map.put(:revision, revision + 1)
        |> Map.put(:revision_accepted_at_ms, now)
        |> Map.put(:message, message)
        |> Map.put(:files, files)

      next = Engine.put_row(engine, :shared_messages, record.id, updated)
      {:ok, carry_seen(next, record, updated)}
    end
  end

  defp carry_seen(engine, previous, updated) do
    files = Map.get(previous, :files, [])

    if BounceDecision.progress_only?(previous.message, files, updated.message, Map.get(updated, :files, [])) do
      Enum.reduce(live_recipients(engine, previous.id), engine, fn person, next ->
        key = {previous.id, person}
        case next.shared_recipients[key] do
          %{seen_revision: revision} = row when revision == previous.revision ->
            Engine.put_row(next, :shared_recipients, key, %{row | seen_revision: updated.revision})
          _ -> next
        end
      end)
    else
      engine
    end
  end

  def close_chat(%Engine{} = engine, record) do
    with :ok <- writable_sender(record),
         true <- record.chat_open || {:error, :already_closed} do
      {:ok, Engine.put_row(engine, :shared_messages, record.id, %{record | chat_open: false})}
    end
  end

  def clear_answers(%Engine{} = engine, %{answer_policy: :everyone} = record, now) do
    with :ok <- writable_sender(record),
         true <- not Map.has_key?(record, :terminal) || {:error, :already_closed} do
      updated = Map.put(record, :terminal, %{"state" => "cleared", "at_ms" => now})
      {:ok, Engine.put_row(engine, :shared_messages, record.id, updated)}
    end
  end

  def clear_answers(_engine, _record, _now), do: {:error, :unsupported}

  def retract(%Engine{} = engine, record, now) do
    with :ok <- writable_sender(record) do
      updated =
        record
        |> Map.put(:terminal, %{"state" => "retracted", "at_ms" => now})
        |> Map.put(:chat_open, false)

      {:ok, Engine.put_row(engine, :shared_messages, record.id, updated)}
    end
  end

  defp writable_sender(%{sender_frozen_revision: _}), do: {:error, :not_found}
  defp writable_sender(%{terminal: %{"state" => "retracted"}}), do: {:error, :already_retracted}
  defp writable_sender(_record), do: :ok

  def live_recipients(engine, id) do
    for {{^id, person}, recipient} <- engine.shared_recipients,
        not Map.has_key?(recipient, :frozen),
        do: person
  end

  def reply(%Engine{} = engine, record, author, attrs) do
    with :ok <- replyable(record),
         :ok <- notify_target(engine, record.id, attrs[:notify_person_id]) do
      position = Map.get(record, :entry_high, 0) + 1

      entry =
        Map.merge(attrs, %{
          shared_id: record.id,
          position: position,
          request_revision: record.revision,
          author: author
        })

      next =
        engine
        |> Engine.put_row(:shared_messages, record.id, Map.put(record, :entry_high, position))
        |> Engine.put_row(:shared_entries, entry.id, entry)

      {:ok, next, entry}
    end
  end

  def read_position(engine, record, person, recipient, local_entry_id) do
    with false <- Map.has_key?(recipient, :frozen),
         %{} = entry <-
           Enum.find(entries(engine, record), &(recipient_entry_id(&1.id, person) == local_entry_id)) do
      prior = Map.get(recipient, :read_position, 0)
      next = Map.put(recipient, :read_position, max(prior, entry.position))
      {:ok, Engine.put_row(engine, :shared_recipients, {record.id, person}, next)}
    else
      true -> {:error, :forbidden}
      _ -> {:error, :not_found}
    end
  end

  def replyable(%{terminal: %{"state" => "retracted"}}), do: {:error, :already_retracted}
  def replyable(%{chat_open: false}), do: {:error, :already_closed}
  def replyable(_record), do: :ok

  defp notify_target(_engine, _id, nil), do: :ok

  defp notify_target(engine, id, person) do
    if person in live_recipients(engine, id),
      do: :ok,
      else: {:error, :invalid_field, "notify_person_id"}
  end

  def entries(engine, record, cutoff \\ nil) do
    high = cutoff || Map.get(record, :entry_high, 0)

    engine.shared_entries
    |> Map.values()
    |> Enum.filter(&(&1.shared_id == record.id and &1.position <= high))
    |> Enum.sort_by(& &1.position)
  end

  def person_entry(recipient, person, entry, rev) do
    entry =
      case get_in(recipient, [:frozen, :delivery]) do
        %{} = delivery ->
          entry |> Map.drop([:served, :handoff]) |> Map.merge(Map.get(delivery, entry.id, %{}))
        nil -> entry
      end

    entry_wire(recipient.item_id, recipient_entry_id(entry.id, person), entry, rev)
    |> add_person_delivery(entry, person)
  end

  def sender_entry(record, entry, rev) do
    entry_wire(record.id, entry.id, entry, rev)
    |> add_sender_delivery(entry)
  end

  defp entry_wire(item_id, entry_id, entry, rev) do
    %{
      "item_id" => item_id, "entry_id" => entry_id, "shared_entry_id" => entry.id,
      "rev" => rev, "position" => entry.position, "kind" => "reply",
      "author" => entry_author(entry.author), "speaker" => entry.speaker,
      "request_revision" => entry.request_revision, "at_ms" => entry.at_ms,
      "text" => entry.text, "files" => SharedAttachments.wire(Map.get(entry, :files, []))
    }
  end

  defp entry_author({:person, _}), do: "person"
  defp entry_author(:sender), do: "origin"

  def recipient_entry_id(id, person) do
    hash = :crypto.hash(:sha256, :erlang.term_to_binary({id, person}))
    "shared-entry_" <> Base.url_encode64(hash, padding: false)
  end

  defp add_person_delivery(wire, %{author: {:person, person}} = entry, person),
    do: add_sender_delivery(wire, entry)

  defp add_person_delivery(wire, _entry, _recipient), do: wire

  defp add_sender_delivery(wire, %{author: {:person, _}} = entry) do
    delivery = %{"delivered" => Map.get(entry, :served, false)}
    delivery = Map.merge(delivery, Map.get(entry, :handoff, %{}))
    Map.put(wire, "delivery", delivery)
  end

  defp add_sender_delivery(wire, _entry), do: wire

  def mark_served(engine, ids) do
    Enum.reduce(ids, engine, fn id, next ->
      case Map.get(next.shared_entries, id) do
        %{author: {:person, _}} = entry ->
          Engine.put_row(next, :shared_entries, id, Map.put(entry, :served, true))
        _ -> next
      end
    end)
  end

  def receipt(engine, %{author: {:person, _}, served: true} = entry, handoff) do
    prior = Map.get(entry, :handoff)

    {status, effective} =
      cond do
        is_nil(prior) -> {"recorded", handoff}
        prior == handoff -> {"already_recorded", prior}
        prior["outcome"] == "handed" -> {"superseded", prior}
        handoff["outcome"] == "handed" -> {"recorded", handoff}
        true -> {"superseded", prior}
      end

    next = Engine.put_row(engine, :shared_entries, entry.id, Map.put(entry, :handoff, effective))
    {:ok, next, %{"status" => status, "stage_outcome" => effective["outcome"]}}
  end

  def receipt(_engine, %{author: {:person, _}}, _handoff), do: {:error, :not_answered}
  def receipt(_engine, _entry, _handoff), do: {:error, :not_found}

  def valid_entry?(%Engine{} = engine, id, %{
        id: id, shared_id: shared_id, position: position, request_revision: revision,
        at_ms: at_ms, text: text, author: author, speaker: speaker
      }) do
    entry = engine.shared_entries[id]

    case Map.get(engine.shared_messages, shared_id) do
      %{entry_high: high, revision: current} ->
        is_binary(id) and position in 1..high and revision in 1..current and
          is_integer(at_ms) and is_binary(text) and is_map(speaker) and valid_author?(engine, author) and
          Enum.all?(SharedAttachments.entry_upload_ids(entry), fn upload_id ->
            match?(%{message_id: ^shared_id, shared_entry_id: ^id}, engine.uploads[upload_id])
          end)
      _ -> false
    end
  end

  def valid_entry?(_, _, _), do: false

  defp valid_author?(_engine, :sender), do: true
  defp valid_author?(engine, {:person, person}), do: Map.has_key?(engine.persons, person)
  defp valid_author?(_engine, _author), do: false

  def freeze_departures(%Engine{} = engine, organization, departing) do
    Enum.reduce(engine.shared_messages, engine, fn
      {id, %{organization_id: ^organization} = record}, next ->
        freeze_message(engine, next, id, record, departing)

      _, next ->
        next
    end)
  end

  def change_group_membership(%Engine{} = engine, group_id, departing, joining) do
    Enum.reduce(engine.shared_messages, engine, fn
      {id, %{group_id: ^group_id} = record}, next ->
        next = freeze_recipients(engine, next, id, record, departing)

        if record.chat_open, do: join_open_group(next, id, joining), else: next

      _, next ->
        next
    end)
  end

  defp join_open_group(engine, id, joining) do
    Enum.reduce(joining, engine, fn person, next ->
      key = {id, person}
      initial = %{item_id: local_item_id(id, person), seen_revision: nil}
      recipient = Map.get(next.shared_recipients, key, initial)
      Engine.put_row(next, :shared_recipients, key, Map.delete(recipient, :frozen))
    end)
  end

  defp freeze_message(snapshot, engine, id, record, departing) do
    engine = freeze_recipients(snapshot, engine, id, record, departing)

    if MapSet.member?(departing, record.sender_person) and not Map.has_key?(record, :sender_frozen_revision),
      do:
        Engine.put_row(engine, :shared_messages, id,
          Map.merge(record, %{sender_frozen_revision: record.revision, sender_frozen_terminal: Map.get(record, :terminal),
                              sender_frozen_entry_high: Map.get(record, :entry_high, 0),
                              sender_frozen_seen: sender_seen?(snapshot, record),
                              sender_frozen_group: group_identity(snapshot, record),
                              sender_frozen_answer_id: Map.get(record, :answer_id),
                              sender_frozen_answers: SharedAnswers.answer_ids(snapshot, record)})),

      else: engine
  end

  defp freeze_recipients(snapshot, engine, id, record, departing) do
    if MapSet.size(departing) == 0 do
      engine
    else
      members = visible_members(snapshot, id)

      Enum.reduce(departing, engine, fn person, next ->
        freeze_recipient(snapshot, next, id, record, person, members)
      end)
    end
  end

  defp freeze_recipient(snapshot, engine, id, record, person, members) do
    key = {id, person}

    case snapshot.shared_recipients[key] do
      %{frozen: _} ->
        engine

      %{} = recipient ->
        cutoff = %{
          revision: record.revision,
          members: members,
          terminal: Map.get(record, :terminal),
          entry_high: Map.get(record, :entry_high, 0),
          delivery: frozen_delivery(snapshot, record, person),
          group: group_identity(snapshot, record)
        }

        cutoff = Map.merge(cutoff, SharedAnswers.freeze_person(snapshot, record, recipient))
        Engine.put_row(engine, :shared_recipients, key, Map.put(recipient, :frozen, cutoff))

      nil ->
        engine
    end
  end

  defp frozen_delivery(engine, record, person) do
    for %{author: {:person, ^person}} = entry <- entries(engine, record),
        into: %{},
        do: {entry.id, Map.take(entry, [:served, :handoff])}
  end

  def retained_history?(engine, person) do
    Enum.any?(engine.shared_recipients, fn
      {{_, ^person}, %{frozen: _}} -> true
      _ -> false
    end) or
      Enum.any?(engine.shared_messages, fn
        {_, %{sender_person: ^person, sender_frozen_revision: _}} -> true
        _ -> false
      end)
  end

  def for_person(engine, person, capability \\ %{}) do
    for {{id, ^person}, recipient} <- engine.shared_recipients,
        record = Map.get(engine.shared_messages, id),
        SharedCapability.visible?(record, capability),
        do: {record, recipient}
  end

  def counts(engine, person, capability \\ %{}) do
    %{"unresolved" => individual} = AttentionServer.ClientAPI.Resources.counts(engine, person)
    affiliations = organizations(engine, person)

    shared =
      Enum.count(for_person(engine, person, capability), fn {message, recipient} ->
        not Map.has_key?(recipient, :frozen) and
          MapSet.member?(affiliations, message.organization_id) and
          SharedAnswers.available?(engine, message, recipient, person)
      end)

    %{"unresolved" => individual + shared}
  end

  def badge_count(engine, person, capability \\ %{}) do
    affiliations = organizations(engine, person)

    Enum.count(for_person(engine, person, capability), fn {record, recipient} ->
      not Map.has_key?(recipient, :frozen) and
        MapSet.member?(affiliations, record.organization_id) and
        (request_attention?(engine, record, recipient, person) or unread_chat?(engine, record, recipient, person))
    end)
  end

  defp request_attention?(engine, record, recipient, person) do
    not AttentionServer.Message.progress_message?(record.message) and
      SharedAnswers.available?(engine, record, recipient, person)
  end

  defp unread_chat?(engine, record, recipient, person) do
    read = Map.get(recipient, :read_position, 0)
    Enum.any?(entries(engine, record), &unread_entry?(&1, person, read))
  end

  defp unread_entry?(entry, person, read), do: entry.position > read and entry.author != {:person, person}

  def recipient_message(engine, person, item_id) do
    Enum.find_value(engine.shared_recipients, {:error, :not_found}, fn
      {{shared_id, ^person}, %{item_id: ^item_id} = recipient} ->
        case Map.fetch(engine.shared_messages, shared_id) do
          {:ok, record} -> {:ok, record, recipient}
          :error -> {:error, :not_found}
        end

      _ ->
        nil
    end)
  end

  def person_item(engine, person, record, recipient, rev) do
    frozen = Map.get(recipient, :frozen)
    record = historical_record(record, frozen && frozen.revision)
    record = if frozen, do: replace_terminal(record, frozen.terminal), else: record

    active? = is_nil(frozen) and MapSet.member?(organizations(engine, person), record.organization_id)
    open? = active? and record.chat_open
    can_answer = active? and SharedAnswers.available?(engine, record, recipient, person)
    answers = SharedAnswers.person_projection(engine, record, recipient, person)

    members = if frozen, do: frozen.members, else: visible_members(engine, record.id)

    %{
      "item_id" => recipient.item_id,
      "rev" => rev,
      "sender" => record.sender_id,
      "request_revision" => record.revision,
      "message" => record.message,
      "files" => SharedAttachments.wire(Map.get(record, :files, [])),
      "seen" => recipient.seen_revision == record.revision,
      "chat" => chat_state(engine, record, recipient, person),
      "rank" => %{"key" => recipient.item_id, "section" => if(not is_nil(frozen) or not is_nil(answers.terminal), do: "resolved", else: "current")},
      "terminal" => answers.terminal,
      "shared" => %{
        "shared_id" => record.id,
        "answer_policy" => answer_policy(record.answer_policy),
        "chat_writable" => open?,
        "can_answer" => can_answer,
        "members" => members,
        "answers" => answers.answers
      }
    }
    |> Map.merge(Map.take(record, ["created_at_ms"]))
    |> maybe_group(if(frozen, do: Map.get(frozen, :group) || group_identity(engine, record), else: group_identity(engine, record)))
  end

  defp chat_state(engine, record, recipient, person) do
    cutoff = get_in(recipient, [:frozen, :entry_high])
    entries = entries(engine, record, cutoff)
    read = Map.get(recipient, :read_position, 0)

    unread = Enum.count(entries, &unread_entry?(&1, person, read))
    last_read = Enum.find(entries, &(&1.position == read))
    last_origin = Enum.find(Enum.reverse(entries), &(&1.author == :sender))

    chat = %{
      "available" => true, "started" => entries != [], "unread_replies" => unread,
      "read_position" => if(last_read, do: recipient_entry_id(last_read.id, person)),
      "last_origin_reply" => if(last_origin, do: recipient_entry_id(last_origin.id, person))
    }

    if last_origin do
      Map.merge(chat, %{
        "reply_group" => nil, "reply_rank_key" => recipient.item_id,
        "reply_priority" => %{"level" => "normal"}
      })
    else
      chat
    end
  end

  defp visible_members(engine, shared_id) do
    engine.shared_recipients
      |> Enum.flat_map(fn
        {{id, member}, state} when id == shared_id ->
          current = engine.shared_messages[id].revision
          seen? = state.seen_revision == current
          [
            %{
              "person" => PeopleProfiles.speaker(engine, member),
              "seen_revision" => if(seen?, do: current),
              "seen_at_ms" => if(seen?, do: Map.get(state, :seen_at_ms))
            }
          ]

        _ ->
          []
      end)
      |> Enum.sort_by(& &1["person"]["id"])
  end

  def historical_record(record, nil), do: record
  def historical_record(%{revision: revision} = record, revision), do: record

  def historical_record(record, revision) do
    historical = record |> Map.fetch!(:history) |> Map.fetch!(revision)
    record |> Map.drop([:terminal, :files]) |> Map.merge(historical)
  end

  def revision(record) do
    %{"request_revision" => record.revision, "message" => record.message,
      "files" => SharedAttachments.wire(Map.get(record, :files, []))}
  end

  def sender_message(engine, record, rev) do
    frozen = Map.get(record, :sender_frozen_revision)
    record = historical_record(record, frozen)
    record = if frozen, do: replace_terminal(record, record.sender_frozen_terminal), else: record
    answers = SharedAnswers.sender_projection(engine, record)

    %{
      "item_id" => record.id,
      "rev" => rev,
      "sender" => record.sender_id,
      "revision" => record.revision,
      "message" => record.message,
      "files" => SharedAttachments.wire(Map.get(record, :files, [])),
      "seen" => if(frozen, do: record.sender_frozen_seen, else: sender_seen?(engine, record)),
      "shared" => %{
        "shared_id" => record.id,
        "organization_id" => record.organization_id,
        "answer_policy" => answer_policy(record.answer_policy),
        "chat_open" => is_nil(frozen) and record.chat_open,
        "answers" => answers.answers
      }
    }
    |> Map.merge(Map.take(record, ["session", "created_at_ms"]))
    |> maybe_terminal(answers.terminal)
    |> maybe_group(if(frozen, do: Map.get(record, :sender_frozen_group), else: group_identity(engine, record)))
  end

  defp maybe_group(wire, nil), do: wire
  defp maybe_group(wire, group), do: put_in(wire, ["shared", "group"], group)

  defp group_identity(engine, %{group_id: id} = record) do
    name =
      case engine.groups[id] do
        %{name: name} -> name
        nil -> Map.get(record, :group_name)
      end

    %{"id" => id, "name" => name}
  end

  defp group_identity(_engine, _record), do: nil

  defp sender_seen?(engine, record) do
    Enum.any?(live_recipients(engine, record.id), fn person ->
      engine.shared_recipients[{record.id, person}].seen_revision == record.revision
    end)
  end

  defp replace_terminal(record, nil), do: Map.delete(record, :terminal)
  defp replace_terminal(record, terminal), do: Map.put(record, :terminal, terminal)

  defp maybe_terminal(wire, nil), do: wire
  defp maybe_terminal(wire, terminal), do: Map.put(wire, "terminal", terminal)

  defp answer_policy(nil), do: nil
  defp answer_policy({:named, people}), do: %{"kind" => "named", "people" => people}
  defp answer_policy(policy), do: %{"kind" => Atom.to_string(policy)}

  def valid_message?(%Engine{} = engine, id, %{
        id: id,
        organization_id: organization,
        sender_person: person,
        sender_key: key,
        sender_id: sender,
        message: message,
        revision: revision,
        chat_open: chat_open,
        answer_policy: policy
      }) do
    is_binary(id) and is_binary(organization) and Map.has_key?(engine.persons, person) and
      is_binary(key) and AttentionServer.Message.sender_id?(sender) and
      is_map(message) and
      Enum.all?(SharedAttachments.upload_ids(engine.shared_messages[id]), &Map.has_key?(engine.uploads, &1)) and
      is_integer(revision) and revision > 0 and is_boolean(chat_open) and
      valid_policy?(policy)
  end

  def valid_message?(_, _, _), do: false

  def valid_recipient?(%Engine{} = engine, {shared_id, person}, %{item_id: item, seen_revision: seen}) do
    if Map.has_key?(engine.persons, person) and item == local_item_id(shared_id, person) do
      case Map.get(engine.shared_messages, shared_id) do
        %{revision: revision} -> is_nil(seen) or (is_integer(seen) and seen in 1..revision)
        _ -> false
      end
    else
      false
    end
  end

  def valid_recipient?(_, _, _), do: false

  defp valid_policy?(policy) when policy in [nil, :first, :everyone], do: true
  defp valid_policy?({:named, people}) when is_list(people) and people != [],
    do: Enum.all?(people, &is_binary/1)

  defp valid_policy?(_), do: false
end
