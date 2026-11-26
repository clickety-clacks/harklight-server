defmodule AttentionServer.SharedMessages do
  @moduledoc false

  alias AttentionServer.Engine
  alias AttentionServer.ClientAPI.PeopleProfiles

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

  def revise(%Engine{} = engine, record, revision, message) do
    with :ok <- writable_sender(record),
         true <- record.revision == revision || {:error, :precondition_failed} do
      history = Map.get(record, :history, %{})
      previous = Map.take(record, [:revision, :message])
      updated =
        record
        |> Map.put(:history, Map.put(history, revision, previous))
        |> Map.put(:revision, revision + 1)
        |> Map.put(:message, message)

      {:ok, Engine.put_row(engine, :shared_messages, record.id, updated)}
    end
  end

  def close_chat(%Engine{} = engine, record) do
    with :ok <- writable_sender(record),
         true <- record.chat_open || {:error, :already_closed} do
      {:ok, Engine.put_row(engine, :shared_messages, record.id, %{record | chat_open: false})}
    end
  end

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

  def freeze_departures(%Engine{} = engine, organization, departing) do
    Enum.reduce(engine.shared_messages, engine, fn
      {id, %{organization_id: ^organization} = record}, next ->
        freeze_message(engine, next, id, record, departing)

      _, next ->
        next
    end)
  end

  defp freeze_message(snapshot, engine, id, record, departing) do
    {engine, _members} =
      Enum.reduce(departing, {engine, nil}, fn person, {next, roster} ->
        key = {id, person}

        case Map.get(snapshot.shared_recipients, key) do
          %{frozen: _} -> {next, roster}
          %{} = recipient ->
            roster = roster || visible_members(snapshot, id)
            cutoff = %{revision: record.revision, members: roster, terminal: Map.get(record, :terminal)}
            {Engine.put_row(next, :shared_recipients, key, Map.put(recipient, :frozen, cutoff)), roster}

          nil -> {next, roster}
        end
      end)

    if MapSet.member?(departing, record.sender_person) and not Map.has_key?(record, :sender_frozen_revision),
      do:
        Engine.put_row(engine, :shared_messages, id,
          Map.merge(record, %{sender_frozen_revision: record.revision, sender_frozen_terminal: Map.get(record, :terminal)})),

      else: engine
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

  def for_person(engine, person) do
    for {{id, ^person}, recipient} <- engine.shared_recipients,
        record = Map.get(engine.shared_messages, id),
        not is_nil(record),
        do: {record, recipient}
  end

  def counts(engine, person) do
    %{"unresolved" => individual} = AttentionServer.ClientAPI.Resources.counts(engine, person)
    affiliations = organizations(engine, person)

    shared =
      Enum.count(for_person(engine, person), fn {message, recipient} ->
        not Map.has_key?(recipient, :frozen) and not Map.has_key?(message, :terminal) and
          not is_nil(message.answer_policy) and
          MapSet.member?(affiliations, message.organization_id)
      end)

    %{"unresolved" => individual + shared}
  end

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

    open? =
      is_nil(frozen) and record.chat_open and
        MapSet.member?(organizations(engine, person), record.organization_id)

    can_answer =
      open? and
        case record.answer_policy do
          nil -> false
          {:named, people} -> person in people
          _ -> true
        end

    members = if frozen, do: frozen.members, else: visible_members(engine, record.id)

    %{
      "item_id" => recipient.item_id,
      "rev" => rev,
      "sender" => record.sender_id,
      "request_revision" => record.revision,
      "message" => record.message,
      "files" => [],
      "seen" => recipient.seen_revision == record.revision,
      "chat" => %{"available" => true, "started" => false, "unread_replies" => 0, "read_position" => nil},
      "rank" => %{"key" => recipient.item_id, "section" => if(not is_nil(frozen) or Map.has_key?(record, :terminal), do: "resolved", else: "current")},
      "terminal" => Map.get(record, :terminal),
      "shared" => %{
        "shared_id" => record.id,
        "answer_policy" => answer_policy(record.answer_policy),
        "chat_writable" => open?,
        "can_answer" => can_answer,
        "members" => members,
        "answers" => []
      }
    }
    |> Map.merge(Map.take(record, ["created_at_ms"]))
  end

  defp visible_members(engine, shared_id) do
    engine.shared_recipients
      |> Enum.flat_map(fn
        {{id, member}, state} when id == shared_id ->
          [
            %{
              "person" => PeopleProfiles.speaker(engine, member),
              "seen_revision" => state.seen_revision,
              "seen_at_ms" => Map.get(state, :seen_at_ms)
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
    record |> Map.drop([:terminal]) |> Map.merge(historical)
  end

  def revision(record) do
    %{"request_revision" => record.revision, "message" => record.message, "files" => []}
  end

  def sender_message(record, rev) do
    frozen = Map.get(record, :sender_frozen_revision)
    record = historical_record(record, frozen)
    record = if frozen, do: replace_terminal(record, record.sender_frozen_terminal), else: record

    %{
      "item_id" => record.id,
      "rev" => rev,
      "sender" => record.sender_id,
      "revision" => record.revision,
      "message" => record.message,
      "files" => [],
      "seen" => false,
      "shared" => %{
        "shared_id" => record.id,
        "organization_id" => record.organization_id,
        "answer_policy" => answer_policy(record.answer_policy),
        "chat_open" => is_nil(frozen) and record.chat_open,
        "answers" => []
      }
    }
    |> Map.merge(Map.take(record, ["session", "created_at_ms"]))
    |> maybe_terminal(record)
  end

  defp replace_terminal(record, nil), do: Map.delete(record, :terminal)
  defp replace_terminal(record, terminal), do: Map.put(record, :terminal, terminal)

  defp maybe_terminal(wire, %{terminal: terminal}), do: Map.put(wire, "terminal", terminal)
  defp maybe_terminal(wire, _record), do: wire

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
      is_map(message) and is_integer(revision) and revision > 0 and is_boolean(chat_open) and
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
