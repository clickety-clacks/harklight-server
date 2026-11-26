defmodule AttentionServer.SharedMessages do
  @moduledoc false

  alias AttentionServer.Engine

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

  def sender_message(record, rev) do
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
        "chat_open" => record.chat_open,
        "answers" => []
      }
    }
    |> Map.merge(Map.take(record, ["session", "created_at_ms"]))
  end

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
