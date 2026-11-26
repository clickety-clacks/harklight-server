defmodule AttentionServer.Groups do
  @moduledoc false

  alias AttentionServer.{Engine, Message, SharedMessages}

  def create(engine, id, person, name) do
    organization = engine.relay_instance_id

    with :ok <- affiliated(engine, organization, person),
         {:ok, name} <- normalize_name(name),
         :ok <- available_name(engine, organization, name, id) do
      group = %{
        id: id,
        organization_id: organization,
        name: name,
        manager: person,
        hosted_ref: nil,
        members: %{person => false},
        invitations: %{}
      }

      {:ok, put(engine, group), group}
    end
  end

  def rename(engine, id, person, name) do
    with {:ok, group} <- managed(engine, id, person),
         {:ok, name} <- normalize_name(name),
         :ok <- available_name(engine, group.organization_id, name, id) do
      group = %{group | name: name}
      {:ok, put(engine, group), group}
    end
  end

  def invite(engine, id, manager, person, invitation_id) do
    with {:ok, group} <- managed(engine, id, manager),
         :ok <- affiliated(engine, group.organization_id, person),
         false <- Map.has_key?(group.members, person) do
      invitation = Map.get(group.invitations, person, invitation_id)
      group = %{group | invitations: Map.put(group.invitations, person, invitation)}
      {:ok, put(engine, group), group, invitation}
    else
      true -> {:error, :already_member}
      error -> error
    end
  end

  def respond(engine, invitation, person, accept?) do
    with {:ok, group} <- find_invitation(engine, invitation, person),
         :ok <- affiliated(engine, group.organization_id, person) do
      members = if accept?, do: Map.put(group.members, person, false), else: group.members
      group_without_invitation = %{group | invitations: Map.delete(group.invitations, person)}
      next = replace_members(engine, group_without_invitation, members)
      {:ok, next, next.groups[group.id]}
    end
  end

  def leave(engine, id, person) do
    with {:ok, %{hosted_ref: nil} = group} <- member_group(engine, id, person) do
      {:ok, replace_members(engine, group, Map.delete(group.members, person))}
    else
      {:ok, _hosted} -> {:error, :not_found}
      error -> error
    end
  end

  def mute(engine, id, person, muted?) when is_boolean(muted?) do
    with {:ok, group} <- member_group(engine, id, person) do
      group = %{group | members: Map.put(group.members, person, muted?)}
      {:ok, put(engine, group), group}
    end
  end

  def replace_hosted(engine, ref, id, raw_name, people) do
    with %{id: organization, members: eligible} <- engine.hosted_organizations[ref],
         {:ok, name} <- normalize_name(raw_name),
         true <- Enum.all?(people, &MapSet.member?(eligible, &1)) do
      group =
        hosted_group(engine, ref) ||
          %{id: id, organization_id: organization, name: name, manager: nil,
            members: %{}, invitations: %{}, hosted_ref: ref}

      members = Map.new(people, &{&1, Map.get(group.members, &1, false)})
      next = replace_members(engine, %{group | name: name}, members)
      {:ok, next, next.groups[group.id]}
    else
      nil -> {:error, :not_found}
      false -> {:error, :invalid_members}
      error -> error
    end
  end

  def delete_hosted(engine, ref) do
    case hosted_group(engine, ref) do
      nil -> engine
      group ->
        engine
        |> replace_members(group, %{})
        |> Engine.delete_row(:groups, group.id)
    end
  end

  def remove_affiliations(engine, organization, departing) do
    people = MapSet.to_list(departing)

    Enum.reduce(engine.groups, engine, fn
      {_, %{organization_id: ^organization} = group}, next ->
        updated = %{group | invitations: Map.drop(group.invitations, people)}
        put(next, %{updated | members: Map.drop(group.members, people)})

      _, next ->
        next
    end)
  end

  def member_group(engine, id, person) do
    with %{members: members, organization_id: organization} = group <- engine.groups[id],
         true <- Map.has_key?(members, person),
         :ok <- affiliated(engine, organization, person) do
      {:ok, group}
    else
      _ -> {:error, :not_found}
    end
  end

  def valid?(engine, id, %{
        id: id, organization_id: organization, name: name, manager: manager,
        members: members, invitations: invitations, hosted_ref: hosted_ref
      }) when is_map(members) and is_map(invitations) do
    Message.uuid_v4?(id) and normalize_name(name) == {:ok, name} and
      valid_custody?(engine, organization, manager, hosted_ref, invitations) and
      Enum.all?(members, fn {person, muted?} ->
        is_boolean(muted?) and affiliated(engine, organization, person) == :ok
      end) and
      Enum.all?(invitations, fn {person, invitation} ->
        Message.uuid_v4?(invitation) and not Map.has_key?(members, person) and
          affiliated(engine, organization, person) == :ok
      end)
  end

  def valid?(_, _, _), do: false

  defp valid_custody?(engine, organization, manager, nil, _invitations),
    do: organization == engine.relay_instance_id and Map.has_key?(engine.persons, manager)

  defp valid_custody?(engine, organization, nil, ref, invitations),
    do: get_in(engine.hosted_organizations, [ref, :id]) == organization and map_size(invitations) == 0

  defp valid_custody?(_, _, _, _), do: false

  defp replace_members(engine, group, members) do
    old = MapSet.new(Map.keys(group.members))
    new = MapSet.new(Map.keys(members))

    engine
    |> SharedMessages.change_group_membership(group.id, MapSet.difference(old, new), MapSet.difference(new, old))
    |> put(%{group | members: members})
  end

  defp put(engine, group), do: Engine.put_row(engine, :groups, group.id, group)

  defp managed(engine, id, person) do
    with %{manager: ^person, organization_id: organization} = group <- engine.groups[id],
         :ok <- affiliated(engine, organization, person) do
      {:ok, group}
    else
      _ -> {:error, :not_found}
    end
  end

  defp find_invitation(engine, invitation, person) do
    case Enum.find(Map.values(engine.groups), &(Map.fetch(&1.invitations, person) == {:ok, invitation})) do
      nil -> {:error, :not_found}
      group -> {:ok, group}
    end
  end

  defp hosted_group(engine, ref),
    do: Enum.find(Map.values(engine.groups), &(&1.hosted_ref == ref))

  defp affiliated(engine, organization, person) do
    if MapSet.member?(SharedMessages.organizations(engine, person), organization),
      do: :ok,
      else: {:error, :not_found}
  end

  defp normalize_name(name) when is_binary(name) do
    case String.trim(name) do
      "" -> {:error, :invalid_name}
      name -> {:ok, name}
    end
  end

  defp normalize_name(_name), do: {:error, :invalid_name}

  defp available_name(engine, organization, name, id) do
    key = name_key(name)

    if Enum.any?(engine.groups, fn {other_id, group} ->
         id != other_id and group.organization_id == organization and name_key(group.name) == key
       end),
      do: {:error, :group_name_conflict},
      else: :ok
  end

  defp name_key(name), do: name |> String.normalize(:nfc) |> String.downcase()
end
