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

  def replace_hosted(engine, {business, group_ref} = ref, id, raw_name, people, version)
      when is_list(people) and is_integer(version) and version >= 0 do
    with true <- valid_ref?(business) and valid_ref?(group_ref) || {:error, :invalid_group},
         :ok <- not_deleted(engine, ref),
         {:ok, name} <- normalize_name(raw_name),
         proposed = %{name: name, roster: MapSet.new(people)},
         {:ok, prior} <- compare_hosted_version(engine, ref, proposed, version),
         %{id: organization, members: eligible} <- engine.hosted_organizations[business],
         true <- MapSet.subset?(proposed.roster, eligible) || {:error, :not_affiliated} do
      group =
        case prior do
          %{group_id: existing} -> Map.fetch!(engine.groups, existing)
          nil ->
            %{id: id, organization_id: organization, name: name, manager: nil,
              members: %{}, invitations: %{}, hosted_ref: ref}
        end

      members = Map.new(people, &{&1, Map.get(group.members, &1, false)})
      rev = if prior, do: prior.rev + 1, else: 1
      row = Map.merge(proposed, %{group_id: group.id, version: version, rev: rev})

      next =
        engine
        |> replace_members(%{group | name: name}, members)
        |> Engine.put_row(:hosted_group_refs, ref, row)

      {:ok, next, receipt(row)}
    else
      {:replayed, row} -> {:ok, engine, receipt(row)}
      nil -> {:error, :not_found}
      error -> error
    end
  end

  def replace_hosted(_engine, _ref, _id, _name, _people, _version), do: {:error, :invalid_group}

  def delete_hosted(engine, {business, group_ref} = ref) do
    cond do
      not valid_ref?(business) or not valid_ref?(group_ref) -> {:error, :invalid_group}
      engine.hosted_group_refs[ref] == :deleted -> {:ok, engine}
      not Map.has_key?(engine.hosted_organizations, business) -> {:error, :not_found}
      true -> {:ok, tombstone(engine, ref)}
    end
  end

  def delete_hosted_business(engine, business) do
    Enum.reduce(engine.hosted_group_refs, engine, fn
      {{^business, _} = ref, %{}}, next -> tombstone(next, ref)
      _, next -> next
    end)
  end

  defp tombstone(engine, ref) do
    engine =
      case engine.hosted_group_refs[ref] do
        %{group_id: id} ->
          engine
          |> replace_members(Map.fetch!(engine.groups, id), %{})
          |> Engine.delete_row(:groups, id)

        nil ->
          engine
      end

    Engine.put_row(engine, :hosted_group_refs, ref, :deleted)
  end

  defp not_deleted(engine, ref) do
    if engine.hosted_group_refs[ref] == :deleted, do: {:error, :group_deleted}, else: :ok
  end

  defp compare_hosted_version(engine, ref, content, version) do
    case engine.hosted_group_refs[ref] do
      %{version: applied} when applied > version -> {:error, :stale_version}
      %{version: ^version} = prior ->
        if Map.take(prior, [:name, :roster]) == content,
          do: {:replayed, prior},
          else: {:error, :version_conflict}

      prior -> {:ok, prior}
    end
  end

  defp receipt(row), do: %{group_id: row.group_id, rev: row.rev}

  def valid_hosted_ref?(_engine, {business, ref}, :deleted),
    do: valid_ref?(business) and valid_ref?(ref)

  def valid_hosted_ref?(engine, {business, ref} = address, %{
        group_id: id, version: version, name: name, roster: %MapSet{} = roster, rev: rev
      }) do
    group = engine.groups[id]
    valid_ref?(business) and valid_ref?(ref) and
      match?(%{hosted_ref: ^address, name: ^name}, group) and
      is_integer(version) and version >= 0 and is_integer(rev) and rev > 0 and
      Enum.all?(roster, &Map.has_key?(engine.persons, &1))
  end

  def valid_hosted_ref?(_, _, _), do: false

  defp valid_ref?(ref) when is_binary(ref) and byte_size(ref) > 0,
    do: String.valid?(ref) and not String.contains?(ref, "/") and not Regex.match?(~r/[\s\p{C}]/u, ref)

  defp valid_ref?(_), do: false

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

  def visible(engine, person) do
    organizations = SharedMessages.organizations(engine, person)

    engine.groups
    |> Map.values()
    |> Enum.filter(fn group ->
      MapSet.member?(organizations, group.organization_id) and
        (group.manager == person or Map.has_key?(group.members, person))
    end)
    |> Enum.sort_by(&{name_key(&1.name), &1.id})
  end

  def invitations(engine, person) do
    organizations = SharedMessages.organizations(engine, person)

    engine.groups
    |> Map.values()
    |> Enum.filter(fn group ->
      MapSet.member?(organizations, group.organization_id) and Map.has_key?(group.invitations, person)
    end)
    |> Enum.sort_by(&{name_key(&1.name), &1.id})
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

  defp valid_custody?(engine, organization, nil, {business, _} = ref, invitations),
    do: get_in(engine.hosted_organizations, [business, :id]) == organization and
      match?(%{group_id: _}, engine.hosted_group_refs[ref]) and map_size(invitations) == 0

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

  defp affiliated(engine, organization, person) do
    if MapSet.member?(SharedMessages.organizations(engine, person), organization),
      do: :ok,
      else: {:error, :not_found}
  end

  defp normalize_name(name) when is_binary(name) do
    case String.trim(name) do
      "" -> {:error, :invalid_name}
      name -> {:ok, String.normalize(name, :nfc)}
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
