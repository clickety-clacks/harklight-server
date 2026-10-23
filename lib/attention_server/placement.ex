defmodule AttentionServer.Placement.Algorithm do
  @moduledoc false

  @callback place([map()], [map()], term()) :: [map()]
  @callback reassess([map()], [map()]) :: :none | :whole_set
  @callback compose([map()], map()) :: {:ok, map()} | {:error, atom()}
end

defmodule AttentionServer.Placement do
  @moduledoc false

  alias AttentionServer.Engine
  alias AttentionServer.Placement.{Baseline, JevSet}

  @algorithms %{"baseline" => Baseline, "jev-set" => JevSet}

  def algorithms, do: Map.keys(@algorithms)

  def algorithm(id), do: Map.get(@algorithms, id)

  def place(id, items, previous, result) do
    case algorithm(id) do
      nil -> Baseline.place(items, previous, result)
      implementation -> implementation.place(items, previous, result)
    end
  end

  def reassess(%Engine{} = engine, person, previous) do
    case algorithm(choice(engine, person)) do
      nil -> :none
      implementation -> implementation.reassess(items(engine, person), previous || [])
    end
  end

  def compose(id, items, connection) do
    case algorithm(id) do
      nil -> {:error, :not_supported}
      implementation -> implementation.compose(items, connection)
    end
  end

  def choice(%Engine{} = engine, person),
    do: Map.get(engine.algorithm_choices, person, "baseline")

  def items(%Engine{} = engine, person) do
    engine.messages
    |> Enum.flat_map(fn
      {{^person, sender, id}, %{terminal: nil} = record} ->
        [
          %{
            identity: {sender, id},
            message: record.message,
            accepted_seq: record.accepted_seq,
            revision: record.revision
          }
        ]

      _ ->
        []
    end)
  end

  def baseline(%Engine{} = engine, person), do: Baseline.place(items(engine, person), [], nil)

  def identity(%{"sender_id" => sender, "message_id" => id}), do: {sender, id}
  def identity(%{identity: identity}), do: identity

  def valid_permutation?(items, identities) when is_list(items) and is_list(identities) do
    expected = Enum.map(items, & &1.identity)

    length(expected) == length(identities) and
      length(Enum.uniq(identities)) == length(identities) and
      MapSet.new(expected) == MapSet.new(identities)
  end

  def valid_permutation?(_, _), do: false
end

defmodule AttentionServer.Placement.Baseline do
  @moduledoc false
  @behaviour AttentionServer.Placement.Algorithm

  @impl true
  def place(items, _previous, _result) do
    items
    |> ordered_items()
    |> Enum.map(fn item ->
      {sender, id} = item.identity
      entry = %{"sender_id" => sender, "message_id" => id}

      case Map.get(item.message, "by_when") do
        deadline when is_integer(deadline) ->
          Map.put(entry, "due_at", deadline)

        _ ->
          Map.put(entry, "reason", "No deadline; newest first")
      end
    end)
  end

  def ordered_items(items) when is_list(items) do
    Enum.sort_by(items, fn item ->
      deadline = Map.get(item.message, "by_when")

      {not is_integer(deadline), if(is_integer(deadline), do: deadline, else: 0),
       -item.accepted_seq, elem(item.identity, 0), elem(item.identity, 1)}
    end)
  end

  @impl true
  def reassess(_items, _previous), do: :none

  @impl true
  def compose(_items, _connection), do: {:error, :not_supported}
end

defmodule AttentionServer.Placement.JevSet do
  @moduledoc false
  @behaviour AttentionServer.Placement.Algorithm

  alias AttentionServer.Placement
  alias AttentionServer.Placement.Baseline
  alias AttentionServer.JevClient

  @impl true
  def place(items, _previous, {:ranked, identities}) do
    items_by_identity = Map.new(items, &{&1.identity, &1})

    identities
    |> Enum.map(fn identity ->
      _item = Map.fetch!(items_by_identity, identity)
      {sender, id} = identity

      %{
        "sender_id" => sender,
        "message_id" => id,
        "reason" => "Ranked by how soon it needs handling"
      }
    end)
  end

  def place(items, previous, _result) do
    items_by_identity = Map.new(items, &{&1.identity, &1})
    baseline = Placement.place("baseline", items, [], nil)
    previous_by_identity = Map.new(previous, &{Placement.identity(&1), &1})

    retained =
      Enum.flat_map(previous, fn entry ->
        identity = Placement.identity(entry)

        if Map.has_key?(items_by_identity, identity),
          do: [Map.fetch!(previous_by_identity, identity)],
          else: []
      end)

    new_entries =
      Enum.filter(baseline, &(not Map.has_key?(previous_by_identity, Placement.identity(&1))))

    retained ++ new_entries
  end

  @impl true
  def reassess(_items, _previous), do: :whole_set

  @impl true
  def compose(items, connection),
    do:
      JevClient.compose(
        Baseline.ordered_items(items),
        connection.endpoint,
        connection.model
      )
end
