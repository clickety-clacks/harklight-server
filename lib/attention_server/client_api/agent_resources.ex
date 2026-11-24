defmodule AttentionServer.ClientAPI.AgentResources do
  @moduledoc "Shared agent snapshot and change-feed projections."

  alias AttentionServer.{AgentChangeLog, ChangeLog, Conversation, Engine, RouteBIndex}
  alias AttentionServer.ClientAPI.{Projection, Resources}

  def own_item_key(state, {person, _agent} = scope, item_id) do
    with {:ok, key} <- Resources.item_key(state.store.engine, person, item_id),
         {:ok, _record} <- own_record(state.store.engine, scope, key) do
      {:ok, key}
    end
  end

  def message(state, {person, agent} = scope, {person, sender, id} = key) do
    engine = state.store.engine

    with {:ok, record} <- own_record(engine, scope, key),
         {:ok, rev} <- known_rev(state.store, scope, {:message, key}),
         request_revision = Map.get(record, :revision, 1),
         {:ok, current} <- revision(state, key, request_revision) do
      Projection.run({:item, Resources.item_id(record)}, fn ->
        {:ok,
         %{
           "item_id" => Resources.item_id(record),
           "rev" => rev,
           "sender" => sender,
           "revision" => request_revision,
           "message" => current["message"],
           "files" => current["files"],
           "seen" => Resources.seen?(engine, key, record)
         }
         |> put("session", Map.get(record, :session_handle))
         |> put("created_at_ms", Map.get(record, :accepted_at))
         |> put("legacy_message_id", if(!Map.get(record, :agent_api_v2, false), do: id))
         |> put("terminal", terminal(engine, scope, record))
         |> put("availability", availability(engine, person, agent, record))
         |> put("advisories", advisories(engine, scope, key))
         |> put("result", Resources.origin_result(record))}
      end)
    end
  end

  def with_retracting_actor(message, state, {person, _, _} = key) do
    terminal = state.store.engine.messages[key].terminal

    case Conversation.retracted_by(person, terminal) do
      nil -> message
      actor -> put_in(message, ["terminal", "retracted_by"], actor)
    end
  end

  def revision(state, key, n) do
    with {:ok, revision} <- Resources.revision(state.store.engine, key, n) do
      {:ok, Map.update!(revision, "message", &Map.drop(&1, ["sender_id", "message_id"]))}
    end
  end

  def entries(state, scope, key) do
    with {:ok, raw} <- raw_entries(state, scope, key) do
      render_entries(state, scope, raw)
    end
  end

  def raw_entries(state, scope, key) do
    engine = state.store.engine

    with {:ok, record} <- own_record(engine, scope, key) do
      entries =
        engine.route_b_index
        |> RouteBIndex.entry_page(key)
        |> :gb_trees.values()
        |> Enum.reverse()
        |> Enum.map(&Resources.agent_entry(engine, key, record, &1))

      {:ok, entries}
    end
  end

  def raw_entries(state, scope, key, positions) do
    engine = state.store.engine

    with {:ok, record} <- own_record(engine, scope, key) do
      entries =
        Enum.flat_map(positions, fn position ->
          case Resources.agent_entry(engine, key, record, position) do
            nil -> []
            entry -> [entry]
          end
        end)

      {:ok, entries}
    end
  end

  def entry_page(state, scope, key, before, limit) do
    engine = state.store.engine
    tree = RouteBIndex.entry_page(engine.route_b_index, key)

    with {:ok, record} <- own_record(engine, scope, key),
         {:ok, iterator} <- entry_iterator(tree, before) do
      positions =
        iterator
        |> Stream.unfold(fn cursor ->
          case :gb_trees.next(cursor) do
            :none -> nil
            {_, position, rest} -> {position, rest}
          end
        end)
        |> Enum.take(limit + 1)

      {page, rest} = Enum.split(positions, limit)
      raw = Enum.map(Enum.reverse(page), &Resources.agent_entry(engine, key, record, &1))

      with {:ok, rendered} <- render_entries(state, scope, raw) do
        {:ok, rendered, rest != []}
      end
    end
  end

  defp entry_iterator(tree, nil), do: {:ok, :gb_trees.iterator(tree)}

  defp entry_iterator(tree, before) do
    with {:ok, position} <- Resources.entry_seq(before),
         true <- :gb_trees.is_defined(-position, tree) do
      {:ok, :gb_trees.iterator_from(1 - position, tree)}
    else
      _ -> {:error, :invalid_field, "before"}
    end
  end

  def render_entries(state, scope, raw_entries) do
    raw_entries
    |> Enum.reduce_while({:ok, []}, fn raw, {:ok, acc} ->
      case render_entry(state, scope, raw) do
        {:ok, entry} -> {:cont, {:ok, [entry | acc]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      error -> error
    end
  end

  def entry(state, scope, key, position) do
    engine = state.store.engine

    with {:ok, record} <- own_record(engine, scope, key),
         %{} = raw <- Resources.agent_entry(engine, key, record, position) do
      render_entry(state, scope, raw)
    else
      _ -> {:error, :not_found}
    end
  end

  defp render_entry(state, {person, _agent} = scope, %{"position" => position} = raw) do
    with {:ok, rev} <- known_rev(state.store, scope, {:entry, position}) do
      at =
        case ChangeLog.created_at(state.store, person, ChangeLog.entry_resource(position)) do
          {:ok, at_ms} -> at_ms
          _ -> nil
        end

      Projection.run({:entry, Resources.entry_id(position)}, fn ->
        {:ok,
         raw
         |> enrich_delivery(state.store.engine, scope)
         |> Resources.put_at(at)
         |> Resources.stamp(rev)
         |> then(&Resources.render(:entry, &1, nil))}
      end)
    end
  end

  defp enrich_delivery(
         %{"author" => "person", "kind" => "reply"} = raw,
         engine,
         {person, _} = scope
       ) do
    event = engine.conversation_events[{person, raw["position"]}]
    delivery = delivery(engine, scope, event.payload["event_id"], raw["files"] || [])
    Map.update!(raw, "delivery", &Map.merge(&1, delivery))
  end

  defp enrich_delivery(raw, _engine, _scope), do: raw

  defp terminal(_engine, _scope, %{terminal: nil}), do: nil

  defp terminal(engine, {person, _agent} = scope, %{terminal: terminal} = record) do
    base =
      %{"state" => terminal.state, "at_ms" => terminal.at}
      |> put("outcome", Resources.answer_outcome(record, terminal))

    base =
      if terminal.answer_present, do: Map.put(base, "answer", terminal.answer), else: base

    if terminal.state == "answered" and Conversation.terminal_author(record, terminal) == "person" do
      event_id = get_in(engine.events, [{person, terminal.seq}, :payload, "event_id"])

      case delivery(engine, scope, event_id, []) do
        %{} = details when map_size(details) > 0 -> Map.put(base, "delivery", details)
        _ -> base
      end
    else
      base
    end
  end

  defp delivery(engine, {person, agent}, event_id, files) when is_binary(event_id) do
    with {seq, ""} <- Integer.parse(event_id),
         %{} = entry <- get_in(engine.origin_feeds, [{person, agent}, :entries, seq]) do
      ack1 = Engine.delivery_ack_fact(engine, person, agent, entry, :ack1)
      ack2 = Engine.delivery_ack_fact(engine, person, agent, entry, :ack2) || %{}
      receipt = Map.get(engine.origin_receipts, {person, agent, seq}, %{})
      uses = Map.get(receipt, :uses, %{})
      transfers = Map.get(receipt, :transfers, %{})

      reports =
        for file <- files,
            use = Map.get(uses, file["name"]),
            transfer = Map.get(transfers, file["name"]),
            use || transfer do
          %{"file_id" => file["file_id"]}
          |> put("transfer", transfer && transfer["status"])
          |> put("transfer_reason", transfer && transfer["reason"])
          |> put("use", use && use["status"])
          |> put("use_reason", use && use["reason"])
        end

      Map.take(ack2, ["outcome", "reason", "rerouted"])
      |> put("agent", get_in(ack2, ["acknowledger", "agent"]))
      |> put("delivered", not is_nil(ack1) or map_size(receipt) > 0)
      |> put("acted", Map.has_key?(engine.origin_actions, {person, agent, seq}))
      |> put("file_uses", if(reports != [], do: reports))
    else
      _ -> %{}
    end
  end

  defp delivery(_engine, _scope, _event_id, _files), do: %{}

  defp availability(engine, person, agent, %{session_handle: handle}) when is_binary(handle) do
    case Map.get(engine.origin_sessions, {person, agent, handle}) do
      %{gone: true} -> %{"state" => "gone"}
      _ -> nil
    end
  end

  defp availability(_engine, _person, _agent, _record), do: nil

  defp advisories(engine, scope, key) do
    case AttentionServer.AgentAPI.Advisories.for_message(engine, scope, key) do
      [] -> nil
      retained -> retained
    end
  end

  defp own_record(engine, {person, agent}, {person, _, _} = key) do
    case Map.get(engine.messages, key) do
      %{origin: ^agent} = record -> {:ok, record}
      _ -> {:error, :not_found}
    end
  end

  defp own_record(_engine, _scope, _key), do: {:error, :not_found}

  defp known_rev(store, scope, resource) do
    case AgentChangeLog.rev(store, scope, resource) do
      {:ok, rev} -> {:ok, rev}
      {:error, :disabled} -> {:error, :not_found}
      _ -> {:error, :storage_unavailable}
    end
  end

  defp put(map, _key, nil), do: map
  defp put(map, key, value), do: Map.put(map, key, value)
end
