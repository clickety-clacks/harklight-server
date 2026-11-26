defmodule AttentionServer.ClientAPI.FeedRenderer do
  @moduledoc """
  Renders change-log rows as the contract's change data (docs/client-api-v2.md
  §6): an `item` row as the Item, `item_removed` as `{item_id}`, an `entry` row
  as the Entry its fact belongs to, `counts` and `settings` as theirs. The feed
  calls it in the relay's serialized step, from current state, as the
  compacted log requires. A row whose resource has no client form, such as a
  read-position fact, or a principal that no longer authenticates, is skipped.
  """

  alias AttentionServer.{AgentAccess, ChangeLog, SharedMessages}
  alias AttentionServer.ClientAPI.{Projection, Reads, Resources}

  @doc """
  Renders one row: `{:ok, data}`, `:skip` for a row with no client form, or
  `{:error, :storage_unavailable}` when the store failed, which the feed must
  treat as a failed step, never as a skipped row.
  """
  def render(context, who, row) do
    [{^row, result}] = render_many(context, who, [row])
    result
  end

  @doc """
  Renders a step's rows together: `[{row, {:ok, data} | :skip | {:error,
  :storage_unavailable}}]`, in order.
  The principal is resolved once, and each item's entries are built once for
  all of its rows, so a long chat's backlog costs one pass, not one per row.
  """
  def render_many(%{engine: engine} = context, who, rows) when is_list(rows) do
    Projection.probe(:feed)

    case Reads.client_principal(engine, who) do
      {:ok, principal} ->
        decoded = Enum.map(rows, &{&1, ChangeLog.decode_resource(&1.resource_id)})
        seqs = for {_row, {:entry, seq}} <- decoded, do: seq
        entries = Resources.entries_by_fact(engine, principal.person, seqs, context.view)
        context = Map.put(context, :entries_by_fact, entries)

        Enum.map(decoded, fn {row, resource} ->
          {row, render_row(row.kind, resource, context, principal, row.rev)}
        end)

      _ ->
        Enum.map(rows, &{&1, :skip})
    end
  end

  defp render_row(:item, {:shared_item, id}, %{shared?: true, engine: engine}, principal, rev) do
    with %{id: ^id} = record <- Map.get(engine.shared_messages, id),
         %{} = recipient <- Map.get(engine.shared_recipients, {id, principal.person}) do
      {:ok, SharedMessages.person_item(engine, principal.person, record, recipient, rev)}
    else
      _ -> :skip
    end
  end

  defp render_row(:item, {:item, {sender, message_id}}, context, principal, rev) do
    key = {principal.person, sender, message_id}

    with true <- AgentAccess.visible_sender?(principal, sender),
         true <- AttentionServer.BounceDecision.person_visible?(context.engine.messages[key]),
         {:ok, item_id} <- Resources.item_id(context.engine, key),
         {:ok, rank} <- Reads.item_rank(context, principal.person, item_id) do
      Projection.run({:item, item_id}, fn ->
        {:ok,
         context.engine
         |> Resources.item(key, rank)
         |> Resources.stamp(rev)
         |> then(&Resources.render(:item, &1, principal))}
      end)
    else
      # The feed must fail the step rather than skip a change it could not
      # render because the store failed.
      {:error, :storage_unavailable} -> {:error, :storage_unavailable}
      _ -> :skip
    end
  end

  defp render_row(:item_removed, {:item, {sender, message_id}}, context, principal, _rev) do
    with true <- AgentAccess.visible_sender?(principal, sender),
         {:ok, item_id} <-
           Resources.removed_item_id(context.engine, {principal.person, sender, message_id}) do
      {:ok, %{"item_id" => item_id}}
    else
      _ -> :skip
    end
  end

  defp render_row(:entry, {:shared_entry, id}, %{shared?: true, engine: engine}, principal, rev) do
    with %{shared_id: message_id, position: position} = entry <- engine.shared_entries[id],
         %{} = recipient <- Map.get(engine.shared_recipients, {message_id, principal.person}),
         cutoff = get_in(recipient, [:frozen, :entry_high]),
         true <- is_nil(cutoff) or position <= cutoff do
      {:ok, SharedMessages.person_entry(recipient, principal.person, entry, rev)}
    else
      _ -> :skip
    end
  end

  defp render_row(:entry, {:entry, seq}, context, principal, rev) do
    with %{} = entry <- Map.get(context.entries_by_fact, seq),
         true <- visible_entry?(context.engine, principal, entry),
         :ok <- keep_entry_row(context, principal.person, entry, seq, rev) do
      at_ms =
        case ChangeLog.created_at(
               context.store,
               principal.person,
               ChangeLog.entry_resource(entry["position"])
             ) do
          {:ok, at_ms} -> at_ms
          _ -> nil
        end

      Projection.run(Projection.change_key(entry), fn ->
        {:ok,
         entry
         |> Resources.stamp(rev)
         |> Resources.put_at(at_ms)
         |> then(&Resources.render(:entry, &1, principal))}
      end)
    else
      absent when absent in [nil, false] -> :skip
      result -> result
    end
  end

  defp render_row(:counts, resource, %{shared?: true, engine: engine}, principal, _rev)
       when resource in [:counts, :shared_counts],
       do: {:ok, SharedMessages.counts(engine, principal.person)}

  defp render_row(:counts, :counts, context, principal, _rev),
    do: {:ok, Resources.counts(context.engine, principal.person)}

  defp render_row(:settings, :settings, context, principal, rev) do
    {:ok,
     context.engine
     |> Resources.settings(principal.person)
     |> Resources.stamp(rev)
     |> then(&Resources.render(:settings, &1, principal))}
  end

  defp render_row(_kind, _resource, _context, _principal, _rev), do: :skip

  defp visible_entry?(engine, %{kind: :agent} = principal, entry) do
    case Resources.item_key(engine, principal.person, entry["item_id"]) do
      {:ok, {_, sender, _}} -> AgentAccess.visible_sender?(principal, sender)
      _ -> false
    end
  end

  defp visible_entry?(_engine, _principal, _entry), do: true

  # The log compacts by fact sequence. Acknowledgments and the original turn
  # can still resolve to the same Entry. Check every source's current rev,
  # including sources on later pages, and let the raw rows drive the cursor.
  defp keep_entry_row(%{compact_entries?: true, store: store}, person, entry, seq, rev) do
    entry
    |> Resources.entry_sources()
    |> Enum.reduce_while(:ok, fn
      ^seq, :ok ->
        {:cont, :ok}

      source, :ok ->
        case ChangeLog.rev(store, person, ChangeLog.entry_resource(source)) do
          {:ok, newer} when newer > rev -> {:halt, :skip}
          {:ok, _} -> {:cont, :ok}
          {:error, :not_found} -> {:cont, :ok}
          {:error, _} -> {:halt, {:error, :storage_unavailable}}
        end
    end)
  end

  defp keep_entry_row(_context, _person, _entry, _seq, _rev), do: :ok
end
