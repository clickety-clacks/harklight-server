defmodule AttentionServer.ClientAPI.FeedRenderer do
  @moduledoc """
  Renders change-log rows as the contract's change data (docs/client-api-v2.md
  §6): an `item` row as the Item, `item_removed` as `{item_id}`, an `entry` row
  as the Entry its fact belongs to, `counts` and `settings` as theirs. The feed
  calls it in the relay's serialized step, from current state, as the
  compacted log requires. A row whose resource has no client form, such as a
  read-position fact, or a principal that no longer authenticates, is skipped.
  """

  alias AttentionServer.ChangeLog
  alias AttentionServer.ClientAPI.{Reads, Resources}

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

  defp render_row(:item, {:item, {sender, message_id}}, context, principal, rev) do
    key = {principal.person, sender, message_id}

    with {:ok, item_id} <- Resources.item_id(context.engine, key),
         {:ok, rank} <- Reads.item_rank(context, principal.person, item_id) do
      {:ok,
       context.engine
       |> Resources.item(key, rank)
       |> Resources.stamp(rev)
       |> then(&Resources.render(:item, &1, principal))}
    else
      # The feed must fail the step rather than skip a change it could not
      # render because the store failed.
      {:error, :storage_unavailable} -> {:error, :storage_unavailable}
      _ -> :skip
    end
  end

  defp render_row(:item_removed, {:item, {sender, message_id}}, context, principal, _rev) do
    case Resources.removed_item_id(context.engine, {principal.person, sender, message_id}) do
      {:ok, item_id} -> {:ok, %{"item_id" => item_id}}
      _ -> :skip
    end
  end

  defp render_row(:entry, {:entry, seq}, context, principal, rev) do
    case Map.get(context.entries_by_fact, seq) do
      nil ->
        :skip

      entry ->
        at_ms =
          case ChangeLog.created_at(
                 context.store,
                 principal.person,
                 ChangeLog.entry_resource(entry["position"])
               ) do
            {:ok, at_ms} -> at_ms
            _ -> nil
          end

        {:ok,
         entry
         |> Resources.stamp(rev)
         |> Resources.put_at(at_ms)
         |> then(&Resources.render(:entry, &1, principal))}
    end
  end

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
end
