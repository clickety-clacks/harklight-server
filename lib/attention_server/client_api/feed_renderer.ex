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

  def render(%{engine: engine} = context, who, %{rev: rev, kind: kind, resource_id: id}) do
    with {:ok, principal} <- Reads.client_principal(engine, who) do
      render_row(kind, ChangeLog.decode_resource(id), context, principal, rev)
    else
      _ -> :skip
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
    case Resources.entry_for_fact(context.engine, principal.person, seq, context.view) do
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
