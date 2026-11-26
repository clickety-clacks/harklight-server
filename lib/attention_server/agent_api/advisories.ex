defmodule AttentionServer.AgentAPI.Advisories do
  @moduledoc false

  alias AttentionServer.{AgentChangeLog, BounceDecision, Engine, RouteBIndex, Store}
  alias AttentionServer.ClientAPI.{Projection, Resources}

  def stamp_decisions(%Store{change_log: false}, next, _keys), do: {:ok, next}

  def stamp_decisions(%Store{} = store, next, keys) do
    with {:ok, next} <- stamp_legacy(store, next, keys) do
      Enum.reduce_while(keys, {:ok, next}, fn {person, _, _} = key, {:ok, current} ->
        record = Map.fetch!(current.messages, key)
        notices = Map.get(record, "item_advisories", %{})

        if Enum.any?(notices, fn {_, notice} -> not Map.has_key?(notice, "decided_at_rev") end) do
          case AgentChangeLog.head(store, {person, record.origin}) do
            {:ok, head} ->
              stamped =
                Map.new(notices, fn {id, notice} ->
                  {id, Map.put_new(notice, "decided_at_rev", head)}
                end)

              {:cont, {:ok, put_notices(current, key, record, stamped)}}

            error ->
              {:halt, error}
          end
        else
          {:cont, {:ok, current}}
        end
      end)
    end
  end

  defp stamp_legacy(store, next, keys) do
    next
    |> BounceDecision.advisory_feed_keys_for_changes(keys)
    |> Enum.reduce_while({:ok, next}, fn {person, agent, _} = scope, {:ok, current} ->
      previous = get_in(store.engine.advisory_feeds, [scope, :high]) || 0
      high = get_in(next.advisory_feeds, [scope, :high]) || 0

      if high > previous do
        case AgentChangeLog.head(store, {person, agent}) do
          {:ok, head} ->
            stamped =
              Enum.reduce((previous + 1)..high, current, fn sequence, engine ->
                put_in(engine.advisory_feeds[scope].entries[sequence]["decided_at_rev"], head)
              end)

            {:cont, {:ok, stamped}}

          error ->
            {:halt, error}
        end
      else
        {:cont, {:ok, current}}
      end
    end)
  end

  def for_message(engine, {person, key}, {person, _, _} = item) do
    case Map.get(engine.messages, item) do
      %{origin: ^key} = record ->
        # FORMAT30 also accepts historical messages with no session field.
        legacy =
          case Map.get(engine.advisory_feeds, {person, key, Map.get(record, :session_handle)}) do
            %{entries: entries, receipts: receipts} ->
              engine.route_b_index
              |> RouteBIndex.advisory_sequences(item)
              |> Enum.sort()
              |> Enum.map(fn seq ->
                render_legacy(engine, item, Map.fetch!(entries, seq), receipts)
              end)

            nil ->
              []
          end

        current =
          record
          |> Map.get("item_advisories", %{})
          |> Map.values()
          |> Enum.sort_by(& &1["request_revision"])
          |> Enum.map(&render_item(engine, item, &1))

        legacy ++ current

      _ ->
        []
    end
  end

  def change(engine, {person, _, _} = scope, id) do
    with {:ok, sequence} <- Map.fetch(engine.advisory_id_index, {scope, id}),
         %{entries: %{^sequence => advisory}, receipts: receipts} <-
           Map.get(engine.advisory_feeds, scope),
         %{"sender_id" => sender, "message_id" => message} <- advisory do
      item = {person, sender, message}
      render_change(engine, item, render_legacy(engine, item, advisory, receipts))
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  def item_change(engine, {person, origin}, {person, _, _} = item, id) do
    case Map.get(engine.messages, item) do
      %{"item_advisories" => %{^id => advisory}, origin: ^origin} ->
        render_change(engine, item, render_item(engine, item, advisory))

      _ ->
        {:error, :storage_unavailable}
    end
  end

  def acknowledge(engine, {person, origin}, {person, _, _} = item, id) do
    record = Map.fetch!(engine.messages, item)

    with %{origin: ^origin} <- record,
         advisory when is_map(advisory) <-
           Enum.find(for_message(engine, {person, origin}, item), &(&1["advisory_id"] == id)) do
      cond do
        advisory["moot"] or advisory["delivered"] ->
          {:ok, engine,
           %{
             "advisory_id" => id,
             "already_delivered" => advisory["delivered"],
             "delivered" => advisory["delivered"],
             "moot" => advisory["moot"]
           }}

        is_map_key(Map.get(record, "item_advisories", %{}), id) ->
          notices = put_in(record["item_advisories"][id]["delivered"], true)["item_advisories"]
          next = put_notices(engine, item, record, notices)
          {:ok, next, %{"advisory_id" => id, "already_delivered" => false, "delivered" => true}}

        true ->
          BounceDecision.advisory_ack(engine, origin, Map.get(record, :session_handle), id)
      end
    else
      _ -> {:error, :not_found}
    end
  end

  defp put_notices(engine, item, record, notices),
    do: Engine.put_row(engine, :messages, item, Map.put(record, "item_advisories", notices))

  defp render_change(engine, item, advisory) do
    case Resources.item_id(engine, item) do
      {:ok, item_id} ->
        Projection.run({:item, item_id}, fn ->
          {:ok, %{"item_id" => item_id, "advisory" => advisory}}
        end)

      _ ->
        {:error, :storage_unavailable}
    end
  end

  defp render_legacy(engine, item, advisory, receipts) do
    advisory
    |> Map.drop(
      ~w(kind sender_id message_id session_handle advisory_seq origin_cursor_at_decision)
    )
    |> Map.put("delivered", MapSet.member?(receipts, advisory["advisory_id"]))
    |> then(&render_item(engine, item, &1))
  end

  defp render_item(engine, item, advisory) do
    advisory
    |> Map.put_new("decided_at_rev", 0)
    |> Map.put("moot", BounceDecision.notice_moot?(engine, item, advisory["request_revision"]))
  end
end
