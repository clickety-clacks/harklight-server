defmodule AttentionServer.InboxPaging do
  @moduledoc false

  alias AttentionServer.{Engine, JSON, Store}

  @page_size 20
  @receipt_frame_limit 1_900_000
  @max_startup_id 128
  @max_batch_id 128

  def start_plan(store, engine, key, frame) do
    requested_since = frame["since"]

    with :ok <- version(frame),
         :ok <-
           fields(
             frame,
             ~w(v type ref paging_version startup_id since),
             ~w(v type paging_version startup_id since)
           ),
         :ok <- bounded_id(frame["startup_id"], @max_startup_id, "startup_id"),
         {:ok, person} <- Engine.authorized_person(engine, key),
         {:ok, existing} <- Store.inbox_startup(store, person, key, frame["startup_id"]) do
      case existing do
        nil ->
          with {:ok, ^person, high} <- Engine.inbox_high_water(engine, key, requested_since) do
            {:build,
             %{
               person_id: person,
               device_id: key,
               startup_id: frame["startup_id"],
               snapshot_id: opaque_id(),
               since: requested_since,
               high_water: high,
               cursor_secret: :crypto.strong_rand_bytes(32)
             }}
          else
            {:error, reason} -> {:error, reason}
            _ -> {:error, :invalid_cursor}
          end

        %{since: ^requested_since, status: "building"} ->
          {:building, existing}

        %{since: ^requested_since} ->
          {:ready, existing}

        _ ->
          {:error, :checkpoint_conflict}
      end
    end
  end

  def snapshot_data(engine, key, plan) do
    with {:ok, items, receipts, operation_ids, high} <-
           Engine.inbox_snapshot(engine, key, plan.since),
         true <- high == plan.high_water,
         {:ok, person} <- Engine.authorized_person(engine, key),
         true <- person == plan.person_id do
      {:ok, %{items: items, receipts: receipts, operation_ids: operation_ids}}
    else
      false -> {:error, :snapshot_unavailable}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :snapshot_unavailable}
    end
  end

  def mutation_types,
    do: [
      "forward",
      "answer",
      "lifecycle",
      "subscribe_remote",
      "push_register",
      "push_remove",
      "source_bind",
      "source_result"
    ]

  def start(store, engine, key, frame) do
    with {:ready, snapshot} <- start_plan(store, engine, key, frame),
         :ok <- usable(snapshot),
         {:ok, page_items} <- project_items(store, engine, key, snapshot, 0) do
      next_offset = min(snapshot.item_count, @page_size)

      reply =
        ok(frame, %{
          "paging_version" => 1,
          "snapshot_id" => snapshot.snapshot_id,
          "high_water" => snapshot.high_water,
          "items" => page_items,
          "next_cursor" =>
            if(next_offset < snapshot.item_count,
              do: cursor(snapshot, :items, next_offset),
              else: nil
            ),
          "receipt_cursor" =>
            if(snapshot.receipt_count == 0, do: nil, else: cursor(snapshot, :receipts, 0))
        })

      {:ok, reply, snapshot, false}
    else
      {:build, _plan} -> {:error, :snapshot_build_required}
      {:building, _snapshot} -> {:error, :snapshot_build_pending}
      {:error, reason} -> {:error, reason}
    end
  end

  def page(store, engine, key, frame) do
    with :ok <- version(frame),
         :ok <-
           fields(
             frame,
             ~w(v type ref paging_version snapshot_id cursor),
             ~w(v type paging_version snapshot_id cursor)
           ),
         {:ok, person} <- Engine.authorized_person(engine, key),
         {:ok, snapshot} <- Store.inbox_snapshot(store, person, key, frame["snapshot_id"]),
         :ok <- usable(snapshot),
         {:ok, offset} <- read_cursor(snapshot, :items, frame["cursor"]),
         true <- offset > 0 and rem(offset, @page_size) == 0 and offset < snapshot.item_count,
         {:ok, page_items} <- project_items(store, engine, key, snapshot, offset) do
      next_offset = min(snapshot.item_count, offset + @page_size)

      {:ok,
       ok(frame, %{
         "paging_version" => 1,
         "snapshot_id" => snapshot.snapshot_id,
         "high_water" => snapshot.high_water,
         "items" => page_items,
         "next_cursor" =>
           if(next_offset < snapshot.item_count,
             do: cursor(snapshot, :items, next_offset),
             else: nil
           )
       }), snapshot}
    else
      false -> {:error, :invalid_page_cursor}
      {:error, reason} -> {:error, reason}
    end
  end

  def reconcile(store, engine, key, frame) do
    with :ok <- version(frame),
         :ok <-
           fields(
             frame,
             ~w(v type ref paging_version snapshot_id batch_id operations),
             ~w(v type paging_version snapshot_id batch_id operations)
           ),
         :ok <- bounded_id(frame["batch_id"], @max_batch_id, "batch_id"),
         operations when is_list(operations) and operations != [] <- frame["operations"],
         true <- length(operations) <= 100,
         {:ok, normalized} <- normalize_operations(operations),
         {:ok, person} <- Engine.authorized_person(engine, key),
         {:ok, snapshot} <- Store.inbox_snapshot(store, person, key, frame["snapshot_id"]),
         :ok <- usable(snapshot),
         digest <- :crypto.hash(:sha256, JSON.canonical(normalized)),
         {:ok, result} <-
           reconcile_batch(store, engine, key, person, snapshot, frame, normalized, digest) do
      {:ok, put_ref(result, frame), snapshot}
    else
      false -> {:error, :invalid_field}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :invalid_field}
    end
  end

  def receipts(store, engine, key, frame) do
    with :ok <- version(frame),
         :ok <-
           fields(
             frame,
             ~w(v type ref paging_version snapshot_id cursor),
             ~w(v type paging_version snapshot_id cursor)
           ),
         {:ok, person} <- Engine.authorized_person(engine, key),
         {:ok, snapshot} <- Store.inbox_snapshot(store, person, key, frame["snapshot_id"]),
         :ok <- usable(snapshot),
         {:ok, offset} <- read_cursor(snapshot, :receipts, frame["cursor"]),
         {:ok, stored} <-
           Store.inbox_receipt_page(store, person, key, snapshot.snapshot_id, offset),
         {:ok, result} <- receipt_result(store, engine, key, person, snapshot, offset, stored) do
      {:ok, put_ref(result, frame), snapshot}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :invalid_page_cursor}
    end
  end

  def checkpoint_ack(store, engine, key, frame) do
    with :ok <- version(frame),
         :ok <-
           fields(
             frame,
             ~w(v type ref paging_version snapshot_id high_water reconciled_batches pending_manifest_complete),
             ~w(v type paging_version snapshot_id high_water reconciled_batches pending_manifest_complete)
           ),
         {:ok, person} <- Engine.authorized_person(engine, key),
         true <- frame["pending_manifest_complete"] == true,
         batches when is_list(batches) <- frame["reconciled_batches"],
         true <-
           Enum.all?(batches, &valid_id?(&1, @max_batch_id)) and
             length(batches) == length(Enum.uniq(batches)),
         {:ok, snapshot} <- Store.inbox_snapshot(store, person, key, frame["snapshot_id"]),
         :ok <- usable(snapshot),
         true <- snapshot.high_water == frame["high_water"],
         {:ok, next_store, duplicate?} <-
           Store.accept_inbox_checkpoint(
             store,
             engine,
             person,
             key,
             snapshot.snapshot_id,
             snapshot.high_water,
             batches,
             %{person: person, actor: key, action: "inbox_checkpoint"}
           ) do
      reply =
        ok(frame, %{
          "paging_version" => 1,
          "snapshot_id" => snapshot.snapshot_id,
          "high_water" => snapshot.high_water,
          "checkpoint_accepted" => true
        })

      reply = if duplicate?, do: Map.put(reply, "duplicate", true), else: reply
      {:ok, reply, next_store, snapshot}
    else
      false -> {:error, :checkpoint_conflict, store}
      {:error, reason, failed_store} -> {:error, reason, failed_store}
      {:error, reason} -> {:error, reason, store}
      _ -> {:error, :invalid_field, store}
    end
  end

  def attach(store, engine, key, frame) do
    with :ok <- version(frame),
         :ok <- fields(frame, attach_fields(frame["mode"]), attach_required(frame["mode"])),
         {:ok, person} <- Engine.authorized_person(engine, key),
         {:ok, snapshot} <- Store.inbox_snapshot(store, person, key, frame["snapshot_id"]),
         :ok <- usable(snapshot),
         {:ok, result} <- attach_mode(engine, key, snapshot, frame) do
      {:ok, result, snapshot}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :invalid_field}
    end
  end

  defp reconcile_batch(store, engine, key, person, snapshot, frame, operations, digest) do
    batch_id = frame["batch_id"]

    with {:ok, existing} <- Store.inbox_batch(store, person, key, snapshot.snapshot_id, batch_id) do
      case existing do
        %{digest: ^digest, result: result} ->
          {:ok, result}

        %{digest: _} ->
          {:error, :checkpoint_conflict}

        nil ->
          with {:ok, results} <-
                 reconcile_operations(store, engine, key, snapshot, operations) do
            result =
              ok(frame, %{
                "paging_version" => 1,
                "snapshot_id" => snapshot.snapshot_id,
                "high_water" => snapshot.high_water,
                "batch_id" => batch_id,
                "results" => results
              })
              |> Map.delete("ref")

            Store.save_inbox_batch(
              store,
              person,
              key,
              snapshot.snapshot_id,
              batch_id,
              digest,
              result
            )
          end
      end
    end
  end

  defp reconcile_operations(store, engine, key, snapshot, operations) do
    Enum.reduce_while(operations, {:ok, []}, fn operation, {:ok, acc} ->
      with {:ok, committed_at_snapshot?} <-
             Store.inbox_operation_at_snapshot?(store, snapshot, operation["op_id"]),
           {:ok, result} <-
             Engine.reconcile_operation(
               engine,
               key,
               operation,
               snapshot.high_water,
               committed_at_snapshot?
             ) do
        {:cont, {:ok, [result | acc]}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, results} -> {:ok, Enum.reverse(results)}
      error -> error
    end
  end

  defp receipt_result(_store, _engine, _key, _person, _snapshot, _offset, stored)
       when not is_nil(stored),
       do: {:ok, stored}

  defp receipt_result(store, engine, key, person, snapshot, offset, nil) do
    with true <- offset == snapshot.receipt_next_offset,
         {:ok, events, next_offset} <- receipt_chunk(store, engine, key, snapshot, offset),
         next_cursor <-
           if(is_nil(next_offset), do: nil, else: cursor(snapshot, :receipts, next_offset)),
         result <- %{
           "v" => 1,
           "type" => "ok",
           "paging_version" => 1,
           "snapshot_id" => snapshot.snapshot_id,
           "high_water" => snapshot.high_water,
           "events" => events,
           "next_cursor" => next_cursor
         },
         {:ok, saved} <-
           Store.save_inbox_receipt_page(
             store,
             person,
             key,
             snapshot.snapshot_id,
             offset,
             next_offset,
             result
           ) do
      {:ok, saved}
    else
      false -> {:error, :invalid_page_cursor}
      {:error, reason} -> {:error, reason}
    end
  end

  defp receipt_chunk(store, engine, key, snapshot, offset) do
    count = min(@page_size, snapshot.receipt_count - offset)

    if count <= 0 do
      {:error, :invalid_page_cursor}
    else
      with {:ok, sequences} <-
             Store.inbox_receipt_sequences(store, snapshot, offset, count),
           {:ok, events, next_offset} <-
             take_receipts(engine, key, snapshot, sequences, offset, [], 0) do
        {:ok, events, next_offset}
      end
    end
  end

  defp take_receipts(_engine, _key, snapshot, [], offset, events, _bytes),
    do: {:ok, Enum.reverse(events), if(offset < snapshot.receipt_count, do: offset, else: nil)}

  defp take_receipts(engine, key, snapshot, [seq | rest], offset, events, bytes) do
    with {:ok, event} <- Engine.inbox_receipt(engine, key, seq) do
      event_bytes = byte_size(JSON.encode!(event))
      next_bytes = bytes + event_bytes

      cond do
        next_bytes > @receipt_frame_limit and events == [] ->
          {:error, :snapshot_unavailable}

        next_bytes > @receipt_frame_limit ->
          {:ok, Enum.reverse(events), offset}

        true ->
          take_receipts(engine, key, snapshot, rest, offset + 1, [event | events], next_bytes)
      end
    end
  end

  defp attach_mode(_engine, _key, snapshot, %{"mode" => "checkpoint_status"} = frame) do
    with true <- frame["since"] == snapshot.since,
         true <- frame["candidate_high_water"] == snapshot.high_water,
         status when status in ["pending", "accepted"] <- snapshot.status do
      {:ok,
       ok(frame, %{
         "paging_version" => 1,
         "snapshot_id" => snapshot.snapshot_id,
         "high_water" => snapshot.high_water,
         "checkpoint_accepted" => status == "accepted",
         "replay_started" => false
       })}
    else
      false -> {:error, :checkpoint_conflict}
      _ -> {:error, :snapshot_replaced}
    end
  end

  defp attach_mode(engine, key, snapshot, %{"mode" => "replay"} = frame) do
    since = frame["since"]

    cond do
      snapshot.status == "pending" ->
        {:error, :checkpoint_pending}

      snapshot.status != "accepted" ->
        {:error, :snapshot_replaced}

      not (is_integer(since) and since >= snapshot.high_water) ->
        {:error, :invalid_cursor}

      true ->
        case Engine.events(engine, key, since) do
          {:ok, events, high} ->
            reply =
              ok(frame, %{
                "paging_version" => 1,
                "snapshot_id" => snapshot.snapshot_id,
                "high_water" => snapshot.high_water,
                "checkpoint_accepted" => true,
                "replay_started" => true
              })

            {:ok, %{reply: reply, events: events, barrier: high, since: since}}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  defp attach_mode(_engine, _key, _snapshot, _frame), do: {:error, :invalid_field}

  defp project_items(store, engine, key, snapshot, offset) do
    with {:ok, identities} <- Store.inbox_items(store, snapshot, offset) do
      Enum.reduce_while(identities, {:ok, []}, fn identity, {:ok, acc} ->
        case Engine.inbox_item(engine, key, identity, snapshot.high_water) do
          {:ok, item} -> {:cont, {:ok, [item | acc]}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
      |> case do
        {:ok, items} -> {:ok, Enum.reverse(items)}
        error -> error
      end
    end
  end

  defp normalize_operations(operations) do
    if Enum.all?(operations, &is_map/1) do
      {:ok, Enum.map(operations, &Map.delete(&1, "ref"))}
    else
      {:error, :invalid_field}
    end
  end

  defp usable(nil), do: {:error, :invalid_snapshot}
  defp usable(%{status: "replaced"}), do: {:error, :snapshot_replaced}
  defp usable(%{status: "building"}), do: {:error, :snapshot_build_pending}

  defp usable(%{item_count: items, receipt_count: receipts, operation_count: operations})
       when is_integer(items) and items >= 0 and is_integer(receipts) and receipts >= 0 and
              is_integer(operations) and operations >= 0,
       do: :ok

  defp usable(_), do: {:error, :snapshot_unavailable}

  defp read_cursor(snapshot, kind, value) when is_binary(value) do
    case Base.url_decode64(value, padding: false) do
      {:ok, <<offset::unsigned-64, supplied::binary-size(32)>>} ->
        expected = cursor_mac(snapshot, kind, offset)

        if :crypto.hash_equals(expected, supplied),
          do: {:ok, offset},
          else: {:error, :invalid_page_cursor}

      _ ->
        {:error, :invalid_page_cursor}
    end
  end

  defp read_cursor(_snapshot, _kind, _value), do: {:error, :invalid_page_cursor}

  defp cursor(snapshot, kind, offset) do
    payload = <<offset::unsigned-64, cursor_mac(snapshot, kind, offset)::binary>>
    Base.url_encode64(payload, padding: false)
  end

  defp cursor_mac(snapshot, kind, offset) do
    data = Enum.join([snapshot.snapshot_id, Atom.to_string(kind), Integer.to_string(offset)], ":")
    :crypto.mac(:hmac, :sha256, snapshot.cursor_secret, data)
  end

  defp opaque_id, do: Base.url_encode64(:crypto.strong_rand_bytes(24), padding: false)

  defp valid_id?(value, limit), do: is_binary(value) and byte_size(value) in 1..limit

  defp bounded_id(value, limit, field) do
    if valid_id?(value, limit), do: :ok, else: {:error, {:invalid_field, field}}
  end

  defp version(%{"v" => 1, "paging_version" => 1}), do: :ok
  defp version(%{"v" => 1, "paging_version" => _}), do: {:error, :unsupported_version}
  defp version(%{"paging_version" => 1}), do: {:error, :unsupported_version}
  defp version(%{"paging_version" => _}), do: {:error, :unsupported_version}
  defp version(_), do: {:error, :invalid_field}

  defp fields(frame, allowed, required) do
    cond do
      Map.has_key?(frame, "ref") and not is_binary(frame["ref"]) ->
        {:error, {:invalid_field, "ref"}}

      not Enum.all?(required, &Map.has_key?(frame, &1)) ->
        {:error, :invalid_field}

      Enum.all?(Map.keys(frame), &(&1 in allowed)) ->
        :ok

      true ->
        {:error, :invalid_field}
    end
  end

  defp attach_fields("checkpoint_status"),
    do: ~w(v type ref paging_version snapshot_id mode since candidate_high_water)

  defp attach_fields("replay"), do: ~w(v type ref paging_version snapshot_id mode since)
  defp attach_fields(_), do: ~w(v type ref paging_version snapshot_id mode)

  defp attach_required("checkpoint_status"),
    do: ~w(v type paging_version snapshot_id mode since candidate_high_water)

  defp attach_required("replay"), do: ~w(v type paging_version snapshot_id mode since)
  defp attach_required(_), do: ~w(v type paging_version snapshot_id mode)

  defp ok(frame, fields) do
    base = %{"v" => 1, "type" => "ok"}
    base = if is_binary(frame["ref"]), do: Map.put(base, "ref", frame["ref"]), else: base
    Map.merge(base, fields)
  end

  defp put_ref(result, frame) do
    if is_binary(frame["ref"]), do: Map.put(result, "ref", frame["ref"]), else: result
  end
end
