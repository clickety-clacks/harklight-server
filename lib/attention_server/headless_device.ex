defmodule AttentionServer.HeadlessDevice do
  @moduledoc """
  Enrolled-device commands over client API v2 on the device port.

  The first `read` pages through `GET /v2/inbox` and stores every item in
  `v2/items.json` together with its cursor, the first page's `rev`, in one
  atomic write. Later reads fetch the JSON change batch after that cursor and
  store each batch with its new cursor the same way; `410 cursor_expired` means
  a fresh snapshot. `respond` resolves `--sender`/`--message-id` to the item
  delivered to this device, stores the exact answer body with a fresh
  `Idempotency-Key`, and only then sends `POST /v2/items/{item_id}/answer`.

  Files a protocol 0.1 tool left in the directory (`cursor.json`, `events/`,
  and `operations/` entries without the `v2-` prefix) are never read.
  """

  import Bitwise

  import AttentionServer.HeadlessV2Write,
    only: [
      existing_private_directory: 1,
      hex: 1,
      private_directory: 1,
      read_document: 1,
      read_json: 1,
      write_private: 2
    ]

  alias AttentionServer.{DeviceIdentity, HeadlessTLS, HeadlessV2Write, JSON}

  # Inbox page size and change-batch size for client API v2 reads (the
  # contract's maxima).
  @v2_page_limit 200
  @v2_batch_limit 500

  def read(directory, endpoint), do: v2_read(directory, endpoint)

  # The answer file is the contract's AnswerBody: an object with at most one of
  # choice, comment and result, and optionally request_revision. `{}` answers
  # with no comment.
  def respond(directory, endpoint, sender, message_id, answer_file) do
    identity = {Path.join(directory, "identity.pem"), Path.join(directory, "identity.key")}
    request = %{"sender_id" => sender, "message_id" => message_id}

    with :ok <- existing_private_directory(directory),
         {:ok, answer} <- read_document(answer_file),
         true <- HeadlessV2Write.answer_body?(answer) || {:error, :invalid_answer},
         {:ok, _} <- enrolled_identity(directory, endpoint),
         {:ok, saved} <-
           HeadlessV2Write.saved_answer(directory, Map.put(request, "answer", answer), nil, fn ->
             with {:ok, item} <- v2_resolve(directory, endpoint, request) do
               body = Map.put_new(answer, "request_revision", item["request_revision"])
               {:ok, item["item_id"], body}
             end
           end) do
      HeadlessV2Write.post_answer(endpoint, identity, saved)
    end
  end

  # A full snapshot: every inbox page, stored, then the cursor. The cursor is
  # the first page's rev, so anything that moved while paging is after it
  # (spec §5.1, rebaseline). Items no longer in the inbox are dropped.
  # With a stored cursor, read forward with the JSON change batch; without one,
  # or when the server says the cursor expired, take a full snapshot.
  defp v2_read(directory, endpoint) do
    identity = {Path.join(directory, "identity.pem"), Path.join(directory, "identity.key")}

    with {:ok, _} <- enrolled_identity(directory, endpoint),
         :ok <- private_directory(Path.join(directory, "v2")) do
      case v2_held(directory) do
        {:ok, rev, items, tombstones} ->
          case v2_forward(directory, endpoint, identity, rev, items, tombstones) do
            {:error, :cursor_expired} -> v2_snapshot(directory, endpoint, identity)
            other -> other
          end

        :none ->
          v2_snapshot(directory, endpoint, identity)
      end
    end
  end

  defp v2_snapshot(directory, endpoint, identity) do
    limit = Map.get(endpoint, :v2_page_limit, @v2_page_limit)

    with {:ok, rev, items} <- v2_inbox(endpoint, identity, limit, nil, nil, %{}),
         :ok <- v2_store(directory, rev, items, %{}) do
      {:ok, %{"rev" => rev, "items" => v2_ordered(items)}}
    end
  end

  # Each batch is gap-free, so it is stored and only then is the cursor
  # advanced to its rev (spec §5.1). Changes are applied by the version rule:
  # an item replaces what is held only at a higher rev, and a removal leaves a
  # tombstone. Entries, counts and settings are not kept here; the cursor still
  # moves past them.
  defp v2_forward(directory, endpoint, identity, rev, items, tombstones) do
    limit = Map.get(endpoint, :v2_batch_limit, @v2_batch_limit)

    case HeadlessTLS.v2_get(endpoint, identity, "/v2/changes?after=#{rev}&limit=#{limit}") do
      {:ok, 200, %{"rev" => next, "more" => true}} when next == rev ->
        # `more` with no progress would repeat this request forever (review C6-4).
        {:error, :no_progress}

      {:ok, 200, %{"changes" => changes, "rev" => next, "more" => more}}
      when is_list(changes) and is_integer(next) and next >= rev and is_boolean(more) ->
        with {:ok, items, tombstones} <- v2_apply(changes, items, tombstones),
             :ok <- v2_store(directory, next, items, tombstones) do
          if more,
            do: v2_forward(directory, endpoint, identity, next, items, tombstones),
            else: {:ok, %{"rev" => next, "items" => v2_ordered(items)}}
        end

      {:ok, 410, %{"error" => "cursor_expired"}} ->
        {:error, :cursor_expired}

      {:ok, _status, %{"error" => token} = error} when is_binary(token) ->
        {:error, {:server, error}}

      {:ok, _status, _body} ->
        {:error, :invalid_v2_response}

      error ->
        error
    end
  end

  defp v2_apply(changes, items, tombstones) do
    Enum.reduce_while(changes, {:ok, items, tombstones}, fn
      %{"kind" => "item", "rev" => rev, "data" => %{"item_id" => id} = item}, {:ok, items, dead}
      when is_integer(rev) and is_binary(id) ->
        held = max(get_in(items, [id, "rev"]) || 0, Map.get(dead, id, 0))

        if rev > held,
          do: {:cont, {:ok, Map.put(items, id, item), Map.delete(dead, id)}},
          else: {:cont, {:ok, items, dead}}

      %{"kind" => "item_removed", "rev" => rev, "data" => %{"item_id" => id}}, {:ok, items, dead}
      when is_integer(rev) and is_binary(id) ->
        if rev > (get_in(items, [id, "rev"]) || 0),
          do:
            {:cont,
             {:ok, Map.delete(items, id), Map.put(dead, id, max(rev, Map.get(dead, id, 0)))}},
          else: {:cont, {:ok, items, dead}}

      %{"kind" => kind, "rev" => rev}, acc
      when is_binary(kind) and kind not in ["item", "item_removed"] and is_integer(rev) ->
        {:cont, acc}

      # A malformed item change is never skipped: the read fails and the
      # cursor stays where it was (review C6-3).
      _change, _acc ->
        {:halt, {:error, :invalid_v2_change}}
    end)
  end

  # The cursor and the items it covers are one file, written by one atomic
  # rename. Overlapping reads (a respond reads too) can only leave an older but
  # consistent state, never a cursor ahead of its items (review C6-1).
  defp v2_store(directory, rev, items, tombstones) do
    write_private(Path.join([directory, "v2", "items.json"]), %{
      "rev" => rev,
      "items" => v2_ordered(items),
      "tombstones" => tombstones
    })
  end

  defp v2_ordered(items), do: items |> Map.values() |> Enum.sort_by(&get_in(&1, ["rank", "key"]))

  # The held state: the cursor and the items stored before it. Anything
  # unreadable means no usable cursor, so the next read is a snapshot.
  defp v2_held(directory) do
    with {:ok, %{"rev" => rev, "items" => items} = held}
         when is_integer(rev) and rev >= 0 and is_list(items) <-
           read_local_json(Path.join([directory, "v2", "items.json"])),
         true <- Enum.all?(items, &v2_item?/1),
         tombstones when is_map(tombstones) <- Map.get(held, "tombstones", %{}) do
      {:ok, rev, Map.new(items, &{&1["item_id"], &1}), tombstones}
    else
      _ -> :none
    end
  end

  defp v2_inbox(endpoint, identity, limit, page, first_rev, items) do
    path =
      "/v2/inbox?limit=#{limit}" <>
        if(page, do: "&page=" <> URI.encode_www_form(page), else: "")

    case HeadlessTLS.v2_get(endpoint, identity, path) do
      {:ok, 200, %{"rev" => rev, "items" => page_items} = body}
      when is_integer(rev) and rev >= 0 and is_list(page_items) ->
        if Enum.all?(page_items, &v2_item?/1),
          do: v2_page(endpoint, identity, limit, body, first_rev || rev, page_items, items),
          else: {:error, :invalid_v2_response}

      {:ok, _status, %{"error" => token} = error} when is_binary(token) ->
        {:error, {:server, error}}

      {:ok, _status, _body} ->
        {:error, :invalid_v2_response}

      error ->
        error
    end
  end

  defp v2_item?(%{"item_id" => id, "rev" => rev}) when is_binary(id) and is_integer(rev),
    do: true

  defp v2_item?(_), do: false

  defp v2_page(endpoint, identity, limit, body, first_rev, page_items, items) do
    items =
      Enum.reduce(page_items, items, fn %{"item_id" => id, "rev" => item_rev} = item, acc ->
        case acc do
          %{^id => %{"rev" => held}} when held >= item_rev -> acc
          _ -> Map.put(acc, id, item)
        end
      end)

    case body["next_page"] do
      next when is_binary(next) -> v2_inbox(endpoint, identity, limit, next, first_rev, items)
      _ -> {:ok, first_rev, items}
    end
  end

  # The item the sender's message became, as delivered to this device: a fresh
  # read, matched by sender and the message's own message_id. As today, an item
  # this device originated (no `as`) is not answered; the server authorizes
  # everything else.
  defp v2_resolve(directory, endpoint, %{"sender_id" => sender, "message_id" => id}) do
    with {:ok, %{"items" => items}} <- v2_read(directory, endpoint) do
      case Enum.find(items, fn item ->
             item["sender"] == sender and get_in(item, ["message", "message_id"]) == id and
               Map.has_key?(item, "as")
           end) do
        %{"item_id" => item_id, "request_revision" => revision} = item
        when is_binary(item_id) and is_integer(revision) ->
          {:ok, item}

        _ ->
          {:error, :not_in_inbox}
      end
    end
  end

  defp enrolled_identity(directory, endpoint) do
    with :ok <- existing_private_directory(directory),
         {:ok, saved} <- read_json(Path.join(directory, "server.json")),
         {:ok, ca} <- File.read(endpoint.ca_file),
         true <-
           saved["name"] == endpoint.name and
             saved["address"] == address_string(endpoint.address) and
             saved["ca_sha256"] == hex(:crypto.hash(:sha256, ca)),
         {:ok, result} <- read_json(Path.join(directory, "enrollment.json")),
         expected when is_binary(expected) <- result["device_id"],
         {:ok, pem} <- File.read(Path.join(directory, "identity.pem")),
         [{:Certificate, der, :not_encrypted}] <- :public_key.pem_decode(pem),
         {:ok, ^expected} <- DeviceIdentity.from_certificate(der),
         {:ok, key_stat} <- File.stat(Path.join(directory, "identity.key")),
         true <- (key_stat.mode &&& 0o077) == 0 do
      {:ok, expected}
    else
      _ -> {:error, :identity_or_server_mismatch}
    end
  rescue
    _ -> {:error, :identity_or_server_mismatch}
  end

  defp address_string(address) when is_tuple(address),
    do: address |> :inet.ntoa() |> List.to_string()

  defp address_string(address) when is_list(address), do: List.to_string(address)

  # Local state the tool wrote itself: no server frame limit applies (review
  # C6-2); the store holds every item the person can see.
  defp read_local_json(path) do
    with {:ok, contents} <- File.read(path), do: JSON.decode_line(contents <> "\n")
  end
end
