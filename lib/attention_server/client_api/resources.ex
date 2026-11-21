defmodule AttentionServer.ClientAPI.Resources do
  @moduledoc """
  The client API v2 resources (docs/client-api-v2.md §3): Item, Entry,
  Settings and Counts, projected read-only from the engine.

  Projections are person-level and the same for every client of the person.
  The few members that differ by viewer travel under the internal `"_viewer"`
  member and are applied by `render/3` for one principal. The change log stores
  person-level payloads, and every read and feed event goes through `render/3`.

  A projected resource has no `"rev"`. The caller stamps it with `stamp/2`
  from the change log before rendering.

  Identifiers are opaque to clients:

    * `item_id` is `"i"` followed by the item's acceptance sequence, which is
      unique per person and never changes;
    * `entry_id` is `"e"` followed by the conversation fact's sequence, which is
      also the entry's `position`;
    * `file_id` is `"fu"` and the upload id for uploaded files, or `"fi"`/`"fe"`
      followed by the item revision or entry and the file's index for files
      carried inline.

  This module reads engine state only. It never changes it.
  """

  alias AttentionServer.{BounceDecision, Conversation, Engine, LargeData, Message}

  @viewer "_viewer"
  @sources "_sources"
  @digits "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
  @base byte_size(@digits)

  @type key :: {person :: String.t(), sender :: String.t(), message_id :: String.t()}
  @type principal :: %{
          person: String.t(),
          key: String.t(),
          kind: :device | :browser,
          senders: MapSet.t()
        }

  ## Identifiers

  @doc "The opaque `item_id` of a message record."
  def item_id(%{accepted_seq: seq}) when is_integer(seq) and seq > 0,
    do: "i" <> Integer.to_string(seq)

  @doc "The `item_id` of the person's message `{sender, message_id}`, if it exists."
  def item_id(%Engine{} = engine, {_person, _sender, _id} = key) do
    case Map.get(engine.messages, key) do
      %{accepted_seq: _} = record -> {:ok, item_id(record)}
      _ -> {:error, :not_found}
    end
  end

  @doc "Resolves an `item_id` to the engine key of one of the person's items."
  def item_key(%Engine{} = engine, person, "i" <> digits) when is_binary(person) do
    with {:ok, seq} <- parse_seq(digits),
         %{key: {^person, _, _} = key} <- Map.get(engine.events, {person, seq}),
         %{accepted_seq: ^seq} <- Map.get(engine.messages, key) do
      {:ok, key}
    else
      _ -> {:error, :not_found}
    end
  end

  def item_key(_engine, _person, _item_id), do: {:error, :not_found}

  @doc "The opaque `entry_id` of a conversation fact."
  def entry_id(seq) when is_integer(seq) and seq > 0, do: "e" <> Integer.to_string(seq)

  @doc "The conversation sequence an `entry_id` names."
  def entry_seq("e" <> digits), do: parse_seq(digits)
  def entry_seq(_), do: {:error, :not_found}

  @doc """
  Resolves a `file_id` to the person's file: `{:upload, upload_id}`, or
  `{:inline, file}` with the stored inline file map.
  """
  def file_source(%Engine{} = engine, person, "fu" <> upload_id) when is_binary(person) do
    case Map.get(engine.uploads, upload_id) do
      %{person: ^person} -> {:ok, {:upload, upload_id}}
      _ -> {:error, :not_found}
    end
  end

  def file_source(%Engine{} = engine, person, "fi" <> rest) when is_binary(person) do
    with [accepted, revision, index] <- String.split(rest, "."),
         {:ok, accepted} <- parse_seq(accepted),
         {:ok, revision} <- parse_seq(revision),
         {:ok, index} <- parse_index(index),
         {:ok, key} <- item_key(engine, person, "i" <> Integer.to_string(accepted)),
         %{files_by_revision: by_revision} <- Map.get(engine.messages, key),
         %{} = file <- by_revision |> Map.get(revision, []) |> Enum.at(index) do
      file_source_of(file)
    else
      _ -> {:error, :not_found}
    end
  end

  def file_source(%Engine{} = engine, person, "fe" <> rest) when is_binary(person) do
    with [seq, index] <- String.split(rest, "."),
         {:ok, seq} <- parse_seq(seq),
         {:ok, index} <- parse_index(index),
         %{key: {^person, _, _}, payload: %{"kind" => kind} = payload} <-
           Map.get(engine.conversation_events, {person, seq}),
         true <- kind in ~w(discussion reply revision),
         %{} = file <- payload |> Map.get("files", []) |> Enum.at(index) do
      file_source_of(file)
    else
      _ -> {:error, :not_found}
    end
  end

  def file_source(_engine, _person, _file_id), do: {:error, :not_found}

  defp file_source_of(%{"tier" => "upload", "upload_id" => id}), do: {:ok, {:upload, id}}
  defp file_source_of(%{"tier" => "inline"} = file), do: {:ok, {:inline, file}}
  defp file_source_of(_), do: {:error, :not_found}

  defp parse_seq(digits) do
    case parse_index(digits) do
      {:ok, n} when n > 0 -> {:ok, n}
      _ -> {:error, :not_found}
    end
  end

  # Canonical decimal only, so each number has exactly one identifier.
  defp parse_index(digits) when is_binary(digits) and byte_size(digits) in 1..20 do
    with {n, ""} <- Integer.parse(digits),
         true <- n >= 0 and Integer.to_string(n) == digits do
      {:ok, n}
    else
      _ -> {:error, :not_found}
    end
  end

  defp parse_index(_), do: {:error, :not_found}

  ## Principals

  @doc """
  The principal a key authenticates, with what `render/3` needs: the device or
  browser key, its person, and the senders this client receives as messages
  rather than knocks.
  """
  def principal(%Engine{} = engine, key) when is_binary(key) do
    with {:ok, person} <- Engine.authorized_person(engine, key) do
      kind = if Map.has_key?(engine.devices, key), do: :device, else: :browser

      senders =
        case Map.get(engine.subscriptions, {person, key}) do
          %{senders: %MapSet{} = senders} -> senders
          _ -> MapSet.new()
        end

      {:ok, %{person: person, key: key, kind: kind, senders: senders}}
    end
  end

  ## Items

  @doc "Every engine key of the person's items, in no particular order."
  def item_keys(%Engine{} = engine, person) do
    for {{^person, _, _} = key, _record} <- engine.messages, do: key
  end

  @doc """
  The person-level Item for one engine key, without `rev`. `rank` is the item's
  Rank from `ranks/4`.
  """
  def item(%Engine{} = engine, {person, sender, _id} = key, %{"key" => _} = rank) do
    record = Map.fetch!(engine.messages, key)
    summary = Map.get(engine.route_b_index.conversation_summary_by_item, key, %{})
    {reply, rank} = Map.pop(rank, "_reply")

    %{
      "item_id" => item_id(record),
      "sender" => sender,
      "request_revision" => record.revision,
      "message" => record.message,
      "files" => item_files(record),
      "seen" => seen?(engine, key, record),
      "chat" => chat(engine, key, record, summary, reply),
      "terminal" => terminal(record),
      "rank" => rank,
      "created_at_ms" => Map.get(record, :accepted_at),
      @viewer => %{"origin" => record.origin}
    }
    |> put_present("availability", item_availability(engine, person, record, summary))
    |> put_present("result", origin_result(record))
  end

  @doc """
  One request revision of an item, 1 to current (contract rulings round 4,
  item 7): `{request_revision, message, files}`, the message passed through
  unchanged.
  """
  def revision(%Engine{} = engine, key, n) when is_integer(n) do
    with %{revisions: %{^n => message}} = record <- Map.get(engine.messages, key) do
      files = record |> Map.get(:files_by_revision, %{}) |> Map.get(n, [])

      {:ok,
       %{
         "request_revision" => n,
         "message" => message,
         "files" => file_refs(files, "fi#{record.accepted_seq}.#{n}.")
       }}
    else
      _ -> {:error, :not_found}
    end
  end

  @doc "The person's Counts."
  def counts(%Engine{} = engine, person),
    do: %{"unresolved" => Map.get(engine.route_b_index.unresolved_count_by_person, person, 0)}

  # Today's `opened`: Seen for the current substantive revision.
  defp seen?(engine, {person, sender, id} = key, record) do
    substantive =
      case Map.get(engine.bounce_revisions, key) do
        %{raw_to_substantive: raw_to_substantive} = state ->
          Map.get(
            raw_to_substantive,
            record.revision,
            Map.get(state, :current_substantive_revision, record.revision)
          )

        _ ->
          record.revision
      end

    MapSet.member?(engine.opened_revisions, {person, sender, id, substantive})
  end

  defp chat(engine, {person, _, _} = key, record, summary, reply) do
    read_through = Map.get(engine.chat_read_positions, key, 0)

    # Agent replies after the read position, as today's reply rows count them.
    unread =
      engine.route_b_index.conversations_by_item
      |> Map.get(key, :gb_trees.empty())
      |> :gb_trees.values()
      |> Enum.count(fn seq ->
        seq > read_through and
          match?(
            %{payload: %{"kind" => "reply", "author" => "origin"}},
            Map.get(engine.conversation_events, {person, seq})
          )
      end)

    chat = %{
      "available" => is_binary(Map.get(record, :session_handle)),
      "started" => Map.get(summary, :chat_started, false),
      "read_position" => optional_entry_id(read_through),
      "last_origin_reply" => optional_entry_id(Map.get(summary, :latest_origin_reply_seq)),
      "unread_replies" => unread
    }

    # Today's chat-reply row, while it has unread replies (contract rounds 5
    # and 6): its key in Rank.key's space, and its group in free order.
    case reply do
      %{"key" => reply_key, "group" => group} when unread > 0 ->
        Map.merge(chat, %{"reply_rank_key" => reply_key, "reply_group" => group})

      _ ->
        chat
    end
  end

  # The current revision's files, plus files of earlier revisions that the
  # current answer screen names (a revision keeps showing revision 1's images).
  defp item_files(record) do
    by_revision = Map.get(record, :files_by_revision, %{})
    current = Map.get(by_revision, record.revision, [])
    own = file_refs(current, "fi#{record.accepted_seq}.#{record.revision}.")
    present = MapSet.new(current, & &1["name"])

    referenced =
      record.message
      |> Map.get("answer_ui")
      |> screen_file_names()
      |> Enum.reject(&MapSet.member?(present, &1))

    earlier =
      Enum.flat_map(Enum.uniq(referenced), fn name ->
        (record.revision - 1)..1//-1
        |> Enum.find_value([], fn n ->
          files = Map.get(by_revision, n, [])

          case Enum.find_index(files, &(&1["name"] == name)) do
            nil ->
              nil

            index ->
              [file_ref(Enum.at(files, index), "fi#{record.accepted_seq}.#{n}.", index)]
          end
        end)
      end)

    own ++ earlier
  end

  # The names a UI/1 screen's image primitives use ("file": name).
  defp screen_file_names(%{} = value) when not is_struct(value) do
    own =
      case value do
        %{"file" => name} when is_binary(name) -> [name]
        _ -> []
      end

    own ++ Enum.flat_map(Map.values(value), &screen_file_names/1)
  end

  defp screen_file_names(values) when is_list(values),
    do: Enum.flat_map(values, &screen_file_names/1)

  defp screen_file_names(_), do: []

  defp optional_entry_id(seq) when is_integer(seq) and seq > 0, do: entry_id(seq)
  defp optional_entry_id(_), do: nil

  defp item_availability(engine, person, record, summary) do
    if is_binary(Map.get(record, :session_handle)) do
      payload =
        case Map.get(summary, :availability) do
          %{payload: payload} ->
            payload

          _ ->
            engine.events |> Map.get({person, record.accepted_seq}, %{}) |> Map.get(:payload, %{})
        end

      availability(payload)
    end
  end

  defp availability(%{"availability" => state} = payload)
       when state in ~w(listening not_listening gone unsupported) do
    put_present(%{"state" => state}, "last_detached_at_ms", time_ms(payload["last_detached_at"]))
  end

  defp availability(_), do: nil

  defp terminal(%{terminal: nil}), do: nil

  defp terminal(%{terminal: %{state: state, at: at} = terminal} = record) do
    %{
      "state" => state,
      "at_ms" => at,
      "by_this_person" => Conversation.terminal_author(record, terminal) == "person"
    }
    |> put_present("answer", answer_outcome(record, terminal))
  end

  # Today's person-facing answer_outcome (contract rulings round 4, item 3).
  # The origin-only raw answer is never included.
  defp answer_outcome(record, terminal) do
    case {Map.get(terminal, :kind), terminal.by, terminal.answer_present, terminal.answer} do
      {"no_comment", _, _, _} ->
        %{"kind" => "no_comment"}

      {_, "default", true, answer} ->
        accepted_at = Map.get(record, :revision_accepted_at, Map.get(record, :accepted_at))

        %{"kind" => "default", "value" => answer}
        |> put_present(
          "text",
          case Message.default_answer(record.message, accepted_at) do
            %{text: text} -> text
            _ -> nil
          end
        )

      {_, "answer", true, nil} ->
        %{"kind" => "seen"}

      {_, "answer", true, answer} when is_binary(answer) ->
        %{"kind" => "reply", "text" => answer}

      {_, "answer", true, %LargeData.Ref{} = answer} ->
        %{"kind" => "reply", "text" => answer}

      _ ->
        nil
    end
  end

  defp origin_result(%{origin_result: %{status: status, revision: revision} = result}) do
    put_present(%{"status" => status, "revision" => revision}, "result_ref", result[:result_ref])
  end

  defp origin_result(_), do: nil

  defp file_refs(files, prefix) when is_list(files) do
    files
    |> Enum.with_index()
    |> Enum.map(fn {file, index} -> file_ref(file, prefix, index) end)
  end

  defp file_refs(_files, _prefix), do: []

  defp file_ref(file, prefix, index) do
    file_id =
      case file do
        %{"tier" => "upload", "upload_id" => id} -> "fu" <> id
        _ -> prefix <> Integer.to_string(index)
      end

    %{
      "file_id" => file_id,
      "name" => file["name"],
      "size" => file["size"],
      "sha256" => file["sha256"]
    }
  end

  ## Rank

  @doc """
  Every item's Rank, keyed by `item_id`, in today's inbox order: the priority
  view's `list_items` (`Placement.list_items/4`), which holds the current and
  resolved sections, free-order groups and chat-reply rows. Items the view
  does not list (none yet, or a progress message) follow: open ones, then
  resolved ones, newest first.

  A Rank has `key` and `section`, plus `group` for current items in free
  order, and `score`/`due_at_ms` from the view. An item with a chat-reply row
  carries that row's key and group under the internal `"_reply"` member, which
  `item/3` turns into `Chat.reply_rank_key` and `Chat.reply_group`.

  `previous` maps rank unit ids (`item_id`, or `"r"` and the `item_id` for a
  reply row) to the keys last given to clients. Units keep their previous key
  wherever the order allows, so moving one rewrites only its key
  (`rank_keys/2`).
  """
  def ranks(%Engine{} = engine, person, view, previous \\ %{}) when is_map(previous) do
    entries = view_entries(view)
    listed = listed_units(engine, person, view)
    listed_items = for {:item, id, _key, _section, _group} <- listed, into: MapSet.new(), do: id

    unlisted =
      engine
      |> item_keys(person)
      |> Enum.map(&{&1, Map.fetch!(engine.messages, &1)})
      |> Enum.reject(fn {_key, record} -> MapSet.member?(listed_items, item_id(record)) end)
      |> Enum.sort_by(fn {_key, record} ->
        {not is_nil(record.terminal), -record.accepted_seq}
      end)
      |> Enum.map(fn {key, record} ->
        section = if is_nil(record.terminal), do: "current", else: "resolved"
        {:item, item_id(record), key, section, nil}
      end)

    units = listed ++ unlisted
    keys = rank_keys(previous, Enum.map(units, &unit_id/1))

    Enum.reduce(units, %{}, fn
      {:item, id, {_, sender, message_id}, section, group}, acc ->
        rank =
          %{"key" => Map.fetch!(keys, id), "section" => section}
          |> put_present("group", group)
          |> put_view_fields(Map.get(entries, {sender, message_id}, %{}))

        Map.update(acc, id, rank, &Map.merge(rank, Map.take(&1, ["_reply"])))

      {:reply, id, _key, _section, group} = unit, acc ->
        reply = %{"_reply" => %{"key" => Map.fetch!(keys, unit_id(unit)), "group" => group}}
        Map.update(acc, id, reply, &Map.merge(&1, reply))
    end)
  end

  defp unit_id({:item, id, _, _, _}), do: id
  defp unit_id({:reply, id, _, _, _}), do: "r" <> id

  # Today's list rows, in order, as rank units. Reply rows count only while
  # they have unread agent replies; read ones are history that clients draw
  # from the item itself.
  defp listed_units(engine, person, %{list_items: rows}) when is_list(rows) do
    {units, _seen} =
      Enum.reduce(rows, {[], MapSet.new()}, fn row, {units, seen} ->
        with %{"sender_id" => sender, "message_id" => message_id} <- row,
             key = {person, sender, message_id},
             %{accepted_seq: _} = record <- Map.get(engine.messages, key),
             {:ok, unit} <- list_unit(row, item_id(record), key, record),
             false <- MapSet.member?(seen, unit_id(unit)) do
          {[unit | units], MapSet.put(seen, unit_id(unit))}
        else
          _ -> {units, seen}
        end
      end)

    Enum.reverse(units)
  end

  defp listed_units(_engine, _person, _view), do: []

  defp list_unit(%{"kind" => "message"} = row, id, key, record) do
    {section, group} =
      case row do
        %{"free_section" => %{"state" => "active", "group" => group}} -> {"current", group}
        %{"free_section" => %{"state" => "history"}} -> {"resolved", nil}
        _ -> {if(is_nil(record.terminal), do: "current", else: "resolved"), nil}
      end

    group = if group in ~w(common earlier_messages), do: group, else: nil
    {:ok, {:item, id, key, section, group}}
  end

  defp list_unit(%{"kind" => "reply", "unseen_count" => unseen} = row, id, key, _record)
       when is_integer(unseen) and unseen > 0 do
    group =
      case row do
        %{"free_section" => %{"group" => group}}
        when group in ~w(earlier_chat_replies earlier_chat_replies_unknown) ->
          group

        _ ->
          nil
      end

    {:ok, {:reply, id, key, "current", group}}
  end

  defp list_unit(_row, _id, _key, _record), do: :skip

  defp view_entries(%{message_entries_by_identity: entries}) when is_map(entries), do: entries
  defp view_entries(_), do: %{}

  defp put_view_fields(rank, entry) do
    rank
    |> put_present("score", if(is_number(entry["score"]), do: entry["score"]))
    |> put_present("due_at_ms", time_ms(entry["due_at"]))
  end

  @doc """
  Assigns rank keys to `ordered_ids` (first first). Keys sort
  ascending, bytewise. The longest run of ids whose `previous` keys are already
  in order keeps them. Every other id gets a new key between its kept
  neighbours, so a single move rewrites a single key.
  """
  def rank_keys(previous, ordered_ids) when is_map(previous) and is_list(ordered_ids) do
    candidates =
      ordered_ids
      |> Enum.with_index()
      |> Enum.flat_map(fn {id, index} ->
        case Map.get(previous, id) do
          "1" <> fraction = key when fraction != "" ->
            if valid_fraction?(fraction), do: [{index, key}], else: []

          _ ->
            []
        end
      end)

    kept = candidates |> longest_increasing() |> Map.new()

    {assigned, pending, low} =
      ordered_ids
      |> Enum.with_index()
      |> Enum.reduce({[], [], ""}, fn {id, index}, {assigned, pending, low} ->
        case Map.get(kept, index) do
          nil ->
            {assigned, [id | pending], low}

          "1" <> fraction = key ->
            fresh = spread(low, fraction, Enum.reverse(pending))
            {[{id, key} | fresh ++ assigned], [], fraction}
        end
      end)

    Map.new(spread(low, nil, Enum.reverse(pending)) ++ assigned)
  end

  defp valid_fraction?(fraction) do
    not String.ends_with?(fraction, "0") and
      fraction |> :binary.bin_to_list() |> Enum.all?(&(digit_value(&1) != nil))
  end

  # Patience sorting: the longest strictly increasing subsequence by key.
  defp longest_increasing([]), do: []

  defp longest_increasing(pairs) do
    {tails, links} =
      Enum.reduce(pairs, {{}, %{}}, fn {_index, key} = pair, {tails, links} ->
        slot = lower_bound(tails, key, 0, tuple_size(tails))
        previous = if slot > 0, do: elem(tails, slot - 1), else: nil
        links = Map.put(links, pair, previous)

        tails =
          if slot == tuple_size(tails),
            do: :erlang.append_element(tails, pair),
            else: put_elem(tails, slot, pair)

        {tails, links}
      end)

    walk(elem(tails, tuple_size(tails) - 1), links, [])
  end

  defp lower_bound(_tails, _key, low, high) when low >= high, do: low

  defp lower_bound(tails, key, low, high) do
    middle = div(low + high, 2)
    {_index, middle_key} = elem(tails, middle)

    if middle_key < key,
      do: lower_bound(tails, key, middle + 1, high),
      else: lower_bound(tails, key, low, middle)
  end

  defp walk(nil, _links, acc), do: acc
  defp walk(pair, links, acc), do: walk(Map.fetch!(links, pair), links, [pair | acc])

  # Evenly bisected fractions strictly between low and high (nil = the end).
  defp spread(_low, _high, []), do: []

  defp spread(low, high, ids) do
    half = div(length(ids), 2)
    {left, [id | right]} = Enum.split(ids, half)
    middle = midpoint(low, high)
    [{id, "1" <> middle} | spread(low, middle, left) ++ spread(middle, high, right)]
  end

  # The fractional-indexing midpoint: a string strictly between `low` and
  # `high` (nil means 1), with no trailing zero digit.
  @doc false
  def midpoint(low, high) when is_binary(low) and (is_nil(high) or low < high) do
    case high && common_prefix(low, high, 0) do
      n when is_integer(n) and n > 0 ->
        binary_part(high, 0, n) <>
          midpoint(
            binary_part(low, min(n, byte_size(low)), max(byte_size(low) - n, 0)),
            binary_part(high, n, byte_size(high) - n)
          )

      _ ->
        low_digit = if low == "", do: 0, else: digit_value(:binary.first(low))
        high_digit = if high, do: digit_value(:binary.first(high)), else: @base

        cond do
          high_digit - low_digit > 1 ->
            digit(div(low_digit + high_digit + 1, 2))

          high && byte_size(high) > 1 ->
            binary_part(high, 0, 1)

          true ->
            rest = if low == "", do: "", else: binary_part(low, 1, byte_size(low) - 1)
            digit(low_digit) <> midpoint(rest, nil)
        end
    end
  end

  defp common_prefix(low, high, n) do
    low_char = if n < byte_size(low), do: :binary.at(low, n), else: ?0

    if n < byte_size(high) and :binary.at(high, n) == low_char,
      do: common_prefix(low, high, n + 1),
      else: n
  end

  defp digit(value), do: binary_part(@digits, value, 1)

  defp digit_value(char) do
    case :binary.match(@digits, <<char>>) do
      {index, 1} -> index
      :nomatch -> nil
    end
  end

  ## Entries

  @doc """
  The person-level entries of one item, without `rev`, ascending by
  `position`. Receipts and acknowledgments are not entries; they show on the
  entry they refer to as `delivery`.
  """
  def entries(%Engine{} = engine, {person, _, _} = key) do
    record = Map.fetch!(engine.messages, key)
    item_id = item_id(record)

    facts =
      engine.route_b_index.conversations_by_item
      |> Map.get(key, :gb_trees.empty())
      |> :gb_trees.values()
      |> Enum.reverse()
      |> Enum.map(&{&1, Map.fetch!(engine.conversation_events, {person, &1}).payload})

    by_event_id =
      for {seq, %{"kind" => kind, "event_id" => event_id}} <- facts,
          kind in ~w(discussion reply revision),
          is_binary(event_id),
          into: %{},
          do: {event_id, seq}

    # Receipt and acknowledgment facts mark the entry they refer to. Each entry
    # records the facts it is made of, so its rev can be the newest of theirs.
    folded =
      Enum.reduce(facts, %{}, fn {seq, payload}, acc ->
        with {field, event_id} <- delivery_effect(payload),
             target when is_integer(target) <- Map.get(by_event_id, event_id) do
          Map.update(acc, target, {%{field => true}, [seq]}, fn {marks, seqs} ->
            {Map.put(marks, field, true), [seq | seqs]}
          end)
        else
          _ -> acc
        end
      end)

    Enum.flat_map(facts, fn {seq, payload} ->
      {marks, folded_seqs} = Map.get(folded, seq, {%{}, []})

      case entry(item_id, record, seq, payload, by_event_id) do
        nil ->
          []

        %{"author" => "person"} = entry ->
          entry
          |> Map.put("delivery", %{
            "delivered" => Map.get(marks, "delivered", false),
            "acted" => Map.get(marks, "acted", false)
          })
          |> Map.put(@sources, Enum.sort([seq | folded_seqs]))
          |> List.wrap()

        entry ->
          [Map.put(entry, @sources, Enum.sort([seq | folded_seqs]))]
      end
    end)
  end

  @doc """
  The conversation sequences an entry is made of: its own fact and the
  receipt and acknowledgment facts folded into it. Its rev is the newest of
  their change-log revs.
  """
  def entry_sources(%{@sources => seqs}), do: seqs

  @doc """
  The entry a conversation fact shows up in: the fact's own entry, the entry
  a receipt or acknowledgment fact marks, or nil for facts that are not part
  of any entry (read positions, file reports, results).
  """
  def entry_for_fact(%Engine{} = engine, person, seq) do
    case Map.get(engine.conversation_events, {person, seq}) do
      %{key: {^person, _, _} = key} ->
        if Map.has_key?(engine.messages, key),
          do: engine |> entries(key) |> Enum.find(&(seq in &1[@sources])),
          else: nil

      _ ->
        nil
    end
  end

  defp delivery_effect(%{"kind" => "receipt", "delivered" => true, "event_id" => id}),
    do: {"delivered", id}

  defp delivery_effect(%{"kind" => "delivery_ack", "stage" => "ack1", "event_id" => id}),
    do: {"delivered", id}

  defp delivery_effect(%{"kind" => "delivery_ack", "stage" => "ack2", "event_id" => id}),
    do: {"acted", id}

  defp delivery_effect(%{"kind" => "acted", "event_id" => id}), do: {"acted", id}
  defp delivery_effect(_), do: nil

  defp entry(item_id, record, seq, %{"kind" => kind} = payload, by_event_id) do
    base = %{
      "entry_id" => entry_id(seq),
      "item_id" => item_id,
      "position" => seq,
      "request_revision" => Map.get(payload, "request_revision") || record.revision
    }

    case kind do
      k when k in ~w(discussion reply) ->
        author = if k == "discussion", do: "person", else: Map.get(payload, "author", "origin")

        base
        |> Map.merge(%{"kind" => "reply", "author" => author})
        |> put_present("text", payload["text"])
        |> put_present("files", entry_files(payload, seq))
        |> put_present("in_reply_to", reply_target(payload, by_event_id))

      "revision" ->
        base
        |> Map.merge(%{"kind" => "revision", "author" => "origin"})
        |> put_present("files", entry_files(payload, seq))

      "availability" ->
        base
        |> Map.merge(%{"kind" => "availability", "author" => "server"})
        |> put_present("availability", availability(payload))

      "retraction" ->
        base
        |> Map.merge(%{"kind" => "lifecycle", "author" => "origin"})
        |> Map.put("terminal", %{"state" => "retracted", "at_ms" => payload["retracted_at"]})

      _ ->
        nil
    end
  end

  defp entry_files(%{"files" => [_ | _] = files}, seq), do: file_refs(files, "fe#{seq}.")
  defp entry_files(_payload, _seq), do: nil

  defp reply_target(%{"in_reply_to" => event_id}, by_event_id) when is_binary(event_id) do
    case Map.get(by_event_id, event_id) do
      nil -> nil
      seq -> entry_id(seq)
    end
  end

  defp reply_target(_payload, _by_event_id), do: nil

  ## Settings

  @doc """
  The person-level Settings, without `rev`. Push binding status is per device
  and is chosen by `render/3`.
  """
  def settings(%Engine{} = engine, person) do
    push =
      for {{^person, device}, binding} <- engine.push_bindings,
          into: %{},
          do: {device, push_status(binding)}

    %{
      "priority_levels" => BounceDecision.levels(engine, person),
      @viewer => %{"push" => push}
    }
  end

  defp push_status(binding) do
    %{"state" => binding.state, "revision" => binding.revision}
    |> put_present("environment", if(binding.active, do: environment(binding.profile_id)))
    |> put_present("last_attempt_at_ms", time_ms(Map.get(binding, :last_attempt_at)))
    |> put_present("next_attempt_at_ms", time_ms(Map.get(binding, :next_at)))
  end

  defp environment("harklight-production"), do: "production"
  defp environment("harklight-sandbox"), do: "sandbox"
  defp environment(_), do: nil

  @unconfigured_push %{"state" => "unconfigured", "revision" => 0}

  ## Rendering

  @doc "Sets the resource `rev` from the change log on a projected resource."
  def stamp(payload, rev) when is_map(payload) and is_integer(rev) and rev > 0,
    do: Map.put(payload, "rev", rev)

  @doc """
  Renders a person-level payload for one principal: the wire form of an Item,
  Entry, Settings, Counts or removal.
  """
  def render(:item, %{@viewer => %{"origin" => origin}} = item, %{key: key} = principal) do
    item = Map.delete(item, @viewer)

    if key == origin do
      item
    else
      sender = Map.fetch!(item, "sender")

      Map.put(
        item,
        "as",
        if(MapSet.member?(principal.senders, sender), do: "message", else: "knock")
      )
    end
  end

  def render(:settings, %{@viewer => %{"push" => push}} = settings, %{key: key, kind: kind}) do
    status =
      if kind == :device, do: Map.get(push, key, @unconfigured_push), else: @unconfigured_push

    settings
    |> Map.delete(@viewer)
    |> Map.put("push", status)
  end

  def render(:entry, entry, _principal), do: Map.delete(entry, @sources)

  def render(kind, payload, _principal) when kind in [:counts, :item_removed],
    do: payload

  ## Helpers

  defp time_ms(value) when is_integer(value) and value >= 0, do: value
  defp time_ms(_), do: nil

  defp put_present(map, _key, nil), do: map
  defp put_present(map, key, value), do: Map.put(map, key, value)
end
