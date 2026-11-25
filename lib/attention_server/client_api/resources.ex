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

  alias AttentionServer.ClientAPI.Projection

  alias AttentionServer.{BounceDecision, Conversation, Engine, LargeData, Message, ReplyPriority}

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

  @doc """
  The `item_id` an item had, for a removal: from its record, or when the
  record is gone, from the delivery event at its acceptance sequence.
  """
  def removed_item_id(%Engine{} = engine, {person, _, _} = key) do
    case item_id(engine, key) do
      {:ok, item_id} ->
        {:ok, item_id}

      _ ->
        high = Map.get(engine.persons, person, 0)

        Enum.find_value(1..max(high, 0)//1, {:error, :not_found}, fn seq ->
          case Map.get(engine.events, {person, seq}) do
            %{kind: :delivery, key: ^key} -> {:ok, "i" <> Integer.to_string(seq)}
            _ -> nil
          end
        end)
    end
  end

  defp item_projection_key(engine, key) do
    case Map.get(engine.messages, key) do
      %{accepted_seq: seq} when is_integer(seq) and seq > 0 -> {:item, "i" <> Integer.to_string(seq)}
      _ -> :item
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

  @doc false
  def person_item_key(engine, person, id) do
    with {:ok, key} <- item_key(engine, person, id),
         true <- BounceDecision.person_visible?(engine.messages[key]) do
      {:ok, key}
    else
      _ -> {:error, :not_found}
    end
  end

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
  #
  # Each item is the delivery event at its acceptance sequence, so the person's
  # own sequence is walked instead of every person's messages.
  def item_keys(%Engine{} = engine, person) do
    high =
      case Map.get(engine.persons, person) do
        high when is_integer(high) and high > 0 -> high
        _ -> 0
      end

    for seq <- 1..high//1,
        %{key: {^person, _, _} = key} <- [Map.get(engine.events, {person, seq})],
        %{accepted_seq: ^seq} = record <- [Map.get(engine.messages, key)],
        BounceDecision.person_visible?(record),
        do: key
  end

  @doc """
  The person-level Item for one engine key, without `rev`. `rank` is the item's
  Rank from `ranks/4`.
  """
  def item(%Engine{} = engine, key, %{"key" => _} = rank) do
    Projection.run(item_projection_key(engine, key), fn ->
      project_item(engine, key, rank)
    end)
  end

  defp project_item(engine, {person, sender, _id} = key, rank) do
    record = Map.fetch!(engine.messages, key)
    revision = Map.get(record, :revision, 1)
    summary = Map.get(engine.route_b_index.conversation_summary_by_item, key, %{})
    {reply, rank} = Map.pop(rank, "_reply")
    {thread, rank} = Map.pop(rank, "_thread")

    %{
      "item_id" => item_id(record),
      "sender" => sender,
      "request_revision" => revision,
      "message" => record.message,
      "files" => engine |> typed_files(item_files(record, revision)),
      "seen" => seen?(engine, key, record),
      "chat" => chat(engine, key, record, summary, reply),
      "terminal" => terminal(person, record),
      "rank" => rank,
      @viewer => %{"origin" => record.origin}
    }
    # Round 8: arrival (today's free-order counter) orders the new-messages
    # area; created_at_ms is display only and absent when the record has none.
    |> put_present("created_at_ms", time_ms(Map.get(record, :accepted_at)))
    |> put_present("arrival", positive(Map.get(record, :free_order)))
    # Round 9: the effective group thread, so clients form today's thread rows
    # from adjacent items with the same (sender, thread).
    |> put_present("thread", thread)
    |> put_present("availability", AttentionServer.AgentAPI.FeedActivity.item_availability(engine, key))
    |> put_present("result", origin_result(record))
  end

  @doc """
  One request revision of an item, 1 to current (contract rulings round 4,
  item 7): `{request_revision, message, files}`, the message passed through
  unchanged.
  """
  def revision(%Engine{} = engine, key, n) when is_integer(n) do
    Projection.run(item_projection_key(engine, key), fn -> project_revision(engine, key, n) end)
  end

  defp project_revision(engine, key, n) do
    with %{message: current} = record <- Map.get(engine.messages, key),
         message when is_map(message) <-
           Map.get(Map.get(record, :revisions, %{1 => current}), n) do
      files = record |> Map.get(:files_by_revision, %{}) |> Map.get(n, [])

      {:ok,
       %{
         "request_revision" => n,
         "message" => message,
         "files" => typed_files(engine, file_refs(files, "fi#{record.accepted_seq}.#{n}."))
       }}
    else
      _ -> {:error, :not_found}
    end
  end

  @doc "The person's Counts."
  def counts(%Engine{} = engine, person),
    do: %{"unresolved" => Map.get(engine.route_b_index.unresolved_count_by_person, person, 0)}

  # Today's `opened`: Seen for the current substantive revision.
  def seen?(engine, {person, sender, id} = key, record) do
    revision = Map.get(record, :revision, 1)

    substantive =
      case Map.get(engine.bounce_revisions, key) do
        %{raw_to_substantive: raw_to_substantive} = state ->
          Map.get(
            raw_to_substantive,
            revision,
            Map.get(state, :current_substantive_revision, revision)
          )

        _ ->
          revision
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

    # Today's chat-reply row, whenever the item has an agent reply (contract
    # round 7): its key in Rank.key's space, its group in free order, and its
    # priority as today's reply rows show it.
    case reply do
      %{"key" => reply_key, "group" => group, "priority" => priority} ->
        Map.merge(chat, %{
          "reply_rank_key" => reply_key,
          "reply_group" => group,
          "reply_priority" => priority
        })

      _ ->
        chat
    end
  end

  # The current revision's files, plus files of earlier revisions that the
  # current answer screen names (a revision keeps showing revision 1's images).
  defp item_files(record, revision) do
    by_revision = Map.get(record, :files_by_revision, %{})
    current = Map.get(by_revision, revision, [])
    own = file_refs(current, "fi#{record.accepted_seq}.#{revision}.")
    present = MapSet.new(current, & &1["name"])

    referenced =
      record.message
      |> Map.get("answer_ui")
      |> screen_file_names()
      |> Enum.reject(&MapSet.member?(present, &1))

    earlier =
      Enum.flat_map(Enum.uniq(referenced), fn name ->
        (revision - 1)..1//-1
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

  defp availability(%{"availability" => state} = payload)
       when state in ~w(listening not_listening gone unsupported) do
    put_present(%{"state" => state}, "last_detached_at_ms", time_ms(payload["last_detached_at"]))
  end

  defp availability(_), do: nil

  defp terminal(_person, %{terminal: nil}), do: nil

  defp terminal(person, %{terminal: %{state: state, at: at} = terminal} = record) do
    %{
      "state" => state,
      "at_ms" => at,
      "by_this_person" => Conversation.terminal_author(record, terminal) == "person"
    }
    |> put_present("answer", answer_outcome(record, terminal))
    |> put_present("retracted_by", Conversation.retracted_by(person, terminal))
  end

  # Today's person-facing answer_outcome (contract rulings round 4, item 3).
  # The origin-only raw answer is never included.
  def answer_outcome(record, terminal) do
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

      # Round 16: on an answer-screen (UI/1) item the outcome follows the
      # item's schema, not the value's shape.
      {_, "answer", true, answer} ->
        if ui1?(record.message),
          do: structured_outcome(terminal, answer),
          else: action_outcome(record, answer)

      _ ->
        nil
    end
  end

  # A /v2 answer to a UI/1 item records its path: a structured result (any
  # JSON value, null included) or a plain-text comment, shown as a reply. An
  # answer with no recorded path (before 0.2) shows exactly as today's
  # answer_outcome does, so nothing is invented (contract round 16, option C).
  defp structured_outcome(terminal, answer) do
    case {Map.get(terminal, :answer_path), answer} do
      {"result", value} ->
        %{"kind" => "result", "value" => value}

      {"comment", text} when is_binary(text) or is_struct(text, LargeData.Ref) ->
        %{"kind" => "reply", "text" => text}

      {nil, nil} ->
        %{"kind" => "seen"}

      {nil, text} when is_binary(text) or is_struct(text, LargeData.Ref) ->
        %{"kind" => "reply", "text" => text}

      _ ->
        nil
    end
  end

  defp ui1?(%{"answer_ui" => %{"schema" => "attention-client.ui/1"}}), do: true
  defp ui1?(_message), do: false

  defp action_outcome(record, answer) do
    case answer do
      nil ->
        %{"kind" => "seen"}

      answer ->
        # An answer through one of the item's value actions stores that
        # action's value. The Seen action is seen; any other is the choice,
        # named by the action (contract round 15). Otherwise text is a reply.
        case value_action(record.message, answer) do
          %{"id" => "seen"} ->
            %{"kind" => "seen"}

          %{"id" => id} = action when is_binary(id) ->
            put_present(%{"kind" => "choice", "id" => id}, "label", text_or_nil(action["label"]))

          nil when is_binary(answer) or is_struct(answer, LargeData.Ref) ->
            %{"kind" => "reply", "text" => answer}

          nil ->
            nil
        end
    end
  end

  defp value_action(%{"answer_ui" => %{"actions" => actions}}, answer) when is_list(actions),
    do: Enum.find(actions, &match?(%{"kind" => "value", "value" => ^answer}, &1))

  defp value_action(_message, _answer), do: nil

  defp text_or_nil(value) when is_binary(value), do: value
  defp text_or_nil(_), do: nil

  def origin_result(%{origin_result: %{status: status, revision: revision} = result}) do
    put_present(%{"status" => status, "revision" => revision}, "result_ref", result[:result_ref])
  end

  def origin_result(_), do: nil

  defp file_refs(files, prefix) when is_list(files) do
    files
    |> Enum.with_index()
    |> Enum.map(fn {file, index} -> file_ref(file, prefix, index) end)
  end

  defp file_refs(_files, _prefix), do: []

  # Round 18: FileRef.content_type is the type an upload was sent with, kept
  # on the upload; files from before the cut-over have none.
  defp typed_files(engine, refs) when is_list(refs) do
    Enum.map(refs, fn
      %{"file_id" => "fu" <> upload_id} = ref ->
        case Map.get(engine.uploads, upload_id) do
          %{content_type: type} when is_binary(type) -> Map.put_new(ref, "content_type", type)
          _ -> ref
        end

      ref ->
        ref
    end)
  end

  defp typed_files(_engine, refs), do: refs

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
    |> put_present("content_type", if(is_binary(file["content_type"]), do: file["content_type"]))
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
    reply_levels = view_reply_entries(view)
    {listed, listed_count} = listed_units(engine, person, view)

    # The view lists every item in the usual case; walking the person's items
    # is needed only when it does not (a progress message, or no view yet).
    resolved =
      Map.get(engine.route_b_index.resolved_messages_by_person, person, :gb_trees.empty())

    total =
      Map.get(engine.route_b_index.unresolved_count_by_person, person, 0) +
        :gb_trees.size(resolved)

    unlisted =
      if listed_count >= total do
        []
      else
        listed_items = for {:item, id, _, _, _} <- listed, into: MapSet.new(), do: id
        unlisted_units(engine, person, listed_items)
      end

    units = listed ++ unlisted
    keys = rank_keys(previous, Enum.map(units, &unit_id/1))

    Enum.reduce(units, %{}, fn
      {:item, id, {_, sender, message_id}, section, group}, acc ->
        entry = Map.get(entries, {sender, message_id}, %{})

        rank =
          %{"key" => Map.fetch!(keys, id), "section" => section}
          |> put_present("group", group)
          |> put_view_fields(entry)
          |> put_present("_thread", view_thread(entry))

        case acc do
          %{^id => %{"_reply" => reply}} -> Map.put(acc, id, Map.put(rank, "_reply", reply))
          _ -> Map.put(acc, id, rank)
        end

      {:reply, id, key, row, _} = unit, acc ->
        reply = %{
          "_reply" => %{
            "key" => Map.fetch!(keys, unit_id(unit)),
            "group" => row.group,
            "priority" =>
              %{"level" => reply_row_level(engine, key, reply_levels)}
              |> put_present("score", row.score)
          }
        }

        Map.update(acc, id, reply, &Map.merge(&1, reply))
    end)
  end

  @doc """
  The keys of `ranks/4`'s output by rank unit id: what the change log keeps
  and passes back as `previous`. An item row's unit id is its `item_id`; its
  chat-reply row's is `"r"` followed by the `item_id`.
  """
  def rank_unit_keys(ranks) when is_map(ranks) do
    Enum.reduce(ranks, %{}, fn {id, rank}, acc ->
      acc = Map.put(acc, id, Map.fetch!(rank, "key"))

      case rank do
        %{"_reply" => %{"key" => key}} -> Map.put(acc, "r" <> id, key)
        _ -> acc
      end
    end)
  end

  @doc "The engine key of the item a rank unit belongs to."
  def rank_unit_item(%Engine{} = engine, person, "r" <> item_id),
    do: item_key(engine, person, item_id)

  def rank_unit_item(%Engine{} = engine, person, item_id), do: item_key(engine, person, item_id)

  # Items the view does not list: open ones, then resolved ones, newest first.
  defp unlisted_units(engine, person, listed_items) do
    engine
    |> item_keys(person)
    |> Enum.map(&{&1, Map.fetch!(engine.messages, &1)})
    |> Enum.reject(fn {_key, record} -> MapSet.member?(listed_items, item_id(record)) end)
    |> Enum.sort_by(fn {_key, record} ->
      {not BounceDecision.inbox_open?(record), -record.accepted_seq}
    end)
    |> Enum.map(fn {key, record} ->
      section = if BounceDecision.inbox_open?(record), do: "current", else: "resolved"
      {:item, item_id(record), key, section, nil}
    end)
  end

  defp unit_id({:item, id, _, _, _}), do: id
  defp unit_id({:reply, id, _, _, _}), do: "r" <> id

  # Today's list rows, in order, as rank units, including every chat-reply row
  # (round 7: clients choose which reply rows to draw).
  defp listed_units(engine, person, %{list_items: rows}) when is_list(rows) do
    {units, _seen, count} =
      Enum.reduce(rows, {[], MapSet.new(), 0}, fn row, {units, seen, count} ->
        with %{"sender_id" => sender, "message_id" => message_id} <- row,
             key = {person, sender, message_id},
             %{accepted_seq: _} = record <- Map.get(engine.messages, key),
             BounceDecision.person_visible?(record),
             {:ok, unit} <- list_unit(row, item_id(record), key, record),
             id = unit_id(unit),
             false <- MapSet.member?(seen, id) do
          count = if elem(unit, 0) == :item, do: count + 1, else: count
          {[unit | units], MapSet.put(seen, id), count}
        else
          _ -> {units, seen, count}
        end
      end)

    {Enum.reverse(units), count}
  end

  defp listed_units(_engine, _person, _view), do: {[], 0}

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
       when is_integer(unseen) and unseen >= 0 do
    group =
      case row do
        %{"free_section" => %{"group" => group}}
        when group in ~w(earlier_chat_replies earlier_chat_replies_unknown) ->
          group

        _ ->
          nil
      end

    score = if is_number(row["score"]), do: row["score"]
    {:ok, {:reply, id, key, %{group: group, score: score}, nil}}
  end

  defp list_unit(_row, _id, _key, _record), do: :skip

  # The effective group thread, as the view's entry carries it: Placement
  # builds each entry from Placement.items, where the thread is inherited
  # through supersedes. Reading it here avoids rescanning every message.
  defp view_thread(%{"thread" => thread}) when is_binary(thread) and thread != "", do: thread
  defp view_thread(_entry), do: nil

  # Today's per-reply priority view entries, by item and conversation sequence.
  defp view_reply_entries(%{reply_entries_by_item: index}), do: index
  defp view_reply_entries(%{replies: replies}) when is_list(replies), do: index_reply_entries(replies)
  defp view_reply_entries(_), do: %{}

  @doc false
  def index_reply_entries(replies) do
    Enum.reduce(replies, %{}, fn
      %{"sender_id" => sender, "message_id" => id, "conversation_seq" => seq} = entry, acc
      when is_integer(seq) ->
        Map.update(acc, {sender, id}, %{seq => entry}, &Map.put(&1, seq, entry))

      _, acc ->
        acc
    end)
  end

  @levels ~w(low normal high now)

  # A reply's level is today's display priority for it: the view's priority,
  # which is the item's effective urgency, or that urgency when the view
  # carries none (the selected ranking does not decorate replies).
  defp reply_level(entry, record) do
    case entry do
      %{"priority" => level} when level in @levels -> level
      _ -> Message.effective_urgency(record.message)
    end
  end

  # A reply row's level: the highest among its unseen replies, or with none
  # unseen, its newest reply's.
  defp reply_row_level(engine, {person, sender, id} = key, reply_entries) do
    record = Map.fetch!(engine.messages, key)
    read_through = Map.get(engine.chat_read_positions, key, 0)
    by_seq = Map.get(reply_entries, {sender, id}, %{})

    replies =
      engine.route_b_index.conversations_by_item
      |> Map.get(key, :gb_trees.empty())
      |> :gb_trees.values()
      |> Enum.filter(fn seq ->
        match?(
          %{payload: %{"kind" => "reply", "author" => "origin"}},
          Map.get(engine.conversation_events, {person, seq})
        )
      end)

    chosen =
      case Enum.filter(replies, &(&1 > read_through)) do
        [] -> Enum.take(replies, 1)
        unseen -> unseen
      end

    chosen
    |> Enum.map(&reply_level(Map.get(by_seq, &1), record))
    |> Enum.max_by(&Enum.find_index(@levels, fn level -> level == &1 end), fn ->
      Message.effective_urgency(record.message)
    end)
  end

  defp view_entries(%{message_entries_by_identity: entries}) when is_map(entries), do: entries
  defp view_entries(_), do: %{}

  # Round 15: level is today's display priority, wherever the view entry has
  # one; score and due time where the view has them.
  defp put_view_fields(rank, entry) do
    rank
    |> put_present("level", if(entry["priority"] in @levels, do: entry["priority"]))
    |> put_present("score", if(is_number(entry["score"]), do: entry["score"]))
    |> put_present("due_at_ms", time_ms(entry["due_at"]))
  end

  @max_rank_key_bytes 48

  @doc """
  Assigns rank keys to `ordered_ids` (first first). Keys sort
  ascending, bytewise. The longest run of ids whose `previous` keys are already
  in order keeps them. Every other id gets a new key between its kept
  neighbours, so a single move normally rewrites a single key. If a gap
  requires an overlong key, the order is spread again with short keys.
  """
  def rank_keys(previous, ordered_ids) when is_map(previous) and is_list(ordered_ids) do
    if map_size(previous) == length(ordered_ids) and previous_in_order?(ordered_ids, previous, "") do
      previous
    else
      candidates =
        ordered_ids
        |> Enum.with_index()
        |> Enum.reduce([], fn {id, index}, acc ->
          case Map.get(previous, id) do
            key when is_binary(key) ->
              if valid_rank_key?(key), do: [{index, key} | acc], else: acc

            _ ->
              acc
          end
        end)
        |> Enum.reverse()

      keys = reassign(ordered_ids, candidates)

      if Enum.any?(keys, fn {_id, key} -> byte_size(key) > @max_rank_key_bytes end) do
        Map.new(spread("", nil, ordered_ids))
      else
        keys
      end
    end
  end

  defp previous_in_order?([], _previous, _last), do: true

  defp previous_in_order?([id | ids], previous, last) do
    case previous do
      %{^id => key} when is_binary(key) and key > last ->
        valid_rank_key?(key) and previous_in_order?(ids, previous, key)

      _ ->
        false
    end
  end

  defp valid_rank_key?(<<"1", fraction::binary>> = key)
       when byte_size(fraction) > 0 and byte_size(key) <= @max_rank_key_bytes,
       do: valid_fraction?(fraction)

  defp valid_rank_key?(_key), do: false

  defp increasing?([{_, first} | rest]) do
    {ok, _} =
      Enum.reduce_while(rest, {true, first}, fn {_, key}, {true, last} ->
        if key > last, do: {:cont, {true, key}}, else: {:halt, {false, key}}
      end)

    ok
  end

  defp increasing?([]), do: true

  defp reassign(ordered_ids, candidates) do
    # An insertion or deletion can leave every remaining key in order.
    kept = if increasing?(candidates), do: candidates, else: longest_increasing(candidates)
    assign_rank_gaps(kept, ordered_ids, 0, "", %{})
  end

  defp assign_rank_gaps([], ids, _offset, low, assigned) do
    Enum.into(spread(low, nil, ids), assigned)
  end

  defp assign_rank_gaps([{index, "1" <> high = key} | kept], ids, offset, low, assigned) do
    {gap, [id | remaining]} = Enum.split(ids, index - offset)

    assigned =
      Enum.reduce(spread(low, high, gap), Map.put(assigned, id, key), fn {id, key}, acc ->
        Map.put(acc, id, key)
      end)

    assign_rank_gaps(kept, remaining, index + 1, high, assigned)
  end

  defp valid_fraction?(<<digit, rest::binary>>)
       when digit in ?0..?9 or digit in ?A..?Z or digit in ?a..?z do
    if rest == "", do: digit != ?0, else: valid_fraction?(rest)
  end

  defp valid_fraction?(_fraction), do: false

  # Patience sorting: the longest strictly increasing subsequence by key, in
  # O(n log n). The tails live in an :array, which updates in O(log n); a
  # tuple would be copied on every step.
  defp longest_increasing([]), do: []

  defp longest_increasing(pairs) do
    {tails, size, links} =
      Enum.reduce(pairs, {:array.new(), 0, %{}}, fn {index, key} = pair, {tails, size, links} ->
        slot = lower_bound(tails, key, 0, size)
        previous = if slot > 0, do: :array.get(slot - 1, tails), else: nil
        size = if slot == size, do: size + 1, else: size
        {:array.set(slot, pair, tails), size, Map.put(links, index, previous)}
      end)

    walk(:array.get(size - 1, tails), links, [])
  end

  defp lower_bound(_tails, _key, low, high) when low >= high, do: low

  defp lower_bound(tails, key, low, high) do
    middle = div(low + high, 2)
    {_index, middle_key} = :array.get(middle, tails)

    if middle_key < key,
      do: lower_bound(tails, key, middle + 1, high),
      else: lower_bound(tails, key, low, middle)
  end

  defp walk(nil, _links, acc), do: acc

  defp walk({index, _key} = pair, links, acc),
    do: walk(Map.fetch!(links, index), links, [pair | acc])

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
  def entries(%Engine{} = engine, {person, _, _} = key, _view \\ nil) do
    record = Map.fetch!(engine.messages, key)
    item_id = item_id(record)

    facts =
      engine.route_b_index.conversations_by_item
      |> Map.get(key, :gb_trees.empty())
      |> :gb_trees.values()
      |> Enum.reverse()
      |> Enum.map(fn seq ->
        Projection.run({:entry, entry_id(seq)}, fn ->
          {seq, Map.fetch!(engine.conversation_events, {person, seq}).payload}
        end)
      end)

    by_event_id =
      for {seq, %{"kind" => kind, "event_id" => event_id}} <- facts,
          kind in ~w(discussion reply revision),
          is_binary(event_id),
          into: %{},
          do: {event_id, seq}

    # Each result fact is a result entry, numbered like Item.result.revision.
    results =
      facts
      |> Enum.filter(fn {_seq, payload} -> payload["kind"] == "result" end)
      |> Enum.with_index(1)
      |> Map.new(fn {{seq, _payload}, n} -> {seq, n} end)

    context = %{
      by_event_id: by_event_id,
      results: results,
      key: key,
      engine: engine
    }

    # Receipt and acknowledgment facts mark the entry they refer to. Each entry
    # records the facts it is made of, so its rev can be the newest of theirs.
    folded =
      Enum.reduce(facts, %{}, fn {seq, payload}, acc ->
        with {event_id, evidence} <- delivery_effect(payload, engine, key, record),
             target when is_integer(target) <- Map.get(by_event_id, event_id) do
          Map.update(acc, target, {evidence, [seq]}, fn {marks, seqs} ->
            {Map.merge(marks, evidence), [seq | seqs]}
          end)
        else
          _ -> acc
        end
      end)

    Enum.flat_map(facts, fn {seq, payload} ->
      {marks, folded_seqs} = Map.get(folded, seq, {%{}, []})

      case entry(item_id, record, seq, payload, context) do
        nil ->
          []

        %{"author" => "person"} = entry ->
          delivery =
            put_present(person_delivery(marks), "receipt_expected", payload["receipt_expected"])

          entry
          |> Map.put("delivery", delivery)
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
  The entries a set of conversation facts show up in, as `%{seq => entry}`:
  each fact's own entry, or the entry a receipt or acknowledgment marks. Facts
  outside any entry are left out. Each item's entries are built once.
  """
  def entries_by_fact(%Engine{} = engine, person, seqs, view \\ nil) do
    seqs
    |> Enum.uniq()
    |> Enum.group_by(fn seq ->
      case Map.get(engine.conversation_events, {person, seq}) do
        %{key: {^person, _, _} = key} -> if BounceDecision.person_visible?(engine.messages[key]), do: key
        _ -> nil
      end
    end)
    |> Enum.reject(fn {key, _seqs} -> is_nil(key) end)
    |> Enum.flat_map(fn {key, wanted} ->
      wanted = MapSet.new(wanted)

      for entry <- entries(engine, key, view),
          seq <- entry[@sources],
          MapSet.member?(wanted, seq),
          do: {seq, entry}
    end)
    |> Map.new()
  end

  @doc """
  The entry a conversation fact shows up in: the fact's own entry, the entry
  a receipt or acknowledgment fact marks, or nil for facts that are not part
  of any entry (read positions and file reports).
  """
  def entry_for_fact(%Engine{} = engine, person, seq, view \\ nil) do
    case Map.get(engine.conversation_events, {person, seq}) do
      %{key: {^person, _, _} = key} ->
        if BounceDecision.person_visible?(engine.messages[key]),
          do: engine |> entries(key, view) |> Enum.find(&(seq in &1[@sources])),
          else: nil

      _ ->
        nil
    end
  end

  @doc false
  def agent_entry(engine, key, record, position) do
    Projection.run({:entry, entry_id(position)}, fn ->
      project_agent_entry(engine, key, record, position)
    end)
  end

  defp project_agent_entry(engine, {person, _, _} = key, record, position) do
    with %{key: ^key, payload: payload} <- Map.get(engine.conversation_events, {person, position}) do
      index = engine.route_b_index
      id = payload["in_reply_to"]
      related = AttentionServer.RouteBIndex.entry_position(index, key, id)
      by_event_id = if related, do: %{id => related}, else: %{}

      results =
        if payload["kind"] == "result",
          do: %{position => AttentionServer.RouteBIndex.result_revision(index, key, position)},
          else: %{}

      context = %{
        by_event_id: by_event_id,
        results: results,
        key: key,
        engine: engine
      }

      case entry(item_id(record), record, position, payload, context) do
        %{"author" => "person"} = projected ->
          projected
          |> Map.put("delivery", %{"delivered" => false, "acted" => false})
          |> Map.put(@sources, [position])

        %{} = projected ->
          Map.put(projected, @sources, [position])

        nil ->
          nil
      end
    else
      _ -> nil
    end
  end

  defp person_delivery(marks) do
    receipt = Map.get(marks, :receipt, %{})
    capability = Map.get(marks, :capability, %{})

    marks
    |> Map.take(~w(delivered acted agent_acted))
    |> Map.put_new("delivered", false)
    |> Map.put_new("acted", false)
    |> Map.merge(receipt)
    |> Map.merge(capability)
  end

  defp delivery_effect(%{"kind" => "receipt", "delivered" => true, "event_id" => id}, _, _, _),
    do: {id, %{"delivered" => true}}

  defp delivery_effect(
         %{"kind" => "delivery_ack", "stage" => "ack1", "event_id" => id} = ack,
         _,
         _,
         _
       ) do
    marks =
      case ack do
        %{"r25_served" => true} ->
          capability = Map.take(ack, ~w(ack2_expected listener_received))
          %{"delivered" => true, :capability => capability}

        %{"ack2_expected" => true} ->
          %{"delivered" => true, :capability => %{"ack2_expected" => true}}

        _ ->
          %{"delivered" => true}
      end

    {id, marks}
  end

  defp delivery_effect(
         %{"kind" => "delivery_ack", "stage" => "ack2", "event_id" => id} = ack,
         _,
         _,
         _
       ) do
    receipt =
      ack
      |> Map.take(~w(outcome reason rerouted))
      |> put_present("agent", get_in(ack, ["acknowledger", "agent"]))

    {id, %{"acted" => true, :receipt => receipt}}
  end

  defp delivery_effect(
         %{"kind" => "acted", "event_id" => id} = act,
         engine,
         {person, _, _},
         record
       ) do
    evidence =
      with acting when is_binary(acting) <- act["acting_event_id"],
           {seq, ""} <- Integer.parse(acting),
           %{"kind" => kind} <-
             get_in(engine.origin_feeds, [{person, record.origin}, :entries, seq]),
           true <- kind in ["revision", "terminal"] do
        %{"acted" => true, "agent_acted" => true}
      else
        _ -> %{"acted" => true}
      end

    {id, evidence}
  end

  defp delivery_effect(_, _, _, _), do: nil

  defp entry(item_id, record, seq, payload, context) do
    key = {:entry, entry_id(seq)}

    Projection.run(key, fn ->
      Projection.probe(key)
      render_entry(item_id, record, seq, payload, context)
    end)
  end

  defp render_entry(item_id, record, seq, %{"kind" => kind} = payload, context) do
    base = %{
      "entry_id" => entry_id(seq),
      "item_id" => item_id,
      "position" => seq,
      "request_revision" => Map.get(payload, "request_revision") || Map.get(record, :revision, 1)
    }

    case kind do
      k when k in ~w(discussion reply) ->
        author = if k == "discussion", do: "person", else: Map.get(payload, "author", "origin")

        entry =
          base
          |> Map.merge(%{"kind" => "reply", "author" => author})
          |> put_present("text", payload["text"])
          |> put_present("files", typed_entry_files(context, payload, seq))
          |> put_present("in_reply_to", reply_target(payload, context.by_event_id))

        # An agent reply carries today's per-reply priority (round 7).
        if author == "origin" do
          priority = ReplyPriority.project(context.engine, context.key, seq, record)
          Map.put(entry, "priority", priority)
        else
          entry
        end

      "result" ->
        result =
          %{"status" => payload["status"], "revision" => Map.fetch!(context.results, seq)}
          |> put_present("result_ref", if(is_binary(payload["op_id"]), do: payload["op_id"]))

        base
        |> Map.merge(%{"kind" => "result", "author" => "origin", "result" => result})
        |> put_present("in_reply_to", reply_target(payload, context.by_event_id))

      "revision" ->
        files =
          if payload["agent_api_v2_files"] do
            case revision(context.engine, context.key, payload["request_revision"]) do
              {:ok, %{"files" => [_ | _] = files}} -> files
              _ -> nil
            end
          else
            typed_entry_files(context, payload, seq)
          end

        base
        |> Map.merge(%{"kind" => "revision", "author" => "origin"})
        |> put_present("files", files)

      "availability" ->
        base
        |> Map.merge(%{"kind" => "availability", "author" => "server"})
        |> put_present("availability", availability(payload))

      "retraction" ->
        base
        |> Map.merge(%{"kind" => "lifecycle", "author" => "origin", "state" => "retracted"})
        # The retraction fact records when it happened, even before cut-over.
        |> put_present("at_ms", time_ms(payload["retracted_at"]))

      _ ->
        nil
    end
  end

  defp entry_files(%{"files" => [_ | _] = files}, seq), do: file_refs(files, "fe#{seq}.")
  defp entry_files(_payload, _seq), do: nil

  defp typed_entry_files(context, payload, seq) do
    case entry_files(payload, seq) do
      nil -> nil
      refs -> typed_files(context.engine, refs)
    end
  end

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
    Projection.run(:settings, fn -> project_settings(engine, person) end)
  end

  defp project_settings(engine, person) do
    push =
      for {{^person, device}, binding} <- engine.push_bindings,
          into: %{},
          do: {device, push_status(binding)}

    %{
      "priority_levels" => BounceDecision.levels(engine, person),
      "alerts" => Engine.alert_choice(engine, person),
      @viewer => %{"push" => push}
    }
  end

  defp push_status(binding) do
    %{"state" => binding.state, "revision" => binding.revision}
    |> put_present("binding_id", if(binding.active, do: Map.get(binding, :binding_id)))
    |> put_present("environment", if(binding.active, do: environment(binding.profile_id)))
    |> put_present("last_attempt_at_ms", time_ms(Map.get(binding, :last_attempt_at)))
    |> put_present("next_attempt_at_ms", time_ms(Map.get(binding, :next_at)))
  end

  defp environment("harklight-production"), do: "production"
  defp environment("harklight-sandbox"), do: "sandbox"
  defp environment(_), do: nil

  @unconfigured_push %{"state" => "unconfigured", "revision" => 0}

  ## Rendering

  @doc """
  Sets `Entry.at_ms` from the change log's first-seen time. Entries from
  before the cut-over have none and stay without it.
  """
  def put_at(entry, at_ms) when is_integer(at_ms) and at_ms >= 0,
    do: Map.put(entry, "at_ms", at_ms)

  def put_at(entry, _none), do: entry

  @doc "Sets the resource `rev` from the change log on a projected resource."
  def stamp(payload, rev) when is_map(payload) and is_integer(rev) and rev > 0,
    do: Map.put(payload, "rev", rev)

  @doc """
  Renders a person-level payload for one principal: the wire form of an Item,
  Entry, Settings, Counts or removal.
  """
  def render(:item, %{@viewer => %{"origin" => origin}} = item, %{key: key} = principal) do
    Projection.run(Projection.change_key(item), fn ->
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
    end)
  end

  def render(:settings, %{@viewer => %{"push" => push}} = settings, %{key: key, kind: kind}) do
    Projection.run(:settings, fn ->
      status =
        if kind == :device, do: Map.get(push, key, @unconfigured_push), else: @unconfigured_push

      settings
      |> Map.delete(@viewer)
      |> Map.put("push", status)
    end)
  end

  def render(:entry, entry, _principal), do: Map.delete(entry, @sources)

  def render(kind, payload, _principal) when kind in [:counts, :item_removed],
    do: payload

  ## Helpers

  defp positive(value) when is_integer(value) and value > 0, do: value
  defp positive(_), do: nil

  defp time_ms(value) when is_integer(value) and value >= 0, do: value
  defp time_ms(_), do: nil

  defp put_present(map, _key, nil), do: map
  defp put_present(map, key, value), do: Map.put(map, key, value)
end
