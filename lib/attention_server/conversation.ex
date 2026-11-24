defmodule AttentionServer.Conversation do
  @moduledoc """
  Durable origin feed for request-bound conversations. Entries are immutable;
  delivery and action facts live separately so replay never changes a turn.
  """

  alias AttentionServer.{ConversationFiles, Engine, Message}

  def valid_session?(handle, vendor_id) do
    Message.uuid_v4?(handle) and
      (is_nil(vendor_id) or
         (is_binary(vendor_id) and vendor_id != "" and String.valid?(vendor_id)))
  end

  @support_fields ~w(route cli_version unsupported_reason ack2 advisory_v1 origin_listener)

  def support_from_frame(frame) do
    if Enum.any?(@support_fields, &Map.has_key?(frame, &1)) do
      route = frame["route"]
      version = frame["cli_version"]
      reason = frame["unsupported_reason"]

      if route in ~w(claude-code codex other) and is_binary(version) and version != "" and
           String.valid?(version) and Map.has_key?(frame, "unsupported_reason") and
           (is_nil(reason) or (is_binary(reason) and reason != "" and String.valid?(reason))) and
           (not Map.has_key?(frame, "ack2") or is_boolean(frame["ack2"])) and
           (not Map.has_key?(frame, "advisory_v1") or is_boolean(frame["advisory_v1"])) and
           (not Map.has_key?(frame, "origin_listener") or is_boolean(frame["origin_listener"])) and
           (frame["origin_listener"] != true or route == "other") do
        {:ok, Map.take(frame, @support_fields)}
      else
        {:error, "listening_support"}
      end
    else
      {:ok, nil}
    end
  end

  def valid_support_record?(nil), do: true

  def valid_support_record?(%{"origin" => origin, "reported_at" => at} = record) do
    is_binary(origin) and origin != "" and is_integer(at) and at > 0 and
      Map.keys(Map.drop(record, ["ack2", "advisory_v1", "origin_listener"])) |> Enum.sort() ==
        ~w(cli_version origin reported_at route unsupported_reason) and
      match?({:ok, _}, support_from_frame(record))
  end

  def valid_support_record?(_), do: false

  def on_send(engine, person, origin, record, {_, sender, message_id}) do
    handle = record.session_handle
    session_key = {person, origin, handle}

    engine =
      if Map.get(record, :agent_api_v2),
        do: reopen(engine, session_key, :accepted_send),
        else: engine

    session =
      Map.get(engine.origin_sessions, session_key, %{
        vendor_session_id: record.vendor_session_id,
        gone: false,
        last_detached_at: nil
      })

    session =
      case Map.get(record, :support) do
        nil ->
          session

        declared ->
          Map.put(
            session,
            :support,
            declared
            |> Map.put("origin", origin)
            |> Map.put("reported_at", System.system_time(:millisecond))
          )
      end

    entry =
      %{
        "kind" => "revision",
        "author" => "origin",
        "sender_id" => sender,
        "message_id" => message_id,
        "session_handle" => handle,
        "request_revision" => 1,
        "op_id" => Map.get(record, :send_op_id),
        "message" => record.message
      }
      |> ConversationFiles.put_entry(Map.get(record, :files_by_revision, %{}) |> Map.get(1, []))

    engine
    |> Engine.put_row(:origin_sessions, session_key, session)
    |> append_entry(person, origin, entry)
  end

  def terminal_author(record, terminal) do
    cond do
      terminal.actor_kind == :server -> "server"
      terminal.actor_kind == :agent -> "agent"
      terminal.state == "superseded" -> "server"
      terminal.actor == record.origin -> "origin"
      terminal.state == "expired" -> "server"
      true -> "person"
    end
  end

  # The first answer settles a request, but the conversation about it stays open
  # after every terminal outcome: the person can keep writing and the agent can
  # keep replying. Chat has no open/closed state of its own.

  # Request and lifecycle facts stay on the reply channel; chat contains only side-conversation turns.
  defp channel(%{"kind" => "discussion"}), do: "chat"
  defp channel(%{"kind" => "reply", "author" => "origin"}), do: "chat"
  defp channel(_), do: "reply"

  def on_terminal(engine, person, record, {_, sender, message_id}, terminal) do
    if is_nil(Map.get(record, :session_handle)) do
      {engine, nil}
    else
      entry = %{
        "kind" => "terminal",
        "author" => terminal_author(record, terminal),
        "sender_id" => sender,
        "message_id" => message_id,
        "session_handle" => record.session_handle,
        "request_revision" => Map.get(record, :revision, 1),
        "terminal_state" => terminal.state,
        "terminal_seq" => terminal.seq,
        "has_answer" => terminal.answer_present
      }

      entry = if terminal.by == "default", do: Map.put(entry, "by", "default"), else: entry

      entry =
        if terminal.state == "retracted",
          do: Map.put(entry, "retracted_at", terminal.at),
          else: entry

      entry =
        if Map.get(terminal, :kind) == "no_comment",
          do: Map.put(entry, "terminal_kind", "no_comment"),
          else: entry

      entry =
        if terminal.answer_present,
          do: Map.put(entry, "answer", terminal.answer),
          else: entry

      append_entry(engine, person, record.origin, entry)
    end
  end

  @doc false
  def file_report(engine, person, origin, feed_seq, name, report) do
    key = {person, origin, feed_seq}
    entry = get_in(engine.origin_feeds, [{person, origin}, :entries, feed_seq])

    with %{} = entry <- entry,
         true <- Enum.any?(Map.get(entry, "files", []), &(&1["name"] == name)),
         %{} = receipt <-
           Map.get(engine.origin_receipts, key) ||
             receipt_from_delivery(engine, person, origin, entry),
         {:ok, next} <- record_file_report(receipt, name, report) do
      reply =
        %{"event_id" => entry["event_id"], "file" => name}
        |> Map.merge(Map.new(report, fn {dimension, value} -> {dimension, value["status"]} end))

      if next == receipt do
        {:ok, engine, Map.put(reply, "duplicate", true)}
      else
        {:ok, Engine.put_row(engine, :origin_receipts, key, next), reply}
      end
    else
      {:error, _} = error -> error
      _ -> {:error, :not_found}
    end
  end

  defp record_file_report(receipt, name, report) do
    Enum.reduce_while(report, {:ok, receipt}, fn {dimension, value}, {:ok, next} ->
      field = if dimension == "transfer", do: :transfers, else: :uses
      current = Map.get(next, field, %{})

      case Map.get(current, name) do
        nil -> {:cont, {:ok, Map.put(next, field, Map.put(current, name, value))}}
        ^value -> {:cont, {:ok, next}}
        _ -> {:halt, {:error, :file_use_conflict}}
      end
    end)
  end

  defp receipt_from_delivery(engine, person, origin, %{"author" => "person"} = entry) do
    case AttentionServer.Engine.delivery_ack_fact(engine, person, origin, entry, :ack1) do
      %{"at_ms" => at} ->
        %{delivered_at: at, transfers: %{}, uses: %{}, implicit_ack: true}

      _ ->
        nil
    end
  end

  defp receipt_from_delivery(_engine, _person, _origin, _entry), do: nil

  def actionable?(%{"kind" => kind, "author" => author}),
    do: kind in ["discussion", "terminal"] and author != "origin"

  def actionable?(_), do: false

  def person_entry(engine, person, origin, seq, {_, sender, message_id}) do
    with %{entries: entries} <- Map.get(engine.origin_feeds, {person, origin}),
         %{"sender_id" => ^sender, "message_id" => ^message_id} = entry <-
           Map.get(entries, seq),
         true <- actionable?(entry) do
      {:ok, entry}
    else
      _ -> {:error, :not_found}
    end
  end

  def act(engine, person, origin, seq, op_id) do
    key = {person, origin, seq}

    if Map.has_key?(engine.origin_actions, key) do
      {:error, :already_acted}
    else
      action = %{op_id: op_id, at: System.system_time(:millisecond)}
      {:ok, Engine.put_row(engine, :origin_actions, key, action)}
    end
  end

  # Rebuild on open so older snapshots and a rollback that appended without the
  # index are both safe. Subsequent appends keep this per-handle lookup current.
  def index_origin_feeds(engine) do
    feeds =
      Map.new(engine.origin_feeds, fn {key, feed} ->
        by_handle =
          Enum.reduce(feed.entries, %{}, fn {seq, entry}, index ->
            Map.update(index, entry["session_handle"], MapSet.new([seq]), &MapSet.put(&1, seq))
          end)

        {key, Map.put(feed, :entry_seqs_by_handle, by_handle)}
      end)

    %{engine | origin_feeds: feeds}
  end

  def status(engine, person, origin, handle, listening?) do
    case Map.get(engine.origin_sessions, {person, origin, handle}) do
      nil ->
        {:error, :not_found}

      session ->
        feed = Map.get(engine.origin_feeds, {person, origin}, %{high: 0, entries: %{}})

        entries =
          feed
          |> Map.get(:entry_seqs_by_handle, %{})
          |> Map.get(handle, MapSet.new())
          |> Stream.map(fn seq -> {seq, Map.fetch!(feed.entries, seq)} end)

        open_requests =
          entries
          |> Enum.reduce(MapSet.new(), fn {_seq, entry}, requests ->
            request_key = {person, entry["sender_id"], entry["message_id"]}
            record = Map.get(engine.messages, request_key)

            if ((entry["session_handle"] == handle and record) && record.origin == origin) and
                 is_nil(record.terminal) do
              MapSet.put(requests, request_key)
            else
              requests
            end
          end)
          |> MapSet.size()

        # A request keeps its chat open while it is unresolved or while a person
        # chat turn on it still waits for the agent to act on it.
        open_chats =
          entries
          |> Enum.reduce(MapSet.new(), fn {seq, entry}, requests ->
            request_key = {person, entry["sender_id"], entry["message_id"]}
            record = Map.get(engine.messages, request_key)

            if ((entry["session_handle"] == handle and record) && record.origin == origin) and
                 (is_nil(record.terminal) or
                    (actionable?(entry) and needs_action?(engine, person, entry) and
                       not Map.has_key?(engine.origin_actions, {person, origin, seq}))) do
              MapSet.put(requests, request_key)
            else
              requests
            end
          end)
          |> MapSet.size()

        {undelivered, delivered_unacted} =
          Enum.reduce(entries, {0, 0}, fn {seq, entry}, {queued, waiting} ->
            if entry["session_handle"] == handle and actionable?(entry) do
              key = {person, origin, seq}
              delivered? = Map.has_key?(engine.origin_receipts, key)
              acted? = Map.has_key?(engine.origin_actions, key)

              if not delivered? do
                {queued + 1, waiting}
              else
                if not acted? and needs_action?(engine, person, entry),
                  do: {queued, waiting + 1},
                  else: {queued, waiting}
              end
            else
              {queued, waiting}
            end
          end)

        availability =
          cond do
            is_nil(Map.get(session, :support)) or
                not is_nil(session.support["unsupported_reason"]) ->
              "unsupported"

            session.gone ->
              "gone"

            listening? ->
              "listening"

            true ->
              "not_listening"
          end

        support = Map.get(session, :support)

        reason =
          cond do
            is_nil(support) -> "no listening support recorded"
            availability == "unsupported" -> support["unsupported_reason"]
            true -> nil
          end

        {:ok,
         %{
           "session_handle" => handle,
           "availability" => availability,
           "unsupported_reason" => reason,
           "listening_support" => support,
           "last_detached_at" => session.last_detached_at,
           "open_requests" => open_requests,
           "pending_undelivered" => undelivered,
           "pending_delivered_unacted" => delivered_unacted,
           "open_chats" => open_chats,
           "cursor" => feed.high
         }}
    end
  end

  def reopened?(before, after_engine, scope) do
    match?(%{gone: true}, Map.get(before.origin_sessions, scope)) and
      match?(%{gone: false}, Map.get(after_engine.origin_sessions, scope))
  end

  def reopen(engine, {person, origin, handle} = scope, act)
      when is_binary(handle) and act in [:accepted_send, :scoped_stream] do
    with {:ok, ^person} <- Engine.authorized_person(engine, origin),
         %{gone: true} = session <- Map.get(engine.origin_sessions, scope) do
      Engine.put_row(engine, :origin_sessions, scope, %{session | gone: false})
    else
      _ -> engine
    end
  end

  def reopen(engine, _, _), do: engine

  def mark_gone(engine, person, origin, handle) do
    key = {person, origin, handle}

    case Map.get(engine.origin_sessions, key) do
      nil ->
        {:error, :not_found}

      session ->
        {:ok, Engine.put_row(engine, :origin_sessions, key, %{session | gone: true})}
    end
  end

  def mark_detached(engine, person, origin, handle) do
    key = {person, origin, handle}

    case Map.get(engine.origin_sessions, key) do
      nil ->
        {:error, :not_found}

      session ->
        updated = %{session | last_detached_at: System.system_time(:millisecond)}
        {:ok, Engine.put_row(engine, :origin_sessions, key, updated)}
    end
  end

  defp needs_action?(engine, person, %{"kind" => "discussion"} = entry) do
    case Map.get(engine.messages, {person, entry["sender_id"], entry["message_id"]}) do
      record when is_map(record) -> true
      _ -> false
    end
  end

  defp needs_action?(_engine, _person, %{"kind" => "terminal", "terminal_state" => "answered"}),
    do: true

  defp needs_action?(_, _, _), do: false

  def append_entry(engine, person, origin, entry) do
    key = {person, origin}
    feed = Map.get(engine.origin_feeds, key, %{high: 0, entries: %{}})
    seq = feed.high + 1

    item =
      entry
      |> Map.put("channel", channel(entry))
      |> Map.merge(%{"feed_seq" => seq, "event_id" => Integer.to_string(seq)})

    by_handle = Map.get(feed, :entry_seqs_by_handle, %{})
    handle = item["session_handle"]

    updated =
      feed
      |> Map.put(:high, seq)
      |> Map.put(:entries, Map.put(feed.entries, seq, item))
      |> Map.put(
        :entry_seqs_by_handle,
        Map.update(by_handle, handle, MapSet.new([seq]), &MapSet.put(&1, seq))
      )

    origin_entries =
      if item["kind"] == "revision" and item["author"] == "origin" and
           is_binary(item["session_handle"]) and is_binary(item["sender_id"]) and
           is_binary(item["message_id"]) do
        MapSet.put(
          engine.bounce_origin_entries,
          {person, origin, item["session_handle"], item["sender_id"], item["message_id"]}
        )
      else
        engine.bounce_origin_entries
      end

    engine =
      %{
        engine
        | origin_feeds: Map.put(engine.origin_feeds, key, updated),
          bounce_origin_entries: origin_entries
      }
      |> Engine.touch_fact({:origin_feed, key, :high})
      |> Engine.touch_fact({:origin_feed, key, {:entry, seq}})

    {engine, item}
  end

  defp valid_delivery_acks?(engine, person, origin, feed) do
    Enum.all?(Map.get(feed, :delivery_acks, %{}), fn {feed_seq, _stages} ->
      valid_delivery_ack?(engine, {person, origin}, feed_seq)
    end)
  end

  @doc false
  def valid_delivery_ack?(engine, {person, origin} = scope, feed_seq) do
    feed = Map.fetch!(engine.origin_feeds, scope)
    entry = Map.fetch!(feed.entries, feed_seq)
    stages = Map.fetch!(feed.delivery_acks, feed_seq)

    entry["author"] == "person" and entry["kind"] in ~w(discussion terminal) and
      Map.has_key?(stages, :ack1) and
      Enum.all?(stages, fn {stage, seq} ->
        fact = Map.fetch!(engine.conversation_events, {person, seq})
        payload = fact.payload

        stage in [:ack1, :ack2] and
          fact.key == {person, entry["sender_id"], entry["message_id"]} and
          engine.messages[fact.key].origin == origin and
          payload["kind"] == "delivery_ack" and payload["stage"] == Atom.to_string(stage) and
          payload["event_id"] == entry["event_id"] and
          payload["request_revision"] == entry["request_revision"] and
          payload["session_handle"] == entry["session_handle"] and
          (stage == :ack1 or seq > stages.ack1)
      end)
  rescue
    _ -> false
  end

  @doc false
  def valid_origin_entry?(seq, high, entry) when is_map(entry) do
    is_integer(seq) and seq > 0 and is_integer(high) and seq <= high and
      entry["feed_seq"] == seq and entry["event_id"] == Integer.to_string(seq) and
      entry["kind"] in ["revision", "discussion", "reply", "terminal", "result"] and
      entry["author"] in ["origin", "person", "agent", "server"] and
      Message.uuid_v4?(entry["session_handle"])
  end

  def valid_origin_entry?(_, _, _), do: false

  @doc false
  def valid_origin_action?(engine, {person, origin, seq}, action) when is_map(action) do
    actionable?(get_in(engine.origin_feeds, [{person, origin}, :entries, seq])) and
      Message.uuid?(Map.get(action, :op_id)) and is_integer(Map.get(action, :at))
  end

  def valid_origin_action?(_, _, _), do: false

  def valid_state?(engine) do
    Enum.all?(engine.origin_feeds, fn {{person, origin}, feed} ->
      get_in(engine.devices, [origin, :person]) == person and
        is_integer(feed.high) and feed.high >= 0 and
        map_size(feed.entries) == feed.high and
        valid_delivery_acks?(engine, person, origin, feed) and
        Enum.all?(feed.entries, fn {seq, entry} -> valid_origin_entry?(seq, feed.high, entry) end)
    end) and
      Enum.all?(engine.origin_sessions, fn {key, session} ->
        valid_origin_session?(engine, key, session)
      end) and
      Enum.all?(engine.origin_receipts, fn {key, receipt} ->
        valid_origin_receipt?(engine, key, receipt)
      end) and
      Enum.all?(engine.origin_actions, fn {key, action} ->
        valid_origin_action?(engine, key, action)
      end)
  rescue
    _ -> false
  end

  @doc false
  def valid_origin_session?(engine, {person, origin, handle}, session) do
    get_in(engine.devices, [origin, :person]) == person and
      valid_session?(handle, session.vendor_session_id) and
      valid_support_record?(Map.get(session, :support)) and
      (is_nil(Map.get(session, :support)) or session.support["origin"] == origin) and
      is_boolean(session.gone) and
      (is_nil(session.last_detached_at) or
         (is_integer(session.last_detached_at) and session.last_detached_at > 0))
  end

  @doc false
  def valid_origin_receipt?(engine, {person, origin, seq}, receipt) do
    entry = get_in(engine.origin_feeds, [{person, origin}, :entries, seq])

    actionable?(entry) and is_integer(receipt.delivered_at) and receipt.delivered_at > 0 and
      valid_receipt_transfers?(engine, person, origin, entry, receipt) and
      is_map(Map.get(receipt, :uses, %{})) and
      Enum.all?(Map.get(receipt, :uses, %{}), fn {name, outcome} ->
        Enum.any?(Map.get(entry, "files", []), &(&1["name"] == name)) and
          ConversationFiles.valid_use?(outcome)
      end)
  end

  defp valid_receipt_transfers?(
         engine,
         person,
         origin,
         entry,
         %{implicit_ack: true, transfers: %{}} = receipt
       ) do
    files = MapSet.new(Map.get(entry, "files", []), & &1["name"])

    is_map(AttentionServer.Engine.delivery_ack_fact(engine, person, origin, entry, :ack1)) and
      Enum.all?(receipt.transfers, fn {name, report} ->
        MapSet.member?(files, name) and ConversationFiles.valid_transfer_report?(name, report)
      end)
  end

  defp valid_receipt_transfers?(_engine, _person, _origin, entry, receipt),
    do: ConversationFiles.valid_transfer_state?(entry, Map.get(receipt, :transfers, %{}))
end
