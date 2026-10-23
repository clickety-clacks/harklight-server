defmodule AttentionServer.Conversation do
  @moduledoc """
  Durable origin feed for request-bound conversations. Entries are immutable;
  delivery and action facts live separately so replay never changes a turn.
  """

  alias AttentionServer.{ConversationFiles, Message}

  def valid_session?(handle, vendor_id) do
    Message.uuid_v4?(handle) and
      (is_nil(vendor_id) or
         (is_binary(vendor_id) and vendor_id != "" and String.valid?(vendor_id)))
  end

  @support_fields ~w(route cli_version unsupported_reason)

  def support_from_frame(frame) do
    if Enum.any?(@support_fields, &Map.has_key?(frame, &1)) do
      route = frame["route"]
      version = frame["cli_version"]
      reason = frame["unsupported_reason"]

      if route in ~w(claude-code codex other) and is_binary(version) and version != "" and
           String.valid?(version) and Map.has_key?(frame, "unsupported_reason") and
           (is_nil(reason) or (is_binary(reason) and reason != "" and String.valid?(reason))) do
        {:ok, Map.take(frame, @support_fields)}
      else
        {:error, "listening_support"}
      end
    else
      {:ok, nil}
    end
  end

  def store_support(engine, person, origin, handle, declared) when is_map(declared) do
    key = {person, origin, handle}

    case Map.get(engine.origin_sessions, key) do
      nil ->
        {:error, :not_found}

      session ->
        record =
          declared
          |> Map.put("origin", origin)
          |> Map.put("reported_at", System.system_time(:millisecond))

        {:ok,
         %{
           engine
           | origin_sessions:
               Map.put(engine.origin_sessions, key, Map.put(session, :support, record))
         }, record}
    end
  end

  def valid_support_record?(nil), do: true

  def valid_support_record?(%{"origin" => origin, "reported_at" => at} = record) do
    is_binary(origin) and origin != "" and is_integer(at) and at > 0 and
      Map.keys(record) |> Enum.sort() ==
        ~w(cli_version origin reported_at route unsupported_reason) and
      match?({:ok, _}, support_from_frame(record))
  end

  def valid_support_record?(_), do: false

  def on_send(engine, person, origin, record, {_, sender, message_id}) do
    handle = record.session_handle
    session_key = {person, origin, handle}

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
    |> Map.update!(:origin_sessions, &Map.put(&1, session_key, session))
    |> append_entry(person, origin, entry)
  end

  def terminal_author(record, terminal) do
    cond do
      terminal.actor_kind == :agent -> "agent"
      terminal.actor == record.origin -> "origin"
      terminal.state in ["expired", "superseded"] -> "server"
      true -> "person"
    end
  end

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

  def fetch(engine, person, origin, since) when is_integer(since) and since >= 0 do
    feed = Map.get(engine.origin_feeds, {person, origin}, %{high: 0, entries: %{}})

    if since <= feed.high do
      entries =
        if since == feed.high,
          do: [],
          else:
            Enum.map((since + 1)..feed.high, fn seq ->
              entry = Map.fetch!(feed.entries, seq)
              key = {person, origin, seq}

              if actionable?(entry) do
                receipt = Map.get(engine.origin_receipts, key)

                entry
                |> Map.put("delivered", not is_nil(receipt))
                |> Map.put("acted", Map.has_key?(engine.origin_actions, key))
                |> then(fn item ->
                  if Map.get(entry, "files", []) == [] do
                    item
                  else
                    item
                    |> Map.put(
                      "file_transfers",
                      if(receipt, do: Map.get(receipt, :transfers, %{}), else: %{})
                    )
                    |> Map.put(
                      "file_use",
                      if(receipt, do: Map.get(receipt, :uses, %{}), else: %{})
                    )
                  end
                end)
              else
                entry
              end
            end)

      {:ok, %{"cursor" => feed.high, "entries" => entries}}
    else
      {:error, :invalid_cursor}
    end
  end

  def fetch(_, _, _, _), do: {:error, :invalid_cursor}

  def ack(engine, person, origin, feed_seq, reports \\ nil)

  def ack(engine, person, origin, feed_seq, reports) when is_integer(feed_seq) and feed_seq > 0 do
    feed = Map.get(engine.origin_feeds, {person, origin}, %{high: 0, entries: %{}})

    case Map.get(feed.entries, feed_seq) do
      %{"kind" => kind, "author" => author} = entry
      when kind in ["discussion", "terminal"] and author != "origin" ->
        key = {person, origin, feed_seq}

        case ConversationFiles.transfers_for(entry, reports) do
          {:ok, transfers} ->
            case Map.get(engine.origin_receipts, key) do
              nil ->
                receipt = %{
                  delivered_at: System.system_time(:millisecond),
                  transfers: transfers,
                  uses: %{}
                }

                {:ok, %{engine | origin_receipts: Map.put(engine.origin_receipts, key, receipt)},
                 %{
                   "status" => "delivered",
                   "event_id" => entry["event_id"],
                   "acted" => Map.has_key?(engine.origin_actions, key),
                   "file_transfers" => transfers
                 }}

              receipt ->
                if Map.get(receipt, :transfers, %{}) == transfers do
                  {:ok, engine,
                   %{
                     "status" => "already_delivered",
                     "event_id" => entry["event_id"],
                     "acted" => Map.has_key?(engine.origin_actions, key),
                     "file_transfers" => transfers
                   }}
                else
                  {:error, :transfer_conflict}
                end
            end

          error ->
            error
        end

      _ ->
        {:error, :not_found}
    end
  end

  def ack(_, _, _, _, _), do: {:error, :not_found}

  def file_use(engine, person, origin, feed_seq, name, outcome)
      when is_integer(feed_seq) and feed_seq > 0 and is_binary(name) do
    key = {person, origin, feed_seq}
    entry = get_in(engine.origin_feeds, [{person, origin}, :entries, feed_seq])
    receipt = Map.get(engine.origin_receipts, key)

    cond do
      is_nil(receipt) or is_nil(entry) ->
        {:error, :not_found}

      not Enum.any?(Map.get(entry, "files", []), &(&1["name"] == name)) ->
        {:error, :not_found}

      not ConversationFiles.valid_use?(outcome) ->
        {:error, :file_use}

      true ->
        uses = Map.get(receipt, :uses, %{})

        case Map.get(uses, name) do
          nil ->
            next_receipt = Map.put(receipt, :uses, Map.put(uses, name, outcome))
            next = %{engine | origin_receipts: Map.put(engine.origin_receipts, key, next_receipt)}

            {:ok, next,
             %{"status" => outcome["status"], "event_id" => entry["event_id"], "file" => name}}

          ^outcome ->
            {:ok, engine,
             %{
               "status" => outcome["status"],
               "event_id" => entry["event_id"],
               "file" => name,
               "duplicate" => true
             }}

          _ ->
            {:error, :file_use_conflict}
        end
    end
  end

  def file_use(_, _, _, _, _, _), do: {:error, :not_found}

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
      {:ok, %{engine | origin_actions: Map.put(engine.origin_actions, key, action)}}
    end
  end

  def status(engine, person, origin, handle, listening?) do
    case Map.get(engine.origin_sessions, {person, origin, handle}) do
      nil ->
        {:error, :not_found}

      session ->
        feed = Map.get(engine.origin_feeds, {person, origin}, %{high: 0, entries: %{}})

        open_requests =
          feed.entries
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

        {undelivered, delivered_unacted} =
          Enum.reduce(feed.entries, {0, 0}, fn {seq, entry}, {queued, waiting} ->
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
           "cursor" => feed.high
         }}
    end
  end

  def mark_gone(engine, person, origin, handle) do
    key = {person, origin, handle}

    case Map.get(engine.origin_sessions, key) do
      nil ->
        {:error, :not_found}

      session ->
        {:ok,
         %{
           engine
           | origin_sessions: Map.put(engine.origin_sessions, key, %{session | gone: true})
         }}
    end
  end

  def mark_attached(engine, person, origin, handle) do
    key = {person, origin, handle}

    case Map.get(engine.origin_sessions, key) do
      nil ->
        {:error, :not_found}

      session ->
        {:ok,
         %{
           engine
           | origin_sessions: Map.put(engine.origin_sessions, key, %{session | gone: false})
         }}
    end
  end

  def mark_detached(engine, person, origin, handle) do
    key = {person, origin, handle}

    case Map.get(engine.origin_sessions, key) do
      nil ->
        {:error, :not_found}

      session ->
        updated = %{session | last_detached_at: System.system_time(:millisecond)}
        {:ok, %{engine | origin_sessions: Map.put(engine.origin_sessions, key, updated)}}
    end
  end

  defp needs_action?(engine, person, %{"kind" => "discussion"} = entry) do
    case Map.get(engine.messages, {person, entry["sender_id"], entry["message_id"]}) do
      %{terminal: nil} -> true
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
    item = Map.merge(entry, %{"feed_seq" => seq, "event_id" => Integer.to_string(seq)})
    updated = %{feed | high: seq, entries: Map.put(feed.entries, seq, item)}
    {%{engine | origin_feeds: Map.put(engine.origin_feeds, key, updated)}, item}
  end

  def valid_state?(engine) do
    Enum.all?(engine.origin_feeds, fn {{person, origin}, feed} ->
      get_in(engine.devices, [origin, :person]) == person and
        is_integer(feed.high) and feed.high >= 0 and
        map_size(feed.entries) == feed.high and
        Enum.all?(feed.entries, fn {seq, entry} ->
          is_integer(seq) and seq > 0 and seq <= feed.high and
            entry["feed_seq"] == seq and entry["event_id"] == Integer.to_string(seq) and
            entry["kind"] in ["revision", "discussion", "reply", "terminal", "result"] and
            entry["author"] in ["origin", "person", "agent", "server"] and
            Message.uuid_v4?(entry["session_handle"])
        end)
    end) and
      Enum.all?(engine.origin_sessions, fn {{person, origin, handle}, session} ->
        get_in(engine.devices, [origin, :person]) == person and
          valid_session?(handle, session.vendor_session_id) and
          valid_support_record?(Map.get(session, :support)) and
          (is_nil(Map.get(session, :support)) or session.support["origin"] == origin) and
          is_boolean(session.gone) and
          (is_nil(session.last_detached_at) or
             (is_integer(session.last_detached_at) and session.last_detached_at > 0))
      end) and
      Enum.all?(engine.origin_receipts, fn {{person, origin, seq}, receipt} ->
        entry = get_in(engine.origin_feeds, [{person, origin}, :entries, seq])

        actionable?(entry) and is_integer(receipt.delivered_at) and receipt.delivered_at > 0 and
          ConversationFiles.valid_transfer_state?(entry, Map.get(receipt, :transfers, %{})) and
          is_map(Map.get(receipt, :uses, %{})) and
          Enum.all?(Map.get(receipt, :uses, %{}), fn {name, outcome} ->
            Enum.any?(Map.get(entry, "files", []), &(&1["name"] == name)) and
              ConversationFiles.valid_use?(outcome)
          end)
      end) and
      Enum.all?(engine.origin_actions, fn {{person, origin, seq}, action} ->
        actionable?(get_in(engine.origin_feeds, [{person, origin}, :entries, seq])) and
          Message.uuid_v4?(action.op_id) and is_integer(action.at)
      end)
  rescue
    _ -> false
  end
end
