defmodule AttentionServer.ClientAPI.SharedChat do
  @moduledoc false

  alias AttentionServer.{AgentChangeLog, ChangeLog, Engine, Relay, SharedAnswers, SharedMessages}
  alias AttentionServer.ClientAPI.{Errors, Idempotency, PeopleProfiles, Reads, SharedAttachments, SharedCapability, Writes}

  def respond(conn, ctx, direction, operation, body, key) do
    case SharedCapability.parse(conn) do
      {:ok, %{shared?: true} = capability} ->
        digest = Idempotency.digest(conn.method, conn.path_info, body)
        request = {ctx.principal, ctx.params, direction, operation, body, key, digest, capability}
        result = execute(ctx.relay, request)
        case result do
          {:answer_conflict, terminal} ->
            Errors.send_error(conn, "already_answered", details: %{"terminal" => terminal})
          _ ->
            Writes.respond(conn, result)
        end

      {:ok, _} -> Errors.send_error(conn, "not_found")
      {:error, token, field} -> Errors.send_error(conn, token, field: field)
    end
  end

  defp execute(relay, {_, _, _, :reply, %{"file_ids" => [_ | _]}, _, _, _} = request) do
    case call(relay, {:prepare, request}) do
      {:ok, dir, files} ->
        case SharedAttachments.stage_and_apply(dir, files, &call(relay, {:apply, request, &1})) do
          {:error, _} = error -> reply_error(error)
          result -> result
        end

      result -> result
    end
  end

  defp execute(relay, request), do: call(relay, {:apply, request, []})

  defp call(relay, request), do: GenServer.call(relay, {:client_api, __MODULE__, request}, :infinity)

  @doc false
  def handle_call({:prepare, request}, from, state), do: handle_call({:apply, request, :prepare}, from, state)

  def handle_call({:apply, {who, params, direction, operation, body, key, digest, capability}, staged}, _from, state) do
    engine = state.store.engine

    with {:ok, principal} <- Reads.client_principal(engine, who),
         true <- SharedCapability.readable?(state, principal.person, capability) || {:error, :unsupported},
         {:ok, record, recipient} <- locate(engine, principal, direction, params.item_id),
         true <- SharedCapability.visible?(record, capability) || {:error, :not_found},
         scope = wire_scope(principal, direction),
         {:ok, stored} <- lookup(state.store, scope, key) do
      case stored do
        %{digest: ^digest, body: body} ->
          {:reply, {:ok, 200, body, true}, state}

        %{} ->
          {:reply, reply_error({:error, :idempotency_conflict}), state}

        nil ->
          chat = %{
            person: principal.person, direction: direction, operation: operation,
            actor: principal.key, receipt_scope: scope, key: key, digest: digest,
            message_id: record.id, now: state.clock.()
          }

          apply_chat(state, record, recipient, params, body, chat, staged)
      end
    else
      error -> {:reply, reply_error(error), state}
    end
  end

  defp locate(engine, %{kind: kind, person: person}, :person, item)
       when kind in [:device, :browser],
       do: SharedMessages.recipient_message(engine, person, item)

  defp locate(engine, %{kind: :device, person: person, key: actor}, :sender, item) do
    with %{sender_person: ^person, sender_key: ^actor} = record <- engine.shared_messages[item],
         true <- Engine.shared_sender_allowed?(engine, person, actor, record.sender_id) do
      {:ok, record, nil}
    else
      _ -> {:error, :not_found}
    end
  end

  defp locate(_engine, _principal, _direction, _item), do: {:error, :not_found}

  defp wire_scope(principal, :person), do: principal.person
  defp wire_scope(principal, :sender), do: Idempotency.agent_scope(principal.person, principal.key)
  defp lookup(_store, _scope, nil), do: {:ok, nil}
  defp lookup(store, scope, key), do: Idempotency.lookup(store, scope, key)

  defp apply_chat(state, record, recipient, _params, body, chat, :prepare) do
    kind = if chat.direction == :sender, do: "agent", else: "client"
    attrs = %{message_id: record.id, sender: record.sender_id, revision: record.revision,
              op_id: chat.key, now: chat.now, kind: kind}

    result =
      with :ok <- room_authority(state.store.engine, chat, record, recipient),
           :ok <- SharedMessages.replyable(record),
           :ok <- SharedAttachments.validate_ids(body["file_ids"]),
           {:ok, prepared} <- SharedAttachments.prepare(state.store, {chat.person, chat.actor}, body["file_ids"], attrs) do
        {:ok, state.store.upload_dir, prepared}
      else
        error -> reply_error(error)
      end

    {:reply, result, state}
  end

  defp apply_chat(state, record, recipient, params, body, chat, staged) do
    engine = state.store.engine
    staged = Enum.map(staged, fn {metadata, path} ->
      {Map.merge(metadata, %{revision: record.revision, shared_entry_id: entry_id(chat)}), path}
    end)
    chat = if staged == [], do: chat, else: Map.put(chat, :files, SharedAttachments.references(staged))

    with :ok <- room_authority(engine, chat, record, recipient),
         {:ok, next, chat} <- transition(engine, record, recipient, params, body, chat),
         audit = %{person: chat.person, actor: chat.actor, action: "shared_chat_" <> Atom.to_string(chat.operation),
                   shared_chat: chat},
         {:ok, store} <- commit_chat(state.store, engine, next, audit, staged),
         {:ok, response} <- result(store, store.engine, chat) do
      next = %{state | store: store}
      next = if staged == [], do: next, else: Relay.refresh_shared_upload_index(next)
      {:reply, {:ok, 200, response, false}, next}
    else
      {:answer_conflict, _} = conflict -> {:reply, conflict, state}
      error -> {:reply, reply_error(error), state}
    end
  end

  defp room_authority(engine, %{person: person, direction: :person}, record, recipient) do
    if not Map.has_key?(recipient, :frozen) and MapSet.member?(SharedMessages.organizations(engine, person), record.organization_id),
      do: :ok,
      else: {:error, :forbidden}
  end

  defp room_authority(engine, %{person: person, direction: :sender}, record, _recipient) do
    if not Map.has_key?(record, :sender_frozen_revision) and MapSet.member?(SharedMessages.organizations(engine, person), record.organization_id),
      do: :ok,
      else: {:error, :not_found}
  end

  defp transition(engine, record, _recipient, _params, body, %{operation: :reply, direction: direction} = chat) do
    allowed = if direction == :sender, do: ~w(text file_ids notify_person_id), else: ~w(text file_ids)

    with :ok <- Writes.only_members(body, allowed),
         text = body["text"],
         true <- (is_binary(text) and byte_size(text) > 0) || {:error, :invalid_field, "text"},
         :ok <- SharedAttachments.validate_ids(Map.get(body, "file_ids", [])),
         notify = body["notify_person_id"],
         true <- is_nil(notify) or is_binary(notify) || {:error, "invalid_field", "notify_person_id"},
         {author, speaker} = speaker(engine, record, chat),
         id = entry_id(chat),
         attrs = %{id: id, at_ms: chat.now, text: text, notify_person_id: notify, speaker: speaker,
                   files: Map.get(chat, :files, [])},
         {:ok, next, _entry} <- SharedMessages.reply(engine, record, author, attrs) do
      {:ok, next, Map.put(chat, :entry_id, id)}
    end
  end

  defp transition(engine, record, recipient, _params, %{"request_revision" => revision} = body,
         %{operation: :answer} = chat) do
    with {:ok, value} <- Writes.answer_value(body),
         {:ok, next, answer_id} <-
           SharedAnswers.answer(engine, record, recipient, chat.person, revision, value, chat.now) do
      {:ok, next, Map.put(chat, :answer_id, answer_id)}
    end
  end

  defp transition(engine, record, _recipient, %{answer_id: id}, body, %{operation: :shared_answer_receipt} = chat) do
    with {:ok, handoff} <- handoff(body),
         %{shared_id: shared_id} = answer when shared_id == record.id <- engine.shared_answers[id],
         {:ok, next, outcome} <- SharedAnswers.receipt(engine, answer, handoff) do
      {:ok, next, Map.merge(chat, %{answer_id: id, outcome: outcome})}
    else
      result when is_nil(result) or is_map(result) -> {:error, :not_found}
      error -> error
    end
  end

  defp transition(engine, record, recipient, _params, %{"request_revision" => revision}, %{operation: :seen} = chat)
       when is_integer(revision) and revision > 0 do
    with {:ok, next, event_id} <-
           SharedMessages.seen(engine, record, chat.person, recipient, revision, chat.now),
      do: {:ok, next, Map.put(chat, :event_id, event_id)}
  end

  defp transition(engine, record, recipient, _params, %{"entry_id" => entry}, %{operation: :read_position} = chat) do
    with {:ok, next} <- SharedMessages.read_position(engine, record, chat.person, recipient, entry),
      do: {:ok, next, chat}
  end

  defp transition(engine, record, _recipient, %{entry_id: id}, body, %{operation: :chat_receipt} = chat) do
    with {:ok, handoff} <- handoff(body),
         %{shared_id: shared_id} = entry when shared_id == record.id <- engine.shared_entries[id],
         {:ok, next, outcome} <- SharedMessages.receipt(engine, entry, handoff) do
      {:ok, next, Map.merge(chat, %{entry_id: id, outcome: outcome})}
    else
      result when is_nil(result) or is_map(result) -> {:error, :not_found}
      error -> error
    end
  end

  defp transition(_engine, _record, _recipient, _params, _body, _chat),
    do: {:error, :invalid_field, "body"}

  defp handoff(%{"outcome" => outcome} = body) when outcome in ["handed", "not_handed"] do
    reason = body["reason"]
    actor = body["agent"]

    with :ok <- Writes.only_members(body, ~w(outcome reason agent rerouted)),
         true <- is_nil(actor) or (is_binary(actor) and String.trim(actor) != ""),
         true <- if(outcome == "handed", do: not Map.has_key?(body, "reason"),
                    else: is_binary(reason) and String.trim(reason) != ""),
         true <- if(outcome == "handed",
                    do: not Map.has_key?(body, "rerouted") or is_boolean(body["rerouted"]),
                    else: not Map.has_key?(body, "rerouted")) do
      {:ok, body}
    else
      _ -> {:error, :invalid_field, "outcome"}
    end
  end
  defp handoff(_body), do: {:error, :invalid_field, "outcome"}

  defp speaker(engine, _record, %{direction: :person, person: person}),
    do: {{:person, person}, PeopleProfiles.speaker(engine, person)}

  defp speaker(_engine, record, %{direction: :sender}) do
    id = digest_id({record.sender_person, record.sender_key, record.sender_id})
    display = if is_binary(record.message["display_name"]), do: record.message["display_name"], else: record.sender_id
    {:sender, %{"kind" => "sender", "id" => id, "display_name" => display}}
  end

  defp entry_id(chat), do: "se_" <> digest_id({chat.receipt_scope, chat.key})
  defp digest_id(value), do: :crypto.hash(:sha256, :erlang.term_to_binary(value)) |> Base.url_encode64(padding: false)

  defp commit_chat(store, engine, engine, %{shared_chat: chat}, []) do
    if is_binary(chat.key) do
      with {:ok, body} <- result(store, engine, chat),
           :ok <- Idempotency.save(store, chat.receipt_scope, chat.key, chat.digest, 200, body),
        do: {:ok, store}
    else
      {:ok, store}
    end
  end

  defp commit_chat(store, _old, updated, audit, staged),
    do: SharedAttachments.commit(store, updated, audit, staged)

  @doc false
  def result(store, engine, %{direction: :person, operation: operation} = chat)
      when operation in [:read_position, :seen, :answer] do
    record = engine.shared_messages[chat.message_id]
    recipient = engine.shared_recipients[{record.id, chat.person}]
    alias AttentionServer.ChangeLog, as: Log

    with {:ok, head} <- Log.head(store, chat.person),
         {:ok, rev} <- Log.rev(store, chat.person, Log.shared_item_resource(record.id)) do
      {:ok, %{"rev" => head, "item" => SharedMessages.person_item(engine, chat.person, record, recipient, rev)}}
    end
  end

  def result(store, engine, %{direction: :person, entry_id: id} = chat) do
    entry = engine.shared_entries[id]
    recipient = engine.shared_recipients[{entry.shared_id, chat.person}]

    with {:ok, head} <- ChangeLog.head(store, chat.person),
         {:ok, rev} <- ChangeLog.rev(store, chat.person, ChangeLog.shared_entry_resource(id)) do
      {:ok, %{"rev" => head, "entry" => SharedMessages.person_entry(recipient, chat.person, entry, rev)}}
    end
  end

  def result(store, engine, %{direction: :sender, answer_id: id} = chat) do
    answer = engine.shared_answers[id]
    record = engine.shared_messages[answer.shared_id]
    scope = {record.sender_person, record.sender_key}

    with {:ok, head} <- AgentChangeLog.head(store, scope) do
      body = %{"rev" => head, "terminal" => SharedAnswers.sender_terminal(answer)}
      {:ok, Map.merge(body, chat.outcome)}
    end
  end

  def result(store, engine, %{direction: :sender, entry_id: id} = chat) do
    entry = engine.shared_entries[id]
    record = engine.shared_messages[entry.shared_id]
    scope = {record.sender_person, record.sender_key}

    with {:ok, head} <- AgentChangeLog.head(store, scope),
         {:ok, rev} <- AgentChangeLog.rev(store, scope, {:shared_entry, id}) do
      body = %{"rev" => head, "entry" => SharedMessages.sender_entry(record, entry, rev)}
      {:ok, Map.merge(body, Map.get(chat, :outcome, %{}))}
    end
  end

  defp reply_error({:error, token, field}), do: {:error, to_string(token), field, false}
  defp reply_error({:error, token}) when is_atom(token), do: {:error, to_string(token), nil, false}
  defp reply_error(_error), do: {:error, "storage_unavailable", nil, false}
end
