defmodule AttentionServer.ClientAPI.AgentWrites do
  @moduledoc "Agent message mutations on the device listener."

  @behaviour AttentionServer.ClientAPI.Routes

  import Plug.Conn, only: [get_req_header: 2, read_body: 1]

  alias AttentionServer.{AgentChangeLog, ConversationFiles, Engine, JSON, Message, Relay}

  alias AttentionServer.ClientAPI.{
    AgentResources,
    Errors,
    Idempotency,
    Resources,
    Uploads,
    Writes
  }

  @lifecycle ~w(answered cleared expired superseded)
  @members %{
    send: ~w(sender session message file_ids supersedes),
    revise: ~w(message file_ids in_reply_to),
    retract: ~w(in_reply_to),
    lifecycle: ~w(action),
    reply: ~w(text file_ids in_reply_to),
    result: ~w(status),
    answer_receipt: ~w(outcome reason agent rerouted),
    chat_receipt: ~w(outcome reason agent rerouted),
    file_use: ~w(file_id transfer use reason),
    advisory_ack: ~w(advisory_id),
    session_end: []
  }

  @impl true
  def routes do
    [
      {"POST", ["agent", "messages"], :send_message},
      {"PUT", ["agent", "messages", :item_id], :revise},
      {"POST", ["agent", "messages", :item_id, "retract"], :retract},
      {"POST", ["agent", "messages", :item_id, "lifecycle"], :lifecycle},
      {"POST", ["agent", "messages", :item_id, "replies"], :reply},
      {"POST", ["agent", "messages", :item_id, "result"], :result},
      {"POST", ["agent", "messages", :item_id, "receipt"], :answer_receipt},
      {"POST", ["agent", "messages", :item_id, "entries", :entry_id, "receipt"], :chat_receipt},
      {"POST", ["agent", "sessions", :session, "end"], :session_end},
      {"POST", ["agent", "messages", :item_id, "entries", :entry_id, "file-use"], :file_use},
      {"POST", ["agent", "messages", :item_id, "advisories", "ack"], :advisory_ack}
    ]
  end

  def send_message(conn, ctx), do: request(conn, ctx, :send)
  def revise(conn, ctx), do: request(conn, ctx, :revise)
  def retract(conn, ctx), do: request(conn, ctx, :retract)
  def lifecycle(conn, ctx), do: request(conn, ctx, :lifecycle)
  def reply(conn, ctx), do: request(conn, ctx, :reply)
  def result(conn, ctx), do: request(conn, ctx, :result)
  def answer_receipt(conn, ctx), do: request(conn, ctx, :answer_receipt)
  def chat_receipt(conn, ctx), do: request(conn, ctx, :chat_receipt)
  def session_end(conn, ctx), do: request(conn, ctx, :session_end)
  def file_use(conn, ctx), do: request(conn, ctx, :file_use)
  def advisory_ack(conn, ctx), do: request(conn, ctx, :advisory_ack)

  defp request(conn, ctx, operation) do
    with :ok <- Writes.device_only(ctx.principal),
         {:ok, key} <- request_key(conn),
         {:ok, body, conn} <- json_body(conn, []),
         :ok <- Writes.only_members(body, Map.fetch!(@members, operation)) do
      precondition = get_req_header(conn, "if-match")
      target = ctx.params

      digest =
        Idempotency.digest(conn.method, conn.path_info, %{
          "body" => body,
          "if_match" => precondition
        })

      op_id = key || ConversationFiles.new_upload_id()
      input = {ctx.principal, operation, target, key, digest, precondition, body}

      result =
        case prepare_uploads(ctx, input, op_id) do
          {:ok, uploads} -> call(ctx, {:execute, input, op_id, uploads})
          error -> refusal(error)
        end

      Writes.respond(conn, result)
    else
      {:error, token, field, response} -> Errors.send_error(response, token, field: field)
      {:error, token, field} -> Errors.send_error(conn, token, field: field)
    end
  end

  defp call(ctx, request),
    do: GenServer.call(ctx.relay, {:client_api, __MODULE__, request}, :infinity)

  defp prepare_uploads(ctx, {_, _, _, _, _, _, %{"file_ids" => [_ | _]}} = input, op_id) do
    case call(ctx, {:prepare_uploads, input, op_id}) do
      {:ok, frame, staged} ->
        with {:ok, prepared} <- Writes.attach_files(ctx.relay, ctx.principal.key, frame, staged),
             do: {:ok, Map.get(prepared, "files", [])}

      {:ok, :replay} ->
        {:ok, []}

      error ->
        error
    end
  end

  defp prepare_uploads(_ctx, _input, _op_id), do: {:ok, []}

  defp request_key(%{method: "POST"} = conn), do: Writes.required_key(conn)
  defp request_key(conn), do: Writes.optional_key(conn)

  def json_body(conn, chunks) do
    case read_body(conn) do
      {:ok, chunk, conn} ->
        raw = [chunk | chunks] |> Enum.reverse() |> IO.iodata_to_binary()
        raw = if raw == "", do: "{}", else: raw

        case JSON.decode_line_for_ui1_admission(raw <> "\n") do
          {:ok, body} when is_map(body) and not is_struct(body) -> {:ok, body, conn}
          _ -> {:error, "invalid_field", nil, conn}
        end

      {:more, chunk, conn} ->
        json_body(conn, [chunk | chunks])

      {:error, _} ->
        {:error, "invalid_field", nil}
    end
  end

  @doc false
  def handle_call(
        {:execute, {%{person: person, key: agent}, operation, item_id, key, digest, match, body},
         op_id, uploads},
        _from,
        state
      ) do
    scope = {person, agent}
    receipt_scope = receipt_scope(scope)

    case authorize(state, scope) do
      :ok ->
        case lookup(state, receipt_scope, key) do
          {:ok, %{digest: ^digest, status: status, body: stored}} ->
            {:reply, {:ok, status, stored, true}, state}

          {:ok, %{}} ->
            {:reply, {:error, "idempotency_conflict", nil, false}, state}

          {:ok, nil} ->
            apply_operation(
              state,
              scope,
              receipt_scope,
              operation,
              item_id,
              key,
              digest,
              match,
              body,
              op_id,
              uploads
            )

          _ ->
            {:reply, {:error, "storage_unavailable", nil, false}, state}
        end

      {:error, reason} ->
        {:reply, {:error, to_string(reason), nil, false}, state}
    end
  end

  def handle_call(
        {:prepare_uploads,
         {%{person: person, key: agent}, operation, target, key, digest, match, body}, op_id},
        _from,
        state
      ) do
    scope = {person, agent}

    reply =
      with :ok <- authorize(state, scope),
           {:ok, nil} <- lookup(state, receipt_scope(scope), key),
           {:ok, {_, sender, id} = message_key, attributes, _} <-
             frame(state, scope, operation, target, op_id, match, body),
           {:ok, selected} <- select_files(state, scope, operation, message_key, body) do
        staged = for {:staged, file} <- selected, do: file
        revision =
          case operation do
            :send -> 1
            :revise -> attributes["base_revision"] + 1
            :reply -> attributes["request_revision"]
          end

        attrs = %{
          "sender_id" => sender,
          "message_id" => id,
          "request_revision" => revision,
          "op_id" => op_id
        }

        {:ok, attrs, staged}
      else
        {:ok, %{digest: ^digest}} -> {:ok, :replay}
        {:ok, %{}} -> {:error, :idempotency_conflict}
        error -> error
      end

    {:reply, reply, state}
  end

  defp receipt_scope({person, agent}), do: Idempotency.agent_scope(person, agent)

  defp select_files(state, scope, :revise, key, body) do
    record = state.store.engine.messages[key]
    revision = Map.get(record, :revision, 1)
    raw = record |> Map.get(:files_by_revision, %{}) |> Map.get(revision, [])

    if Map.has_key?(body, "file_ids") do
      with {:ok, current} <- Resources.revision(state.store.engine, key, revision) do
        carried = Map.new(Enum.zip(Enum.map(current["files"], & &1["file_id"]), raw))
        select_files(state, scope, body["file_ids"], carried)
      end
    else
      {:ok, Enum.map(raw, &{:carried, &1})}
    end
  end

  defp select_files(state, scope, _operation, _key, body),
    do: select_files(state, scope, Map.get(body, "file_ids", []), %{})

  defp select_files(state, scope, ids, carried) do
    Enum.reduce_while(ids, {:ok, []}, fn id, {:ok, acc} ->
      case Map.fetch(carried, id) do
        {:ok, existing} -> {:cont, {:ok, [{:carried, existing} | acc]}}
        :error ->
          case Uploads.fetch_owned(state.store, scope, id) do
            {:ok, file} -> {:cont, {:ok, [{:staged, file} | acc]}}
            {:error, :not_found} -> {:halt, {:error, "invalid_field", "file_ids"}}
            error -> {:halt, error}
          end
      end
    end)
    |> case do
      {:ok, files} -> {:ok, Enum.reverse(files)}
      error -> error
    end
  end

  defp attach_selected(frame, operation, selected, uploads) do
    staged = Enum.count(selected, &match?({:staged, _}, &1))

    if staged == length(uploads) do
      frame = if uploads == [], do: frame, else: Map.put(frame, "files", uploads)

      if operation == :revise do
        order = Enum.map(selected, fn
          {:carried, file} -> {:carried, file}
          {:staged, _} -> :uploaded
        end)
        {:ok, Map.put(frame, :agent_file_order, order)}
      else
        {:ok, frame}
      end
    else
      {:error, "invalid_field", "file_ids"}
    end
  end

  defp authorize(state, {person, agent}) do
    with true <- state.store.change_log,
         {:ok, ^person} <- Engine.authorized_person(state.store.engine, agent) do
      if Engine.delivery_stopped?(state.store.engine, person),
        do: {:error, :account_paused},
        else: :ok
    else
      false -> {:error, :not_found}
      _ -> {:error, :unauthorized}
    end
  end

  defp lookup(_state, _scope, nil), do: {:ok, nil}
  defp lookup(state, scope, key), do: Idempotency.lookup(state.store, scope, key)

  defp apply_operation(
         state,
         {_, agent} = scope,
         receipt_scope,
         operation,
         item_id,
         key,
         digest,
         match,
         body,
         op_id,
         uploads
       ) do
    case frame(state, scope, operation, item_id, op_id, match, body) do
      {:ok, message_key, attributes, extras} ->
        {reply, next} =
          with {:ok, selected} <- select_files(state, scope, operation, message_key, body),
               {:ok, frame} <- attach_selected(attributes, operation, selected, uploads) do
            staged = for {:staged, file} <- selected, do: file.file_id
            sources = Map.new(Enum.zip(Enum.map(uploads, & &1["upload_id"]), staged))
            apply_frame(state, agent, operation, frame, op_id, sources)
          else
            error -> {error, state}
          end

        response =
          with {:ok, fields} <- reply,
               {:ok, body} <- render(next, scope, operation, message_key, fields, extras),
               :ok <- save(next, receipt_scope, key, digest, body) do
            {:ok, 200, body, false}
          else
            error -> refusal(error)
          end

        {:reply, response, next}

      error ->
        {:reply, refusal(error), state}
    end
  end

  defp apply_frame(state, agent, operation, attributes, op_id, sources) when is_map(attributes) do
    frame = Map.merge(attributes, %{"v" => 1, "op_id" => op_id, :agent_api_v2 => true})
    {reply, next} = Relay.apply_agent_frame(state, agent, frame, sources)
    {decode_reply(reply, operation), next}
  end

  defp apply_frame(state, agent, _operation, {:receipt, sequence, body}, op_id, _sources) do
    {:reply, reply, next} =
      Relay.apply_agent_command(
        state,
        {:origin_ack2, agent, sequence, Map.put(body, "op_id", op_id)}
      )

    {reply, next}
  end

  defp apply_frame(state, agent, _operation, {:session_end, session}, _op_id, _sources) do
    {:reply, reply, next} =
      Relay.apply_agent_command(state, {:origin_session_gone, agent, session, :agent_v2})

    {if(reply == :ok, do: {:ok, %{}}, else: reply), next}
  end

  defp apply_frame(state, agent, _operation, {:file_use, sequence, name, outcome}, op_id, _sources) do
    case Relay.apply_agent_file_use(state, agent, sequence, name, outcome, op_id) do
      {{:error, :file_use_conflict}, next} -> {{:error, :already_closed}, next}
      {{:error, :file_use}, next} -> {{:error, "invalid_field", "outcome"}, next}
      result -> result
    end
  end

  defp apply_frame(state, agent, _operation, {:advisory_ack, session, id}, _op_id, _sources),
    do: Relay.apply_agent_advisory_ack(state, agent, session, id)

  defp frame(_state, _scope, :session_end, %{session: session}, _op, _match, _body) do
    if Message.uuid_v4?(session),
      do: {:ok, nil, {:session_end, session}, nil},
      else: {:error, "invalid_field", "session"}
  end

  defp frame(state, {person, agent} = scope, :send, _, op_id, _match, body) do
    sender = body["sender"]

    with true <- Message.sender_id?(sender),
         {:ok, message} <- content(body),
         :ok <- session(body),
         :ok <- files(body),
         {:ok, targets, warnings} <- supersedes(state, scope, sender, body) do
      id =
        :crypto.hash(:sha256, :erlang.term_to_binary({agent, op_id}))
        |> Base.url_encode64(padding: false)

      message = message |> Map.merge(%{"sender_id" => sender, "message_id" => id})
      message = if targets == [], do: message, else: Map.put(message, "supersedes", targets)
      frame = %{"type" => "forward", "message" => message}

      frame =
        if body["session"], do: Map.put(frame, "session_handle", body["session"]), else: frame

      {:ok, {person, sender, id}, frame, warnings}
    else
      false -> {:error, "invalid_field", "sender"}
      error -> error
    end
  end

  defp frame(state, scope, operation, %{item_id: item_id} = params, _op_id, match, body) do
    with {:ok, {_, sender, id} = key} <- AgentResources.own_item_key(state, scope, item_id),
         record = Map.fetch!(state.store.engine.messages, key),
         {:ok, fields, extras} <-
           item_frame(
             state,
             key,
             record,
             operation,
             match,
             Map.put(body, :entry_id, params[:entry_id])
           ) do
      frame =
        if is_map(fields),
          do: Map.merge(fields, %{"sender_id" => sender, "message_id" => id}),
          else: fields

      {:ok, key, frame, extras}
    end
  end

  defp item_frame(state, {_, sender, id} = key, record, :revise, match, body) do
    with :ok <- open(record),
         :ok <- if_match(match, record.revision),
         {:ok, message} <- content(body),
         :ok <- files(body),
         {:ok, correlation, position} <- correlation(state, key, record, body) do
      frame = %{
        "type" => "conversation_revise",
        "message" => Map.merge(message, %{"sender_id" => sender, "message_id" => id}),
        "base_revision" => record.revision
      }
      {:ok, Map.merge(frame, correlation), position}
    end
  end

  defp item_frame(state, key, record, :retract, _match, body) do
    with :ok <- open(record),
         {:ok, correlation, position} <- correlation(state, key, record, body),
         {:ok, frame} <- retract_frame(state, record, correlation) do
      {:ok, frame, position}
    end
  end

  defp item_frame(state, _key, record, :lifecycle, _match, body) do
    with true <- body["action"] in @lifecycle,
         nil <- record.terminal do
      {:ok, %{"type" => "lifecycle", "state" => body["action"], "at" => state.clock.()}, nil}
    else
      false -> {:error, "invalid_field", "action"}
      %{} -> {:error, :already_closed}
    end
  end

  defp item_frame(state, key, record, :reply, _match, body) do
    with true <- is_binary(record.session_handle),
         :ok <- files(body),
         {:ok, correlation, _position} <- correlation(state, key, record, body) do
      frame =
        Map.merge(
          %{"type" => "conversation_reply", "request_revision" => record.revision},
          correlation
        )

      {:ok, Map.merge(frame, Map.take(body, ["text"])), nil}
    else
      false -> {:error, :unsupported}
      error -> error
    end
  end

  defp item_frame(state, key, record, :result, _match, body) do
    with :ok <- supports_chat(record),
         {:ok, sequence} <- answer_target(state, key, record),
         true <- body["status"] in ~w(received applied refused) do
      {:ok,
       %{"type" => "conversation_result", "in_reply_to" => sequence, "status" => body["status"]},
       nil}
    else
      false -> {:error, "invalid_field", "status"}
      error -> error
    end
  end

  defp item_frame(state, key, record, :answer_receipt, _match, body) do
    with :ok <- supports_chat(record),
         {:ok, sequence} <- answer_target(state, key, record) do
      {:ok, {:receipt, sequence, Map.delete(body, :entry_id)}, nil}
    end
  end

  defp item_frame(state, key, record, :chat_receipt, _match, body) do
    with :ok <- supports_chat(record),
         {:ok, sequence, position, _} <- person_turn(state, key, body[:entry_id]) do
      {:ok, {:receipt, sequence, Map.delete(body, :entry_id)}, position}
    end
  end

  defp item_frame(state, {person, _, _} = key, record, :file_use, _match, body) do
    with :ok <- supports_chat(record),
         {:ok, sequence, position, _} <- person_turn(state, key, body[:entry_id]),
         {:ok, entry} <- AgentResources.entry(state, {person, record.origin}, key, position),
         file when is_map(file) <-
           Enum.find(entry["files"] || [], &(&1["file_id"] == body["file_id"])),
         {:ok, report} <- file_report(body, file["name"]) do
      {:ok, {:file_use, sequence, file["name"], report}, {position, file["file_id"]}}
    else
      nil -> {:error, :not_found}
      false -> {:error, "invalid_field", "outcome"}
      error -> error
    end
  end

  defp item_frame(state, {person, sender, message} = _key, record, :advisory_ack, _match, body) do
    id = body["advisory_id"]
    session = record.session_handle
    feed_key = {person, record.origin, session}
    index = state.store.engine.advisory_id_index

    if Message.uuid_v4?(id) do
      with {:ok, sequence} <- Map.fetch(index, {feed_key, id}),
           %{"sender_id" => ^sender, "message_id" => ^message} <-
             get_in(state.store.engine.advisory_feeds, [feed_key, :entries, sequence]) do
        {:ok, {:advisory_ack, session, id}, nil}
      else
        _ -> {:error, :not_found}
      end
    else
      {:error, "invalid_field", "advisory_id"}
    end
  end

  defp file_report(body, name) do
    dimensions = Map.take(body, ["transfer", "use"])
    reason = body["reason"]
    failures = ["not_transferred", "unusable"]

    cond do
      map_size(dimensions) == 0 ->
        {:error, "invalid_field", "transfer"}

      Map.has_key?(body, "reason") and
          not Enum.any?(dimensions, fn {_, status} -> status in failures end) ->
        {:error, "invalid_field", "reason"}

      true ->
        Enum.reduce_while(dimensions, {:ok, %{}}, fn {field, status}, {:ok, acc} ->
          report = %{"status" => status}
          report = if status in failures, do: Map.put(report, "reason", reason), else: report

          valid =
            case field do
              "transfer" -> ConversationFiles.valid_transfer_report?(name, report)
              "use" -> ConversationFiles.valid_use?(report)
            end

          if valid,
            do: {:cont, {:ok, Map.put(acc, field, report)}},
            else: {:halt, {:error, "invalid_field", field}}
        end)
    end
  end

  defp supports_chat(%{session_handle: handle}) when is_binary(handle), do: :ok
  defp supports_chat(_record), do: {:error, :unsupported}

  defp answer_target(state, {person, _, _}, %{
         terminal: %{state: "answered", seq: seq},
         origin: agent
       }) do
    engine = state.store.engine
    event = get_in(engine.events, [{person, seq}, :payload, "event_id"])

    with id when is_binary(id) <- event,
         {sequence, ""} <- Integer.parse(id),
         %{"author" => "person", "kind" => "terminal", "terminal_seq" => ^seq} <-
           get_in(engine.origin_feeds, [{person, agent}, :entries, sequence]) do
      {:ok, sequence}
    else
      _ -> {:error, :not_answered}
    end
  end

  defp answer_target(_state, _key, _record), do: {:error, :not_answered}

  defp open(%{terminal: nil}), do: :ok
  defp open(%{terminal: %{state: "retracted"}}), do: {:error, :already_retracted}
  defp open(_record), do: {:error, :already_closed}

  defp if_match([supplied], revision) do
    value = Integer.to_string(revision)
    if supplied in [value, ~s("#{value}")], do: :ok, else: {:error, :precondition_failed}
  end

  defp if_match(_headers, _revision), do: {:error, :precondition_failed}

  defp retract_frame(state, %{session_handle: nil}, correlation) when map_size(correlation) == 0,
    do:
      {:ok,
       %{
         "type" => "lifecycle",
         "state" => "retracted",
         "at" => state.clock.(),
         "by" => "origin_withdrawal"
       }}

  defp retract_frame(_state, _record, correlation),
    do: {:ok, Map.put(correlation, "type", "conversation_withdraw")}

  defp correlation(state, key, record, %{"in_reply_to" => id}) do
    with {:ok, sequence, position, revision} <- person_turn(state, key, id) do
      if revision == record.revision,
        do: {:ok, %{"in_reply_to" => sequence}, position},
        else: {:error, :stale_request}
    end
  end

  defp correlation(_state, _key, _record, _body), do: {:ok, %{}, nil}

  defp person_turn(state, {person, _, _} = key, id) do
    with {:ok, position} <- Resources.entry_seq(id),
         %{
           key: ^key,
           payload: %{"kind" => "discussion", "event_id" => event, "request_revision" => revision}
         } <-
           Map.get(state.store.engine.conversation_events, {person, position}),
         {sequence, ""} when sequence > 0 <- Integer.parse(event) do
      {:ok, sequence, position, revision}
    else
      _ -> {:error, :not_found}
    end
  end

  defp content(%{"message" => message}) when is_map(message) and not is_struct(message) do
    case Enum.find(~w(sender_id message_id supersedes), &Map.has_key?(message, &1)) do
      nil -> {:ok, message}
      field -> {:error, "invalid_field", field}
    end
  end

  defp content(_body), do: {:error, "invalid_field", "message"}

  defp session(%{"session" => value}) do
    if Message.uuid_v4?(value), do: :ok, else: {:error, "invalid_field", "session"}
  end

  defp session(_body), do: :ok

  defp files(body) do
    case Map.get(body, "file_ids", []) do
      ids when is_list(ids) ->
        if Enum.all?(ids, &(is_binary(&1) and byte_size(&1) in 1..128)),
          do: :ok,
          else: {:error, "invalid_field", "file_ids"}

      _ ->
        {:error, "invalid_field", "file_ids"}
    end
  end

  defp supersedes(state, scope, sender, body) do
    case Map.get(body, "supersedes", []) do
      ids when is_list(ids) ->
        if Enum.all?(ids, &(is_binary(&1) and byte_size(&1) in 1..128)) do
          {targets, warnings} =
            ids
            |> Enum.uniq()
            |> Enum.reduce({[], []}, fn id, {targets, warnings} ->
              case AgentResources.own_item_key(state, scope, id) do
                {:ok, {_, ^sender, legacy_id} = key} ->
                  if state.store.engine.messages[key].terminal,
                    do: {targets, [%{"item_id" => id, "reason" => "closed"} | warnings]},
                    else: {[legacy_id | targets], warnings}

                _ ->
                  {targets, [%{"item_id" => id, "reason" => "unknown"} | warnings]}
              end
            end)

          {:ok, Enum.reverse(targets), Enum.reverse(warnings)}
        else
          {:error, "invalid_field", "supersedes"}
        end

      _ ->
        {:error, "invalid_field", "supersedes"}
    end
  end

  defp decode_reply({:ok, line}, operation) do
    case JSON.decode_line(line) do
      {:ok, %{"type" => "ok"} = response} ->
        {:ok, response}

      {:ok, %{"type" => "error", "reason" => reason} = response} ->
        {:error, engine_error(reason, operation), response["field"]}

      _ ->
        {:error, :storage_unavailable}
    end
  end

  defp decode_reply(_, _), do: {:error, :storage_unavailable}

  defp engine_error("sender_not_authorized", _), do: "forbidden"
  defp engine_error("closed", _), do: "already_closed"
  defp engine_error("already_acted", _), do: "already_closed"
  defp engine_error("receipt_conflict", _), do: "already_closed"
  defp engine_error("retracted", _), do: "already_retracted"
  defp engine_error("stale_revision", :revise), do: "precondition_failed"

  defp engine_error(reason, _) do
    case Enum.find(Errors.tokens(), &(&1 == reason)) do
      nil ->
        case reason do
          "unknown_message" -> "not_found"
          "operation_conflict" -> "idempotency_conflict"
          "inactive_account" -> "account_paused"
          "stale_revision" -> "stale_request"
          "not_origin" -> "not_found"
          _ -> "invalid_field"
        end

      token ->
        token
    end
  end

  defp render(state, scope, :reply, key, %{"seq" => seq}, _extras) do
    with {:ok, rev} <- AgentChangeLog.head(state.store, scope),
         {:ok, entry} <- AgentResources.entry(state, scope, key, seq) do
      {:ok, %{"rev" => rev, "entry" => entry}}
    end
  end

  defp render(state, scope, :session_end, _key, _fields, _extra) do
    with {:ok, rev} <- AgentChangeLog.head(state.store, scope), do: {:ok, %{"rev" => rev}}
  end

  defp render(state, scope, :result, key, %{"seq" => seq}, _extra) do
    with {:ok, rev} <- AgentChangeLog.head(state.store, scope),
         {:ok, message} <- AgentResources.message(state, scope, key),
         {:ok, entry} <- AgentResources.entry(state, scope, key, seq) do
      {:ok, %{"rev" => rev, "message" => message, "entry" => entry}}
    end
  end

  defp render(state, scope, :answer_receipt, key, fields, _extra) do
    with {:ok, rev} <- AgentChangeLog.head(state.store, scope),
         {:ok, message} <- AgentResources.message(state, scope, key) do
      body = Map.take(fields, ["status", "stage_outcome"])
      {:ok, Map.merge(body, %{"rev" => rev, "message" => message})}
    end
  end

  defp render(state, scope, :chat_receipt, key, fields, position) do
    with {:ok, rev} <- AgentChangeLog.head(state.store, scope),
         {:ok, entry} <- AgentResources.entry(state, scope, key, position) do
      body = Map.take(fields, ["status", "stage_outcome"])
      {:ok, Map.merge(body, %{"rev" => rev, "entry" => entry})}
    end
  end

  defp render(state, scope, :file_use, key, fields, {position, file_id}) do
    with {:ok, rev} <- AgentChangeLog.head(state.store, scope),
         {:ok, entry} <- AgentResources.entry(state, scope, key, position) do
      body = Map.take(fields, ["duplicate", "transfer", "use"])

      {:ok,
       Map.merge(body, %{
         "rev" => rev,
         "entry" => entry,
         "file_id" => file_id
       })}
    end
  end

  defp render(state, scope, :advisory_ack, _key, fields, _extras) do
    with {:ok, rev} <- AgentChangeLog.head(state.store, scope) do
      {:ok,
       fields |> Map.take(~w(advisory_id delivered already_delivered)) |> Map.put("rev", rev)}
    end
  end

  defp render(state, scope, operation, key, _fields, extras) do
    with {:ok, rev} <- AgentChangeLog.head(state.store, scope),
         {:ok, message} <- AgentResources.message(state, scope, key) do
      body = %{"rev" => rev, "message" => message}

      case {operation, extras} do
        {:send, [_ | _] = warnings} ->
          {:ok, Map.put(body, "supersedes_warnings", warnings)}

        {mutation, seq} when mutation in [:retract, :revise] and is_integer(seq) ->
          with {:ok, entry} <- AgentResources.entry(state, scope, key, seq) do
            {:ok, Map.put(body, "entry", entry)}
          end

        _ ->
          {:ok, body}
      end
    end
  end

  defp save(state, scope, key, digest, body, status \\ 200)
  defp save(_state, _scope, nil, _digest, _body, _status), do: :ok

  defp save(state, scope, key, digest, body, status),
    do: Idempotency.save(state.store, scope, key, digest, status, body)

  defp refusal({:error, token, field}), do: {:error, to_string(token), field, false}
  defp refusal({:error, token}) when is_atom(token), do: {:error, to_string(token), nil, false}
  defp refusal(_), do: {:error, "storage_unavailable", nil, false}
end
