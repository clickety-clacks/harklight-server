defmodule AttentionServer.ClientAPI.AgentWrites do
  @moduledoc "Agent message mutations on the device listener."

  @behaviour AttentionServer.ClientAPI.Routes

  import Plug.Conn, only: [get_req_header: 2, read_body: 1]

  alias AttentionServer.{AgentChangeLog, ConversationFiles, Engine, JSON, Message, Relay, SharedMessages, Store}

  alias AttentionServer.AgentAPI.Advisories

  alias AttentionServer.ClientAPI.{
    AgentResources,
    AgentShow,
    Errors,
    Idempotency,
    Projection,
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
      {"POST", ["agent", "messages", :item_id, "retract"], :retract, granted_agent: true},
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
    with :ok <- write_principal(ctx.principal, operation),
         {:ok, key} <- request_key(conn),
         {:ok, body, conn} <- json_body(conn, []) do
      dispatch_write(conn, ctx, operation, key, body)
    else
      {:error, token, field, response} -> Errors.send_error(response, token, field: field)
      {:error, token, field} -> Errors.send_error(conn, token, field: field)
    end
  end

  defp dispatch_write(conn, ctx, :send, key, %{"recipients" => _} = body) do
    case get_req_header(conn, "harklight-features") do
      ["shared_messages"] ->
        digest = write_digest(conn, get_req_header(conn, "if-match"), body)

        case call(ctx, {:shared_send, ctx.principal, key, digest, body}) do
          {:shared_error, token, details} -> Errors.send_error(conn, token, details: details)
          result -> Writes.respond(conn, replay_success(ctx, result))
        end

      [] ->
        Errors.send_error(conn, "unsupported")

      _ ->
        Errors.send_error(conn, "invalid_field", field: "features")
    end
  end

  defp dispatch_write(conn, ctx, operation, key, body) do
    with :ok <- Writes.only_members(body, Map.fetch!(@members, operation)) do
      precondition = get_req_header(conn, "if-match")
      digest = write_digest(conn, precondition, body)
      op_id = key || ConversationFiles.new_upload_id()
      input = {ctx.principal, operation, ctx.params, key, digest, precondition, body}

      result =
        case prepare_uploads(ctx, input, op_id) do
          {:ok, uploads} -> call(ctx, {:execute, input, op_id, uploads})
          error -> refusal(error)
        end

      Writes.respond(conn, replay_success(ctx, result))
    else
      {:error, token, field} -> Errors.send_error(conn, token, field: field)
    end
  end

  defp write_digest(conn, precondition, body) do
    Idempotency.digest(conn.method, conn.path_info, %{"body" => body, "if_match" => precondition})
  end

  defp write_principal(%{kind: :agent}, :retract), do: :ok
  defp write_principal(principal, _operation), do: Writes.device_only(principal)

  defp call(ctx, request),
    do: GenServer.call(ctx.relay, {:client_api, __MODULE__, request}, :infinity)

  defp replay_success(ctx, {:stored, status, encoded}) do
    case JSON.decode_line(encoded <> "\n") do
      {:ok, receipt} when is_map(receipt) ->
        case replay_body(ctx, receipt) do
          {:ok, body} -> {:ok, status, body, true}
          error -> refusal(error)
        end

      _ ->
        refusal({:error, :storage_unavailable})
    end
  end

  defp replay_success(_ctx, result), do: result

  defp replay_body(_ctx, %{"agent_write" => 1, "item_id" => nil, "outcome" => outcome}),
    do: {:ok, outcome}

  defp replay_body(ctx, %{"agent_write" => 1, "outcome" => outcome} = receipt) do
    with {:ok, resources} <- call(ctx, {:replay_resources, ctx.principal, receipt}) do
      {:ok, Map.merge(outcome, resources)}
    end
  end

  # Earlier versions stored the full success. Decode it in the HTTP process.
  defp replay_body(_ctx, body), do: {:ok, body}

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
  def handle_call({:shared_send, %{person: person, key: origin}, key, digest, body}, _from, state) do
    scope = {person, origin}

    if state.shared_messages and state.store.mode == :keyed do
      case authorize(state, scope, :send) do
        :ok -> shared_send(state, scope, key, digest, body)
        error -> {:reply, refusal(error), state}
      end
    else
      {:reply, refusal({:error, :unsupported}), state}
    end
  end

  def handle_call(
        {:execute, {%{person: person, key: agent}, operation, item_id, key, digest, match, body},
         op_id, uploads},
        _from,
        state
      ) do
    scope = {person, agent}
    receipt_scope = receipt_scope(scope)

    case authorize(state, scope, operation) do
      :ok ->
        case lookup(state, receipt_scope, key) do
          {:ok, %{digest: ^digest, status: status, body: stored}} when is_binary(stored) ->
            {:reply, {:stored, status, stored}, state}

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
             checked_frame(state, scope, operation, target, op_id, match, body),
           {:ok, selected} <- checked_files(state, scope, operation, message_key, body) do
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

  def handle_call(
        {:replay_resources, %{person: person, key: agent}, %{"item_id" => item} = receipt},
        _from,
        state
      ) do
    scope = {person, agent}

    operation = if receipt["account_retract"], do: :retract, else: :other

    result =
      with :ok <- authorize(state, scope, operation),
           {:ok, key} <- operation_item_key(state, scope, operation, item) do
        project_receipt(state, scope, key, receipt)
      end

    {:reply, result, state}
  end

  defp project_receipt(_state, _scope, _key, %{"item_id" => nil}), do: {:ok, %{}}

  defp project_receipt(state, scope, key, receipt) do
    projection_scope = receipt_projection_scope(state, scope, key, receipt)

    Projection.capture(fn ->
      with {:ok, rev} <- AgentChangeLog.head(state.store, projection_scope),
           {:ok, message} <-
             replay_message(state, projection_scope, key, receipt["include_message"]),
           {:ok, entry} <- replay_entry(state, projection_scope, key, receipt["entry_position"]) do
        message =
          if projection_scope != scope do
            Map.delete(message, "advisories")
          else
            message
          end

        resources =
          %{"rev" => rev}
          |> put_resource("message", message)
          |> put_resource("entry", entry)
          |> refresh_advisory_ack(state.store.engine, scope, key, receipt["outcome"])

        {:ok, resources}
      end
    end)
  end

  defp refresh_advisory_ack(resources, engine, scope, key, %{"advisory_id" => id}) do
    case Enum.find(Advisories.for_message(engine, scope, key), &(&1["advisory_id"] == id)) do
      %{"moot" => true, "delivered" => delivered} ->
        Map.merge(resources, %{"moot" => true, "delivered" => delivered})

      _ ->
        resources
    end
  end

  defp refresh_advisory_ack(resources, _engine, _scope, _key, _outcome), do: resources

  defp receipt_projection_scope(state, {person, _}, key, %{"account_retract" => true}),
    do: {person, state.store.engine.messages[key].origin}

  defp receipt_projection_scope(_state, scope, _key, _receipt), do: scope

  defp replay_message(state, scope, key, true), do: AgentResources.message(state, scope, key)
  defp replay_message(_state, _scope, _key, false), do: {:ok, nil}

  defp replay_entry(_state, _scope, _key, nil), do: {:ok, nil}
  defp replay_entry(state, scope, key, position), do: AgentResources.entry(state, scope, key, position)

  defp put_resource(body, _field, nil), do: body
  defp put_resource(body, field, resource), do: Map.put(body, field, resource)

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

  # Two staged files with the same bytes under one name attach as one upload
  # twice. Relay refused that before the engine, naming no field; keep it.
  defp distinct_upload_names(uploads) do
    names = Enum.map(uploads, & &1["name"])

    if length(Enum.uniq(names)) == length(names),
      do: :ok,
      else: {:error, "invalid_field", nil}
  end

  defp shared_send(state, scope, key, digest, body) do
    case lookup(state, receipt_scope(scope), key) do
      {:ok, %{digest: ^digest, status: status, body: stored}} ->
        {:reply, {:stored, status, stored}, state}

      {:ok, %{}} ->
        {:reply, refusal({:error, :idempotency_conflict}), state}

      {:ok, nil} ->
        admit_shared_send(state, scope, key, digest, body)

      error ->
        {:reply, refusal(error), state}
    end
  end

  defp admit_shared_send(state, {person, origin} = scope, key, digest, body) do
    engine = state.store.engine
    sender = body["sender"]

    with :ok <- shared_send_fields(body),
         {:ok, people, org} <- shared_recipients(body["recipients"]),
         {:ok, message} <- content(body),
         :ok <- session(body),
         true <- Engine.shared_sender_allowed?(engine, person, origin, sender) ||
                   {:error, :forbidden},
         {:ok, policy} <- shared_answer_policy(message, body),
         id <- shared_id(scope, key),
         message <- Map.merge(message, %{"sender_id" => sender, "message_id" => id}),
         :ok <- shared_content_valid(message) do
      {message, _diagnostics} = Message.admit_answer_ui(message, [])
      attrs = %{
        :id => id,
        :sender_person => person,
        :sender_key => origin,
        :sender_id => sender,
        :message => message,
        :answer_policy => policy,
        "created_at_ms" => state.clock.()
      }
      attrs = Map.merge(attrs, Map.take(body, ["session"]))
      case SharedMessages.create(engine, attrs, people, org) do
        {:ok, next, _record, outcomes} ->
          commit_shared_send(state, next, scope, key, digest, id, outcomes)

        error ->
          {:reply, shared_send_refusal(error), state}
      end
    else
      error -> {:reply, shared_send_refusal(error), state}
    end
  end

  defp commit_shared_send(state, transition, {person, origin} = scope, key, digest, id, outcomes) do
    audit = %{
      person: person, actor: origin, action: "shared_message_send",
      shared_send_receipt: {scope, key, digest, id, wire_outcomes(outcomes)}
    }

    case Store.commit(state.store, transition, audit) do
      {:ok, store} ->
        reply =
          case Idempotency.lookup(store, receipt_scope(scope), key) do
            {:ok, %{body: body}} -> {:ok, 200, body, false}
            _ -> refusal({:error, :storage_unavailable})
          end

        {:reply, reply, %{state | store: store}}

      _ ->
        {:reply, refusal({:error, :storage_unavailable}), state}
    end
  end

  defp shared_send_fields(body) do
    with :ok <- Writes.only_members(body, ~w(sender session message file_ids recipients answer_policy)),
         true <- Message.sender_id?(body["sender"]) || {:error, "invalid_field", "sender"},
         :ok <- files(body) do
      if Map.get(body, "file_ids", []) == [],
        do: :ok,
        else: {:error, "unsupported", "file_ids"}
    end
  end

  defp shared_recipients(%{"kind" => "people", "people" => people} = selector) do
    organization = selector["organization_id"]

    with :ok <- Writes.only_members(selector, ~w(kind people organization_id)),
         true <- is_list(people) and people != [] and Enum.all?(people, &opaque_id?/1),
         true <- not Map.has_key?(selector, "organization_id") or opaque_id?(organization) do
      {:ok, people, organization}
    else
      _ -> {:error, "invalid_field", "recipients"}
    end
  end

  defp shared_recipients(%{"kind" => "group"}), do: {:error, :not_found}
  defp shared_recipients(_), do: {:error, "invalid_field", "recipients"}

  defp opaque_id?(value), do: is_binary(value) and byte_size(value) in 1..128

  defp shared_answer_policy(%{"message_type" => "fyi"}, body) do
    if Map.has_key?(body, "answer_policy"),
      do: {:error, "invalid_field", "answer_policy"},
      else: {:ok, nil}
  end

  defp shared_answer_policy(_message, body) do
    case Map.get(body, "answer_policy", %{"kind" => "first"}) do
      %{"kind" => "first"} = policy when map_size(policy) == 1 -> {:ok, :first}
      %{"kind" => "everyone"} = policy when map_size(policy) == 1 -> {:ok, :everyone}
      %{"kind" => "named", "people" => people} = policy when map_size(policy) == 2 ->
        if is_list(people) and people != [] and Enum.all?(people, &opaque_id?/1),
          do: {:ok, {:named, Enum.uniq(people)}},
          else: {:error, "invalid_field", "answer_policy"}

      _ -> {:error, "invalid_field", "answer_policy"}
    end
  end

  defp shared_content_valid(message) do
    with :ok <- Message.validate(message),
         :ok <- Message.validate_present_message_type(message),
         :ok <- Message.validate_typed_message(message) do
      :ok
    else
      {:error, field} when is_binary(field) -> {:error, "invalid_field", field}
      {:error, _} -> {:error, "invalid_field", "message"}
    end
  end

  defp shared_send_refusal({:error, :organization_required, ids}),
    do: {:shared_error, "organization_required", %{"organization_ids" => ids}}

  defp shared_send_refusal({:error, reason, outcomes})
       when reason in [:no_recipients, :decider_unreachable],
       do: {:shared_error, Atom.to_string(reason), %{"recipients" => wire_outcomes(outcomes)}}

  defp shared_send_refusal({:error, :invalid_deciders}),
    do: refusal({:error, "invalid_field", "answer_policy"})

  defp shared_send_refusal(error), do: refusal(error)

  defp wire_outcomes(outcomes),
    do: Enum.map(outcomes, fn {id, status} -> %{"person_id" => id, "status" => Atom.to_string(status)} end)

  defp shared_id(scope, key) do
    hash = :crypto.hash(:sha256, :erlang.term_to_binary({scope, key}))
    "shared_" <> Base.url_encode64(hash, padding: false)
  end

  defp authorize(state, scope, operation \\ :other)

  defp authorize(state, {person, agent}, operation) do
    with true <- state.store.change_log,
         {:ok, %{person: ^person, kind: kind}} <-
           Engine.account_device_principal(state.store.engine, agent),
         true <- kind == :device or (kind == :agent and operation == :retract) do
      if Engine.delivery_stopped?(state.store.engine, person),
        do: {:error, :account_paused},
        else: :ok
    else
      false -> {:error, :not_found}
      _ -> {:error, :unauthorized}
    end
  end

  defp lookup(state, scope, key), do: AgentShow.lookup(state, scope, key)

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
    case checked_frame(state, scope, operation, item_id, op_id, match, body) do
      {:ok, message_key, attributes, extras} ->
        {reply, next} =
          with {:ok, selected} <- checked_files(state, scope, operation, message_key, body),
               {:ok, frame} <- attach_selected(attributes, operation, selected, uploads),
               :ok <- distinct_upload_names(uploads) do
            staged = for {:staged, file} <- selected, do: file.file_id
            sources = Map.new(Enum.zip(Enum.map(uploads, & &1["upload_id"]), staged))
            apply_frame(state, agent, operation, frame, op_id, sources, message_key)
          else
            error -> {error, state}
          end

        response =
          with {:ok, fields} <- reply,
               {:ok, receipt} <-
                 committed_receipt(next, scope, operation, message_key, fields, extras),
               :ok <- save(next, receipt_scope, key, digest, receipt),
               {:ok, resources} <- project_receipt(next, scope, message_key, receipt) do
            {:ok, 200, Map.merge(receipt["outcome"], resources), false}
          else
            error -> refusal(error)
          end

        {:reply, response, next}

      error ->
        {:reply, refusal(error), state}
    end
  end

  defp apply_frame(state, agent, :retract, attributes, op_id, _sources, key) do
    frame = Map.merge(attributes, %{"op_id" => op_id})
    {reply, next} = Relay.apply_account_retract(state, key, agent, frame)
    {command_result(reply, :retract), next}
  end

  defp apply_frame(state, agent, operation, attributes, op_id, sources, _key)
       when is_map(attributes) do
    frame = Map.merge(attributes, %{"op_id" => op_id, :agent_api_v2 => true})
    {reply, next} = Relay.apply_agent_command(state, agent, frame, sources)
    {command_result(reply, operation), next}
  end

  defp apply_frame(state, agent, _operation, {:receipt, sequence, body}, op_id, _sources, _key) do
    {:reply, reply, next} =
      Relay.apply_agent_command(
        state,
        {:origin_ack2, agent, sequence, Map.put(body, "op_id", op_id)}
      )

    {reply, next}
  end

  defp apply_frame(state, agent, _operation, {:session_end, session}, _op_id, _sources, _key) do
    {:reply, reply, next} =
      Relay.apply_agent_command(state, {:origin_session_gone, agent, session, :agent_v2})

    {if(reply == :ok, do: {:ok, %{}}, else: reply), next}
  end

  defp apply_frame(
         state,
         agent,
         _operation,
         {:file_use, sequence, name, outcome},
         op_id,
         _sources,
         _key
       ) do
    case Relay.apply_agent_file_use(state, agent, sequence, name, outcome, op_id) do
      {{:error, :file_use_conflict}, next} -> {{:error, :already_closed}, next}
      {{:error, :file_use}, next} -> {{:error, "invalid_field", "outcome"}, next}
      result -> result
    end
  end

  defp apply_frame(state, agent, _operation, {:advisory_ack, item, id}, _op_id, _sources, _key),
    do: Relay.apply_agent_advisory_ack(state, item, agent, id)

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
    with {:ok, {_, sender, id} = key} <- operation_item_key(state, scope, operation, item_id),
         record = Map.fetch!(state.store.engine.messages, key),
         :ok <- authorize_correlation(scope, record, operation, body),
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

  defp operation_item_key(state, {person, _}, :retract, id),
    do: Resources.item_key(state.store.engine, person, id)

  defp operation_item_key(state, scope, _operation, id),
    do: AgentResources.own_item_key(state, scope, id)

  defp authorize_correlation({_, actor}, %{origin: origin}, :retract, %{"in_reply_to" => _})
       when actor != origin,
       do: {:error, :not_found}

  defp authorize_correlation(_scope, _record, _operation, _body), do: :ok

  defp item_frame(state, {_, sender, id} = key, record, :revise, match, body) do
    with :ok <- open_revision(record),
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
         {:ok, correlation, position} <- correlation(state, key, record, body) do
      {:ok, Map.put(correlation, "type", "conversation_withdraw"), position}
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

  defp item_frame(_state, _key, %{"full_bounced_at_ms" => _}, :reply, _match, _body),
    do: {:error, :already_closed}

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

  defp item_frame(_state, item, _record, :advisory_ack, _match, body) do
    id = body["advisory_id"]

    if Message.uuid_v4?(id),
      do: {:ok, {:advisory_ack, item, id}, nil},
      else: {:error, "invalid_field", "advisory_id"}
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
         %{"kind" => "terminal", "terminal_seq" => ^seq} = entry <-
           get_in(engine.origin_feeds, [{person, agent}, :entries, sequence]) do
      # A receipt and a result follow a person's answer. Another author's
      # answer (a registered agent's) is an answer, so not_answered does not
      # apply; the operation is unsupported for it (contract addendum 8).
      if entry["author"] == "person",
        do: {:ok, sequence},
        else: {:error, :unsupported}
    else
      _ -> {:error, :not_answered}
    end
  end

  defp answer_target(_state, _key, _record), do: {:error, :not_answered}

  defp open_revision(%{"full_bounced_at_ms" => _, terminal: nil}),
    do: {:error, :already_closed}

  defp open_revision(record), do: open(record)

  defp open(%{terminal: nil}), do: :ok
  defp open(%{terminal: %{state: "retracted"}}), do: {:error, :already_retracted}
  defp open(_record), do: {:error, :already_closed}

  defp if_match([supplied], revision) do
    value = Integer.to_string(revision)
    if supplied in [value, ~s("#{value}")], do: :ok, else: {:error, :precondition_failed}
  end

  defp if_match(_headers, _revision), do: {:error, :precondition_failed}

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
                {:ok, {_, ^sender, stored_message_id} = key} ->
                  if state.store.engine.messages[key].terminal,
                    do: {targets, [%{"item_id" => id, "reason" => "closed"} | warnings]},
                    else: {[stored_message_id | targets], warnings}

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

  defp command_result({:ok, fields}, _operation), do: {:ok, fields}

  defp command_result({:error, {:typed_message, _code, reason}, field, _terminal}, operation),
    do: {:error, engine_error(reason, operation), field}

  defp command_result({:error, reason, field, _terminal}, operation),
    do: {:error, engine_error(reason, operation), field}

  defp command_result(_, _), do: {:error, :storage_unavailable}

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

  defp checked_frame(state, scope, operation, target, op_id, match, body) do
    Projection.capture(fn -> frame(state, scope, operation, target, op_id, match, body) end)
  end

  defp checked_files(state, scope, operation, key, body) do
    Projection.capture(fn -> select_files(state, scope, operation, key, body) end)
  end

  defp committed_receipt(state, scope, operation, key, fields, extras) do
    {message?, position, outcome} = receipt_fields(operation, fields, extras)

    receipt = if operation == :retract, do: %{"account_retract" => true}, else: %{}
    projection_scope = receipt_projection_scope(state, scope, key, receipt)

    with {:ok, rev} <- AgentChangeLog.head(state.store, projection_scope),
         {:ok, item_id} <-
           receipt_item(
             state,
             key,
             message? or is_integer(position) or operation == :advisory_ack
           ) do
      {:ok,
       Map.merge(receipt, %{
         "agent_write" => 1,
         "item_id" => item_id,
         "include_message" => message?,
         "entry_position" => position,
         "outcome" => Map.put(outcome, "rev", rev)
       })}
    end
  end

  defp receipt_item(state, key, true), do: Resources.item_id(state.store.engine, key)
  defp receipt_item(_state, _key, false), do: {:ok, nil}

  defp receipt_fields(:reply, %{"seq" => seq}, _extras), do: {false, seq, %{}}
  defp receipt_fields(:result, %{"seq" => seq}, _extras), do: {true, seq, %{}}
  defp receipt_fields(:session_end, _fields, _extras), do: {false, nil, %{}}

  defp receipt_fields(:answer_receipt, fields, _extras),
    do: {true, nil, Map.take(fields, ~w(status stage_outcome))}

  defp receipt_fields(:chat_receipt, fields, position),
    do: {false, position, Map.take(fields, ~w(status stage_outcome))}

  defp receipt_fields(:file_use, fields, {position, file_id}),
    do:
      {false, position,
       fields |> Map.take(~w(duplicate transfer use)) |> Map.put("file_id", file_id)}

  defp receipt_fields(:advisory_ack, fields, _extras),
    do: {false, nil, Map.take(fields, ~w(advisory_id delivered already_delivered moot))}

  defp receipt_fields(:send, _fields, [_ | _] = warnings),
    do: {true, nil, %{"supersedes_warnings" => warnings}}

  defp receipt_fields(operation, _fields, seq)
       when operation in [:retract, :revise] and is_integer(seq),
       do: {true, seq, %{}}

  defp receipt_fields(_operation, _fields, _extras), do: {true, nil, %{}}

  defp save(_state, _scope, nil, _digest, _receipt), do: :ok

  defp save(state, scope, key, digest, receipt),
    do: Idempotency.save(state.store, scope, key, digest, 200, receipt)

  defp refusal({:error, %Projection{}} = error), do: error
  defp refusal({:error, token, field}), do: {:error, to_string(token), field, false}
  defp refusal({:error, token}) when is_atom(token), do: {:error, to_string(token), nil, false}
  defp refusal(_), do: {:error, "storage_unavailable", nil, false}
end
