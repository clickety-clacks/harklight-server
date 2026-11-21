defmodule AttentionServer.ClientAPI.Writes do
  @moduledoc """
  The client API v2 writes (docs/client-api-v2.md §5).

  A write runs in three steps:

  1. **Begin**, in one relay call: authorize the principal, check the
     `Idempotency-Key` store (a stored outcome is replayed; a different
     request under the same key is `409 idempotency_conflict`), resolve the
     item and build today's engine operation.
  2. **Apply** through the relay's ordinary request path, exactly as today's
     clients' frames are applied. The commit, its change-log rows and every
     follow-on (origin feeds, push, levels) happen there, on the one write
     path. The key is the operation's `op_id`, so the engine's own receipts
     also deduplicate a retry from the same client.
  3. **Finish**, in one relay call: render the changed resources and the
     position from one state, and store the outcome under the key.

  Results are `{:ok, status, body, replayed?}`, or
  `{:error, token, field | nil, replayed?}` with a token (a string) from the
  contract's closed list. `replayed?` is true when the outcome is the stored
  one of an earlier request with the same key.

  The `who` argument is the request's principal, as for
  `AttentionServer.ClientAPI.Reads`: a `ClientAPI.Principal`-shaped map, or a
  bare device key.
  """

  @behaviour AttentionServer.ClientAPI.Routes

  import Plug.Conn

  alias AttentionServer.{BrowserRecipient, ChangeLog, Engine, JSON, Relay}
  alias AttentionServer.ClientAPI.{Context, Errors, FeedRenderer, Idempotency, Reads, Resources}

  @max_body_bytes 64 * 1024

  ## Routes (mounted by ClientAPI.Router behind client_api_v2)

  @impl true
  def routes do
    [
      {"POST", ["items", :item_id, "seen"], :post_seen},
      {"PUT", ["items", :item_id, "read-position"], :put_read_position},
      {"POST", ["items", :item_id, "replies"], :post_replies},
      {"POST", ["items", :item_id, "answer"], :post_answer}
    ]
  end

  @doc false
  def post_seen(conn, %Context{} = ctx) do
    with {:ok, key} <- required_key(conn),
         {:ok, body, conn} <- json_body(conn),
         :ok <- only_members(body, []) do
      respond(conn, seen(ctx.relay, ctx.principal, ctx.params.item_id, key))
    else
      {:error, token, field, conn} -> Errors.send_error(conn, token, field: field)
      {:error, token, field} -> Errors.send_error(conn, token, field: field)
    end
  end

  @doc false
  def put_read_position(conn, %Context{} = ctx) do
    with {:ok, key} <- optional_key(conn),
         {:ok, body, conn} <- json_body(conn),
         :ok <- only_members(body, ["entry_id"]),
         {:ok, entry_id} <- string_member(body, "entry_id") do
      respond(conn, read_position(ctx.relay, ctx.principal, ctx.params.item_id, entry_id, key))
    else
      {:error, token, field, conn} -> Errors.send_error(conn, token, field: field)
      {:error, token, field} -> Errors.send_error(conn, token, field: field)
    end
  end

  @doc false
  def post_replies(conn, %Context{} = ctx) do
    with {:ok, key} <- required_key(conn),
         {:ok, body, conn} <- json_body(conn),
         :ok <- only_members(body, ["text", "file_ids"]),
         {:ok, text} <- string_member(body, "text"),
         {:ok, file_ids} <- file_ids(body) do
      respond(conn, reply(ctx.relay, ctx.principal, ctx.params.item_id, text, file_ids, key))
    else
      {:error, token, field, conn} -> Errors.send_error(conn, token, field: field)
      {:error, token, field} -> Errors.send_error(conn, token, field: field)
    end
  end

  @doc false
  def post_answer(conn, %Context{} = ctx) do
    with {:ok, key} <- required_key(conn),
         {:ok, body, conn} <- json_body(conn),
         :ok <- only_members(body, ["request_revision", "choice", "comment", "result"]),
         {:ok, revision} <- request_revision(body),
         {:ok, value} <- answer_value(body) do
      respond(conn, answer(ctx.relay, ctx.principal, ctx.params.item_id, revision, value, key))
    else
      {:error, token, field, conn} -> Errors.send_error(conn, token, field: field)
      {:error, token, field} -> Errors.send_error(conn, token, field: field)
    end
  end

  ## Writes, called outside the relay process

  @doc "`POST /v2/items/{item_id}/seen`: mark the item's current revision Seen."
  def seen(relay, who, item_id, idempotency_key) do
    write(relay, who, idempotency_key, {:seen, item_id}, %{
      method: "POST",
      path: ["items", item_id, "seen"],
      body: %{}
    })
  end

  @doc """
  `PUT /v2/items/{item_id}/read-position`: the chat read position, compared by
  entry `position`, max-wins. It reads through the newest agent reply at or
  before `entry_id`; an entry at or below the current position leaves the item
  unchanged and answers it.
  """
  def read_position(relay, who, item_id, entry_id, idempotency_key \\ nil) do
    write(relay, who, idempotency_key, {:read_position, item_id, entry_id}, %{
      method: "PUT",
      path: ["items", item_id, "read-position"],
      body: %{"entry_id" => entry_id}
    })
  end

  @doc """
  `POST /v2/items/{item_id}/replies`: the person's reply in the item's chat
  (today's `discussion`), accepted in every item state and whether or not the
  agent is listening.
  """
  def reply(relay, who, item_id, text, file_ids \\ [], idempotency_key) do
    body = %{"text" => text}
    body = if file_ids == [], do: body, else: Map.put(body, "file_ids", file_ids)

    write(relay, who, idempotency_key, {:reply, item_id, text, file_ids}, %{
      method: "POST",
      path: ["items", item_id, "replies"],
      body: body
    })
  end

  @doc """
  `POST /v2/items/{item_id}/answer`: answer the item's open request at
  `request_revision`. `value` is `{:choice, action_id}`, `{:comment, text}`,
  `{:result, map}` or `:no_comment`. The first answer settles the request.
  """
  def answer(relay, who, item_id, request_revision, value, idempotency_key) do
    body =
      case value do
        {:choice, id} -> %{"choice" => id}
        {:comment, text} -> %{"comment" => text}
        {:result, result} -> %{"result" => result}
        :no_comment -> %{}
      end

    write(relay, who, idempotency_key, {:answer, item_id, request_revision, value}, %{
      method: "POST",
      path: ["items", item_id, "answer"],
      body: Map.put(body, "request_revision", request_revision)
    })
  end

  defp write(relay, who, key, operation, request) do
    digest = Idempotency.digest(request.method, request.path, request.body)

    case call(relay, {:begin, who, key, digest, operation}) do
      {:replay, status, body} ->
        result({:ok, status, body}, true)

      {:apply, engine_key, frame} ->
        outcome = apply_frame(relay, engine_key, frame)
        result(call(relay, {:finish, who, key, digest, operation, outcome}), false)

      :unchanged ->
        result(call(relay, {:finish, who, key, digest, operation, {:ok, :unchanged}}), false)

      error ->
        result(error, false)
    end
  end

  defp result({:ok, _status, %{"error" => token} = body}, replayed?),
    do: {:error, token, body["field"], replayed?}

  defp result({:ok, status, body}, replayed?), do: {:ok, status, body, replayed?}
  defp result({:error, token}, replayed?), do: {:error, to_string(token), nil, replayed?}

  defp result({:error, token, field}, replayed?),
    do: {:error, to_string(token), field, replayed?}

  defp apply_frame(relay, engine_key, frame) do
    case Relay.request_line(relay, engine_key, JSON.encode!(frame) <> "\n") do
      {:ok, line} ->
        case JSON.decode_line(line) do
          {:ok, %{"type" => "ok"} = reply} -> {:ok, reply}
          {:ok, %{"type" => "error", "reason" => reason} = reply} -> refusal(reason, reply)
          _ -> {:error, "retry_later"}
        end

      {:error, :forbidden} ->
        {:error, "unauthorized"}

      _ ->
        {:error, "retry_later"}
    end
  end

  # Today's refusal reasons, as tokens (ClientAPI.Errors). A reason with no
  # token is a request the client cannot fix by resending.
  @reasons Map.new(
             ~w(invalid_field bad_frame forbidden not_person not_origin unknown_message
                not_found stale_revision closed retracted operation_conflict
                stale_push_binding inactive_account too_large upload_cap_exceeded
                storage_unavailable)a,
             &{Atom.to_string(&1), &1}
           )

  defp refusal(reason, reply) do
    token =
      with {:ok, atom} <- Map.fetch(@reasons, reason),
           {:ok, token} <- Errors.token_for_reason(atom) do
        token
      else
        _ -> "invalid_field"
      end

    case reply do
      %{"field" => field} when is_binary(field) and token == "invalid_field" ->
        {:error, token, field}

      _ ->
        {:error, token}
    end
  end

  defp call(relay, request),
    do: GenServer.call(relay, {:client_api, __MODULE__, request}, :infinity)

  ## Inside the relay

  @doc false
  def handle_call({:begin, who, key, digest, operation}, _from, state) do
    reply =
      with {:ok, principal} <- principal(state.store.engine, who),
           {:ok, nil} <- stored(state, principal.person, key, digest),
           {:ok, frame} <- frame(state.store.engine, principal, key, operation) do
        if frame == :unchanged, do: :unchanged, else: {:apply, principal.engine_key, frame}
      else
        {:ok, %{status: status, body: body}} -> {:replay, status, body}
        error -> error
      end

    {:reply, reply, state}
  end

  def handle_call({:finish, who, key, digest, operation, outcome}, from, state) do
    reply =
      with {:ok, principal} <- principal(state.store.engine, who) do
        result =
          case outcome do
            {:ok, reply} -> render(operation, who, principal.person, reply, from, state)
            error -> error
          end

        remember(state, principal.person, key, digest, result)
      end

    {:reply, reply, state}
  end

  defp stored(_state, _person, nil, _digest), do: {:ok, nil}

  defp stored(state, person, key, digest) do
    case Idempotency.lookup(state.store, person, key) do
      {:ok, nil} -> {:ok, nil}
      {:ok, %{digest: ^digest} = receipt} -> {:ok, receipt}
      {:ok, _other} -> {:error, :idempotency_conflict}
      {:error, :disabled} -> {:error, :not_found}
      {:error, _} -> {:error, :storage_unavailable}
    end
  end

  # The first outcome a key gets is the one every repeat gets. Outcomes the
  # client may retry (503s) are not stored, so the retry is applied.
  defp remember(state, person, key, digest, result) do
    case stored_form(result) do
      {:store, status, body} when is_binary(key) ->
        case Idempotency.save(state.store, person, key, digest, status, body) do
          :ok -> {:ok, status, body}
          _ -> {:error, :storage_unavailable}
        end

      {:store, status, body} ->
        {:ok, status, body}

      :transient ->
        result
    end
  end

  defp stored_form({:ok, status, body}), do: {:store, status, body}

  defp stored_form({:error, token}), do: stored_form({:error, token, nil})

  defp stored_form({:error, token, field}) do
    token = to_string(token)

    if Errors.retryable?(token) or token == "unauthorized" do
      :transient
    else
      body = %{"error" => token}
      body = if field, do: Map.put(body, "field", field), else: body
      {:store, Errors.status(token), body}
    end
  end

  defp render({:seen, item_id}, who, _person, _reply, from, state),
    do: render_item(item_id, who, from, state)

  defp render({:read_position, item_id, _entry_id}, who, _person, _reply, from, state),
    do: render_item(item_id, who, from, state)

  defp render({:answer, item_id, _revision, _value}, who, _person, _reply, from, state),
    do: render_item(item_id, who, from, state)

  defp render({:reply, item_id, _text, _files}, who, person, %{"seq" => seq}, from, state) do
    with {:ok, 200, body} <- render_item(item_id, who, from, state),
         {:ok, entry} <- render_entry(state, who, person, seq) do
      {:ok, 200, Map.put(body, "entry", entry)}
    end
  end

  defp render_item(item_id, who, from, state) do
    case Reads.handle_call({:item, who, item_id}, from, state) do
      {:reply, {:ok, body}, _state} -> {:ok, 200, body}
      {:reply, error, _state} -> error
    end
  end

  # The entry a write created, rendered from the same state as its item, by
  # the renderer the reads and the feed use. Its rev is the newest of the
  # facts folded into it.
  defp render_entry(state, who, person, seq) do
    engine = state.store.engine
    view = Map.get(state.priority_views, person)

    with %{} = entry <- Resources.entry_for_fact(engine, person, seq, view),
         {:ok, rev} <- entry_rev(state, person, entry),
         context = %{engine: engine, store: state.store, view: view},
         row = %{rev: rev, kind: :entry, resource_id: ChangeLog.entry_resource(seq)},
         {:ok, rendered} <- FeedRenderer.render(context, who, row) do
      {:ok, rendered}
    else
      {:error, _} = error -> error
      _ -> {:error, :not_found}
    end
  end

  defp entry_rev(state, person, entry) do
    entry
    |> Resources.entry_sources()
    |> Enum.reduce_while({:ok, 1}, fn source, {:ok, max} ->
      case ChangeLog.rev(state.store, person, ChangeLog.entry_resource(source)) do
        {:ok, rev} -> {:cont, {:ok, Kernel.max(rev, max)}}
        {:error, :not_found} -> {:cont, {:ok, max}}
        _ -> {:halt, {:error, :storage_unavailable}}
      end
    end)
  end

  ## Operations, as today's engine frames

  defp frame(engine, principal, key, {:seen, item_id}) do
    with {:ok, {_person, sender, message_id} = message_key} <-
           Resources.item_key(engine, principal.person, item_id) do
      %{revision: revision} = Map.fetch!(engine.messages, message_key)

      {:ok,
       %{
         "v" => 1,
         "type" => "message_open",
         "op_id" => op_id(key),
         "sender_id" => sender,
         "message_id" => message_id,
         "request_revision" => revision
       }}
    end
  end

  # The read position moves to the newest agent reply at or before the entry,
  # never backward. With none past the current position nothing changes.
  defp frame(engine, principal, key, {:read_position, item_id, entry_id}) do
    person = principal.person

    with {:ok, {_person, sender, message_id} = message_key} <-
           Resources.item_key(engine, person, item_id),
         {:ok, seq} <- item_entry_seq(engine, person, message_key, entry_id) do
      current = Map.get(engine.chat_read_positions, message_key, 0)

      case newest_origin_reply(engine, person, message_key, seq, current) do
        nil ->
          {:ok, :unchanged}

        through ->
          {:ok,
           %{
             "v" => 1,
             "type" => "conversation_read",
             "op_id" => op_id(key),
             "sender_id" => sender,
             "message_id" => message_id,
             "read_through_conversation_seq" => through
           }}
      end
    end
  end

  # Chat never closes: the reply goes to the item's current revision in any
  # state. The engine stores it for the agent whether or not it is listening.
  defp frame(engine, principal, key, {:reply, item_id, text, file_ids}) do
    with {:ok, {_person, sender, message_id} = message_key} <-
           Resources.item_key(engine, principal.person, item_id),
         :ok <- no_file_ids(file_ids) do
      %{revision: revision} = Map.fetch!(engine.messages, message_key)

      {:ok,
       %{
         "v" => 1,
         "type" => "discussion",
         "op_id" => op_id(key),
         "sender_id" => sender,
         "message_id" => message_id,
         "request_revision" => revision,
         "text" => text
       }}
    end
  end

  # An answer goes to the engine as today's answer (or answer_no_comment)
  # frame, which keeps the first-answer-settles, stale-revision and
  # retraction rules. A settled item goes straight to the engine, so its
  # refusal is today's.
  defp frame(engine, principal, key, {:answer, item_id, revision, value}) do
    with {:ok, {_person, sender, message_id} = message_key} <-
           Resources.item_key(engine, principal.person, item_id) do
      record = Map.fetch!(engine.messages, message_key)

      cond do
        not answerable?(record) ->
          {:ok, answer_frame(key, sender, message_id, revision, nil)}

        revision != record.revision ->
          {:error, :stale_request}

        true ->
          with {:ok, stored} <- stored_answer(record, value) do
            case stored do
              :no_comment ->
                {:ok,
                 %{
                   "type" => "answer_no_comment",
                   "request_id" => message_id,
                   "request_revision" => revision
                 }}

              {:answer, answer} ->
                {:ok, answer_frame(key, sender, message_id, revision, answer)}
            end
          end
      end
    end
  end

  defp answer_frame(key, sender, message_id, revision, answer) do
    %{
      "v" => 1,
      "type" => "answer",
      "op_id" => op_id(key),
      "sender_id" => sender,
      "message_id" => message_id,
      "request_revision" => revision,
      "answer" => answer
    }
  end

  # An item is answerable while it has no terminal state, exactly today's
  # engine rule, FYIs included (contract rulings round 10). A settled item
  # goes to the engine, so its refusal is today's.
  defp answerable?(record), do: is_nil(record.terminal)

  # The value today's answer stores: a choice stores its action's value, a
  # comment its text, a UI/1 result the result object.
  defp stored_answer(_record, :no_comment), do: {:ok, :no_comment}
  defp stored_answer(_record, {:comment, text}), do: {:ok, {:answer, text}}

  defp stored_answer(record, {:choice, id}) do
    case Map.get(record.message, "answer_ui") do
      %{"schema" => "attention-client.actions/1", "actions" => actions} when is_list(actions) ->
        case Enum.find(actions, &(is_map(&1) and &1["id"] == id and &1["kind"] == "value")) do
          %{} = action -> {:ok, {:answer, Map.get(action, "value")}}
          nil -> {:error, :invalid_field, "choice"}
        end

      _ ->
        {:error, :invalid_field, "choice"}
    end
  end

  defp stored_answer(record, {:result, result}) do
    case Map.get(record.message, "answer_ui") do
      %{"schema" => "attention-client.ui/1"} -> {:ok, {:answer, result}}
      _ -> {:error, :invalid_field, "result"}
    end
  end

  # Uploaded files arrive with POST /v2/files, a later slice.
  defp no_file_ids([]), do: :ok
  defp no_file_ids(_), do: {:error, :unsupported, "file_ids"}

  defp item_entry_seq(engine, person, message_key, entry_id) do
    with {:ok, seq} <- Resources.entry_seq(entry_id),
         %{key: ^message_key} <- Map.get(engine.conversation_events, {person, seq}) do
      {:ok, seq}
    else
      _ -> {:error, :invalid_field, "entry_id"}
    end
  end

  defp newest_origin_reply(_engine, _person, _key, seq, current) when seq <= current, do: nil

  defp newest_origin_reply(engine, person, key, seq, current) do
    case Map.get(engine.conversation_events, {person, seq}) do
      %{key: ^key, payload: %{"kind" => "reply", "author" => "origin"}} -> seq
      _ -> newest_origin_reply(engine, person, key, seq - 1, current)
    end
  end

  # A PUT or DELETE may come without a key; the engine still needs an op_id.
  defp op_id(key) when is_binary(key), do: key
  defp op_id(nil), do: uuid_v4(:crypto.strong_rand_bytes(16))

  ## Principals
  #
  # The engine key a write is applied as: the device's key, or for a browser
  # session a browser-recipient key derived from the session, so the same
  # session always acts as the same recipient.

  defp principal(engine, %{kind: :browser, person: person, key: owner_key} = who)
       when is_binary(person) do
    pinned = Map.get(who, :owner_revision)
    session = to_string(Map.get(who, :session_id))

    case Engine.owner_revision(engine, person, owner_key) do
      {:ok, ^pinned} when session != "" ->
        recipient = uuid_v4(:crypto.hash(:sha256, "client_api_v2:" <> session))

        {:ok,
         %{
           person: person,
           engine_key: BrowserRecipient.key(person, session, recipient)
         }}

      _ ->
        {:error, :unauthorized}
    end
  end

  defp principal(engine, %{kind: :device, key: key}), do: principal(engine, key)

  defp principal(engine, key) when is_binary(key) do
    case Resources.principal(engine, key) do
      {:ok, %{kind: :device, person: person}} -> {:ok, %{person: person, engine_key: key}}
      _ -> {:error, :unauthorized}
    end
  end

  defp principal(_engine, _who), do: {:error, :unauthorized}

  defp uuid_v4(<<a::48, _::4, b::12, _::2, c::62, _::binary>>) do
    <<a::48, 4::4, b::12, 2::2, c::62>>
    |> Base.encode16(case: :lower)
    |> then(fn hex ->
      <<p1::binary-8, p2::binary-4, p3::binary-4, p4::binary-4, p5::binary-12>> = hex
      Enum.join([p1, p2, p3, p4, p5], "-")
    end)
  end

  ## HTTP helpers

  defp required_key(conn) do
    case get_req_header(conn, "idempotency-key") do
      [key] ->
        if Idempotency.valid_key?(key),
          do: {:ok, key},
          else: {:error, "invalid_field", "Idempotency-Key"}

      _ ->
        {:error, "invalid_field", "Idempotency-Key"}
    end
  end

  defp optional_key(conn) do
    case get_req_header(conn, "idempotency-key") do
      [] -> {:ok, nil}
      _ -> required_key(conn)
    end
  end

  defp string_member(body, name) do
    case Map.get(body, name) do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, "invalid_field", name}
    end
  end

  defp request_revision(body) do
    case Map.get(body, "request_revision") do
      n when is_integer(n) and n >= 1 -> {:ok, n}
      _ -> {:error, "invalid_field", "request_revision"}
    end
  end

  # At most one of choice, comment and result; none is no comment.
  defp answer_value(body) do
    case Map.take(body, ["choice", "comment", "result"]) |> Map.to_list() do
      [] ->
        {:ok, :no_comment}

      [{"choice", id}] when is_binary(id) and id != "" ->
        {:ok, {:choice, id}}

      [{"comment", text}] when is_binary(text) ->
        {:ok, {:comment, text}}

      [{"result", result}] when is_map(result) and not is_struct(result) ->
        {:ok, {:result, result}}

      [{name, _}] ->
        {:error, "invalid_field", name}

      _ ->
        {:error, "invalid_field", nil}
    end
  end

  defp file_ids(body) do
    case Map.get(body, "file_ids", []) do
      ids when is_list(ids) ->
        if Enum.all?(ids, &(is_binary(&1) and &1 != "")),
          do: {:ok, ids},
          else: {:error, "invalid_field", "file_ids"}

      _ ->
        {:error, "invalid_field", "file_ids"}
    end
  end

  defp json_body(conn) do
    case read_body(conn, length: @max_body_bytes) do
      {:ok, "", conn} ->
        {:ok, %{}, conn}

      {:ok, raw, conn} ->
        case JSON.decode_line(raw <> "\n") do
          {:ok, body} when is_map(body) and not is_struct(body) -> {:ok, body, conn}
          _ -> {:error, "invalid_field", nil, conn}
        end

      {:more, _partial, conn} ->
        {:error, "payload_too_large", nil, conn}

      {:error, _} ->
        {:error, "invalid_field", nil}
    end
  end

  defp only_members(body, allowed) do
    case Enum.find(Map.keys(body), &(&1 not in allowed)) do
      nil -> :ok
      member -> {:error, "invalid_field", member}
    end
  end

  defp respond(conn, {:ok, status, body, replayed?}),
    do: conn |> replayed(replayed?) |> Errors.send_json(status, body)

  defp respond(conn, {:error, token, field, replayed?}),
    do: conn |> replayed(replayed?) |> Errors.send_error(token, field: field)

  defp replayed(conn, true), do: put_resp_header(conn, "idempotent-replayed", "true")
  defp replayed(conn, false), do: conn
end
