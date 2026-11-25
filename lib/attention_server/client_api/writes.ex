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

  alias AttentionServer.{BrowserRecipient, ChangeLog, ConversationFiles, Engine, JSON, Relay}

  alias AttentionServer.ClientAPI.{
    Context,
    Errors,
    FeedRenderer,
    Focus,
    Idempotency,
    Projection,
    Reads,
    Resources,
    Uploads
  }

  # Today's request frame limit (TlsRelay packet_size), so a UI/1 result or a
  # long reply that today's path accepts is accepted here too. No new limit.
  @max_body_bytes 2 * 1024 * 1024

  ## Routes (mounted by ClientAPI.Router)

  @impl true
  def routes do
    [
      {"POST", ["items", :item_id, "seen"], :post_seen},
      {"PUT", ["items", :item_id, "read-position"], :put_read_position},
      {"POST", ["items", :item_id, "replies"], :post_replies},
      {"POST", ["items", :item_id, "answer"], :post_answer, granted_agent: true},
      {"PUT", ["settings", "priority-levels"], :put_priority_levels},
      {"PUT", ["settings", "alerts"], :put_alerts},
      {"PUT", ["devices", "me", "push"], :put_push},
      {"DELETE", ["devices", "me", "push"], :delete_push},
      {"POST", ["files"], :post_files},
      {"DELETE", ["files", :file_id], :delete_file},
      {"PUT", ["devices", "me", "focus"], :put_focus}
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

  @doc false
  def put_priority_levels(conn, %Context{} = ctx) do
    with {:ok, key} <- optional_key(conn),
         {:ok, if_match} <- if_match(conn),
         {:ok, body, conn} <- json_body(conn),
         :ok <- only_members(body, ["levels"]),
         {:ok, levels} <- list_member(body, "levels") do
      respond(conn, priority_levels(ctx.relay, ctx.principal, levels, if_match, key))
    else
      {:error, token, field, conn} -> Errors.send_error(conn, token, field: field)
      {:error, token, field} -> Errors.send_error(conn, token, field: field)
    end
  end

  @doc false
  def put_alerts(conn, %Context{} = ctx) do
    with {:ok, key} <- optional_key(conn),
         {:ok, if_match} <- if_match(conn),
         {:ok, body, conn} <- json_body(conn),
         :ok <- only_members(body, ["alerts"]),
         {:ok, alerts} <- string_member(body, "alerts") do
      respond(conn, alerts(ctx.relay, ctx.principal, alerts, if_match, key))
    else
      {:error, token, field, conn} -> Errors.send_error(conn, token, field: field)
      {:error, token, field} -> Errors.send_error(conn, token, field: field)
    end
  end

  @doc false
  def put_push(conn, %Context{} = ctx) do
    with :ok <- device_only(ctx.principal),
         {:ok, key} <- optional_key(conn),
         {:ok, body, conn} <- json_body(conn),
         :ok <- only_members(body, ["token", "environment", "revision"]),
         {:ok, token} <- string_member(body, "token"),
         {:ok, environment} <- environment(body),
         {:ok, revision} <- push_revision(body) do
      respond(conn, push(ctx.relay, ctx.principal, token, environment, revision, key))
    else
      {:error, token, field, conn} -> Errors.send_error(conn, token, field: field)
      {:error, token, field} -> Errors.send_error(conn, token, field: field)
    end
  end

  @doc false
  def delete_push(conn, %Context{} = ctx) do
    with :ok <- device_only(ctx.principal),
         {:ok, key} <- optional_key(conn) do
      respond(conn, remove_push(ctx.relay, ctx.principal, key))
    else
      {:error, token, field} -> Errors.send_error(conn, token, field: field)
    end
  end

  @doc false
  def post_files(conn, %Context{} = ctx) do
    with {:ok, key} <- required_key(conn),
         {:ok, name} <- file_name(conn),
         {:ok, length} <- content_length(conn),
         {:ok, dir, cap} <- call(ctx.relay, {:upload_target, ctx.principal}),
         :ok <- within_cap(length, cap) do
      tmp = Path.join(dir, ".tmp-" <> Base.url_encode64(:crypto.strong_rand_bytes(12)))

      try do
        case receive_file(conn, tmp, length, cap) do
          {:ok, size, sha256, conn} ->
            meta = %{name: name, content_type: content_type(conn), size: size, sha256: sha256}
            respond(conn, file(ctx.relay, ctx.principal, meta, tmp, key))

          {:error, token, field, conn} ->
            Errors.send_error(conn, token, field: field)
        end
      after
        File.rm(tmp)
      end
    else
      {:error, token} -> Errors.send_error(conn, to_string(token))
      {:error, token, field} -> Errors.send_error(conn, to_string(token), field: field)
    end
  end

  @doc false
  def delete_file(conn, %Context{} = ctx) do
    respond_to_discard(conn, ctx, fn file_id, key, digest ->
      call(ctx.relay, {:discard_file, ctx.principal, file_id, key, digest})
    end)
  end

  @doc "Serve a discard using a caller that authorizes the principal and runs the write in Relay."
  def respond_to_discard(conn, %Context{params: %{file_id: file_id}} = ctx, discard) do
    with {:ok, key} <- optional_key(conn) do
      digest = Idempotency.digest("DELETE", ["files", file_id], %{})

      case discard.(file_id, key, digest) do
        {status, 204, %{}} when status in [:ok, :replay] ->
          Uploads.clean_discarded(ctx.relay, file_id)
          conn |> replayed(status == :replay) |> send_resp(204, "")

        error ->
          respond(conn, result(error, false))
      end
    else
      {:error, token, field} -> Errors.send_error(conn, token, field: field)
    end
  end

  @doc false
  def put_focus(conn, %Context{} = ctx) do
    with {:ok, _key} <- optional_key(conn),
         {:ok, body, conn} <- json_body(conn) do
      respond(conn, focus(ctx.relay, ctx.principal, body))
    else
      {:error, token, field, conn} -> Errors.send_error(conn, token, field: field)
      {:error, token, field} -> Errors.send_error(conn, token, field: field)
    end
  end

  ## Writes, called outside the relay process

  @doc """
  `PUT /v2/devices/me/focus`: today's focus report for this device. `report`
  has `"screen"`, `"ttl_s"` and, as today's screens need them, `"item_id"` and
  `"visible_entry_id"`. Answers `{rev, show?}` with the device's held show.
  """
  def focus(relay, who, report) do
    browser? = match?(%{kind: :browser}, who)

    case focus_report(report, browser?) do
      {:ok, report} -> result(call(relay, {:focus, who, report}), false)
      {:error, token, field} -> {:error, token, field, false}
    end
  end

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

  @doc """
  `PUT /v2/settings/priority-levels`: replace the person's priority levels.
  `if_match` is the `ETag` of the Settings the client holds; a stale one is
  `412 precondition_failed`.
  """
  def priority_levels(relay, who, levels, if_match, idempotency_key \\ nil) do
    write(relay, who, idempotency_key, {:priority_levels, levels, if_match}, %{
      method: "PUT",
      path: ["settings", "priority-levels"],
      body: %{"levels" => levels, "if_match" => if_match}
    })
  end

  @doc "Replace the person's alert choice with the same precondition as priority levels."
  def alerts(relay, who, alerts, if_match, idempotency_key \\ nil) do
    write(relay, who, idempotency_key, {:alerts, alerts, if_match}, %{
      method: "PUT",
      path: ["settings", "alerts"],
      body: %{"alerts" => alerts, "if_match" => if_match}
    })
  end

  # The old digest identifies a receipt with no recorded push response. A
  # retry with that key must conflict instead of replaying current settings.
  @push_receipt_version 2

  @doc """
  `PUT /v2/devices/me/push`: register or replace this device's push binding.
  `environment` is `"production"` or `"sandbox"`. A `revision` at or below the
  current binding's is `412 precondition_failed`.
  """
  def push(relay, who, token, environment, revision, idempotency_key \\ nil) do
    write(relay, who, idempotency_key, {:push, token, environment, revision}, %{
      method: "PUT",
      path: ["devices", "me", "push"],
      body: %{
        "token" => token,
        "environment" => environment,
        "revision" => revision,
        "receipt_version" => @push_receipt_version
      }
    })
  end

  @doc "`DELETE /v2/devices/me/push`: remove this device's push binding."
  def remove_push(relay, who, idempotency_key \\ nil) do
    write(relay, who, idempotency_key, :remove_push, %{
      method: "DELETE",
      path: ["devices", "me", "push"],
      body: %{"receipt_version" => @push_receipt_version}
    })
  end

  @doc """
  `POST /v2/files`: stage a file the person's replies can name by `file_id`.
  `meta` has `:name`, `:content_type`, `:size` and `:sha256` of the bytes
  written to `tmp_path`, which is moved into place.
  """
  def file(relay, who, meta, tmp_path, idempotency_key) do
    write(relay, who, idempotency_key, {:file, meta, tmp_path}, %{
      method: "POST",
      path: ["files"],
      body: %{
        "name" => meta.name,
        "content_type" => meta.content_type,
        "size" => meta.size,
        "sha256" => meta.sha256
      }
    })
  end

  defp write(relay, who, key, operation, request) do
    digest = Idempotency.digest(request.method, request.path, request.body)

    case call(relay, {:begin, who, key, digest, operation}) do
      {:replay, status, body} ->
        result({:ok, status, body}, true)

      {:apply_agent, engine_key, frame} ->
        outcome =
          case Relay.client_agent_answer(relay, engine_key, frame) do
            {:error, "unauthorized", _, _} -> {:error, :unauthorized}
            {:error, reason, field, _terminal} -> refusal(reason, %{"field" => field})
            result -> result
          end

        result(call(relay, {:finish, who, key, digest, operation, outcome}), false)

      {:apply, engine_key, frame, staged} ->
        outcome =
          with {:ok, frame} <- attach_files(relay, engine_key, frame, staged),
               do: apply_frame(relay, engine_key, frame, staged)

        result(call(relay, {:finish, who, key, digest, operation, outcome}), false)

      :unchanged ->
        result(call(relay, {:finish, who, key, digest, operation, {:ok, :unchanged}}), false)

      error ->
        result(error, false)
    end
  end

  defp result({:ok, status, body}, replayed?), do: {:ok, status, body, replayed?}
  defp result({:error, %Projection{} = fault}, _replayed?), do: {:error, fault}
  defp result({:error, token}, replayed?), do: {:error, to_string(token), nil, replayed?}

  defp result({:error, token, field}, replayed?),
    do: {:error, to_string(token), field, replayed?}

  defp apply_frame(relay, engine_key, frame, []) do
    relay
    |> Relay.request_line(engine_key, JSON.encode!(frame) <> "\n")
    |> apply_reply()
  end

  defp apply_frame(relay, engine_key, frame, staged) do
    sources =
      Map.new(Enum.zip(frame["files"], staged), fn {file, source} ->
        {file["upload_id"], source.file_id}
      end)

    relay |> Relay.request_staged_reply(engine_key, frame, sources) |> apply_reply()
  end

  defp apply_reply({:ok, line}) do
    case JSON.decode_line(line) do
      {:ok, %{"type" => "ok"} = reply} -> {:ok, reply}
      {:ok, %{"type" => "error", "reason" => reason} = reply} -> refusal(reason, reply)
      _ -> {:error, "retry_later"}
    end
  end

  defp apply_reply({:error, :forbidden}), do: {:error, "unauthorized"}
  defp apply_reply({:error, :not_found}), do: {:error, "invalid_field", "file_ids"}
  defp apply_reply({:error, :storage_unavailable}), do: {:error, "storage_unavailable"}
  defp apply_reply(_), do: {:error, "retry_later"}

  # Today's refusal reasons, as tokens (ClientAPI.Errors). A reason with no
  # token is a request the client cannot fix by resending.
  @reasons Map.new(
             ~w(invalid_field bad_frame forbidden not_person not_origin unknown_message
                not_found stale_revision closed retracted operation_conflict
                stale_push_binding inactive_account too_large upload_cap_exceeded
                storage_unavailable)a,
             &{Atom.to_string(&1), &1}
           )

  # Today's field names that the client API renamed.
  @fields %{
    "address" => "token",
    "address_kind" => "token",
    "profile_id" => "environment",
    "files" => "file_ids"
  }

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
        {:error, token, Map.get(@fields, field, field)}

      _ ->
        {:error, token}
    end
  end

  # Each staged file is uploaded to the reply's message through today's
  # upload path, under the reply's operation, then named in its frame.
  def attach_files(_relay, _engine_key, frame, []), do: {:ok, frame}

  def attach_files(relay, engine_key, frame, staged) do
    Enum.reduce_while(staged, {:ok, []}, fn file, {:ok, acc} ->
      attrs = %{
        "op_id" => frame["op_id"],
        "sender_id" => frame["sender_id"],
        "message_id" => frame["message_id"],
        "revision" => frame["request_revision"],
        "name" => file.name,
        client_api_v2_content_type: file.content_type
      }

      case upload(relay, engine_key, attrs, file) do
        {:ok, upload_id} ->
          ref = %{"name" => file.name, "upload_id" => upload_id, "size" => file.size}
          {:cont, {:ok, [Map.put(ref, "sha256", file.sha256) | acc]}}

        error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, files} -> {:ok, Map.put(frame, "files", Enum.reverse(files))}
      error -> error
    end
  end

  defp upload(relay, engine_key, attrs, file) do
    with {:ok, lease} <- Relay.upload_begin(relay, engine_key, attrs, file.size) do
      try do
        Relay.upload_file(relay, engine_key, attrs, file.path, file.size, file.sha256, lease)
      after
        Relay.upload_end(relay, lease)
      end
    end
    |> case do
      {:ok, %{"upload_id" => upload_id}} -> {:ok, upload_id}
      {:error, :upload_cap_exceeded} -> {:error, "payload_too_large"}
      {:error, :upload_conflict} -> {:error, "invalid_field", "file_ids"}
      {:error, :forbidden} -> {:error, "forbidden"}
      {:error, :storage_unavailable} -> {:error, "storage_unavailable"}
      _ -> {:error, "retry_later"}
    end
  end

  defp call(relay, request),
    do: GenServer.call(relay, {:client_api, __MODULE__, request}, :infinity)

  ## Inside the relay

  @doc false
  def handle_call({:upload_target, who}, _from, state) do
    reply =
      with {:ok, _principal} <- principal(state.store.engine, who),
           {:ok, dir} <- staging_dir(state) do
        {:ok, dir, Map.get(state, :upload_cap, :none)}
      end

    {:reply, reply, state}
  end

  def handle_call({:discard_file, who, file_id, key, digest}, _from, state) do
    reply =
      with {:ok, principal} <- principal(state.store.engine, who) do
        discard_staged_upload(
          state,
          principal.person,
          file_id,
          key,
          digest,
          discard_owner(principal)
        )
      end

    {:reply, reply, state}
  end

  def handle_call({:focus, who, report}, _from, state) do
    engine = state.store.engine

    with {:ok, principal} <- principal(engine, who),
         {:ok, position} <- position(state, principal.person),
         {:ok, reported} <- reported_focus(engine, principal.person, report) do
      {key, browser} = focus_reporter(who, principal, report)

      {held, next} =
        if report["screen"] == "signed_out" do
          AttentionServer.Relay.client_api_focus(state, {:signed_out, key})
        else
          # One lapse timer per device: a renewal cancels the previous one.
          Focus.cancel_lapse(get_in(state.focus_clients, [key, :client_api_token]))
          token = Focus.schedule_lapse(self(), key, report["ttl_s"] * 1000)
          AttentionServer.Relay.client_api_focus(
            state,
            {:report, key, principal.person, browser, reported, token}
          )
        end

      result =
        Projection.capture(fn ->
          show = held && Focus.show(key, held, state.clock.())
          body = if show, do: %{"rev" => position, "show" => show}, else: %{"rev" => position}
          {:ok, 200, body}
        end)

      # A failed projection did not deliver the pending Show.
      next =
        case result do
          {:error, %Projection{}} when is_map(held) -> put_in(next.held_shows[key], held)
          _ -> next
        end

      {:reply, result, next}
    else
      error -> {:reply, error, state}
    end
  end

  # Only the timer of the device's newest report ends its focus.
  def handle_call({:focus_lapse, key, id}, _from, state) do
    case get_in(state.focus_clients, [key, :client_api_token]) do
      {:lapse, ^id, _timer} = token ->
        {nil, next} = AttentionServer.Relay.client_api_focus(state, {:lapse, key, token})
        {:reply, :ok, next}

      _ ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:begin, who, key, digest, operation}, from, state) do
    reply =
      with {:ok, principal} <- operation_principal(state.store.engine, who, operation),
           {:ok, nil} <- stored(state, principal.person, key, digest, who, from),
           :ok <- precondition(state, who, principal, operation),
           {:ok, frame} <- frame(state.store.engine, principal, key, operation),
           {:ok, staged} <- staged_files(state, principal, operation) do
        cond do
          principal.kind == :agent -> {:apply_agent, principal.engine_key, frame}
          frame == :unchanged -> :unchanged
          true -> {:apply, principal.engine_key, frame, staged}
        end
      else
        {:replay, status, body} -> {:replay, status, body}
        error -> error
      end

    {:reply, reply, state}
  end

  def handle_call({:finish, who, key, digest, operation, outcome}, from, state) do
    reply =
      with {:ok, principal} <- operation_principal(state.store.engine, who, operation) do
        case outcome do
          {:ok, reply} ->
            result =
              Projection.capture(fn -> render(operation, who, principal.person, reply, from, state) end)

            receipt = receipt(operation, reply, result, principal)
            remember(state, principal.person, key, digest, result, receipt)

          error ->
            error
        end
      end

    {:reply, reply, state}
  end

  defp discard_owner(%{kind: :device, engine_key: device}), do: device
  defp discard_owner(_principal), do: nil

  @doc "Discard within Relay after the caller has authorized the principal."
  def discard_staged_upload(state, person, file_id, key, digest, owner \\ nil) do
    with {:ok, nil} <- stored(state, person, key, digest, nil, nil),
         :ok <- Uploads.verify_discard_owner(state.store, file_id, owner),
         false <- Relay.staged_upload_attached?(state, person, file_id),
         :ok <- Uploads.discard(state.store, person, file_id, key, digest) do
      {:ok, 204, %{}}
    else
      true -> {:error, :already_closed}
      {:error, :disabled} -> {:error, :not_found}
      other -> other
    end
  end

  defp stored(_state, _person, nil, _digest, _who, _from), do: {:ok, nil}

  defp stored(state, person, key, digest, who, from) do
    case Idempotency.lookup(state.store, person, key) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, %{digest: ^digest, status: status, body: receipt}} ->
        case Projection.capture(fn -> replay(receipt, who, person, from, state) end) do
          {:ok, body} -> {:replay, status, body}
          error -> error
        end

      {:ok, _other} ->
        {:error, :idempotency_conflict}

      {:error, :disabled} ->
        {:error, :not_found}

      {:error, _} ->
        {:error, :storage_unavailable}
    end
  end

  # Only a committed success is stored under the key, as today's engine keeps
  # operation receipts only on success: a refused request retried with the
  # same key is evaluated again. The receipt is compact (what the write
  # changed); a replay renders those resources from current state.
  defp remember(state, person, key, digest, {:ok, status, _body} = result, receipt),
    do: remember_committed(state, person, key, digest, status, result, receipt)

  defp remember(state, person, key, digest, {:error, %Projection{}} = result, receipt),
    do: remember_committed(state, person, key, digest, 200, result, receipt)

  defp remember(_state, _person, _key, _digest, result, _receipt), do: result

  defp remember_committed(_state, _person, nil, _digest, _status, result, _receipt), do: result

  defp remember_committed(state, person, key, digest, status, result, receipt) do
    case Idempotency.save(state.store, person, key, digest, status, receipt) do
      :ok -> result
      _ -> {:error, :storage_unavailable}
    end
  end

  # A push response identifies the binding at the time of the write. Keep
  # that small response so a rebind or later delivery cannot rewrite a replay.
  defp receipt(operation, _reply, {:ok, 200, body}, %{engine_key: device})
       when operation == :remove_push or elem(operation, 0) == :push,
       do: %{"push_result" => body, "device" => device}

  defp receipt(operation, _reply, {:error, %Projection{}}, %{engine_key: device})
       when operation == :remove_push or elem(operation, 0) == :push,
       do: %{"push_projection_failed" => device}

  defp receipt(operation, reply, result, _principal), do: receipt(operation, reply, result)

  defp receipt({:reply, item_id, _text, _files}, %{"seq" => seq}, _result),
    do: %{"item_id" => item_id, "entry_seq" => seq}

  defp receipt({kind, item_id}, _reply, _result) when kind in [:seen], do: %{"item_id" => item_id}
  defp receipt({:read_position, item_id, _}, _reply, _result), do: %{"item_id" => item_id}
  defp receipt({:answer, item_id, _, _}, _reply, _result), do: %{"item_id" => item_id}
  defp receipt({:file, _meta, _tmp}, _reply, {:ok, _status, body}), do: %{"file" => body}
  defp receipt(_settings_write, _reply, _result), do: %{"settings" => true}

  defp replay(%{"file" => body}, _who, _person, _from, _state), do: {:ok, body}
  defp replay(%{"discarded_file" => true}, _who, _person, _from, _state), do: {:ok, %{}}

  defp replay(%{"push_projection_failed" => device}, who, _person, _from, state) do
    case principal(state.store.engine, who) do
      {:ok, %{kind: :device, engine_key: ^device}} ->
        {:error, %Projection{key: :settings, cause: "committed_projection_failed"}}

      {:ok, _} ->
        {:error, :idempotency_conflict}

      error ->
        error
    end
  end

  defp replay(%{"push_result" => body, "device" => device}, who, _person, _from, state) do
    case principal(state.store.engine, who) do
      {:ok, %{kind: :device, engine_key: ^device}} -> {:ok, body}
      {:ok, _} -> {:error, :idempotency_conflict}
      error -> error
    end
  end

  defp replay(%{"settings" => true}, who, _person, from, state) do
    case Reads.handle_call({:settings, who}, from, state) do
      {:reply, {:ok, body}, _state} -> {:ok, body}
      {:reply, error, _state} -> error
    end
  end

  defp replay(%{"item_id" => item_id} = receipt, who, person, from, state) do
    with {:ok, _status, body} <- render_item(item_id, who, from, state) do
      case receipt do
        %{"entry_seq" => seq} ->
          with {:ok, entry} <- render_entry(state, who, person, seq),
               do: {:ok, Map.put(body, "entry", entry)}

        _ ->
          {:ok, body}
      end
    end
  end

  defp render({:seen, item_id}, who, _person, _reply, from, state),
    do: render_item(item_id, who, from, state)

  defp render({:read_position, item_id, _entry_id}, who, _person, _reply, from, state),
    do: render_item(item_id, who, from, state)

  defp render({:answer, item_id, _revision, _value}, who, _person, _reply, from, state),
    do: render_item(item_id, who, from, state)

  defp render({:file, meta, tmp_path}, who, person, _reply, _from, state) do
    owner = if match?(%{kind: :device}, who), do: who.key, else: nil

    case Uploads.stage(state.store, person, meta, tmp_path, owner) do
      {:ok, file_id} ->
        {:ok, 201,
         %{
           "file_id" => file_id,
           "name" => meta.name,
           "sha256" => meta.sha256,
           "size" => meta.size
         }}

      {:error, :disabled} ->
        {:error, :not_found}

      _ ->
        {:error, :storage_unavailable}
    end
  end

  defp render(operation, who, _person, _reply, from, state)
       when operation == :remove_push or elem(operation, 0) in [:priority_levels, :alerts, :push] do
    case Reads.handle_call({:settings, who}, from, state) do
      {:reply, {:ok, body}, _state} -> {:ok, 200, body}
      {:reply, error, _state} -> error
    end
  end

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
    Projection.run({:entry, Resources.entry_id(seq)}, fn ->
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
    end)
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

  ## Preconditions checked in the begin step

  # If-Match is compared with the ETag of the Settings this principal reads
  # now, the same value GET /v2/settings sends.
  defp precondition(state, who, _principal, {kind, _value, if_match})
       when kind in [:priority_levels, :alerts] do
    case Reads.handle_call({:settings, who}, nil, state) do
      {:reply, {:ok, %{"settings" => settings}}, _state} ->
        Projection.capture(fn ->
          Projection.run(:settings, fn ->
            if if_match == "*" or Reads.etag(settings) in if_match_tags(if_match),
              do: :ok,
              else: {:error, :precondition_failed}
          end)
        end)

      {:reply, error, _state} ->
        error
    end
  end

  defp precondition(state, _who, principal, operation)
       when operation == :remove_push or elem(operation, 0) == :push do
    cond do
      principal.kind != :device -> {:error, :forbidden}
      not Map.get(state, :push_enabled, false) -> {:error, :unsupported}
      true -> :ok
    end
  end

  defp precondition(_state, _who, _principal, _operation), do: :ok

  defp if_match_tags(value),
    do: value |> String.split(",") |> Enum.map(&String.trim/1)

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
  # An item sent without a session has no conversation at all: a reply there
  # is 400 unsupported (contract rulings round 13); not_found is only for an
  # item the person cannot see.
  defp frame(engine, principal, key, {:reply, item_id, text, _file_ids}) do
    with {:ok, {_person, sender, message_id} = message_key} <-
           Resources.item_key(engine, principal.person, item_id),
         record = Map.fetch!(engine.messages, message_key),
         :ok <- has_conversation(record) do
      {:ok,
       %{
         "v" => 1,
         "type" => "discussion",
         "op_id" => op_id(key),
         "sender_id" => sender,
         "message_id" => message_id,
         "request_revision" => record.revision,
         "text" => text
       }}
    end
  end

  # An answer goes to the engine as today's answer (or answer_no_comment)
  # frame, which keeps the first-answer-settles, stale-revision and
  # retraction rules. A settled item goes straight to the engine, so its
  # refusal is today's.
  defp frame(
         engine,
         %{kind: :agent, person: person},
         key,
         {:answer, item_id, revision, :no_comment}
       ) do
    with {:ok, {_, sender, message_id}} <- Resources.item_key(engine, person, item_id) do
      {:ok,
       key
       |> answer_frame(sender, message_id, revision, nil)
       |> Map.put("type", "answer_no_comment")}
    end
  end

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
                frame = answer_frame(key, sender, message_id, revision, answer)
                {:ok, Map.put(frame, "answer_path", answer_path(value))}
            end
          end
      end
    end
  end

  defp frame(_engine, _principal, key, {:priority_levels, levels, _if_match}) do
    {:ok,
     %{
       "v" => 1,
       "type" => "levels_set",
       "ref" => "client_api_v2",
       "op_id" => op_id(key),
       "levels" => levels
     }}
  end

  defp frame(_engine, _principal, key, {:alerts, alerts, _if_match}) do
    {:ok,
     %{
       "v" => 1,
       "type" => "alerts_set",
       "ref" => "client_api_v2",
       "op_id" => op_id(key),
       "alerts" => alerts
     }}
  end

  # PUT is idempotent: the same binding at the same revision is an unchanged
  # success, so a retry after a lost response is not a stale 412.
  defp frame(engine, principal, key, {:push, token, environment, revision}) do
    case Map.get(engine.push_bindings, {principal.person, principal.engine_key}) do
      %{revision: ^revision, active: true, address: address, profile_id: profile_id} ->
        if address == String.downcase(token) and profile_id == "harklight-" <> environment,
          do: {:ok, :unchanged},
          else: push_frame(key, token, environment, revision)

      _ ->
        push_frame(key, token, environment, revision)
    end
  end

  # The binding is removed at the next revision; DELETE carries none. An
  # invalidated endpoint still has a binding ID until removed.
  defp frame(engine, principal, key, :remove_push) do
    case Map.get(engine.push_bindings, {principal.person, principal.engine_key}) do
      %{revision: current, binding_id: id} when is_binary(id) ->
        {:ok,
         %{"v" => 1, "type" => "push_remove", "op_id" => op_id(key), "revision" => current + 1}}

      _ ->
        {:ok, :unchanged}
    end
  end

  defp frame(_engine, _principal, _key, {:file, _meta, _tmp_path}), do: {:ok, :unchanged}

  defp has_conversation(record) do
    if is_binary(Map.get(record, :session_handle)), do: :ok, else: {:error, :unsupported}
  end

  defp push_frame(key, token, environment, revision) do
    {:ok,
     %{
       "v" => 1,
       "type" => "push_register",
       "op_id" => op_id(key),
       "revision" => revision,
       "address_kind" => "apns_token",
       "address" => token,
       "profile_id" => "harklight-" <> environment
     }}
  end

  # Which answer path stored the value, so a UI/1 result and a plain-text
  # answer stay distinguishable (contract rulings round 16 addendum).
  defp answer_path({:result, _}), do: "result"
  defp answer_path({:comment, _}), do: "comment"
  defp answer_path({:choice, _}), do: "choice"

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

  # The staged files a reply names, resolved for its person.
  defp staged_files(state, principal, {:reply, _item_id, _text, file_ids}) do
    Enum.reduce_while(file_ids, {:ok, []}, fn file_id, {:ok, acc} ->
      case Uploads.fetch(state.store, principal.person, file_id) do
        {:ok, staged} -> {:cont, {:ok, [staged | acc]}}
        {:error, :not_found} -> {:halt, {:error, :invalid_field, "file_ids"}}
        {:error, :disabled} -> {:halt, {:error, :not_found}}
        _ -> {:halt, {:error, :storage_unavailable}}
      end
    end)
    |> case do
      {:ok, staged} ->
        staged = Enum.reverse(staged)

        # One name per reply, as today's engine requires; a duplicate is the
        # client's to fix, never a retry.
        if length(Enum.uniq_by(staged, & &1.name)) == length(staged),
          do: {:ok, staged},
          else: {:error, :invalid_field, "file_ids"}

      error ->
        error
    end
  end

  defp staged_files(_state, _principal, _operation), do: {:ok, []}

  defp staging_dir(state) do
    case Uploads.dir(state.store) do
      {:ok, dir} -> {:ok, dir}
      {:error, :disabled} -> {:error, :not_found}
      _ -> {:error, :storage_unavailable}
    end
  end

  defp item_entry_seq(engine, person, message_key, entry_id) do
    with {:ok, seq} <- Resources.entry_seq(entry_id),
         %{key: ^message_key} <- Map.get(engine.conversation_events, {person, seq}) do
      {:ok, seq}
    else
      _ -> {:error, :invalid_field, "entry_id"}
    end
  end

  # Walks only this item's conversation, newest first, from the entry down to
  # the current position (the Route B index keeps each item's seqs in order).
  defp newest_origin_reply(engine, person, key, seq, current) do
    case Map.get(engine.route_b_index.conversations_by_item, key) do
      nil ->
        nil

      tree ->
        first_origin_reply(:gb_trees.iterator_from({-seq, seq}, tree), engine, person, current)
    end
  end

  defp first_origin_reply(iterator, engine, person, current) do
    case :gb_trees.next(iterator) do
      {{_, seq}, _, _next} when seq <= current ->
        nil

      {{_, seq}, _, next} ->
        case Map.get(engine.conversation_events, {person, seq}) do
          %{payload: %{"kind" => "reply", "author" => "origin"}} -> seq
          _ -> first_origin_reply(next, engine, person, current)
        end

      :none ->
        nil
    end
  end

  # A PUT or DELETE may come without a key; the engine still needs an op_id.
  defp op_id(key) when is_binary(key), do: key
  defp op_id(nil), do: uuid_v4(:crypto.strong_rand_bytes(16))

  defp operation_principal(
         engine,
         %{kind: :agent, person: person, key: key},
         {:answer, item_id, _, _}
       ) do
    with {:ok, {^person, sender, _id}} <- Resources.item_key(engine, person, item_id),
         {:ok, %{person: ^person}} <- Engine.client_agent_answer_scope(engine, key, sender) do
      {:ok, %{kind: :agent, person: person, engine_key: key}}
    end
  end

  defp operation_principal(engine, who, _operation), do: principal(engine, who)

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
           kind: :browser,
           engine_key: BrowserRecipient.key(person, session, recipient)
         }}

      _ ->
        {:error, :unauthorized}
    end
  end

  defp principal(engine, %{kind: :device, key: key}), do: principal(engine, key)

  defp principal(engine, key) when is_binary(key) do
    case Resources.principal(engine, key) do
      {:ok, %{kind: :device, person: person}} ->
        {:ok, %{person: person, kind: :device, engine_key: key}}

      _ ->
        {:error, :unauthorized}
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

  def required_key(conn) do
    case get_req_header(conn, "idempotency-key") do
      [key] ->
        if Idempotency.valid_key?(key),
          do: {:ok, key},
          else: {:error, "invalid_field", "Idempotency-Key"}

      _ ->
        {:error, "invalid_field", "Idempotency-Key"}
    end
  end

  def optional_key(conn) do
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

  # At most one of choice, comment and result, counted by presence; none is no
  # comment. A result is any JSON value, null included, stored unchanged
  # (contract rulings round 16).
  defp answer_value(body) do
    case Map.take(body, ["choice", "comment", "result"]) |> Map.to_list() do
      [] ->
        {:ok, :no_comment}

      [{"choice", id}] when is_binary(id) and id != "" ->
        {:ok, {:choice, id}}

      [{"comment", text}] when is_binary(text) ->
        {:ok, {:comment, text}}

      [{"result", result}] ->
        {:ok, {:result, result}}

      [{name, _}] ->
        {:error, "invalid_field", name}

      _ ->
        {:error, "invalid_field", nil}
    end
  end

  # Content-Disposition: attachment; filename*=UTF-8''<percent-encoded name>
  # (RFC 6266, RFC 5987).
  defp file_name(conn) do
    with [value | _] <- get_req_header(conn, "content-disposition"),
         [_, encoded] <- Regex.run(~r/filename\*\s*=\s*UTF-8''([^;\s]+)/i, value),
         name when is_binary(name) <- percent_decode(encoded),
         true <- name != "" and String.valid?(name) and ConversationFiles.valid_name?(name) do
      {:ok, name}
    else
      _ -> {:error, "invalid_field", "name"}
    end
  end

  defp percent_decode(encoded) do
    URI.decode(encoded)
  rescue
    ArgumentError -> nil
  end

  defp content_length(conn) do
    with [value] <- get_req_header(conn, "content-length"),
         {n, ""} when n >= 0 <- Integer.parse(value) do
      {:ok, n}
    else
      _ -> {:error, "invalid_field", "Content-Length"}
    end
  end

  defp content_type(conn) do
    case get_req_header(conn, "content-type") do
      [value | _] when value != "" -> value
      _ -> "application/octet-stream"
    end
  end

  defp within_cap(_length, :none), do: :ok
  defp within_cap(length, cap) when is_integer(cap) and length <= cap, do: :ok
  defp within_cap(_length, _cap), do: {:error, "payload_too_large", nil}

  # Streams the body to `tmp`, hashing it, and checks it is exactly
  # Content-Length bytes and within the upload cap.
  defp receive_file(conn, tmp, length, cap) do
    case File.open(tmp, [:write, :binary, :exclusive]) do
      {:ok, io} ->
        try do
          receive_chunks(conn, io, :crypto.hash_init(:sha256), 0, length, cap)
        after
          File.close(io)
        end

      _ ->
        {:error, "storage_unavailable", nil, conn}
    end
  end

  defp receive_chunks(conn, io, hash, size, length, cap) do
    case read_body(conn, length: 1_000_000, read_length: 1_000_000) do
      {status, data, conn} when status in [:ok, :more] ->
        size = size + byte_size(data)

        cond do
          is_integer(cap) and size > cap ->
            {:error, "payload_too_large", nil, conn}

          size > length ->
            {:error, "invalid_field", "Content-Length", conn}

          IO.binwrite(io, data) != :ok ->
            {:error, "storage_unavailable", nil, conn}

          status == :more ->
            receive_chunks(conn, io, :crypto.hash_update(hash, data), size, length, cap)

          size != length ->
            {:error, "invalid_field", "Content-Length", conn}

          true ->
            digest = hash |> :crypto.hash_update(data) |> :crypto.hash_final()
            {:ok, size, Base.encode16(digest, case: :lower), conn}
        end

      {:error, _} ->
        {:error, "invalid_field", nil, conn}
    end
  end

  @focus_screens ~w(inbox fyi message chat settings signed_out not_in_front)

  # Today's focus_report shape rules (focus spec r6 V2), with item_id for the
  # pair and visible_entry_id for visible_event_id.
  defp focus_report(body, browser?) do
    screen = body["screen"]
    pair? = screen in ["message", "chat"]
    fields = ~w(screen item_id visible_entry_id ttl_s)
    fields = if browser?, do: ["device_name" | fields], else: fields

    cond do
      member = Enum.find(Map.keys(body), &(&1 not in fields)) ->
        {:error, "invalid_field", member}

      screen not in @focus_screens ->
        {:error, "invalid_field", "screen"}

      pair? and not (is_binary(body["item_id"]) and body["item_id"] != "") ->
        {:error, "invalid_field", "item_id"}

      not pair? and Map.has_key?(body, "item_id") ->
        {:error, "invalid_field", "item_id"}

      Map.has_key?(body, "visible_entry_id") and
          not (screen == "chat" and is_binary(body["visible_entry_id"])) ->
        {:error, "invalid_field", "visible_entry_id"}

      not (is_integer(body["ttl_s"]) and body["ttl_s"] in 1..3600) ->
        {:error, "invalid_field", "ttl_s"}

      browser? and (screen != "signed_out" or Map.has_key?(body, "device_name")) and
          not focus_device_name?(body["device_name"]) ->
        {:error, "invalid_field", "device_name"}

      true ->
        {:ok, body}
    end
  end

  defp focus_device_name?(name),
    do: is_binary(name) and byte_size(name) in 1..128 and String.trim(name) != ""

  defp focus_reporter(%{kind: :browser} = who, _principal, report) do
    browser = %{owner: who.key, revision: who.owner_revision, name: report["device_name"]}
    {{:browser, who.session_id}, browser}
  end

  defp focus_reporter(_who, principal, _report), do: {principal.engine_key, nil}

  # The report in today's identifiers, which the origin focus APIs read.
  defp reported_focus(engine, person, report) do
    base = %{"screen" => report["screen"]}

    case report["item_id"] do
      nil ->
        {:ok, base}

      item_id ->
        with {:ok, {_person, sender, message_id} = message_key} <-
               item_or_invalid(engine, person, item_id),
             {:ok, visible} <- visible_event(engine, person, message_key, report) do
          base
          |> Map.merge(%{"sender_id" => sender, "message_id" => message_id})
          |> then(&if(visible, do: Map.put(&1, "visible_event_id", visible), else: &1))
          |> then(&{:ok, &1})
        end
    end
  end

  defp item_or_invalid(engine, person, item_id) do
    case Resources.item_key(engine, person, item_id) do
      {:ok, key} -> {:ok, key}
      _ -> {:error, :invalid_field, "item_id"}
    end
  end

  defp visible_event(engine, person, message_key, %{"visible_entry_id" => entry_id}) do
    with {:ok, seq} <- Resources.entry_seq(entry_id),
         %{key: ^message_key, payload: %{"event_id" => event_id}} when is_binary(event_id) <-
           Map.get(engine.conversation_events, {person, seq}) do
      {:ok, event_id}
    else
      _ -> {:error, :invalid_field, "visible_entry_id"}
    end
  end

  defp visible_event(_engine, _person, _message_key, _report), do: {:ok, nil}

  defp position(state, person) do
    case ChangeLog.head(state.store, person) do
      {:ok, head} -> {:ok, head}
      {:error, :disabled} -> {:error, :not_found}
      _ -> {:error, :storage_unavailable}
    end
  end

  defp if_match(conn) do
    case get_req_header(conn, "if-match") do
      [value | _] when value != "" -> {:ok, Enum.join(get_req_header(conn, "if-match"), ",")}
      _ -> {:error, "invalid_field", "If-Match"}
    end
  end

  defp list_member(body, name) do
    case Map.get(body, name) do
      value when is_list(value) -> {:ok, value}
      _ -> {:error, "invalid_field", name}
    end
  end

  defp environment(body) do
    case Map.get(body, "environment") do
      value when value in ["production", "sandbox"] -> {:ok, value}
      _ -> {:error, "invalid_field", "environment"}
    end
  end

  defp push_revision(body) do
    case Map.get(body, "revision") do
      n when is_integer(n) and n >= 1 -> {:ok, n}
      _ -> {:error, "invalid_field", "revision"}
    end
  end

  def device_only(%{kind: :device}), do: :ok
  def device_only(_principal), do: {:error, "forbidden", nil}

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

  def only_members(body, allowed) do
    case Enum.find(Map.keys(body), &(&1 not in allowed)) do
      nil -> :ok
      member -> {:error, "invalid_field", member}
    end
  end

  def respond(conn, {:ok, status, body, replayed?}),
    do: conn |> replayed(replayed?) |> Errors.send_json(status, body)

  def respond(conn, {:error, %Projection{} = fault}), do: Errors.projection_error(conn, fault)

  def respond(conn, {:error, token, field, replayed?}),
    do: conn |> replayed(replayed?) |> Errors.send_error(token, field: field)

  defp replayed(conn, true), do: put_resp_header(conn, "idempotent-replayed", "true")
  defp replayed(conn, false), do: conn
end
