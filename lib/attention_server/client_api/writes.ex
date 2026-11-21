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

  alias AttentionServer.{BrowserRecipient, Engine, JSON, Relay}
  alias AttentionServer.ClientAPI.{Context, Errors, Idempotency, Reads, Resources}

  @max_body_bytes 64 * 1024

  ## Routes (mounted by ClientAPI.Router behind client_api_v2)

  @impl true
  def routes do
    [
      {"POST", ["items", :item_id, "seen"], :post_seen}
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

  ## Writes, called outside the relay process

  @doc "`POST /v2/items/{item_id}/seen`: mark the item's current revision Seen."
  def seen(relay, who, item_id, idempotency_key) do
    write(relay, who, idempotency_key, {:seen, item_id}, %{
      method: "POST",
      path: ["items", item_id, "seen"],
      body: %{}
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

  # Today's refusal reasons, as tokens. A reason with no token is a request the
  # client cannot fix by resending.
  defp refusal(reason, reply) do
    token =
      with true <- is_binary(reason),
           {:ok, token} <- Errors.token_for_reason(String.to_existing_atom(reason)) do
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
  rescue
    ArgumentError -> {:error, "invalid_field"}
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
        {:apply, principal.engine_key, frame}
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
            {:ok, _reply} -> render(operation, who, from, state)
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

  defp render({:seen, item_id}, who, from, state) do
    case Reads.handle_call({:item, who, item_id}, from, state) do
      {:reply, {:ok, body}, _state} -> {:ok, 200, body}
      {:reply, error, _state} -> error
    end
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
