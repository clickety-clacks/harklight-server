defmodule AttentionServer.ClientAPI.AgentReads do
  @moduledoc "The enrolled agent's sent-message and conversation snapshots."

  @behaviour AttentionServer.ClientAPI.Routes

  import Plug.Conn, only: [fetch_query_params: 1]

  alias AttentionServer.{AgentChangeLog, Engine, Message, RouteBIndex}
  alias AttentionServer.AgentAPI.RecipientReach
  alias AttentionServer.ClientAPI.{AgentResources, Context, Projection, Reads, SharedReads}

  @page_size 50
  @filters ~w(session sender legacy_message_id)

  @impl true
  def routes do
    [
      {"GET", ["agent", "messages"], :messages},
      {"GET", ["agent", "messages", :item_id], :message},
      {"GET", ["agent", "messages", :item_id, "entries"], :entries},
      {"GET", ["agent", "messages", :item_id, "revisions", :n], :revision}
    ]
  end

  def messages(conn, ctx) do
    conn = fetch_query_params(conn)
    Reads.respond(conn, call(ctx, {:messages, Map.take(conn.query_params, ["page" | @filters])}))
  end

  def message(conn, ctx) do
    SharedReads.respond(conn, ctx, :sender, :summary, fn ->
      Reads.respond(conn, call(ctx, {:message, ctx.params.item_id}), &Reads.etag(&1["message"]))
    end)
  end

  def entries(conn, ctx) do
    conn = fetch_query_params(conn)
    SharedReads.respond(conn, ctx, :sender, {:entries, conn.query_params}, fn ->
      Reads.respond(conn, call(ctx, {:entries, ctx.params.item_id, conn.query_params}))
    end)
  end

  def revision(conn, ctx) do
    SharedReads.respond(conn, ctx, :sender, {:revision, ctx.params.n}, fn ->
      Reads.respond(conn, call(ctx, {:revision, ctx.params.item_id, ctx.params.n}))
    end)
  end

  defp call(%Context{principal: %{kind: :device} = principal, relay: relay}, request) do
    GenServer.call(relay, {:client_api, __MODULE__, {principal, request}}, :infinity)
  end

  defp call(_ctx, _request), do: {:error, :forbidden}

  @doc false
  def handle_call({%{person: person, key: key}, request}, _from, state) do
    scope = {person, key}

    result =
      with {:ok, ^person} <- Engine.authorized_person(state.store.engine, key),
           {:ok, head} <- AgentChangeLog.head(state.store, scope) do
        Projection.capture(fn -> read(request, state, scope, head) end)
      else
        {:error, :disabled} -> {:error, :not_found}
        {:error, :unauthorized} -> {:error, :unauthorized}
        _ -> {:error, :storage_unavailable}
      end

    {:reply, result, state}
  end

  defp read({:messages, params}, state, scope, head) do
    filters = Map.take(params, @filters)

    with :ok <- validate_filters(filters),
         prefix = page_prefix(scope, filters),
         {:ok, start} <- page_start(Map.get(params, "page"), prefix) do
      tree =
        RouteBIndex.message_page(state.store.engine.route_b_index, scope, filters)

      rows =
        iterator(tree, start)
        |> Stream.unfold(fn iterator ->
          case :gb_trees.next(iterator) do
            :none -> nil
            {seq, key, rest} -> {{seq, key}, rest}
          end
        end)
        |> Enum.take(@page_size + 1)

      {page, remaining} = Enum.split(rows, @page_size)

      with {:ok, messages} <- render_messages(state, scope, page) do
        body = %{"rev" => head, "messages" => messages}

        case remaining do
          [] -> {:ok, body}
          [{next, _}] -> {:ok, Map.put(body, "next_page", prefix <> Integer.to_string(-next))}
        end
      end
    end
  end

  defp read({:message, item_id}, state, scope, head) do
    with {:ok, key} <- AgentResources.own_item_key(state, scope, item_id),
         {:ok, message} <- AgentResources.message(state, scope, key) do
      message = RecipientReach.add(message, state, scope)
      {:ok, %{"rev" => head, "message" => message}}
    end
  end

  defp read({:entries, item_id, params}, state, scope, head) do
    with {:ok, key} <- AgentResources.own_item_key(state, scope, item_id),
         {:ok, limit} <- Reads.limit(params),
         {:ok, entries, older} <-
           AgentResources.entry_page(state, scope, key, params["before"], limit) do
      {:ok, %{"rev" => head, "entries" => entries, "older" => older}}
    end
  end

  defp read({:revision, item_id, n}, state, scope, head) do
    with {:ok, key} <- AgentResources.own_item_key(state, scope, item_id),
         {:ok, revision} <- Reads.revision_number(n),
         {:ok, resource} <- AgentResources.revision(state, key, revision) do
      {:ok, %{"rev" => head, "revision" => resource}}
    end
  end

  defp render_messages(state, scope, rows) do
    rows
    |> Enum.reduce_while({:ok, []}, fn {_, key}, {:ok, acc} ->
      case AgentResources.message(state, scope, key) do
        {:ok, message} -> {:cont, {:ok, [message | acc]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, messages} -> {:ok, Enum.reverse(messages)}
      error -> error
    end
  end

  defp validate_filters(filters) do
    Enum.reduce_while(filters, :ok, fn {name, value}, :ok ->
      valid =
        case name do
          "session" -> Message.uuid_v4?(value)
          "sender" -> Message.sender_id?(value) and value != Message.hosted_notice_sender()
          "legacy_message_id" -> Message.nonempty?(value)
        end

      if valid, do: {:cont, :ok}, else: {:halt, {:error, :invalid_field, name}}
    end)
  end

  defp iterator(tree, nil), do: :gb_trees.iterator(tree)
  defp iterator(tree, start), do: :gb_trees.iterator_from(start, tree)

  defp page_prefix(scope, filters) do
    digest = :crypto.hash(:sha256, :erlang.term_to_binary({scope, filters}))
    "a" <> Base.url_encode64(digest, padding: false) <> "."
  end

  defp page_start(nil, _prefix), do: {:ok, nil}

  defp page_start(token, prefix) when is_binary(token) and byte_size(token) <= 128 do
    size = byte_size(prefix)

    with <<^prefix::binary-size(size), position::binary>> <- token,
         {seq, ""} when seq > 0 <- Integer.parse(position) do
      {:ok, -seq}
    else
      _ -> {:error, :invalid_field, "page"}
    end
  end

  defp page_start(_token, _prefix), do: {:error, :invalid_field, "page"}
end
