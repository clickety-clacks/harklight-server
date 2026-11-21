defmodule AttentionServer.ClientAPI.Reads do
  @moduledoc """
  The client API v2 reads (docs/client-api-v2.md §4): the inbox, one item, an
  item's entries, settings and files.

  Each read runs in one relay call, so the resources and the position (`rev`)
  it reports come from the same serialized state. Read results are plain maps;
  the HTTP layer encodes them and sets `ETag` with `etag/1`.

  Results are `{:ok, body}`, or `{:error, token}` / `{:error, token, field}`
  with a token from the contract's closed error list.

  The `who` argument of each read is the request's principal: a map with
  `kind: :device` and the device `key`, or `kind: :browser` with `person`, the
  owner `key`, `owner_revision` and `session_id` (the router's
  `ClientAPI.Principal`). A bare device key is accepted too.
  """

  @behaviour AttentionServer.ClientAPI.Routes

  import Plug.Conn

  alias AttentionServer.{
    ChangeLog,
    ConversationFiles,
    Engine,
    FeatureFlags,
    JSON,
    LargeData,
    Relay,
    UI1
  }

  alias AttentionServer.ClientAPI.{Context, Errors, Resources}

  # The client features still behind an operator flag at 0.2 (contract §4).
  @client_features ~w(server_matched_push_routing priority_level_editing)
  @retention_days 30

  @default_limit 50
  @max_limit 200

  ## Routes (mounted by ClientAPI.Router behind client_api_v2)

  @impl true
  def routes do
    [
      {"GET", ["inbox"], :get_inbox},
      {"GET", ["items", :item_id], :get_item},
      {"GET", ["items", :item_id, "entries"], :get_entries},
      {"GET", ["items", :item_id, "revisions", :n], :get_revision},
      {"GET", ["settings"], :get_settings},
      {"GET", ["me"], :get_me},
      {"GET", ["files", :file_id], :get_file}
    ]
  end

  @doc false
  def get_inbox(conn, %Context{} = ctx) do
    conn = fetch_query_params(conn)
    respond(conn, inbox(ctx.relay, ctx.principal, query(conn, ~w(limit page))))
  end

  @doc false
  def get_item(conn, %Context{} = ctx),
    do: respond(conn, item(ctx.relay, ctx.principal, ctx.params.item_id))

  @doc false
  def get_entries(conn, %Context{} = ctx) do
    conn = fetch_query_params(conn)

    respond(
      conn,
      entries(ctx.relay, ctx.principal, ctx.params.item_id, query(conn, ~w(before limit)))
    )
  end

  @doc false
  def get_revision(conn, %Context{} = ctx),
    do: respond(conn, revision(ctx.relay, ctx.principal, ctx.params.item_id, ctx.params.n))

  @doc false
  def get_settings(conn, %Context{} = ctx),
    do: respond(conn, settings(ctx.relay, ctx.principal), &etag(&1["settings"]))

  @doc false
  def get_me(conn, %Context{} = ctx), do: respond(conn, me(ctx.relay, ctx.principal))

  @doc false
  def get_file(conn, %Context{} = ctx) do
    case file(ctx.relay, ctx.principal, ctx.params.file_id) do
      {:ok, file} ->
        tag = file_etag(file)

        conn =
          conn
          |> put_resp_header("etag", tag)
          |> put_resp_header("cache-control", "private, no-cache")

        cond do
          not_modified?(conn, tag) ->
            send_resp(conn, 304, "")

          Map.has_key?(file, :path) ->
            conn |> file_headers(ctx.principal, file) |> send_file(200, file.path)

          true ->
            conn |> file_headers(ctx.principal, file) |> send_resp(200, file.data)
        end

      error ->
        respond(conn, error)
    end
  end

  # Round 18: a browser gets a file exactly as today's browser download, an
  # attachment of application/octet-stream, so the web origin never renders
  # it inline; a device gets the stored type.
  defp file_headers(conn, %{kind: :browser}, file) do
    disposition =
      case file[:name] do
        name when is_binary(name) and name != "" ->
          "attachment; filename*=UTF-8''" <> URI.encode(name, &URI.char_unreserved?/1)

        _ ->
          "attachment"
      end

    conn
    |> put_resp_header("content-type", "application/octet-stream")
    |> put_resp_header("content-disposition", disposition)
  end

  defp file_headers(conn, _device, file),
    do: put_resp_header(conn, "content-type", file.content_type)

  # Only the parameters a read takes; anything else in the query is ignored.
  defp query(conn, names), do: Map.take(conn.query_params, names)

  defp respond(conn, result, etag_of \\ &etag/1)

  defp respond(conn, {:ok, body}, etag_of) do
    tag = etag_of.(body)

    conn =
      conn
      |> put_resp_header("etag", tag)
      |> put_resp_header("cache-control", "private, no-cache")

    if not_modified?(conn, tag),
      do: send_resp(conn, 304, ""),
      else: Errors.send_json(conn, 200, body)
  end

  defp respond(conn, {:error, token}, _etag_of),
    do: Errors.send_error(conn, Atom.to_string(token))

  defp respond(conn, {:error, token, field}, _etag_of),
    do: Errors.send_error(conn, Atom.to_string(token), field: field)

  # If-None-Match uses the weak comparison (RFC 9110 §13.1.2).
  defp not_modified?(conn, tag) do
    conn
    |> get_req_header("if-none-match")
    |> Enum.flat_map(&String.split(&1, ","))
    |> Enum.map(&String.trim/1)
    |> Enum.any?(&(&1 == "*" or &1 == tag or &1 == "W/" <> tag))
  end

  ## Reads, called outside the relay process

  @doc "`GET /v2/inbox`: params `\"limit\"` and `\"page\"`, as given in the query."
  def inbox(relay, key, params \\ %{}), do: call(relay, {:inbox, key, params})

  @doc "`GET /v2/items/{item_id}`."
  def item(relay, key, item_id), do: call(relay, {:item, key, item_id})

  @doc "`GET /v2/items/{item_id}/entries`: params `\"before\"` and `\"limit\"`."
  def entries(relay, key, item_id, params \\ %{}),
    do: call(relay, {:entries, key, item_id, params})

  @doc "`GET /v2/items/{item_id}/revisions/{n}`: one request revision, 1 to current."
  def revision(relay, key, item_id, n), do: call(relay, {:revision, key, item_id, n})

  @doc "`GET /v2/me`: the account's state, client features and limits."
  def me(relay, key), do: call(relay, {:me, key})

  @doc "`GET /v2/settings`."
  def settings(relay, key), do: call(relay, {:settings, key})

  @doc """
  `GET /v2/files/{file_id}`: `{:ok, file}` where `file` has `:size`, `:sha256`,
  `:content_type` and either `:path` (a verified upload) or `:data`.
  """
  def file(relay, key, file_id) do
    case call(relay, {:file, key, file_id}) do
      {:ok, {:upload, upload_id}} ->
        case fetch_upload(relay, key, upload_id) do
          {:ok, %{size: size, sha256: sha256} = upload, path} ->
            # Uploads made through POST /v2/files keep the type they were sent
            # with; older uploads have none.
            content_type = Map.get(upload, :content_type) || "application/octet-stream"

            {:ok,
             file_result(size, sha256,
               path: path,
               content_type: content_type,
               name: Map.get(upload, :name)
             )}

          {:error, :storage_unavailable} ->
            {:error, :storage_unavailable}

          _ ->
            {:error, :not_found}
        end

      {:ok, {:inline, data, sha256, name}} ->
        with {:ok, bytes} <- inline_bytes(data, sha256),
             do: {:ok, file_result(byte_size(bytes), sha256, data: bytes, name: name)}

      error ->
        error
    end
  end

  defp fetch_upload(relay, %{kind: :browser} = who, upload_id),
    do: Relay.browser_fetch_file(relay, who.person, who.key, who.owner_revision, upload_id)

  defp fetch_upload(relay, %{kind: :device, key: key}, upload_id),
    do: Relay.fetch_file(relay, key, upload_id)

  defp fetch_upload(relay, key, upload_id), do: Relay.fetch_file(relay, key, upload_id)

  defp file_result(size, sha256, source),
    do:
      Map.merge(
        %{size: size, sha256: sha256, content_type: "application/octet-stream"},
        Map.new(source)
      )

  @doc """
  The strong `ETag` of a read body. For `GET /v2/settings` it is taken over the
  Settings resource alone, so it is also the `If-Match` value for settings
  writes and does not change when unrelated changes move the position.
  """
  def etag(body) when is_map(body) do
    digest = :crypto.hash(:sha256, JSON.encode!(body))
    ~s("#{Base.url_encode64(digest, padding: false)}")
  end

  @doc "The `ETag` for a file: its quoted sha256."
  def file_etag(%{sha256: sha256}), do: ~s("#{sha256}")

  defp call(relay, request),
    do: GenServer.call(relay, {:client_api, __MODULE__, request}, :infinity)

  ## Inside the relay

  @doc false
  def handle_call(request, _from, state) do
    reply =
      try do
        read(request, state)
      catch
        {:client_api_read, token} -> {:error, token}
      end

    {:reply, reply, state}
  end

  defp read({:inbox, key, params}, state) do
    engine = state.store.engine

    with {:ok, principal} <- principal(engine, key),
         {:ok, limit} <- limit(params),
         {:ok, after_key} <- page(params),
         {:ok, _} <- head(state, principal.person) do
      person = principal.person
      {shown, more?} = rank_page(state, person, after_key, limit)

      items =
        Enum.flat_map(shown, fn {item_id, rank} ->
          case Resources.item_key(engine, person, item_id) do
            {:ok, item_key} -> [render_item(state, principal, item_key, rank)]
            _ -> []
          end
        end)

      {:ok, position} = head(state, person)

      body = %{
        "rev" => position,
        "items" => items,
        "counts" => Resources.counts(engine, person)
      }

      if more? do
        {_item_id, %{"key" => last_key}} = List.last(shown)
        {:ok, Map.put(body, "next_page", "p" <> Base.url_encode64(last_key, padding: false))}
      else
        {:ok, body}
      end
    end
  end

  defp read({:item, key, item_id}, state) do
    engine = state.store.engine

    with {:ok, principal} <- principal(engine, key),
         {:ok, item_key} <- Resources.item_key(engine, principal.person, item_id),
         {:ok, position} <- head(state, principal.person),
         context = %{engine: engine, store: state.store, view: view(state, principal.person)},
         {:ok, rank} <- item_rank(context, principal.person, item_id) do
      {:ok,
       %{
         "rev" => position,
         "item" => render_item(state, principal, item_key, rank)
       }}
    end
  end

  defp read({:revision, key, item_id, n}, state) do
    engine = state.store.engine

    with {:ok, principal} <- principal(engine, key),
         {:ok, item_key} <- Resources.item_key(engine, principal.person, item_id),
         {:ok, n} <- revision_number(n),
         {:ok, revision} <- Resources.revision(engine, item_key, n),
         {:ok, position} <- head(state, principal.person) do
      {:ok, %{"rev" => position, "revision" => revision}}
    end
  end

  defp read({:entries, key, item_id, params}, state) do
    engine = state.store.engine

    with {:ok, principal} <- principal(engine, key),
         {:ok, item_key} <- Resources.item_key(engine, principal.person, item_id),
         {:ok, limit} <- limit(params),
         {:ok, position} <- head(state, principal.person) do
      all = Resources.entries(engine, item_key, Map.get(state.priority_views, principal.person))

      with {:ok, older_than} <- before(params, all) do
        eligible = if older_than, do: Enum.filter(all, &(&1["position"] < older_than)), else: all
        page = Enum.take(eligible, -limit)

        entries =
          Enum.map(page, fn entry ->
            entry
            |> Resources.stamp(entry_rev(state, principal.person, entry))
            |> Resources.put_at(first_seen(state, principal.person, entry))
            |> then(&Resources.render(:entry, &1, principal))
          end)

        {:ok,
         %{
           "rev" => position,
           "entries" => entries,
           "older" => length(eligible) > limit
         }}
      end
    end
  end

  defp read({:me, key}, state) do
    engine = state.store.engine

    with {:ok, principal} <- principal(engine, key) do
      # Read with warnings already given, so a read never logs; the Relay warns
      # about an unreadable flags file where it reads it for itself.
      {flags, _warned} = FeatureFlags.read(state.feature_flags_path, true)

      {:ok,
       %{
         "account" => %{
           "state" =>
             if(Engine.delivery_stopped?(engine, principal.person), do: "paused", else: "active")
         },
         "features" => Map.new(@client_features, &{&1, Map.get(flags, &1) == true}),
         "limits" => %{
           "upload_cap_bytes_per_file" =>
             if(state.upload_cap == :none, do: "none", else: state.upload_cap),
           "inline_decoded_bytes_per_entry" => ConversationFiles.inline_limit(),
           "answer_ui_limits" => UI1.published_limits(),
           "retention_days" => @retention_days
         }
       }}
    end
  end

  defp read({:settings, key}, state) do
    engine = state.store.engine

    with {:ok, principal} <- principal(engine, key),
         {:ok, position} <- head(state, principal.person) do
      settings =
        engine
        |> Resources.settings(principal.person)
        |> Resources.stamp(rev(state, principal.person, "settings"))
        |> then(&Resources.render(:settings, &1, principal))

      {:ok, %{"rev" => position, "settings" => settings}}
    end
  end

  defp read({:file, key, file_id}, state) do
    engine = state.store.engine

    with {:ok, principal} <- principal(engine, key),
         {:ok, source} <- Resources.file_source(engine, principal.person, file_id) do
      case source do
        {:upload, upload_id} -> {:ok, {:upload, upload_id}}
        {:inline, file} -> inline_file(file)
      end
    end
  end

  # The bytes are decoded and checked by the caller, outside the relay: large
  # inline data lives in a file, and reading it must neither block the relay
  # nor crash it when the file is missing or damaged.
  defp inline_file(%{"data_base64" => data, "sha256" => sha256} = file)
       when is_binary(data) or is_struct(data, LargeData.Ref),
       do: {:ok, {:inline, data, sha256, file["name"]}}

  defp inline_file(_file), do: {:error, :not_found}

  defp inline_bytes(data, sha256) do
    encoded =
      case data do
        %LargeData.Ref{} = ref -> LargeData.read!(ref)
        data -> data
      end

    with {:ok, bytes} <- Base.decode64(encoded),
         ^sha256 <- Base.encode16(:crypto.hash(:sha256, bytes), case: :lower) do
      {:ok, bytes}
    else
      _ -> {:error, :storage_unavailable}
    end
  rescue
    ArgumentError -> {:error, :storage_unavailable}
  end

  defp render_item(state, principal, item_key, rank) do
    state.store.engine
    |> Resources.item(item_key, rank)
    |> Resources.stamp(rev(state, principal.person, ChangeLog.item_resource(item_key)))
    |> then(&Resources.render(:item, &1, principal))
  end

  @doc false
  # The render principal for a request principal, shared with the feed renderer.
  def client_principal(engine, who), do: principal(engine, who)

  # A browser session is the person's owner key pinned to an owner revision,
  # re-checked here as today's browser paths do. It is never an item's origin
  # and has no subscriptions, so it sees knocks, as today's browser rows do.
  defp principal(engine, %{kind: :browser, person: person, key: owner_key} = who) do
    pinned = Map.get(who, :owner_revision)

    case Engine.owner_revision(engine, person, owner_key) do
      {:ok, ^pinned} ->
        {:ok,
         %{
           person: person,
           # Internal only: never an origin, never sent or logged.
           key: "browser:" <> Base.url_encode64(to_string(Map.get(who, :session_id))),
           kind: :browser,
           senders: MapSet.new()
         }}

      _ ->
        {:error, :unauthorized}
    end
  end

  defp principal(engine, %{kind: :device, key: key}), do: principal(engine, key)

  defp principal(engine, key) when is_binary(key) do
    case Resources.principal(engine, key) do
      {:ok, %{kind: :device} = principal} -> {:ok, principal}
      _ -> {:error, :unauthorized}
    end
  end

  defp principal(_engine, _who), do: {:error, :unauthorized}

  # Ranks are stored by the change log's rank hook, once per change (review
  # findings F3/F5). A read pages from that stored order. Only a person with
  # nothing stored yet has them computed here, from the same inputs the hook
  # uses, so the result is the same.

  defp rank_page(state, person, after_key, limit) do
    case ChangeLog.rank_page(state.store, person, after_key, limit) do
      {:ok, page, more?} ->
        {page, more?}

      {:error, :none} ->
        candidates =
          state
          |> computed_ranks(person)
          |> Enum.sort_by(fn {_item_id, rank} -> rank["key"] end)
          |> Enum.drop_while(fn {_item_id, rank} -> after_key && rank["key"] <= after_key end)
          |> Enum.take(limit + 1)

        {Enum.take(candidates, limit), length(candidates) > limit}

      _ ->
        throw({:client_api_read, :storage_unavailable})
    end
  end

  @doc false
  # One item's rank, for reads and the feed renderer: stored, or computed for
  # a person with nothing stored yet.
  def item_rank(%{engine: engine, store: store, view: view}, person, item_id) do
    case ChangeLog.rank(store, person, item_id) do
      {:ok, rank} ->
        {:ok, rank}

      # A store that fails is a storage failure, never a reason to compute
      # every rank inside the relay.
      {:error, reason} when reason not in [:not_found, :disabled] ->
        {:error, :storage_unavailable}

      _ ->
        case engine
             |> Resources.ranks(person, view, stored_unit_keys(store, person))
             |> Map.fetch(item_id) do
          {:ok, rank} -> {:ok, rank}
          :error -> {:error, :not_found}
        end
    end
  end

  defp computed_ranks(state, person),
    do:
      Resources.ranks(
        state.store.engine,
        person,
        view(state, person),
        stored_unit_keys(state.store, person)
      )

  defp stored_unit_keys(store, person) do
    case ChangeLog.rank_keys(store, person) do
      {:ok, keys} -> keys
      _ -> %{}
    end
  end

  defp view(state, person), do: Map.get(state.priority_views, person)

  ## Change-log positions
  #
  # Positions and resource revs come from the change log, in this same relay
  # step. Ranks have no rows of their own yet, so rank keys are recomputed from
  # the current order until the change log keeps the keys it last emitted.

  defp head(state, person) do
    case ChangeLog.head(state.store, person) do
      {:ok, head} -> {:ok, head}
      # With client_api_v2 off there is no /v2 to read.
      {:error, :disabled} -> {:error, :not_found}
      _ -> {:error, :storage_unavailable}
    end
  end

  defp rev(state, person, resource_id) do
    case ChangeLog.rev(state.store, person, resource_id) do
      {:ok, rev} -> rev
      # A resource that exists with no row: settings of a person who had no
      # data at cut-over, with defaults only. It reads as the first version.
      {:error, :not_found} -> 1
      _ -> throw({:client_api_read, :storage_unavailable})
    end
  end

  defp entry_rev(state, person, entry) do
    entry
    |> Resources.entry_sources()
    |> Enum.map(&rev(state, person, ChangeLog.entry_resource(&1)))
    |> Enum.max()
  end

  defp first_seen(state, person, entry) do
    case ChangeLog.created_at(state.store, person, ChangeLog.entry_resource(entry["position"])) do
      {:ok, at_ms} -> at_ms
      _ -> nil
    end
  end

  ## Parameters

  defp limit(params) do
    case Map.get(params, "limit") do
      nil -> {:ok, @default_limit}
      value -> parse_limit(value)
    end
  end

  defp parse_limit(value) when is_integer(value) and value > 0, do: {:ok, min(value, @max_limit)}

  defp parse_limit(value) when is_binary(value) do
    case Integer.parse(value) do
      {n, ""} when n > 0 -> {:ok, min(n, @max_limit)}
      _ -> {:error, :invalid_field, "limit"}
    end
  end

  defp parse_limit(_value), do: {:error, :invalid_field, "limit"}

  defp revision_number(n) when is_integer(n) and n > 0, do: {:ok, n}

  defp revision_number(n) when is_binary(n) do
    case Integer.parse(n) do
      {value, ""} when value > 0 -> {:ok, value}
      _ -> {:error, :not_found}
    end
  end

  defp revision_number(_n), do: {:error, :not_found}

  defp page(params) do
    case Map.get(params, "page") do
      nil ->
        {:ok, nil}

      "p" <> encoded ->
        case Base.url_decode64(encoded, padding: false) do
          {:ok, rank_key} when rank_key != "" -> {:ok, rank_key}
          _ -> {:error, :invalid_field, "page"}
        end

      _ ->
        {:error, :invalid_field, "page"}
    end
  end

  defp before(params, entries) do
    case Map.get(params, "before") do
      nil ->
        {:ok, nil}

      entry_id ->
        case Enum.find(entries, &(&1["entry_id"] == entry_id)) do
          %{"position" => position} -> {:ok, position}
          nil -> {:error, :invalid_field, "before"}
        end
    end
  end
end
