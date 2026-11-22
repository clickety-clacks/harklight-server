defmodule AttentionServer.AgentAPIInProcess do
  @moduledoc """
  Drives the client API v2 agent routes inside a Relay started directly, with
  a trusted device key in place of a TLS peer. Each call runs the route's own
  Relay step, so it reads and writes exactly what the HTTP route does after
  the request body has been accepted.

  - `item_id/4`: the v2 item id of a stored message.
  - `message/4`, `entries/5`: `GET /v2/agent/messages/{id}` and `/entries`.
  - `changes/4`: one `GET /v2/agent/changes` batch, which commits ACK1 for
    the person entries and answers it returns.
  - `write/6`: one `POST /v2/agent/...` write, for example
    `write(relay, "A", "origin", :chat_receipt, %{item_id: id, entry_id: e}, body)`.
  """

  alias AttentionServer.ConversationFiles
  alias AttentionServer.AgentAPI.FeedStep
  alias AttentionServer.ClientAPI.{AgentReads, AgentWrites, Resources}

  @doc "The v2 item id of `{person, sender_id, message_id}`."
  def item_id(relay, person, sender_id, message_id) do
    {:ok, id} = Resources.item_id(engine(relay), {person, sender_id, message_id})
    id
  end

  def message(relay, person, key, item_id),
    do: read(relay, person, key, {:message, item_id})

  def entries(relay, person, key, item_id, params \\ %{}),
    do: read(relay, person, key, {:entries, item_id, params})

  @doc "Reads one agent feed batch from `cursor`. Returns `{:ok, changes, more, position}`."
  def changes(relay, person, key, cursor \\ 0, opts \\ []) do
    request =
      {:read, principal(person, key), Keyword.get(opts, :session), cursor,
       Keyword.get(opts, :limit, 1_000)}

    GenServer.call(relay, {:client_api, FeedStep, request}, :infinity)
  end

  @doc """
  Runs one agent write. `target` is the route's path parameters. Returns the
  route's `{:ok, status, body, replayed?}` or its refusal. A replay answers
  the stored `{:stored, status, encoded}` form, as the route's Relay step does.
  """
  def write(relay, person, key, operation, target, body, idempotency_key \\ nil) do
    # A POST requires an Idempotency-Key; the route uses it as the op_id.
    op_id = idempotency_key || ConversationFiles.new_upload_id()
    digest = :crypto.hash(:sha256, :erlang.term_to_binary({operation, target, body}))
    input = {principal(person, key), operation, target, op_id, digest, [], body}

    GenServer.call(
      relay,
      {:client_api, AgentWrites, {:execute, input, op_id, []}},
      :infinity
    )
  end

  defp read(relay, person, key, request),
    do:
      GenServer.call(
        relay,
        {:client_api, AgentReads, {principal(person, key), request}},
        :infinity
      )

  defp principal(person, key), do: %{kind: :device, person: person, key: key}

  defp engine(relay), do: :sys.get_state(relay).store.engine
end
