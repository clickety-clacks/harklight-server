defmodule AttentionServer.ClientAPI.Errors do
  @moduledoc """
  The client API v2 error envelope and its closed token table.

  An error body is `{error, field?, retryable, request_id}` (docs/client-api-v2.md
  §7). Tokens, statuses and `retryable` come only from the table below. Bodies
  never carry free-form text, message content, tokens or identities. Every `503`
  carries `Retry-After`.

  Handlers answer errors with `send_error/3` and successes with `send_json/3`.
  `token_for_reason/1` maps today's engine reasons to tokens.
  """

  import Plug.Conn

  alias AttentionServer.JSON

  # token => {status, retryable}
  @tokens %{
    "invalid_field" => {400, false},
    "unsupported" => {400, false},
    "unauthorized" => {401, false},
    "forbidden" => {403, false},
    "account_paused" => {403, false},
    "not_found" => {404, false},
    "stale_request" => {409, false},
    "already_answered" => {409, false},
    "already_retracted" => {409, false},
    "already_closed" => {409, false},
    "not_answered" => {409, false},
    "idempotency_conflict" => {409, false},
    "cursor_expired" => {410, false},
    "precondition_failed" => {412, false},
    "payload_too_large" => {413, false},
    "retry_later" => {503, true},
    "storage_unavailable" => {503, true}
  }

  # Today's engine reasons, as listed in docs/client-api-v2.md §7.
  @reasons %{
    invalid_field: "invalid_field",
    bad_frame: "invalid_field",
    forbidden: "forbidden",
    not_person: "forbidden",
    not_origin: "forbidden",
    unknown_message: "not_found",
    not_found: "not_found",
    stale_revision: "stale_request",
    closed: "already_answered",
    retracted: "already_retracted",
    operation_conflict: "idempotency_conflict",
    stale_push_binding: "precondition_failed",
    inactive_account: "account_paused",
    too_large: "payload_too_large",
    upload_cap_exceeded: "payload_too_large",
    storage_unavailable: "storage_unavailable"
  }

  @default_retry_after_s 1

  @type token :: String.t()

  @doc "Every token in the closed list."
  @spec tokens() :: [token()]
  def tokens, do: Map.keys(@tokens)

  @doc "The HTTP status for a token."
  @spec status(token()) :: pos_integer()
  def status(token), do: @tokens |> Map.fetch!(token) |> elem(0)

  @doc "Whether a token is retryable (spec §6.1)."
  @spec retryable?(token()) :: boolean()
  def retryable?(token), do: @tokens |> Map.fetch!(token) |> elem(1)

  @doc """
  The token for one of today's engine reasons, or `:error` when the reason has
  no client-facing token. Callers decide what an unmapped reason means.
  """
  @spec token_for_reason(atom()) :: {:ok, token()} | :error
  def token_for_reason(reason), do: Map.fetch(@reasons, reason)

  @doc """
  Send the error envelope for `token`.

  Options: `field` (the offending member name, a string or an atom) and `retry_after` (seconds, used
  only on `503`; defaults to #{@default_retry_after_s}).
  """
  @spec send_error(Plug.Conn.t(), token(), keyword()) :: Plug.Conn.t()
  def send_error(conn, token, opts \\ []) do
    {status, retryable} = Map.fetch!(@tokens, token)

    body =
      %{"error" => token, "retryable" => retryable, "request_id" => request_id(conn)}
      |> maybe_put("field", Keyword.get(opts, :field))

    conn =
      if status == 503 do
        retry_after = Keyword.get(opts, :retry_after, @default_retry_after_s)
        put_resp_header(conn, "retry-after", Integer.to_string(retry_after))
      else
        conn
      end

    conn
    |> assign(:client_api_error, token)
    |> send_json(status, body)
  end

  @doc "Send a JSON body with `status`."
  @spec send_json(Plug.Conn.t(), pos_integer(), map()) :: Plug.Conn.t()
  def send_json(conn, status, body) when is_map(body) do
    conn
    |> put_resp_content_type("application/json", "utf-8")
    |> send_resp(status, JSON.encode!(body))
  end

  @doc "The request's `Request-Id`, set by the router before any handler runs."
  @spec request_id(Plug.Conn.t()) :: String.t() | nil
  def request_id(conn), do: conn.assigns[:client_api_request_id]

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value) when is_binary(value), do: Map.put(map, key, value)

  defp maybe_put(map, key, value) when is_atom(value),
    do: Map.put(map, key, Atom.to_string(value))
end
