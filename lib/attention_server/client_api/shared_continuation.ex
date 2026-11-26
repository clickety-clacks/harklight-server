defmodule AttentionServer.ClientAPI.SharedContinuation do
  @moduledoc false

  @version 1
  @lifetime_ms 30 * 24 * 60 * 60 * 1_000

  def issue(key, principal, route, filters, revision, now)
      when is_integer(revision) and revision >= 0 and is_integer(now) and now >= 0 do
    scope = scope(principal, route, filters)
    claims = <<@version, revision::unsigned-64, now::unsigned-64, scope::binary>>
    Base.url_encode64(claims <> signature(key, claims), padding: false)
  end

  def validate(token, key, principal, route, filters, cursor, now)
      when is_binary(token) and byte_size(token) <= 128 and is_integer(cursor) and is_integer(now) do
    with {:ok, <<claims::binary-size(49), provided::binary-size(32)>>} <-
           Base.url_decode64(token, padding: false),
         <<@version, baseline::unsigned-64, issued::unsigned-64, bound_scope::binary-size(32)>> <-
           claims,
         true <- cursor >= baseline and now >= issued and now - issued <= @lifetime_ms,
         true <- bound_scope == scope(principal, route, filters),
         true <- Plug.Crypto.secure_compare(provided, signature(key, claims)) do
      :ok
    else
      _ -> {:error, :cursor_expired}
    end
  end

  def validate(_token, _key, _principal, _route, _filters, _cursor, _now),
    do: {:error, :cursor_expired}

  defp scope(principal, route, filters) do
    identity = Map.take(principal, [:kind, :person, :key, :session_id, :owner_revision])
    canonical = :erlang.term_to_binary({:shared_messages, identity, route, filters}, [:deterministic])
    :crypto.hash(:sha256, canonical)
  end

  defp signature(key, claims),
    do: :crypto.mac(:hmac, :sha256, key, ["harklight-shared-continuation", claims])
end
