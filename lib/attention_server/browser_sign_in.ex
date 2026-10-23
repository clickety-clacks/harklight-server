defmodule AttentionServer.BrowserSignIn do
  @moduledoc """
  Owner-issued, person-bound browser credentials, separate from device invites.

  Only purpose-separated token hashes enter durable state. Consuming a token
  returns its stored current person-owner identity once; browser session storage,
  HTTP routes and account-wide authority remain the caller's responsibility.
  """

  alias AttentionServer.Engine

  @lifetime_ms 10 * 60 * 1_000
  @retention_ms 24 * 60 * 60 * 1_000
  @limit 1_024

  def token_hash("hbs1_" <> hex) when byte_size(hex) == 64 do
    with {:ok, secret} <- Base.decode16(hex, case: :lower),
         true <- byte_size(secret) == 32 do
      {:ok, :crypto.hash(:sha256, "harklight-browser-sign-in-v1\0" <> secret)}
    else
      _ -> {:error, :invalid_sign_in}
    end
  end

  def token_hash(_), do: {:error, :invalid_sign_in}

  def issue(engine, person, owner_key, hash, now) do
    with {:ok, revision} <- Engine.owner_revision(engine, person, owner_key),
         true <- hash?(hash),
         true <- is_integer(now) and now >= engine.browser_sign_in_clock_high_water do
      tokens =
        Map.reject(engine.browser_sign_ins, fn {_, item} ->
          not is_nil(item.consumed_at) or item.expires_at <= now or item.retain_until <= now or
            Engine.owner_revision(engine, item.person, item.owner_key) !=
              {:ok, item.owner_revision}
        end)

      cond do
        Map.has_key?(tokens, hash) ->
          {:error, :collision}

        map_size(tokens) >= @limit ->
          {:error, :capacity}

        true ->
          record = %{
            person: person,
            owner_key: owner_key,
            owner_revision: revision,
            issued_at: now,
            expires_at: now + @lifetime_ms,
            retain_until: now + @lifetime_ms + @retention_ms,
            consumed_at: nil
          }

          {:ok,
           %{
             engine
             | browser_sign_ins: Map.put(tokens, hash, record),
               browser_sign_in_clock_high_water: now
           }, record}
      end
    else
      {:error, :forbidden} -> {:error, :forbidden}
      _ -> {:error, :clock_unavailable}
    end
  end

  # Commit this fence even when expiry refuses consumption, so clock rollback
  # cannot make a previously observed expired token valid again.
  def observe_time(engine, hash, now) when is_integer(now) do
    case Map.get(engine.browser_sign_ins, hash) do
      %{expires_at: deadline} when now >= deadline ->
        %{
          engine
          | browser_sign_in_clock_high_water:
              max(engine.browser_sign_in_clock_high_water, deadline)
        }

      _ ->
        engine
    end
  end

  def consume(engine, hash, now) when is_integer(now) and now >= 0 do
    now = max(now, engine.browser_sign_in_clock_high_water)

    with item when is_map(item) <- Map.get(engine.browser_sign_ins, hash),
         {:ok, revision} <- Engine.owner_revision(engine, item.person, item.owner_key),
         true <- revision == item.owner_revision do
      cond do
        not is_nil(item.consumed_at) ->
          {:error, :used}

        now >= item.expires_at ->
          {:error, :expired}

        true ->
          next = %{
            engine
            | browser_sign_ins:
                Map.put(engine.browser_sign_ins, hash, %{item | consumed_at: now}),
              browser_sign_in_clock_high_water: now
          }

          principal = %{
            person_id: item.person,
            owner_key: item.owner_key,
            owner_revision: item.owner_revision,
            scope: :person_owner
          }

          {:ok, next, principal}
      end
    else
      _ -> {:error, :invalid_sign_in}
    end
  end

  def consume(_, _, _), do: {:error, :invalid_sign_in}

  def valid_state?(engine) do
    floor = engine.browser_sign_in_clock_high_water

    is_integer(floor) and floor >= 0 and is_map(engine.browser_sign_ins) and
      map_size(engine.browser_sign_ins) <= @limit and
      Enum.all?(engine.browser_sign_ins, fn {hash, record} ->
        hash?(hash) and map_size(record) == 7 and Map.has_key?(engine.persons, record.person) and
          key?(record.owner_key) and is_integer(record.owner_revision) and
          record.owner_revision > 0 and
          is_integer(record.issued_at) and record.issued_at >= 0 and
          record.expires_at == record.issued_at + @lifetime_ms and
          record.retain_until == record.expires_at + @retention_ms and
          record.issued_at <= floor and
          (is_nil(record.consumed_at) or
             (is_integer(record.consumed_at) and record.consumed_at >= record.issued_at and
                record.consumed_at < record.expires_at and record.consumed_at <= floor))
      end)
  end

  defp hash?(hash), do: is_binary(hash) and byte_size(hash) == 32
  defp key?(key), do: is_binary(key) and Regex.match?(~r/\A[0-9a-f]{64}\z/, key)
end
