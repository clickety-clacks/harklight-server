defmodule AttentionServer.BrowserSignIn do
  @moduledoc """
  Owner-issued, person-bound browser credentials, separate from device invites.

  Only purpose-separated token hashes enter durable state. Consuming a token
  returns its stored current person-owner identity once; browser session storage,
  HTTP routes and account-wide authority remain the caller's responsibility.
  """

  alias AttentionServer.Engine

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
         true <- is_integer(now) and now >= 0 do
      tokens =
        Map.reject(engine.browser_sign_ins, fn {_, item} ->
          Engine.owner_revision(engine, item.person, item.owner_key) !=
            {:ok, item.owner_revision}
        end)

      cond do
        Map.has_key?(tokens, hash) ->
          {:error, :collision}

        true ->
          record = %{
            person: person,
            owner_key: owner_key,
            owner_revision: revision,
            issued_at: now,
            expires_at: nil
          }

          next =
            engine
            |> Engine.replace_rows(:browser_sign_ins, tokens)
            |> Engine.put_row(:browser_sign_ins, hash, record)

          {:ok, next, record}
      end
    else
      {:error, :forbidden} -> {:error, :forbidden}
      _ -> {:error, :invalid_request}
    end
  end

  def consume(engine, hash) do
    with item when is_map(item) <- Map.get(engine.browser_sign_ins, hash),
         {:ok, revision} <- Engine.owner_revision(engine, item.person, item.owner_key),
         true <- revision == item.owner_revision do
      next = Engine.delete_row(engine, :browser_sign_ins, hash)

      principal = %{
        person_id: item.person,
        owner_key: item.owner_key,
        owner_revision: item.owner_revision,
        scope: :person_owner
      }

      {:ok, next, principal}
    else
      _ -> {:error, :invalid_sign_in}
    end
  end

  def valid_state?(engine) do
    floor = engine.browser_sign_in_clock_high_water

    is_integer(floor) and floor >= 0 and is_map(engine.browser_sign_ins) and
      Enum.all?(engine.browser_sign_ins, fn {hash, record} ->
        valid_record?(engine, hash, record)
      end)
  end

  # FORMAT30 snapshots may still contain records from before consumption
  # deleted the token. Keep usable links and remove spent tokens.
  @doc false
  def normalize_snapshot_records(engine) when is_map(engine.browser_sign_ins) do
    tokens =
      Enum.reduce(engine.browser_sign_ins, %{}, fn
        {hash, %{retain_until: retain, expires_at: expires, consumed_at: consumed} = record}, acc
        when map_size(record) == 7 and is_integer(retain) and is_integer(expires) and
               retain > expires ->
          current = Map.drop(record, [:retain_until, :consumed_at])

          cond do
            not valid_record?(engine, hash, current) ->
              Map.put(acc, hash, record)

            is_nil(consumed) ->
              Map.put(acc, hash, current)

            is_integer(consumed) and consumed >= current.issued_at ->
              acc

            true ->
              Map.put(acc, hash, record)
          end

        {hash, record}, acc ->
          Map.put(acc, hash, record)
      end)

    %{engine | browser_sign_ins: tokens}
  end

  def normalize_snapshot_records(engine), do: engine

  @doc false
  def valid_record?(engine, hash, %{
        person: person,
        owner_key: owner_key,
        owner_revision: owner_revision,
        issued_at: issued_at,
        expires_at: expires_at
      } = record)
      when map_size(record) == 5 do
    hash?(hash) and Map.has_key?(engine.persons, person) and key?(owner_key) and
      is_integer(owner_revision) and owner_revision > 0 and is_integer(issued_at) and
      issued_at >= 0 and (is_nil(expires_at) or is_integer(expires_at))
  end

  def valid_record?(_engine, _hash, _record), do: false

  defp hash?(hash), do: is_binary(hash) and byte_size(hash) == 32
  defp key?(key), do: is_binary(key) and Regex.match?(~r/\A[0-9a-f]{64}\z/, key)
end
