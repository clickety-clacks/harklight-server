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
          not is_nil(Map.get(item, :consumed_at)) or
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
      if is_nil(Map.get(item, :consumed_at)) do
        next = Engine.delete_row(engine, :browser_sign_ins, hash)

        principal = %{
          person_id: item.person,
          owner_key: item.owner_key,
          owner_revision: item.owner_revision,
          scope: :person_owner
        }

        {:ok, next, principal}
      else
        {:error, :used}
      end
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

  # Records keep the stored shape; older ones carry a deadline that is no longer
  # enforced, current ones carry nil.
  @doc false
  def valid_record?(engine, hash, record) when is_map(record) do
    person = Map.get(record, :person)
    owner_key = Map.get(record, :owner_key)
    owner_revision = Map.get(record, :owner_revision)
    issued_at = Map.get(record, :issued_at)
    expires_at = Map.get(record, :expires_at)
    retain_until = Map.get(record, :retain_until)
    consumed_at = Map.get(record, :consumed_at)

    current? = map_size(record) == 5

    legacy? =
      map_size(record) == 7 and is_integer(retain_until) and is_integer(expires_at) and
        retain_until > expires_at and
        (is_nil(consumed_at) or (is_integer(consumed_at) and consumed_at >= issued_at))

    hash?(hash) and Map.has_key?(engine.persons, person) and key?(owner_key) and
      is_integer(owner_revision) and owner_revision > 0 and is_integer(issued_at) and
      issued_at >= 0 and (is_nil(expires_at) or is_integer(expires_at)) and
      (current? or legacy?)
  end

  def valid_record?(_engine, _hash, _record), do: false

  defp hash?(hash), do: is_binary(hash) and byte_size(hash) == 32
  defp key?(key), do: is_binary(key) and Regex.match?(~r/\A[0-9a-f]{64}\z/, key)
end
