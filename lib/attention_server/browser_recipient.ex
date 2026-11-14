defmodule AttentionServer.BrowserRecipient do
  @moduledoc false

  @recipient_pattern ~r/\Abrowser:([A-Za-z0-9_-]+):([A-Za-z0-9_-]{43}):([0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\z/

  def key(person, session_id, recipient)
      when is_binary(person) and is_binary(session_id) and is_binary(recipient) do
    session_digest =
      :crypto.hash(:sha256, session_id)
      |> Base.url_encode64(padding: false)

    "browser:" <>
      Base.url_encode64(person, padding: false) <>
      ":" <> session_digest <> ":" <> recipient
  end

  def person(key) when is_binary(key) do
    case Regex.run(@recipient_pattern, key) do
      [_, encoded, _session_id, _recipient] -> Base.url_decode64(encoded, padding: false)
      _ -> :error
    end
  end

  def person(_), do: :error

  def session_digest(key) when is_binary(key) do
    case Regex.run(@recipient_pattern, key) do
      [_, _person, session_digest, _recipient] -> {:ok, session_digest}
      _ -> :error
    end
  end

  def session_digest(_), do: :error

  def valid_for?(key, person) when is_binary(person) do
    case person(key) do
      {:ok, ^person} -> true
      _ -> false
    end
  end

  def valid_for?(_, _), do: false
end
