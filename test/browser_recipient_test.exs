defmodule AttentionServer.BrowserRecipientTest do
  use ExUnit.Case, async: true

  alias AttentionServer.BrowserRecipient

  @person "account-person"
  @session_id String.duplicate("a", 43)
  @recipient "00000000-0000-4000-8000-000000000001"

  test "a recipient key round-trips only its session person and recipient" do
    key = BrowserRecipient.key(@person, @session_id, @recipient)
    expected_digest = :crypto.hash(:sha256, @session_id) |> Base.url_encode64(padding: false)

    assert {:ok, @person} == BrowserRecipient.person(key)
    {:ok, digest} = BrowserRecipient.session_digest(key)
    assert digest == expected_digest
    refute String.contains?(key, @session_id)
    assert BrowserRecipient.valid_for?(key, @person)
    refute BrowserRecipient.valid_for?(key, "another-person")
    refute key == BrowserRecipient.key(@person, "another-session", @recipient)
  end

  test "malformed recipient keys do not decode as browser principals" do
    refute BrowserRecipient.valid_for?("browser:%%%:not-a-session:not-a-uuid", @person)
    assert :error == BrowserRecipient.person("device-key")
    assert :error == BrowserRecipient.session_digest("browser:person:session")
  end
end
