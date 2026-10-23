defmodule AttentionServer.EnrolledLeafVerify do
  @moduledoc """
  OTP TLS certificate-path callback for an explicitly enrolled self-signed leaf.

  The only overridden path error is `selfsigned_peer`, and it is accepted only
  for a current exact SPKI enrollment of the requested principal kind. All
  other certificate errors (including validity, key usage and CA failures)
  remain fatal. OTP still proves possession through TLS CertificateVerify.
  """

  alias AttentionServer.{DeviceIdentity, Relay}

  def device(relay),
    do: {fn _certificate, der, event, state -> verify(relay, :device, der, event, state) end, nil}

  # Retained exact SPKI admits only the enrollment listener's own-device cleanup.
  # Revoked devices remain forbidden on every relay/owner/agent interface.
  def removal(relay),
    do:
      {fn _certificate, der, event, state -> verify(relay, :removal, der, event, state) end, nil}

  def owner(relay),
    do: {fn _certificate, der, event, state -> verify(relay, :owner, der, event, state) end, nil}

  def agent(relay),
    do: {fn _certificate, der, event, state -> verify(relay, :agent, der, event, state) end, nil}

  defp verify(relay, kind, der, {:bad_cert, :selfsigned_peer}, state) do
    with {:ok, key} <- DeviceIdentity.from_certificate(der),
         true <- active?(relay, kind, key) do
      {:valid, state}
    else
      _ -> {:fail, :unenrolled_selfsigned_leaf}
    end
  end

  defp verify(_relay, _kind, _der, {:bad_cert, reason}, _state), do: {:fail, reason}
  defp verify(_relay, _kind, _der, {:extension, _}, state), do: {:unknown, state}

  defp verify(_relay, _kind, _der, valid, state) when valid in [:valid, :valid_peer],
    do: {:valid, state}

  defp verify(_relay, _kind, _der, _, _state), do: {:fail, :invalid_certificate}

  defp active?(relay, :device, key), do: match?({:ok, _}, Relay.acknowledged_cursor(relay, key))
  defp active?(relay, :removal, key), do: match?({:ok, _}, Relay.removal_identity(relay, key))
  defp active?(relay, :owner, key), do: match?({:ok, _, _}, Relay.owner_identity(relay, key))
  defp active?(relay, :agent, key), do: match?({:ok, _, _, _}, Relay.agent_identity(relay, key))
end
