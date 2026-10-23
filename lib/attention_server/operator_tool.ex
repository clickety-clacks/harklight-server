defmodule AttentionServer.OperatorTool do
  @moduledoc """
  Local database provisioner. The operator supplies only an owner's public
  certificate; no first visitor or network request can invoke this action.
  """

  alias AttentionServer.Relay

  def bootstrap(store_path, certificate_path),
    do:
      with_relay(store_path, certificate_path, fn relay, der ->
        Relay.operator_bootstrap(relay, der)
      end)

  def recover(store_path, person, certificate_path),
    do:
      with_relay(store_path, certificate_path, fn relay, der ->
        Relay.operator_recover(relay, person, der)
      end)

  defp with_relay(store_path, certificate_path, action) do
    with {:ok, pem} <- File.read(certificate_path),
         {:ok, der} <- certificate_der(pem),
         {:ok, relay} <- Relay.start_link(store_path: store_path) do
      try do
        action.(relay, der)
      after
        GenServer.stop(relay)
      end
    end
  end

  defp certificate_der(pem) do
    try do
      case :public_key.pem_decode(pem) do
        [{:Certificate, der, :not_encrypted}] -> {:ok, der}
        _ -> {:error, :invalid_certificate}
      end
    rescue
      _ -> {:error, :invalid_certificate}
    end
  end
end
