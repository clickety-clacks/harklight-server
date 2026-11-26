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

  def organization_member(store_path, person, member?) do
    with_relay(store_path, &Relay.operator_organization_member(&1, person, member?))
  end

  defp with_relay(store_path, certificate_path, action) do
    with {:ok, pem} <- File.read(certificate_path),
         {:ok, der} <- certificate_der(pem) do
      with_relay(store_path, &action.(&1, der))
    end
  end

  defp with_relay(store_path, action) do
    with {:ok, relay} <-
           GenServer.start(Relay, store_path: store_path, operator_startup_errors: true) do
      # Link only after the store has opened so a startup failure is returned
      # to the CLI while a running relay keeps the operator's lifecycle.
      Process.link(relay)

      try do
        action.(relay)
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
