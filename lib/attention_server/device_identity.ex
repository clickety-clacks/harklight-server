defmodule AttentionServer.DeviceIdentity do
  @moduledoc """
  Device identity is the lowercase SHA-256 of the DER SubjectPublicKeyInfo.

  TLS validates the certificate chain, validity and private-key proof before
  this identity is looked up in current relay enrollment. This module only
  extracts the stable public-key identity; it does not validate certificates.
  """

  @spec from_certificate(binary()) :: {:ok, binary()} | {:error, :invalid_certificate}
  def from_certificate(der) when is_binary(der) do
    try do
      {:Certificate, tbs, _, _} = :public_key.pkix_decode_cert(der, :plain)
      {:TBSCertificate, _, _, _, _, _, _, spki, _, _, _} = tbs
      {:SubjectPublicKeyInfo, _, _} = spki

      id =
        spki
        |> then(&:public_key.der_encode(:SubjectPublicKeyInfo, &1))
        |> then(&:crypto.hash(:sha256, &1))
        |> Base.encode16(case: :lower)

      {:ok, id}
    rescue
      _ -> {:error, :invalid_certificate}
    catch
      _, _ -> {:error, :invalid_certificate}
    end
  end

  def from_certificate(_), do: {:error, :invalid_certificate}
end
