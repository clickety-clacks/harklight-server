defmodule AttentionServer.DeviceProof do
  @moduledoc false

  @p256 {1, 2, 840, 10045, 3, 1, 7}
  @ec_public_key {1, 2, 840, 10045, 2, 1}
  @rsa_public_key {1, 2, 840, 113549, 1, 1, 1}

  def sign_in_bytes(reference, session_hash),
    do: "harklight-v2:sign-in\n" <> reference <> "\n" <> session_hash

  def activation_bytes(session_hash),
    do: "harklight-v2:migration-complete\n" <> session_hash

  def digest(value), do: Base.encode16(:crypto.hash(:sha256, value), case: :lower)

  def native_key(public_key) do
    with {:ok, der} <- decode(public_key, 256),
         {:SubjectPublicKeyInfo, {:AlgorithmIdentifier, @ec_public_key, _}, _} <-
           :public_key.der_decode(:SubjectPublicKeyInfo, der),
         {{:ECPoint, <<4, _::binary-size(64)>>}, {:namedCurve, @p256}} = key <-
           :public_key.pem_entry_decode({:SubjectPublicKeyInfo, der, :not_encrypted}) do
      {:ok, key}
    else
      _ -> {:error, :invalid_key}
    end
  rescue
    _ -> {:error, :invalid_key}
  end

  def legacy_key(public_key, id) do
    with {:ok, der} <- decode(public_key, 512),
         ^id <- digest(der),
         {:SubjectPublicKeyInfo, {:AlgorithmIdentifier, @rsa_public_key, _}, _} <-
           :public_key.der_decode(:SubjectPublicKeyInfo, der),
         {:RSAPublicKey, modulus, _} = key <-
           :public_key.pem_entry_decode({:SubjectPublicKeyInfo, der, :not_encrypted}),
         <<1::1, _::bitstring-size(2047)>> <- :binary.encode_unsigned(modulus) do
      {:ok, key}
    else
      _ -> {:error, :invalid_key}
    end
  rescue
    _ -> {:error, :invalid_key}
  end

  def verify(key, bytes, signature) do
    with {:ok, decoded} <- decode(signature, 512) do
      :public_key.verify(bytes, :sha256, decoded, key)
    else
      _ -> false
    end
  rescue
    _ -> false
  end

  def secret?(value) do
    match?({:ok, <<_::binary-size(32)>>}, decode(value, 43))
  end

  def hash?(value) when is_binary(value) and byte_size(value) == 64,
    do: match?({:ok, _}, Base.decode16(value, case: :lower))

  def hash?(_), do: false

  defp decode(value, max) when is_binary(value) and byte_size(value) <= max do
    with {:ok, decoded} <- Base.url_decode64(value, padding: false),
         ^value <- Base.url_encode64(decoded, padding: false) do
      {:ok, decoded}
    else
      _ -> {:error, :invalid_encoding}
    end
  end

  defp decode(_, _), do: {:error, :invalid_encoding}
end
