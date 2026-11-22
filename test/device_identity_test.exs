defmodule AttentionServer.DeviceIdentityTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{DeviceIdentity, TLSFixtures}

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  test "the device key is the SHA-256 of the SPKI DER that OpenSSL extracts", %{certs: certs} do
    assert {:ok, key} = DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "a1"))

    {public_pem, 0} =
      System.cmd("openssl", ["x509", "-in", Path.join(certs, "a1.pem"), "-pubkey", "-noout"])

    public_path = Path.join(certs, "a1-public.pem")
    File.write!(public_path, public_pem)

    {spki_der, 0} =
      System.cmd("openssl", ["pkey", "-pubin", "-in", public_path, "-outform", "DER"])

    assert key == Base.encode16(:crypto.hash(:sha256, spki_der), case: :lower)

    assert {:ok, other} =
             DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "b1"))

    refute other == key
  end
end
