defmodule AttentionServer.TLSFixtures do
  @moduledoc false

  def create!(opts \\ []) do
    dir =
      Path.join(System.tmp_dir!(), "attention-tls-#{System.unique_integer([:positive])}")

    File.mkdir_p!(Path.join(dir, "newcerts"))
    File.write!(Path.join(dir, "index.txt"), "")
    File.write!(Path.join(dir, "serial"), "01\n")

    run!(dir, [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-sha256",
      "-days",
      "2",
      "-keyout",
      "ca.key",
      "-out",
      "ca.pem",
      "-subj",
      "/CN=Attention Test CA",
      "-addext",
      "basicConstraints=critical,CA:TRUE",
      "-addext",
      "keyUsage=critical,keyCertSign,cRLSign"
    ])

    run!(dir, [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-sha256",
      "-days",
      "2",
      "-keyout",
      "untrusted-ca.key",
      "-out",
      "untrusted-ca.pem",
      "-subj",
      "/CN=Untrusted Test CA",
      "-addext",
      "basicConstraints=critical,CA:TRUE"
    ])

    server_san = Keyword.get(opts, :server_san, "DNS:relay.test")

    File.write!(Path.join(dir, "ca.cnf"), """
    [ ca ]
    default_ca = local_ca
    [ local_ca ]
    dir = #{dir}
    database = $dir/index.txt
    new_certs_dir = $dir/newcerts
    certificate = $dir/ca.pem
    private_key = $dir/ca.key
    serial = $dir/serial
    default_md = sha256
    default_days = 2
    policy = supplied
    unique_subject = no
    [ supplied ]
    commonName = supplied
    [ server_ext ]
    basicConstraints = critical,CA:FALSE
    keyUsage = critical,digitalSignature
    extendedKeyUsage = serverAuth
    subjectAltName = #{server_san}
    [ client_ext ]
    basicConstraints = critical,CA:FALSE
    keyUsage = critical,digitalSignature
    extendedKeyUsage = clientAuth
    """)

    make_cert!(dir, "server", "server_ext")
    Enum.each(~w(a1 a2 b1 unknown expired future), &make_cert!(dir, &1, "client_ext"))
    make_rogue!(dir)
    dir
  end

  def certificate_der!(dir, name) do
    [{:Certificate, der, _}] =
      dir
      |> Path.join("#{name}.pem")
      |> File.read!()
      |> :public_key.pem_decode()

    der
  end

  def make_self_signed_client!(dir, name) do
    run!(dir, [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-sha256",
      "-days",
      "2",
      "-keyout",
      "#{name}.key",
      "-out",
      "#{name}.pem",
      "-subj",
      "/CN=#{name}",
      "-addext",
      "basicConstraints=critical,CA:FALSE",
      "-addext",
      "keyUsage=critical,digitalSignature",
      "-addext",
      "extendedKeyUsage=clientAuth"
    ])
  end

  def make_dated_self_signed_client!(dir, name, from_seconds, until_seconds) do
    run!(dir, [
      "req",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-sha256",
      "-keyout",
      "#{name}.key",
      "-out",
      "#{name}.csr",
      "-subj",
      "/CN=#{name}"
    ])

    now = DateTime.utc_now()

    run!(dir, [
      "ca",
      "-selfsign",
      "-batch",
      "-config",
      "ca.cnf",
      "-extensions",
      "client_ext",
      "-keyfile",
      "#{name}.key",
      "-in",
      "#{name}.csr",
      "-out",
      "#{name}.pem",
      "-notext",
      "-startdate",
      stamp(DateTime.add(now, from_seconds, :second)),
      "-enddate",
      stamp(DateTime.add(now, until_seconds, :second))
    ])
  end

  defp make_cert!(dir, name, extension) do
    run!(dir, [
      "req",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-sha256",
      "-keyout",
      "#{name}.key",
      "-out",
      "#{name}.csr",
      "-subj",
      "/CN=#{name}"
    ])

    now = DateTime.utc_now()

    dates =
      case name do
        "expired" -> validity(now, -172_800, -86_400)
        "future" -> validity(now, 86_400, 172_800)
        _ -> []
      end

    run!(dir, [
      "ca",
      "-batch",
      "-config",
      "ca.cnf",
      "-extensions",
      extension,
      "-in",
      "#{name}.csr",
      "-out",
      "#{name}.pem",
      "-notext"
      | dates
    ])
  end

  defp validity(now, from_seconds, until_seconds) do
    [
      "-startdate",
      stamp(DateTime.add(now, from_seconds, :second)),
      "-enddate",
      stamp(DateTime.add(now, until_seconds, :second))
    ]
  end

  defp make_rogue!(dir) do
    run!(dir, [
      "req",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-sha256",
      "-keyout",
      "rogue.key",
      "-out",
      "rogue.csr",
      "-subj",
      "/CN=rogue"
    ])

    run!(dir, [
      "x509",
      "-req",
      "-in",
      "rogue.csr",
      "-CA",
      "untrusted-ca.pem",
      "-CAkey",
      "untrusted-ca.key",
      "-CAcreateserial",
      "-out",
      "rogue.pem",
      "-days",
      "2",
      "-extfile",
      "ca.cnf",
      "-extensions",
      "client_ext"
    ])
  end

  defp stamp(date_time), do: Calendar.strftime(date_time, "%y%m%d%H%M%SZ")

  defp run!(dir, args) do
    case System.cmd("openssl", args, cd: dir, stderr_to_stdout: true) do
      {_, 0} -> :ok
      {output, code} -> raise "openssl fixture generation failed (#{code}): #{output}"
    end
  end
end
