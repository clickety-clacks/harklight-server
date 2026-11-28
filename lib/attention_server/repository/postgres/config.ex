defmodule AttentionServer.Repository.Postgres.Config do
  @moduledoc false

  @loopback ["127.0.0.1", "localhost", "::1"]

  def from_env(env) do
    case Map.get(env, "ATTENTION_SERVER_STORE_BACKEND", "sqlite") do
      "sqlite" -> {:ok, :sqlite}
      "postgres" -> postgres_options(env)
      _ -> {:error, :invalid_store_backend}
    end
  end

  defp postgres_options(env) do
    with {:ok, uri} <- connection_uri(env["ATTENTION_SERVER_POSTGRES_URL"]),
         {:ok, password} <- load_password(env["ATTENTION_SERVER_POSTGRES_PASSWORD_FILE"]),
         {:ok, pool_size} <- pool_size(env["ATTENTION_SERVER_POSTGRES_POOL_SIZE"]),
         {:ok, ssl} <- tls_options(uri, env["ATTENTION_SERVER_POSTGRES_CA_FILE"]) do
      {:ok,
       {:postgres,
        [
          hostname: uri.host,
          database: URI.decode(String.trim_leading(uri.path, "/")),
          username: URI.decode(uri.userinfo),
          password: password,
          port: uri.port || 5432,
          pool_size: pool_size,
          ssl: ssl,
          log: false
        ]}}
    end
  end

  defp connection_uri(url) when is_binary(url) do
    case URI.new(url) do
      {:ok,
       %URI{scheme: scheme, host: host, userinfo: user, path: "/" <> database, fragment: nil} =
           uri}
      when scheme in ["postgres", "postgresql"] and is_binary(host) and host != "" and
             is_binary(user) and user != "" and database != "" ->
        if String.contains?(URI.decode(user), ":") or
             String.contains?(URI.decode(database), "/") or
             (uri.port && uri.port not in 1..65_535) do
          {:error, :invalid_postgres_url}
        else
          {:ok, uri}
        end

      _ ->
        {:error, :invalid_postgres_url}
    end
  end

  defp connection_uri(_), do: {:error, :invalid_postgres_url}

  defp load_password("/" <> _ = path) do
    with {:ok, bytes} <- File.read(path),
         password when password != "" <- String.trim_trailing(bytes, "\n") do
      {:ok, password}
    else
      _ -> {:error, :postgres_password_unavailable}
    end
  end

  defp load_password(_), do: {:error, :postgres_password_unavailable}

  defp pool_size(nil), do: {:ok, 4}

  defp pool_size(value) do
    case Integer.parse(value) do
      {size, ""} when size > 0 -> {:ok, size}
      _ -> {:error, :invalid_postgres_pool_size}
    end
  end

  defp tls_options(uri, ca_file) do
    case URI.query_decoder(uri.query || "") |> Enum.to_list() do
      [] when uri.host in @loopback and is_nil(ca_file) -> {:ok, false}
      [{"sslmode", "disable"}] when uri.host in @loopback and is_nil(ca_file) -> {:ok, false}
      [] -> verified_tls(ca_file)
      [{"sslmode", "verify-full"}] -> verified_tls(ca_file)
      _ -> {:error, :invalid_postgres_tls_options}
    end
  end

  defp verified_tls(ca_file) do
    with {:ok, trust} <- trust_options(ca_file) do
      {:ok,
       trust ++
         [
           verify: :verify_peer,
           customize_hostname_check: [
             match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
           ]
         ]}
    end
  end

  defp trust_options(nil), do: {:ok, [cacerts: :public_key.cacerts_get()]}
  defp trust_options("/" <> _ = path), do: {:ok, [cacertfile: String.to_charlist(path)]}
  defp trust_options(_), do: {:error, :invalid_postgres_ca_file}
end
