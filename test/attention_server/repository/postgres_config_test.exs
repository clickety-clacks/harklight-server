defmodule AttentionServer.Repository.PostgresConfigTest do
  use ExUnit.Case, async: true

  alias AttentionServer.Repository.Postgres.Config

  test "the default backend stays SQLite and ignores PostgreSQL settings" do
    assert {:ok, :sqlite} = Config.from_env(%{"ATTENTION_SERVER_POSTGRES_URL" => "invalid"})

    assert {:error, :invalid_store_backend} =
             Config.from_env(%{"ATTENTION_SERVER_STORE_BACKEND" => "other"})
  end

  @tag :tmp_dir
  test "connection options come from the installer URL and password file", %{tmp_dir: dir} do
    password_path = Path.join(dir, "password")
    File.write!(password_path, "synthetic password\n")

    env = %{
      "ATTENTION_SERVER_STORE_BACKEND" => "postgres",
      "ATTENTION_SERVER_POSTGRES_URL" =>
        "postgres://fixture@127.0.0.1:55443/harklight?sslmode=disable",
      "ATTENTION_SERVER_POSTGRES_PASSWORD_FILE" => password_path,
      "ATTENTION_SERVER_POSTGRES_POOL_SIZE" => "6"
    }

    assert {:ok, {:postgres, opts}} = Config.from_env(env)
    assert opts[:username] == "fixture"
    assert opts[:hostname] == "127.0.0.1"
    assert opts[:port] == 55_443
    assert opts[:database] == "harklight"
    assert opts[:password] == "synthetic password"
    assert opts[:pool_size] == 6
    assert opts[:ssl] == false

    assert {:error, :invalid_postgres_url} =
             Config.from_env(%{
               env
               | "ATTENTION_SERVER_POSTGRES_URL" => "postgres://fixture:pw@127.0.0.1/harklight"
             })

    assert {:error, :invalid_postgres_tls_options} =
             Config.from_env(%{
               env
               | "ATTENTION_SERVER_POSTGRES_URL" =>
                   "postgres://fixture@db.example.test/harklight?sslmode=disable"
             })

    assert {:ok, {:postgres, remote_opts}} =
             env
             |> Map.put(
               "ATTENTION_SERVER_POSTGRES_URL",
               "postgres://fixture@db.example.test/harklight?sslmode=verify-full"
             )
             |> Map.put("ATTENTION_SERVER_POSTGRES_CA_FILE", "/fixture/ca.pem")
             |> Config.from_env()

    assert remote_opts[:ssl][:verify] == :verify_peer
    assert remote_opts[:ssl][:cacertfile] == ~c"/fixture/ca.pem"
    assert remote_opts[:port] == 5432
  end
end
