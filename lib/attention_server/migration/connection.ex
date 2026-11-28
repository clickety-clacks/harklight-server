defmodule AttentionServer.Migration.Connection do
  @moduledoc false
  alias AttentionServer.Repository.Postgres.Config

  def options(env) do
    mapped = %{
      "ATTENTION_SERVER_STORE_BACKEND" => "postgres",
      "ATTENTION_SERVER_POSTGRES_URL" => env["HARKLIGHT_MIGRATOR_URL"],
      "ATTENTION_SERVER_POSTGRES_PASSWORD_FILE" => env["HARKLIGHT_MIGRATOR_PASSWORD_FILE"],
      "ATTENTION_SERVER_POSTGRES_CA_FILE" => env["HARKLIGHT_MIGRATOR_CA_FILE"]
    }
    with {:ok, {:postgres, options}} <- Config.from_env(mapped),
      do: {:ok, Keyword.delete(options, :pool_size)}
  end

  def with_connection(env, operation) do
    with {:ok, options} <- options(env), do: with_options(options, operation)
  end

  def with_options(options, operation) do
    with {:ok, _} <- Application.ensure_all_started(:postgrex),
         {:ok, db} <- Postgrex.start_link(options) do
      try do
        operation.(db)
      after
        GenServer.stop(db)
      end
    end
  end
end
