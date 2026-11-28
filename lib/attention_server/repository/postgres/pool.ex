defmodule AttentionServer.Repository.Postgres.Pool do
  @moduledoc false

  alias AttentionServer.Repository.Postgres.{Config, Repo, Transaction}

  def start_from_env(env \\ System.get_env()) do
    case Config.from_env(env) do
      {:ok, {:postgres, opts}} -> start_ready(opts)
      {:ok, :sqlite} -> {:error, :postgres_disabled}
      {:error, _} = error -> error
    end
  end

  def child_spec(config) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [config]}, type: :supervisor}
  end

  # The normal Server supervisor still selects FORMAT32. Isolated backend
  # callers start this pool explicitly after applying the migration.
  def start_link(config) do
    if Application.get_env(:attention_server, :postgres_repository, false) == true do
      start_ready(config)
    else
      {:error, :postgres_disabled}
    end
  end

  defp start_ready(config) do
    opts = config |> Keyword.put_new(:pool_size, 4) |> Keyword.put(:name, Repo)

    with {:ok, pid} <- Repo.start_link(opts) do
      case Transaction.verify_ready() do
        :ok ->
          {:ok, pid}

        {:error, _} = error ->
          Supervisor.stop(pid)
          error
      end
    end
  end
end
