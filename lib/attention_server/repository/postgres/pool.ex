defmodule AttentionServer.Repository.Postgres.Pool do
  @moduledoc false

  alias AttentionServer.Repository.Postgres.{Repo, Transaction}

  def child_spec(config) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [config]}, type: :supervisor}
  end

  # The normal Server supervisor still selects FORMAT32. Isolated backend
  # callers start this pool explicitly after applying the migration.
  def start_link(config) do
    if Application.get_env(:attention_server, :postgres_repository, false) == true do
      with {:ok, pid} <- Repo.start_link(config |> Keyword.put_new(:pool_size, 4) |> Keyword.put(:name, Repo)) do
        case Transaction.verify_ready() do
          :ok ->
            {:ok, pid}

          {:error, _} = error ->
            Supervisor.stop(pid)
            error
        end
      end
    else
      {:error, :postgres_disabled}
    end
  end
end
