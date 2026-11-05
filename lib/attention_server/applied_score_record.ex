defmodule AttentionServer.AppliedScoreRecord do
  @moduledoc false
  use GenServer
  require Logger

  alias AttentionServer.JSON

  # File IO must not hold up the relay or propagate failures into delivery.
  # The unlinked writer owns no lifetime beyond its relay.
  def start(:self_hosted, path) when is_binary(path) and path != "" do
    {:ok, pid} = GenServer.start(__MODULE__, {self(), path})
    pid
  end

  def start(_mode, _path), do: nil

  def record(nil, _rows), do: :ok
  def record(pid, rows), do: GenServer.cast(pid, {:record, rows})

  @impl true
  def init({owner, path}) do
    {:ok, %{owner: Process.monitor(owner), path: path, warned: false}}
  end

  @impl true
  def handle_cast({:record, rows}, state) do
    case append(state.path, rows) do
      :ok ->
        {:noreply, state}

      :error ->
        unless state.warned do
          Logger.warning("event=harklight_applied_score_record outcome=write_failed")
        end

        {:noreply, %{state | warned: true}}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{owner: ref} = state),
    do: {:stop, :normal, state}

  defp append(_path, []), do: :ok

  defp append(path, rows) do
    bytes = Enum.map(rows, &[JSON.encode!(&1), "\n"])

    case :file.open(path, [:raw, :append, :binary]) do
      {:ok, file} ->
        result =
          with :ok <- File.chmod(path, 0o600),
               :ok <- :file.write(file, bytes),
               do: :ok

        closed = :file.close(file)
        if result == :ok and closed == :ok, do: :ok, else: :error

      {:error, _reason} ->
        :error
    end
  rescue
    _ -> :error
  end
end
