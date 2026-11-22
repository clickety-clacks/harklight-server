defmodule AttentionServer.SessionWriter do
  @moduledoc false

  # One active writer per socket, sequenced by Relay. The watcher can cancel a
  # blocked SSL send when either its core or connection worker disappears.
  def start(relay, owner, socket, token, frames) do
    spawn_monitor(fn ->
      writer = self()
      watcher = spawn(fn -> watch(relay, owner, writer, socket) end)

      receive do
        {:watching, ^watcher} -> :ok
      end

      result =
        Enum.reduce_while(frames, :ok, fn frame, _ ->
          with {:ok, output} <-
                 GenServer.call(relay, {:socket_write_frame, socket, token, frame}, :infinity),
               :ok <- write(socket, output) do
            {:cont, :ok}
          else
            error -> {:halt, error}
          end
        end)

      send(watcher, {:finished, writer, result})
      send(relay, {:socket_write_done, socket, token, result})
    end)
  end

  defp write(socket, {:websocket, %AttentionServer.JSON.StreamedLine{} = line}),
    do: AttentionServer.WebSocketRelay.send_text_stream(socket, line)

  defp write(socket, {_transport, %AttentionServer.JSON.StreamedLine{} = line}),
    do: AttentionServer.JSON.send_stream(socket, line)

  defp write(socket, {:websocket, bytes}),
    do: :ssl.send(socket, AttentionServer.WebSocketRelay.encode_text(bytes))

  defp write(socket, {_transport, bytes}), do: :ssl.send(socket, bytes)

  def close(socket) do
    # A TLS close alert can wait behind the blocked sender. Read shutdown
    # generates transport closure first, allowing cancellation without waiting
    # for the peer to read. Connection adapters use the same teardown so their
    # cleanup cannot enter a blocking TLS close ahead of this operation.
    _ = :ssl.shutdown(socket, :read)
    :ssl.close(socket)
  end

  defp watch(relay, owner, writer, socket) do
    core_ref = Process.monitor(relay)
    owner_ref = Process.monitor(owner)
    writer_ref = Process.monitor(writer)
    send(writer, {:watching, self()})

    receive do
      {:finished, ^writer, :ok} ->
        :ok

      {:finished, ^writer, _error} ->
        close(socket)

      {:DOWN, ref, :process, _, _} when ref in [core_ref, owner_ref, writer_ref] ->
        Process.exit(writer, :kill)
        close(socket)
    end
  end
end
