defmodule AttentionServer.PushHTTP do
  @moduledoc false

  @response_limit 16_384

  def receive_response(conn, ref, request_timeout) do
    deadline = System.monotonic_time(:millisecond) + request_timeout
    receive_response(conn, ref, nil, [], <<>>, false, deadline)
  end

  defp receive_response(conn, ref, status, headers, body, too_large, deadline) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    case Mint.HTTP.recv(conn, 0, timeout) do
      {:ok, next, events} ->
        {status, headers, body, too_large, done?} =
          Enum.reduce(events, {status, headers, body, too_large, false}, fn event,
                                                                            {s, h, b, large, done} ->
            case event do
              {:status, ^ref, value} ->
                {value, h, b, large, done}

              {:headers, ^ref, value} ->
                {s, value, b, large, done}

              {:data, ^ref, value} when byte_size(b) + byte_size(value) <= @response_limit ->
                {s, h, b <> value, large, done}

              {:data, ^ref, _value} ->
                {s, h, b, true, done}

              {:done, ^ref} ->
                {s, h, b, large, true}

              _ ->
                {s, h, b, large, done}
            end
          end)

        if done? do
          if is_integer(status) and not too_large,
            do: {:ok, next, status, headers, body},
            else: {:error, next, :invalid_response}
        else
          receive_response(next, ref, status, headers, body, too_large, deadline)
        end

      {:error, next, error, _responses} ->
        {:error, next, error}

      _ ->
        {:error, conn, :invalid_response}
    end
  end
end
