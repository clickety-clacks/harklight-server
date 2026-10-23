defmodule AttentionServer.HeadlessTLS do
  @moduledoc """
  Small TLS client for the supplied headless enrollment and owner tools.

  Every connection validates the configured server CA and DNS name. No
  redirect, fallback trust root or plaintext transport is supported.
  """

  alias AttentionServer.JSON

  @timeout 3_000
  @max_http 64 * 1024
  @max_agent_http 6 * 1024 * 1024

  def certificate_lifecycle(certificate) do
    case System.cmd("openssl", ["x509", "-checkend", "2592000", "-noout", "-in", certificate],
           stderr_to_stdout: true
         ) do
      {_, 0} -> :ok
      _ -> {:warning, :certificate_expiring_or_invalid}
    end
  rescue
    _ -> {:warning, :certificate_expiring_or_invalid}
  end

  def connect(endpoint, identity \\ nil)

  def connect(%{address: address, name: name, ca_file: ca_file, port: port}, identity)
      when is_binary(name) and is_binary(ca_file) and is_integer(port) and port in 1..65_535 do
    options = [
      versions: [:"tlsv1.3"],
      verify: :verify_peer,
      cacertfile: String.to_charlist(ca_file),
      server_name_indication: String.to_charlist(name),
      session_tickets: :disabled,
      active: false,
      mode: :binary,
      packet: :raw
    ]

    options =
      case identity do
        {certificate, key} ->
          options ++ [certfile: String.to_charlist(certificate), keyfile: String.to_charlist(key)]

        nil ->
          options
      end

    :ssl.connect(address, port, options, @timeout)
  end

  def connect(_, _), do: {:error, :invalid_endpoint}

  def post_json(endpoint, path, request) when path in ["/enroll", "/enroll/status"] do
    with {:ok, socket} <- connect(endpoint) do
      try do
        body = JSON.encode!(request)

        with :ok <-
               :ssl.send(socket, [
                 "POST #{path} HTTP/1.1\r\n",
                 "Host: #{endpoint.name}\r\n",
                 "Content-Type: application/json\r\n",
                 "Content-Length: #{byte_size(body)}\r\n",
                 "Connection: close\r\n\r\n",
                 body
               ]),
             {:ok, response} <- read_http(socket, <<>>),
             {:ok, status, value} <- decode_http(response) do
          {:ok, status, value}
        end
      after
        :ssl.close(socket)
      end
    end
  end

  def owner_request(endpoint, certificate, key, request) do
    with {:ok, socket} <- connect(endpoint, {certificate, key}) do
      try do
        with :ok <- :ssl.send(socket, JSON.encode!(request) <> "\n"),
             {:ok, line} <- read_owner_line(socket, <<>>),
             {:ok, response} <- JSON.decode_line(line) do
          {:ok, response}
        end
      after
        :ssl.close(socket)
      end
    end
  end

  defp read_owner_line(_socket, bytes) when byte_size(bytes) > 64 * 1024,
    do: {:error, :response_too_large}

  defp read_owner_line(socket, bytes) do
    case :binary.match(bytes, "\n") do
      {index, 1} when index == byte_size(bytes) - 1 ->
        {:ok, bytes}

      {_, 1} ->
        {:error, :invalid_response}

      :nomatch ->
        case :ssl.recv(socket, 0, @timeout) do
          {:ok, chunk} -> read_owner_line(socket, bytes <> chunk)
          error -> error
        end
    end
  end

  @doc "One authenticated agent request with a bounded JSON response."
  def agent_request(endpoint, certificate, key, request) do
    with {:ok, socket} <- connect(endpoint, {certificate, key}) do
      try do
        with :ok <- :ssl.send(socket, JSON.encode!(request) <> "\n"),
             {:ok, line} <- read_agent_line(socket, <<>>),
             {:ok, response} <- JSON.decode_line(line, 6 * 1024 * 1024) do
          {:ok, response}
        end
      after
        :ssl.close(socket)
      end
    end
  end

  @doc "One mutual-TLS G6 HTTPS request. The HTTP code is separate from application success."
  def agent_https_request(endpoint, certificate, key, request) do
    body = JSON.encode!(request)

    if byte_size(body) > 2 * 1024 * 1024 do
      {:error, :request_too_large}
    else
      with {:ok, socket} <- connect(endpoint, {certificate, key}) do
        try do
          with :ok <-
                 :ssl.send(socket, [
                   "POST /agent/v1 HTTP/1.1\r\n",
                   "Host: #{endpoint.name}\r\n",
                   "Content-Type: application/json; charset=utf-8\r\n",
                   "Content-Length: #{byte_size(body)}\r\n",
                   "Connection: close\r\n\r\n",
                   body
                 ]),
               {:ok, response} <- read_http(socket, <<>>, @max_agent_http),
               {:ok, status, value} <- decode_http(response, @max_agent_http) do
            {:ok, status, value}
          end
        after
          :ssl.close(socket)
        end
      end
    end
  end

  defp read_agent_line(_socket, bytes) when byte_size(bytes) > 6 * 1024 * 1024,
    do: {:error, :response_too_large}

  defp read_agent_line(socket, bytes) do
    case :binary.match(bytes, "\n") do
      {index, 1} when index == byte_size(bytes) - 1 ->
        {:ok, bytes}

      {_, 1} ->
        {:error, :invalid_response}

      :nomatch ->
        case :ssl.recv(socket, 0, @timeout) do
          {:ok, chunk} -> read_agent_line(socket, bytes <> chunk)
          error -> error
        end
    end
  end

  defp read_http(socket, buffer, max \\ @max_http)

  defp read_http(_socket, buffer, max) when byte_size(buffer) > max,
    do: {:error, :response_too_large}

  defp read_http(socket, buffer, max) do
    case :ssl.recv(socket, 0, @timeout) do
      {:ok, chunk} -> read_http(socket, buffer <> chunk, max)
      {:error, :closed} -> {:ok, buffer}
      error -> error
    end
  end

  defp decode_http(response, max \\ @max_http) do
    with [header, body] <- :binary.split(response, "\r\n\r\n"),
         [status_line | fields] <- String.split(header, "\r\n"),
         ["HTTP/1.1", status_string | _] <- String.split(status_line, " "),
         {status, ""} <- Integer.parse(status_string),
         true <- status in [200, 400, 404, 405, 409, 503],
         [length] <-
           for(
             field <- fields,
             String.starts_with?(String.downcase(field), "content-length:"),
             do: field
           ),
         [_, length_string] <- :binary.split(length, ":"),
         {body_length, ""} <- Integer.parse(String.trim(length_string)),
         true <- body_length == byte_size(body),
         true <- body_length <= max,
         {:ok, value} <- JSON.decode_line(body <> "\n", max + 1) do
      {:ok, status, value}
    else
      _ -> {:error, :invalid_http_response}
    end
  end
end
