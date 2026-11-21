defmodule AttentionServer.HeadlessTLS do
  @moduledoc """
  Small TLS client for the supplied headless enrollment and owner tools.

  Every connection validates the configured server CA and DNS name. No
  redirect, fallback trust root or plaintext transport is supported.
  """

  alias AttentionServer.JSON

  @max_http 64 * 1024
  # Agent replies carry whole messages; the page size stops at 4 MiB only after
  # its first item, so one large message can exceed any small fixed size.
  @max_agent_http 64 * 1024 * 1024

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

    # No client-side deadline: wait for the server's answer or a real
    # connection end/error from the network stack.
    :ssl.connect(address, port, options, :infinity)
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
        case :ssl.recv(socket, 0, :infinity) do
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

  @doc """
  One mutual-TLS client API v2 `GET` over HTTP/1.1 with the device identity.
  Returns the HTTP status and the decoded JSON body; the status is separate
  from application success.
  """
  def v2_get(endpoint, identity, path) do
    with {:ok, status, _headers, value} <-
           v2_request(endpoint, identity, "GET", path, [{"accept", "application/json"}], nil),
         do: {:ok, status, value}
  end

  @doc """
  One mutual-TLS client API v2 request over HTTP/1.1 with the device identity.
  Returns the HTTP status, the response headers (lowercase names) and the
  decoded JSON body.
  """
  def v2_request(endpoint, {certificate, key}, method, path, headers, body) do
    options = [
      hostname: endpoint.name,
      protocols: [:http1],
      mode: :passive,
      transport_opts: [
        versions: [:"tlsv1.3"],
        cacertfile: endpoint.ca_file,
        certfile: certificate,
        keyfile: key,
        server_name_indication: String.to_charlist(endpoint.name),
        session_tickets: :disabled
      ]
    ]

    with {:ok, conn} <- Mint.HTTP.connect(:https, host(endpoint.address), endpoint.port, options) do
      try do
        with {:ok, conn, ref} <- mint(Mint.HTTP.request(conn, method, path, headers, body)),
             {:ok, status, response_headers, response} <- v2_response(conn, ref, nil, [], []),
             {:ok, value} <- JSON.decode_line(response <> "\n", @max_agent_http + 1) do
          {:ok, status, response_headers, value}
        else
          {:error, :response_too_large} = error -> error
          {:error, %{__exception__: true}} -> {:error, :connection_failed}
          _ -> {:error, :invalid_http_response}
        end
      after
        Mint.HTTP.close(conn)
      end
    else
      _ -> {:error, :connection_failed}
    end
  end

  defp host(address) when is_tuple(address), do: address |> :inet.ntoa() |> List.to_string()
  defp host(address) when is_list(address), do: List.to_string(address)

  defp mint({:error, _conn, reason}), do: {:error, reason}
  defp mint(other), do: other

  # No client-side deadline, as for the other tools.
  defp v2_response(conn, ref, status, headers, body) do
    case Mint.HTTP.recv(conn, 0, :infinity) do
      {:ok, conn, responses} ->
        {status, headers, body, done} =
          Enum.reduce(responses, {status, headers, body, false}, fn
            {:status, ^ref, value}, {_, headers, body, done} ->
              {value, headers, body, done}

            {:headers, ^ref, more}, {status, headers, body, done} ->
              {status, headers ++ more, body, done}

            {:data, ^ref, data}, {status, headers, body, done} ->
              {status, headers, [body | data], done}

            {:done, ^ref}, {status, headers, body, _} ->
              {status, headers, body, true}

            _other, acc ->
              acc
          end)

        cond do
          IO.iodata_length(body) > @max_agent_http -> {:error, :response_too_large}
          done and is_integer(status) -> {:ok, status, headers, IO.iodata_to_binary(body)}
          true -> v2_response(conn, ref, status, headers, body)
        end

      {:error, _conn, reason, _responses} ->
        {:error, reason}
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
        case :ssl.recv(socket, 0, :infinity) do
          {:ok, chunk} -> read_agent_line(socket, bytes <> chunk)
          error -> error
        end
    end
  end

  defp read_http(socket, buffer, max \\ @max_http)

  defp read_http(_socket, buffer, max) when byte_size(buffer) > max,
    do: {:error, :response_too_large}

  defp read_http(socket, buffer, max) do
    case :ssl.recv(socket, 0, :infinity) do
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
