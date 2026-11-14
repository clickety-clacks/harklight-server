defmodule AttentionServer.HeadlessEnrollmentNullExpiryTest.Responder do
  @moduledoc false

  alias AttentionServer.JSON

  def init(opts), do: opts

  def call(conn, {owner, responses}) do
    {:ok, request_body, conn} = Plug.Conn.read_body(conn)

    response =
      Agent.get_and_update(responses, fn state ->
        key = if conn.request_path == "/enroll", do: :enroll, else: :status
        [response | rest] = Map.fetch!(state, key)
        {response, Map.put(state, key, rest)}
      end)

    case {conn.request_path, JSON.decode_line(request_body <> "\n")} do
      {"/enroll", {:ok, %{"public_certificate" => certificate}}} ->
        send(owner, {:enrollment_submission, certificate})

      {"/enroll/status", _} ->
        send(owner, :status_lookup)
    end

    {status, payload} = response

    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, JSON.encode!(payload))
  end
end

defmodule AttentionServer.HeadlessEnrollmentNullExpiryTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{HeadlessEnrollment, JSON, TLSFixtures}

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  test "nullable expiry is accepted and a missing pending status is resubmitted with the same key",
       %{
         certs: certs
       } do
    first_code = "ABCDEFG234"
    second_code = "HIJKLMN567"

    responses = %{
      enroll: [
        {200,
         %{
           "approval_code" => first_code,
           "status_secret" => String.duplicate("a", 64),
           "expires_at" => nil,
           "status_expires_at" => nil
         }},
        {200,
         %{
           "approval_code" => second_code,
           "status_secret" => String.duplicate("b", 64),
           "expires_at" => nil,
           "status_expires_at" => nil
         }}
      ],
      status: [{404, %{"error" => "not_found"}}]
    }

    {:ok, response_queue} = Agent.start_link(fn -> responses end)
    port = available_port()

    {:ok, server} =
      Bandit.start_link(
        plug:
          {AttentionServer.HeadlessEnrollmentNullExpiryTest.Responder, {self(), response_queue}},
        scheme: :https,
        ip: {127, 0, 0, 1},
        port: port,
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        startup_log: false
      )

    Process.unlink(server)
    on_exit(fn -> if Process.alive?(server), do: Supervisor.stop(server) end)
    on_exit(fn -> if Process.alive?(response_queue), do: Agent.stop(response_queue) end)

    endpoint = %{
      address: {127, 0, 0, 1},
      name: "relay.test",
      port: port,
      ca_file: Path.join(certs, "ca.pem")
    }

    directory =
      Path.join(
        System.tmp_dir!(),
        "attention-null-expiry-#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(directory) end)

    assert {:ok, %{approval_code: ^first_code, expires_at: nil}} =
             HeadlessEnrollment.start(directory, endpoint, "laptop")

    first_pending = File.read!(Path.join(directory, "pending.json"))

    assert {:ok, %{"expires_at" => nil, "status_expires_at" => nil}} =
             JSON.decode_line(first_pending)

    assert {:ok, %{approval_code: ^second_code, expires_at: nil}} =
             HeadlessEnrollment.submit_existing(directory, endpoint, "laptop")

    assert_receive {:enrollment_submission, certificate}
    assert_receive :status_lookup
    assert_receive {:enrollment_submission, ^certificate}

    assert File.read!(Path.join(directory, "pending-#{first_code}.json")) == first_pending

    assert {:ok, %{"approval_code" => ^second_code, "expires_at" => nil}} =
             directory
             |> Path.join("pending.json")
             |> File.read!()
             |> JSON.decode_line()
  end

  defp available_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_address, port}} = :inet.sockname(socket)
    :ok = :gen_tcp.close(socket)
    port
  end
end
