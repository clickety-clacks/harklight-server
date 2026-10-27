defmodule AttentionServer.CLI.Enroll do
  @moduledoc """
  Headless requesting-device enrollment with a locally generated key.

      mix attention.enroll start --directory ./device --connect 127.0.0.1 --name relay.test --port 8443 --ca ca.pem --label 'Laptop'
      mix attention.enroll retry --directory ./device --connect 127.0.0.1 --name relay.test --port 8443 --ca ca.pem --label 'Laptop'
      mix attention.enroll status --directory ./device --connect 127.0.0.1 --name relay.test --port 8443 --relay-port 8445 --ca ca.pem

  The displayed approval code is for the already authenticated owner. The
  status secret and private key remain in the local 0700 directory.
  """

  alias AttentionServer.{HeadlessEnrollment, ToolEndpoint}

  def run([command | args]) when command in ["start", "retry", "status"] do
    {options, rest, invalid} =
      OptionParser.parse(args,
        strict: [
          directory: :string,
          connect: :string,
          name: :string,
          port: :integer,
          relay_port: :integer,
          ca: :string,
          label: :string
        ]
      )

    if rest != [] or invalid != [],
      do: AttentionServer.CLI.error!("unexpected enrollment arguments")

    directory =
      Keyword.get(options, :directory) || AttentionServer.CLI.error!("--directory is required")

    endpoint =
      case ToolEndpoint.from_options(options) do
        {:ok, value} -> value
        _ -> AttentionServer.CLI.error!("--connect, --name, --port and --ca are required")
      end

    AttentionServer.CLI.start_application!()

    if command == "status" and HeadlessEnrollment.certificate_lifecycle(directory) != :ok,
      do:
        AttentionServer.CLI.shell().info(
          "warning: local device certificate expires within 30 days or is invalid"
        )

    result =
      case command do
        "start" ->
          label =
            Keyword.get(options, :label) || AttentionServer.CLI.error!("--label is required")

          HeadlessEnrollment.start(directory, endpoint, label)

        "retry" ->
          label =
            Keyword.get(options, :label) || AttentionServer.CLI.error!("--label is required")

          HeadlessEnrollment.submit_existing(directory, endpoint, label)

        "status" ->
          relay_port =
            Keyword.get(options, :relay_port) ||
              AttentionServer.CLI.error!("--relay-port is required")

          case HeadlessEnrollment.status(directory, endpoint) do
            {:ok, %{"state" => "approved"} = approval} ->
              case HeadlessEnrollment.verify_relay(directory, %{endpoint | port: relay_port}) do
                :ok -> {:ok, approval}
                error -> error
              end

            other ->
              other
          end
      end

    case result do
      {:ok, %{approval_code: code, expires_at: deadline}} ->
        AttentionServer.CLI.shell().info("approval code: #{code}; expires_at: #{deadline}")

      {:ok, %{"state" => "approved"}} when command == "retry" ->
        AttentionServer.CLI.shell().info(
          "historical approval observed; run status with --relay-port to prove current key access before treating enrollment as complete"
        )

      {:ok, %{"state" => "approved", "person_id" => person, "device_id" => device}} ->
        AttentionServer.CLI.shell().info("approved person=#{person} device=#{device}")

      {:ok, %{"state" => state}} ->
        AttentionServer.CLI.shell().info(state)

      {:error, reason} ->
        case reason do
          :indeterminate_submission ->
            AttentionServer.CLI.error!(
              "submission indeterminate; keep this directory and retry the same key"
            )

          :key_pending ->
            AttentionServer.CLI.error!(
              "key_pending: no old code or secret is recoverable; keep this key, wait ten minutes after this response, then explicitly retry; this is not enrollment success"
            )

          :key_in_use ->
            AttentionServer.CLI.error!(
              "key_in_use: keep this key; authenticated owner result reconciliation is not supported by this tool, and no account result can be inferred"
            )

          :not_found ->
            AttentionServer.CLI.error!(
              "status handle is unknown or past its readback deadline; keep this key, and use authenticated owner reconciliation when available; this tool cannot infer an account result"
            )

          :capacity ->
            AttentionServer.CLI.error!(
              "capacity: no request was created; keep this key and retry later"
            )

          _ ->
            AttentionServer.CLI.error!("enrollment failed: #{inspect(reason)}")
        end
    end
  end

  def run(_),
    do: AttentionServer.CLI.error!("use start, retry or status; see mix help attention.enroll")
end
