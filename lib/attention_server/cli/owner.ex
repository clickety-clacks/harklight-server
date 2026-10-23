defmodule AttentionServer.CLI.Owner do
  @moduledoc """
  Headless owner approval over a current owner-client-certificate TLS channel.

      mix attention.owner approve --connect 127.0.0.1 --name relay.test --port 8444 --ca ca.pem --cert owner.pem --key owner.key --person UUID --code CODE

  The command displays the stored label with unsafe bytes visibly escaped and the device-key fingerprint and
  requires typing `APPROVE` before a one-use approval. Optional `--senders`
  accepts comma-separated exact sender IDs; absent means no sender grant.
  With the same `--connect`, `--name`, `--port`, `--ca`, `--cert`, `--key` and
  `--person` options, additional commands are:

  * `revoke-device --device SPKI` (requires typing `REVOKE`)
  * `invite --output /private/directory/invitation.json` (30-minute configured-trust invitation)
  * `browser-sign-in --output /private/directory/sign-in-link.txt` (10-minute one-use host-page link)
  * `decline --code CODE` (requires typing `DECLINE` for the stored exact request)
  * `grant-sender --device SPKI --sender ID`
  * `revoke-sender --device SPKI --sender ID`
  * `grant-read --connector SPKI --grant UUID --source ID --target-kind device|agent --target-id ID --target-key SPKI`
  * `revoke-read --grant UUID`
  * `grant-maintainer --grant UUID --grantee PERSON --scopes devices,senders`
  * `revoke-maintainer --grant UUID`
  * `agent-register --agent-id UUID --agent-cert ./agent/identity.pem`
  * `agent-list`
  * `agent-grants-replace --agent-key SPKI --revision N --read app:one,app:prefix/ --respond app:one`
  * `agent-revoke --agent-key SPKI`

  Maintainer grants are durable records only. All scope exercise is `not_supported`.
  """

  alias AttentionServer.{HeadlessAgent, HeadlessOwner, HeadlessTLS, ToolEndpoint}

  def run([command | args]) when command in ["invite", "browser-sign-in", "decline"] do
    {options, rest, invalid} =
      OptionParser.parse(args,
        strict: [
          connect: :string,
          name: :string,
          port: :integer,
          ca: :string,
          cert: :string,
          key: :string,
          person: :string,
          output: :string,
          request_id: :string,
          handoff: :boolean,
          code: :string
        ]
      )

    if rest != [] or invalid != [], do: AttentionServer.CLI.error!("unexpected owner arguments")
    endpoint = get_endpoint!(options)
    certificate = required!(options, :cert)
    key = required!(options, :key)
    person = required!(options, :person)

    if command != "invite" and
         (Keyword.get(options, :handoff, false) or Keyword.has_key?(options, :request_id)),
       do: AttentionServer.CLI.error!("handoff and request-id belong to invitation creation")

    AttentionServer.CLI.start_application!()

    cond do
      command == "invite" and Keyword.get(options, :handoff, false) ->
        if Keyword.has_key?(options, :output),
          do: AttentionServer.CLI.error!("use one invitation result channel")

        request_id = required!(options, :request_id)

        result =
          AttentionServer.InvitationHandoff.create(endpoint, certificate, key, person, request_id)

        # Explicit sensitive product result; shell status/log output never receives the URL.
        IO.puts(AttentionServer.InvitationHandoff.encode_result(result))
        result

      command in ["invite", "browser-sign-in"] ->
        path = required!(options, :output)

        result =
          File.open(path, [:write, :exclusive, :binary], fn io ->
            :ok = File.chmod(path, 0o600)

            case command do
              "invite" ->
                response =
                  case Keyword.get(options, :request_id) do
                    nil ->
                      HeadlessOwner.invite(endpoint, certificate, key, person)

                    request_id ->
                      HeadlessOwner.invite(endpoint, certificate, key, person, request_id)
                  end

                case response do
                  {:ok, %{"invitation" => invitation}} ->
                    :ok = IO.binwrite(io, AttentionServer.JSON.encode!(invitation) <> "\n")

                    AttentionServer.CLI.shell().info(
                      "Invitation saved to #{path}; expires #{invitation["expires_at"]}"
                    )

                  _ ->
                    AttentionServer.CLI.error!("invitation request failed")
                end

              "browser-sign-in" ->
                case HeadlessOwner.browser_sign_in(endpoint, certificate, key, person) do
                  {:ok, %{"browser_sign_in" => %{"sign_in_url" => url, "expires_at" => deadline}}}
                  when is_binary(url) ->
                    :ok = IO.binwrite(io, url <> "\n")

                    AttentionServer.CLI.shell().info(
                      "One-use sign-in link saved to #{path}; expires #{deadline}"
                    )

                  _ ->
                    AttentionServer.CLI.error!("browser sign-in link unavailable")
                end
            end
          end)

        if match?({:error, _}, result),
          do: AttentionServer.CLI.error!("cannot create private invitation file")

      true ->
        if Keyword.get(options, :handoff, false) or Keyword.has_key?(options, :request_id),
          do: AttentionServer.CLI.error!("handoff is an invitation action")

        code = required!(options, :code)

        case HeadlessOwner.lookup(endpoint, certificate, key, person, code) do
          {:ok, %{"device_id" => device, "expires_at" => deadline, "label" => label}} ->
            AttentionServer.CLI.shell().info(
              "Account: #{person}\nDevice: #{display_label(label)}\nPublic-key fingerprint: #{device}\nExpires: #{deadline}"
            )

            if String.trim(
                 AttentionServer.CLI.shell().prompt("Type DECLINE for this exact request: ") || ""
               ) == "DECLINE" do
              case HeadlessOwner.decline(
                     endpoint,
                     certificate,
                     key,
                     person,
                     code,
                     device,
                     deadline
                   ) do
                {:ok, %{"ok" => true}} -> AttentionServer.CLI.shell().info("declined")
                _ -> AttentionServer.CLI.error!("decline failed")
              end
            else
              AttentionServer.CLI.error!("decline cancelled")
            end

          _ ->
            AttentionServer.CLI.error!("pending request not found")
        end
    end
  end

  def run(["approve" | args]) do
    {options, rest, invalid} =
      OptionParser.parse(args,
        strict: [
          connect: :string,
          name: :string,
          port: :integer,
          ca: :string,
          cert: :string,
          key: :string,
          person: :string,
          code: :string,
          senders: :string
        ]
      )

    if rest != [] or invalid != [], do: AttentionServer.CLI.error!("unexpected owner arguments")
    endpoint = get_endpoint!(options)
    certificate = required!(options, :cert)
    key = required!(options, :key)
    person = required!(options, :person)
    code = required!(options, :code)

    senders =
      case Keyword.get(options, :senders) do
        nil -> []
        value -> String.split(value, ",", trim: true)
      end

    AttentionServer.CLI.start_application!()

    if HeadlessTLS.certificate_lifecycle(certificate) != :ok,
      do:
        AttentionServer.CLI.shell().info(
          "warning: owner certificate expires within 30 days or is invalid"
        )

    case HeadlessOwner.lookup(endpoint, certificate, key, person, code) do
      {:ok, %{"label" => label, "device_id" => device, "expires_at" => deadline}} ->
        AttentionServer.CLI.shell().info(
          "Account: #{person}\nDevice label (unsafe bytes escaped): #{display_label(label)}\nPublic-key fingerprint: #{device}\nExpires: #{deadline}"
        )

        if String.trim(
             AttentionServer.CLI.shell().prompt("Type APPROVE to bind this exact key: ") || ""
           ) ==
             "APPROVE" do
          case HeadlessOwner.approve(
                 endpoint,
                 certificate,
                 key,
                 person,
                 code,
                 device,
                 deadline,
                 senders
               ) do
            {:ok, %{"device_id" => ^device}} ->
              AttentionServer.CLI.shell().info("approved #{device}")

            {:error, reason} ->
              AttentionServer.CLI.error!("approval failed: #{inspect(reason)}")

            _ ->
              AttentionServer.CLI.error!("approval response mismatch")
          end
        else
          AttentionServer.CLI.error!("approval cancelled")
        end

      {:error, reason} ->
        AttentionServer.CLI.error!("lookup failed: #{inspect(reason)}")

      _ ->
        AttentionServer.CLI.error!("invalid lookup response")
    end
  end

  def run([command | args])
      when command in [
             "revoke-device",
             "grant-sender",
             "revoke-sender",
             "grant-read",
             "revoke-read",
             "grant-maintainer",
             "revoke-maintainer",
             "agent-register",
             "agent-list",
             "agent-grants-replace",
             "agent-revoke"
           ] do
    {options, rest, invalid} =
      OptionParser.parse(args,
        strict: [
          connect: :string,
          name: :string,
          port: :integer,
          ca: :string,
          cert: :string,
          key: :string,
          person: :string,
          device: :string,
          sender: :string,
          connector: :string,
          grant: :string,
          source: :string,
          target_kind: :string,
          target_id: :string,
          target_key: :string,
          grantee: :string,
          scopes: :string,
          agent_cert: :string,
          agent_id: :string,
          agent_key: :string,
          revision: :integer,
          read: :string,
          respond: :string
        ]
      )

    if rest != [] or invalid != [], do: AttentionServer.CLI.error!("unexpected owner arguments")
    endpoint = get_endpoint!(options)
    certificate = required!(options, :cert)
    key = required!(options, :key)
    person = required!(options, :person)
    AttentionServer.CLI.start_application!()

    if HeadlessTLS.certificate_lifecycle(certificate) != :ok,
      do:
        AttentionServer.CLI.shell().info(
          "warning: owner certificate expires within 30 days or is invalid"
        )

    result =
      case command do
        "revoke-device" ->
          device = required!(options, :device)

          if String.trim(
               AttentionServer.CLI.shell().prompt("Type REVOKE to fence device #{device}: ") || ""
             ) ==
               "REVOKE",
             do: HeadlessOwner.revoke_device(endpoint, certificate, key, person, device),
             else: {:error, :cancelled}

        "grant-sender" ->
          HeadlessOwner.grant_sender(
            endpoint,
            certificate,
            key,
            person,
            required!(options, :device),
            required!(options, :sender)
          )

        "revoke-sender" ->
          HeadlessOwner.revoke_sender(
            endpoint,
            certificate,
            key,
            person,
            required!(options, :device),
            required!(options, :sender)
          )

        "grant-read" ->
          target = %{
            "kind" => required!(options, :target_kind),
            "id" => required!(options, :target_id),
            "key_fingerprint" => required!(options, :target_key)
          }

          HeadlessOwner.grant_read(
            endpoint,
            certificate,
            key,
            person,
            required!(options, :connector),
            required!(options, :grant),
            required!(options, :source),
            target
          )

        "revoke-read" ->
          HeadlessOwner.revoke_read(
            endpoint,
            certificate,
            key,
            person,
            required!(options, :grant)
          )

        "grant-maintainer" ->
          scopes = required!(options, :scopes) |> String.split(",", trim: true)

          HeadlessOwner.grant_maintainer(
            endpoint,
            certificate,
            key,
            person,
            required!(options, :grant),
            required!(options, :grantee),
            scopes
          )

        "revoke-maintainer" ->
          HeadlessOwner.revoke_maintainer(
            endpoint,
            certificate,
            key,
            person,
            required!(options, :grant)
          )

        "agent-register" ->
          id = required!(options, :agent_id)

          fingerprint =
            case HeadlessAgent.fingerprint(required!(options, :agent_cert)) do
              {:ok, value} -> value
              _ -> AttentionServer.CLI.error!("agent certificate is invalid")
            end

          AttentionServer.CLI.shell().info(
            "Account: #{person}\nAgent ID: #{id}\nPublic-key fingerprint: #{fingerprint}"
          )

          if String.trim(
               AttentionServer.CLI.shell().prompt(
                 "Type ENROLL to bind this exact agent key with no grants: "
               ) ||
                 ""
             ) == "ENROLL",
             do:
               HeadlessOwner.register_agent(endpoint, certificate, key, person, id, fingerprint),
             else: {:error, :cancelled}

        "agent-list" ->
          HeadlessOwner.list_agents(endpoint, certificate, key, person)

        "agent-grants-replace" ->
          fingerprint = required!(options, :agent_key)
          revision = required!(options, :revision)
          read = split_patterns(Keyword.get(options, :read, ""))
          respond = split_patterns(Keyword.get(options, :respond, ""))

          AttentionServer.CLI.shell().info(
            "Account: #{person}\nAgent key: #{fingerprint}\nExpected grant revision: #{revision}\nRead: #{inspect(read)}\nRespond: #{inspect(respond)}"
          )

          if String.trim(
               AttentionServer.CLI.shell().prompt("Type REPLACE to commit these exact scopes: ") ||
                 ""
             ) ==
               "REPLACE",
             do:
               HeadlessOwner.replace_agent_grants(
                 endpoint,
                 certificate,
                 key,
                 person,
                 fingerprint,
                 revision,
                 read,
                 respond
               ),
             else: {:error, :cancelled}

        "agent-revoke" ->
          fingerprint = required!(options, :agent_key)

          if String.trim(
               AttentionServer.CLI.shell().prompt(
                 "Type REVOKE to permanently disable agent key #{fingerprint}: "
               ) ||
                 ""
             ) == "REVOKE",
             do: HeadlessOwner.revoke_agent(endpoint, certificate, key, person, fingerprint),
             else: {:error, :cancelled}
      end

    case result do
      {:ok, %{"agents" => agents}} ->
        AttentionServer.CLI.shell().info(AttentionServer.JSON.encode!(agents))

      {:ok, %{"ok" => true}} ->
        AttentionServer.CLI.shell().info("owner change committed")

      {:error, reason} ->
        AttentionServer.CLI.error!("owner change failed: #{inspect(reason)}")

      _ ->
        AttentionServer.CLI.error!("invalid owner response")
    end
  end

  def run(_),
    do:
      AttentionServer.CLI.error!(
        "use approve or a scoped owner change; see mix help attention.owner"
      )

  defp get_endpoint!(options) do
    case ToolEndpoint.from_options(options) do
      {:ok, endpoint} -> endpoint
      _ -> AttentionServer.CLI.error!("--connect, --name, --port and --ca are required")
    end
  end

  defp required!(options, key),
    do: Keyword.get(options, key) || AttentionServer.CLI.error!("--#{key} is required")

  defp split_patterns(""), do: []
  defp split_patterns(value), do: String.split(value, ",", trim: true)

  # Pending labels are attacker supplied. Keep terminal control, bidi and
  # ambiguous backslash bytes out of the trusted owner approval display.
  defp display_label(label) when is_binary(label) do
    for <<byte <- label>>, into: "" do
      if byte in 0x20..0x7E and byte != ?\\,
        do: <<byte>>,
        else: "\\x" <> Base.encode16(<<byte>>)
    end
  end
end
