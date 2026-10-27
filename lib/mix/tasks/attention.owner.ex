defmodule Mix.Tasks.Attention.Owner do
  @moduledoc """
  Headless owner approval over a current owner-client-certificate TLS channel.

      mix attention.owner approve --connect 127.0.0.1 --name relay.test --port 8444 --ca ca.pem --cert owner.pem --key owner.key --person UUID --code CODE

  The command displays the stored label with unsafe bytes visibly escaped and the device-key fingerprint and
  requires typing `APPROVE` before a one-use approval. Optional `--senders`
  accepts comma-separated exact sender IDs; absent means no sender grant.
  With the same `--connect`, `--name`, `--port`, `--ca`, `--cert`, `--key` and
  `--person` options, additional commands are:

  * `revoke-device --device SPKI` (requires typing `REVOKE`)
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

  use Mix.Task
  @shortdoc "Review and explicitly approve one pending Attention device"

  @impl true
  def run(args) do
    Mix.Task.run("app.start")

    AttentionServer.CLI.with_io(Mix.shell(), Mix.Error, fn ->
      AttentionServer.CLI.Owner.run(args)
    end)
  end
end
