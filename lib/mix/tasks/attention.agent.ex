defmodule Mix.Tasks.Attention.Agent do
  @moduledoc """
  Locally generate a service identity and use its separately approved read or
  response grants over client API v2 on the device port, with mutual TLS.

      mix attention.agent keygen --directory ./agent
      mix attention.agent search --directory ./agent --connect 127.0.0.1 --name relay.test --port 8443 --ca ca.pem --sender app:one --q text
      mix attention.agent get --directory ./agent --connect 127.0.0.1 --name relay.test --port 8443 --ca ca.pem --item-id ITEM --entries
      mix attention.agent answer --directory ./agent --connect 127.0.0.1 --name relay.test --port 8443 --ca ca.pem --sender app:one --message MESSAGE --request-revision REVISION --answer-file answer.json

  `--port` is the device port. `search` is `GET /v2/search` (`--q`, `--sender`,
  `--message`, `--state`, `--limit`, `--page` from a previous `next_page`).
  `get` is `GET /v2/items/{item_id}` for `--item-id`, or for the item an exact
  `--sender` and `--message` name; `--entries` adds its entries page
  (`--before`, `--limit`). Each prints the server's JSON body.

  `answer` (or `respond`) is `POST /v2/items/{item_id}/answer`. The answer file
  is an AnswerBody object: at most one of `choice` (an action `id`), `comment`
  or `result`, or `{}` to answer with no comment. `--request-revision` is the
  item's `request_revision`. The answer needs typing ANSWER; after that and
  before anything is sent, the exact body and an `Idempotency-Key` (`--key
  UUID`, or a generated one) are saved under `operations/` in the agent
  directory. A cancelled answer saves nothing.
  Rerunning the same command resends the same body with the same key; the
  server replays a committed answer (`"replayed": true`) instead of writing a
  second one. Nothing is retried automatically, including after a
  non-retryable `internal_error`.

  The owner must first register the generated public certificate fingerprint
  and assign explicit read/respond scopes with `mix attention.owner`. Reading
  needs no read grant; answering needs both grants for the item's sender.
  """

  use Mix.Task
  @shortdoc "Generate or use an owner-enrolled Attention agent key"

  @impl true
  def run(args) do
    Mix.Task.run("app.start")

    AttentionServer.CLI.with_io(Mix.shell(), Mix.Error, fn ->
      AttentionServer.CLI.Agent.run(args)
    end)
  end
end
