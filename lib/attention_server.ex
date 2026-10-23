defmodule AttentionServer do
  @moduledoc """
  Harklight's self-hosted relay server.

  `AttentionServer` is the retained module namespace from the product's former
  Attention name. The supervised server authenticates TLS client certificates
  before device or agent requests reach `AttentionServer.Relay`; local operator
  setup functions remain separate from untrusted wire requests.
  """
end
