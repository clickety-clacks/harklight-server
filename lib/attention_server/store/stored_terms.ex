defmodule AttentionServer.Store.StoredTerms do
  @moduledoc false

  alias AttentionServer.{
    AgentAccess,
    APNS,
    BounceDecision,
    BrowserSignIn,
    Conversation,
    Engine,
    GuidedJoin,
    JSON.DecimalToken,
    Message
  }

  # Safe ETF decoding can only use atoms already present in the VM.
  def load_atoms do
    for module <- [
          Engine,
          BounceDecision,
          Conversation,
          GuidedJoin,
          BrowserSignIn,
          DecimalToken,
          APNS,
          AgentAccess,
          Message
        ] do
      {:module, ^module} = Code.ensure_loaded(module)
    end

    :ok
  end

  def load_keyed_atoms do
    for module <- [
          AttentionServer.Groups,
          AttentionServer.SharedMessages,
          AttentionServer.SharedAnswers,
          AttentionServer.SharedPush,
          AttentionServer.SharedReminders,
          AttentionServer.LargeData.Ref
        ] do
      Code.ensure_loaded!(module)
    end

    load_atoms()
  end

end
