defmodule AttentionServer.RelayReaders do
  @moduledoc false

  # Test-only readers of a running Relay's committed engine: the protocol 0.1
  # device event streams and the conversation cursor. No production code reads
  # them; Relay keeps no device sessions. `:sys.get_state/1` is ordered after
  # the caller's earlier requests, so a read observes their commits.

  alias AttentionServer.Engine

  def events(relay, key, since), do: Engine.events(engine(relay), key, since)

  def conversation_events(relay, key, since),
    do: Engine.conversation_events(engine(relay), key, since)

  def conversation_cursor(relay, key), do: Engine.conversation_cursor(engine(relay), key)

  defp engine(relay), do: :sys.get_state(relay).store.engine
end
