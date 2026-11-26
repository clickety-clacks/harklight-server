defmodule AttentionServer.AgentAccess do
  @moduledoc """
  Registered-agent grant patterns and the state and content matching helpers
  shared by client API v2 agent search.
  """

  alias AttentionServer.{JSON, LargeData, Message}

  @hosted_notice_sender Message.hosted_notice_sender()

  def valid_patterns?(patterns) do
    is_list(patterns) and length(patterns) <= 100 and
      length(Enum.uniq(patterns)) == length(patterns) and
      Enum.all?(patterns, fn pattern ->
        Message.sender_id?(pattern)
      end)
  end

  def grantable_patterns?(patterns),
    do: valid_patterns?(patterns) and @hosted_notice_sender not in patterns

  def visible_sender?(%{kind: :agent}, @hosted_notice_sender), do: false
  def visible_sender?(_principal, _sender), do: true

  def allowed?(_patterns, @hosted_notice_sender), do: false

  def allowed?(patterns, sender) when is_binary(sender) do
    Enum.any?(patterns, fn pattern ->
      pattern == sender or
        (String.ends_with?(pattern, "/") and String.starts_with?(sender, pattern))
    end)
  end

  def allowed?(_, _), do: false

  @doc false
  def state_matches?(_, nil), do: true
  def state_matches?(%{terminal: nil}, "open"), do: true
  def state_matches?(%{terminal: %{state: state}}, state), do: true
  def state_matches?(_, _), do: false

  @doc false
  def contains?(_value, nil), do: true
  def contains?(value, needle) when is_binary(value), do: String.contains?(value, needle)
  def contains?(%LargeData.Ref{} = ref, needle), do: LargeData.contains?(ref, needle)
  def contains?(%JSON.DecimalToken{}, _needle), do: false

  def contains?(value, needle) when is_map(value),
    do: Enum.any?(value, fn {_, v} -> contains?(v, needle) end)

  def contains?(value, needle) when is_list(value), do: Enum.any?(value, &contains?(&1, needle))
  def contains?(_, _), do: false
end
