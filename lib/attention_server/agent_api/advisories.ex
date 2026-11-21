defmodule AttentionServer.AgentAPI.Advisories do
  @moduledoc false

  alias AttentionServer.RouteBIndex
  alias AttentionServer.ClientAPI.Resources

  def for_message(engine, {person, key}, {person, _, _} = item) do
    with %{origin: ^key, session_handle: session} when is_binary(session) <-
           Map.get(engine.messages, item),
         %{entries: entries, receipts: receipts} <-
           Map.get(engine.advisory_feeds, {person, key, session}) do
      engine.route_b_index
      |> RouteBIndex.advisory_sequences(item)
      |> Enum.sort()
      |> Enum.map(fn seq -> render(Map.fetch!(entries, seq), receipts) end)
    else
      _ -> []
    end
  end

  def change(engine, {person, _, _} = scope, id) do
    with {:ok, sequence} <- Map.fetch(engine.advisory_id_index, {scope, id}),
         %{entries: %{^sequence => advisory}, receipts: receipts} <-
           Map.get(engine.advisory_feeds, scope),
         %{"sender_id" => sender, "message_id" => message_id} <- advisory,
         {:ok, item_id} <- Resources.item_id(engine, {person, sender, message_id}) do
      {:ok, %{"item_id" => item_id, "advisory" => render(advisory, receipts)}}
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  defp render(advisory, receipts) do
    advisory
    |> Map.drop(
      ~w(kind sender_id message_id session_handle advisory_seq origin_cursor_at_decision)
    )
    |> Map.put("delivered", MapSet.member?(receipts, advisory["advisory_id"]))
  end
end
