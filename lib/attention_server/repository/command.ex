defmodule AttentionServer.Repository.Command do
  @moduledoc """
  Prepares a normalized client or Agent command against the current repository
  view. The caller supplies the authenticated actor, admitted command, request
  time, and staged upload references. The resulting Engine carries the exact
  touched row addresses that Store validates and persists.

  Files are copied outside this boundary. The source references are checked
  against the current Store immediately before an app command uses them.
  """

  alias AttentionServer.Engine
  alias AttentionServer.JSON, warn: false
  alias AttentionServer.AgentAPI.Advisories
  alias AttentionServer.ClientAPI.Uploads

  def verify_sources(store, person, sources) do
    Enum.reduce_while(sources, :ok, fn {_upload_id, file_id}, :ok ->
      case Uploads.fetch(store, person, file_id) do
        {:ok, _} -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  def execute(:postgres, %{kind: :browser} = actor, {:alerts, value, if_match}, key, at) do
    AttentionServer.Repository.Postgres.AlertSettings.execute(actor, value, if_match, key, at)
  end

  def execute(store, actor, frame, sources, at) do
    case Engine.request_command(store.engine, actor, frame, at) do
      {next, {:ok, _} = reply} -> {record_sources(next, sources), reply}
      result -> result
    end
  end

  defp record_sources(engine, sources) do
    Enum.reduce(sources, engine, fn {upload_id, file_id}, state ->
      upload = Map.fetch!(state.uploads, upload_id)
      Engine.put_row(state, :uploads, upload_id, Map.put_new(upload, :staged_file_id, file_id))
    end)
  end

  if Mix.env() == :test do
    def prepare_commit(store, next, %{action: "message_open"} = audit, raw) when is_binary(raw) do
      with {:ok, command} <- JSON.decode_line(raw), do: prepare_commit(store, next, audit, command)
    end
  end

  def prepare_commit(store, next, %{action: "message_open", person: person}, %{
        "sender_id" => sender,
        "message_id" => id
      }) do
    Advisories.stamp_decisions(store, next, [{person, sender, id}])
  end

  def prepare_commit(_store, next, _audit, _original), do: {:ok, next}
end
