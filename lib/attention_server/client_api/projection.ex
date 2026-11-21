defmodule AttentionServer.ClientAPI.Projection do
  @moduledoc false

  require Logger

  alias AttentionServer.Diagnostics
  alias AttentionServer.ClientAPI.Errors

  defstruct [:key, :cause]

  # Convert the fault where the resource key is known. Catch only its tagged
  # result at route/step boundaries, leaving mutation and store errors alone.
  def run(key, render) do
    inject_fault(key)
    render.()
  rescue
    exception -> throw(%__MODULE__{key: key, cause: Diagnostics.cause_class(exception)})
  end

  def capture(render) do
    render.()
  catch
    %__MODULE__{} = fault -> {:error, fault}
  end

  def log(conn, %__MODULE__{key: key, cause: cause}) do
    Logger.error(
      "event=harklight_projection_failure request_id=#{Errors.request_id(conn)} " <>
        "resource=#{safe_key(key)} cause=#{cause}"
    )

    conn
  end

  def resource(%{entry_id: id}), do: {:entry, id}
  def resource(%{item_id: id}), do: {:item, id}
  def resource(%{file_id: _}), do: :file
  def resource(_), do: :response

  def change_key(%{"entry_id" => id}), do: {:entry, id}
  def change_key(%{"item_id" => id}), do: {:item, id}
  def change_key(_), do: :change

  def response_key(%{"entry" => entry}), do: change_key(entry)
  def response_key(%{"item" => item}), do: change_key(item)
  def response_key(%{"message" => %{"item_id" => _} = message}), do: change_key(message)
  def response_key(%{"settings" => _}), do: :settings
  def response_key(_body), do: :response

  defp safe_key({kind, id}) when kind in [:item, :entry] and is_binary(id) do
    if byte_size(id) <= 32 and Regex.match?(~r/\A[ie][0-9]+\z/, id),
      do: "#{kind}:#{id}",
      else: Atom.to_string(kind)
  end

  defp safe_key({kind, _id}) when kind in [:item, :entry], do: Atom.to_string(kind)
  defp safe_key(kind) when is_atom(kind), do: Atom.to_string(kind)

  if Mix.env() == :test do
    defp inject_fault(key) do
      case Application.get_env(:attention_server, :client_api_projection_fault) do
        nil -> :ok
        probe -> probe.(key)
      end
    end
  else
    defp inject_fault(_key), do: :ok
  end
end
