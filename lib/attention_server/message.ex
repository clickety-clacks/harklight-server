defmodule AttentionServer.Message do
  @moduledoc false

  @namespace ~r/\A[a-z][a-z0-9+.\-]*\z/
  @forbidden_rest ~r/[\s\p{Cc}]/u
  @uuid_v4 ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/

  def sender_id?(value) when is_binary(value) do
    case String.split(value, ":", parts: 2) do
      [namespace, rest] ->
        rest != "" and Regex.match?(@namespace, namespace) and
          not Regex.match?(@forbidden_rest, rest)

      _ ->
        false
    end
  end

  def sender_id?(_), do: false

  def nonempty?(value), do: is_binary(value) and value != ""
  def uuid_v4?(value), do: is_binary(value) and Regex.match?(@uuid_v4, value)

  def validate(message) when is_map(message) and not is_struct(message) do
    required = [
      {"sender_id", &sender_id?/1},
      {"message_id", &nonempty?/1},
      {"created_at", &is_integer/1},
      {"title", &nonempty?/1}
    ]

    optional = [
      {"body", &is_binary/1},
      {"stake", &is_binary/1},
      {"by_when", &is_integer/1},
      {"answer_ui", &plain_object?/1},
      {"callback", &plain_object?/1},
      {"open", &valid_open?/1}
    ]

    Enum.find_value(required, :ok, fn {field, valid?} ->
      if valid?.(Map.get(message, field)), do: false, else: {:error, field}
    end)
    |> case do
      :ok ->
        Enum.find_value(optional, :ok, fn {field, valid?} ->
          if not Map.has_key?(message, field) or valid?.(Map.fetch!(message, field)),
            do: false,
            else: {:error, field}
        end)

      error ->
        error
    end
  end

  def validate(_), do: {:error, "message"}

  defp plain_object?(value), do: is_map(value) and not is_struct(value)

  defp valid_open?(%{"argv" => argv, "terminal" => terminal})
       when is_list(argv) and argv != [] and is_boolean(terminal),
       do: Enum.all?(argv, &is_binary/1)

  defp valid_open?(_), do: false
end
