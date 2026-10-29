defmodule AttentionServer.Message do
  @moduledoc false

  @namespace ~r/\A[a-z][a-z0-9+.\-]*\z/
  @forbidden_rest ~r/[\s\p{Cc}]/u
  @uuid_v4 ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/
  @urgencies ~w(low normal high now)

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

  @doc "Applies field-local acceptance rules without rewriting ignored message fields."
  def normalize_optional_fields(message, accepted_at, style_hold? \\ nil)

  def normalize_optional_fields(message, accepted_at, nil) when is_map(message) do
    normalize_optional_fields(
      message,
      accepted_at,
      Application.get_env(:attention_server, :choice_styles_held, true) != false
    )
  end

  def normalize_optional_fields(message, accepted_at, style_hold?)
      when is_map(message) and is_integer(accepted_at) and is_boolean(style_hold?) do
    checks = [
      {"urgency", fn value -> if valid_urgency?(value), do: nil, else: "invalid" end},
      {"default", &default_warning(&1, message, accepted_at)},
      {"by_when", fn value -> if is_integer(value), do: nil, else: "invalid" end},
      {"thread", fn value -> if nonempty?(value), do: nil, else: "invalid" end},
      {"choice_styles", &choice_styles_warning(&1, message, style_hold?)},
      {"stake", fn value -> if nonempty?(value), do: nil, else: "invalid" end}
    ]

    {normalized, warnings} =
      Enum.reduce(checks, {message, []}, fn {field, warning_for}, {current, warnings} ->
        case Map.fetch(current, field) do
          :error ->
            {current, warnings}

          {:ok, _value} when field == "choice_styles" and style_hold? ->
            {Map.delete(current, field), [%{"field" => field, "reason" => "held"} | warnings]}

          {:ok, nil} ->
            {current, warnings}

          {:ok, value} ->
            case warning_for.(value) do
              nil ->
                {current, warnings}

              "held" when field == "choice_styles" ->
                {Map.delete(current, field), [%{"field" => field, "reason" => "held"} | warnings]}

              reason ->
                {current, [%{"field" => field, "reason" => reason} | warnings]}
            end
        end
      end)

    {normalized, Enum.reverse(warnings)}
  end

  def normalize_optional_fields(message, _accepted_at, _style_hold?), do: {message, []}

  def valid_urgency?(value), do: value in @urgencies

  def effective_urgency(message) when is_map(message) do
    case Map.get(message, "urgency") do
      value when value in @urgencies -> value
      _ -> "normal"
    end
  end

  def effective_urgency(_), do: "normal"

  def effective_by_when(message) when is_map(message) do
    case Map.get(message, "by_when") do
      value when is_integer(value) -> value
      _ -> nil
    end
  end

  def effective_by_when(_), do: nil

  def effective_stake(message) when is_map(message) do
    case Map.get(message, "stake") do
      value when is_binary(value) and value != "" -> value
      _ -> ""
    end
  end

  def effective_stake(_), do: ""

  def valid_thread(message) when is_map(message) do
    case Map.get(message, "thread") do
      value when is_binary(value) and value != "" -> value
      _ -> nil
    end
  end

  def valid_thread(_), do: nil

  @doc "Returns the accepted default and its first matching value-action label, if valid."
  def default_answer(message, accepted_at) when is_map(message) and is_integer(accepted_at) do
    with %{"value" => value, "at" => at} = default <- Map.get(message, "default"),
         true <-
           map_size(default) == 2 and not is_nil(value) and is_integer(at) and at > accepted_at,
         actions when is_list(actions) <- get_in(message, ["answer_ui", "actions"]),
         action when is_map(action) <-
           Enum.find(actions, fn action ->
             is_map(action) and action["kind"] == "value" and action["value"] == value
           end) do
      %{value: value, at: at, text: Map.get(action, "label")}
    else
      _ -> nil
    end
  end

  def default_answer(_, _), do: nil

  defp default_warning(value, message, accepted_at) do
    cond do
      not valid_default_shape?(value) -> "invalid"
      not matching_value_action?(message, value["value"]) -> "not_a_choice"
      value["at"] <= accepted_at -> "past"
      true -> nil
    end
  end

  defp valid_default_shape?(%{"value" => value, "at" => at} = default),
    do: map_size(default) == 2 and not is_nil(value) and is_integer(at)

  defp valid_default_shape?(_), do: false

  defp matching_value_action?(message, value) do
    case get_in(message, ["answer_ui", "actions"]) do
      actions when is_list(actions) ->
        Enum.any?(actions, fn
          %{"kind" => "value", "value" => action_value} -> action_value == value
          _ -> false
        end)

      _ ->
        false
    end
  end

  defp choice_styles_warning(_value, _message, true), do: "held"

  defp choice_styles_warning(value, message, false) do
    action_ids =
      case get_in(message, ["answer_ui", "actions"]) do
        actions when is_list(actions) ->
          actions
          |> Enum.flat_map(fn
            %{"id" => id} when is_binary(id) -> [id]
            _ -> []
          end)
          |> MapSet.new()

        _ ->
          MapSet.new()
      end

    if is_map(value) and not is_struct(value) and
         Enum.all?(value, fn {id, style} ->
           is_binary(id) and MapSet.member?(action_ids, id) and is_binary(style)
         end),
       do: nil,
       else: "invalid"
  end

  defp plain_object?(value), do: is_map(value) and not is_struct(value)

  defp valid_open?(%{"argv" => argv, "terminal" => terminal})
       when is_list(argv) and argv != [] and is_boolean(terminal),
       do: Enum.all?(argv, &is_binary/1)

  defp valid_open?(_), do: false
end
