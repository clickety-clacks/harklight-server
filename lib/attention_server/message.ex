defmodule AttentionServer.Message do
  @moduledoc false

  @namespace ~r/\A[a-z][a-z0-9+.\-]*\z/
  @forbidden_rest ~r/[\s\p{Cc}]/u
  @uuid ~r/\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\z/
  @uuid_v4 ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/
  @urgencies ~w(low normal high now)
  @message_types ~w(fyi request)
  @message_kinds ~w(blocker success)
  @seen_descriptor %{
    "schema" => "attention-client.actions/1",
    "actions" => [
      %{
        "id" => "seen",
        "label" => "Seen",
        "kind" => "value",
        "value" => %{"acknowledgment" => "seen"}
      }
    ]
  }
  @actions_schema "attention-client.actions/1"
  @ui_schema "attention-client.ui/1"
  @request_needs_controls_reason ~s(message_type "request" needs at least one choice or a reply action in answer_ui; add a "value" choice or {"id":"reply","label":"Reply","kind":"text"}, or set message_type "fyi" with the Seen descriptor)
  @request_needs_ui1_controls_reason ~s(message_type "request" with attention-client.ui/1 answer_ui needs at least one input in "inputs")
  @request_has_seen_reason ~s(message_type "request" cannot carry the Seen action: remove the action with id "seen", or send as message_type "fyi" with only the Seen descriptor)

  @hosted_notice_sender "app:harklight/notices"

  def hosted_notice_sender, do: @hosted_notice_sender
  def seen_descriptor, do: @seen_descriptor

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
  @doc "Any well-formed UUID; operation ids need uniqueness, not a particular version."
  def uuid?(value), do: is_binary(value) and Regex.match?(@uuid, value)
  def uuid_v4?(value), do: is_binary(value) and Regex.match?(@uuid_v4, value)

  def validate(message) when is_map(message) and not is_struct(message) do
    validate_message(message, Map.get(message, "message_type") in @message_types)
  end

  def validate(_), do: {:error, "message"}

  @doc false
  def validate_revision_base(message) when is_map(message) and not is_struct(message),
    do: validate_message(message, true)

  def validate_revision_base(_), do: {:error, "message"}

  defp validate_message(message, skip_answer_ui?) do
    required = [
      {"sender_id", &sender_id?/1},
      {"message_id", &nonempty?/1},
      {"created_at", &is_integer/1},
      {"title", &(nonempty?(&1) or AttentionServer.LargeData.ref?(&1))}
    ]

    optional = [
      {"body", &(is_binary(&1) or AttentionServer.LargeData.ref?(&1))},
      {"answer_ui", &plain_object?/1},
      {"callback", &plain_object?/1},
      {"open", &valid_open?/1}
    ]

    optional =
      if skip_answer_ui?,
        do: Enum.reject(optional, fn {field, _valid?} -> field == "answer_ui" end),
        else: optional

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

  @doc false
  def validate_present_message_type(message) when is_map(message) and not is_struct(message) do
    case Map.fetch(message, "message_type") do
      :error -> :ok
      {:ok, value} when value in @message_types -> :ok
      {:ok, value} -> {:error, typed_error("message_type_invalid", invalid_type_reason(value))}
    end
  end

  def validate_present_message_type(_), do: :ok

  @doc false
  def validate_typed_message(message) when is_map(message) and not is_struct(message) do
    with :ok <- validate_present_message_type(message) do
      case Map.fetch(message, "message_type") do
        :error -> :ok
        {:ok, "fyi"} -> validate_fyi(message)
        {:ok, "request"} -> validate_request(message)
      end
    end
  end

  def validate_typed_message(_), do: :ok

  @doc false
  def request_for_alert?(%{"message_type" => "request"}), do: true
  def request_for_alert?(%{"message_type" => _}), do: false

  def request_for_alert?(%{"answer_ui" => %{"schema" => @actions_schema, "actions" => actions}})
      when is_list(actions) do
    Enum.any?(actions, fn action ->
      not seen_action?(action) and match?({:ok, _}, validate_action(action, 1, MapSet.new()))
    end)
  end

  def request_for_alert?(%{"answer_ui" => %{"schema" => @ui_schema, "inputs" => [_ | _]}}),
    do: true

  def request_for_alert?(_), do: false

  def valid_message_kind?(value), do: value in @message_kinds
  def valid_message_type?(value), do: value in @message_types

  @doc false
  def progress_message?(%{"message_type" => "fyi"} = message),
    do: Map.has_key?(message, "progress")

  def progress_message?(_), do: false

  @doc false
  def type_seen_only(%{
        "answer_ui" => %{
          "schema" => @actions_schema,
          "actions" => [%{"id" => "seen", "kind" => "value", "value" => value}]
        }
      } = message)
      when not is_nil(value),
      do: Map.put_new(message, "message_type", "fyi")

  def type_seen_only(message), do: message

  defp validate_fyi(message) do
    if Map.get(message, "answer_ui") == @seen_descriptor do
      :ok
    else
      {:error,
       typed_error(
         "fyi_has_answer_controls",
         ~s(message_type "fyi" carries only the Seen acknowledgment: set answer_ui to the Seen descriptor and remove choices and reply, or set message_type "request")
       )}
    end
  end

  defp validate_request(message) do
    answer_ui = Map.get(message, "answer_ui")

    cond do
      not Map.has_key?(message, "answer_ui") ->
        request_needs_controls()

      not plain_object?(answer_ui) ->
        request_unsupported_schema(answer_ui)

      Map.get(answer_ui, "schema") != @actions_schema ->
        # A custom screen (UI/1, or a version this Server does not know) never
        # refuses the message. UI/1 is checked by the single send-time
        # admission path after duplicate receipt handling (admit_answer_ui/2);
        # an unknown version is delivered for the client to show or to offer
        # the plain reply instead.
        :ok

      Map.get(answer_ui, "actions") == [] ->
        request_needs_controls()

      true ->
        validate_request_actions(answer_ui)
    end
  end

  @doc false
  def validate_ui1_controls(message) when is_map(message) and not is_struct(message) do
    case {Map.get(message, "message_type"), Map.get(message, "answer_ui")} do
      {"request", %{"schema" => @ui_schema} = answer_ui} ->
        case Map.get(answer_ui, "inputs") do
          inputs when is_list(inputs) and inputs != [] ->
            :ok

          _ ->
            {:error,
             typed_error("request_needs_answer_controls", @request_needs_ui1_controls_reason)}
        end

      _ ->
        :ok
    end
  end

  def validate_ui1_controls(_), do: :ok

  @reply_descriptor %{
    "schema" => @actions_schema,
    "actions" => [%{"id" => "reply", "label" => "Reply", "kind" => "text"}]
  }

  @doc """
  Admits a message's custom answer screen at send time. A screen that cannot
  be shown (over a limit, malformed, bad file reference, or a request
  screen with no input) never refuses the message: the screen is
  replaced by the plain Reply that a message with no screen gets, and the
  delivered message carries `answer_ui_unavailable` with the reason so
  clients can say the custom screen is not shown. Returns the message and any
  field warnings for the sender.
  """
  def admit_answer_ui(message, delivered_file_names, limits \\ AttentionServer.UI1.limits())

  def admit_answer_ui(message, delivered_file_names, limits)
      when is_map(message) and not is_struct(message) and is_list(delivered_file_names) do
    case Map.get(message, "answer_ui") do
      %{"schema" => @ui_schema} = answer_ui ->
        case ui1_problem(message, answer_ui, delivered_file_names, limits) do
          nil -> {Map.delete(message, "answer_ui_unavailable"), []}
          detail -> degrade_answer_ui(message, detail)
        end

      _ ->
        # The actions descriptor is validated with the message type; a version
        # this Server does not know is passed through for the client to show or
        # offer the plain reply.
        {message, []}
    end
  end

  def admit_answer_ui(message, _delivered_file_names, _limits), do: {message, []}

  @duplicate_keys :__attention_server_duplicate_keys__

  @doc """
  Removes the decoder's duplicate-member markers from a frame's message. A
  document with duplicate members cannot be shown, so its screen is replaced
  (admit_answer_ui/3); the marker must not reach the stored receipt.
  """
  def without_duplicate_markers(%{} = map) when not is_struct(map) do
    map
    |> Map.delete(@duplicate_keys)
    |> Map.new(fn {key, value} -> {key, without_duplicate_markers(value)} end)
  end

  def without_duplicate_markers(list) when is_list(list),
    do: Enum.map(list, &without_duplicate_markers/1)

  def without_duplicate_markers(other), do: other

  defp ui1_problem(message, answer_ui, delivered_file_names, limits) do
    case AttentionServer.UI1.validate(answer_ui, delivered_file_names, limits) do
      :ok ->
        case validate_ui1_controls(message) do
          :ok ->
            nil

          {:error, {:typed_message, _code, _reason}} ->
            "answer_ui (#{@ui_schema}): a request needs at least one input"
        end

      {:error, path, reason} ->
        "answer_ui (#{@ui_schema}) at #{path}: #{reason}"
    end
  end

  defp degrade_answer_ui(message, detail) do
    degraded =
      if Map.get(message, "message_type") == "request",
        do: Map.put(message, "answer_ui", @reply_descriptor),
        else: Map.delete(message, "answer_ui")

    {Map.put(degraded, "answer_ui_unavailable", detail),
     [%{"field" => "answer_ui", "reason" => "unavailable", "detail" => detail}]}
  end

  defp request_unsupported_schema(answer_ui) do
    schema =
      case answer_ui do
        value when is_map(value) and not is_struct(value) ->
          if Map.has_key?(value, "schema"),
            do: "got schema " <> json_value(Map.get(value, "schema")),
            else: "got no schema member"

        _ ->
          "got no schema member"
      end

    {:error,
     typed_error(
       "request_answer_ui_unsupported",
       ~s(message_type "request" supports answer_ui schema "#{@actions_schema}" or "#{@ui_schema}"; #{schema})
     )}
  end

  defp validate_request_actions(answer_ui) do
    actions = Map.get(answer_ui, "actions")

    cond do
      not exact_members?(answer_ui, ["schema", "actions"]) ->
        {:error,
         typed_error(
           "request_answer_ui_invalid",
           descriptor_members_reason(answer_ui)
         )}

      not is_list(actions) ->
        {:error,
         typed_error(
           "request_answer_ui_invalid",
           ~s(message_type "request" answer_ui member "actions" must be a non-empty array; add a "value" choice or Reply action)
         )}

      true ->
        case validate_actions(actions) do
          :ok ->
            seen_count = Enum.count(actions, &seen_action?/1)

            cond do
              seen_count == 0 ->
                :ok

              length(actions) == 1 ->
                request_needs_controls()

              true ->
                {:error, typed_error("request_has_seen", @request_has_seen_reason)}
            end

          {:error, reason} ->
            {:error, typed_error("request_answer_ui_invalid", reason)}
        end
    end
  end

  defp validate_actions(actions) do
    Enum.reduce_while(Enum.with_index(actions, 1), MapSet.new(), fn {action, index}, ids ->
      case validate_action(action, index, ids) do
        {:ok, id} -> {:cont, MapSet.put(ids, id)}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      %MapSet{} -> :ok
      error -> error
    end
  end

  defp validate_action(action, index, ids) when is_map(action) and not is_struct(action) do
    id = Map.get(action, "id")
    label = action_label(index, id)

    cond do
      not nonempty?(id) ->
        {:error,
         ~s(message_type "request" answer_ui #{label} needs a non-empty string "id"; give every action a unique id)}

      MapSet.member?(ids, id) ->
        {:error,
         ~s(message_type "request" answer_ui #{label} repeats "id"; use a unique non-empty string id)}

      not nonempty?(Map.get(action, "label")) ->
        {:error,
         ~s(message_type "request" answer_ui #{label} needs a non-empty string "label"; add a label such as "Reply" for a text action)}

      Map.get(action, "kind") not in ["value", "text"] ->
        {:error,
         ~s(message_type "request" answer_ui #{label} needs "kind" "value" or "text"; use "value" with a "value" member for a choice or "text" for Reply)}

      Map.get(action, "kind") == "value" and not Map.has_key?(action, "value") ->
        {:error,
         ~s(message_type "request" answer_ui #{label} needs member "value" for kind "value"; add the choice value)}

      not action_has_exact_members?(action) ->
        {:error, action_members_reason(action, index, id)}

      true ->
        {:ok, id}
    end
  end

  defp validate_action(_action, index, _ids) do
    {:error,
     ~s(message_type "request" answer_ui action #{index} must be an object; use an action with string "id", "label" and "kind" members)}
  end

  defp action_has_exact_members?(%{"kind" => "value"} = action),
    do: exact_members?(action, ["id", "label", "kind", "value"])

  defp action_has_exact_members?(%{"kind" => "text"} = action),
    do: exact_members?(action, ["id", "label", "kind"])

  defp descriptor_members_reason(answer_ui) do
    expected = MapSet.new(["schema", "actions"])
    actual = Map.keys(answer_ui) |> MapSet.new()

    case {MapSet.difference(expected, actual) |> MapSet.to_list() |> Enum.sort(),
          MapSet.difference(actual, expected) |> MapSet.to_list() |> Enum.sort()} do
      {[missing | _], _} ->
        ~s(message_type "request" answer_ui is missing member "#{missing}"; add only "schema" and "actions")

      {[], [extra | _]} ->
        ~s(message_type "request" answer_ui has unsupported member "#{extra}"; remove it and keep only "schema" and "actions")

      _ ->
        ~s(message_type "request" answer_ui must contain exactly "schema" and "actions")
    end
  end

  defp action_members_reason(action, index, id) do
    expected =
      if action["kind"] == "value",
        do: MapSet.new(["id", "label", "kind", "value"]),
        else: MapSet.new(["id", "label", "kind"])

    actual = Map.keys(action) |> MapSet.new()
    label = action_label(index, id)

    case {MapSet.difference(expected, actual) |> MapSet.to_list() |> Enum.sort(),
          MapSet.difference(actual, expected) |> MapSet.to_list() |> Enum.sort()} do
      {[missing | _], _} ->
        ~s(message_type "request" answer_ui #{label} is missing member "#{missing}"; add it to match the "#{action["kind"]}" action shape)

      {[], [extra | _]} ->
        ~s(message_type "request" answer_ui #{label} has unsupported member "#{extra}"; remove it to match the "#{action["kind"]}" action shape)

      _ ->
        ~s(message_type "request" answer_ui #{label} has an unsupported member shape; use an exact value or text action)
    end
  end

  defp exact_members?(value, expected) when is_map(value) do
    Map.keys(value) |> MapSet.new() == MapSet.new(expected)
  end

  defp action_label(index, id) when is_binary(id), do: "action #{index} (#{json_value(id)})"
  defp action_label(index, nil), do: "action #{index}"
  defp action_label(index, id), do: "action #{index} (id #{json_value(id)})"

  defp seen_action?(%{
         "id" => "seen",
         "kind" => "value",
         "value" => %{"acknowledgment" => "seen"}
       }),
       do: true

  defp seen_action?(_), do: false

  defp request_needs_controls do
    {:error, typed_error("request_needs_answer_controls", @request_needs_controls_reason)}
  end

  defp typed_error(code, reason), do: {:typed_message, code, reason}

  defp invalid_type_reason(value) do
    ~s(message_type must be "fyi" or "request"; got #{json_value(value)})
  end

  defp json_value(value), do: AttentionServer.JSON.encode!(value)

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
      {"stake", fn value -> if nonempty?(value), do: nil, else: "invalid" end},
      {"message_kind", fn value -> if valid_message_kind?(value), do: nil, else: "invalid" end}
    ]

    {normalized, warnings} =
      Enum.reduce(checks, {message, []}, fn {field, warning_for}, {current, warnings} ->
        case Map.fetch(current, field) do
          :error ->
            {current, warnings}

          {:ok, _value} when field == "choice_styles" and style_hold? ->
            {Map.delete(current, field), [%{"field" => field, "reason" => "held"} | warnings]}

          {:ok, nil} when field != "message_kind" ->
            {current, warnings}

          {:ok, value} ->
            case warning_for.(value) do
              nil ->
                {current, warnings}

              "held" when field == "choice_styles" ->
                {Map.delete(current, field), [%{"field" => field, "reason" => "held"} | warnings]}

              reason ->
                current =
                  if field == "message_kind", do: Map.delete(current, field), else: current

                {current, [%{"field" => field, "reason" => reason} | warnings]}
            end
        end
      end)

    normalized = if normalized["alarm"] == false, do: Map.delete(normalized, "alarm"), else: normalized
    {normalized, Enum.reverse(warnings)}
  end

  def normalize_optional_fields(message, _accepted_at, _style_hold?), do: {message, []}

  def alarm?(%{"alarm" => true}), do: true
  def alarm?(_message), do: false

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
