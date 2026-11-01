defmodule AttentionServer.UI1 do
  @moduledoc false

  alias AttentionServer.JSON

  @schema "attention-client.ui/1"
  @duplicate_keys :__attention_server_duplicate_keys__
  @required ["schema", "requires", "illustrations", "inputs", "open", "resolved", "submission"]
  @optional ["resolvedByState", "answeredWithResult"]
  @terminal_states ~w(answered no_comment cleared expired superseded)

  @doc "Validates a delivered UI/1 document against the app-authored grammar corpus."
  def validate(document, delivered_file_names) when is_list(delivered_file_names) do
    try do
      validate_document!(document, delivered_file_names)
      :ok
    catch
      {:invalid, path, reason} -> {:error, path, reason}
    end
  end

  def validate(_document, _delivered_file_names),
    do: {:error, "$", "document must be an object; send a UI/1 document object"}

  defp validate_document!(document, delivered_file_names) do
    reject_duplicate_keys!(document, "$")
    root = object!(document, "$", "document must be an object")
    exact_fields!(root, @required, @optional, "$", "top-level members must match the UI/1 shape")

    schema = string!(root["schema"], "schema", false, "schema must be a string")
    require!(schema == @schema, "schema", "schema must be attention-client.ui/1")

    byte_count = byte_size(JSON.encode!(document))

    require!(
      byte_count <= 65_536,
      "$",
      "answer_ui document is #{byte_count} bytes; it must be at most 65536 UTF-8 bytes"
    )

    required =
      string_array!(root["requires"], "requires", true, 8, "requires must be a string array")

    unique_array!(required, "requires", "each used capability appears exactly once")

    {illustration_ids, contains_image} =
      parse_illustrations!(
        root["illustrations"],
        MapSet.new(delivered_file_names),
        delivered_file_names
      )

    {input_kinds, input_illustrations} = parse_inputs!(root["inputs"], illustration_ids)

    scene_state = %{
      count: 0,
      open_input_counts: %{},
      referenced_illustrations: input_illustrations,
      uses_result_point: false
    }

    {_, scene_state} =
      parse_scene!(
        root["open"],
        "open",
        1,
        false,
        false,
        true,
        input_kinds,
        illustration_ids,
        scene_state
      )

    {_, scene_state} =
      parse_scene!(
        root["resolved"],
        "resolved",
        1,
        true,
        false,
        false,
        input_kinds,
        illustration_ids,
        scene_state
      )

    scene_state =
      case Map.fetch(root, "resolvedByState") do
        :error ->
          scene_state

        {:ok, states} ->
          parse_resolved_by_state!(states, input_kinds, illustration_ids, scene_state)
      end

    scene_state =
      case Map.fetch(root, "answeredWithResult") do
        :error ->
          scene_state

        {:ok, answer} ->
          {_, next} =
            parse_scene!(
              answer,
              "answeredWithResult",
              1,
              true,
              true,
              false,
              input_kinds,
              illustration_ids,
              scene_state
            )

          next
      end

    submission = object!(root["submission"], "submission", "submission must be an object")

    exact_fields!(
      submission,
      ["label", "result"],
      [],
      "submission",
      "submission members must match its exact shape"
    )

    string!(submission["label"], "submission.label", true, "submission label must be non-empty")

    {result_references, result_count} =
      parse_constructor!(
        submission["result"],
        "submission.result",
        1,
        input_kinds,
        0,
        MapSet.new()
      )

    require!(
      result_count <= 128,
      "submission.result",
      "at most 128 result constructor nodes are allowed"
    )

    require!(scene_state.count <= 128, "open", "at most 128 scene nodes are allowed")

    input_ids = Map.keys(input_kinds) |> MapSet.new()
    open_counts = scene_state.open_input_counts

    require!(
      MapSet.new(Map.keys(open_counts)) == input_ids,
      "open",
      "each declared input must occur exactly once in open"
    )

    case Enum.find(input_ids, fn id -> Map.get(open_counts, id) != 1 end) do
      nil -> :ok
      id -> invalid!("open", "input #{inspect(id)} must occur exactly once in open")
    end

    require!(
      MapSet.subset?(input_ids, result_references),
      "submission.result",
      "every input must be used by the result constructor"
    )

    root["illustrations"]
    |> Enum.with_index()
    |> Enum.each(fn {illustration, index} ->
      id = illustration["id"]

      require!(
        MapSet.member?(scene_state.referenced_illustrations, id),
        "illustrations[#{index}]",
        "every illustration must be referenced"
      )
    end)

    expected =
      capabilities(input_kinds, illustration_ids, contains_image, scene_state.uses_result_point)

    unless MapSet.new(required) == expected do
      missing =
        MapSet.difference(expected, MapSet.new(required)) |> MapSet.to_list() |> Enum.sort()

      extra = MapSet.difference(MapSet.new(required), expected) |> MapSet.to_list() |> Enum.sort()
      fix = capability_fix(missing, extra)
      invalid!("requires", "requires must list exactly the capabilities used; #{fix}")
    end

    :ok
  end

  defp parse_illustrations!(value, delivered_files, delivered_file_names) do
    values = array!(value, "illustrations", "illustrations must be an array")
    require!(length(values) <= 64, "illustrations", "at most 64 illustrations are allowed")

    {ids, _primitive_total, contains_image} =
      values
      |> Enum.with_index()
      |> Enum.reduce({MapSet.new(), 0, false}, fn {item, index}, {ids, total, image?} ->
        path = "illustrations[#{index}]"
        item = object!(item, path, "illustration must be an object")

        exact_fields!(
          item,
          ["id", "label", "width", "height", "primitives"],
          [],
          path,
          "illustration members must match the required shape"
        )

        id = identifier!(item["id"], "#{path}.id")

        require!(
          !MapSet.member?(ids, id),
          "#{path}.id",
          "illustration identifiers must be unique; use a unique id"
        )

        string!(item["label"], "#{path}.label", true, "label must be non-empty")
        bounded_integer!(item["width"], "#{path}.width", 1, 4096, "width")
        bounded_integer!(item["height"], "#{path}.height", 1, 4096, "height")

        primitives =
          array!(item["primitives"], "#{path}.primitives", "primitives must be an array")

        require!(
          length(primitives) <= 512,
          "#{path}.primitives",
          "at most 512 primitives are allowed in one illustration"
        )

        found_image =
          primitives
          |> Enum.with_index()
          |> Enum.reduce(false, fn {primitive, primitive_index}, found ->
            parse_primitive!(
              primitive,
              "#{path}.primitives[#{primitive_index}]",
              delivered_files,
              delivered_file_names
            ) or found
          end)

        total = total + length(primitives)

        require!(
          total <= 512,
          "illustrations",
          "at most 512 primitives are allowed across illustrations"
        )

        {MapSet.put(ids, id), total, image? or found_image}
      end)

    {ids, contains_image}
  end

  defp parse_primitive!(raw, path, delivered_files, delivered_file_names) do
    primitive = object!(raw, path, "primitive must be an object")
    kind = required_string!(primitive, "kind", path, "primitive kind is required")

    case kind do
      "rect" ->
        exact_fields!(
          primitive,
          ~w(kind x y width height fill),
          [],
          path,
          "rect members must match its exact shape"
        )

        bounded_integer!(primitive["x"], "#{path}.x", 0, 4096, "x")
        bounded_integer!(primitive["y"], "#{path}.y", 0, 4096, "y")
        bounded_integer!(primitive["width"], "#{path}.width", 1, 4096, "width")
        bounded_integer!(primitive["height"], "#{path}.height", 1, 4096, "height")
        color!(primitive["fill"], "#{path}.fill")
        false

      "circle" ->
        exact_fields!(
          primitive,
          ~w(kind cx cy r fill),
          [],
          path,
          "circle members must match its exact shape"
        )

        bounded_integer!(primitive["cx"], "#{path}.cx", 0, 4096, "circle center")
        bounded_integer!(primitive["cy"], "#{path}.cy", 0, 4096, "circle center")
        bounded_integer!(primitive["r"], "#{path}.r", 1, 4096, "circle radius")
        color!(primitive["fill"], "#{path}.fill")
        false

      "line" ->
        exact_fields!(
          primitive,
          ~w(kind x1 y1 x2 y2 stroke strokeWidth),
          [],
          path,
          "line members must match its exact shape"
        )

        for key <- ~w(x1 y1 x2 y2),
            do: bounded_integer!(primitive[key], "#{path}.#{key}", 0, 4096, key)

        color!(primitive["stroke"], "#{path}.stroke")
        bounded_integer!(primitive["strokeWidth"], "#{path}.strokeWidth", 1, 64, "strokeWidth")
        false

      "text" ->
        exact_fields!(
          primitive,
          ~w(kind x y text fill fontSize align),
          [],
          path,
          "text primitive members must match its exact shape"
        )

        bounded_integer!(primitive["x"], "#{path}.x", 0, 4096, "x")
        bounded_integer!(primitive["y"], "#{path}.y", 0, 4096, "y")
        string!(primitive["text"], "#{path}.text", false, "text must be a string")
        color!(primitive["fill"], "#{path}.fill")
        bounded_integer!(primitive["fontSize"], "#{path}.fontSize", 1, 256, "fontSize")

        align =
          string!(primitive["align"], "#{path}.align", true, "align must be a non-empty string")

        require!(
          align in ["start", "middle", "end"],
          "#{path}.align",
          "align must be start, middle, or end"
        )

        false

      "image" ->
        exact_fields!(
          primitive,
          ~w(kind x y width height file),
          [],
          path,
          "image members must be exactly kind, x, y, width, height, and file"
        )

        bounded_integer!(primitive["x"], "#{path}.x", 0, 4096, "x")
        bounded_integer!(primitive["y"], "#{path}.y", 0, 4096, "y")
        bounded_integer!(primitive["width"], "#{path}.width", 1, 4096, "width")
        bounded_integer!(primitive["height"], "#{path}.height", 1, 4096, "height")
        file = string!(primitive["file"], "#{path}.file", true, "file must be a non-empty string")

        unless MapSet.member?(delivered_files, file) do
          names = Enum.map_join(delivered_file_names, ", ", &inspect/1)

          invalid!(
            "#{path}.file",
            "#{inspect(file)} is not one of this message's files (#{names}); attach it with the send or name an attached file"
          )
        end

        true

      _ ->
        invalid!("#{path}.kind", "primitive kind must be rect, circle, line, text, or image")
    end
  end

  defp parse_inputs!(value, illustration_ids) do
    inputs = array!(value, "inputs", "inputs must be an array")
    require!(length(inputs) <= 16, "inputs", "at most 16 inputs are allowed")

    Enum.reduce(Enum.with_index(inputs), {%{}, MapSet.new()}, fn {raw, index}, {kinds, refs} ->
      path = "inputs[#{index}]"
      input = object!(raw, path, "input must be an object")
      kind = required_string!(input, "kind", path, "input kind is required")

      {input_id, next_refs} =
        case kind do
          "response" ->
            exact_fields!(
              input,
              ~w(kind id label actions),
              [],
              path,
              "response input members must match its exact shape"
            )

            id = identifier!(input["id"], "#{path}.id")
            string!(input["label"], "#{path}.label", true, "label must be non-empty")
            actions = array!(input["actions"], "#{path}.actions", "actions must be an array")

            require!(
              actions != [],
              "#{path}.actions",
              "response input requires at least one action"
            )

            require!(length(actions) <= 64, "#{path}.actions", "at most 64 actions are allowed")
            action_refs = parse_actions!(actions, "#{path}.actions", illustration_ids)
            {id, MapSet.union(refs, action_refs)}

          "text" ->
            exact_fields!(
              input,
              ~w(kind id label),
              [],
              path,
              "text input members must match its exact shape"
            )

            id = identifier!(input["id"], "#{path}.id")
            string!(input["label"], "#{path}.label", true, "label must be non-empty")
            {id, refs}

          "point" ->
            exact_fields!(
              input,
              ~w(kind id label illustration),
              [],
              path,
              "point input members must match its exact shape"
            )

            id = identifier!(input["id"], "#{path}.id")
            string!(input["label"], "#{path}.label", true, "label must be non-empty")
            illustration = identifier!(input["illustration"], "#{path}.illustration")

            require!(
              MapSet.member?(illustration_ids, illustration),
              "#{path}.illustration",
              "point input illustration must name a declared illustration"
            )

            {id, MapSet.put(refs, illustration)}

          _ ->
            invalid!("#{path}.kind", "input kind must be response, text, or point")
        end

      require!(!Map.has_key?(kinds, input_id), "#{path}.id", "input identifiers must be unique")
      {Map.put(kinds, input_id, kind), next_refs}
    end)
  end

  defp parse_actions!(actions, path, illustration_ids) do
    actions
    |> Enum.with_index()
    |> Enum.reduce({MapSet.new(), MapSet.new()}, fn {raw, index}, {seen, refs} ->
      action_path = "#{path}[#{index}]"
      action = object!(raw, action_path, "action must be an object")
      kind = required_string!(action, "kind", action_path, "action kind is required")

      exact_fields!(
        action,
        ~w(id label kind),
        ~w(value illustration),
        action_path,
        "action members must match its exact shape"
      )

      id = identifier!(action["id"], "#{action_path}.id")

      require!(
        !MapSet.member?(seen, id),
        "#{action_path}.id",
        "action identifiers within a response must be unique"
      )

      string!(action["label"], "#{action_path}.label", true, "action label must be non-empty")

      case kind do
        "value" ->
          require!(
            Map.has_key?(action, "value"),
            "#{action_path}.value",
            "a value action must include value"
          )

        "text" ->
          require!(
            !Map.has_key?(action, "value"),
            "#{action_path}.value",
            "a text action cannot include value"
          )

        _ ->
          invalid!("#{action_path}.kind", "action kind must be value or text")
      end

      next_refs =
        case Map.fetch(action, "illustration") do
          :error ->
            refs

          {:ok, raw_illustration} ->
            illustration = identifier!(raw_illustration, "#{action_path}.illustration")

            require!(
              MapSet.member?(illustration_ids, illustration),
              "#{action_path}.illustration",
              "action illustration must name a declared illustration"
            )

            MapSet.put(refs, illustration)
        end

      {MapSet.put(seen, id), next_refs}
    end)
    |> elem(1)
  end

  defp parse_resolved_by_state!(value, input_kinds, illustration_ids, state) do
    states = object!(value, "resolvedByState", "resolvedByState must be an object")

    Enum.reduce(states, state, fn {key, scene}, current ->
      require!(
        key in @terminal_states,
        "resolvedByState.#{key}",
        "state key must be a known terminal state"
      )

      {_, next} =
        parse_scene!(
          scene,
          "resolvedByState.#{key}",
          1,
          true,
          false,
          false,
          input_kinds,
          illustration_ids,
          current
        )

      next
    end)
  end

  defp parse_scene!(
         raw,
         path,
         depth,
         terminal,
         result_branch,
         in_open,
         input_kinds,
         illustration_ids,
         state
       ) do
    require!(depth <= 16, path, "scene depth must be at most 16")
    node = object!(raw, path, "scene node must be an object")
    kind = required_string!(node, "kind", path, "scene kind is required")
    state = %{state | count: state.count + 1}
    require!(state.count <= 128, path, "at most 128 scene nodes are allowed")

    case kind do
      "stack" ->
        exact_fields!(
          node,
          ~w(kind axis children),
          [],
          path,
          "stack node members must match its exact shape"
        )

        axis = string!(node["axis"], "#{path}.axis", true, "axis must be a non-empty string")

        require!(
          axis in ["vertical", "horizontal"],
          "#{path}.axis",
          "axis must be vertical or horizontal"
        )

        children = array!(node["children"], "#{path}.children", "stack children must be an array")
        require!(children != [], "#{path}.children", "stack requires at least one child")

        parse_scene_children!(
          children,
          "#{path}.children",
          depth,
          terminal,
          result_branch,
          in_open,
          input_kinds,
          illustration_ids,
          state
        )

      "grid" ->
        exact_fields!(
          node,
          ~w(kind columns children),
          [],
          path,
          "grid node members must match its exact shape"
        )

        bounded_integer!(node["columns"], "#{path}.columns", 1, 4, "grid columns")
        children = array!(node["children"], "#{path}.children", "grid children must be an array")
        require!(children != [], "#{path}.children", "grid requires at least one child")

        parse_scene_children!(
          children,
          "#{path}.children",
          depth,
          terminal,
          result_branch,
          in_open,
          input_kinds,
          illustration_ids,
          state
        )

      "text" ->
        exact_fields!(
          node,
          ~w(kind text style),
          [],
          path,
          "text node members must match its exact shape"
        )

        string!(node["text"], "#{path}.text", false, "text must be a string")
        style = string!(node["style"], "#{path}.style", true, "style must be a non-empty string")

        require!(
          style in ["title", "body", "caption"],
          "#{path}.style",
          "style must be title, body, or caption"
        )

        {node, state}

      "illustration" ->
        exact_fields!(
          node,
          ~w(kind id),
          [],
          path,
          "illustration scene members must match its exact shape"
        )

        id = identifier!(node["id"], "#{path}.id")

        require!(
          MapSet.member?(illustration_ids, id),
          "#{path}.id",
          "illustration identifier must exist"
        )

        {node,
         %{state | referenced_illustrations: MapSet.put(state.referenced_illustrations, id)}}

      "input" ->
        exact_fields!(
          node,
          ~w(kind id layout),
          [],
          path,
          "input scene members must match its exact shape"
        )

        require!(!terminal, path, "input scene nodes are not allowed in terminal scenes")
        id = identifier!(node["id"], "#{path}.id")
        input_kind = Map.get(input_kinds, id)
        require!(!is_nil(input_kind), "#{path}.id", "input scene must name a declared input")

        layout =
          string!(node["layout"], "#{path}.layout", true, "layout must be a non-empty string")

        require!(
          compatible_layout?(input_kind, layout),
          "#{path}.layout",
          "input layout must be recognized and compatible with the input kind"
        )

        counts =
          if in_open,
            do: Map.update(state.open_input_counts, id, 1, &(&1 + 1)),
            else: state.open_input_counts

        {node, %{state | open_input_counts: counts}}

      "result-point" ->
        exact_fields!(
          node,
          ~w(kind id label path),
          [],
          path,
          "result-point members must match its exact shape"
        )

        require!(
          terminal and result_branch,
          path,
          "result-point is allowed only in the terminal result scene"
        )

        id = identifier!(node["id"], "#{path}.id")

        require!(
          MapSet.member?(illustration_ids, id),
          "#{path}.id",
          "illustration identifier must exist"
        )

        string!(node["label"], "#{path}.label", true, "label must be non-empty")
        path_values = array!(node["path"], "#{path}.path", "result-point path must be an array")

        require!(
          length(path_values) <= 16,
          "#{path}.path",
          "result-point path permits at most 16 components"
        )

        Enum.with_index(path_values)
        |> Enum.each(fn {component, index} ->
          component_path = "#{path}.path[#{index}]"

          cond do
            is_binary(component) ->
              :ok

            is_integer(component) and component >= 0 and component <= 65_535 ->
              :ok

            is_integer(component) ->
              invalid!(component_path, "integer path index must be from 0 through 65535")

            true ->
              invalid!(component_path, "path component must be a string or integer index")
          end
        end)

        {node,
         %{
           state
           | uses_result_point: true,
             referenced_illustrations: MapSet.put(state.referenced_illustrations, id)
         }}

      _ ->
        invalid!(
          "#{path}.kind",
          "scene kind must be stack, grid, text, illustration, input, or result-point"
        )
    end
  end

  defp parse_scene_children!(
         children,
         path,
         depth,
         terminal,
         result_branch,
         in_open,
         input_kinds,
         illustration_ids,
         state
       ) do
    children
    |> Enum.with_index()
    |> Enum.reduce({[], state}, fn {child, index}, {parsed, current} ->
      {node, next} =
        parse_scene!(
          child,
          "#{path}[#{index}]",
          depth + 1,
          terminal,
          result_branch,
          in_open,
          input_kinds,
          illustration_ids,
          current
        )

      {[node | parsed], next}
    end)
    |> then(fn {nodes, next} -> {Enum.reverse(nodes), next} end)
  end

  defp parse_constructor!(raw, path, depth, input_kinds, count, refs) do
    require!(depth <= 16, path, "result constructor depth must be at most 16")
    node = object!(raw, path, "result constructor must be an object")
    kind = required_string!(node, "kind", path, "result constructor kind is required")
    count = count + 1
    require!(count <= 128, path, "at most 128 result constructor nodes are allowed")

    case kind do
      "literal" ->
        exact_fields!(
          node,
          ~w(kind value),
          [],
          path,
          "literal constructor members must match its exact shape"
        )

        {refs, count}

      "input" ->
        exact_fields!(
          node,
          ~w(kind id),
          [],
          path,
          "input constructor members must match its exact shape"
        )

        id = identifier!(node["id"], "#{path}.id")

        require!(
          Map.has_key?(input_kinds, id),
          "#{path}.id",
          "input constructor must name a declared input"
        )

        {MapSet.put(refs, id), count}

      "array" ->
        exact_fields!(
          node,
          ~w(kind items),
          [],
          path,
          "array constructor members must match its exact shape"
        )

        items = array!(node["items"], "#{path}.items", "array constructor items must be an array")
        require!(length(items) <= 128, "#{path}.items", "at most 128 array items are allowed")
        parse_constructor_children!(items, "#{path}.items", depth, input_kinds, count, refs)

      "object" ->
        exact_fields!(
          node,
          ~w(kind members),
          [],
          path,
          "object constructor members must match its exact shape"
        )

        members =
          array!(
            node["members"],
            "#{path}.members",
            "object constructor members must be an array"
          )

        require!(
          length(members) <= 128,
          "#{path}.members",
          "at most 128 object members are allowed"
        )

        {refs, count, _seen} =
          members
          |> Enum.with_index()
          |> Enum.reduce({refs, count, MapSet.new()}, fn {raw_member, index},
                                                         {current_refs, current_count, seen} ->
            member_path = "#{path}.members[#{index}]"

            member =
              object!(raw_member, member_path, "object constructor member must be an object")

            exact_fields!(
              member,
              ~w(name value),
              [],
              member_path,
              "object constructor member members must match its exact shape"
            )

            name =
              string!(
                member["name"],
                "#{member_path}.name",
                false,
                "object member name must be a string"
              )

            require!(
              !MapSet.member?(seen, name),
              "#{member_path}.name",
              "object member names must be unique"
            )

            {next_refs, next_count} =
              parse_constructor!(
                member["value"],
                "#{member_path}.value",
                depth + 1,
                input_kinds,
                current_count,
                current_refs
              )

            {next_refs, next_count, MapSet.put(seen, name)}
          end)

        {refs, count}

      _ ->
        invalid!(
          "#{path}.kind",
          "result constructor kind must be literal, input, array, or object"
        )
    end
  end

  defp parse_constructor_children!(items, path, depth, input_kinds, count, refs) do
    items
    |> Enum.with_index()
    |> Enum.reduce({refs, count}, fn {item, index}, {current_refs, current_count} ->
      parse_constructor!(
        item,
        "#{path}[#{index}]",
        depth + 1,
        input_kinds,
        current_count,
        current_refs
      )
    end)
  end

  defp capabilities(input_kinds, illustration_ids, contains_image, uses_result_point) do
    base = MapSet.new(["scene/1", "result/1"])

    base =
      if MapSet.size(illustration_ids) > 0, do: MapSet.put(base, "illustration/1"), else: base

    base =
      if Enum.any?(input_kinds, fn {_id, kind} -> kind == "response" end),
        do: MapSet.put(base, "response/1"),
        else: base

    base =
      if Enum.any?(input_kinds, fn {_id, kind} -> kind == "text" end),
        do: MapSet.put(base, "text/1"),
        else: base

    base =
      if Enum.any?(input_kinds, fn {_id, kind} -> kind == "point" end),
        do: MapSet.put(base, "point/1"),
        else: base

    base = if uses_result_point, do: MapSet.put(base, "result-point/1"), else: base
    if contains_image, do: MapSet.put(base, "image/1"), else: base
  end

  defp compatible_layout?("response", layout), do: layout in ["cards", "list"]
  defp compatible_layout?("text", "editor"), do: true
  defp compatible_layout?("point", "diagram"), do: true
  defp compatible_layout?(_, _), do: false

  defp object!(value, _path, _message) when is_map(value) and not is_struct(value), do: value
  defp object!(_value, path, message), do: invalid!(path, message)

  defp exact_fields!(object, required, optional, path, mismatch_message) do
    keys = Map.keys(object) |> Enum.reject(&(&1 == @duplicate_keys))
    string_keys = Enum.filter(keys, &is_binary/1)
    actual = MapSet.new(string_keys)
    required_set = MapSet.new(required)
    allowed = MapSet.union(required_set, MapSet.new(optional))

    missing = MapSet.difference(required_set, actual) |> MapSet.to_list() |> Enum.sort()
    extra = MapSet.difference(actual, allowed) |> MapSet.to_list() |> Enum.sort()

    cond do
      missing != [] ->
        field = hd(missing)
        invalid!(child_path(path, field), "required member #{inspect(field)} is missing; add it")

      extra != [] ->
        invalid!(path, "#{mismatch_message}; remove unsupported member #{inspect(hd(extra))}")

      length(string_keys) != length(keys) ->
        invalid!(path, mismatch_message)

      true ->
        :ok
    end
  end

  defp reject_duplicate_keys!(value, path) when is_map(value) and not is_struct(value) do
    case Map.get(value, @duplicate_keys, []) do
      [key | _] ->
        invalid!(
          child_path(path, key),
          "object member names must be unique; remove the duplicate member"
        )

      _ ->
        :ok
    end

    value
    |> Enum.reject(fn {key, _} -> key == @duplicate_keys end)
    |> Enum.sort_by(fn {key, _} -> to_string(key) end)
    |> Enum.each(fn {key, child} -> reject_duplicate_keys!(child, child_path(path, key)) end)
  end

  defp reject_duplicate_keys!(value, path) when is_list(value) do
    value
    |> Enum.with_index()
    |> Enum.each(fn {child, index} -> reject_duplicate_keys!(child, "#{path}[#{index}]") end)
  end

  defp reject_duplicate_keys!(_value, _path), do: :ok

  defp child_path("$", key), do: to_string(key)
  defp child_path(path, key), do: "#{path}.#{key}"

  defp string_array!(value, path, nonempty, max_count, failure) do
    values = array!(value, path, failure)
    require!(!nonempty or values != [], path, "requires must contain the capabilities used")
    require!(length(values) <= max_count, path, "requires permits at most eight capabilities")

    values
    |> Enum.with_index()
    |> Enum.map(fn {item, index} ->
      string!(item, "#{path}[#{index}]", true, "each capability must be a non-empty string")
    end)
  end

  defp unique_array!(values, path, message) do
    Enum.with_index(values)
    |> Enum.reduce(MapSet.new(), fn {value, index}, seen ->
      require!(!MapSet.member?(seen, value), "#{path}[#{index}]", message)
      MapSet.put(seen, value)
    end)
  end

  defp array!(value, _path, _failure) when is_list(value), do: value
  defp array!(_value, path, failure), do: invalid!(path, failure)

  defp required_string!(object, key, parent, missing_message) do
    unless Map.has_key?(object, key), do: invalid!(child_path(parent, key), missing_message)
    string!(object[key], child_path(parent, key), true, missing_message)
  end

  defp identifier!(value, path) do
    string = string!(value, path, true, "identifier must be non-empty")
    require!(byte_size(string) <= 128, path, "identifier is at most 128 UTF-8 bytes")
    string
  end

  defp string!(value, path, nonempty, message) when is_binary(value) do
    if nonempty and value == "", do: invalid!(path, message)
    value
  end

  defp string!(_value, path, _nonempty, message), do: invalid!(path, message)

  defp bounded_integer!(value, path, minimum, maximum, name) do
    cond do
      is_integer(value) and value < minimum ->
        invalid!(path, "#{name} must be an integer from #{minimum} through #{maximum}")

      is_integer(value) and value > maximum ->
        invalid!(
          path,
          "#{name} is outside its permitted integer range #{minimum} through #{maximum}"
        )

      is_integer(value) ->
        :ok

      true ->
        invalid!(path, "#{name} must be an integer")
    end
  end

  defp color!(value, path) do
    color = string!(value, path, true, "color must be # followed by six hexadecimal digits")

    require!(
      byte_size(color) == 7 and Regex.match?(~r/\A#[0-9A-Fa-f]{6}\z/, color),
      path,
      "color must be # followed by six hexadecimal digits"
    )
  end

  defp capability_fix([], []), do: "remove duplicate or unused entries"
  defp capability_fix([missing | _], []), do: "add #{inspect(missing)}"
  defp capability_fix([], [extra | _]), do: "remove #{inspect(extra)}"

  defp capability_fix([missing | _], [extra | _]),
    do: "add #{inspect(missing)} and remove #{inspect(extra)}"

  defp require!(true, _path, _reason), do: :ok
  defp require!(false, path, reason), do: invalid!(path, reason)

  defp invalid!(path, reason), do: throw({:invalid, path, reason})
end
