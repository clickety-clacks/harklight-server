# A strict validator for the JSON Schema subset docs/client-api-v2.md uses, for
# tests. Members a schema does not declare are errors here, so a rendered
# resource carries exactly the contract's fields and nothing internal.
defmodule AttentionServer.ClientAPISchema do
  @moduledoc false

  def defs(path) do
    ~r/```json\n(.*?)```/s
    |> Regex.scan(File.read!(path), capture: :all_but_first)
    |> Enum.reduce(%{}, fn [block], acc ->
      # Schema blocks hold $defs; other JSON blocks are examples.
      case AttentionServer.JSON.decode_line(compact(block) <> "\n") do
        {:ok, %{"$defs" => defs}} -> Map.merge(acc, defs)
        _ -> acc
      end
    end)
  end

  defp compact(block), do: block |> String.split("\n") |> Enum.map_join(" ", &String.trim/1)

  @doc "The contract's defs, from docs/client-api-v2.md."
  def contract_defs(path), do: defs(path)

  def errors(defs, name, value), do: check(defs, Map.fetch!(defs, name), value, name)

  defp check(defs, %{"$ref" => "#/$defs/" <> name}, value, at),
    do: check(defs, Map.fetch!(defs, name), value, at)

  defp check(defs, %{"oneOf" => options}, value, at) do
    matches = Enum.count(options, &(check(defs, &1, value, at) == []))
    if matches == 1, do: [], else: ["#{at}: matches #{matches} of oneOf"]
  end

  defp check(defs, schema, value, at) do
    known =
      ~w(type required properties items enum const minimum maximum exclusiveMinimum minLength maxLength pattern description additionalProperties)

    unknown = Map.keys(schema) -- known
    if unknown != [], do: raise("unsupported schema keywords #{inspect(unknown)} at #{at}")

    type_errors(schema, value, at) ++
      enum_errors(schema, value, at) ++
      const_errors(schema, value, at) ++
      bound_errors(schema, value, at) ++
      object_errors(defs, schema, value, at) ++
      array_errors(defs, schema, value, at)
  end

  defp type_errors(%{"type" => type}, value, at) do
    ok =
      case type do
        "object" -> is_map(value) and not is_struct(value)
        "array" -> is_list(value)
        "string" -> is_binary(value)
        "integer" -> is_integer(value)
        "number" -> is_number(value)
        "boolean" -> is_boolean(value)
        "null" -> is_nil(value)
      end

    if ok, do: [], else: ["#{at}: not #{type}: #{inspect(value)}"]
  end

  defp type_errors(_schema, _value, _at), do: []

  defp enum_errors(%{"enum" => values}, value, at),
    do: if(value in values, do: [], else: ["#{at}: #{inspect(value)} not in enum"])

  defp enum_errors(_schema, _value, _at), do: []

  defp const_errors(%{"const" => expected}, value, at),
    do:
      if(value == expected,
        do: [],
        else: ["#{at}: #{inspect(value)} is not #{inspect(expected)}"]
      )

  defp const_errors(_schema, _value, _at), do: []

  defp bound_errors(schema, value, at) do
    [
      {"minimum", &(is_number(value) and value < &1)},
      {"maximum", &(is_number(value) and value > &1)},
      {"exclusiveMinimum", &(is_number(value) and value <= &1)},
      {"minLength", &(is_binary(value) and String.length(value) < &1)},
      {"maxLength", &(is_binary(value) and String.length(value) > &1)},
      {"pattern", &(is_binary(value) and not Regex.match?(Regex.compile!(&1), value))}
    ]
    |> Enum.flat_map(fn {keyword, violates} ->
      case Map.fetch(schema, keyword) do
        {:ok, bound} -> if violates.(bound), do: ["#{at}: violates #{keyword}"], else: []
        :error -> []
      end
    end)
  end

  defp object_errors(defs, %{"type" => "object"} = schema, value, at) when is_map(value) do
    properties = Map.get(schema, "properties")

    missing =
      for field <- Map.get(schema, "required", []),
          not Map.has_key?(value, field),
          do: "#{at}: missing #{field}"

    # An object schema without properties (message content) is passed through.
    undeclared =
      if is_map(properties),
        do:
          for(
            field <- Map.keys(value) -- Map.keys(properties),
            do: "#{at}: undeclared #{field}"
          ),
        else: []

    nested =
      for {field, child} <- value,
          sub = is_map(properties) && Map.get(properties, field),
          is_map(sub),
          error <- check(defs, sub, child, "#{at}.#{field}"),
          do: error

    missing ++ undeclared ++ nested
  end

  defp object_errors(_defs, _schema, _value, _at), do: []

  defp array_errors(defs, %{"type" => "array", "items" => item}, value, at)
       when is_list(value) do
    value
    |> Enum.with_index()
    |> Enum.flat_map(fn {element, index} -> check(defs, item, element, "#{at}[#{index}]") end)
  end

  defp array_errors(_defs, _schema, _value, _at), do: []
end
