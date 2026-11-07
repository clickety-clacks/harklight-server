defmodule AttentionServer.BouncePolicy do
  @moduledoc false

  alias AttentionServer.JSON.DecimalToken

  @milliseconds_per_day 86_400_000
  @level_fields ["name", "lower_bound", "bounce_after_days"]

  def normalize_levels(levels) when is_list(levels) and levels != [] do
    with {:ok, normalized} <- normalize_rows(levels),
         true <- starts_at_zero?(normalized),
         true <- strictly_increasing?(normalized) do
      {:ok, normalized}
    else
      _ -> {:error, :invalid_levels}
    end
  end

  def normalize_levels(_), do: {:error, :invalid_levels}

  def public_levels(levels) when is_list(levels) do
    Enum.map(levels, fn level ->
      %{
        "name" => level["name"],
        "lower_bound" => level["lower_bound"],
        "bounce_after_days" => level["bounce_after_days"]
      }
    end)
  end

  def valid_normalized_levels?(levels) when is_list(levels) do
    with true <- Enum.all?(levels, &(is_map(&1) and not is_struct(&1))),
         {:ok, normalized} <- normalize_levels(public_levels(levels)) do
      equivalent_levels?(levels, normalized) and
        Enum.all?(levels, fn level ->
          Map.keys(level) |> Enum.sort() ==
            ["bounce_after_days", "interval_ms", "lower_bound", "lower_ratio", "name"]
        end)
    else
      _ -> false
    end
  end

  def valid_normalized_levels?(_), do: false

  def equivalent_levels?(left, right) when is_list(left) and is_list(right) do
    length(left) == length(right) and
      Enum.all?(Enum.zip(left, right), fn {a, b} ->
        a["name"] == b["name"] and a["lower_ratio"] == b["lower_ratio"] and
          equivalent_days?(a["bounce_after_days"], b["bounce_after_days"])
      end)
  end

  def equivalent_levels?(_, _), do: false

  defp equivalent_days?(nil, nil), do: true

  defp equivalent_days?(left, right) do
    with {:ok, left_ratio} <- number_ratio(left),
         {:ok, right_ratio} <- number_ratio(right) do
      left_ratio == right_ratio
    else
      _ -> false
    end
  end

  def select_level(levels, score) when is_list(levels) do
    with {:ok, score_ratio} <- number_ratio(score),
         true <- in_unit_interval?(score_ratio) do
      levels
      |> Enum.filter(&(compare_ratios(&1["lower_ratio"], score_ratio) != :gt))
      |> List.last()
      |> case do
        nil -> {:error, :invalid_score}
        level -> {:ok, level}
      end
    else
      _ -> {:error, :invalid_score}
    end
  end

  def select_level(_, _), do: {:error, :invalid_score}

  def substantive_content(message, files) when is_map(message) and is_list(files) do
    {canonical_value(message),
     files
     |> Enum.map(&Map.take(&1, ["name", "size", "sha256"]))
     |> canonical_value()}
  end

  def same_substance?(old_message, old_files, new_message, new_files) do
    substantive_content(old_message, old_files) == substantive_content(new_message, new_files)
  end

  defp canonical_value(%DecimalToken{} = value) do
    case number_ratio(value) do
      {:ok, ratio} -> {:number, ratio}
      _ -> value
    end
  end

  defp canonical_value(value) when is_integer(value) or is_float(value) do
    case number_ratio(value) do
      {:ok, ratio} -> {:number, ratio}
      _ -> value
    end
  end

  defp canonical_value(value) when is_map(value) do
    Map.new(value, fn {key, item} -> {key, canonical_value(item)} end)
  end

  defp canonical_value(value) when is_list(value), do: Enum.map(value, &canonical_value/1)
  defp canonical_value(value), do: value

  def interval_ms(nil), do: {:ok, nil}

  def interval_ms(days) do
    with {:ok, {numerator, denominator}} <- number_ratio(days),
         true <- numerator > 0 do
      product = numerator * @milliseconds_per_day
      quotient = div(product, denominator)
      remainder = rem(product, denominator)
      {:ok, quotient + if(remainder == 0, do: 0, else: 1)}
    else
      _ -> {:error, :invalid_interval}
    end
  end

  def number_ratio(value) when is_integer(value), do: {:ok, {value, 1}}

  def number_ratio(%DecimalToken{raw: raw}), do: decimal_ratio(raw)

  def number_ratio(value) when is_float(value) do
    value
    |> :erlang.float_to_binary([:short])
    |> decimal_ratio()
  rescue
    _ -> {:error, :invalid_number}
  end

  def number_ratio(_), do: {:error, :invalid_number}

  defp normalize_rows(rows) do
    rows
    |> Enum.reduce_while({:ok, []}, fn row, {:ok, acc} ->
      case normalize_row(row) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      error -> error
    end
  end

  defp normalize_row(row) when is_map(row) and not is_struct(row) do
    with true <- Enum.sort(Map.keys(row)) == Enum.sort(@level_fields),
         name when is_binary(name) <- row["name"],
         true <- String.valid?(name) and String.trim(name) != "",
         {:ok, lower_ratio} <- number_ratio(row["lower_bound"]),
         true <- in_unit_interval?(lower_ratio),
         {:ok, interval_ms} <- interval_ms(row["bounce_after_days"]) do
      {:ok,
       %{
         "name" => name,
         "lower_bound" => row["lower_bound"],
         "lower_ratio" => lower_ratio,
         "bounce_after_days" => row["bounce_after_days"],
         "interval_ms" => interval_ms
       }}
    else
      _ -> {:error, :invalid_level}
    end
  end

  defp normalize_row(_), do: {:error, :invalid_level}

  defp starts_at_zero?([%{"lower_ratio" => {0, _}} | _]), do: true
  defp starts_at_zero?(_), do: false

  defp strictly_increasing?([_]), do: true

  defp strictly_increasing?([first, second | rest]) do
    compare_ratios(first["lower_ratio"], second["lower_ratio"]) == :lt and
      strictly_increasing?([second | rest])
  end

  defp strictly_increasing?(_), do: false

  defp in_unit_interval?({numerator, denominator}) do
    compare_ratios({0, 1}, {numerator, denominator}) != :gt and
      compare_ratios({numerator, denominator}, {1, 1}) != :gt
  end

  defp compare_ratios({left_n, left_d}, {right_n, right_d}) do
    case left_n * right_d - right_n * left_d do
      value when value < 0 -> :lt
      0 -> :eq
      _ -> :gt
    end
  end

  defp decimal_ratio(raw) when is_binary(raw) do
    pattern =
      ~r/\A(?<sign>-?)(?<whole>0|[1-9][0-9]*)(?:\.(?<fraction>[0-9]+))?(?:[eE](?<exponent>[+-]?[0-9]+))?\z/

    with %{"sign" => sign, "whole" => whole, "fraction" => fraction, "exponent" => exponent} <-
           Regex.named_captures(pattern, raw),
         {power, ""} <- Integer.parse(if(exponent == "", do: "0", else: exponent)) do
      fraction = fraction || ""
      digits = String.to_integer(whole <> fraction)
      numerator = if sign == "-", do: -digits, else: digits
      scale = String.length(fraction) - power

      if scale >= 0 do
        {:ok, reduce_ratio(numerator, Integer.pow(10, scale))}
      else
        {:ok, reduce_ratio(numerator * Integer.pow(10, -scale), 1)}
      end
    else
      _ -> {:error, :invalid_number}
    end
  rescue
    _ -> {:error, :invalid_number}
  end

  defp decimal_ratio(_), do: {:error, :invalid_number}

  defp reduce_ratio(0, _denominator), do: {0, 1}

  defp reduce_ratio(numerator, denominator) do
    divisor = gcd(abs(numerator), denominator)
    {div(numerator, divisor), div(denominator, divisor)}
  end

  defp gcd(value, 0), do: value
  defp gcd(left, right), do: gcd(right, rem(left, right))
end
