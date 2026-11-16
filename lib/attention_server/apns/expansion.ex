defmodule AttentionServer.APNS.Expansion do
  @moduledoc false

  alias AttentionServer.{JSON, LargeData}

  @limit 4_096
  @single "harklight.expanded.single.v1"
  @batch "harklight.expanded.batch.v1"

  # Run only after legacy fitting and server insertion. Nothing here may refit
  # the collapsed alert or remove any of its routing fields to make space.
  def add(payload, claim) do
    expand(payload, claim)
  rescue
    # Expansion is optional. A content/encoding failure must not turn an
    # otherwise valid legacy notification into a failed provider request.
    _ -> payload
  end

  defp expand(payload, %{presentation: :alert, expanded: :single}) do
    candidate = put_in(payload, ["aps", "category"], @single)
    if fits?(candidate), do: candidate, else: payload
  end

  defp expand(payload, %{
         presentation: :alert,
         expanded: %{titles: titles, total_count: total}
       }) do
    candidate =
      payload
      |> put_in(["aps", "category"], @batch)
      |> put_in(["attention", "expanded"], %{
        "v" => 1,
        "titles" => [],
        "total_count" => total
      })

    if fits?(candidate) do
      Enum.reduce_while(titles, candidate, fn title, current ->
        shown = current["attention"]["expanded"]["titles"]
        # JSON quotes and a possible separating comma are unavoidable. The raw
        # size is a lower bound, not a substitute for final escaped JSON sizing.
        available =
          @limit - byte_size(JSON.encode!(current)) - 2 - if(shown == [], do: 0, else: 1)

        with {:ok, value} <- whole_title(title, available),
             next = put_in(current, ["attention", "expanded", "titles"], shown ++ [value]),
             true <- fits?(next) do
          {:cont, next}
        else
          _ -> {:halt, current}
        end
      end)
    else
      payload
    end
  end

  defp expand(payload, _claim), do: payload

  defp fits?(payload), do: byte_size(JSON.encode!(payload)) <= @limit

  defp whole_title(title, available) when is_binary(title) and byte_size(title) <= available,
    do: {:ok, title}

  # A normal externalized title is much larger than APNs can carry. Do not open
  # it or pass the ref to JSON.encode!, which would read the complete file.
  # Small references are read only here, in the dispatcher, never in Relay.
  defp whole_title(%LargeData.Ref{size: size} = ref, available)
       when is_integer(size) and size >= 0 and size <= available do
    with path when is_binary(path) <- ref.path,
         {:ok, file} <- File.open(path, [:read, :binary]) do
      try do
        case IO.binread(file, size + 1) do
          bytes when is_binary(bytes) and byte_size(bytes) == size ->
            if String.valid?(bytes) and
                 Base.encode16(:crypto.hash(:sha256, bytes), case: :lower) == ref.sha256,
               do: {:ok, bytes},
               else: :unavailable

          _ ->
            :unavailable
        end
      after
        File.close(file)
      end
    else
      _ -> :unavailable
    end
  end

  defp whole_title(_title, _available), do: :unavailable
end
