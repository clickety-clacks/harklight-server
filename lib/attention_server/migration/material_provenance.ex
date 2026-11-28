defmodule AttentionServer.Migration.MaterialProvenance do
  @moduledoc false
  alias AttentionServer.Migration.Source

  @tls %{"cert" => "ATTENTION_SERVER_CERTFILE", "key" => "ATTENTION_SERVER_KEYFILE",
    "ca" => "ATTENTION_SERVER_CACERTFILE"}

  # Runs between the stopped-source observations. No shell evaluation or SQLite
  # access: the configured paths and the exact copied bytes are the TLS anchor.
  def capture(material) when is_map(material) do
    Enum.reduce_while(material, {:ok, %{}}, fn {name, path}, {:ok, files} ->
      case Source.file_observation(path) do
        {:ok, observed} -> {:cont, {:ok, Map.put(files, "material/" <> name,
          %{"path" => Path.expand(path), "before" => observed})}}
        error -> {:halt, error}
      end
    end)
  end

  def confirm(files, root, material) do
    result = Enum.reduce_while(files, {:ok, %{}}, fn {name, entry}, {:ok, acc} ->
      before = entry["before"]
      case Source.file_observation(entry["path"]) do
        {:ok, ^before} ->
          observed = before
          {:cont, {:ok, Map.put(acc, name, Map.put(entry, "after", observed))}}
        _ -> {:halt, {:error, :source_material_changed}}
      end
    end)
    with {:ok, observed} <- result,
         receipt = %{"version" => 1, "anchor" => "stopped_source_configuration", "files" => observed},
         :ok <- verify(root, material, receipt) do
      {:ok, receipt}
    end
  end

  def verify(root, material, %{"version" => 1, "anchor" => "stopped_source_configuration", "files" => files}) do
    valid = Enum.sort(Map.keys(files)) == Enum.sort(Map.keys(material)) and
      Enum.all?(files, fn {name, entry} ->
        before = entry["before"]
        is_map(before) and before == entry["after"] and is_binary(entry["path"]) and
          Path.type(entry["path"]) == :absolute and
          Map.take(before, [:size, :sha256]) == Map.take(Map.fetch!(material, name), [:size, :sha256])
      end)
    with true <- valid,
         {:ok, _} <- tls_paths(root, material, files) do
      :ok
    else
      false -> {:error, :material_provenance_mismatch}
      error -> error
    end
  rescue
    _ -> {:error, :material_provenance_mismatch}
  end
  def verify(_, _, _), do: {:error, :material_provenance_required}

  def tls_paths(root, material, files) do
    if Map.has_key?(material, "material/server.env") do
      with {:ok, bytes} <- File.read(Path.join(root, "material/server.env")),
           {:ok, configured} <- configuration(bytes) do
        Enum.reduce_while(@tls, {:ok, %{}}, fn {role, variable}, {:ok, acc} ->
          configured_path = configured[variable]
          matches = Enum.filter(files, fn {_, entry} -> entry["path"] == configured_path end)
          case matches do
            [{name, _}] when is_binary(configured_path) -> {:cont, {:ok, Map.put(acc, role, name)}}
            _ -> {:halt, {:error, :configured_tls_material_missing}}
          end
        end)
      end
    else
      {:ok, %{}}
    end
  end

  # Accept literal shell assignments produced by the installer, including %q
  # backslash quoting and single/double quotes. Refuse expansion; never source it.
  def configuration(bytes) do
    Enum.reduce_while(String.split(bytes, "\n"), {:ok, %{}}, fn line, {:ok, acc} ->
      case Regex.run(~r/^\s*(?:export\s+)?(ATTENTION_SERVER_(?:CERTFILE|KEYFILE|CACERTFILE))=(.*)$/, line) do
        [_, key, value] ->
          with {:ok, path} <- literal(String.trim(value), :plain, []),
               true <- Path.type(path) == :absolute and not Map.has_key?(acc, key) do
            {:cont, {:ok, Map.put(acc, key, Path.expand(path))}}
          else
            _ -> {:halt, {:error, :unsupported_tls_configuration}}
          end
        _ -> {:cont, {:ok, acc}}
      end
    end)
  end

  defp literal("", :plain, acc), do: {:ok, acc |> Enum.reverse() |> IO.iodata_to_binary()}
  defp literal(<<"\\", c::utf8, rest::binary>>, mode, acc) when mode == :plain or (mode == :double and c in [?$, ?`, ?", ?\\]),
    do: literal(rest, mode, [<<c::utf8>> | acc])
  defp literal(<<"\\", _::binary>>, :double, _), do: {:error, :unsupported_tls_configuration}
  defp literal(<<"'", rest::binary>>, :plain, acc), do: literal(rest, :single, acc)
  defp literal(<<"'", rest::binary>>, :single, acc), do: literal(rest, :plain, acc)
  defp literal(<<"\"", rest::binary>>, :plain, acc), do: literal(rest, :double, acc)
  defp literal(<<"\"", rest::binary>>, :double, acc), do: literal(rest, :plain, acc)
  defp literal(<<c::utf8, rest::binary>>, :single, acc), do: literal(rest, :single, [<<c::utf8>> | acc])
  defp literal(<<c::utf8, _::binary>>, mode, _) when c in [?$ , ?`, ?\n, ?\r, 0] or
    (mode == :plain and c in [?\s, ?\t, ?;, ?&, ?|, ?<, ?>, ?(, ?), ?#, ?*, ??, ?[, ?], ?~]),
    do: {:error, :unsupported_tls_configuration}
  defp literal(<<c::utf8, rest::binary>>, mode, acc), do: literal(rest, mode, [<<c::utf8>> | acc])
  defp literal(_, _, _), do: {:error, :unsupported_tls_configuration}
end
