defmodule AttentionServer.LargeData.Ref do
  @moduledoc false
  @enforce_keys [:id, :size, :sha256]
  defstruct [:id, :size, :sha256, :path]
end

defmodule AttentionServer.LargeData do
  @moduledoc false

  alias AttentionServer.LargeData.Ref

  @inline_limit 2 * 1024 * 1024
  @chunk_size 64 * 1024
  @persisted_ref_prefix "[Full content requires a newer Harklight Server. sha256="

  def inline_limit, do: @inline_limit
  def ref?(%Ref{}), do: true
  def ref?(_), do: false

  def contains_ref?(%Ref{}), do: true
  def contains_ref?(value) when is_map(value), do: Enum.any?(Map.values(value), &contains_ref?/1)
  def contains_ref?(value) when is_list(value), do: Enum.any?(value, &contains_ref?/1)

  def contains_ref?(value) when is_tuple(value),
    do: value |> Tuple.to_list() |> Enum.any?(&contains_ref?/1)

  def contains_ref?(_), do: false

  @doc false
  def externalize(term, nil), do: {:ok, term, []}

  def externalize(term, directory) when is_binary(directory) do
    with :ok <- File.mkdir_p(directory),
         :ok <- File.chmod(directory, 0o700) do
      case transform(term, directory, []) do
        {:ok, next, created} ->
          {:ok, next, Enum.uniq(created)}

        {:error, reason, created} ->
          cleanup_created(created)
          {:error, reason}

        error ->
          error
      end
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  @doc false
  def persist_refs(term), do: walk_refs(term, &reference_marker/1)

  @doc false
  def persisted_references(term) do
    term
    |> collect_reference_rows([], %{})
    |> Map.values()
    |> Enum.sort_by(&hd/1)
  end

  @doc false
  def attach_paths(term, directory, rows) when is_binary(directory) and is_list(rows) do
    with {:ok, references} <- references_by_path(rows, directory) do
      engine =
        term
        |> attach_reference_markers([], references)
        |> walk_refs(&%{&1 | path: Path.join(directory, &1.id)})

      {:ok, engine}
    end
  end

  @doc false
  def refs(term) do
    collect_refs(term, %{})
  end

  @doc false
  def valid_refs?(term) do
    Enum.all?(refs(term), fn {id, ref} ->
      is_binary(id) and id == ref.id and valid_ref_metadata?(ref)
    end)
  end

  @doc false
  def verify(ref) do
    with true <- valid_ref?(ref),
         {:ok, %{size: size}} <- File.stat(ref.path),
         true <- size == ref.size,
         {:ok, hash} <- hash_file(ref.path),
         true <- hash == ref.sha256 do
      :ok
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  @doc false
  def verify_all(term) do
    term
    |> refs()
    |> Enum.reduce_while(:ok, fn {_id, ref}, :ok ->
      case verify(ref) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  @doc false
  def prune(directory, term) when is_binary(directory) do
    references = refs(term)

    with :ok <- verify_all(term),
         {:ok, names} <- File.ls(directory) do
      names
      |> Enum.filter(fn name ->
        String.ends_with?(name, ".staged") or
          (Regex.match?(~r/\A[0-9a-f]{64}\z/, name) and not Map.has_key?(references, name))
      end)
      |> Enum.reduce_while(:ok, fn name, :ok ->
        case File.rm(Path.join(directory, name)) do
          :ok -> {:cont, :ok}
          {:error, :enoent} -> {:cont, :ok}
          _ -> {:halt, {:error, :storage_unavailable}}
        end
      end)
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  @doc false
  def delete_unreferenced(directory, old_term, next_term) when is_binary(directory) do
    retained = refs(next_term)

    refs(old_term)
    |> Enum.reject(fn {id, _ref} -> Map.has_key?(retained, id) end)
    |> Enum.reduce_while(:ok, fn {id, _ref}, :ok ->
      if Regex.match?(~r/\A[0-9a-f]{64}\z/, id) do
        case File.rm(Path.join(directory, id)) do
          :ok -> {:cont, :ok}
          {:error, :enoent} -> {:cont, :ok}
          _ -> {:halt, {:error, :storage_unavailable}}
        end
      else
        {:halt, {:error, :storage_unavailable}}
      end
    end)
  end

  @doc false
  def cleanup_created(paths) when is_list(paths) do
    Enum.each(paths, fn path ->
      case File.rm(path) do
        :ok -> :ok
        {:error, :enoent} -> :ok
        _ -> :ok
      end
    end)

    :ok
  end

  @doc false
  def cleanup_transient_refs(term) do
    term
    |> refs()
    |> Enum.each(fn {_id, ref} ->
      if is_binary(ref.path) and
           String.starts_with?(Path.basename(ref.path), [
             "attention-json-body-",
             "attention-json-base64-"
           ]) do
        _ = File.rm(ref.path)
      end
    end)

    :ok
  end

  @doc false
  def decode_json_string_file(source, target) when is_binary(source) and is_binary(target) do
    case File.open(source, [:read, :binary]) do
      {:ok, input} ->
        try do
          case File.open(target, [:write, :binary, :exclusive]) do
            {:ok, output} ->
              try do
                state = %{
                  mode: :normal,
                  utf8_tail: <<>>,
                  hash: :crypto.hash_init(:sha256),
                  size: 0
                }

                result =
                  case decode_json_string_chunks(input, output, state) do
                    {:ok, %{mode: :normal, utf8_tail: <<>>, hash: hash, size: size}} ->
                      digest = hash |> :crypto.hash_final() |> Base.encode16(case: :lower)

                      with :ok <- :file.sync(output),
                           do: {:ok, %Ref{id: digest, size: size, sha256: digest, path: target}}

                    {:ok, _incomplete} ->
                      {:error, :bad_json_string}

                    {:error, :bad_json_string} ->
                      {:error, :bad_json_string}

                    _ ->
                      {:error, :storage_unavailable}
                  end

                if match?({:error, _}, result), do: File.rm(target)
                result
              rescue
                _ ->
                  _ = File.rm(target)
                  {:error, :storage_unavailable}
              after
                _ = File.close(output)
              end

            error ->
              error
          end
        after
          _ = File.close(input)
        end

      error ->
        error
    end
  end

  @doc false
  def decode_json_base64_string_file(source, target)
      when is_binary(source) and is_binary(target) do
    base64_path = target <> ".base64"

    with {:ok, _base64_ref} <- decode_json_string_file(source, base64_path),
         {:ok, input} <- File.open(base64_path, [:read, :binary]) do
      try do
        case File.open(target, [:write, :binary, :exclusive]) do
          {:ok, output} ->
            try do
              result =
                case decode_base64_file(input, output, %{
                       tail: <<>>,
                       hash: :crypto.hash_init(:sha256),
                       size: 0
                     }) do
                  {:ok, %{hash: hash, size: size}} ->
                    digest = hash |> :crypto.hash_final() |> Base.encode16(case: :lower)

                    case :file.sync(output) do
                      :ok -> {:ok, %Ref{id: digest, size: size, sha256: digest, path: target}}
                      _ -> {:error, :storage_unavailable}
                    end

                  {:error, :bad_base64} ->
                    {:error, :bad_base64}

                  {:error, _reason} ->
                    {:error, :storage_unavailable}
                end

              if match?({:error, _}, result), do: File.rm(target)
              result
            after
              _ = File.close(output)
            end

          _ ->
            {:error, :storage_unavailable}
        end
      after
        _ = File.close(input)
        _ = File.rm(base64_path)
      end
    else
      {:error, :bad_base64} = error ->
        _ = File.rm(base64_path)
        _ = File.rm(target)
        error

      {:error, :bad_json_string} = error ->
        _ = File.rm(base64_path)
        _ = File.rm(target)
        error

      _ ->
        _ = File.rm(base64_path)
        _ = File.rm(target)
        {:error, :storage_unavailable}
    end
  end

  @doc false
  def read!(%Ref{} = ref) do
    case File.read(ref.path || "") do
      {:ok, bytes} when byte_size(bytes) == ref.size ->
        if Base.encode16(:crypto.hash(:sha256, bytes), case: :lower) == ref.sha256,
          do: bytes,
          else: raise(ArgumentError, "large data checksum mismatch")

      _ ->
        raise ArgumentError, "large data unavailable"
    end
  end

  @doc false
  def contains?(%Ref{} = ref, needle) when is_binary(needle) do
    case File.open(ref.path || "", [:read, :binary]) do
      {:ok, file} ->
        try do
          contains_file?(file, ref, needle, <<>>, :crypto.hash_init(:sha256), 0, false)
        after
          File.close(file)
        end

      _ ->
        false
    end
  end

  @doc false
  def preview(%Ref{} = ref, limit) when is_integer(limit) and limit > 0 do
    # The provider preview is bounded; stop reading once enough visible
    # graphemes have been found instead of loading the complete file.
    case File.open(ref.path || "", [:read, :binary]) do
      {:ok, file} ->
        try do
          preview_file(file, <<>>, <<>>, 0, limit)
        after
          File.close(file)
        end

      _ ->
        ""
    end
  end

  @doc false
  def escaped_stream(%Ref{} = ref) do
    Stream.resource(
      fn ->
        case File.open(ref.path || "", [:read, :binary]) do
          {:ok, file} -> {file, :crypto.hash_init(:sha256), 0}
          _ -> raise ArgumentError, "large data unavailable"
        end
      end,
      fn {file, hash, size} = state ->
        case IO.binread(file, @chunk_size) do
          bytes when is_binary(bytes) ->
            escaped = escape_json_bytes(bytes)
            {[escaped], {file, :crypto.hash_update(hash, bytes), size + byte_size(bytes)}}

          :eof ->
            digest = hash |> :crypto.hash_final() |> Base.encode16(case: :lower)

            if size == ref.size and digest == ref.sha256,
              do: {:halt, state},
              else: raise(ArgumentError, "large data checksum mismatch")

          error ->
            raise ArgumentError, "large data read failed: #{inspect(error)}"
        end
      end,
      fn {file, _hash, _size} -> File.close(file) end
    )
  end

  defp transform(%Ref{} = ref, directory, created) do
    target = Path.join(directory, ref.id)
    prepared = %{ref | path: target}

    cond do
      ref.path == target and verify(prepared) == :ok ->
        {:ok, prepared, created}

      File.exists?(target) and verify(prepared) == :ok ->
        {:ok, prepared, created}

      is_binary(ref.path) ->
        case copy_ref(ref, prepared, directory) do
          :ok -> {:ok, prepared, [target | created]}
          _ -> {:error, :storage_unavailable, created}
        end

      true ->
        {:error, :storage_unavailable, created}
    end
  end

  defp transform(value, directory, created) when is_map(value) do
    if is_struct(value) do
      {module, fields} = {value.__struct__, Map.from_struct(value)}

      with {:ok, mapped, created} <- transform_map(fields, directory, created) do
        {:ok, struct(module, mapped), created}
      else
        {:error, reason, created} -> {:error, reason, created}
        {:error, reason} -> {:error, reason, created}
      end
    else
      transform_map(value, directory, created)
    end
  end

  defp transform(value, directory, created) when is_list(value),
    do: transform_list(value, directory, created, [])

  defp transform(value, directory, created) when is_tuple(value) do
    with {:ok, values, created} <- transform(Tuple.to_list(value), directory, created) do
      {:ok, List.to_tuple(values), created}
    else
      {:error, reason, created} -> {:error, reason, created}
      {:error, reason} -> {:error, reason, created}
    end
  end

  defp transform(value, _directory, created), do: {:ok, value, created}

  defp transform_map(map, directory, created) do
    Enum.reduce_while(map, {:ok, %{}, created}, fn {key, value}, {:ok, mapped, files} ->
      result =
        if key in ["body", "answer", "title"] and is_binary(value) and
             byte_size(value) > @inline_limit do
          persist(value, directory, files)
        else
          transform(value, directory, files)
        end

      case result do
        {:ok, transformed, next_files} ->
          {:cont, {:ok, Map.put(mapped, key, transformed), next_files}}

        {:error, reason, failed_files} ->
          {:halt, {:error, reason, failed_files}}

        {:error, reason} ->
          {:halt, {:error, reason, files}}

        _error ->
          {:halt, {:error, :storage_unavailable, files}}
      end
    end)
  end

  defp transform_list([], _directory, created, reversed),
    do: {:ok, Enum.reverse(reversed), created}

  defp transform_list([value | rest], directory, created, reversed) do
    case transform(value, directory, created) do
      {:ok, transformed, next_created} ->
        transform_list(rest, directory, next_created, [transformed | reversed])

      {:error, reason, failed_created} ->
        {:error, reason, failed_created}

      {:error, reason} ->
        {:error, reason, created}
    end
  end

  defp persist(bytes, directory, created) do
    sha256 = Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
    path = Path.join(directory, sha256)
    ref = %Ref{id: sha256, size: byte_size(bytes), sha256: sha256, path: path}

    case File.stat(path) do
      {:ok, %{size: size}} when size == byte_size(bytes) ->
        case verify(ref) do
          :ok -> {:ok, ref, created}
          _ -> {:error, :storage_unavailable}
        end

      {:ok, _} ->
        {:error, :storage_unavailable}

      {:error, :enoent} ->
        staged = path <> ".#{System.unique_integer([:positive])}.staged"

        result =
          with :ok <- File.write(staged, bytes, [:binary, :exclusive]),
               :ok <- sync_file(staged),
               :ok <- File.rename(staged, path) do
            case sync_directory(directory) do
              :ok ->
                {:ok, ref, [path | created]}

              _ ->
                _ = File.rm(path)
                {:error, :storage_unavailable}
            end
          end

        case result do
          {:ok, _, _} = success ->
            success

          _ ->
            _ = File.rm(staged)
            {:error, :storage_unavailable}
        end

      _ ->
        {:error, :storage_unavailable}
    end
  end

  defp copy_ref(source, target, directory) do
    staged = target.path <> ".#{System.unique_integer([:positive])}.staged"

    result =
      try do
        with :ok <- verify(source),
             {:ok, input} <- File.open(source.path, [:read, :binary]) do
          try do
            case File.open(staged, [:write, :binary, :exclusive]) do
              {:ok, output} ->
                try do
                  case copy_chunks(input, output, :crypto.hash_init(:sha256), 0) do
                    {:ok, hash, size} when size == source.size and hash == source.sha256 ->
                      with :ok <- :file.sync(output),
                           :ok <- File.rename(staged, target.path) do
                        case sync_directory(directory) do
                          :ok ->
                            :ok

                          _ ->
                            _ = File.rm(target.path)
                            {:error, :storage_unavailable}
                        end
                      end

                    _ ->
                      {:error, :storage_unavailable}
                  end
                after
                  _ = File.close(output)
                end

              _ ->
                {:error, :storage_unavailable}
            end
          after
            _ = File.close(input)
          end
        end
      rescue
        # IO.binwrite/2 raises on a full filesystem instead of returning an
        # error tuple. Keep only that storage failure inside the persistence boundary.
        error in ErlangError ->
          if error.original == :enospc,
            do: {:error, :storage_unavailable},
            else: reraise(error, __STACKTRACE__)
      end

    if result != :ok, do: File.rm(staged)
    result
  end

  defp copy_chunks(input, output, hash, size) do
    case IO.binread(input, @chunk_size) do
      bytes when is_binary(bytes) ->
        with :ok <- IO.binwrite(output, bytes) do
          copy_chunks(input, output, :crypto.hash_update(hash, bytes), size + byte_size(bytes))
        end

      :eof ->
        {:ok, hash |> :crypto.hash_final() |> Base.encode16(case: :lower), size}

      error ->
        error
    end
  end

  defp decode_base64_file(input, output, state) do
    case IO.binread(input, @chunk_size) do
      bytes when is_binary(bytes) ->
        combined = state.tail <> bytes
        usable = div(max(byte_size(combined) - 4, 0), 4) * 4
        prefix = binary_part(combined, 0, usable)
        tail = binary_part(combined, usable, byte_size(combined) - usable)

        case decode_base64_chunk(prefix, false) do
          {:ok, decoded} ->
            case IO.binwrite(output, decoded) do
              :ok ->
                decode_base64_file(input, output, %{
                  tail: tail,
                  hash: :crypto.hash_update(state.hash, decoded),
                  size: state.size + byte_size(decoded)
                })

              _ ->
                {:error, :storage_unavailable}
            end

          _ ->
            {:error, :bad_base64}
        end

      :eof ->
        case decode_base64_chunk(state.tail, true) do
          {:ok, decoded} ->
            case IO.binwrite(output, decoded) do
              :ok ->
                {:ok,
                 %{
                   state
                   | tail: <<>>,
                     hash: :crypto.hash_update(state.hash, decoded),
                     size: state.size + byte_size(decoded)
                 }}

              _ ->
                {:error, :storage_unavailable}
            end

          _ ->
            {:error, :bad_base64}
        end

      _ ->
        {:error, :storage_unavailable}
    end
  end

  defp decode_base64_chunk(<<>>, _final?), do: {:ok, <<>>}

  defp decode_base64_chunk(encoded, final?) do
    opts = if final?, do: [], else: [padding: false]

    case Base.decode64(encoded, opts) do
      {:ok, decoded} ->
        if Base.encode64(decoded, opts) == encoded,
          do: {:ok, decoded},
          else: {:error, :bad_base64}

      :error ->
        {:error, :bad_base64}
    end
  end

  defp decode_json_string_chunks(input, output, state) do
    case IO.binread(input, @chunk_size) do
      bytes when is_binary(bytes) ->
        with {:ok, produced, next_state} <- decode_json_string_bytes(bytes, state),
             {:ok, emitted_state} <- emit_utf8(output, produced, next_state) do
          decode_json_string_chunks(input, output, emitted_state)
        end

      :eof ->
        {:ok, state}

      error ->
        error
    end
  end

  defp decode_json_string_bytes(bytes, state) do
    bytes
    |> :binary.bin_to_list()
    |> Enum.reduce_while({:ok, [], state}, fn byte, {:ok, output, current} ->
      case decode_json_byte(byte, current) do
        {:ok, nil, next} -> {:cont, {:ok, output, next}}
        {:ok, produced, next} -> {:cont, {:ok, [produced | output], next}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, produced, next} -> {:ok, produced |> Enum.reverse() |> IO.iodata_to_binary(), next}
      error -> error
    end
  end

  defp decode_json_byte(byte, %{mode: :normal} = state) do
    cond do
      byte == ?\\ -> {:ok, nil, %{state | mode: :escape}}
      byte < 0x20 -> {:error, :bad_json_string}
      true -> {:ok, <<byte>>, state}
    end
  end

  defp decode_json_byte(34, %{mode: :escape} = state), do: {:ok, <<34>>, %{state | mode: :normal}}

  defp decode_json_byte(?\\, %{mode: :escape} = state),
    do: {:ok, <<?\\>>, %{state | mode: :normal}}

  defp decode_json_byte(?/, %{mode: :escape} = state), do: {:ok, <<?/>>, %{state | mode: :normal}}
  defp decode_json_byte(?b, %{mode: :escape} = state), do: {:ok, <<8>>, %{state | mode: :normal}}
  defp decode_json_byte(?f, %{mode: :escape} = state), do: {:ok, <<12>>, %{state | mode: :normal}}
  defp decode_json_byte(?n, %{mode: :escape} = state), do: {:ok, <<10>>, %{state | mode: :normal}}
  defp decode_json_byte(?r, %{mode: :escape} = state), do: {:ok, <<13>>, %{state | mode: :normal}}
  defp decode_json_byte(?t, %{mode: :escape} = state), do: {:ok, <<9>>, %{state | mode: :normal}}

  defp decode_json_byte(?u, %{mode: :escape} = state),
    do: {:ok, nil, %{state | mode: {:unicode, 0, 4}}}

  defp decode_json_byte(_byte, %{mode: :escape}), do: {:error, :bad_json_string}

  defp decode_json_byte(byte, %{mode: {:unicode, value, remaining}} = state) do
    case hex_digit(byte) do
      nil ->
        {:error, :bad_json_string}

      digit when remaining > 1 ->
        {:ok, nil, %{state | mode: {:unicode, value * 16 + digit, remaining - 1}}}

      digit ->
        codepoint = value * 16 + digit

        cond do
          codepoint in 0xD800..0xDBFF -> {:ok, nil, %{state | mode: {:low_slash, codepoint}}}
          codepoint in 0xDC00..0xDFFF -> {:error, :bad_json_string}
          true -> {:ok, <<codepoint::utf8>>, %{state | mode: :normal}}
        end
    end
  end

  defp decode_json_byte(?\\, %{mode: {:low_slash, high}} = state),
    do: {:ok, nil, %{state | mode: {:low_u, high}}}

  defp decode_json_byte(?u, %{mode: {:low_u, high}} = state),
    do: {:ok, nil, %{state | mode: {:low_unicode, high, 0, 4}}}

  defp decode_json_byte(byte, %{mode: {:low_unicode, high, value, remaining}} = state) do
    case hex_digit(byte) do
      nil ->
        {:error, :bad_json_string}

      digit when remaining > 1 ->
        {:ok, nil, %{state | mode: {:low_unicode, high, value * 16 + digit, remaining - 1}}}

      digit ->
        low = value * 16 + digit

        if low in 0xDC00..0xDFFF do
          codepoint = 0x10000 + Bitwise.bsl(high - 0xD800, 10) + low - 0xDC00
          {:ok, <<codepoint::utf8>>, %{state | mode: :normal}}
        else
          {:error, :bad_json_string}
        end
    end
  end

  defp decode_json_byte(_byte, %{mode: {:low_slash, _}}), do: {:error, :bad_json_string}
  defp decode_json_byte(_byte, %{mode: {:low_u, _}}), do: {:error, :bad_json_string}

  defp emit_utf8(output, produced, state) do
    case :unicode.characters_to_binary(state.utf8_tail <> produced, :utf8, :utf8) do
      valid when is_binary(valid) ->
        with :ok <- IO.binwrite(output, valid) do
          {:ok,
           %{
             state
             | utf8_tail: <<>>,
               hash: :crypto.hash_update(state.hash, valid),
               size: state.size + byte_size(valid)
           }}
        end

      {:incomplete, valid, tail} ->
        with :ok <- IO.binwrite(output, valid) do
          {:ok,
           %{
             state
             | utf8_tail: tail,
               hash: :crypto.hash_update(state.hash, valid),
               size: state.size + byte_size(valid)
           }}
        end

      {:error, _valid, _rest} ->
        {:error, :bad_json_string}
    end
  end

  defp hex_digit(byte) when byte in ?0..?9, do: byte - ?0
  defp hex_digit(byte) when byte in ?a..?f, do: byte - ?a + 10
  defp hex_digit(byte) when byte in ?A..?F, do: byte - ?A + 10
  defp hex_digit(_), do: nil

  defp sync_file(path) do
    with {:ok, file} <- :file.open(String.to_charlist(path), [:read, :write, :binary]),
         result <- :file.sync(file),
         :ok <- :file.close(file),
         :ok <- result do
      :ok
    end
  end

  defp sync_directory(directory) do
    case :file.open(String.to_charlist(directory), [:read, :raw]) do
      {:ok, file} ->
        result = :file.sync(file)
        _ = :file.close(file)
        result

      {:error, :eisdir} ->
        :ok

      error ->
        error
    end
  end

  defp hash_file(path) do
    case File.open(path, [:read, :binary]) do
      {:ok, file} ->
        try do
          hash_file_chunks(file, :crypto.hash_init(:sha256))
        after
          File.close(file)
        end

      error ->
        error
    end
  end

  defp hash_file_chunks(file, context) do
    case IO.binread(file, @chunk_size) do
      bytes when is_binary(bytes) -> hash_file_chunks(file, :crypto.hash_update(context, bytes))
      :eof -> {:ok, context |> :crypto.hash_final() |> Base.encode16(case: :lower)}
      error -> error
    end
  end

  defp contains_file?(file, ref, needle, tail, hash, seen, found?) do
    case IO.binread(file, @chunk_size) do
      bytes when is_binary(bytes) ->
        window = tail <> bytes
        found? = found? or :binary.match(window, needle) != :nomatch
        keep = min(max(byte_size(needle) - 1, 0), byte_size(window))
        suffix = if keep == 0, do: <<>>, else: binary_part(window, byte_size(window) - keep, keep)

        contains_file?(
          file,
          ref,
          needle,
          suffix,
          :crypto.hash_update(hash, bytes),
          seen + byte_size(bytes),
          found?
        )

      :eof ->
        digest = hash |> :crypto.hash_final() |> Base.encode16(case: :lower)
        seen == ref.size and digest == ref.sha256 and found?

      _ ->
        false
    end
  end

  defp preview_file(file, pending, normalized, count, limit) do
    if count >= limit do
      normalized
    else
      case IO.binread(file, @chunk_size) do
        bytes when is_binary(bytes) ->
          {complete, rest} = valid_prefix(pending <> bytes)
          {next, next_count} = preview_text(complete, normalized, count, false, limit)
          preview_file(file, rest, next, next_count, limit)

        :eof ->
          {next, _count} = preview_text(pending, normalized, count, false, limit)
          next

        _ ->
          normalized
      end
    end
  end

  defp preview_text(<<>>, normalized, count, _space?, _limit), do: {normalized, count}

  defp preview_text(binary, normalized, count, space?, limit) do
    case String.next_grapheme(binary) do
      {grapheme, rest} ->
        if String.trim(grapheme) == "" do
          preview_text(rest, normalized, count, normalized != "", limit)
        else
          prefix =
            if space? and normalized != "",
              do: normalized <> " " <> grapheme,
              else: normalized <> grapheme

          next_count = count + 1

          if next_count >= limit,
            do: {prefix, next_count},
            else: preview_text(rest, prefix, next_count, false, limit)
        end

      nil ->
        {normalized, count}
    end
  end

  defp valid_prefix(binary) do
    case String.valid?(binary) do
      true ->
        {binary, <<>>}

      false ->
        max_tail = min(byte_size(binary), 4)

        Enum.reduce_while(1..max_tail, {binary, <<>>}, fn suffix_size, _acc ->
          prefix_size = byte_size(binary) - suffix_size
          prefix = binary_part(binary, 0, prefix_size)
          suffix = binary_part(binary, prefix_size, suffix_size)

          if String.valid?(prefix),
            do: {:halt, {prefix, suffix}},
            else: {:cont, {binary, <<>>}}
        end)
    end
  end

  defp walk_refs(%Ref{} = ref, fun), do: fun.(ref)

  defp walk_refs(value, fun) when is_map(value) do
    {module, map} =
      if is_struct(value), do: {value.__struct__, Map.from_struct(value)}, else: {nil, value}

    result = Map.new(map, fn {key, child} -> {key, walk_refs(child, fun)} end)
    if module, do: struct(module, result), else: result
  end

  defp walk_refs(value, fun) when is_list(value), do: Enum.map(value, &walk_refs(&1, fun))

  defp walk_refs(value, fun) when is_tuple(value),
    do: value |> Tuple.to_list() |> Enum.map(&walk_refs(&1, fun)) |> List.to_tuple()

  defp walk_refs(value, _fun), do: value

  defp reference_marker(%Ref{id: id, size: size}),
    do: @persisted_ref_prefix <> id <> "; bytes=" <> Integer.to_string(size) <> "]"

  defp attach_reference_markers(%Ref{} = ref, _path, _references), do: ref

  defp attach_reference_markers(value, path, references) when is_binary(value) do
    case Map.get(references, path_key(path)) do
      {^value, ref} -> ref
      _ -> value
    end
  end

  defp attach_reference_markers(value, path, references) when is_map(value) do
    {module, fields} =
      if is_struct(value), do: {value.__struct__, Map.from_struct(value)}, else: {nil, value}

    mapped =
      Map.new(fields, fn {key, child} ->
        {key, attach_reference_markers(child, [{:map, key} | path], references)}
      end)

    if module, do: struct(module, mapped), else: mapped
  end

  defp attach_reference_markers(value, path, references) when is_list(value) do
    value
    |> Enum.with_index()
    |> Enum.map(fn {child, index} ->
      attach_reference_markers(child, [{:list, index} | path], references)
    end)
  end

  defp attach_reference_markers(value, path, references) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.with_index()
    |> Enum.map(fn {child, index} ->
      attach_reference_markers(child, [{:tuple, index} | path], references)
    end)
    |> List.to_tuple()
  end

  defp attach_reference_markers(value, _path, _references), do: value

  defp references_by_path(rows, directory) do
    Enum.reduce_while(rows, {:ok, %{}}, fn
      [path, marker, id, size, sha256], {:ok, references}
      when is_binary(path) and is_binary(marker) and is_binary(id) and is_integer(size) and
             is_binary(sha256) ->
        ref = %Ref{id: id, size: size, sha256: sha256, path: Path.join(directory, id)}

        if valid_ref_metadata?(ref) and marker == reference_marker(ref) do
          {:cont, {:ok, Map.put(references, path, {marker, ref})}}
        else
          {:halt, {:error, :storage_unavailable}}
        end

      _row, _acc ->
        {:halt, {:error, :storage_unavailable}}
    end)
  end

  defp collect_reference_rows(%Ref{} = ref, path, rows) do
    key = path_key(path)
    Map.put(rows, key, [key, reference_marker(ref), ref.id, ref.size, ref.sha256])
  end

  defp collect_reference_rows(value, path, rows) when is_map(value) do
    fields = if is_struct(value), do: Map.from_struct(value), else: value

    Enum.reduce(fields, rows, fn {key, child}, acc ->
      collect_reference_rows(child, [{:map, key} | path], acc)
    end)
  end

  defp collect_reference_rows(value, path, rows) when is_list(value) do
    value
    |> Enum.with_index()
    |> Enum.reduce(rows, fn {child, index}, acc ->
      collect_reference_rows(child, [{:list, index} | path], acc)
    end)
  end

  defp collect_reference_rows(value, path, rows) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.with_index()
    |> Enum.reduce(rows, fn {child, index}, acc ->
      collect_reference_rows(child, [{:tuple, index} | path], acc)
    end)
  end

  defp collect_reference_rows(_value, _path, rows), do: rows

  defp path_key(path), do: path |> Enum.reverse() |> :erlang.term_to_binary([:deterministic])

  defp collect_refs(%Ref{} = ref, acc), do: Map.put(acc, ref.id, ref)

  defp collect_refs(value, acc) when is_map(value) do
    value
    |> Map.values()
    |> Enum.reduce(acc, &collect_refs/2)
  end

  defp collect_refs(value, acc) when is_list(value), do: Enum.reduce(value, acc, &collect_refs/2)

  defp collect_refs(value, acc) when is_tuple(value),
    do: value |> Tuple.to_list() |> Enum.reduce(acc, &collect_refs/2)

  defp collect_refs(_value, acc), do: acc

  defp valid_ref?(%Ref{id: id, size: size, sha256: hash, path: path}) do
    valid_ref_metadata?(%Ref{id: id, size: size, sha256: hash, path: path}) and is_binary(path)
  end

  defp valid_ref?(_), do: false

  defp valid_ref_metadata?(%Ref{id: id, size: size, sha256: hash}) do
    is_binary(id) and Regex.match?(~r/\A[0-9a-f]{64}\z/, id) and id == hash and
      is_integer(size) and size >= 0
  end

  defp valid_ref_metadata?(_), do: false

  defp escape_json_bytes(bytes) do
    bytes
    |> :binary.bin_to_list()
    |> Enum.map(fn
      0x22 ->
        "\\\""

      0x5C ->
        "\\\\"

      0x08 ->
        "\\b"

      0x0C ->
        "\\f"

      0x0A ->
        "\\n"

      0x0D ->
        "\\r"

      0x09 ->
        "\\t"

      control when control < 0x20 ->
        hex = control |> Integer.to_string(16) |> String.pad_leading(2, "0")
        "\\u00" <> hex

      byte ->
        <<byte>>
    end)
    |> IO.iodata_to_binary()
  end
end
