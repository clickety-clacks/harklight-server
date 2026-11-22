defmodule AttentionServer.JSON do
  @moduledoc """
  JSON wire codec that retains decimal tokens instead of converting
  them to IEEE-754 floats. Object key order and insignificant whitespace are
  not preserved; JSON values and array order are.
  """

  defmodule DecimalToken do
    @moduledoc false
    @enforce_keys [:raw]
    defstruct [:raw]
  end

  defmodule StreamedLine do
    @moduledoc false
    @enforce_keys [:value]
    defstruct [:value]
  end

  @ui1_schema "attention-client.ui/1"
  @duplicate_keys_marker :__attention_server_duplicate_keys__
  @decimal_token ~r/\A-?(?:0|[1-9][0-9]*)(?:\.[0-9]+(?:[eE][+-]?[0-9]+)?|[eE][+-]?[0-9]+)\z/

  @spec decode_line(binary(), pos_integer() | :infinity) :: {:ok, term()} | {:error, :bad_frame}
  def decode_line(raw, max_frame \\ :infinity)

  def decode_line(raw, max_frame), do: decode_line_with(raw, max_frame, &put_unique/3)

  @doc false
  def decode_line_for_ui1_admission(raw, max_frame \\ :infinity) do
    case decode_line(raw, max_frame) do
      {:ok, _value} = decoded ->
        decoded

      {:error, :bad_frame} ->
        case decode_line_with(raw, max_frame, &put_duplicate_marker/3) do
          {:ok, frame} when is_map(frame) and not is_struct(frame) ->
            if ui1_duplicate_request?(frame), do: {:ok, frame}, else: {:error, :bad_frame}

          _ ->
            {:error, :bad_frame}
        end
    end
  end

  @doc false
  def decode_line_file(path, max_frame \\ :infinity) when is_binary(path),
    do: decode_line_file_streaming(path, max_frame, &decode_line/1)

  @doc false
  def decode_line_file_for_ui1_admission(path, max_frame \\ :infinity) when is_binary(path),
    do: decode_line_file_streaming(path, max_frame, &decode_line_for_ui1_admission/1)

  defp decode_line_file_streaming(path, max_frame, decode) do
    Process.put(:attention_json_temp_paths, [])

    try do
      result =
        with {:ok, %{size: size}} <- File.stat(path),
             true <-
               max_frame == :infinity or
                 (is_integer(max_frame) and max_frame > 0 and size + 1 <= max_frame),
             {:ok, skeleton_path, markers} <- mask_large_bodies(path),
             {:ok, skeleton} <- File.read(skeleton_path),
             {:ok, value} <- decode.(skeleton),
             {:ok, replaced} <- replace_body_markers(value, markers) do
          {:ok, replaced}
        else
          {:error, :storage_unavailable} = error -> error
          {:error, :bad_frame} = error -> error
          {:error, _reason} -> {:error, :storage_unavailable}
          _ -> {:error, :bad_frame}
        end

      keep =
        case result do
          {:ok, _} -> Process.get(:attention_json_keep_paths, [])
          _ -> []
        end

      Process.get(:attention_json_temp_paths, [])
      |> Enum.reject(&(&1 in keep))
      |> Enum.each(&File.rm/1)

      if match?({:error, _}, result) and result != {:error, :storage_unavailable} do
        AttentionServer.LargeData.cleanup_transient_refs(
          Process.get(:attention_json_markers, %{})
        )
      end

      result
    after
      Process.delete(:attention_json_temp_paths)
      Process.delete(:attention_json_keep_paths)
      Process.delete(:attention_json_markers)
    end
  end

  defp mask_large_bodies(path) do
    skeleton_path = temp_path("attention-json-skeleton-")
    track_temp(skeleton_path)

    with {:ok, input} <- File.open(path, [:read, :binary]),
         {:ok, output} <- File.open(skeleton_path, [:write, :binary, :exclusive]) do
      state = %{
        mode: :normal,
        escaped: false,
        key_buffer: <<>>,
        key_buffer_overflow: false,
        last_body_key: nil,
        expect_body_value: nil,
        expect_files_value: false,
        depth: 0,
        files_array_depth: nil,
        body_kind: nil,
        body_path: nil,
        body_file: nil,
        markers: %{},
        token_paths: []
      }

      try do
        case scan_json_file(input, output, state) do
          {:ok, state} ->
            case :file.sync(output) do
              :ok ->
                Process.put(:attention_json_markers, state.markers)

                Process.put(
                  :attention_json_keep_paths,
                  state.markers
                  |> Map.values()
                  |> Enum.flat_map(fn
                    %AttentionServer.LargeData.Ref{path: path} -> [path]
                    _ -> []
                  end)
                )

                {:ok, skeleton_path, state.markers}

              _ ->
                cleanup_json_temp_files(state)
                {:error, :storage_unavailable}
            end

          error ->
            cleanup_json_temp_files(state)
            error
        end
      after
        _ = File.close(input)
        _ = File.close(output)
      end
    else
      _ ->
        _ = File.rm(skeleton_path)
        {:error, :storage_unavailable}
    end
  end

  defp scan_json_file(input, output, state) do
    case IO.binread(input, 64 * 1024) do
      bytes when is_binary(bytes) ->
        with {:ok, next} <- scan_json_bytes(bytes, output, state, [], []) do
          scan_json_file(input, output, next)
        end

      :eof ->
        if state.mode == :normal do
          with :ok <- IO.binwrite(output, "\n"), do: {:ok, state}
        else
          {:error, :bad_frame}
        end

      _ ->
        {:error, :storage_unavailable}
    end
  end

  defp scan_json_bytes(<<>>, output, state, output_bytes, body_bytes) do
    with :ok <- write_iodata(output, Enum.reverse(output_bytes)),
         :ok <- write_token_bytes(state, Enum.reverse(body_bytes)) do
      {:ok, state}
    end
  end

  defp scan_json_bytes(
         <<byte, rest::binary>>,
         output,
         %{mode: :normal} = state,
         output_bytes,
         body_bytes
       ) do
    cond do
      state.expect_files_value and byte in [9, 10, 13, 32] ->
        scan_json_bytes(rest, output, state, [byte | output_bytes], body_bytes)

      state.expect_files_value and byte == ?[ ->
        depth = state.depth + 1

        scan_json_bytes(
          rest,
          output,
          %{state | expect_files_value: false, depth: depth, files_array_depth: depth},
          [byte | output_bytes],
          body_bytes
        )

      state.expect_files_value ->
        scan_json_bytes(
          <<byte, rest::binary>>,
          output,
          %{state | expect_files_value: false, last_body_key: false},
          output_bytes,
          body_bytes
        )

      is_binary(state.expect_body_value) and byte in [9, 10, 13, 32] ->
        scan_json_bytes(rest, output, state, [byte | output_bytes], body_bytes)

      is_binary(state.expect_body_value) and byte == ?" ->
        with :ok <- write_iodata(output, Enum.reverse(output_bytes)),
             {:ok, raw_path, raw_file} <- open_temp("attention-json-token-") do
          next = %{
            state
            | mode: :body_string,
              escaped: false,
              expect_body_value: nil,
              last_body_key: nil,
              body_kind: state.expect_body_value,
              body_path: raw_path,
              body_file: raw_file,
              token_paths: [raw_path | state.token_paths]
          }

          scan_json_bytes(rest, output, next, [], body_bytes)
        else
          _ -> {:error, :storage_unavailable}
        end

      state.expect_body_value ->
        scan_json_bytes(
          <<byte, rest::binary>>,
          output,
          %{state | expect_body_value: nil, last_body_key: false},
          output_bytes,
          body_bytes
        )

      byte == ?" ->
        next = %{
          state
          | mode: :string,
            escaped: false,
            key_buffer: <<>>,
            key_buffer_overflow: false
        }

        scan_json_bytes(rest, output, next, [byte | output_bytes], body_bytes)

      is_binary(state.last_body_key) and byte in [9, 10, 13, 32] ->
        scan_json_bytes(rest, output, state, [byte | output_bytes], body_bytes)

      is_binary(state.last_body_key) and byte == ?: ->
        expect_body_value =
          if state.last_body_key in ["body", "answer", "title", "data_base64"],
            do: state.last_body_key,
            else: nil

        scan_json_bytes(
          rest,
          output,
          %{
            state
            | last_body_key: nil,
              expect_body_value: expect_body_value,
              expect_files_value: state.last_body_key == "files" and state.depth == 1
          },
          [byte | output_bytes],
          body_bytes
        )

      byte in [9, 10, 13, 32] ->
        scan_json_bytes(
          rest,
          output,
          %{state | last_body_key: false},
          [byte | output_bytes],
          body_bytes
        )

      byte == ?{ or byte == ?[ ->
        scan_json_bytes(
          rest,
          output,
          %{state | depth: state.depth + 1, last_body_key: false},
          [byte | output_bytes],
          body_bytes
        )

      byte == ?} or byte == ?] ->
        files_array_depth =
          if byte == ?] and state.files_array_depth == state.depth,
            do: nil,
            else: state.files_array_depth

        scan_json_bytes(
          rest,
          output,
          %{
            state
            | depth: state.depth - 1,
              files_array_depth: files_array_depth,
              last_body_key: false
          },
          [byte | output_bytes],
          body_bytes
        )

      true ->
        scan_json_bytes(
          rest,
          output,
          %{state | last_body_key: false},
          [byte | output_bytes],
          body_bytes
        )
    end
  end

  defp scan_json_bytes(
         <<byte, rest::binary>>,
         output,
         %{mode: :string} = state,
         output_bytes,
         body_bytes
       ) do
    closing? = byte == ?" and not state.escaped

    next =
      if closing? do
        stream_key =
          if state.key_buffer_overflow,
            do: nil,
            else: decoded_stream_key(state.key_buffer, state)

        %{state | mode: :normal, escaped: false, last_body_key: stream_key}
      else
        key_buffer =
          if state.key_buffer_overflow or byte_size(state.key_buffer) >= 32,
            do: state.key_buffer,
            else: state.key_buffer <> <<byte>>

        overflow? = state.key_buffer_overflow or byte_size(state.key_buffer) >= 32

        %{
          state
          | escaped: byte == ?\\ and not state.escaped,
            key_buffer: key_buffer,
            key_buffer_overflow: overflow?
        }
      end

    scan_json_bytes(rest, output, next, [byte | output_bytes], body_bytes)
  end

  defp scan_json_bytes(
         <<byte, rest::binary>>,
         output,
         %{mode: :body_string} = state,
         output_bytes,
         body_bytes
       ) do
    closing? = byte == ?" and not state.escaped

    if closing? do
      with :ok <- write_token_bytes(state, Enum.reverse(body_bytes)),
           :ok <- File.close(state.body_file),
           {:ok, next, skeleton_bytes} <- finish_body_token(state),
           :ok <- write_iodata(output, skeleton_bytes) do
        scan_json_bytes(rest, output, next, [], [])
      else
        {:error, :bad_frame} -> {:error, :bad_frame}
        _ -> {:error, :storage_unavailable}
      end
    else
      next = %{state | escaped: byte == ?\\ and not state.escaped}
      scan_json_bytes(rest, output, next, output_bytes, [byte | body_bytes])
    end
  end

  defp decoded_stream_key(raw, state) do
    case decode_line("\"" <> raw <> "\"\n") do
      {:ok, key} when key in ["body", "answer", "title"] ->
        key

      {:ok, "files"} when state.depth == 1 ->
        "files"

      {:ok, "data_base64"}
      when is_integer(state.files_array_depth) and state.depth == state.files_array_depth + 1 ->
        "data_base64"

      _ ->
        nil
    end
  end

  defp finish_body_token(state) do
    with {:ok, %{size: raw_size}} <- File.stat(state.body_path) do
      cond do
        state.body_kind == "data_base64" -> finish_base64_token(state)
        raw_size > 2 * 1024 * 1024 -> finish_large_text_token(state)
        true -> finish_inline_token(state)
      end
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  defp finish_base64_token(state) do
    marker =
      "__attention_large_text_" <>
        Base.encode16(:crypto.strong_rand_bytes(16), case: :lower) <> "__"

    target = temp_path("attention-json-base64-")
    track_temp(target)

    case AttentionServer.LargeData.decode_json_base64_string_file(state.body_path, target) do
      {:ok, ref} ->
        finish_ref_token(state, marker, ref)

      {:error, :bad_json_string} ->
        _ = File.rm(target)
        {:error, :bad_frame}

      {:error, :bad_base64} ->
        _ = File.rm(target)
        _ = File.rm(state.body_path)
        {:ok, finish_token_state(state), encode!("!")}

      _ ->
        _ = File.rm(target)
        {:error, :storage_unavailable}
    end
  end

  defp finish_large_text_token(state) do
    marker =
      "__attention_large_text_" <>
        Base.encode16(:crypto.strong_rand_bytes(16), case: :lower) <> "__"

    target = temp_path("attention-json-body-")
    track_temp(target)

    case AttentionServer.LargeData.decode_json_string_file(state.body_path, target) do
      {:ok, ref} ->
        finish_ref_token(state, marker, ref)

      {:error, :bad_json_string} ->
        _ = File.rm(target)
        {:error, :bad_frame}

      _ ->
        _ = File.rm(target)
        {:error, :storage_unavailable}
    end
  end

  defp finish_inline_token(state) do
    with {:ok, token} <- File.read(state.body_path) do
      _ = File.rm(state.body_path)
      {:ok, finish_token_state(state), [?", token, ?"]}
    else
      _ -> {:error, :storage_unavailable}
    end
  end

  defp finish_ref_token(state, marker, ref) do
    next = finish_token_state(state)
    {:ok, %{next | markers: Map.put(next.markers, marker, ref)}, encode!(marker)}
  end

  defp finish_token_state(state) do
    _ = File.rm(state.body_path)

    %{
      state
      | mode: :normal,
        escaped: false,
        last_body_key: nil,
        body_path: nil,
        body_file: nil,
        token_paths: Enum.reject(state.token_paths, &(&1 == state.body_path))
    }
  end

  defp replace_body_markers(value, markers) do
    {:ok, replace_nested_markers(value, markers)}
  end

  defp replace_nested_markers(%{} = map, markers) when not is_struct(map) do
    Map.new(map, fn
      {key, marker}
      when key in ["body", "answer", "title", "data_base64"] and is_binary(marker) ->
        {key, Map.get(markers, marker, marker)}

      {key, value} ->
        {key, replace_nested_markers(value, markers)}
    end)
  end

  defp replace_nested_markers(value, markers) when is_list(value),
    do: Enum.map(value, &replace_nested_markers(&1, markers))

  defp replace_nested_markers(value, _markers), do: value

  defp write_token_bytes(_state, []), do: :ok

  defp write_token_bytes(state, bytes) do
    case IO.binwrite(state.body_file, bytes) do
      :ok -> :ok
      _ -> {:error, :storage_unavailable}
    end
  end

  defp write_iodata(_file, []), do: :ok

  defp write_iodata(file, data) do
    case IO.binwrite(file, data) do
      :ok -> :ok
      _ -> {:error, :storage_unavailable}
    end
  end

  defp open_temp(prefix) do
    path = temp_path(prefix)

    case File.open(path, [:write, :binary, :exclusive]) do
      {:ok, file} ->
        track_temp(path)
        {:ok, path, file}

      error ->
        error
    end
  end

  defp temp_path(prefix),
    do:
      Path.join(
        System.tmp_dir!(),
        prefix <> Integer.to_string(System.unique_integer([:positive, :monotonic]))
      )

  defp track_temp(path),
    do:
      Process.put(:attention_json_temp_paths, [path | Process.get(:attention_json_temp_paths, [])])

  defp cleanup_json_temp_files(state) do
    Enum.each(state.token_paths, &File.rm/1)
    AttentionServer.LargeData.cleanup_transient_refs(state.markers)
  end

  @doc false
  def ui1_duplicate_request?(frame) when is_map(frame) and not is_struct(frame) do
    type = Map.get(frame, "type")
    message = Map.get(frame, "message")
    answer_ui = if is_map(message), do: Map.get(message, "answer_ui"), else: nil

    with true <- type in ["send", "revise", "forward", "conversation_revise"],
         %{"schema" => @ui1_schema} <- answer_ui,
         true <- has_marked_duplicates?(answer_ui) do
      without_answer_ui =
        frame
        |> Map.put("message", Map.put(message, "answer_ui", nil))

      not has_marked_duplicates?(without_answer_ui)
    else
      _ -> false
    end
  end

  def ui1_duplicate_request?(_), do: false

  @doc false
  def has_marked_duplicates?(value) when is_map(value) and not is_struct(value) do
    case Map.get(value, @duplicate_keys_marker) do
      [_ | _] ->
        true

      _ ->
        Enum.any?(value, fn {key, child} ->
          key != @duplicate_keys_marker and has_marked_duplicates?(child)
        end)
    end
  end

  def has_marked_duplicates?(value) when is_list(value),
    do: Enum.any?(value, &has_marked_duplicates?/1)

  def has_marked_duplicates?(_), do: false

  defp decode_line_with(raw, max_frame, object_push)
       when is_binary(raw) and
              (max_frame == :infinity or (is_integer(max_frame) and max_frame > 0)) do
    if max_frame == :infinity or byte_size(raw) <= max_frame do
      decode_line_content(raw, object_push)
    else
      {:error, :bad_frame}
    end
  end

  defp decode_line_with(_, _, _), do: {:error, :bad_frame}

  defp decode_line_content(raw, object_push) do
    if String.ends_with?(raw, "\n") do
      json = binary_part(raw, 0, byte_size(raw) - 1)

      try do
        case JSON.decode(json, nil,
               float: fn token -> %DecimalToken{raw: token} end,
               object_start: fn _ -> %{} end,
               object_push: object_push,
               object_finish: fn object, old_acc -> {object, old_acc} end
             ) do
          {value, nil, rest} ->
            if String.trim(rest) == "", do: {:ok, value}, else: {:error, :bad_frame}

          _ ->
            {:error, :bad_frame}
        end
      rescue
        _ -> {:error, :bad_frame}
      catch
        :throw, :duplicate_key -> {:error, :bad_frame}
      end
    else
      {:error, :bad_frame}
    end
  end

  defp put_unique(key, value, object) do
    if Map.has_key?(object, key), do: throw(:duplicate_key)
    Map.put(object, key, value)
  end

  defp put_duplicate_marker(key, value, object) do
    if Map.has_key?(object, key) do
      Map.update(object, @duplicate_keys_marker, [key], &(&1 ++ [key]))
    else
      Map.put(object, key, value)
    end
  end

  @spec encode!(term()) :: binary()
  def encode!(%DecimalToken{raw: raw}) when is_binary(raw) do
    if Regex.match?(@decimal_token, raw),
      do: raw,
      else: raise(ArgumentError, "invalid decimal token")
  end

  def encode!(%AttentionServer.LargeData.Ref{} = ref),
    do: encode!(AttentionServer.LargeData.read!(ref))

  def encode!(value)
      when is_nil(value) or is_boolean(value) or is_integer(value) or is_binary(value),
      do: JSON.encode!(value)

  # Server-computed scores must round-trip as JSON numbers without bucketing.
  def encode!(value) when is_float(value), do: :erlang.float_to_binary(value, [:short])

  def encode!(value) when is_list(value),
    do: "[" <> Enum.map_join(value, ",", &encode!/1) <> "]"

  def encode!(value) when is_map(value) do
    if not Enum.all?(Map.keys(value), &is_binary/1),
      do: raise(ArgumentError, "non-string JSON key")

    members =
      value
      |> Enum.sort_by(fn {key, _} -> key end)
      |> Enum.map_join(",", fn {key, item} -> JSON.encode!(key) <> ":" <> encode!(item) end)

    "{" <> members <> "}"
  end

  def encode!(_), do: raise(ArgumentError, "not a JSON value")

  @doc false
  def encode_line(value) do
    if AttentionServer.LargeData.contains_ref?(value),
      do: %StreamedLine{value: value},
      else: encode!(value) <> "\n"
  end

  @doc false
  def encode_stream(%StreamedLine{value: value}),
    do: Stream.concat([encode_value_stream(value), once("\n")])

  def encode_stream(value), do: encode_value_stream(value)

  @doc false
  def encoded_size(value) do
    value
    |> encode_value_stream()
    |> Enum.reduce(0, fn chunk, size -> size + byte_size(chunk) end)
  end

  defp encode_value_stream(%AttentionServer.LargeData.Ref{} = ref) do
    Stream.concat([once("\""), AttentionServer.LargeData.escaped_stream(ref), once("\"")])
  end

  defp encode_value_stream(%DecimalToken{} = value), do: once(encode!(value))

  defp encode_value_stream(value)
       when is_nil(value) or is_boolean(value) or is_number(value) or is_binary(value),
       do: once(encode!(value))

  defp encode_value_stream(values) when is_list(values) do
    parts =
      [once("[")] ++ interleave(Enum.map(values, &encode_value_stream/1), ",") ++ [once("]")]

    Stream.concat(parts)
  end

  defp encode_value_stream(value) when is_map(value) do
    if not Enum.all?(Map.keys(value), &is_binary/1),
      do: raise(ArgumentError, "non-string JSON key")

    parts =
      value
      |> Enum.sort_by(fn {key, _} -> key end)
      |> Enum.map(fn {key, item} ->
        Stream.concat([once(encode!(key)), once(":"), encode_value_stream(item)])
      end)
      |> interleave(",")

    Stream.concat([once("{")] ++ parts ++ [once("}")])
  end

  defp encode_value_stream(_), do: raise(ArgumentError, "not a JSON value")

  defp interleave([], _separator), do: []

  defp interleave([first | rest], separator),
    do: [first | Enum.flat_map(rest, &[once(separator), &1])]

  defp once(value), do: Stream.map([value], & &1)

  @spec canonical(term()) :: binary()
  def canonical(value), do: value |> canonical_large_bodies() |> encode!()

  defp canonical_large_bodies(%AttentionServer.LargeData.Ref{size: size, sha256: hash}) do
    %{"__attention_server_large_text__" => %{"size" => size, "sha256" => hash}}
  end

  defp canonical_large_bodies(value) when is_map(value) and not is_struct(value) do
    Map.new(value, fn
      {key, %AttentionServer.LargeData.Ref{} = ref} when key in ["body", "answer", "title"] ->
        {key, canonical_large_bodies(ref)}

      {key, body}
      when key in ["body", "answer", "title"] and is_binary(body) and
             byte_size(body) > 2 * 1024 * 1024 ->
        {key,
         %{
           "__attention_server_large_text__" => %{
             "size" => byte_size(body),
             "sha256" => Base.encode16(:crypto.hash(:sha256, body), case: :lower)
           }
         }}

      {key, child} ->
        {key, canonical_large_bodies(child)}
    end)
  end

  defp canonical_large_bodies(value) when is_list(value),
    do: Enum.map(value, &canonical_large_bodies/1)

  defp canonical_large_bodies(value), do: value
end
