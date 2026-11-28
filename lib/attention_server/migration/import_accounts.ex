defmodule AttentionServer.Migration.ImportAccounts do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportFacts, ImportRows, ImportSession}
  alias AttentionServer.Repository.Postgres.Attributes
  alias AttentionServer.JSON

  @settings [:algorithm_choices, :alert_choices, :bounce_levels]
  @phases [
    people: [:persons],
    profiles: [:people_profiles, :delivery_states | @settings],
    principals: [:owners, :devices, :agents],
    grants: [:sender_grants, :origin_sessions],
    highs: [:free_order_highs, :conversation_highs]
  ]

  def run(db, identity, root) do
    with {:ok, _} <- ImportFacts.run(db, identity, root),
         {:ok, manifest, _} <- Bundle.verify(root),
         :ok <- import_identity(db, identity, root, manifest) do
      Enum.reduce_while(@phases, :ok, fn {phase, fields}, :ok ->
        select = fn
          {{:row, field, _}, _} -> field in fields
          _ -> false
        end

        case ImportRows.run(
               db,
               identity,
               "accounts/" <> Atom.to_string(phase) <> "/v1",
               Path.join(root, "records.frames"),
               manifest.data.records,
               select,
               &write/2
             ) do
          {:ok, _} -> {:cont, :ok}
          error -> {:halt, error}
        end
      end)
    end
  end

  defp import_identity(db, identity, root, manifest) do
    with {:ok, values} <-
           Frames.fold(Path.join(root, "records.frames"), manifest.data.records, %{}, fn
             {{:field, field}, value}, acc
             when field in [:relay_instance_id, :agent_cursor_key] ->
               {:ok, Map.put(acc, field, value)}

             _, acc ->
               {:ok, acc}
           end),
         %{relay_instance_id: instance, agent_cursor_key: key} <- values,
         true <-
           is_binary(instance) and byte_size(instance) > 0 and is_binary(key) and
             byte_size(key) > 0,
         {:ok, epoch_table} <- Map.fetch(manifest.data.tables, "client_change_log_epoch"),
         {:ok, [[1, epoch]]} <-
           Frames.fold(Path.join(root, epoch_table.file), epoch_table.summary, [], fn row, acc ->
             {:ok, [row | acc]}
           end),
         true <- is_integer(epoch) and epoch > 0,
         {:ok, checkpoint} <- ImportSession.checkpoint(db, identity, "accounts/identity/v1") do
      entry = {instance, key, epoch}

      case checkpoint.ordinal do
        0 ->
          case ImportSession.batch(
                 db,
                 identity,
                 "accounts/identity/v1",
                 checkpoint,
                 [entry],
                 fn tx, _ ->
                   execute(
                     tx,
                     "INSERT INTO harklight.server_identity (instance_id,cursor_signing_key,cursor_epoch) VALUES ($1,$2,$3)",
                     [instance, key, epoch]
                   )
                 end
               ) do
            {:ok, _} -> :ok
            error -> error
          end

        1 ->
          expected =
            ImportSession.advance(%{ordinal: 0, digest: :crypto.hash(:sha256, <<>>)}, [entry])

          if checkpoint == expected, do: :ok, else: {:error, :checkpoint_source_mismatch}

        _ ->
          {:error, :checkpoint_source_mismatch}
      end
    else
      {:error, _} = error -> error
      _ -> {:error, :source_identity_missing}
    end
  end

  defp write(db, entries) do
    Enum.reduce_while(entries, :ok, fn entry, :ok ->
      case row(db, entry) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp row(db, {{:row, :persons, person}, high}) do
    with :ok <- execute(db, "INSERT INTO harklight.people(id) VALUES ($1)", [person]),
         do: high(db, person, "accepted", high)
  end

  defp row(db, {{:row, :people_profiles, person}, profile}),
    do:
      execute(db, "UPDATE harklight.people SET profile=$2::text::jsonb WHERE id=$1", [
        person,
        encode(profile)
      ])

  defp row(db, {{:row, :delivery_states, person}, state}),
    do: execute(db, "UPDATE harklight.people SET delivery_state=$2 WHERE id=$1", [person, state])

  defp row(db, {{:row, field, person}, value}) when field in @settings do
    with {:ok, %{rows: [[text]]}} <-
           ImportSession.query(db, "SELECT settings::text FROM harklight.people WHERE id=$1", [
             person
           ]),
         {:ok, envelope} <- JSON.decode_line(text <> "\n"),
         {:ok, settings} <- settings(envelope) do
      execute(db, "UPDATE harklight.people SET settings=$2::text::jsonb WHERE id=$1", [
        person,
        encode(Map.put(settings, field, value))
      ])
    else
      {:error, _} = error -> error
      _ -> {:error, :missing_person}
    end
  end

  defp row(db, {{:row, :owners, person}, %{key: key, revision: revision} = value}),
    do: principal(db, key, person, "owner", revision, false, value)

  defp row(
         db,
         {{:row, field, key},
          %{person: person, enrollment_revision: revision, revoked: revoked} = value}
       )
       when field in [:devices, :agents],
       do:
         principal(
           db,
           key,
           person,
           if(field == :devices, do: "device", else: "agent"),
           revision,
           revoked,
           value
         )

  defp row(db, {{:row, :sender_grants, {person, key}}, %MapSet{} = patterns}) do
    Enum.reduce_while(Enum.sort(patterns), :ok, fn pattern, :ok ->
      case execute(
             db,
             """
             INSERT INTO harklight.sender_grants(principal_key,pattern)
             SELECT key,$3 FROM harklight.principals WHERE person_id=$1 AND key=$2
             """,
             [person, key, pattern]
           ) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp row(db, {{:row, :origin_sessions, {person, origin, handle}}, value}),
    do:
      execute(
        db,
        "INSERT INTO harklight.origin_sessions(person_id,origin_key,handle,attributes) VALUES ($1,$2,$3,$4::text::jsonb)",
        [person, origin, handle, encode(value)]
      )

  defp row(db, {{:row, field, person}, value})
       when field in [:free_order_highs, :conversation_highs],
       do:
         high(
           db,
           person,
           if(field == :free_order_highs, do: "free_order", else: "conversation"),
           value
         )

  defp row(_, _), do: {:error, :unsupported_account_fact}

  defp settings(map) when map == %{}, do: {:ok, %{}}
  defp settings(envelope), do: Attributes.decode(envelope)

  defp high(db, person, kind, value),
    do:
      execute(
        db,
        "INSERT INTO harklight.allocation_highs(person_id,origin_key,session_handle,kind,high) VALUES ($1,'','',$2,$3)",
        [person, kind, value]
      )

  defp principal(db, key, person, kind, revision, revoked, value),
    do:
      execute(
        db,
        "INSERT INTO harklight.principals(key,person_id,kind,revision,revoked,attributes) VALUES ($1,$2,$3,$4,$5,$6::text::jsonb)",
        [key, person, kind, revision, revoked, encode(value)]
      )

  defp encode(value), do: JSON.encode!(Attributes.encode(value))

  defp execute(db, sql, params) do
    case ImportSession.query(db, sql, params) do
      {:ok, %{num_rows: 1}} -> :ok
      {:ok, _} -> {:error, :import_row_count_mismatch}
      error -> error
    end
  end
end
