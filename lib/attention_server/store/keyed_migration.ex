defmodule AttentionServer.Store.KeyedMigration do
  @moduledoc false

  alias AttentionServer.Store.{RecordCodec, RecordTable}
  alias Exqlite.Sqlite3

  @format 30

  def load(db) do
    # Load the codec's identity atom before decoding the anchor with :safe.
    supported_identity = RecordCodec.identity()

    with {:ok, [[version, payload, checksum]]} <-
           query(db, "SELECT version, payload, checksum FROM relay_state WHERE id = 1", []),
         true <- :crypto.hash(:sha256, payload) == checksum,
         {:ok, identity} <- decode_anchor(payload),
         :ok <- check_identity(identity, supported_identity),
         {:ok, generation} <- RecordTable.generation(db),
         true <- generation >= version,
         {:ok, engine} <- read_records(db) do
      {:ok, engine, generation}
    else
      false -> {:error, :invalid_state}
      {:ok, _} -> {:error, :invalid_state}
      error -> error
    end
  end

  defp decode_anchor(payload) do
    case :erlang.binary_to_term(payload, [:safe]) do
      {:attention_relay_keyed_state, @format, identity} -> {:ok, identity}
      {:attention_relay_keyed_state, _, _} -> {:error, :unsupported_keyed_format}
      _ -> {:error, :not_keyed}
    end
  rescue
    ArgumentError -> {:error, :invalid_state}
  end

  defp check_identity(identity, identity), do: :ok

  defp check_identity({codec, written_major}, {codec, running_major}),
    do: {:error, {:unsupported_otp_major, written_major, running_major}}

  defp check_identity(_, _), do: {:error, :unsupported_record_codec}

  defp read_records(db) do
    with {:ok, rows} <- RecordTable.rows(db), do: RecordCodec.decode(rows)
  end

  defp query(db, sql, params), do: statement(db, sql, params, &Sqlite3.fetch_all(db, &1))

  defp statement(db, sql, params, execute) do
    with {:ok, stmt} <- Sqlite3.prepare(db, sql) do
      try do
        with :ok <- Sqlite3.bind(stmt, params), do: execute.(stmt)
      after
        Sqlite3.release(db, stmt)
      end
    end
  end
end
