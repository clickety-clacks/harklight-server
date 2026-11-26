defmodule AttentionServer.Store.AuthSchema do
  @moduledoc false

  alias AttentionServer.ClientAPI.Idempotency
  alias AttentionServer.Store.RecordTable
  alias Exqlite.Sqlite3

  @generation 1

  @schema """
  CREATE TABLE IF NOT EXISTS harklight_auth_schema (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    minimum_generation INTEGER NOT NULL CHECK (minimum_generation > 0),
    device_certificates_disabled INTEGER NOT NULL CHECK (device_certificates_disabled IN (0, 1))
  );
  """

  def supported_generation, do: @generation

  # A compatibility check never creates tables or changes an existing store.
  def read(db) do
    with {:ok, tables} <-
           RecordTable.select(
             db,
             "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'harklight_auth_schema'"
           ) do
      case tables do
        [] -> {:ok, nil}
        [[1]] -> read_metadata(db)
      end
    end
  end

  # This runs under the startup writer before the legacy receipt pruner or any
  # request can run. A restart must not import the later browser-only receipts.
  def initialize(db) do
    RecordTable.transaction(db, fn ->
      case read(db) do
        {:ok, nil} -> initialize_generation(db)
        result -> result
      end
    end)
  end

  defp initialize_generation(db) do
    with :ok <- Sqlite3.execute(db, @schema),
         :ok <- Idempotency.retain_person_receipts(db),
         :ok <-
           RecordTable.run(
             db,
             "INSERT INTO harklight_auth_schema VALUES (1, ?, 0)",
             [@generation]
           ) do
      read_metadata(db)
    end
  end

  defp read_metadata(db) do
    with {:ok, rows} <-
           RecordTable.select(
             db,
             "SELECT minimum_generation, device_certificates_disabled FROM harklight_auth_schema WHERE id = 1"
           ) do
      case rows do
        [[@generation, disabled]] when disabled in [0, 1] ->
          {:ok, %{minimum_generation: @generation, device_certificates_disabled: disabled == 1}}

        [[generation, _]] when is_integer(generation) and generation > @generation ->
          {:error, :auth_generation_unsupported}

        _ ->
          {:error, :invalid_auth_schema}
      end
    end
  end
end
