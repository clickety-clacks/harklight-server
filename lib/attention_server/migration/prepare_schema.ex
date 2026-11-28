defmodule AttentionServer.Migration.PrepareSchema do
  @moduledoc false
  @behaviour Postgrex.SimpleConnection
  alias AttentionServer.Repository.Postgres.Schema
  alias Postgrex.SimpleConnection

  # Explicit migration entry only: never called by application startup. The
  # session advisory lock spans the original SQL files' individual transactions.
  # No statement splitting or rewriting of the shipped migration bytes.
  def run(options, identity, roles \\ %{migrator: "harklight_migrator", runtime: "harklight"})
  def run(options, %{id: id, source: source, schema: schema} = identity, %{migrator: migrator, runtime: runtime} = roles)
      when is_binary(id) and byte_size(id) > 0 and byte_size(source) == 32 and byte_size(schema) == 32 and is_binary(migrator) and is_binary(runtime) and migrator != runtime do
    options = options |> Keyword.delete(:pool_size) |> Keyword.put(:auto_reconnect, false)
    with {:ok, connection} <- SimpleConnection.start_link(__MODULE__, [], options) do
      try do
        with :ok <- roles(connection, roles),
             {:ok, [["t"]]} <- rows(connection, "SELECT pg_try_advisory_lock(1212240455,1)"),
             :ok <- private_destination(connection),
             {:ok, version} <- version(connection),
             :ok <- custody(connection, version, identity),
             :ok <- apply_files(connection, version),
             {:ok, actual} <- version(connection),
             true <- actual == Schema.version() do
          {:ok, %{schema_version: actual}}
        else
          {:error, _} = error -> error
          _ -> {:error, :schema_preparation_refused}
        end
      after
        GenServer.stop(connection)
      end
    end
  end
  def run(_, _, _), do: {:error, :invalid_import_identity}

  # Role names come from the installer contract, not from the login being tested.
  # Refuse before any DDL, even when the schema is already current.
  defp roles(connection, %{migrator: migrator, runtime: runtime}) do
    sql = """
    SELECT current_user::text,session_user::text,pg_get_userbyid(d.datdba),
      m.rolsuper,m.rolcreaterole,m.rolreplication,m.rolbypassrls,
      r.rolsuper,r.rolcreatedb,r.rolcreaterole,r.rolreplication,r.rolbypassrls,
      pg_has_role(r.oid,m.oid,'MEMBER'),has_database_privilege(r.oid,d.oid,'CREATE'),
      EXISTS(SELECT 1 FROM pg_namespace n WHERE n.nspname IN ('public','harklight') AND has_schema_privilege(r.oid,n.oid,'CREATE')),
      EXISTS(SELECT 1 FROM pg_namespace n WHERE n.nspname='harklight' AND n.nspowner<>m.oid),
      EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='harklight' AND c.relowner<>m.oid)
    FROM pg_database d JOIN pg_roles m ON m.rolname=#{literal(migrator)}
      JOIN pg_roles r ON r.rolname=#{literal(runtime)} WHERE d.datname=current_database()
    """
    expected = [migrator, migrator, migrator] ++ List.duplicate("f", 14)
    case rows(connection, sql) do
      {:ok, [^expected]} -> :ok
      {:ok, _} -> {:error, :migration_role_boundary_required}
      error -> error
    end
  end

  defp literal(value), do: "convert_from(decode('" <> Base.encode16(value, case: :lower) <> "','hex'),'UTF8')"

  defp private_destination(connection) do
    case rows(connection, """
      SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname NOT IN ('harklight','information_schema')
        AND n.nspname NOT LIKE 'pg_%' AND c.relkind IN ('r','p','v','m','S','f')
      """) do
      {:ok, [["0"]]} -> :ok
      {:ok, _} -> {:error, :destination_not_empty}
      error -> error
    end
  end

  defp version(connection) do
    with {:ok, [[exists, schema_exists]]} <- rows(connection,
           "SELECT to_regclass('harklight.schema_migrations') IS NOT NULL, EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='harklight')") do
      case {exists, schema_exists} do
        {"f", "f"} -> {:ok, 0}
        {"t", "t"} ->
          with {:ok, values} <- rows(connection, "SELECT version FROM harklight.schema_migrations ORDER BY version"),
               versions = Enum.map(values, fn [text] -> String.to_integer(text) end),
               last when is_integer(last) and last > 0 <- List.last(versions),
               true <- last <= Schema.version() and versions == Enum.to_list(1..last) do
            {:ok, last}
          else
            _ -> {:error, :unsupported_schema}
          end
        _ -> {:error, :unsupported_schema}
      end
    end
  end

  defp custody(_, 0, _), do: :ok
  defp custody(connection, _, identity) do
    with {:ok, states} <- rows(connection,
           "SELECT import_id,encode(source_sha256,'hex'),encode(schema_sha256,'hex'),phase FROM harklight.import_state") do
      expected = [identity.id, hex(identity.source), hex(identity.schema), "loading"]
      case states do
        [^expected] -> :ok
        [] -> empty_schema(connection)
        _ -> {:error, :import_identity_or_phase_mismatch}
      end
    end
  end

  defp empty_schema(connection) do
    with {:ok, tables} <- rows(connection,
           "SELECT tablename FROM pg_tables WHERE schemaname='harklight' AND tablename <> 'schema_migrations' ORDER BY tablename") do
      Enum.reduce_while(tables, :ok, fn [table], :ok ->
        quoted = "\"" <> String.replace(table, "\"", "\"\"") <> "\""
        case rows(connection, "SELECT EXISTS(SELECT 1 FROM harklight." <> quoted <> " LIMIT 1)") do
          {:ok, [["f"]]} -> {:cont, :ok}
          {:ok, _} -> {:halt, {:error, :destination_not_empty}}
          error -> {:halt, error}
        end
      end)
    end
  end

  defp apply_files(connection, version) do
    Schema.migration_files() |> Enum.drop(version) |> Enum.reduce_while(:ok, fn name, :ok ->
      path = Application.app_dir(:attention_server, "priv/postgres/" <> name)
      with {:ok, sql} <- File.read(path), :ok <- execute(connection, sql) do
        {:cont, :ok}
      else
        _ -> {:halt, {:error, :schema_migration_failed}}
      end
    end)
  end

  defp execute(connection, sql) do
    case SimpleConnection.call(connection, {:query, sql}, 75_000) do
      {:ok, _} -> :ok
      error -> error
    end
  end
  defp rows(connection, sql) do
    case SimpleConnection.call(connection, {:query, sql}, 15_000) do
      {:ok, [%Postgrex.Result{rows: rows}]} -> {:ok, rows}
      {:error, _} = error -> error
      _ -> {:error, :schema_query_failed}
    end
  end
  defp hex(bytes), do: Base.encode16(bytes, case: :lower)

  @impl true
  def init(_), do: {:ok, nil}
  @impl true
  def handle_call({:query, sql}, from, nil), do: {:query, sql, from}
  @impl true
  def handle_result(results, from) when is_list(results) do
    SimpleConnection.reply(from, {:ok, results})
    {:noreply, nil}
  end
  def handle_result(%Postgrex.Error{}, from) do
    SimpleConnection.reply(from, {:error, :schema_query_failed})
    {:noreply, nil}
  end
end
