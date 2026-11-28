defmodule AttentionServer.Repository.Postgres.Identities do
  @moduledoc false

  alias AttentionServer.BrowserAuth
  alias AttentionServer.Repository.Postgres.Transaction

  @browser_scope """
  SELECT session.person_id, session.principal_key, session.principal_revision
  FROM harklight.auth_sessions AS session
  JOIN harklight.principals AS owner
    ON owner.person_id = session.person_id AND owner.key = session.principal_key
  WHERE session.token_hash = $1 AND session.kind = 'browser'
    AND (session.expires_at_ms IS NULL OR session.expires_at_ms > $2)
    AND owner.kind = 'owner' AND NOT owner.revoked
    AND owner.revision = session.principal_revision
  """

  # A read can use this scope in a CTE to authorize and select data from
  # the same committed snapshot.
  def browser_scope_sql, do: @browser_scope

  # Implements the browser backend callback used by ClientAPI.Auth. The
  # cookie is hashed with the same bytes used by the FORMAT32 session table.
  def browser_context(cookie) when is_binary(cookie) do
    hash = BrowserAuth.hash(cookie)

    case Transaction.query(@browser_scope, [hash, System.system_time(:millisecond)]) do
      {:ok, %{rows: [[person, key, revision]]}} ->
        {:ok, %{person: person, key: key, revision: revision, browser_session_id: hash}}

      {:ok, %{rows: []}} ->
        {:error, :session_ended}

      {:error, _} ->
        {:error, :storage_unavailable}
    end
  end

  def owner_identity(key) do
    case Transaction.query(
           """
           SELECT person_id, revision FROM harklight.principals
           WHERE key = $1 AND kind = 'owner' AND NOT revoked
           """,
           [key]
         ) do
      {:ok, %{rows: [[person, revision]]}} -> {:ok, person, revision}
      {:ok, %{rows: []}} -> {:error, :forbidden}
      {:error, _} -> {:error, :storage_unavailable}
    end
  end
end
