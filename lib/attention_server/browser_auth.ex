defmodule AttentionServer.BrowserAuth do
  @moduledoc false

  alias AttentionServer.Store.RecordTable
  alias Exqlite.Sqlite3

  @cookie "__Host-harklight"
  @browser_max_age 10 * 365 * 24 * 60 * 60
  @grant_lifetime_ms 10 * 60 * 1_000
  @hosted_methods ~w(google apple email_code passkey)

  @schema """
  CREATE TABLE harklight_browser_sessions (
    session_hash BLOB PRIMARY KEY NOT NULL,
    person_id TEXT NOT NULL,
    owner_key TEXT NOT NULL,
    owner_revision INTEGER NOT NULL,
    issuer TEXT NOT NULL
  );
  CREATE TABLE harklight_browser_grants (
    grant_hash BLOB PRIMARY KEY NOT NULL,
    transaction_id TEXT NOT NULL,
    person_id TEXT NOT NULL,
    owner_key TEXT NOT NULL,
    owner_revision INTEGER NOT NULL,
    method TEXT NOT NULL,
    expires_ms INTEGER NOT NULL
  );
  """

  def create_tables(db), do: Sqlite3.execute(db, @schema)

  def put_cookie(conn, value, persistent?) do
    options = [secure: true, http_only: true, same_site: "Lax", path: "/"]
    options = if persistent?, do: Keyword.put(options, :max_age, @browser_max_age), else: options
    Plug.Conn.put_resp_cookie(conn, @cookie, value, options)
  end

  def new_secret, do: :crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false)
  def hash(value) when is_binary(value), do: :crypto.hash(:sha256, value)

  def issue_hosted(db, transaction, person, key, revision, method, now)
      when is_binary(transaction) and byte_size(transaction) in 1..256 and method in @hosted_methods do
    grant = new_secret()
    grant_hash = {:blob, hash(grant)}

    with :ok <-
           RecordTable.run(
             db,
             """
             INSERT INTO harklight_browser_grants
               (grant_hash, transaction_id, person_id, owner_key, owner_revision, method, expires_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             """,
             [grant_hash, transaction, person, key, revision, method, now + @grant_lifetime_ms]
           ) do
      {:ok, grant}
    end
  end

  def issue_hosted(_, _, _, _, _, _, _), do: {:error, :invalid_request}

  def redeem_hosted(db, engine, grant, transaction, now)
      when is_binary(grant) and byte_size(grant) == 43 and is_binary(transaction) do
    grant_hash = {:blob, hash(grant)}

    RecordTable.transaction(db, fn ->
      with {:ok, [[person, key, revision, method]]} <-
             RecordTable.select(
               db,
               """
               SELECT person_id, owner_key, owner_revision, method
               FROM harklight_browser_grants
               WHERE grant_hash = ? AND transaction_id = ? AND expires_ms > ?
               """,
               [grant_hash, transaction, now]
             ),
           {:ok, ^revision} <- AttentionServer.Engine.owner_revision(engine, person, key),
           :ok <- RecordTable.run(db, "DELETE FROM harklight_browser_grants WHERE grant_hash = ?", [grant_hash]) do
        cookie = new_secret()

        with :ok <- add_session(db, hash(cookie), person, key, revision, "hosted"),
             do: {:ok, %{cookie: cookie, method: method}}
      else
        {:ok, _} -> {:error, :invalid_sign_in}
        {:error, :forbidden} -> {:error, :invalid_sign_in}
        error -> error
      end
    end)
  end

  def redeem_hosted(_, _, _, _, _), do: {:error, :invalid_sign_in}

  # The caller may compose session issuance with relay changes in the same transaction.
  def add_session(db, hash, person, key, revision, issuer) do
    RecordTable.run(
      db,
      """
      INSERT INTO harklight_browser_sessions
        (session_hash, person_id, owner_key, owner_revision, issuer)
      VALUES (?, ?, ?, ?, ?)
      """,
      [{:blob, hash}, person, key, revision, issuer]
    )
  end

  def session(db, hash) do
    with {:ok, rows} <-
           RecordTable.select(
             db,
             """
             SELECT person_id, owner_key, owner_revision FROM harklight_browser_sessions
             WHERE session_hash = ?
             """,
             [{:blob, hash}]
           ) do
      case rows do
        [[person, key, revision]] -> {:ok, %{person: person, key: key, revision: revision}}
        [] -> {:error, :invalid_session}
      end
    end
  end

  def remove_session(db, hash) do
    RecordTable.run(db, "DELETE FROM harklight_browser_sessions WHERE session_hash = ?", [{:blob, hash}])
  end
end
