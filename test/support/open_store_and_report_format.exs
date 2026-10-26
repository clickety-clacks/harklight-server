alias AttentionServer.Store
alias Exqlite.Sqlite3

{:ok, _started} = Application.ensure_all_started(:crypto)
path = Path.join(System.fetch_env!("HARKLIGHT_TEST_ROOT"), "state/relay.sqlite3")
{:ok, store} = Store.open(path)
:ok = Store.close(store)

{:ok, db} = Sqlite3.open(path)
{:ok, statement} = Sqlite3.prepare(db, "SELECT payload FROM relay_state WHERE id = 1")
{:row, [payload]} = Sqlite3.step(db, statement)
:ok = Sqlite3.release(db, statement)
:ok = Sqlite3.close(db)
{:attention_relay_state, format, _engine} = :erlang.binary_to_term(payload, [:safe])

File.write!(System.fetch_env!("HARKLIGHT_TEST_MIGRATION_LOG"), "format=#{format}\n")
IO.puts("candidate store opened, migrated, and closed at format #{format}")
