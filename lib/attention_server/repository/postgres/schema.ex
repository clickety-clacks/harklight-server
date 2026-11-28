defmodule AttentionServer.Repository.Postgres.Schema do
  @moduledoc false

  @migration_files [
    "001_initial.sql",
    "002_message_history.sql",
    "003_large_text.sql",
    "004_conversation_read_positions.sql"
  ]
  @version 4

  def version, do: @version
  def migration_files, do: @migration_files

  def digest do
    root = Application.app_dir(:attention_server, "priv/postgres")
    contents = Enum.map(@migration_files, &File.read!(Path.join(root, &1)))
    :crypto.hash(:sha256, contents)
  end
end
