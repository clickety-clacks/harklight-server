defmodule AttentionServer.Repository.Postgres.Repo do
  @moduledoc false
  use Ecto.Repo, otp_app: :attention_server, adapter: Ecto.Adapters.Postgres
end
