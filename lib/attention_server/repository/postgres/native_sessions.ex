defmodule AttentionServer.Repository.Postgres.NativeSessions do
  @moduledoc false

  alias AttentionServer.DeviceProof
  alias AttentionServer.Repository.Postgres.{Attributes, Transaction}

  def resolve(bearer) do
    if DeviceProof.secret?(bearer) do
      hash = DeviceProof.digest(bearer)
      find(Base.decode16!(hash, case: :lower), hash, System.system_time(:millisecond))
    else
      {:error, :unauthorized}
    end
  end

  defp find(digest, hash, now) do
    # Native sessions follow active device authority. Unlike a browser
    # session, this credential is independent of the enrollment revision.
    case Transaction.query(
           """
           SELECT session.principal_key, session.person_id, session.attributes
           FROM harklight.auth_sessions AS session
           JOIN harklight.principals AS device
             ON device.key = session.principal_key AND device.person_id = session.person_id
           WHERE session.token_hash = $1 AND session.kind = 'native'
             AND (session.expires_at_ms IS NULL OR session.expires_at_ms > $2)
             AND device.kind = 'device' AND NOT device.revoked
           """,
           [digest, now]
         ) do
      {:ok, %{rows: [[device, person, attributes]]}} ->
        with {:ok, %{pending: pending}} when is_boolean(pending) <- Attributes.decode(attributes) do
          {:ok, %{device_id: device, person_id: person, hash: hash, pending: pending}}
        else
          _ -> {:error, :storage_unavailable}
        end

      {:ok, %{rows: []}} ->
        {:error, :unauthorized}

      {:error, _} ->
        {:error, :storage_unavailable}
    end
  end
end
