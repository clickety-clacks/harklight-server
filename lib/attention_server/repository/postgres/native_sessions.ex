defmodule AttentionServer.Repository.Postgres.NativeSessions do
  @moduledoc false

  alias AttentionServer.DeviceProof
  alias AttentionServer.Repository.Postgres.{Attributes, Transaction}

  # Native authority follows the device without pinning its enrollment revision.
  @scope_sql """
  SELECT session.principal_key, session.person_id, session.attributes
  FROM harklight.auth_sessions AS session
  JOIN harklight.principals AS device
    ON device.key = session.principal_key AND device.person_id = session.person_id
  WHERE session.token_hash = $1 AND session.kind = 'native'
    AND (session.expires_at_ms IS NULL OR session.expires_at_ms > $2)
    AND device.kind = 'device' AND NOT device.revoked
  """

  def resolve(bearer) do
    if DeviceProof.secret?(bearer) do
      hash = DeviceProof.digest(bearer)
      params = [Base.decode16!(hash, case: :lower), System.system_time(:millisecond)]
      find(@scope_sql, params, hash)
    else
      {:error, :unauthorized}
    end
  end

  def lock(%{session_id: hash, person: person, key: device}, at) when is_binary(hash) do
    with {:ok, <<_::256>> = digest} <- Base.decode16(hash, case: :lower) do
      sql = @scope_sql <> """
        AND session.person_id = $3 AND session.principal_key = $4
        FOR SHARE OF session, device
      """

      find(sql, [digest, at, person, device], hash)
    else
      _ -> {:error, :unauthorized}
    end
  end

  def lock(_principal, _at), do: {:error, :unauthorized}

  defp find(sql, params, hash) do
    case Transaction.query(sql, params) do
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
