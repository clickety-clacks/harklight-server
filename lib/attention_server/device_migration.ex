defmodule AttentionServer.DeviceMigration do
  @moduledoc false

  alias AttentionServer.{BrowserAuth, DeviceProof, Engine, NativeSignIn}
  alias AttentionServer.Store.RecordTable
  alias Exqlite.Sqlite3

  @grant_window_ms 10 * 60 * 1_000

  @schema """
  CREATE TABLE harklight_migration_roster (
    device_id TEXT PRIMARY KEY NOT NULL,
    person_id TEXT NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('legacy', 'pending', 'activated', 'removed'))
  );
  CREATE TABLE harklight_migration_grants (
    token_hash TEXT PRIMARY KEY NOT NULL,
    device_id TEXT NOT NULL,
    expires_ms INTEGER NOT NULL,
    session_hash TEXT,
    public_key TEXT,
    label TEXT
  );
  """

  def create_tables(db), do: Sqlite3.execute(db, @schema)

  def seed(db, engine, devices) do
    Enum.reduce_while(devices, :ok, fn id, :ok ->
      case engine.devices[id] do
        %{person: person, revoked: false} ->
          case RecordTable.run(
                 db,
                 "INSERT INTO harklight_migration_roster VALUES (?, ?, 'legacy')",
                 [id, person]
               ) do
            :ok -> {:cont, :ok}
            error -> {:halt, error}
          end

        _ ->
          {:halt, {:error, :unknown_migration_device}}
      end
    end)
  end

  def phase(db, device) do
    case RecordTable.select(
           db,
           "SELECT person_id, phase FROM harklight_migration_roster WHERE device_id = ?",
           [device]
         ) do
      {:ok, [[person, phase]]} -> {:ok, person, phase}
      {:ok, []} -> {:error, :unknown_device}
      error -> error
    end
  end

  def eligible?(db, person, device) do
    case phase(db, device) do
      {:ok, ^person, phase} when phase in ["legacy", "pending"] -> true
      _ -> false
    end
  end

  def issue(db, person, device, origin, now) do
    if eligible?(db, person, device) do
      token = BrowserAuth.new_secret()

      with :ok <-
             RecordTable.run(
               db,
               "INSERT INTO harklight_migration_grants (token_hash, device_id, expires_ms) VALUES (?, ?, ?)",
               [DeviceProof.digest(token), device, now + @grant_window_ms]
             ) do
        {:ok, %{"origin" => origin, "token" => token}}
      end
    else
      {:error, :unauthorized}
    end
  end

  def redeem(db, engine, request, now) do
    with {:ok, proof} <- NativeSignIn.proof(request, "token"),
         {:ok, [[device, expires, used_hash, used_key, used_label]]} <-
           RecordTable.select(
             db,
             """
             SELECT device_id, expires_ms, session_hash, public_key, label
             FROM harklight_migration_grants WHERE token_hash = ?
             """,
             [DeviceProof.digest(proof.reference)]
           ),
         {:ok, person, phase} <- phase(db, device),
         {:ok, ^person} <- Engine.authorized_person(engine, device) do
      case {used_hash, phase} do
        {nil, "activated"} ->
          {:error, :already_migrated}

        {nil, phase} when phase in ["legacy", "pending"] and expires > now ->
          attach_pending(db, person, device, proof, now)

        {hash, phase} when is_binary(hash) and phase in ["pending", "activated"] ->
          if {hash, used_key, used_label} == {proof.hash, proof.key, proof.label},
            do: {:ok, NativeSignIn.result(person, device, nil, true), nil},
            else: {:error, :unauthorized}

        _ ->
          {:error, :unauthorized}
      end
    else
      {:ok, []} -> {:error, :unauthorized}
      {:error, :unknown_device} -> {:error, :unauthorized}
      {:error, :forbidden} -> {:error, :unauthorized}
      error -> error
    end
  end

  defp attach_pending(db, person, device, proof, now) do
    RecordTable.transaction(db, fn ->
      old = NativeSignIn.device_session_hash(db, device)

      with {:ok, previous_hash} <- old,
           :ok <- NativeSignIn.remove_device_sessions(db, person, device),
           :ok <- NativeSignIn.add_session(db, proof.hash, device, person, proof.key, now, true),
           :ok <- set_phase(db, device, "pending"),
           :ok <-
             RecordTable.run(
               db,
               """
               UPDATE harklight_migration_grants SET session_hash = ?, public_key = ?, label = ?
               WHERE token_hash = ? AND session_hash IS NULL
               """,
               [proof.hash, proof.key, proof.label, DeviceProof.digest(proof.reference)]
             ),
           {:ok, 1} <- Sqlite3.changes(db) do
        {:ok, {NativeSignIn.result(person, device, nil, false), previous_hash}}
      else
        {:ok, 0} -> {:error, :unauthorized}
        error -> error
      end
    end)
    |> case do
      {:ok, {result, previous}} -> {:ok, result, previous}
      error -> error
    end
  end

  def activate(db, engine, hash, signature) do
    with {:ok, %{device_id: device, person_id: person, public_key: public_key, pending: pending}} <-
           NativeSignIn.lookup_session(db, hash),
         {:ok, ^person} <- Engine.authorized_person(engine, device),
         {:ok, ^person, phase} <- phase(db, device),
         {:ok, key} <- DeviceProof.native_key(public_key),
         true <- DeviceProof.verify(key, DeviceProof.activation_bytes(hash), signature) do
      case {phase, pending} do
        {"activated", false} -> {:ok, device, false}
        {"pending", true} -> commit_activation(db, device, hash)
        _ -> {:error, :unauthorized}
      end
    else
      _ -> {:error, :unauthorized}
    end
  end

  defp commit_activation(db, device, hash) do
    RecordTable.transaction(db, fn ->
      with :ok <- set_phase(db, device, "activated"),
           :ok <- RecordTable.run(db, "UPDATE harklight_device_sessions SET pending = 0 WHERE session_hash = ?", [hash]) do
        {:ok, device}
      end
    end)
    |> case do
      {:ok, ^device} -> {:ok, device, true}
      error -> error
    end
  end

  def set_phase(db, device, phase) do
    RecordTable.run(db, "UPDATE harklight_migration_roster SET phase = ? WHERE device_id = ?", [phase, device])
  end
end
