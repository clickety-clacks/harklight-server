defmodule AttentionServer.Store.KeyedTransaction do
  @moduledoc false

  alias AttentionServer.{
    AgentChangeLog,
    BrowserAuth,
    ChangeLog,
    DeviceMigration,
    NativeSignIn,
    SharedMessages
  }

  alias AttentionServer.ClientAPI.{GroupRoutes, Idempotency, SharedChat}
  alias AttentionServer.Store.{AuthSchema, DerivedIndex, Persistence, RecordTable}
  import RecordTable, only: [run: 3]

  # The ordinary FORMAT32 commit hands its worker just the encoded record
  # changes and the SQL side effects. Receipt projection and auth activation
  # may still need an Engine. Keep those inputs out of other transactions.
  def prepare(store, next, prepared, derived, log, audit) do
    receipt? = projects_receipt?(audit)

    %{
      db: store.db,
      version: store.version,
      records: prepared.records,
      deleted_paths: prepared.delete_paths,
      references: prepared.reference_rows,
      derived: derived,
      log: log,
      audit: audit,
      receipt_store: if(receipt?, do: store),
      receipt_engine: if(receipt?, do: next),
      auth_engine: if(is_map_key(audit, :auth_activation), do: prepared.engine),
      app_store:
        if(is_map_key(audit, :app_receipt_capture), do: %{store | engine: prepared.engine})
    }
  end

  defp projects_receipt?(audit) do
    is_map_key(audit, :shared_send_receipt) or
      match?({_, key, _, _} when not is_nil(key), audit[:shared_mutation_receipt]) or
      match?(%{key: key} when is_binary(key), audit[:shared_chat]) or
      is_map_key(audit, :group_receipt)
  end

  def commit(%{db: db, version: version, records: records} = transaction) do
    Persistence.run(fn ->
      result =
        RecordTable.commit(db, version, records, fn db, generation ->
          write(db, generation, transaction)
        end)

      if match?({:ok, _}, result) do
        :telemetry.execute(
          [:harklight, :store, :commit, :committed],
          %{generation: version + 1},
          %{}
        )
      end

      result
    end)
  end

  defp write(db, generation, transaction) do
    %{audit: audit, receipt_store: store, receipt_engine: engine} = transaction

    with :ok <- DerivedIndex.write(db, transaction.derived),
         :ok <- write_reference_delta(db, transaction.deleted_paths, transaction.references),
         :ok <- write_audit(db, audit),
         :ok <- write_auth_activation(db, transaction.auth_engine, audit),
         :ok <- write_auth_changes(db, audit),
         {:ok, heads, agent_heads} <- append_change_logs(db, transaction.log, generation),
         :ok <- write_shared_send_receipt(store, engine, audit),
         :ok <- write_shared_chat_receipt(store, engine, audit),
         :ok <- write_group_receipt(store, engine, audit),
         :ok <- DerivedIndex.advance(db, generation),
         :ok <- write_app_receipt(audit, transaction.app_store, generation) do
      {:ok, {heads, agent_heads}}
    end
  end

  defp write_reference_delta(db, deleted_paths, reference_rows) do
    with :ok <-
           each_ok(deleted_paths, fn path ->
             run(db, "DELETE FROM relay_large_data_refs WHERE path = ?", [{:blob, path}])
           end) do
      each_ok(reference_rows, fn
        [path, marker, id, size, sha256] ->
          run(
            db,
            "INSERT INTO relay_large_data_refs (path, marker, id, size, sha256) VALUES (?, ?, ?, ?, ?)",
            [{:blob, path}, marker, id, size, sha256]
          )

        _row ->
          {:error, :invalid_large_data_reference}
      end)
    end
  end

  defp each_ok(rows, write) do
    Enum.reduce_while(rows, :ok, fn row, :ok ->
      case write.(row) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp write_group_receipt(store, engine, %{group_receipt: receipt}),
    do: GroupRoutes.save_receipt(store, engine, receipt)

  defp write_group_receipt(_store, _engine, _audit), do: :ok

  defp write_shared_chat_receipt(store, engine, %{shared_chat: %{key: key} = chat})
       when is_binary(key) do
    with {:ok, wire} <- SharedChat.result(store, engine, chat) do
      Idempotency.save(store, chat.receipt_scope, key, chat.digest, 200, wire)
    end
  end

  defp write_shared_chat_receipt(_store, _engine, _audit), do: :ok

  defp write_shared_send_receipt(store, engine, %{
         shared_send_receipt: {{person, origin} = scope, key, digest, id, outcomes}
       }) do
    record = Map.fetch!(engine.shared_messages, id)

    with {:ok, rev} <- AgentChangeLog.head(store, scope),
         {:ok, resource_rev} <- AgentChangeLog.rev(store, scope, {:shared_message, id}) do
      body = %{
        "rev" => rev,
        "message" => SharedMessages.sender_message(engine, record, resource_rev),
        "recipients" => outcomes
      }

      Idempotency.save(store, Idempotency.agent_scope(person, origin), key, digest, 200, body)
    end
  end

  defp write_shared_send_receipt(_store, _engine, %{shared_mutation_receipt: {_, nil, _, _}}),
    do: :ok

  defp write_shared_send_receipt(store, engine, %{
         shared_mutation_receipt: {{person, origin} = scope, key, digest, id}
       }) do
    record = Map.fetch!(engine.shared_messages, id)

    with {:ok, rev} <- AgentChangeLog.head(store, scope),
         {:ok, resource_rev} <- AgentChangeLog.rev(store, scope, {:shared_message, id}) do
      body = %{
        "rev" => rev,
        "message" => SharedMessages.sender_message(engine, record, resource_rev)
      }

      Idempotency.save(store, Idempotency.agent_scope(person, origin), key, digest, 200, body)
    end
  end

  defp write_shared_send_receipt(_store, _engine, _audit), do: :ok

  defp write_auth_activation(db, engine, %{auth_activation: devices}) do
    with {:ok, %{} = _schema} <- AuthSchema.initialize_in_transaction(db, engine, devices),
         do: :ok
  end

  defp write_auth_activation(_db, _engine, _audit), do: :ok

  defp write_app_receipt(%{app_receipt_capture: capture}, store, generation),
    do: capture.(%{store | version: generation})

  defp write_app_receipt(_audit, _store, _generation), do: :ok

  def write_auth_changes(db, %{
        person: person,
        actor: owner,
        browser_session: {hash, revision, issuer}
      }),
      do: BrowserAuth.add_session(db, hash, person, owner, revision, issuer)

  def write_auth_changes(db, %{native_redemption: redemption}),
    do: NativeSignIn.commit_redemption(db, redemption)

  def write_auth_changes(db, %{person: person, auth_device_removal: device}) do
    with :ok <- NativeSignIn.remove_device_sessions(db, person, device),
         do: DeviceMigration.set_phase(db, device, "removed")
  end

  def write_auth_changes(_db, _audit), do: :ok

  def write_audit(db, audit) do
    run(db, "INSERT INTO relay_audit (person_id, actor_id, action, at_ms) VALUES (?, ?, ?, ?)", [
      audit.person,
      audit.actor,
      audit.action,
      System.system_time(:millisecond)
    ])
  end

  def append_change_logs(_db, nil, _version), do: {:ok, %{}, %{}}

  def append_change_logs(db, log, version) do
    with {:ok, heads} <-
           ChangeLog.append(db, log.pending, log.seeded, version, log.created),
         {:ok, agent_heads} <-
           AgentChangeLog.append(db, log.agent_pending, version) do
      {:ok, heads, agent_heads}
    end
  end
end
