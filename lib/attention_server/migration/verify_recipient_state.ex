defmodule AttentionServer.Migration.VerifyRecipientState do
  @moduledoc false
  alias AttentionServer.{BounceDecision, ClientAPI.Resources}
  alias AttentionServer.Migration.{Bundle, Frames, ImportAnswers, ImportLargeText, ImportMessages, ImportRecipients, ImportSession, VerifyMessages}
  alias AttentionServer.Repository.Postgres.Attributes

  def run(db, identity, bundle) do
    with {:ok, _} <- VerifyMessages.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, answers} <- Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, 0, fn
           {{:row, :messages, key} = address, record}, count ->
             with {:ok, projected} <- ImportLargeText.project(db, address, record),
                  :ok <- recipient(db, key, projected),
                  {:ok, n} <- answer(db, key, projected) do
               {:ok, count + n}
             end
           _, count -> {:ok, count}
         end),
         {:ok, %{rows: [[^answers]]}} <- ImportSession.query(db,
           "SELECT count(*) FROM harklight.answers a JOIN harklight.messages m ON m.id=a.message_id WHERE m.kind='ordinary'", []) do
      {:ok, %{ordinary_answers: answers}}
    else
      {:error, _} = error -> error
      _ -> {:error, :ordinary_answer_count_mismatch}
    end
  end

  defp recipient(db, {person, sender, external} = key, record) do
    id = ImportMessages.id(key, record)
    with {:ok, bounce} <- fact(db, "bounce_revisions", {:row, :bounce_revisions, key}),
         {:ok, revision} <- ImportRecipients.substantive(record, bounce),
         {:ok, opened} when opened in [nil, true] <- fact(db, "opened_revisions",
           {:member, :opened_revisions, {person, sender, external, revision}}),
         seen = if(opened == true, do: revision),
         expected = Attributes.encode(%{visible: BounceDecision.person_visible?(record),
           substantive_revision: revision, origin_result: Map.get(record, :origin_result)}),
         {:ok, %{rows: [[^seen, ^expected]]}} <- ImportSession.query(db,
           "SELECT seen_revision,attributes FROM harklight.message_recipients WHERE message_id=$1 AND person_id=$2", [id, person]) do
      :ok
    else
      {:error, _} = error -> error
      _ -> mismatch(:recipient, id)
    end
  end

  defp answer(db, {person, _, _} = key, %{terminal: %{state: "answered"}} = record) do
    message = ImportMessages.id(key, record)
    revision = Map.get(record, :revision, 1)
    expected = [ImportAnswers.id(message, revision), person, revision,
      Attributes.encode(%{terminal: record.terminal, item_id: Resources.item_id(record)})]
    case ImportSession.query(db,
      "SELECT id,person_id,request_revision,body FROM harklight.answers WHERE message_id=$1", [message]) do
      {:ok, %{rows: [^expected]}} -> {:ok, 1}
      {:error, _} = error -> error
      _ -> mismatch(:answer, message)
    end
  end
  defp answer(db, key, record) do
    id = ImportMessages.id(key, record)
    case ImportSession.query(db, "SELECT count(*) FROM harklight.answers WHERE message_id=$1", [id]) do
      {:ok, %{rows: [[0]]}} -> {:ok, 0}
      {:error, _} = error -> error
      _ -> mismatch(:unexpected_answer, id)
    end
  end

  defp fact(db, family, address) do
    case ImportSession.query(db,
      "SELECT body FROM harklight.compatibility_facts WHERE family=$1 AND address=$2", [family, Attributes.key(address)]) do
      {:ok, %{rows: []}} -> {:ok, nil}
      {:ok, %{rows: [[body]]}} -> Attributes.decode(body)
      {:error, _} = error -> error
    end
  end
  defp mismatch(field, id), do: {:error, {:recipient_state_mismatch, field,
    Base.encode16(:crypto.hash(:sha256, id), case: :lower)}}
end
