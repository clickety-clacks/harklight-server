defmodule AttentionServer.SharedAnswers do
  @moduledoc false

  alias AttentionServer.{Engine, Message}
  alias AttentionServer.ClientAPI.PeopleProfiles

  def answer(engine, record, recipient, person, revision, value, at) do
    with :ok <- answer_precondition(engine, record, recipient, person, revision),
         {:ok, raw, outcome} <- format_answer(record.message, value) do
      id = answer_id(record.id, person, revision)
      answer = %{
        id: id, shared_id: record.id, person: person, request_revision: revision,
        at_ms: at, speaker: PeopleProfiles.speaker(engine, person), raw: raw, outcome: outcome
      }

      {:ok, put_answer(engine, record, recipient, answer), id}
    end
  end

  defp put_answer(engine, record, recipient, answer) do
    next = Engine.put_row(engine, :shared_answers, answer.id, answer)

    if record.answer_policy == :everyone do
      Engine.put_row(next, :shared_recipients, {record.id, answer.person}, Map.put(recipient, :answer_id, answer.id))
    else
      Engine.put_row(next, :shared_messages, record.id, Map.put(record, :answer_id, answer.id))
    end
  end

  def next_default(engine) do
    engine
    |> pending_defaults()
    |> Enum.map(fn {_, _, _, default} -> default.at end)
    |> Enum.min(fn -> nil end)
  end

  def apply_defaults(engine, now) do
    engine
    |> pending_defaults()
    |> Enum.filter(fn {_, _, _, default} -> default.at <= now end)
    |> Enum.reduce({engine, []}, fn {record, recipient, person, default}, {next, ids} ->
      id = answer_id(record.id, person, record.revision)
      outcome = %{"kind" => "default", "value" => default.value}
      outcome = if is_binary(default.text), do: Map.put(outcome, "text", default.text), else: outcome
      answer = %{
        id: id, shared_id: record.id, person: person, request_revision: record.revision,
        at_ms: now, speaker: %{"kind" => "server", "id" => engine.relay_instance_id, "display_name" => "Harklight Server"},
        raw: {:stored, default.value}, outcome: outcome
      }

      {put_answer(next, record, recipient, answer), [id | ids]}
    end)
  end

  defp pending_defaults(engine) do
    for {{message_id, person}, recipient} <- engine.shared_recipients,
        not Map.has_key?(recipient, :frozen),
        %{answer_policy: :everyone} = record <- [engine.shared_messages[message_id]],
        available?(engine, record, recipient, person),
        accepted = Map.get(record, :revision_accepted_at_ms, record["created_at_ms"]),
        %{at: _} = default <- [Message.default_answer(record.message, accepted)],
        do: {record, recipient, person, default}
  end

  defp answer_precondition(engine, record, recipient, person, revision) do
    cond do
      record.revision != revision -> {:error, :stale_request}
      not eligible?(record.answer_policy, person) -> {:error, :forbidden}
      answer = resolved_answer(engine, record, recipient) ->
        {:answer_conflict, person_terminal(answer, person)}

      Map.has_key?(record, :terminal) -> {:error, :already_closed}
      true -> :ok
    end
  end

  def available?(engine, record, recipient, person) do
    eligible?(record.answer_policy, person) and not Map.has_key?(record, :terminal) and
      is_nil(resolved_answer(engine, record, recipient))
  end

  defp eligible?(nil, _person), do: false
  defp eligible?({:named, people}, person), do: person in people
  defp eligible?(_policy, _person), do: true

  defp resolved_answer(engine, record, recipient) do
    id = if record.answer_policy == :everyone, do: Map.get(recipient, :answer_id), else: Map.get(record, :answer_id)
    engine.shared_answers[id]
  end

  def person_projection(engine, record, recipient, person) do
    case Map.get(recipient, :frozen) do
      %{answers: answers, effective_answer_id: id} ->
        %{terminal: terminal_for_person(engine, id, person, record),
          answers: projected_answers(engine, answers, {:person, person})}

      _ ->
        answer = resolved_answer(engine, record, recipient)
        terminal =
          cond do
            get_in(record, [:terminal, "state"]) == "retracted" -> record.terminal
            answer -> person_terminal(answer, person)
            true -> Map.get(record, :terminal)
          end
        terminal = if terminal && terminal["state"] == "cleared", do: Map.put(terminal, "by_this_person", false), else: terminal
        %{terminal: terminal, answers: projected_answers(engine, answer_ids(engine, record), {:person, person})}
    end
  end

  def sender_projection(engine, record) do
    {answer_id, ids} =
      if Map.has_key?(record, :sender_frozen_revision) do
        {Map.get(record, :sender_frozen_answer_id), Map.get(record, :sender_frozen_answers, [])}
      else
        {Map.get(record, :answer_id), answer_ids(engine, record)}
      end

    terminal =
      cond do
        get_in(record, [:terminal, "state"]) == "retracted" -> record.terminal
        answer = engine.shared_answers[answer_id] -> sender_terminal(answer)
        true -> Map.get(record, :terminal)
      end

    %{terminal: terminal, answers: projected_answers(engine, ids, :sender)}
  end

  def freeze_person(engine, record, recipient) do
    answer = resolved_answer(engine, record, recipient)
    %{answers: answer_ids(engine, record), effective_answer_id: answer && answer.id}
  end

  def answer_ids(engine, %{answer_policy: :everyone, id: id}) do
    ids =
      for {{^id, person}, %{answer_id: answer_id}} <- engine.shared_recipients,
          do: {person, answer_id}

    Enum.sort(ids)
  end

  def answer_ids(_engine, _record), do: []

  defp projected_answers(engine, ids, direction) do
    Enum.map(ids, fn {person, id} ->
      answer = Map.fetch!(engine.shared_answers, id)
      terminal =
        case direction do
          :sender -> sender_terminal(answer)
          {:person, viewer} -> person_terminal(answer, viewer)
        end

      %{"person_id" => person, "terminal" => terminal}
    end)
  end

  defp terminal_for_person(engine, id, person, record) do
    case engine.shared_answers[id] do
      nil -> Map.get(record, :terminal)
      answer -> person_terminal(answer, person)
    end
  end

  def person_terminal(answer, viewer) do
    %{
      "state" => "answered", "at_ms" => answer.at_ms, "answer_id" => answer.id,
      "speaker" => answer.speaker, "answer" => answer.outcome,
      "by_this_person" => answer.speaker["kind"] == "person" and answer.person == viewer
    }
  end

  def sender_terminal(answer) do
    delivery = Map.merge(%{"delivered" => Map.get(answer, :served, false)}, Map.get(answer, :handoff, %{}))
    result = %{
      "state" => "answered", "at_ms" => answer.at_ms, "answer_id" => answer.id,
      "speaker" => answer.speaker, "outcome" => answer.outcome, "delivery" => delivery
    }

    case answer.raw do
      :absent -> result
      {:stored, value} -> Map.put(result, "answer", value)
    end
  end

  def event(answer) do
    %{
      "item_id" => answer.shared_id, "answer_id" => answer.id, "person_id" => answer.person,
      "request_revision" => answer.request_revision, "terminal" => sender_terminal(answer)
    }
  end

  def mark_served(engine, ids) do
    Enum.reduce(ids, engine, fn id, next ->
      case next.shared_answers[id] do
        %{} = answer -> Engine.put_row(next, :shared_answers, id, Map.put(answer, :served, true))
        nil -> next
      end
    end)
  end

  def receipt(engine, %{served: true} = answer, handoff) do
    prior = Map.get(answer, :handoff)

    {status, effective} =
      cond do
        is_nil(prior) -> {"recorded", handoff}
        prior == handoff -> {"already_recorded", prior}
        prior["outcome"] == "handed" -> {"superseded", prior}
        handoff["outcome"] == "handed" -> {"recorded", handoff}
        true -> {"superseded", prior}
      end

    next = Engine.put_row(engine, :shared_answers, answer.id, Map.put(answer, :handoff, effective))
    {:ok, next, %{"status" => status, "stage_outcome" => effective["outcome"]}}
  end

  def receipt(_engine, _answer, _handoff), do: {:error, :not_answered}

  def valid?(%Engine{} = engine, id, %{
        id: id, shared_id: shared_id, person: person, request_revision: revision,
        at_ms: at, speaker: speaker, outcome: outcome, raw: raw
      }) do
    case engine.shared_messages[shared_id] do
      %{revision: current} ->
        is_binary(id) and revision in 1..current and is_integer(at) and
          is_map(outcome) and (raw == :absent or match?({:stored, _}, raw)) and
          valid_speaker?(speaker, person) and Map.has_key?(engine.shared_recipients, {shared_id, person})
      nil -> false
    end
  end

  def valid?(_, _, _), do: false

  defp valid_speaker?(%{"kind" => "person", "id" => person}, person), do: true
  defp valid_speaker?(%{"kind" => "server"}, _person), do: true
  defp valid_speaker?(_, _), do: false

  defp answer_id(shared_id, person, revision) do
    hash = :crypto.hash(:sha256, :erlang.term_to_binary({shared_id, person, revision}))
    "answer_" <> Base.url_encode64(hash, padding: false)
  end

  defp format_answer(_message, :no_comment), do: {:ok, :absent, %{"kind" => "no_comment"}}
  defp format_answer(_message, {:comment, text}), do: {:ok, {:stored, text}, %{"kind" => "reply", "text" => text}}

  defp format_answer(%{"answer_ui" => %{"schema" => "attention-client.ui/1"}}, {:result, value}),
    do: {:ok, {:stored, value}, %{"kind" => "result", "value" => value}}

  defp format_answer(%{"answer_ui" => %{"actions" => actions}}, {:choice, id}) when is_list(actions) do
    case Enum.find(actions, &match?(%{"id" => ^id, "kind" => "value", "value" => _}, &1)) do
      %{"value" => value} when id == "seen" -> {:ok, {:stored, value}, %{"kind" => "seen"}}
      %{"value" => value} = action ->
        {:ok, {:stored, value}, Map.merge(%{"kind" => "choice", "id" => id}, Map.take(action, ["label"]))}
      _ -> {:error, :invalid_field, "choice"}
    end
  end

  defp format_answer(_message, {:choice, _}), do: {:error, :invalid_field, "choice"}
  defp format_answer(_message, {:result, _}), do: {:error, :invalid_field, "result"}
end
