defmodule AttentionServer.NoCommentPartialContractTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{BrowserRecipient, Engine, JSON, Store}

  @person "synthetic-person"
  @key {@person, "app:test", "request"}
  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  setup do
    {:ok, engine} = Engine.setup_person(Engine.new(), @person)

    engine =
      Enum.reduce(~w(origin reader other), engine, fn device, engine ->
        {:ok, engine} = Engine.setup_device(engine, @person, device)
        engine
      end)

    {:ok, engine} = Engine.setup_sender(engine, "origin", "app:test")

    {engine, %{"type" => "ok"}} =
      request(engine, "origin", %{
        "type" => "forward",
        "op_id" => op(1),
        "session_handle" => @handle,
        "message" => message("Original request")
      })

    %{engine: engine}
  end

  for mode <- [:direct, :reconcile] do
    test "#{mode}: first older No comment commits once, preserves binding and cannot reopen", %{
      engine: engine
    } do
      mode = unquote(mode)
      {engine, %{"revision" => 2}} = request(engine, "origin", revise(1, 2))
      {engine, result} = no_comment(engine, "reader", mode, 1, 3)
      assert committed_seq(result) == engine.messages[@key].terminal.seq
      terminal = engine.messages[@key].terminal
      assert terminal.kind == "no_comment"
      assert terminal.actor == "reader"
      assert terminal.answer_present == false
      assert {:ok, %{"entries" => entries}} = Engine.origin_feed(engine, "origin", 0)
      assert Enum.count(entries, &(&1["kind"] == "terminal")) == 1
      assert List.last(entries)["has_answer"] == false
      refute Map.has_key?(List.last(entries), "answer")
      assert List.last(entries)["request_revision"] == 2

      {same, retry} = no_comment(engine, "reader", mode, 1, 3)
      assert committed_seq(retry) == terminal.seq
      assert duplicate?(retry)
      assert same == engine
      {same, %{"reason" => "closed"}} = request(engine, "origin", revise(2, 4))
      assert same == engine
      assert same.messages[@key].terminal == terminal
      assert same.messages[@key].revision == 2
    end

    test "#{mode}: invalid/future revisions and missing binding do not commit", %{engine: engine} do
      mode = unquote(mode)

      for revision <- [0, -1, nil, "1", 2] do
        {same, result} = no_comment(engine, "reader", mode, revision, 3)
        assert same == engine
        assert result["reason"] == if(revision == 2, do: "stale_revision", else: "invalid_field")
      end

      frame = %{nc(1) | "request_id" => "missing"}
      {same, %{"reason" => "not_found"}} = request(engine, "reader", frame)
      assert same == engine
      assert {^engine, {:error, :forbidden}} = Engine.request_frame(engine, "unknown", nc(1), 1)
    end
  end

  for commit_mode <- [:direct, :reconcile] do
    test "#{commit_mode} terminal refuses different devices but preserves direct retries", %{
      engine: engine
    } do
      {engine, first} = no_comment(engine, "reader", unquote(commit_mode), 1, 3)
      seq = committed_seq(first)
      {same, retry} = no_comment(engine, "reader", :direct, 1, 4)
      assert same == engine
      assert committed_seq(retry) == seq
      assert duplicate?(retry)

      for mode <- [:direct, :reconcile], operation <- [3, 4] do
        {same, refusal} = no_comment(engine, "other", mode, 1, operation)
        assert same == engine
        assert refusal["reason"] == "closed"

        if mode == :reconcile do
          assert refusal == %{
                   "op_id" => op(operation),
                   "status" => "refused",
                   "reason" => "closed"
                 }

          refute Map.has_key?(same.receipts, {@person, "other", op(operation)})
        end
      end
    end
  end

  test "request IDs remain person-scoped and ambiguous sender bindings are refused", %{
    engine: engine
  } do
    {:ok, engine} = Engine.setup_person(engine, "another-person")
    {:ok, engine} = Engine.setup_device(engine, "another-person", "outsider")
    {same, %{"reason" => "not_found"}} = request(engine, "outsider", nc(1))
    assert same == engine

    {:ok, engine} = Engine.setup_sender(engine, "origin", "app:other")

    {engine, %{"type" => "ok"}} =
      request(engine, "origin", %{
        "type" => "forward",
        "op_id" => op(2),
        "message" => %{message("Another sender's request") | "sender_id" => "app:other"}
      })

    for mode <- [:direct, :reconcile] do
      {same, %{"reason" => "not_found"}} = no_comment(engine, "reader", mode, 1, 3)
      assert same == engine
    end
  end

  test "receipt lookup precedes classification and preserves canonical bytes, seq and conflicts",
       %{engine: engine} do
    {engine, _} = request(engine, "origin", revise(1, 2))
    frame = Map.put(nc(1), "op_id", op(3))
    {:ok, engine, first} = Engine.reconcile_no_comment(engine, "reader", frame)
    receipt = engine.receipts[{@person, "reader", op(3)}]
    assert receipt.semantic == JSON.canonical(nc(1))
    {:ok, same, retry} = Engine.reconcile_no_comment(engine, "reader", frame)
    assert same == engine
    assert committed_seq(first) == committed_seq(retry)
    assert retry["result"]["duplicate"] == true

    {:ok, same, conflict} =
      Engine.reconcile_no_comment(engine, "reader", %{frame | "request_revision" => 2})

    assert conflict == %{"op_id" => op(3), "status" => "operation_conflict"}
    assert same == engine
    assert same.receipts[{@person, "reader", op(3)}] == receipt
    unavailable = put_in(engine.messages[@key].terminal, nil)

    assert {:error, :snapshot_unavailable} =
             Engine.reconcile_no_comment(unavailable, "reader", frame)

    # A previously persisted, person-wide adoption receipt from the old behavior
    # remains authoritative even though a new adoption by this device is refused.
    legacy = put_in(engine.receipts[{@person, "other", op(3)}], receipt)
    {:ok, same, retry} = Engine.reconcile_no_comment(legacy, "other", frame)
    assert same == legacy
    assert committed_seq(retry) == committed_seq(first)
    assert retry["result"]["duplicate"] == true

    {:ok, ^legacy, %{"status" => "operation_conflict"}} =
      Engine.reconcile_no_comment(legacy, "other", %{frame | "request_revision" => 2})
  end

  test "same-principal known ID recovery and different ID refusal apply to device and browser", %{
    engine: initial
  } do
    for actor <- ["reader", browser("session-one")] do
      {engine, first} = no_comment(initial, actor, :reconcile, 1, 3)
      # Exercise matching retained terminal identity when its receipt is absent.
      without_receipt = %{engine | receipts: Map.delete(engine.receipts, {@person, actor, op(3)})}
      {restored, retry} = no_comment(without_receipt, actor, :reconcile, 1, 3)
      assert committed_seq(retry) == committed_seq(first)
      assert duplicate?(retry)
      assert restored.messages == engine.messages
      {same, refusal} = no_comment(engine, actor, :reconcile, 1, 4)
      assert refusal == %{"op_id" => op(4), "status" => "refused", "reason" => "closed"}
      assert same == engine
      {same, retry} = no_comment(engine, actor, :direct, 1, 5)
      assert same == engine
      assert duplicate?(retry)
      assert committed_seq(retry) == committed_seq(first)
    end
  end

  test "legacy nil-ID adoption remains compatible, without asserting a new intent is distinguishable",
       %{engine: initial} do
    for actor <- ["reader", browser("session-one")] do
      {engine, first} = no_comment(initial, actor, :direct, 1, 3)
      assert engine.messages[@key].terminal.op_id == nil
      {adopted, retry} = no_comment(engine, actor, :reconcile, 1, 4)
      assert duplicate?(retry)
      assert committed_seq(retry) == committed_seq(first)
      assert adopted.messages == engine.messages
      assert adopted.receipts[{@person, actor, op(4)}].semantic == JSON.canonical(nc(1))
    end
  end

  test "browser session/key changes and mixed principals preserve the explicit unresolved adoption",
       %{engine: initial} do
    for {committer, requester} <- [
          {browser("session-one"), browser("session-two")},
          {browser("session-one"), "reader"},
          {"reader", browser("session-one")}
        ],
        commit_mode <- [:direct, :reconcile],
        retry_mode <- [:direct, :reconcile] do
      {engine, first} = no_comment(initial, committer, commit_mode, 1, 3)
      {next, retry} = no_comment(engine, requester, retry_mode, 1, 4)
      assert committed_seq(retry) == committed_seq(first)
      assert duplicate?(retry)
      assert next.messages == engine.messages
      assert next.persons == engine.persons
    end
  end

  test "other terminals, stale text/choice, semantic conflicts and strict direct shape stay unchanged",
       %{engine: engine} do
    {engine, _} = request(engine, "origin", revise(1, 2))

    for answer <- ["text", %{"choice" => "Yes"}] do
      {same, %{"reason" => "stale_revision"}} = request(engine, "reader", answer(1, answer, 5))
      assert same == engine
    end

    {same, %{"reason" => "invalid_field"}} =
      request(engine, "reader", Map.put(nc(1), "op_id", op(3)))

    assert same == engine
    {engine, %{"type" => "ok"}} = request(engine, "reader", answer(2, "text", 5))

    {same, %{"reason" => "operation_conflict"}} =
      request(engine, "reader", answer(2, "changed", 5))

    assert same == engine

    for mode <- [:direct, :reconcile] do
      {same, %{"reason" => "closed"}} = no_comment(engine, "reader", mode, 1, 6)
      assert same == engine
    end
  end

  test "committed older operation survives store reopen with original receipt and terminal replay",
       %{engine: engine} do
    dir = Path.join(System.tmp_dir!(), "no-comment-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "state.sqlite3")
    {:ok, store} = Store.open(path)
    {engine, _} = request(engine, "origin", revise(1, 2))
    {engine, first} = no_comment(engine, "reader", :reconcile, 1, 3)

    {:ok, stored} =
      Store.commit(store, engine, %{
        person: @person,
        actor: "reader",
        action: "no_comment_regression"
      })

    assert :ok = Store.close(stored)
    {:ok, reopened} = Store.open(path)
    on_exit(fn -> Store.close(reopened) end)
    assert reopened.engine.receipts == engine.receipts
    assert reopened.engine.messages[@key].terminal == engine.messages[@key].terminal
    {same, retry} = no_comment(reopened.engine, "reader", :reconcile, 1, 3)
    assert same == reopened.engine
    assert committed_seq(retry) == committed_seq(first)
    assert duplicate?(retry)
    assert {:ok, events, _} = Engine.events(same, "reader", 0)
    assert Enum.any?(events, &(&1["terminal_kind"] == "no_comment"))
  end

  defp no_comment(engine, actor, :direct, revision, _operation),
    do: request(engine, actor, nc(revision))

  defp no_comment(engine, actor, :reconcile, revision, operation) do
    assert {:ok, next, result} =
             Engine.reconcile_no_comment(
               engine,
               actor,
               Map.put(nc(revision), "op_id", op(operation))
             )

    {next, result}
  end

  defp nc(revision),
    do: %{
      "type" => "answer_no_comment",
      "request_id" => "request",
      "request_revision" => revision
    }

  defp committed_seq(%{"status" => "committed", "result" => result}), do: result["seq"]
  defp committed_seq(%{"type" => "ok", "seq" => seq}), do: seq
  defp duplicate?(%{"result" => result}), do: result["duplicate"] == true
  defp duplicate?(result), do: result["duplicate"] == true

  defp request(engine, actor, frame) do
    wire = if frame["type"] == "answer_no_comment", do: frame, else: Map.put(frame, "v", 1)
    {next, {:ok, line}} = Engine.request_frame(engine, actor, wire, 10)
    assert {:ok, result} = JSON.decode_line(line)
    {next, result}
  end

  defp message(title),
    do: %{
      "sender_id" => "app:test",
      "message_id" => "request",
      "created_at" => 1,
      "title" => title,
      "choices" => ["Yes", "No"]
    }

  defp revise(base, operation),
    do: %{
      "type" => "conversation_revise",
      "op_id" => op(operation),
      "sender_id" => "app:test",
      "message_id" => "request",
      "base_revision" => base,
      "message" => message("Revised request")
    }

  defp answer(revision, value, operation),
    do: %{
      "type" => "answer",
      "op_id" => op(operation),
      "sender_id" => "app:test",
      "message_id" => "request",
      "request_revision" => revision,
      "answer" => value
    }

  defp browser(session), do: BrowserRecipient.key(@person, session, op(100))
  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"
end
