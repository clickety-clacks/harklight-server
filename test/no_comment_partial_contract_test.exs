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

  test "first older No comment commits once, preserves binding and cannot reopen", %{
    engine: engine
  } do
    {engine, %{"revision" => 2}} = request(engine, "origin", revise(1, 2))
    {engine, result} = no_comment(engine, "reader", 1)
    assert committed_seq(result) == engine.messages[@key].terminal.seq
    terminal = engine.messages[@key].terminal
    assert terminal.kind == "no_comment"
    assert terminal.actor == "reader"
    assert terminal.answer_present == false
    entries = origin_feed_entries(engine)
    assert Enum.count(entries, &(&1["kind"] == "terminal")) == 1
    assert List.last(entries)["has_answer"] == false
    refute Map.has_key?(List.last(entries), "answer")
    assert List.last(entries)["request_revision"] == 2

    {same, retry} = no_comment(engine, "reader", 1)
    assert committed_seq(retry) == terminal.seq
    assert duplicate?(retry)
    assert same == engine
    {same, %{"reason" => "closed"}} = request(engine, "origin", revise(2, 4))
    assert same == engine
    assert same.messages[@key].terminal == terminal
    assert same.messages[@key].revision == 2
  end

  test "invalid/future revisions and missing binding do not commit", %{engine: engine} do
    for revision <- [0, -1, nil, "1", 2] do
      {same, result} = no_comment(engine, "reader", revision)
      assert same == engine
      assert result["reason"] == if(revision == 2, do: "stale_revision", else: "invalid_field")
    end

    frame = %{nc(1) | "request_id" => "missing"}
    {same, %{"reason" => "not_found"}} = request(engine, "reader", frame)
    assert same == engine
    assert {^engine, {:error, :forbidden}} = Engine.request_frame(engine, "unknown", nc(1), 1)
  end

  test "terminal refuses different devices but preserves direct retries", %{engine: engine} do
    {engine, first} = no_comment(engine, "reader", 1)
    seq = committed_seq(first)
    {same, retry} = no_comment(engine, "reader", 1)
    assert same == engine
    assert committed_seq(retry) == seq
    assert duplicate?(retry)

    {same, refusal} = no_comment(engine, "other", 1)
    assert same == engine
    assert refusal["reason"] == "closed"
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

    {same, %{"reason" => "not_found"}} = no_comment(engine, "reader", 1)
    assert same == engine
  end

  test "browser session/key changes and mixed principals preserve the explicit unresolved adoption",
       %{engine: initial} do
    for {committer, requester} <- [
          {browser("session-one"), browser("session-two")},
          {browser("session-one"), "reader"},
          {"reader", browser("session-one")}
        ] do
      {engine, first} = no_comment(initial, committer, 1)
      {next, retry} = no_comment(engine, requester, 1)
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

    {same, %{"reason" => "closed"}} = no_comment(engine, "reader", 1)
    assert same == engine
  end

  test "committed older operation survives store reopen with original receipt and terminal replay",
       %{engine: engine} do
    dir = Path.join(System.tmp_dir!(), "no-comment-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "state.sqlite3")
    {:ok, store} = Store.open(path)
    {engine, _} = request(engine, "origin", revise(1, 2))
    {engine, first} = no_comment(engine, "reader", 1)

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
    {same, retry} = no_comment(reopened.engine, "reader", 1)
    assert same == reopened.engine
    assert committed_seq(retry) == committed_seq(first)
    assert duplicate?(retry)
    assert {:ok, events, _} = Engine.events(same, "reader", 0)
    assert Enum.any?(events, &(&1["terminal_kind"] == "no_comment"))
  end

  defp no_comment(engine, actor, revision), do: request(engine, actor, nc(revision))

  # The origin's stored conversation feed, in feed order.
  defp origin_feed_entries(engine) do
    engine.origin_feeds[{@person, "origin"}].entries
    |> Enum.sort()
    |> Enum.map(fn {_seq, entry} -> entry end)
  end

  defp nc(revision),
    do: %{
      "type" => "answer_no_comment",
      "request_id" => "request",
      "request_revision" => revision
    }

  defp committed_seq(%{"type" => "ok", "seq" => seq}), do: seq
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
