defmodule AttentionServer.Store.KeyedIncrementalFallbackTest do
  # One proposal per full-validator check ported to per-address keyed
  # validation (W3a; rows of the W3 validator inventory in the test names).
  # Each proposal is refused by the whole-state check. It must be refused by
  # the per-address verdict for the stated reason, and committing it through
  # the keyed Store, which runs both validators in the test environment and
  # raises when they disagree, must fail.
  use ExUnit.Case, async: false

  alias AttentionServer.{Engine, JSON, LargeData, Store}
  alias AttentionServer.Store.{KeyedValidation, RecordCodec}

  @moduletag :tmp_dir
  @moduletag :capture_log

  @person "a"
  @other "b"
  @sender "app:test/worker"
  @origin String.duplicate("a", 64)
  @reader String.duplicate("b", 64)
  @owner_key String.duplicate("c", 64)
  @retired String.duplicate("d", 64)
  @agent_key String.duplicate("e", 64)
  @spare String.duplicate("f", 64)
  @agent_id "00000000-0000-4000-8000-0000000000a1"
  @handle "00000000-0000-4000-8000-000000000001"
  @uuid "00000000-0000-4000-8000-0000000000b1"
  @m1 {@person, @sender, "m1"}
  @m2 {@person, @sender, "m2"}
  @scope {@person, @origin}
  @code "AAAAAAAAAA"
  @code2 "BBBBBBBBBB"
  @secret :binary.copy(<<7>>, 32)

  setup %{tmp_dir: directory} do
    path = Path.join(directory, "state.sqlite3")
    {:ok, legacy} = Store.open(path)
    {:ok, engine} = Engine.setup_person(legacy.engine, @person)
    {:ok, engine} = Engine.setup_person(engine, @other)
    {:ok, engine} = Engine.setup_device(engine, @person, @origin)
    {:ok, engine} = Engine.setup_device(engine, @person, @reader)
    {:ok, engine} = Engine.setup_sender(engine, @origin, @sender)
    {:ok, engine} = Engine.setup_agent_fixture(engine, @person, @agent_id, @agent_key, [@sender])

    engine =
      [
        {@origin, forward("m1")},
        {@origin, forward("m2")},
        {@reader,
         %{
           "type" => "discussion",
           "sender_id" => @sender,
           "message_id" => "m1",
           "request_revision" => 1,
           "text" => "p"
         }},
        {@origin,
         %{
           "type" => "conversation_reply",
           "sender_id" => @sender,
           "message_id" => "m1",
           "request_revision" => 1,
           "in_reply_to" => 3,
           "text" => "o"
         }}
      ]
      |> Enum.with_index(1)
      |> Enum.reduce(engine, fn {{actor, frame}, n}, engine ->
        op = "00000000-0000-4000-8000-" <> String.pad_leading(Integer.to_string(n), 12, "0")
        raw = frame |> Map.merge(%{"v" => 1, "op_id" => op}) |> JSON.encode_line()
        {engine, {:ok, response}} = Engine.request_line(engine, actor, raw, 1_000 + n)
        refute response =~ ~s("type":"error"), response
        engine
      end)

    engine = %{
      engine
      | owners: %{@person => %{key: @owner_key, revision: 1}},
        retired_owner_keys: MapSet.new([@retired]),
        hosted_accounts: %{"account" => %{person: @person, label: nil, test: false}},
        push_bindings: %{{@person, @reader} => canceled_binding()}
    }

    {:ok, legacy} = Store.commit(legacy, engine, audit())
    :ok = Store.close(legacy)
    {:ok, staged} = Store.open(path, :keyed_cutover)
    :ok = Store.close(staged)
    {:ok, store} = Store.open(path, :keyed)
    on_exit(fn -> Store.close(store) end)
    %{store: store}
  end

  # From the #337 review probes: each proposal must get the same verdict from both validators
  # (commit in :both raises on disagreement) and the stated incremental reason.
  defp probe(store, next, touched \\ nil) do
    touched = touched || codec_diff(store.engine, next)
    verdict = Store.incremental_verdict(store, next, touched)

    outcome =
      try do
        case Store.commit(store, {next, touched}, audit()) do
          {:ok, _} -> :committed
          {:error, reason, _} -> {:refused, reason}
        end
      rescue
        e -> {:raised, Exception.message(e)}
      end

    refute match?({:raised, _}, outcome)
    {verdict, outcome}
  end

  test "P1 message removed falls back", %{store: store} do
    next = %{store.engine | messages: Map.delete(store.engine.messages, @m2)}
    assert {{:error, {:irregular, :message_removed}}, {:refused, _}} = probe(store, next)
  end

  test "P2 message re-originated falls back", %{store: store} do
    engine = store.engine
    next = put_in(engine.messages[@m1].origin, @reader)
    {verdict, outcome} = probe(store, next)
    assert verdict == :ok == (outcome == :committed)
  end

  test "P3 published conversation event rewritten falls back", %{store: store} do
    engine = store.engine
    seq = origin_reply_seq(engine)
    next = put_in(engine.conversation_events[{@person, seq}].payload["author"], "person")
    {verdict, outcome} = probe(store, next)
    assert verdict == :ok == (outcome == :committed)
  end

  test "P4 free-order high lowered falls back", %{store: store} do
    engine = store.engine
    high = Map.get(engine.free_order_highs, @person)
    assert is_integer(high) and high > 0
    next = put_in(engine.free_order_highs[@person], 0)
    assert {{:error, {:irregular, :free_order_lowered}}, {:refused, _}} = probe(store, next)
  end

  test "P5 removing an upload a message revision references", %{store: store} do
    upload = %{
      id: @uuid,
      person: @person,
      uploader: @origin,
      op_id: @uuid,
      sender_id: @sender,
      message_id: "m1",
      revision: 1,
      name: "a.txt",
      size: 1,
      last_activity_ms: 1,
      sha256: String.duplicate("0", 64)
    }

    next =
      store.engine
      |> put_in([Access.key(:uploads), @uuid], upload)
      |> put_in([Access.key(:messages), @m1, :files_by_revision], %{1 => [upload_file()]})

    assert :ok = Store.incremental_verdict(store, next, codec_diff(store.engine, next))
    {:ok, store} = Store.commit(store, {next, codec_diff(store.engine, next)}, audit())

    removed = %{store.engine | uploads: %{}}
    assert {{:error, {:address, {:row, :messages, @m1}}}, {:refused, _}} = probe(store, removed)
  end

  test "P6 lowering a person high below a message's accepted seq", %{store: store} do
    engine = store.engine
    high = engine.persons[@person]

    next = %{
      engine
      | persons: Map.put(engine.persons, @person, high - 1),
        events: Map.delete(engine.events, {@person, high})
    }

    {verdict, outcome} = probe(store, next)
    assert match?({:refused, _}, outcome) == (verdict != :ok)
  end

  test "P7 removing a person with rows", %{store: store} do
    engine = store.engine
    next = %{engine | persons: Map.delete(engine.persons, @other)}
    {verdict, outcome} = probe(store, next)
    assert match?({:refused, _}, outcome) == (verdict != :ok)
  end

  test "P8 reassigning a device to another person", %{store: store} do
    engine = store.engine
    next = put_in(engine.devices[@reader].person, @other)
    {verdict, outcome} = probe(store, next)
    assert {:error, _} = verdict
    assert {:refused, _} = outcome
  end

  test "P9 removing the origin device", %{store: store} do
    next = %{store.engine | devices: Map.delete(store.engine.devices, @origin)}
    {verdict, outcome} = probe(store, next)
    assert {:error, {:address, _}} = verdict
    assert {:refused, _} = outcome
  end

  test "P10 removing a pending row whose history row holds the same code", %{store: store} do
    engine = store.engine
    next = put_in(engine.pending_enrollments[@code], pending_item())
    {:ok, store} = Store.commit(store, {next, codec_diff(store.engine, next)}, audit())
    # Move to history with a duplicate secret: pending removed, history added.
    moved = %{
      store.engine
      | pending_enrollments: %{},
        pending_history: %{@code => expired_receipt()}
    }

    {verdict, outcome} = probe(store, moved)
    assert verdict == :ok == (outcome == :committed)
  end

  test "P12 re-originating a message whose delivery ack names the origin", %{store: store} do
    engine = store.engine

    {seq, _entry} =
      Enum.find(engine.origin_feeds[@scope].entries, fn {_seq, entry} ->
        entry["author"] == "person" and entry["kind"] == "discussion"
      end)

    {:ok, acked, _reply} =
      Engine.origin_ack2(engine, @origin, seq, %{"op_id" => @uuid, "outcome" => "handed"})

    assert map_size(acked.origin_feeds[@scope].delivery_acks) == 1
    {:ok, store} = Store.commit(store, {acked, codec_diff(engine, acked)}, audit())
    engine = store.engine
    next = put_in(engine.messages[@m1].origin, @reader)
    assert {{:error, {:irregular, :message_origin}}, {:refused, _}} = probe(store, next)
  end

  test "P13 rewriting a published event a chat position reads", %{store: store} do
    engine = store.engine
    seq = origin_reply_seq(engine)
    positioned = put_in(engine.chat_read_positions[@m1], seq)
    {:ok, store} = Store.commit(store, {positioned, codec_diff(engine, positioned)}, audit())
    engine = store.engine
    next = put_in(engine.conversation_events[{@person, seq}].payload["author"], "person")

    assert {{:error, {:irregular, :conversation_event_rewritten}}, {:refused, _}} =
             probe(store, next)
  end

  test "P14 removing a person whose conversation high row remains", %{store: store} do
    engine = store.engine
    engine = put_in(engine.conversation_highs[@other], 0)
    {:ok, store} = Store.commit(store, {engine, codec_diff(store.engine, engine)}, audit())
    engine = store.engine
    next = %{engine | persons: Map.delete(engine.persons, @other)}

    assert {{:error, {:address, {:row, :conversation_highs, @other}}}, {:refused, _}} =
             probe(store, next)
  end

  test "P15 removing a pending request a history row with the same code shadows", %{store: store} do
    engine = store.engine
    # Only the history row is touched; the untouched pending row must be rechecked.
    engine = put_in(engine.pending_enrollments[@code], pending_item())
    {:ok, store} = Store.commit(store, {engine, codec_diff(store.engine, engine)}, audit())
    engine = store.engine
    next = put_in(engine.pending_history[@code], expired_receipt(<<9::256>>))
    {verdict, outcome} = probe(store, next)
    assert {:error, {:address, {:row, _, @code}}} = verdict
    assert {:refused, _} = outcome
  end

  test "A1 a new conversation event above the free-order high is refused", %{store: store} do
    engine = store.engine
    seq = origin_reply_seq(engine)
    event = Map.fetch!(engine.conversation_events, {@person, seq})
    high = Map.fetch!(engine.free_order_highs, @person)
    next_seq = Map.fetch!(engine.conversation_highs, @person) + 1

    next =
      engine
      |> put_in(
        [Access.key!(:conversation_events), {@person, next_seq}],
        Map.put(event, :free_order, high + 5)
      )
      |> put_in([Access.key!(:conversation_highs), @person], next_seq)

    {verdict, outcome} = probe(store, next)
    assert {:error, _} = verdict
    assert {:refused, _} = outcome
  end

  test "P11 full mode is the default outside the test config", %{store: store} do
    previous = Application.get_env(:attention_server, :keyed_validation)
    Application.put_env(:attention_server, :keyed_validation, :full)

    try do
      engine = store.engine
      bad = put_in(engine.delivery_states[@person], "BAD")

      assert {:error, :storage_unavailable, _} =
               Store.commit(store, {bad, codec_diff(store.engine, bad)}, audit())

      good = put_in(engine.delivery_states[@person], "STOPPED")
      assert {:ok, _} = Store.commit(store, {good, codec_diff(store.engine, good)}, audit())
    after
      Application.put_env(:attention_server, :keyed_validation, previous)
    end
  end

  defp refuse(store, mutate, expected) do
    next = mutate.(store.engine)
    assert_refused(store, next, codec_diff(store.engine, next), expected)
  end

  defp assert_refused(store, next, touched, expected) do
    verdict = Store.incremental_verdict(store, next, touched)

    case expected do
      :preflight ->
        assert {:error, :invalid_fact} = KeyedValidation.preflight(store.engine, next, touched)
        assert verdict == {:error, {:irregular, :preflight}}

      {:check, tag} ->
        assert verdict == {:error, tag}

      {kind, detail} ->
        assert verdict == {:error, {kind, detail}}
    end

    assert {:error, :storage_unavailable, _} = Store.commit(store, {next, touched}, audit())
  end

  defp origin_reply_seq(engine) do
    Enum.find_value(engine.conversation_events, fn
      {{@person, seq}, %{key: @m1, payload: %{"kind" => "reply", "author" => "origin"}}} -> seq
      _ -> nil
    end)
  end

  defp forward(id) do
    %{
      "type" => "forward",
      "session_handle" => @handle,
      "message" => %{
        "sender_id" => @sender,
        "message_id" => id,
        "created_at" => 1,
        "title" => id,
        "body" => "b"
      }
    }
  end

  defp upload_file do
    %{
      "tier" => "upload",
      "upload_id" => @uuid,
      "name" => "a.txt",
      "size" => 1,
      "sha256" => String.duplicate("0", 64)
    }
  end

  defp pending_item do
    %{
      key: @spare,
      certificate: nil,
      legacy_recovery: true,
      person: @person,
      label: "phone",
      secret_hash: @secret,
      expires_at: nil,
      status_expires_at: nil,
      request_generation: 1,
      state: :pending
    }
  end

  defp expired_receipt(secret \\ @secret) do
    %{
      state: :expired,
      secret_hash: secret,
      key: @spare,
      expires_at: nil,
      status_expires_at: nil
    }
  end

  defp canceled_binding do
    %{
      active: false,
      revision: 1,
      binding_id: nil,
      address: nil,
      profile_id: nil,
      registered_at: nil,
      state: "canceled",
      pending_high: 0,
      accepted_high: 0,
      pending_conversation_high: 0,
      accepted_conversation_high: 0,
      deadline_at: nil,
      next_at: nil,
      attempt_count: 0,
      last_attempt_at: nil,
      claim_id: nil,
      claim_high: nil,
      claim_conversation_high: nil,
      claim_at: nil,
      last_outcome: nil
    }
  end

  defp active_binding(binding_id) do
    %{
      canceled_binding()
      | active: true,
        binding_id: binding_id,
        address: String.duplicate("9", 64),
        profile_id: "harklight-sandbox",
        registered_at: 1,
        state: "ready"
    }
  end

  defp audit, do: %{person: @person, actor: @origin, action: "keyed_incremental_probe"}

  defp codec_diff(old, next) do
    a = encode(old)
    b = encode(next)

    MapSet.union(MapSet.new(Map.keys(a)), MapSet.new(Map.keys(b)))
    |> Enum.reject(&(Map.fetch(a, &1) === Map.fetch(b, &1)))
    |> MapSet.new(&elem(:erlang.binary_to_term(&1), 1))
  end

  defp encode(engine) do
    Map.new(RecordCodec.encode(LargeData.persist_refs(engine)), fn {key, bytes, _} ->
      {key, bytes}
    end)
  end
end
