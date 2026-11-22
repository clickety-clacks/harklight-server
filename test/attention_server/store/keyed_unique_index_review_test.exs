defmodule AttentionServer.Store.KeyedUniqueIndexReviewTest do
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

  defp commit!(store, next) do
    {:ok, store} = Store.commit(store, {next, codec_diff(store.engine, next)}, audit())
    store
  end

  defp assert_indexes_rebuilt(store) do
    assert store.unique_index == KeyedValidation.unique_index(store.engine)
    assert store.authority_index == KeyedValidation.authority_index(store.engine)
  end

  test "Q1 a refused uniqueness commit leaves the carried index equal to a rebuild", %{
    store: store
  } do
    engine = store.engine
    bad = put_in(engine.devices[@owner_key], engine.devices[@reader])
    assert {{:error, {:unique, :principal_key}}, {:refused, _}} = probe(store, bad)
    assert_indexes_rebuilt(store)
    # The same key becomes free once the owner moves to the spare key.
    good =
      engine
      |> put_in([Access.key(:owners), @person], %{key: @spare, revision: 2})
      |> put_in([Access.key(:devices), @owner_key], engine.devices[@reader])

    assert {:ok, :committed} = probe(store, good)
  end

  test "Q2 two pending requests swap secrets in one commit", %{store: store} do
    engine = store.engine
    a = pending_item()
    b = %{pending_item() | key: @retired, secret_hash: <<9::256>>}
    store = commit!(store, %{engine | pending_enrollments: %{@code => a, @code2 => b}})
    engine = store.engine

    swapped = %{
      engine
      | pending_enrollments: %{
          @code => %{a | secret_hash: b.secret_hash},
          @code2 => %{b | secret_hash: a.secret_hash}
        }
    }

    {verdict, outcome} = probe(store, swapped)
    assert verdict == :ok == (outcome == :committed)
  end

  test "Q3 owner takes a retired key that is unretired in the same commit", %{store: store} do
    engine = store.engine

    next = %{
      engine
      | owners: %{@person => %{key: @retired, revision: 2}},
        retired_owner_keys: MapSet.new([@owner_key])
    }

    {verdict, outcome} = probe(store, next)
    assert {:ok, :committed} = {verdict, outcome}
  end

  test "Q4 hosted accounts swap persons in one commit", %{store: store} do
    engine = store.engine
    engine = %{engine | owners: Map.put(engine.owners, @other, %{key: @spare, revision: 1})}

    store =
      commit!(
        store,
        put_in(engine.hosted_accounts["second"], %{person: @other, label: nil, test: false})
      )

    engine = store.engine

    swapped = %{
      engine
      | hosted_accounts: %{
          "account" => %{person: @other, label: nil, test: false},
          "second" => %{person: @person, label: nil, test: false}
        }
    }

    assert {:ok, :committed} = probe(store, swapped)
    assert_indexes_rebuilt(store)
  end

  test "Q5 an agent id moves to a new agent while the old agent is removed", %{store: store} do
    engine = store.engine
    agent = engine.agents[@agent_key]
    next = %{engine | agents: %{@spare => agent}}
    {verdict, outcome} = probe(store, next)
    assert verdict == :ok == (outcome == :committed)
  end

  test "Q7 two inactive bindings may share an address (nil)", %{store: store} do
    engine = store.engine
    next = put_in(engine.push_bindings[{@person, @origin}], canceled_binding())
    assert {:ok, :committed} = probe(store, next)
  end

  test "Q8 a key clash is found when another touched address sorts first", %{store: store} do
    engine = store.engine

    next =
      engine
      |> put_in([Access.key(:delivery_states), @person], "STOPPED")
      |> put_in([Access.key(:devices), @owner_key], engine.devices[@reader])

    assert {{:error, {:unique, :principal_key}}, {:refused, _}} = probe(store, next)
  end

  test "Q6 indexes after a run of commits and refusals equal a rebuild", %{store: store} do
    engine = store.engine
    store = commit!(store, put_in(engine.pending_enrollments[@code], pending_item()))
    engine = store.engine
    {_, {:refused, _}} = probe(store, put_in(engine.pending_history[@code2], expired_receipt()))

    store =
      commit!(store, %{
        engine
        | pending_enrollments: %{},
          pending_history: %{@code => expired_receipt()}
      })

    engine = store.engine

    store =
      commit!(store, %{
        engine
        | owners: %{@person => %{key: @spare, revision: 2}},
          retired_owner_keys: MapSet.put(engine.retired_owner_keys, @owner_key)
      })

    assert_indexes_rebuilt(store)
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
