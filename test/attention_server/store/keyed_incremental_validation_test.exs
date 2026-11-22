defmodule AttentionServer.Store.KeyedIncrementalValidationTest do
  # One proposal per full-validator check ported to per-address keyed
  # validation (W3a; rows of the W3 validator inventory in the test names).
  # Each proposal is refused by the whole-state check. It must be refused by
  # the per-address verdict for the stated reason, and committing it through
  # the keyed Store, which runs both validators in the test environment and
  # raises when they disagree, must fail.
  use ExUnit.Case, async: true

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

  test "the test environment runs both keyed validators", %{store: store} do
    assert Application.get_env(:attention_server, :keyed_validation) == :both
    assert map_size(store.engine.messages) == 2
    assert is_integer(origin_reply_seq(store.engine))

    # A valid proposal is accepted by both verdicts and commits.
    engine = store.engine
    next = put_in(engine.delivery_states[@person], "STOPPED")
    touched = codec_diff(store.engine, next)
    assert :ok = Store.incremental_verdict(store, next, touched)
    assert {:ok, _} = Store.commit(store, {next, touched}, audit())
  end

  describe "scalar fields" do
    test "#1 relay instance id", %{store: store} do
      refuse(store, &%{&1 | relay_instance_id: "xyz"}, {:address, {:field, :relay_instance_id}})
    end

    test "#2 agent cursor key", %{store: store} do
      refuse(store, &%{&1 | agent_cursor_key: <<1, 2>>}, {:address, {:field, :agent_cursor_key}})
    end

    test "#4 pending clock high water", %{store: store} do
      refuse(store, &%{&1 | pending_clock_high_water: -1}, :preflight)
    end

    test "#15 browser sign-in clock high water", %{store: store} do
      refuse(store, &%{&1 | browser_sign_in_clock_high_water: -1}, :preflight)
    end

    test "#22 jev endpoint and model", %{store: store} do
      refuse(store, &%{&1 | jev_endpoint: "not a url"}, {:address, {:field, :jev_endpoint}})
      refuse(store, &%{&1 | jev_model: 5}, {:address, {:field, :jev_model}})
    end
  end

  describe "principals" do
    test "#5 person high", %{store: store} do
      refuse(store, &put_in(&1.persons[@other], -1), :preflight)
    end

    test "#6 owner key may not be a device key", %{store: store} do
      refuse(
        store,
        &put_in(&1.owners[@other], %{key: @origin, revision: 1}),
        {:address, {:row, :owners, @other}}
      )
    end

    test "#6 a new device may not take an owner key (W3b fallback)", %{store: store} do
      refuse(
        store,
        &put_in(&1.devices[@owner_key], &1.devices[@reader]),
        {:fallback, :owner_keys}
      )
    end

    test "#7 owner keys are unique (W3b fallback)", %{store: store} do
      refuse(
        store,
        &put_in(&1.owners[@other], %{key: @owner_key, revision: 1}),
        {:fallback, :owner_keys}
      )
    end

    test "#8 retired owner keys", %{store: store} do
      refuse(
        store,
        &%{&1 | retired_owner_keys: MapSet.put(&1.retired_owner_keys, "nothex")},
        {:address, {:member, :retired_owner_keys, "nothex"}}
      )

      refuse(
        store,
        &put_in(&1.devices[@retired], &1.devices[@reader]),
        {:fallback, :owner_keys}
      )
    end

    test "#38 device row", %{store: store} do
      refuse(store, &put_in(&1.devices[@reader].label, ""), {:address, {:row, :devices, @reader}})
    end

    test "#38 device removal rechecks the rows it owned", %{store: store} do
      next = %{store.engine | devices: Map.delete(store.engine.devices, @origin)}
      touched = codec_diff(store.engine, next)
      assert {:error, {:address, address}} = Store.incremental_verdict(store, next, touched)
      refute MapSet.member?(touched, address)
      assert {:error, :storage_unavailable, _} = Store.commit(store, {next, touched}, audit())
    end

    test "#39 agent row", %{store: store} do
      refuse(
        store,
        &put_in(&1.agents[@agent_key].id, "x"),
        {:address, {:row, :agents, @agent_key}}
      )
    end

    test "#39 a new device may not take an agent key (W3b fallback)", %{store: store} do
      refuse(
        store,
        &put_in(&1.devices[@agent_key], &1.devices[@reader]),
        {:fallback, :agent_keys}
      )
    end

    test "#40 agents are unique by person and id (W3b fallback)", %{store: store} do
      refuse(
        store,
        &put_in(&1.agents[@spare], &1.agents[@agent_key]),
        {:fallback, :agent_ids}
      )
    end
  end

  describe "enrollment" do
    test "#9 pending enrollment", %{store: store} do
      refuse(
        store,
        &put_in(&1.pending_enrollments[@code], %{pending_item() | state: :approved, key: @spare}),
        {:address, {:row, :pending_enrollments, @code}}
      )
    end

    test "#10 pending secrets are unique (W3b fallback)", %{store: store} do
      refuse(
        store,
        fn engine ->
          %{
            engine
            | pending_enrollments: %{@code => pending_item(), @code2 => pending_item()}
          }
        end,
        {:fallback, :pending_secrets}
      )
    end

    test "#11 pending history receipt", %{store: store} do
      refuse(
        store,
        &put_in(&1.pending_history["bad"], expired_receipt()),
        {:address, {:row, :pending_history, "bad"}}
      )
    end

    test "#12 a history code may not also be pending", %{store: store} do
      next =
        store.engine
        |> put_in([Access.key(:pending_enrollments), @code], pending_item())
        |> put_in([Access.key(:pending_history), @code], expired_receipt(<<8::256>>))

      touched = codec_diff(store.engine, next)

      assert {:error, {:address, {:row, field, @code}}} =
               Store.incremental_verdict(store, next, touched)

      assert field in [:pending_enrollments, :pending_history]
      assert {:error, :storage_unavailable, _} = Store.commit(store, {next, touched}, audit())
    end

    test "#13 secrets are unique across pending and history (W3b fallback)", %{store: store} do
      refuse(
        store,
        fn engine ->
          %{
            engine
            | pending_enrollments: %{@code => pending_item()},
              pending_history: %{@code2 => expired_receipt()}
          }
        end,
        {:fallback, :pending_secrets}
      )
    end

    test "#14 join invitation", %{store: store} do
      hash = :binary.copy(<<1>>, 32)

      refuse(
        store,
        &put_in(&1.join_invitations[hash], %{person: @person}),
        {:address, {:row, :join_invitations, hash}}
      )
    end

    test "#16 browser sign-in", %{store: store} do
      hash = :binary.copy(<<2>>, 32)

      record = %{
        person: "missing",
        owner_key: @owner_key,
        owner_revision: 1,
        issued_at: 0,
        expires_at: nil
      }

      refuse(
        store,
        &put_in(&1.browser_sign_ins[hash], record),
        {:address, {:row, :browser_sign_ins, hash}}
      )
    end

    test "#54 enrollment read grant", %{store: store} do
      refuse(
        store,
        &put_in(&1.enrollment_read_grants["x"], %{}),
        {:address, {:row, :enrollment_read_grants, "x"}}
      )
    end
  end

  describe "accounts and settings" do
    test "#17 maintainer grant", %{store: store} do
      refuse(
        store,
        &put_in(&1.maintainer_grants["x"], %{}),
        {:address, {:row, :maintainer_grants, "x"}}
      )
    end

    test "#18 hosted account needs an owner", %{store: store} do
      refuse(
        store,
        &put_in(&1.hosted_accounts["other"], %{person: @other, label: nil, test: false}),
        {:address, {:row, :hosted_accounts, "other"}}
      )
    end

    test "#18 removing the owner of a hosted account (W3b fallback)", %{store: store} do
      refuse(store, &%{&1 | owners: %{}}, {:fallback, :hosted_accounts})
    end

    test "#19 one hosted account per person (W3b fallback)", %{store: store} do
      refuse(
        store,
        &put_in(&1.hosted_accounts["second"], %{person: @person, label: nil, test: false}),
        {:fallback, :hosted_accounts}
      )
    end

    test "#20 delivery state", %{store: store} do
      refuse(
        store,
        &put_in(&1.delivery_states[@person], "BAD"),
        {:address, {:row, :delivery_states, @person}}
      )
    end

    test "#21 algorithm choice", %{store: store} do
      refuse(
        store,
        &put_in(&1.algorithm_choices[@person], "nope"),
        {:address, {:row, :algorithm_choices, @person}}
      )
    end

    test "#29 bounce configuration", %{store: store} do
      refuse(
        store,
        &put_in(&1.bounce_levels[@person], %{}),
        {:address, {:row, :bounce_levels, @person}}
      )
    end
  end

  describe "push" do
    test "#23 push binding highs", %{store: store} do
      refuse(
        store,
        &put_in(&1.push_bindings[{@person, @reader}].pending_high, 999),
        {:address, {:row, :push_bindings, {@person, @reader}}}
      )
    end

    test "#24 active push addresses are unique (W3b fallback)", %{store: store} do
      refuse(
        store,
        fn engine ->
          %{
            engine
            | push_bindings: %{
                {@person, @reader} => active_binding("00000000-0000-4000-8000-0000000000c1"),
                {@person, @origin} => active_binding("00000000-0000-4000-8000-0000000000c2")
              }
          }
        end,
        {:fallback, :push_addresses}
      )
    end

    test "#23 revoking a device ends its active binding (W3b fallback)", %{store: store} do
      binding = active_binding("00000000-0000-4000-8000-0000000000c1")
      engine = store.engine
      next = put_in(engine.push_bindings[{@person, @reader}], binding)
      touched = codec_diff(store.engine, next)
      assert :ok = Store.incremental_verdict(store, next, touched)
      assert {:ok, store} = Store.commit(store, {next, touched}, audit())

      refuse(store, &put_in(&1.devices[@reader].revoked, true), {:fallback, :push_devices})
    end
  end

  describe "device-owned rows" do
    test "#41 sender grant", %{store: store} do
      refuse(
        store,
        &put_in(&1.sender_grants[{@person, @spare}], MapSet.new([@sender])),
        {:address, {:row, :sender_grants, {@person, @spare}}}
      )
    end

    test "#42 subscription", %{store: store} do
      refuse(
        store,
        &put_in(&1.subscriptions[{@person, @spare}], %{revision: 0, senders: MapSet.new()}),
        {:address, {:row, :subscriptions, {@person, @spare}}}
      )
    end

    test "#43 delivery cursor", %{store: store} do
      refuse(
        store,
        &put_in(&1.cursors[{@person, @reader}], 999),
        {:address, {:row, :cursors, {@person, @reader}}}
      )
    end

    test "#50 conversation cursor", %{store: store} do
      refuse(
        store,
        &put_in(&1.conversation_cursors[{@person, @reader}], 999),
        {:address, {:row, :conversation_cursors, {@person, @reader}}}
      )
    end

    test "#51 conversation opened", %{store: store} do
      refuse(
        store,
        &%{&1 | conversation_opened: MapSet.put(&1.conversation_opened, {@person, @spare})},
        {:address, {:member, :conversation_opened, {@person, @spare}}}
      )
    end

    test "#52 operation receipt", %{store: store} do
      key = {@person, @spare, @uuid}
      refuse(store, &put_in(&1.receipts[key], %{}), {:address, {:row, :receipts, key}})
    end

    test "#53 source report grant fields", %{store: store} do
      key = {@person, @origin, ""}
      grant = %{delegation_ref: "d", revoked: false, senders: MapSet.new()}

      refuse(
        store,
        &put_in(&1.source_report_grants[key], grant),
        {:address, {:row, :source_report_grants, key}}
      )
    end
  end

  describe "origin feeds" do
    test "#25 origin feed high and entries", %{store: store} do
      refuse(store, &put_in(&1.origin_feeds[@scope].high, 9), :preflight)
    end

    test "#26 origin session", %{store: store} do
      key = {@person, @origin, @handle}

      refuse(
        store,
        &put_in(&1.origin_sessions[key].gone, "no"),
        {:address, {:row, :origin_sessions, key}}
      )
    end

    test "#27 origin receipt", %{store: store} do
      key = {@person, @origin, 3}

      refuse(
        store,
        &put_in(&1.origin_receipts[key], %{delivered_at: 0}),
        {:address, {:row, :origin_receipts, key}}
      )
    end

    test "#28 origin action", %{store: store} do
      refuse(
        store,
        &put_in(&1.origin_actions[{@person, @origin, 3}], %{op_id: "x", at: 1}),
        :preflight
      )
    end

    test "#36 an origin entry's upload must exist", %{store: store} do
      high = store.engine.origin_feeds[@scope].high + 1

      entry = %{
        "feed_seq" => high,
        "event_id" => Integer.to_string(high),
        "kind" => "discussion",
        "author" => "person",
        "session_handle" => @handle,
        "files" => [upload_file()]
      }

      refuse(
        store,
        fn engine ->
          engine
          |> put_in([Access.key(:origin_feeds), @scope, :high], high)
          |> put_in([Access.key(:origin_feeds), @scope, :entries, high], entry)
        end,
        {:address, {:origin_feed, @scope, {:entry, high}}}
      )
    end
  end

  describe "items and conversation" do
    test "#55 message record", %{store: store} do
      refuse(
        store,
        &put_in(&1.messages[@m1].accepted_seq, 999),
        {:address, {:row, :messages, @m1}}
      )
    end

    test "#55 message revision files must reference their uploads", %{store: store} do
      refuse(
        store,
        &put_in(&1.messages[@m1].files_by_revision, %{1 => [upload_file()]}),
        {:address, {:row, :messages, @m1}}
      )
    end

    test "#35 upload row", %{store: store} do
      upload = %{
        id: "x",
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

      refuse(store, &put_in(&1.uploads["x"], upload), {:address, {:row, :uploads, "x"}})
    end

    test "#44 free order", %{store: store} do
      refuse(store, &put_in(&1.messages[@m1].free_order, 999), {:address, {:row, :messages, @m1}})

      refuse(
        store,
        &put_in(&1.free_order_highs["missing"], 1),
        {:address, {:row, :free_order_highs, "missing"}}
      )
    end

    test "#45 conversation high needs its person", %{store: store} do
      refuse(
        store,
        &put_in(&1.conversation_highs["missing"], 0),
        {:address, {:row, :conversation_highs, "missing"}}
      )
    end

    test "#46 #47 conversation event beyond its high", %{store: store} do
      refuse(
        store,
        &put_in(
          &1.conversation_events[{@person, &1.conversation_highs[@person] + 1}],
          &1.conversation_events[{@person, 1}]
        ),
        {:check, :sequences}
      )
    end

    test "#57 #58 delivery event beyond its high", %{store: store} do
      refuse(
        store,
        &put_in(&1.events[{@person, 3}], &1.events[{@person, 1}]),
        {:check, :sequences}
      )
    end

    test "#48 chat read position", %{store: store} do
      refuse(
        store,
        &put_in(&1.chat_read_positions[@m1], 1),
        {:address, {:row, :chat_read_positions, @m1}}
      )
    end

    test "#49 reply group history", %{store: store} do
      refuse(
        store,
        &put_in(&1.reply_group_history[@m1], %{scores: %{}, score: 2}),
        {:address, {:row, :reply_group_history, @m1}}
      )
    end

    test "#49 raising the chat position rechecks reply scores", %{store: store} do
      engine = store.engine
      seq = origin_reply_seq(engine)
      next = put_in(engine.reply_group_history[@m1], %{scores: %{seq => 0.5}, score: 0.5})
      touched = codec_diff(store.engine, next)
      assert :ok = Store.incremental_verdict(store, next, touched)
      assert {:ok, store} = Store.commit(store, {next, touched}, audit())

      refuse(
        store,
        &put_in(&1.chat_read_positions[@m1], seq),
        {:address, {:row, :reply_group_history, @m1}}
      )
    end

    test "#56 source bindings are unique", %{store: store} do
      binding = fn seq ->
        %{
          "source_instance_id" => "s",
          "request_id" => "r",
          "request_revision" => "1",
          "acceptance_seq" => seq
        }
      end

      refuse(
        store,
        fn engine ->
          engine
          |> put_in([Access.key(:messages), @m1, :source_binding], binding.(1))
          |> put_in([Access.key(:messages), @m2, :source_binding], binding.(2))
        end,
        {:check, :source_bindings}
      )
    end

    test "#37 large-data references", %{store: store} do
      ref = %LargeData.Ref{id: "bad", size: -1, sha256: "bad", path: nil}
      engine = store.engine
      next = put_in(engine.sender_grants[{@person, @origin}], ref)
      touched = MapSet.new([{:row, :sender_grants, {@person, @origin}}])
      assert_refused(store, next, touched, :preflight)
    end
  end

  describe "later-slice fallbacks" do
    test "#3 derived default index (W3c fallback)", %{store: store} do
      next = %{
        store.engine
        | open_default_index: %{bad: 1},
          delivery_states: %{@person => "STOPPED"}
      }

      assert_refused(
        store,
        next,
        codec_diff(store.engine, next),
        {:fallback, :open_default_index}
      )
    end

    test "#30 bounce revision state (W3c fallback)", %{store: store} do
      refuse(store, &put_in(&1.bounce_revisions[@m1], %{}), {:fallback, :bounce_state})
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
