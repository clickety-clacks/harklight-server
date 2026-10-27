defmodule AttentionServer.Engine do
  @moduledoc """
  The stable-v3 relay decision core. A caller must supply a
  peer key derived from a verified TLS client certificate; the wire cannot
  select a person or key. `AttentionServer.Relay` serializes state changes.

  The trusted setup functions model provisioned persons, enrolled device keys,
  sender grants and revocation for conformance tests. They are not wire APIs.
  `AttentionServer.Relay` durably commits supported decisions and exposes
  internal replay/ACK sessions when configured with a store path. Authenticated
  transport and owner management are separate modules. Optional APNs bindings
  and provider work are durably committed through the relay.
  """

  alias AttentionServer.{
    APNS,
    AgentAccess,
    Conversation,
    ConversationFiles,
    DeviceIdentity,
    JSON,
    Message,
    SenderNames
  }

  @terminal_states ~w(answered cleared expired superseded)
  @pending_types ~w(resync ack push_register push_remove push_status)
  @push_hint_lifetime_ms 3_600_000
  @push_window_ms 300_000
  @push_claim_timeout_ms 30_000
  @maintainer_scopes MapSet.new(~w(devices senders view_items configure))

  defstruct relay_instance_id: nil,
            agent_cursor_key: nil,
            persons: %{},
            owners: %{},
            retired_owner_keys: MapSet.new(),
            pending_enrollments: %{},
            pending_history: %{},
            join_invitations: %{},
            browser_sign_ins: %{},
            browser_sign_in_clock_high_water: 0,
            pending_clock_high_water: 0,
            maintainer_grants: %{},
            devices: %{},
            agents: %{},
            sender_grants: %{},
            messages: %{},
            events: %{},
            conversation_highs: %{},
            conversation_events: %{},
            chat_read_positions: %{},
            conversation_cursors: %{},
            conversation_opened: MapSet.new(),
            receipts: %{},
            origin_feeds: %{},
            origin_sessions: %{},
            origin_receipts: %{},
            origin_actions: %{},
            uploads: %{},
            source_report_grants: %{},
            enrollment_read_grants: %{},
            subscriptions: %{},
            cursors: %{},
            hosted_accounts: %{},
            algorithm_choices: %{},
            jev_endpoint: nil,
            jev_model: nil,
            push_bindings: %{}

  def new do
    %__MODULE__{
      relay_instance_id: Base.encode16(:crypto.strong_rand_bytes(16), case: :lower),
      agent_cursor_key: :crypto.strong_rand_bytes(32)
    }
  end

  @doc false
  def claim_push(%__MODULE__{} = engine, now_ms) when is_integer(now_ms) do
    bindings =
      Map.new(engine.push_bindings, fn {key, binding} ->
        cond do
          binding.active and (binding.pending_high > 0 or binding.pending_conversation_high > 0) and
            is_integer(binding.deadline_at) and
              binding.deadline_at <= now_ms ->
            {key,
             clear_push_claim(%{
               binding
               | state: "expired",
                 pending_high: 0,
                 pending_conversation_high: 0,
                 deadline_at: nil,
                 next_at: nil
             })}

          binding.state == "sending" and is_integer(binding.claim_at) and
              binding.claim_at + @push_claim_timeout_ms <= now_ms ->
            {key, clear_push_claim(%{binding | state: "waiting_retry", next_at: now_ms})}

          true ->
            {key, binding}
        end
      end)

    next = %{engine | push_bindings: bindings}

    candidate =
      next.push_bindings
      |> Enum.filter(fn {_key, binding} ->
        binding.active and (binding.pending_high > 0 or binding.pending_conversation_high > 0) and
          binding.state in ["queued", "waiting_retry"] and
          is_integer(binding.next_at) and binding.next_at <= now_ms
      end)
      |> Enum.sort_by(fn {{person, device}, binding} ->
        {binding.next_at, person, device}
      end)
      |> List.first()

    case candidate do
      nil ->
        {:ok, next, nil}

      {{person, device} = key, binding} ->
        claim_id = uuid_v4()
        gathered = push_gathered_records(next, person, device, binding)
        presentation = if gathered == [], do: :background, else: :alert

        claim = %{
          person: person,
          device: device,
          revision: binding.revision,
          binding_id: binding.binding_id,
          profile_id: binding.profile_id,
          address: binding.address,
          high: binding.pending_high,
          conversation_high: binding.pending_conversation_high,
          presentation: presentation,
          badge: inbox_badge(next, person),
          deadline_at: binding.deadline_at,
          registered_at: binding.registered_at,
          claim_id: claim_id
        }

        claim =
          case gathered do
            [] ->
              claim

            [selected] ->
              claim = Map.put(claim, :alert, push_alert(selected))

              case push_locator(selected, person, device, binding.binding_id) do
                nil -> claim
                locator -> Map.put(claim, :locator, locator)
              end

            many ->
              Map.put(claim, :alert, grouped_push_alert(many))
          end

        claimed = %{
          binding
          | state: "sending",
            attempt_count: binding.attempt_count + 1,
            last_attempt_at: now_ms,
            claim_id: claim_id,
            claim_high: binding.pending_high,
            claim_conversation_high: binding.pending_conversation_high,
            claim_at: now_ms,
            next_at: nil
        }

        {:ok, %{next | push_bindings: Map.put(next.push_bindings, key, claimed)}, claim}
    end
  end

  @doc false
  def finish_push(%__MODULE__{} = engine, claim, outcome, now_ms) when is_integer(now_ms) do
    key = {claim.person, claim.device}

    case Map.get(engine.push_bindings, key) do
      %{active: true, revision: revision, binding_id: binding_id, claim_id: claim_id} = binding
      when revision == claim.revision and binding_id == claim.binding_id and
             claim_id == claim.claim_id ->
        updated = finish_push_binding(binding, claim, outcome, now_ms)
        {:ok, %{engine | push_bindings: Map.put(engine.push_bindings, key, updated)}}

      _ ->
        {:ok, engine}
    end
  end

  defp push_register(engine, %{person: person, key: device}, frame) do
    with :ok <- valid_no_selectors(frame),
         :ok <- valid_op(frame),
         :ok <- valid_push_revision(frame),
         :ok <- valid_push_address(frame),
         :ok <- valid_push_profile(frame) do
      address = String.downcase(frame["address"])

      semantic = %{
        "type" => "push_register",
        "revision" => frame["revision"],
        "profile_id" => frame["profile_id"],
        "address_kind" => "apns_token",
        "address_sha256" => Base.encode16(:crypto.hash(:sha256, address), case: :lower)
      }

      case receipt(engine, person, device, frame["op_id"], semantic) do
        {:duplicate, reply} ->
          {:ok, engine, Map.put(reply, "profile_id", frame["profile_id"])}

        :conflict ->
          failure("operation_conflict")

        :missing ->
          register_push_binding(engine, person, device, frame, semantic, address)
      end
    else
      {:error, field} -> failure("invalid_field", field)
    end
  end

  defp register_push_binding(engine, person, device, frame, semantic, address) do
    key = {person, device}
    old = Map.get(engine.push_bindings, key)
    revision = frame["revision"]

    cond do
      old && revision <= old.revision ->
        failure("stale_push_binding")

      Enum.any?(engine.push_bindings, fn
        {{^person, ^device}, _binding} -> false
        {_key, %{active: true, address: ^address}} -> true
        _ -> false
      end) ->
        failure("push_address_in_use")

      true ->
        now_ms = System.system_time(:millisecond)
        high = Map.fetch!(engine.persons, person)
        acknowledged = Map.get(engine.cursors, key, 0)
        pending_high = if high > acknowledged, do: high, else: 0
        conversation_high = Map.get(engine.conversation_highs, person, 0)
        conversation_cursor = Map.get(engine.conversation_cursors, key, 0)

        pending_conversation_high =
          if MapSet.member?(engine.conversation_opened, key) and
               conversation_high > conversation_cursor,
             do: conversation_high,
             else: 0

        pending? = pending_high > 0 or pending_conversation_high > 0
        last_attempt_at = if old, do: old.last_attempt_at, else: nil
        open_window_at = if old && is_integer(old.next_at), do: old.next_at, else: nil

        binding = %{
          revision: revision,
          binding_id: uuid_v4(),
          profile_id: frame["profile_id"],
          address: address,
          active: true,
          registered_at: now_ms,
          state: if(pending?, do: "queued", else: "ready"),
          pending_high: pending_high,
          accepted_high: 0,
          pending_conversation_high: pending_conversation_high,
          accepted_conversation_high: 0,
          deadline_at: if(pending?, do: now_ms + @push_hint_lifetime_ms, else: nil),
          next_at: if(pending?, do: open_window_at || now_ms + @push_window_ms, else: nil),
          attempt_count: 0,
          last_attempt_at: last_attempt_at,
          last_outcome: nil,
          claim_id: nil,
          claim_high: nil,
          claim_conversation_high: nil,
          claim_at: nil
        }

        reply = %{
          "revision" => revision,
          "binding_id" => binding.binding_id,
          "profile_id" => binding.profile_id
        }

        next =
          engine
          |> Map.put(:push_bindings, Map.put(engine.push_bindings, key, binding))
          |> save_receipt(person, device, frame["op_id"], semantic, reply)

        {:ok, next, reply}
    end
  end

  defp push_remove(engine, %{person: person, key: device}, frame) do
    with :ok <- valid_no_selectors(frame),
         :ok <- valid_op(frame),
         :ok <- valid_push_revision(frame) do
      semantic = %{"type" => "push_remove", "revision" => frame["revision"]}

      case receipt(engine, person, device, frame["op_id"], semantic) do
        {:duplicate, reply} ->
          {:ok, engine, reply}

        :conflict ->
          failure("operation_conflict")

        :missing ->
          key = {person, device}
          old = Map.get(engine.push_bindings, key)
          revision = frame["revision"]

          if old && revision <= old.revision do
            failure("stale_push_binding")
          else
            canceled = canceled_push_binding(revision)
            reply = %{"revision" => revision}

            next =
              engine
              |> Map.put(:push_bindings, Map.put(engine.push_bindings, key, canceled))
              |> save_receipt(person, device, frame["op_id"], semantic, reply)

            {:ok, next, reply}
          end
      end
    else
      {:error, field} -> failure("invalid_field", field)
    end
  end

  defp push_status(engine, %{person: person, key: device}, frame) do
    if Enum.all?(Map.keys(frame), &(&1 in ["v", "type", "ref"])) do
      key = {person, device}

      fields =
        case Map.get(engine.push_bindings, key) do
          nil ->
            %{"push" => %{"revision" => 0, "state" => "unconfigured"}}

          binding ->
            status = %{
              "revision" => binding.revision,
              "state" => binding.state,
              "attempt_count" => binding.attempt_count
            }

            status =
              if binding.active do
                Map.merge(status, %{
                  "binding_id" => binding.binding_id,
                  "profile_id" => binding.profile_id
                })
              else
                status
              end

            status = put_status_time(status, "last_attempt_at", binding.last_attempt_at)
            status = put_status_time(status, "next_attempt_at", binding.next_at)
            status = put_status_time(status, "deadline_at", binding.deadline_at)

            status =
              if binding.last_outcome,
                do: Map.put(status, "last_outcome", binding.last_outcome),
                else: status

            %{"push" => status}
        end

      {:ok, engine, fields}
    else
      failure("invalid_field")
    end
  end

  defp valid_push_revision(%{"revision" => revision})
       when is_integer(revision) and revision > 0 and revision < 9_223_372_036_854_775_808,
       do: :ok

  defp valid_push_revision(_), do: {:error, "revision"}

  defp valid_push_address(%{"address_kind" => "apns_token", "address" => address})
       when is_binary(address) do
    if Regex.match?(~r/\A[0-9a-fA-F]{64}\z/, address), do: :ok, else: {:error, "address"}
  end

  defp valid_push_address(%{"address_kind" => kind}) when kind != "apns_token",
    do: {:error, "address_kind"}

  defp valid_push_address(_), do: {:error, "address"}

  defp valid_push_profile(%{"profile_id" => profile_id}) do
    if APNS.profile_id?(profile_id), do: :ok, else: {:error, "profile_id"}
  end

  defp valid_push_profile(_), do: {:error, "profile_id"}

  defp put_status_time(status, _field, nil), do: status
  defp put_status_time(status, field, value), do: Map.put(status, field, value)

  defp canceled_push_binding(revision) do
    %{
      revision: revision,
      binding_id: nil,
      profile_id: nil,
      address: nil,
      active: false,
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
      last_outcome: nil,
      claim_id: nil,
      claim_high: nil,
      claim_conversation_high: nil,
      claim_at: nil
    }
  end

  defp clear_push_claim(binding) do
    %{binding | claim_id: nil, claim_high: nil, claim_conversation_high: nil, claim_at: nil}
  end

  defp finish_push_binding(binding, claim, outcome, now_ms) do
    state = Map.get(outcome, :state)
    http_status = Map.get(outcome, :http_status)
    reason = Map.get(outcome, :reason)
    reason = if APNS.safe_reason?(reason), do: reason, else: nil
    retry_after_ms = Map.get(outcome, :retry_after_ms, 0)

    last_outcome =
      %{"provider" => "apns", "state" => state}
      |> maybe_put("http_status", http_status)
      |> maybe_put("reason", reason)
      |> maybe_put("request_id", Map.get(outcome, :request_id))

    base = %{binding | last_outcome: last_outcome}

    case state do
      "provider_accepted" ->
        base = %{
          base
          | accepted_high: max(binding.accepted_high, claim.high),
            accepted_conversation_high:
              max(binding.accepted_conversation_high, claim.conversation_high)
        }

        if binding.pending_high <= claim.high and
             binding.pending_conversation_high <= claim.conversation_high do
          clear_push_claim(%{
            base
            | state: "ready",
              pending_high: 0,
              pending_conversation_high: 0,
              deadline_at: nil,
              next_at: nil
          })
        else
          clear_push_claim(%{
            base
            | state: "queued",
              next_at: binding.next_at || now_ms + @push_window_ms
          })
        end

      "endpoint_invalid" ->
        invalidated_at = Map.get(outcome, :invalidated_at_ms)

        if is_integer(invalidated_at) and invalidated_at >= binding.registered_at do
          clear_push_claim(%{
            base
            | active: false,
              address: nil,
              state: "endpoint_invalid",
              pending_high: 0,
              pending_conversation_high: 0,
              deadline_at: nil,
              next_at: nil
          })
        else
          blocked_push(base, now_ms)
        end

      "expired" ->
        clear_push_claim(%{
          base
          | state: "expired",
            pending_high: 0,
            pending_conversation_high: 0,
            deadline_at: nil,
            next_at: nil
        })

      "retry" ->
        retry_push(base, now_ms, retry_after_ms)

      _ ->
        blocked_push(base, now_ms)
    end
  end

  defp retry_push(binding, now_ms, retry_after_ms) do
    cond do
      not is_integer(binding.deadline_at) or binding.deadline_at <= now_ms ->
        clear_push_claim(%{
          binding
          | state: "expired",
            pending_high: 0,
            pending_conversation_high: 0,
            deadline_at: nil,
            next_at: nil
        })

      true ->
        backoff = min(1_200_000, 1_000 * trunc(:math.pow(2, min(binding.attempt_count - 1, 10))))

        next_at = now_ms + max(0, retry_after_ms) + backoff

        if next_at >= binding.deadline_at do
          clear_push_claim(%{
            binding
            | state: "expired",
              pending_high: 0,
              pending_conversation_high: 0,
              deadline_at: nil,
              next_at: nil
          })
        else
          clear_push_claim(%{binding | state: "waiting_retry", next_at: next_at})
        end
    end
  end

  defp blocked_push(binding, now_ms) do
    if not is_integer(binding.deadline_at) or binding.deadline_at <= now_ms do
      clear_push_claim(%{
        binding
        | state: "expired",
          pending_high: 0,
          pending_conversation_high: 0,
          deadline_at: nil,
          next_at: nil
      })
    else
      clear_push_claim(%{binding | state: "blocked_configuration", next_at: nil})
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp uuid_v4 do
    <<a::binary-size(6), b::8, c::8, d::8, e::binary-size(7)>> = :crypto.strong_rand_bytes(16)
    b = Bitwise.bor(Bitwise.band(b, 15), 64)
    d = Bitwise.bor(Bitwise.band(d, 63), 128)
    hex = Base.encode16(<<a::binary, b, c, d, e::binary>>, case: :lower)

    Enum.join(
      [
        String.slice(hex, 0, 8),
        String.slice(hex, 8, 4),
        String.slice(hex, 12, 4),
        String.slice(hex, 16, 4),
        String.slice(hex, 20, 12)
      ],
      "-"
    )
  end

  def setup_person(%__MODULE__{} = engine, person)
      when is_binary(person) and person != "" do
    if Map.has_key?(engine.persons, person) do
      {:error, :person_exists}
    else
      {:ok, %{engine | persons: Map.put(engine.persons, person, 0)}}
    end
  end

  def setup_person(_, _), do: {:error, :invalid_person}

  @doc "Local-operator bootstrap; the owner key is a certificate SPKI fingerprint."
  def operator_bootstrap(%__MODULE__{} = engine, person, owner_key) do
    cond do
      not Message.uuid_v4?(person) ->
        {:error, :invalid_person}

      Map.has_key?(engine.persons, person) ->
        {:error, :person_exists}

      not hex_key?(owner_key) or principal_key_used?(engine, owner_key) ->
        {:error, :key_in_use}

      true ->
        {:ok,
         %{
           engine
           | persons: Map.put(engine.persons, person, 0),
             owners: Map.put(engine.owners, person, %{key: owner_key, revision: 1})
         }}
    end
  end

  @doc """
  Create or return the one person for a hosted account reference.

  The account's owner authority is a server-held key: a hosted account acts only
  as its own person through that key and gains nothing over any other person.
  Repeating a creation returns the same person; the test designation, once
  recorded, stays.
  """
  def hosted_account_create(%__MODULE__{} = engine, account_ref, test?, person, owner_key)
      when is_boolean(test?) do
    cond do
      not hosted_account_ref?(account_ref) ->
        {:error, :invalid_account_ref}

      Map.has_key?(engine.hosted_accounts, account_ref) ->
        existing = engine.hosted_accounts[account_ref]
        record = %{existing | test: existing.test or test?}

        {:ok, %{engine | hosted_accounts: Map.put(engine.hosted_accounts, account_ref, record)},
         record}

      true ->
        with {:ok, next} <- operator_bootstrap(engine, person, owner_key) do
          record = %{person: person, label: nil, test: test?}

          {:ok, %{next | hosted_accounts: Map.put(next.hosted_accounts, account_ref, record)},
           record}
        end
    end
  end

  @doc "Record the readable label the hosted side supplies for a hosted person."
  def hosted_account_label(%__MODULE__{} = engine, person, label) do
    case hosted_account(engine, person) do
      %{account_ref: ref} = record ->
        if hosted_label?(label) do
          stored = record |> Map.delete(:account_ref) |> Map.put(:label, label)
          {:ok, %{engine | hosted_accounts: Map.put(engine.hosted_accounts, ref, stored)}}
        else
          {:error, :invalid_label}
        end

      nil ->
        {:error, :not_hosted}
    end
  end

  @doc "The hosted account record owning a person, or nil for every self-hosted person."
  def hosted_account(%__MODULE__{} = engine, person) do
    Enum.find_value(engine.hosted_accounts, fn {ref, record} ->
      if record.person == person, do: Map.put(record, :account_ref, ref)
    end)
  end

  @doc "The server-held owner key a hosted account acts under; nil for any other person."
  def hosted_owner_key(%__MODULE__{} = engine, person) do
    case {hosted_account(engine, person), Map.get(engine.owners, person)} do
      {%{}, %{key: key}} -> key
      _ -> nil
    end
  end

  @doc false
  def set_algorithm_choice(%__MODULE__{} = engine, person, algorithm)
      when algorithm in ["baseline", "jev-set"] do
    if Map.has_key?(engine.persons, person) do
      if Map.get(engine.algorithm_choices, person, "baseline") == algorithm do
        {:ok, engine}
      else
        {:ok, %{engine | algorithm_choices: Map.put(engine.algorithm_choices, person, algorithm)}}
      end
    else
      {:error, :not_found}
    end
  end

  def set_algorithm_choice(%__MODULE__{}, _person, _algorithm), do: {:error, :unknown_algorithm}

  @doc false
  def set_jev_connection(%__MODULE__{} = engine, endpoint, model) do
    {:ok, %{engine | jev_endpoint: endpoint, jev_model: model}}
  end

  @doc """
  Return the designated test person to having no devices, pending requests or
  open invitations. Only a person whose account record is durably marked as the
  test account can be reset; the person, its owner key and its sign-ins remain.
  """
  def hosted_test_reset(%__MODULE__{} = engine, person) do
    case hosted_account(engine, person) do
      %{test: true} ->
        keys =
          engine.devices
          |> Enum.filter(fn {_key, device} -> device.person == person end)
          |> Enum.map(fn {key, _} -> key end)
          |> MapSet.new()

        pending_codes =
          engine.pending_enrollments
          |> Enum.filter(fn {_code, item} -> item.person == person end)
          |> Enum.map(fn {code, _} -> code end)

        history_codes =
          engine.pending_history
          |> Enum.filter(fn {_code, item} -> Map.get(item, :person) == person end)
          |> Enum.map(fn {code, _} -> code end)

        invitations =
          engine.join_invitations
          |> Enum.filter(fn {_hash, invite} -> invite.person == person end)
          |> Enum.map(fn {hash, _} -> hash end)

        retained? = fn
          {^person, key} -> not MapSet.member?(keys, key)
          {^person, key, _} -> not MapSet.member?(keys, key)
          _ -> true
        end

        drop = fn map -> Map.filter(map, fn {k, _} -> retained?.(k) end) end

        cond do
          Enum.any?(engine.agents, fn {_, agent} -> agent.person == person end) or
            Enum.any?(engine.messages, fn {{p, _, _}, _} -> p == person end) or
            Enum.any?(engine.uploads, fn {_, upload} -> upload.person == person end) or
              Enum.any?(engine.origin_feeds, fn {{p, _}, _} -> p == person end) ->
            {:error, :not_resettable}

          true ->
            next = %{
              engine
              | devices: Map.drop(engine.devices, MapSet.to_list(keys)),
                pending_enrollments: Map.drop(engine.pending_enrollments, pending_codes),
                pending_history: Map.drop(engine.pending_history, history_codes),
                join_invitations: Map.drop(engine.join_invitations, invitations),
                push_bindings: drop.(engine.push_bindings),
                sender_grants: drop.(engine.sender_grants),
                subscriptions: drop.(engine.subscriptions),
                cursors: drop.(engine.cursors),
                conversation_cursors: drop.(engine.conversation_cursors),
                conversation_opened: MapSet.filter(engine.conversation_opened, retained?),
                receipts: drop.(engine.receipts),
                source_report_grants: drop.(engine.source_report_grants),
                enrollment_read_grants:
                  Map.filter(engine.enrollment_read_grants, fn {_, grant} ->
                    not (grant.person == person and MapSet.member?(keys, grant.connector))
                  end)
            }

            {:ok, next, MapSet.to_list(keys)}
        end

      _ ->
        {:error, :not_test_account}
    end
  end

  defp hosted_account_ref?(ref),
    do:
      is_binary(ref) and byte_size(ref) in 1..256 and String.valid?(ref) and
        not Regex.match?(~r/[\s\p{C}]/u, ref)

  defp hosted_label?(label),
    do:
      is_binary(label) and byte_size(label) in 1..256 and String.valid?(label) and
        String.trim(label) != "" and not Regex.match?(~r/\p{C}/u, label)

  @doc "Local-operator recovery preserves person identity and retires the old key forever."
  def operator_recover(%__MODULE__{} = engine, person, new_key) do
    case Map.get(engine.owners, person) do
      %{key: old_key, revision: revision} = owner ->
        if hex_key?(new_key) and not principal_key_used?(engine, new_key) do
          {:ok,
           %{
             engine
             | owners:
                 Map.put(engine.owners, person, %{owner | key: new_key, revision: revision + 1}),
               retired_owner_keys: MapSet.put(engine.retired_owner_keys, old_key)
           }}
        else
          {:error, :key_in_use}
        end

      _ ->
        {:error, :not_found}
    end
  end

  def owner_revision(%__MODULE__{} = engine, person, key) do
    case Map.get(engine.owners, person) do
      %{key: ^key, revision: revision} -> {:ok, revision}
      _ -> {:error, :forbidden}
    end
  end

  @doc "Record an inert maintainer grant under the target person's current owner."
  def owner_grant_maintainer(engine, person, owner_key, grant_id, grantee, scopes, now_ms) do
    with {:ok, _revision} <- owner_revision(engine, person, owner_key),
         true <-
           Message.uuid_v4?(grant_id) and Map.has_key?(engine.persons, grantee),
         true <-
           is_list(scopes) and scopes != [] and length(scopes) <= 4 and
             Enum.all?(scopes, &MapSet.member?(@maintainer_scopes, &1)) and
             length(Enum.uniq(scopes)) == length(scopes),
         true <- is_integer(now_ms) and now_ms > 0 do
      grant = %{
        target_person: person,
        grantee_person: grantee,
        scopes: MapSet.new(scopes),
        granted_at: now_ms,
        revoked_at: nil,
        granted_by: owner_key,
        revoked_by: nil
      }

      case Map.get(engine.maintainer_grants, grant_id) do
        nil ->
          {:ok, %{engine | maintainer_grants: Map.put(engine.maintainer_grants, grant_id, grant)}}

        %{target_person: ^person, grantee_person: ^grantee, scopes: existing, revoked_at: nil}
        when existing == grant.scopes ->
          {:ok, engine}

        _ ->
          {:error, :not_found}
      end
    else
      {:error, :forbidden} -> {:error, :forbidden}
      _ -> {:error, :invalid_grant}
    end
  end

  @doc "Tombstone an inert maintainer grant; its ID is never reusable."
  def owner_revoke_maintainer(engine, person, owner_key, grant_id, now_ms) do
    with {:ok, _revision} <- owner_revision(engine, person, owner_key),
         %{target_person: ^person} = grant <- Map.get(engine.maintainer_grants, grant_id),
         true <- is_integer(now_ms) and now_ms >= grant.granted_at do
      if is_integer(grant.revoked_at) do
        {:ok, engine}
      else
        revoked = %{grant | revoked_at: now_ms, revoked_by: owner_key}
        {:ok, %{engine | maintainer_grants: Map.put(engine.maintainer_grants, grant_id, revoked)}}
      end
    else
      {:error, :forbidden} -> {:error, :forbidden}
      _ -> {:error, :not_found}
    end
  end

  @doc "Resolve only a current owner public key; retired keys are never owners."
  def owner_identity(%__MODULE__{} = engine, key) do
    case Enum.find(engine.owners, fn {_person, owner} -> owner.key == key end) do
      {person, %{revision: revision}} -> {:ok, person, revision}
      _ -> {:error, :forbidden}
    end
  end

  defp principal_key_used?(engine, key) do
    Map.has_key?(engine.devices, key) or Map.has_key?(engine.agents, key) or
      MapSet.member?(engine.retired_owner_keys, key) or
      Enum.any?(engine.owners, fn {_person, owner} -> owner.key == key end)
  end

  @doc "Enroll one separate service key under the authenticated person owner."
  def owner_register_agent(engine, person, owner_key, id, key) do
    with {:ok, _} <- owner_revision(engine, person, owner_key) do
      cond do
        not Message.uuid_v4?(id) or not hex_key?(key) ->
          {:error, :invalid_agent}

        principal_key_used?(engine, key) or pending_key_used?(engine, key) ->
          {:error, :key_in_use}

        Enum.any?(engine.agents, fn {_, agent} -> agent.person == person and agent.id == id end) ->
          {:error, :id_in_use}

        true ->
          agent = %{
            person: person,
            id: id,
            senders: MapSet.new(),
            read_grants: MapSet.new(),
            respond_grants: MapSet.new(),
            grant_revision: 1,
            granted_by: owner_key,
            grant_history: [
              grant_snapshot(1, owner_key, MapSet.new(), MapSet.new(), false, "register")
            ],
            revoked: false,
            enrollment_revision: 1
          }

          {:ok, %{engine | agents: Map.put(engine.agents, key, agent)}}
      end
    end
  end

  defp pending_key_used?(engine, key) do
    Enum.any?(engine.pending_enrollments, fn {_, item} -> item.key == key end) or
      Enum.any?(engine.pending_history, fn {_, item} -> item.key == key end)
  end

  @doc "Replace both independent scopes at one durable, owner-attributed revision."
  def owner_replace_agent_grants(engine, person, owner_key, key, expected_revision, read, respond) do
    with {:ok, _} <- owner_revision(engine, person, owner_key),
         %{person: ^person, revoked: false} = agent <- Map.get(engine.agents, key),
         true <- expected_revision == agent.grant_revision,
         true <- AgentAccess.valid_patterns?(read) and AgentAccess.valid_patterns?(respond) do
      next_revision = agent.grant_revision + 1
      read_set = MapSet.new(read)
      respond_set = MapSet.new(respond)

      updated = %{
        agent
        | read_grants: read_set,
          respond_grants: respond_set,
          grant_revision: next_revision,
          granted_by: owner_key,
          grant_history:
            agent.grant_history ++
              [grant_snapshot(next_revision, owner_key, read_set, respond_set, false, "replace")]
      }

      {:ok, %{engine | agents: Map.put(engine.agents, key, updated)}}
    else
      {:error, :forbidden} -> {:error, :forbidden}
      false -> {:error, :stale_or_invalid_grant}
      _ -> {:error, :not_found}
    end
  end

  def owner_revoke_agent(engine, person, owner_key, key) do
    with {:ok, _} <- owner_revision(engine, person, owner_key),
         %{person: ^person} = agent <- Map.get(engine.agents, key) do
      if agent.revoked do
        {:ok, engine}
      else
        next_revision = agent.grant_revision + 1

        updated = %{
          agent
          | revoked: true,
            enrollment_revision: agent.enrollment_revision + 1,
            grant_revision: next_revision,
            granted_by: owner_key,
            grant_history:
              agent.grant_history ++
                [
                  grant_snapshot(
                    next_revision,
                    owner_key,
                    agent.read_grants,
                    agent.respond_grants,
                    true,
                    "revoke"
                  )
                ]
        }

        {:ok, %{engine | agents: Map.put(engine.agents, key, updated)}}
      end
    else
      {:error, :forbidden} -> {:error, :forbidden}
      _ -> {:error, :not_found}
    end
  end

  defp grant_snapshot(revision, actor, read, respond, revoked, action) do
    %{
      revision: revision,
      actor: actor,
      read_grants: read,
      respond_grants: respond,
      revoked: revoked,
      action: action,
      at_ms: System.system_time(:millisecond)
    }
  end

  def owner_list_agents(engine, person, owner_key) do
    with {:ok, _} <- owner_revision(engine, person, owner_key) do
      agents =
        for {key, %{person: ^person} = agent} <- engine.agents do
          %{
            "agent_id" => agent.id,
            "key_fingerprint" => key,
            "read_grants" => agent.read_grants |> MapSet.to_list() |> Enum.sort(),
            "respond_grants" => agent.respond_grants |> MapSet.to_list() |> Enum.sort(),
            "grant_revision" => agent.grant_revision,
            "revoked" => agent.revoked,
            "granted_by" => agent.granted_by
          }
        end

      {:ok, Enum.sort_by(agents, & &1["agent_id"])}
    end
  end

  def agent_identity(engine, key) do
    case Map.get(engine.agents, key) do
      %{person: person, id: id, revoked: false, enrollment_revision: revision} ->
        {:ok, person, id, revision}

      _ ->
        {:error, :forbidden}
    end
  end

  def agent_search(engine, key, request), do: AgentAccess.search(engine, key, request)
  def agent_get(engine, key, request), do: AgentAccess.get(engine, key, request)

  def agent_answer(engine, key, frame) do
    case Map.get(engine.agents, key) do
      %{person: person, id: id, revoked: false} = agent ->
        sender = frame["sender_id"]

        cond do
          not AgentAccess.allowed?(agent.read_grants, sender) or
              not AgentAccess.allowed?(agent.respond_grants, sender) ->
            failure("forbidden")

          Enum.sort(Map.keys(frame)) !=
            ~w(acceptance_seq answer deliberate intent_ref message_id op_id sender_id type v) or
              frame["v"] != 1 ->
            failure("invalid_field", "request")

          frame["type"] != "answer" or frame["deliberate"] != true or
              not bounded_text?(frame["intent_ref"]) ->
            failure("invalid_field", "intent_ref")

          not (is_integer(frame["acceptance_seq"]) and frame["acceptance_seq"] > 0) ->
            failure("invalid_field", "acceptance_seq")

          is_nil(Map.get(engine.messages, {person, sender, frame["message_id"]})) ->
            failure("not_found")

          get_in(engine.messages, [{person, sender, frame["message_id"]}, :accepted_seq]) !=
              frame["acceptance_seq"] ->
            failure("stale_target")

          true ->
            terminal(engine, %{person: person, key: key, id: id, kind: :agent}, frame, true)
        end

      _ ->
        failure("forbidden")
    end
  end

  @pending_limit 1_024
  @pending_lifetime_ms 24 * 60 * 60 * 1_000
  @status_lifetime_ms 24 * 60 * 60 * 1_000

  @doc "Create a content-free pending request; only the secret hash is persisted."
  def pending_enroll(
        %__MODULE__{pending_clock_high_water: floor},
        _cert,
        _label,
        _code,
        _hash,
        now_ms
      )
      when is_integer(now_ms) and now_ms < floor,
      do: {:error, :clock_unavailable}

  def pending_enroll(%__MODULE__{} = engine, cert_der, label, code, secret_hash, now_ms)
      when is_integer(now_ms) and now_ms >= 0 do
    with {:ok, key} <- DeviceIdentity.from_certificate(cert_der) do
      compacted = compact_pending(engine, now_ms)

      cond do
        not bounded_text?(label) or not valid_code?(code) or
          not (is_binary(secret_hash) and byte_size(secret_hash) == 32) or
            not (is_integer(now_ms) and now_ms >= 0) ->
          {:error, :invalid_request}

        principal_key_used?(compacted, key) ->
          {:error, :key_in_use}

        Enum.any?(compacted.pending_enrollments, fn {_code, item} ->
          item.key == key and item.state == :pending
        end) ->
          {:error, :key_pending}

        map_size(compacted.pending_enrollments) + map_size(compacted.pending_history) >=
            @pending_limit ->
          {:error, :capacity}

        Map.has_key?(compacted.pending_enrollments, code) or
          Map.has_key?(compacted.pending_history, code) or
          Enum.any?(compacted.pending_enrollments, fn {_code, item} ->
            item.secret_hash == secret_hash
          end) or
            Enum.any?(compacted.pending_history, fn {_code, item} ->
              item.secret_hash == secret_hash
            end) ->
          {:error, :collision}

        true ->
          expires_at = now_ms + @pending_lifetime_ms

          item = %{
            key: key,
            certificate: cert_der,
            label: label,
            secret_hash: secret_hash,
            expires_at: expires_at,
            status_expires_at: expires_at + @status_lifetime_ms,
            state: :pending,
            person: nil,
            request_generation: 1
          }

          {:ok,
           %{
             compacted
             | pending_enrollments: Map.put(compacted.pending_enrollments, code, item),
               pending_clock_high_water: now_ms
           }}
      end
    end
  end

  def pending_enroll(_, _, _, _, _, _), do: {:error, :invalid_request}

  def pending_status(%__MODULE__{} = engine, secret_hash, now_ms) do
    now_ms = max(now_ms, engine.pending_clock_high_water)

    case find_pending_secret(engine, secret_hash) do
      {_, %{status_expires_at: deadline}} when now_ms >= deadline ->
        {:error, :not_found}

      {code, %{state: :approved, person: person, key: key} = item} ->
        if get_in(engine.devices, [key, :revoked]) == false do
          if Map.get(item, :joined, false) do
            {:ok, pending_response(Map.put(item, :code, code), "approved")}
          else
            {:ok,
             %{
               "state" => "approved",
               "person_id" => person,
               "device_id" => key,
               "request_generation" => Map.get(item, :request_generation, 1)
             }}
          end
        else
          {:ok, %{"state" => "revoked"}}
        end

      {_, %{state: :expired}} ->
        {:ok, %{"state" => "expired"}}

      {_, %{state: state}} when state in [:declined, :cancelled] ->
        {:ok, %{"state" => Atom.to_string(state)}}

      {code, %{state: :pending, expires_at: deadline, person: person} = item}
      when now_ms >= deadline and not is_nil(person) ->
        {:ok, pending_response(Map.put(item, :code, code), "expired")}

      {_, %{state: :pending, expires_at: deadline}} when now_ms >= deadline ->
        {:ok, %{"state" => "expired"}}

      {code, %{state: :pending} = item} ->
        if is_nil(item.person),
          do: {:ok, %{"state" => "pending"}},
          else: {:ok, pending_response(Map.put(item, :code, code), "pending")}

      _ ->
        {:error, :not_found}
    end
  end

  defp pending_response(item, state) do
    base = %{
      "state" => state,
      "approval_code" => item.code,
      "expires_at" => item.expires_at,
      "status_expires_at" => item.status_expires_at,
      "device_id" => item.key,
      "request_generation" => Map.get(item, :request_generation, 1)
    }

    if item.person, do: Map.put(base, "person_id", item.person), else: base
  end

  @doc "Resume one original secret-bound waiting request; only its approval epoch changes."
  def pending_resume(engine, secret_hash, cert_der, fresh_code, now_ms) do
    cond do
      not is_integer(now_ms) or now_ms < engine.pending_clock_high_water ->
        {:error, :clock_unavailable}

      true ->
        with {:ok, key} <- DeviceIdentity.from_certificate(cert_der),
             {old_code, item} <- find_pending_secret(engine, secret_hash),
             true <- item.key == key do
          cond do
            Map.get(item, :joined, false) != true or is_nil(item.person) ->
              {:error, :not_found}

            item.state == :expired ->
              {:error, :not_found}

            item.state != :pending ->
              case item.state do
                :approved ->
                  if get_in(engine.devices, [key, :revoked]) == false,
                    do:
                      {:ok, engine, pending_response(Map.put(item, :code, old_code), "approved")},
                    else: {:ok, engine, %{"state" => "revoked"}}

                state when state in [:cancelled, :declined] ->
                  {:ok, engine, %{"state" => Atom.to_string(state)}}

                _ ->
                  {:error, :not_found}
              end

            is_nil(item.person) or not Map.has_key?(engine.persons, item.person) ->
              {:error, :not_found}

            principal_key_used?(engine, key) ->
              {:error, :conflict}

            now_ms < item.expires_at ->
              {:ok, engine, pending_response(Map.put(item, :code, old_code), "pending")}

            not valid_code?(fresh_code) or Map.has_key?(engine.pending_enrollments, fresh_code) or
                Map.has_key?(engine.pending_history, fresh_code) ->
              {:error, :collision}

            true ->
              renewed =
                item
                |> Map.put(:certificate, cert_der)
                |> Map.put(:legacy_recovery, false)
                |> Map.put(:expires_at, now_ms + @pending_lifetime_ms)
                |> Map.put(
                  :status_expires_at,
                  now_ms + @pending_lifetime_ms + @status_lifetime_ms
                )
                |> Map.put(:request_generation, item.request_generation + 1)

              invitations =
                Map.new(engine.join_invitations, fn {hash, invite} ->
                  if invite.code == old_code and invite.key == key and
                       invite.secret_hash == secret_hash and invite.person == item.person,
                     do: {hash, %{invite | code: fresh_code}},
                     else: {hash, invite}
                end)

              next = %{
                engine
                | pending_enrollments:
                    engine.pending_enrollments
                    |> Map.delete(old_code)
                    |> Map.put(fresh_code, renewed),
                  join_invitations: invitations,
                  pending_clock_high_water: now_ms
              }

              {:ok, next, pending_response(Map.put(renewed, :code, fresh_code), "pending")}
          end
        else
          _ -> {:error, :not_found}
        end
    end
  end

  def pending_lookup(%__MODULE__{} = engine, person, owner_key, code, now_ms) do
    now_ms = max(now_ms, engine.pending_clock_high_water)

    with {:ok, _revision} <- owner_revision(engine, person, owner_key),
         item when not is_nil(item) <-
           Map.get(engine.pending_enrollments, code) || Map.get(engine.pending_history, code),
         true <- is_nil(Map.get(item, :person)) or item.person == person,
         true <- now_ms < item.status_expires_at do
      case item do
        %{state: :pending, expires_at: deadline} when now_ms < deadline ->
          {:ok,
           %{
             label: item.label,
             key: item.key,
             expires_at: deadline,
             can_grant_senders: is_nil(Map.get(item, :person))
           }}

        %{state: :approved, person: ^person, approved_by: ^owner_key} ->
          {:ok, %{label: item.label, key: item.key, expires_at: item.expires_at}}

        _ ->
          {:error, :not_found}
      end
    else
      _ -> {:error, :not_found}
    end
  end

  def pending_approve(
        %__MODULE__{} = engine,
        person,
        owner_key,
        code,
        expected_key,
        expected_expires_at,
        senders,
        now_ms
      ) do
    now_ms = max(now_ms, engine.pending_clock_high_water)

    with {:ok, _revision} <- owner_revision(engine, person, owner_key),
         {:ok, :expected} <-
           if(hex_key?(expected_key) and is_integer(expected_expires_at),
             do: {:ok, :expected},
             else: {:error, :invalid_expected_binding}
           ),
         true <-
           is_list(senders) and length(senders) <= 32 and
             Enum.all?(senders, fn sender ->
               Message.sender_id?(sender) and byte_size(sender) <= 256
             end),
         item when not is_nil(item) <-
           Map.get(engine.pending_enrollments, code) || Map.get(engine.pending_history, code) do
      cond do
        not is_nil(Map.get(item, :person)) and item.person != person ->
          {:error, :not_found}

        not is_nil(Map.get(item, :person)) and item.state == :pending and senders != [] ->
          {:error, :invalid_senders}

        item.key != expected_key or item.expires_at != expected_expires_at ->
          {:error, :not_found}

        now_ms >= item.status_expires_at and
            not (item.state == :approved and Map.get(item, :joined, false)) ->
          {:error, :not_found}

        item.state == :approved and get_in(engine.devices, [item.key, :revoked]) == true ->
          {:error, :revoked}

        item.state == :approved and item.person == person and
            item.approved_by == owner_key ->
          {:ok, engine, item.key}

        item.state == :approved ->
          {:error, :not_found}

        item.state == :expired ->
          {:error, :expired}

        item.state in [:declined, :cancelled] ->
          {:error, item.state}

        now_ms >= item.expires_at ->
          {:error, :expired}

        principal_key_used?(engine, item.key) ->
          {:error, :key_in_use}

        true ->
          with {:ok, enrolled} <- setup_device(engine, person, item.key),
               enrolled <- put_device_metadata(enrolled, item.key, item.label, now_ms),
               {:ok, granted} <- grant_pending_senders(enrolled, item.key, senders) do
            receipt = %{
              state: :approved,
              secret_hash: item.secret_hash,
              person: person,
              key: item.key,
              label: item.label,
              expires_at: item.expires_at,
              status_expires_at: item.status_expires_at,
              approved_by: owner_key,
              request_generation: item.request_generation,
              joined: Map.get(item, :joined, false)
            }

            {:ok,
             %{
               granted
               | pending_enrollments: Map.delete(granted.pending_enrollments, code),
                 pending_history: Map.put(granted.pending_history, code, receipt),
                 pending_clock_high_water: now_ms
             }, item.key}
          end
      end
    else
      {:error, :forbidden} -> {:error, :forbidden}
      {:error, :invalid_expected_binding} -> {:error, :invalid_expected_binding}
      false -> {:error, :invalid_senders}
      _ -> {:error, :not_found}
    end
  end

  defp find_pending_secret(engine, secret_hash) do
    Enum.find(engine.pending_enrollments, fn {_code, item} -> item.secret_hash == secret_hash end) ||
      Enum.find(engine.pending_history, fn {_code, item} -> item.secret_hash == secret_hash end)
  end

  @doc "Persist a crossed pending deadline before releasing an observation."
  def pending_observe_time(%__MODULE__{} = engine, {:secret, hash}, now_ms) do
    observe_pending_deadline(engine, find_pending_secret(engine, hash), now_ms)
  end

  def pending_observe_time(%__MODULE__{} = engine, {:code, code}, now_ms) do
    item = Map.get(engine.pending_enrollments, code) || Map.get(engine.pending_history, code)
    observe_pending_deadline(engine, if(item, do: {code, item}, else: nil), now_ms)
  end

  defp observe_pending_deadline(engine, {_, item}, now_ms) do
    crossed =
      cond do
        now_ms >= item.status_expires_at -> item.status_expires_at
        now_ms >= item.expires_at -> item.expires_at
        true -> engine.pending_clock_high_water
      end

    if crossed > engine.pending_clock_high_water,
      do: %{engine | pending_clock_high_water: crossed},
      else: engine
  end

  defp observe_pending_deadline(engine, _item, _now_ms), do: engine

  defp compact_pending(engine, now_ms) do
    retained_history =
      Map.reject(engine.pending_history, fn {_code, receipt} ->
        receipt.status_expires_at <= now_ms and
          not (receipt.state == :approved and Map.get(receipt, :joined, false))
      end)

    {active, history} =
      Enum.reduce(engine.pending_enrollments, {%{}, retained_history}, fn {code, item},
                                                                          {active, history} ->
        cond do
          item.state == :approved ->
            receipt = %{
              state: :approved,
              secret_hash: item.secret_hash,
              person: item.person,
              key: item.key,
              label: item.label,
              expires_at: item.expires_at,
              status_expires_at: item.status_expires_at,
              approved_by: Map.get(item, :approved_by),
              request_generation: Map.get(item, :request_generation, 1),
              joined: Map.get(item, :joined, false)
            }

            if now_ms < item.status_expires_at or receipt.joined,
              do: {active, Map.put(history, code, receipt)},
              else: {active, history}

          item.expires_at <= now_ms and is_nil(item.person) ->
            if now_ms < item.status_expires_at do
              receipt = %{
                state: :expired,
                secret_hash: item.secret_hash,
                key: item.key,
                expires_at: item.expires_at,
                status_expires_at: item.status_expires_at
              }

              {active, Map.put(history, code, receipt)}
            else
              {active, history}
            end

          true ->
            {Map.put(active, code, item), history}
        end
      end)

    %{engine | pending_enrollments: active, pending_history: history}
    |> AttentionServer.GuidedJoin.compact(now_ms)
  end

  def pending_decline(engine, person, owner_key, code, expected_key, expected_deadline, now) do
    now = max(now, engine.pending_clock_high_water)

    with {:ok, _} <- owner_revision(engine, person, owner_key),
         %{state: :pending, person: ^person} = item <- Map.get(engine.pending_enrollments, code),
         true <- item.key == expected_key and item.expires_at == expected_deadline do
      receipt = %{
        state: :declined,
        person: person,
        key: item.key,
        label: item.label,
        secret_hash: item.secret_hash,
        expires_at: item.expires_at,
        status_expires_at: item.status_expires_at,
        declined_by: owner_key,
        joined: Map.get(item, :joined, false)
      }

      {:ok,
       %{
         engine
         | pending_enrollments: Map.delete(engine.pending_enrollments, code),
           pending_history: Map.put(engine.pending_history, code, receipt),
           pending_clock_high_water: now
       }}
    else
      {:error, :forbidden} -> {:error, :forbidden}
      _ -> {:error, :not_found}
    end
  end

  @doc "Cancel only the exact secret-bound pending request; an already committed approval wins."
  def pending_cancel(engine, secret_hash, code, expected_key, expected_deadline, now) do
    now = max(now, engine.pending_clock_high_water)

    with {current_code, item} <- find_pending_secret(engine, secret_hash),
         true <- item.key == expected_key do
      cond do
        item.state == :pending and
            (code != current_code or expected_deadline != item.expires_at) ->
          {:error, :stale_request,
           pending_response(
             Map.put(item, :code, current_code),
             if(now >= item.expires_at, do: "expired", else: "pending")
           )}

        item.state != :pending ->
          cond do
            item.state == :approved and Map.get(item, :joined, false) and
                get_in(engine.devices, [item.key, :revoked]) == false ->
              {:ok, engine, pending_response(Map.put(item, :code, current_code), "approved")}

            item.state == :approved and Map.get(item, :joined, false) ->
              {:ok, engine, %{"state" => "revoked"}}

            item.state in [:cancelled, :declined] and Map.get(item, :joined, false) ->
              {:ok, engine, %{"state" => Atom.to_string(item.state)}}

            true ->
              with {:ok, status} <- pending_status(engine, secret_hash, now),
                   do: {:ok, engine, status}
          end

        true ->
          receipt =
            Map.take(item, [
              :person,
              :key,
              :label,
              :secret_hash,
              :expires_at,
              :status_expires_at,
              :joined
            ])

          receipt = Map.put(receipt, :state, :cancelled)

          next = %{
            engine
            | pending_enrollments: Map.delete(engine.pending_enrollments, code),
              pending_history: Map.put(engine.pending_history, code, receipt),
              pending_clock_high_water: now
          }

          {:ok, next, %{"state" => "cancelled"}}
      end
    else
      _ -> {:error, :not_found}
    end
  end

  defp put_device_metadata(engine, key, label, now) do
    device = Map.fetch!(engine.devices, key)

    %{
      engine
      | devices:
          Map.put(engine.devices, key, Map.merge(device, %{label: label, enrolled_at: now}))
    }
  end

  @doc "A proved TLS device key removes only itself; retained revoked keys support lost-reply retry."
  def device_remove(engine, verified_key) do
    with %{person: person} <- Map.get(engine.devices, verified_key),
         {:ok, next} <- revoke_device(engine, verified_key) do
      {:ok, next,
       %{
         "state" => "removed",
         "person_id" => person,
         "device_id" => verified_key,
         "enrollment_revision" => next.devices[verified_key].enrollment_revision
       }}
    else
      _ -> {:error, :not_found}
    end
  end

  defp grant_pending_senders(engine, _key, []), do: {:ok, engine}

  defp grant_pending_senders(engine, key, [sender | rest]) do
    with {:ok, next} <- setup_sender(engine, key, sender),
         do: grant_pending_senders(next, key, rest)
  end

  defp valid_code?(code),
    do: is_binary(code) and Regex.match?(~r/\A[A-Z2-7]{10}\z/, code)

  @doc "Owner-scoped device revocation; cross-person and absent keys are indistinguishable."
  def owner_revoke_device(%__MODULE__{} = engine, person, owner_key, device_key) do
    with {:ok, _revision} <- owner_revision(engine, person, owner_key),
         %{person: ^person} <- Map.get(engine.devices, device_key) do
      revoke_device(engine, device_key)
    else
      {:error, :forbidden} -> {:error, :forbidden}
      _ -> {:error, :not_found}
    end
  end

  def owner_grant_sender(%__MODULE__{} = engine, person, owner_key, device_key, pattern) do
    with {:ok, _revision} <- owner_revision(engine, person, owner_key),
         %{person: ^person, revoked: false} <- Map.get(engine.devices, device_key),
         true <- Message.sender_id?(pattern) and byte_size(pattern) <= 256 do
      setup_sender(engine, device_key, pattern)
    else
      {:error, :forbidden} -> {:error, :forbidden}
      false -> {:error, :invalid_sender}
      _ -> {:error, :not_found}
    end
  end

  def owner_revoke_sender(%__MODULE__{} = engine, person, owner_key, device_key, pattern) do
    with {:ok, _revision} <- owner_revision(engine, person, owner_key),
         %{person: ^person} <- Map.get(engine.devices, device_key),
         true <- Message.sender_id?(pattern) and byte_size(pattern) <= 256 do
      grants = Map.get(engine.sender_grants, {person, device_key}, MapSet.new())

      if MapSet.member?(grants, pattern) do
        {:ok,
         %{
           engine
           | sender_grants:
               Map.put(engine.sender_grants, {person, device_key}, MapSet.delete(grants, pattern))
         }}
      else
        {:error, :not_found}
      end
    else
      {:error, :forbidden} -> {:error, :forbidden}
      false -> {:error, :invalid_sender}
      _ -> {:error, :not_found}
    end
  end

  def owner_grant_enrollment_read(
        %__MODULE__{} = engine,
        person,
        owner_key,
        connector,
        grant_id,
        source,
        target
      ) do
    with {:ok, _revision} <- owner_revision(engine, person, owner_key) do
      case grant_enrollment_read(engine, person, connector, grant_id, source, target, owner_key) do
        {:ok, next} -> {:ok, next}
        {:error, :grant_conflict} -> {:error, :grant_conflict}
        _ -> {:error, :not_found}
      end
    end
  end

  def owner_revoke_enrollment_read(%__MODULE__{} = engine, person, owner_key, grant_id) do
    with {:ok, _revision} <- owner_revision(engine, person, owner_key),
         do: revoke_enrollment_read(engine, person, grant_id)
  end

  def setup_device(%__MODULE__{} = engine, person, key)
      when is_binary(key) and key != "" do
    cond do
      not Map.has_key?(engine.persons, person) ->
        {:error, :unknown_person}

      principal_key_used?(engine, key) ->
        {:error, :key_in_use}

      true ->
        {:ok,
         %{
           engine
           | devices:
               Map.put(engine.devices, key, %{
                 person: person,
                 revoked: false,
                 enrollment_revision: 1,
                 label: nil,
                 enrolled_at: nil
               }),
             subscriptions:
               Map.put(engine.subscriptions, {person, key}, %{revision: 0, senders: MapSet.new()}),
             cursors: Map.put(engine.cursors, {person, key}, 0)
         }}
    end
  end

  def setup_device(_, _, _), do: {:error, :invalid_device}

  @doc "Trusted local fixture principal for testing historical agent-winner projection only."
  def setup_agent_fixture(%__MODULE__{} = engine, person, id, key, sender_ids) do
    cond do
      not Map.has_key?(engine.persons, person) ->
        {:error, :unknown_person}

      not Message.uuid_v4?(id) ->
        {:error, :invalid_agent_id}

      not (is_binary(key) and Regex.match?(~r/\A[0-9a-f]{64}\z/, key)) ->
        {:error, :invalid_agent_key}

      principal_key_used?(engine, key) ->
        {:error, :key_in_use}

      Enum.any?(engine.agents, fn {_key, agent} -> agent.id == id end) ->
        {:error, :id_in_use}

      not (is_list(sender_ids) and sender_ids != [] and
               Enum.all?(sender_ids, &Message.sender_id?/1)) ->
        {:error, :invalid_senders}

      true ->
        agent = %{
          person: person,
          id: id,
          senders: MapSet.new(sender_ids),
          read_grants: MapSet.new(),
          respond_grants: MapSet.new(),
          grant_revision: 1,
          granted_by: nil,
          grant_history: [grant_snapshot(1, nil, MapSet.new(), MapSet.new(), false, "fixture")],
          revoked: false,
          enrollment_revision: 1
        }

        {:ok, %{engine | agents: Map.put(engine.agents, key, agent)}}
    end
  end

  def fixture_agent_answer(%__MODULE__{} = engine, key, frame) do
    case Map.get(engine.agents, key) do
      %{person: person, id: id, senders: senders, revoked: false} ->
        target = {person, frame["sender_id"], frame["message_id"]}

        cond do
          frame["type"] != "answer" ->
            failure("invalid_field", "type")

          not MapSet.member?(senders, frame["sender_id"]) ->
            failure("forbidden")

          frame["deliberate"] != true or not bounded_text?(frame["intent_ref"]) ->
            failure("invalid_field", "intent_ref")

          not (is_integer(frame["acceptance_seq"]) and frame["acceptance_seq"] > 0) ->
            failure("invalid_field", "acceptance_seq")

          get_in(engine.messages, [target, :accepted_seq]) != frame["acceptance_seq"] ->
            failure("stale_target")

          true ->
            terminal(engine, %{person: person, key: key, id: id, kind: :agent}, frame, true)
        end

      _ ->
        failure("forbidden")
    end
  end

  def setup_sender(%__MODULE__{} = engine, key, pattern) do
    with {:ok, %{person: person}} <- principal(engine, key),
         true <- Message.sender_id?(pattern) do
      grants =
        Map.update(
          engine.sender_grants,
          {person, key},
          MapSet.new([pattern]),
          &MapSet.put(&1, pattern)
        )

      {:ok, %{engine | sender_grants: grants}}
    else
      _ -> {:error, :forbidden}
    end
  end

  @doc "Classifies who started an uncommitted file group using the existing sender grant."
  def upload_source(%__MODULE__{} = engine, person, device, sender, message_id) do
    case Map.get(engine.messages, {person, sender, message_id}) do
      %{origin: ^device} ->
        :agent

      %{origin: _other} ->
        :client

      nil ->
        if sender_allowed?(engine, person, device, sender), do: :agent, else: :client
    end
  end

  def revoke_device(%__MODULE__{} = engine, key) do
    case Map.fetch(engine.devices, key) do
      {:ok, device} ->
        if device.revoked do
          {:ok, engine}
        else
          updated = %{device | revoked: true, enrollment_revision: device.enrollment_revision + 1}
          binding_key = {device.person, key}
          binding = Map.get(engine.push_bindings, binding_key)
          revision = if binding, do: binding.revision + 1, else: 1

          {:ok,
           %{
             engine
             | devices: Map.put(engine.devices, key, updated),
               push_bindings:
                 Map.put(engine.push_bindings, binding_key, canceled_push_binding(revision))
           }}
        end

      :error ->
        {:error, :unknown_device}
    end
  end

  @doc "Trusted local fixture revocation for a registered agent key; not an agent service."
  def revoke_agent_fixture(%__MODULE__{} = engine, key) do
    case Map.fetch(engine.agents, key) do
      {:ok, agent} ->
        if agent.revoked do
          {:ok, engine}
        else
          next_revision = agent.grant_revision + 1

          updated = %{
            agent
            | revoked: true,
              enrollment_revision: agent.enrollment_revision + 1,
              grant_revision: next_revision,
              grant_history:
                agent.grant_history ++
                  [
                    grant_snapshot(
                      next_revision,
                      agent.granted_by,
                      agent.read_grants,
                      agent.respond_grants,
                      true,
                      "fixture_revoke"
                    )
                  ]
          }

          {:ok, %{engine | agents: Map.put(engine.agents, key, updated)}}
        end

      :error ->
        {:error, :unknown_agent}
    end
  end

  @doc "Trusted owner-management fixture: one exact connector/source/target disclosure grant."
  def grant_enrollment_read(
        %__MODULE__{} = engine,
        person,
        connector,
        grant_id,
        source,
        target,
        issuer
      ) do
    proposed = %{
      person: person,
      connector: connector,
      source_instance_id: source,
      target: target,
      issuer: issuer,
      revoked: false
    }

    cond do
      not (Message.uuid_v4?(grant_id) and bounded_text?(source) and bounded_text?(issuer)) ->
        {:error, :invalid_grant}

      not valid_target?(target) ->
        {:error, :invalid_target}

      true ->
        case Map.get(engine.enrollment_read_grants, grant_id) do
          nil ->
            cond do
              not hex_key?(connector) or get_in(engine.devices, [connector, :person]) != person or
                  get_in(engine.devices, [connector, :revoked]) != false ->
                {:error, :invalid_connector}

              not match?({:ok, :active, _}, target_record(engine, person, target)) ->
                {:error, :invalid_target}

              true ->
                {:ok,
                 %{
                   engine
                   | enrollment_read_grants:
                       Map.put(engine.enrollment_read_grants, grant_id, proposed)
                 }}
            end

          ^proposed ->
            {:ok, engine}

          _ ->
            {:error, :grant_conflict}
        end
    end
  end

  @doc "Trusted owner-management fixture revocation; a revoked grant ID is never revived."
  def revoke_enrollment_read(%__MODULE__{} = engine, person, grant_id) do
    case Map.get(engine.enrollment_read_grants, grant_id) do
      %{person: ^person, revoked: false} = grant ->
        next = Map.put(engine.enrollment_read_grants, grant_id, %{grant | revoked: true})
        {:ok, %{engine | enrollment_read_grants: next}}

      %{person: ^person, revoked: true} ->
        {:ok, engine}

      _ ->
        {:error, :not_found}
    end
  end

  @doc "Trusted local fixture seam: install a recorded source-owner delegation for one origin."
  def grant_source_report(%__MODULE__{} = engine, person, key, source, senders, delegation_ref) do
    with {:ok, %{person: ^person}} <- principal(engine, key),
         true <- bounded_text?(source) and bounded_text?(delegation_ref),
         true <- is_list(senders) and senders != [] and Enum.all?(senders, &Message.sender_id?/1) do
      grant = %{senders: MapSet.new(senders), delegation_ref: delegation_ref, revoked: false}

      {:ok,
       %{
         engine
         | source_report_grants:
             Map.put(engine.source_report_grants, {person, key, source}, grant)
       }}
    else
      _ -> {:error, :forbidden}
    end
  end

  def revoke_source_report(%__MODULE__{} = engine, person, key, source) do
    case Map.get(engine.source_report_grants, {person, key, source}) do
      nil ->
        {:error, :not_found}

      grant ->
        {:ok,
         %{
           engine
           | source_report_grants:
               Map.put(engine.source_report_grants, {person, key, source}, %{
                 grant
                 | revoked: true
               })
         }}
    end
  end

  @doc """
  Handles one newline-terminated JSON request from a previously verified peer.
  Inactive/unknown keys are refused before parsing, as TLS admission would do.
  """
  def request_line(%__MODULE__{} = engine, key, raw) do
    with {:ok, principal} <- principal(engine, key) do
      {next, frame} =
        case JSON.decode_line(raw) do
          {:ok, value} ->
            handle_frame(engine, principal, value)

          {:error, :bad_frame} ->
            {engine, %{"v" => 1, "type" => "error", "reason" => "bad_frame"}}
        end

      {next, {:ok, JSON.encode!(frame) <> "\n"}}
    else
      _ -> {engine, {:error, :forbidden}}
    end
  end

  @doc "Projects retained events to one current device, without advancing a cursor."
  def events(%__MODULE__{} = engine, key, since) do
    with {:ok, %{person: person, key: device}} <- principal(engine, key),
         true <- is_integer(since) and since >= 0 and since <= engine.persons[person] do
      high = engine.persons[person]

      projected =
        if since == high do
          []
        else
          Enum.map((since + 1)..high, fn seq ->
            project_event(engine, person, device, seq, :current)
          end)
        end

      {:ok, projected, high}
    else
      {:error, :forbidden} -> {:error, :forbidden}
      false -> {:error, :invalid_cursor}
    end
  end

  @doc "Conversation facts have their own contiguous person sequence and device cursor."
  def conversation_events(%__MODULE__{} = engine, key, since) do
    with {:ok, %{person: person, key: device}} <- principal(engine, key),
         high <- Map.get(engine.conversation_highs, person, 0),
         true <- is_integer(since) and since >= 0 and since <= high do
      projected =
        if since == high do
          []
        else
          Enum.map((since + 1)..high, fn seq ->
            project_conversation_event(engine, person, device, seq)
          end)
        end

      {:ok, projected, high}
    else
      {:error, :forbidden} -> {:error, :forbidden}
      false -> {:error, :invalid_cursor}
    end
  end

  @doc false
  def inbox_high_water(%__MODULE__{} = engine, key, since) do
    with {:ok, %{person: person}} <- principal(engine, key),
         high when is_integer(high) <- Map.get(engine.persons, person),
         true <- is_integer(since) and since >= 0 and since <= high do
      {:ok, person, high}
    else
      {:error, :forbidden} -> {:error, :forbidden}
      false -> {:error, :invalid_cursor}
      _ -> {:error, :invalid_cursor}
    end
  end

  @doc false
  def inbox_snapshot(%__MODULE__{} = engine, key, since, view_order \\ []) do
    with {:ok, %{person: person, key: device}} <- principal(engine, key),
         true <- is_integer(since) and since >= 0 and since <= engine.persons[person] do
      high = engine.persons[person]
      view_ranks = view_order |> Enum.with_index() |> Map.new()

      items =
        engine.messages
        |> Enum.filter(fn {{message_person, _sender, _id}, record} ->
          message_person == person and record.accepted_seq <= high
        end)
        |> Enum.split_with(fn {_key, record} ->
          not match?(%{seq: seq} when seq <= high, record.terminal)
        end)
        |> then(fn {waiting, resolved} ->
          ordered_waiting =
            Enum.sort_by(waiting, fn {{_person, sender, id}, record} ->
              {Map.get(view_ranks, {sender, id}, map_size(view_ranks)), -record.accepted_seq,
               sender, id}
            end)

          ordered_resolved =
            Enum.sort_by(resolved, fn {{_person, sender, id}, record} ->
              {-record.accepted_seq, sender, id}
            end)

          ordered_waiting ++ ordered_resolved
        end)
        |> Enum.map(fn {{_person, sender, id}, _record} -> {sender, id} end)

      receipts =
        if since == high do
          []
        else
          Enum.filter((since + 1)..high, fn seq ->
            case Map.get(engine.events, {person, seq}) do
              %{kind: :lifecycle, key: message_key} ->
                case Map.get(engine.messages, message_key) do
                  %{origin: ^device} -> true
                  _ -> false
                end

              _ ->
                false
            end
          end)
        end

      operation_ids =
        engine.receipts
        |> Enum.flat_map(fn
          {{^person, ^device, op_id}, _receipt} -> [op_id]
          _ -> []
        end)
        |> Enum.sort()

      {:ok, items, receipts, operation_ids, high}
    else
      {:error, :forbidden} -> {:error, :forbidden}
      false -> {:error, :invalid_cursor}
    end
  end

  def conversation_cursor(%__MODULE__{} = engine, key) do
    with {:ok, %{person: person, key: device}} <- principal(engine, key) do
      {:ok, Map.get(engine.conversation_cursors, {person, device}, 0)}
    end
  end

  @doc "The account's read positions for conversation catch-up."
  def chat_read_positions(%__MODULE__{} = engine, key) do
    with {:ok, %{person: person}} <- principal(engine, key) do
      positions =
        engine.chat_read_positions
        |> Enum.flat_map(fn
          {{^person, sender, id}, seq} ->
            [
              %{
                "sender_id" => sender,
                "message_id" => id,
                "read_through_conversation_seq" => seq
              }
            ]

          _ ->
            []
        end)
        |> Enum.sort_by(&{&1["sender_id"], &1["message_id"]})

      {:ok, positions}
    end
  end

  def open_conversation(%__MODULE__{} = engine, key) do
    with {:ok, %{person: person, key: device}} <- principal(engine, key) do
      {:ok,
       %{engine | conversation_opened: MapSet.put(engine.conversation_opened, {person, device})}}
    end
  end

  def acknowledge_conversation(%__MODULE__{} = engine, key, seq, highest_sent) do
    with {:ok, %{person: person, key: device}} <- principal(engine, key) do
      current = Map.get(engine.conversation_cursors, {person, device}, 0)

      if is_integer(seq) and seq >= current and seq <= highest_sent do
        next = %{
          engine
          | conversation_cursors: Map.put(engine.conversation_cursors, {person, device}, seq)
        }

        {:ok, next, %{"conversation_seq" => seq}}
      else
        {:error, :invalid_cursor}
      end
    end
  end

  @doc "Authoritative origin feed, separate from the ordinary device relay cursor."
  def origin_feed(%__MODULE__{} = engine, key, since) do
    with {:ok, person} <- authorized_person(engine, key) do
      Conversation.fetch(engine, person, key, since)
    end
  end

  @doc "One delivered receipt for a person-authored actionable feed entry."
  def origin_ack(%__MODULE__{} = engine, key, feed_seq, reports \\ nil) do
    with {:ok, person} <- authorized_person(engine, key) do
      case Conversation.ack(engine, person, key, feed_seq, reports) do
        {:ok, next, %{"status" => "delivered"} = reply} ->
          entry = next.origin_feeds[{person, key}].entries[feed_seq]
          message_key = {person, entry["sender_id"], entry["message_id"]}

          payload = %{
            "kind" => "receipt",
            "author" => "origin",
            "event_id" => entry["event_id"],
            "request_revision" => entry["request_revision"],
            "session_handle" => entry["session_handle"],
            "delivered" => true
          }

          {next, _seq} = append_conversation_fact(next, person, message_key, payload)

          next =
            Map.get(reply, "file_transfers", %{})
            |> Enum.sort_by(fn {name, _outcome} -> name end)
            |> Enum.reduce(next, fn {name, outcome}, current ->
              fact = %{
                "kind" => "file_transfer",
                "author" => "origin",
                "event_id" => entry["event_id"],
                "request_revision" => entry["request_revision"],
                "session_handle" => entry["session_handle"],
                "file" => name,
                "status" => outcome["status"]
              }

              fact =
                if outcome["reason"], do: Map.put(fact, "reason", outcome["reason"]), else: fact

              {updated, _seq} = append_conversation_fact(current, person, message_key, fact)
              updated
            end)

          {:ok, next, reply}

        other ->
          other
      end
    end
  end

  @doc "One origin-session file-use report after a delivered person turn."
  def origin_file_use(%__MODULE__{} = engine, key, feed_seq, name, outcome, op_id) do
    with {:ok, person} <- authorized_person(engine, key),
         true <- Message.uuid_v4?(op_id),
         {:ok, next, reply} <- Conversation.file_use(engine, person, key, feed_seq, name, outcome) do
      if next == engine do
        {:ok, next, reply}
      else
        entry = next.origin_feeds[{person, key}].entries[feed_seq]
        message_key = {person, entry["sender_id"], entry["message_id"]}

        payload = %{
          "kind" => "file_use",
          "author" => "origin",
          "event_id" => entry["event_id"],
          "request_revision" => entry["request_revision"],
          "session_handle" => entry["session_handle"],
          "file" => name,
          "status" => outcome["status"],
          "op_id" => op_id
        }

        payload =
          if outcome["reason"], do: Map.put(payload, "reason", outcome["reason"]), else: payload

        {next, seq} = append_conversation_fact(next, person, message_key, payload)

        {:ok, next, Map.put(reply, "seq", seq)}
      end
    else
      false -> {:error, :invalid_op_id}
      error -> error
    end
  end

  @doc "Session status is origin-key scoped; a handle is a route, not proof of identity."
  def origin_status(%__MODULE__{} = engine, key, handle, listening?) do
    with {:ok, person} <- authorized_person(engine, key) do
      Conversation.status(engine, person, key, handle, listening?)
    end
  end

  @doc "Store the origin's explicit route/support statement for its own session handle."
  def origin_support(%__MODULE__{} = engine, key, frame) do
    with {:ok, person} <- authorized_person(engine, key),
         true <- Message.uuid_v4?(frame["op_id"]),
         true <- Message.uuid_v4?(frame["session_handle"]),
         {:ok, declared} when is_map(declared) <- Conversation.support_from_frame(frame) do
      semantic =
        %{"type" => "support", "session_handle" => frame["session_handle"], "support" => declared}

      case receipt(engine, person, key, frame["op_id"], semantic) do
        {:duplicate, reply} ->
          {:ok, engine, Map.put(reply, "duplicate", true)}

        :conflict ->
          {:error, :operation_conflict}

        :missing ->
          with {:ok, next, record} <-
                 Conversation.store_support(
                   engine,
                   person,
                   key,
                   frame["session_handle"],
                   declared
                 ) do
            reply = %{"session_handle" => frame["session_handle"], "listening_support" => record}
            {:ok, save_receipt(next, person, key, frame["op_id"], semantic, reply), reply}
          end
      end
    else
      false -> {:error, :invalid_field}
      {:ok, nil} -> {:error, :invalid_field}
      {:error, field} when is_binary(field) -> {:error, :invalid_field}
      error -> error
    end
  end

  @doc "Update only the current vendor attachment for an existing origin session handle."
  def origin_attachment_update(%__MODULE__{} = engine, key, frame) do
    with {:ok, person} <- authorized_person(engine, key),
         true <- Message.uuid_v4?(frame["op_id"]),
         true <- Map.has_key?(frame, "vendor_session_id"),
         true <-
           Conversation.valid_session?(frame["session_handle"], frame["vendor_session_id"]) do
      semantic = %{
        "type" => "attachment_update",
        "session_handle" => frame["session_handle"],
        "vendor_session_id" => frame["vendor_session_id"]
      }

      case receipt(engine, person, key, frame["op_id"], semantic) do
        {:duplicate, reply} ->
          {:ok, engine, reply}

        :conflict ->
          {:error, :operation_conflict}

        :missing ->
          with {:ok, next, stored} <-
                 Conversation.update_vendor_session_id(
                   engine,
                   person,
                   key,
                   frame["session_handle"],
                   frame["vendor_session_id"]
                 ) do
            reply = %{
              "session_handle" => frame["session_handle"],
              "vendor_session_id" => stored.vendor_session_id
            }

            {:ok, save_receipt(next, person, key, frame["op_id"], semantic, reply), reply}
          end
      end
    else
      false -> {:error, :invalid_field}
      error -> error
    end
  end

  def origin_session_gone(%__MODULE__{} = engine, key, handle) do
    with {:ok, person} <- authorized_person(engine, key) do
      Conversation.mark_gone(engine, person, key, handle)
    end
  end

  @doc "A durable same-person availability change for open requests on one origin handle."
  def origin_availability(%__MODULE__{} = engine, person, origin, handle, status, detached_at) do
    support = get_in(engine.origin_sessions, [{person, origin, handle}, :support])

    requests =
      engine.messages
      |> Enum.filter(fn {{request_person, _, _}, record} ->
        request_person == person and record.origin == origin and
          Map.get(record, :session_handle) == handle and is_nil(record.terminal)
      end)
      |> Enum.sort_by(fn {_, record} -> record.accepted_seq end)

    next =
      Enum.reduce(requests, engine, fn {message_key, record}, current ->
        {current, seq} = next_conversation_seq(current, person)

        payload = %{
          "kind" => "availability",
          "author" => "server",
          "session_handle" => handle,
          "availability" => status,
          "last_detached_at" => detached_at,
          "listening_support" => support,
          "unsupported_reason" =>
            if(status == "unsupported",
              do:
                if(support,
                  do: support["unsupported_reason"],
                  else: "no listening support recorded"
                ),
              else: nil
            ),
          "request_revision" => Map.get(record, :revision, 1)
        }

        put_conversation_event(current, person, seq, message_key, payload)
      end)

    if next == engine,
      do: next,
      else: queue_conversation_hint(next, person, next.conversation_highs[person])
  end

  @doc "Add the current origin-handle status to a newly accepted delivery event."
  def origin_delivery_availability(%__MODULE__{} = engine, person, seq, status, detached_at) do
    event = engine.events[{person, seq}]
    payload = %{"availability" => status, "last_detached_at" => detached_at}

    updated = Map.put(event, :payload, Map.merge(Map.get(event, :payload, %{}), payload))
    %{engine | events: Map.put(engine.events, {person, seq}, updated)}
  end

  @doc false
  def inbox_item(%__MODULE__{} = engine, key, {sender, id}, high) do
    with {:ok, %{person: person, key: device}} <- principal(engine, key),
         %{accepted_seq: accepted} = record when accepted <= high <-
           Map.get(engine.messages, {person, sender, id}) do
      # A frozen page may only show retraction state when its terminal was
      # included at the page's captured high water.
      acceptance = project_event(engine, person, device, accepted, :snapshot)

      acceptance =
        case Map.get(record, :terminal) do
          %{state: "retracted", seq: seq, at: at} when seq <= high ->
            if Map.has_key?(acceptance, "message") do
              Map.merge(acceptance, %{"state" => "retracted", "retracted_at" => at})
            else
              acceptance
            end

          _ ->
            acceptance
        end

      terminal =
        case Map.get(record, :terminal) do
          %{seq: seq} when seq <= high -> project_terminal(record, {person, sender, id}, device)
          _ -> nil
        end

      item = %{"acceptance" => acceptance}
      item = if terminal, do: Map.put(item, "terminal", terminal), else: item
      {:ok, item}
    else
      _ -> {:error, :snapshot_unavailable}
    end
  end

  @doc false
  def inbox_receipt(%__MODULE__{} = engine, key, seq) do
    with {:ok, %{person: person, key: device}} <- principal(engine, key),
         %{kind: :lifecycle, key: {^person, _sender, _id} = message_key} <-
           Map.get(engine.events, {person, seq}),
         %{origin: ^device} = record <- Map.get(engine.messages, message_key) do
      {:ok, project_terminal(record, message_key, device)}
    else
      _ -> {:error, :snapshot_unavailable}
    end
  end

  @doc false
  def reconcile_operation(%__MODULE__{} = engine, key, frame, high, committed_at_snapshot?)
      when is_map(frame) and is_boolean(committed_at_snapshot?) do
    with {:ok, %{person: person, key: device}} <- principal(engine, key),
         :ok <- valid_op(frame),
         {:ok, semantic} <- inbox_operation_semantic(engine, person, device, frame) do
      semantic_bytes = JSON.canonical(semantic)

      if not committed_at_snapshot? do
        {:ok, %{"op_id" => frame["op_id"], "status" => "not_committed_at_snapshot"}}
        |> add_uncommitted_terminal(engine, person, device, frame, high)
      else
        case Map.get(engine.receipts, {person, device, frame["op_id"]}) do
          nil ->
            {:error, :snapshot_unavailable}

          %{semantic: stored} = receipt ->
            if stored != semantic_bytes do
              {:ok, %{"op_id" => frame["op_id"], "status" => "operation_conflict"}}
            else
              case Map.get(receipt, :reply) do
                reply when is_map(reply) ->
                  if is_integer(Map.get(reply, "seq")) and reply["seq"] > high do
                    {:error, :snapshot_unavailable}
                  else
                    committed_result(engine, person, device, frame, reply, high)
                  end

                _ ->
                  {:error, :snapshot_unavailable}
              end
            end

          _ ->
            {:error, :snapshot_unavailable}
        end
      end
    else
      {:error, "op_id"} -> {:error, :invalid_field}
      {:error, :forbidden} -> {:error, :forbidden}
      {:error, reason} when is_atom(reason) -> {:error, reason}
      {:error, _field} -> {:error, :invalid_field}
      _ -> {:error, :invalid_field}
    end
  end

  @doc false
  def checkpoint_cursor(%__MODULE__{} = engine, key, high) do
    with {:ok, %{person: person}} <- principal(engine, key),
         true <- is_integer(high) and high >= 0 and high <= engine.persons[person] do
      {:ok, %{engine | cursors: Map.put(engine.cursors, {person, key}, high)}}
    else
      {:error, :forbidden} -> {:error, :forbidden}
      false -> {:error, :invalid_cursor}
    end
  end

  defp principal(engine, key) do
    case Map.get(engine.devices, key) do
      %{person: person, revoked: false} -> {:ok, %{person: person, key: key, kind: :device}}
      _ -> {:error, :forbidden}
    end
  end

  @doc "The current person for one enrolled, unrevoked device key."
  def authorized_person(%__MODULE__{} = engine, key) do
    case principal(engine, key) do
      {:ok, %{person: person}} -> {:ok, person}
      other -> other
    end
  end

  @doc "The device's durable acknowledged cursor, requiring current authority."
  def acknowledged_cursor(%__MODULE__{} = engine, key) do
    with {:ok, person} <- authorized_person(engine, key) do
      {:ok, Map.get(engine.cursors, {person, key}, 0)}
    end
  end

  @doc "Internal source-status projection; ordinary device wire has no item lookup."
  def source_status(%__MODULE__{} = engine, person, sender, id) do
    case Map.get(engine.messages, {person, sender, id}) do
      nil ->
        {:error, :not_found}

      record ->
        case {Map.get(record, :origin_result), Map.get(record, :source_result)} do
          {%{status: origin_status} = origin_result, source_result}
          when not is_nil(source_result) ->
            {:ok,
             %{
               "source_status" => "source_reported",
               "source_instance_id" => record.source_binding["source_instance_id"],
               "status" => source_result.status,
               "result_ref" => source_result.result_ref,
               "result_revision" => source_result.revision,
               "origin_result" => %{
                 "status" => origin_status,
                 "result_ref" => origin_result.result_ref,
                 "result_revision" => origin_result.revision
               }
             }}

          {%{status: status} = result, _} ->
            {:ok,
             %{
               "source_status" => "source_reported",
               "status" => status,
               "result_ref" => result.result_ref,
               "result_revision" => result.revision
             }}

          {nil, nil} ->
            {:ok, %{"source_status" => "unreported"}}

          {nil, result} ->
            {:ok,
             %{
               "source_status" => "source_reported",
               "source_instance_id" => record.source_binding["source_instance_id"],
               "status" => result.status,
               "result_ref" => result.result_ref,
               "result_revision" => result.revision
             }}
        end
    end
  end

  @doc "Accepts only a cursor actually sent contiguously on this session."
  def acknowledge(%__MODULE__{} = engine, key, seq, highest_sent) do
    with {:ok, person} <- authorized_person(engine, key) do
      current = Map.get(engine.cursors, {person, key}, 0)

      if is_integer(seq) and seq >= current and seq <= highest_sent do
        next = %{engine | cursors: Map.put(engine.cursors, {person, key}, seq)}
        {:ok, next, %{"seq" => seq}}
      else
        {:error, :invalid_cursor}
      end
    end
  end

  defp handle_frame(engine, _principal, value) when not is_map(value) or is_struct(value),
    do: {engine, %{"v" => 1, "type" => "error", "reason" => "bad_frame"}}

  # No comment stays payload-free and is bound to the revision displayed by the app.
  # Resolve its request against the authenticated person's retained messages.
  defp handle_frame(engine, principal, %{"type" => "answer_no_comment"} = frame) do
    expected = ["request_id", "request_revision", "type"]

    cond do
      not Map.has_key?(frame, "request_revision") ->
        {engine, error("invalid_field", nil, "request_revision")}

      Enum.sort(Map.keys(frame)) != expected ->
        {engine, error("invalid_field", nil)}

      not (is_integer(frame["request_revision"]) and frame["request_revision"] > 0) ->
        {engine, error("invalid_field", nil, "request_revision")}

      true ->
        case answer_no_comment(
               engine,
               principal,
               frame["request_id"],
               frame["request_revision"]
             ) do
          {:ok, next, fields} -> {next, Map.merge(base("ok", nil), fields)}
          {:error, reason, field, terminal} -> {engine, error(reason, nil, field, terminal)}
        end
    end
  end

  defp handle_frame(engine, principal, frame) do
    ref = Map.get(frame, "ref")

    cond do
      Map.has_key?(frame, "ref") and not is_binary(ref) ->
        {engine, error("invalid_field", nil, "ref")}

      frame["v"] != 1 or not is_integer(frame["v"]) ->
        {engine, error("unsupported_version", ref)}

      not is_binary(frame["type"]) ->
        {engine, error("invalid_field", ref, "type")}

      true ->
        case dispatch(engine, principal, frame) do
          {:ok, next, fields} -> {next, Map.merge(base("ok", ref), fields)}
          {:error, reason, field, terminal} -> {engine, error(reason, ref, field, terminal)}
        end
    end
  end

  defp dispatch(engine, principal, %{"type" => "forward"} = frame),
    do: forward(engine, principal, frame)

  defp dispatch(engine, principal, %{"type" => "discussion"} = frame),
    do: discussion(engine, principal, frame)

  defp dispatch(engine, principal, %{"type" => "conversation_read"} = frame),
    do: conversation_read(engine, principal, frame)

  defp dispatch(engine, principal, %{"type" => "conversation_reply"} = frame),
    do: conversation_reply(engine, principal, frame)

  defp dispatch(engine, principal, %{"type" => "conversation_revise"} = frame),
    do: conversation_revise(engine, principal, frame)

  defp dispatch(engine, principal, %{"type" => "conversation_withdraw"} = frame),
    do: conversation_withdraw(engine, principal, frame)

  defp dispatch(engine, principal, %{"type" => "conversation_result"} = frame),
    do: conversation_result(engine, principal, frame)

  defp dispatch(engine, principal, %{"type" => "answer"} = frame),
    do: terminal(engine, principal, frame, true)

  defp dispatch(engine, principal, %{"type" => "lifecycle"} = frame),
    do: terminal(engine, principal, frame, false)

  defp dispatch(engine, principal, %{"type" => "subscribe_remote"} = frame),
    do: subscribe_remote(engine, principal, frame)

  defp dispatch(engine, principal, %{"type" => "answer_receipt"} = frame),
    do: answer_receipt(engine, principal, frame)

  defp dispatch(engine, principal, %{"type" => "enrollment_readback"} = frame),
    do: enrollment_readback(engine, principal, frame)

  defp dispatch(engine, principal, %{"type" => "source_bind"} = frame),
    do: source_bind(engine, principal, frame)

  defp dispatch(engine, principal, %{"type" => "source_result"} = frame),
    do: source_result(engine, principal, frame)

  defp dispatch(engine, principal, %{"type" => "push_register"} = frame),
    do: push_register(engine, principal, frame)

  defp dispatch(engine, principal, %{"type" => "push_remove"} = frame),
    do: push_remove(engine, principal, frame)

  defp dispatch(engine, principal, %{"type" => "push_status"} = frame),
    do: push_status(engine, principal, frame)

  defp dispatch(_engine, _principal, %{"type" => kind}) when kind in @pending_types,
    do: failure("not_supported")

  defp dispatch(_engine, _principal, _frame), do: failure("unknown_type")

  defp enrollment_readback(engine, %{person: person, key: connector}, frame) do
    grant_id = frame["grant_id"]
    source = frame["source_instance_id"]
    target = frame["target"]

    cond do
      frame["proof_version"] != 1 ->
        failure("unsupported_version")

      valid_no_selectors(frame) != :ok ->
        failure("invalid_field", "principal")

      not Message.uuid_v4?(grant_id) ->
        failure("invalid_field", "grant_id")

      not bounded_text?(source) ->
        failure("invalid_field", "source_instance_id")

      not valid_target?(target) ->
        failure("invalid_field", "target")

      true ->
        case Map.get(engine.enrollment_read_grants, grant_id) do
          %{
            person: ^person,
            connector: ^connector,
            source_instance_id: ^source,
            target: ^target,
            revoked: false
          } ->
            case target_record(engine, person, target) do
              {:ok, state, revision} ->
                proof = %{
                  "proof_version" => 1,
                  "relay_instance_id" => engine.relay_instance_id,
                  "person_id" => person,
                  "source_instance_id" => source,
                  "grant_id" => grant_id,
                  "connector_device_id" => connector,
                  "target" => target,
                  "state" => Atom.to_string(state),
                  "enrollment_revision" => revision,
                  "observed_at" => DateTime.utc_now() |> DateTime.to_iso8601()
                }

                {:ok, engine, %{"proof" => proof}}

              _ ->
                failure("not_found")
            end

          _ ->
            failure("not_found")
        end
    end
  end

  defp valid_target?(target) when is_map(target) do
    Enum.sort(Map.keys(target)) == ~w(id key_fingerprint kind) and
      hex_key?(target["key_fingerprint"]) and
      case target["kind"] do
        "device" -> hex_key?(target["id"])
        "agent" -> Message.uuid_v4?(target["id"])
        _ -> false
      end
  end

  defp valid_target?(_), do: false

  defp hex_key?(value),
    do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{64}\z/, value)

  defp target_record(
         engine,
         person,
         %{"kind" => "device", "id" => id, "key_fingerprint" => key}
       ) do
    if id == key do
      case Map.get(engine.devices, id) do
        %{person: ^person, revoked: revoked, enrollment_revision: revision} ->
          {:ok, if(revoked, do: :revoked, else: :active), revision}

        _ ->
          :not_found
      end
    else
      :not_found
    end
  end

  defp target_record(engine, person, %{"kind" => "agent", "id" => id, "key_fingerprint" => key}) do
    case Map.get(engine.agents, key) do
      %{person: ^person, id: ^id, revoked: revoked, enrollment_revision: revision} ->
        {:ok, if(revoked, do: :revoked, else: :active), revision}

      _ ->
        :not_found
    end
  end

  defp answer_receipt(engine, %{person: person, key: device}, frame) do
    cond do
      frame["receipt_version"] != 1 ->
        failure("unsupported_version")

      valid_no_selectors(frame) != :ok ->
        failure("invalid_field", "principal")

      valid_global_id(frame) != :ok ->
        {:error, field} = valid_global_id(frame)
        failure("invalid_field", field)

      not (is_integer(frame["terminal_seq"]) and frame["terminal_seq"] > 0) ->
        failure("invalid_field", "terminal_seq")

      true ->
        key = {person, frame["sender_id"], frame["message_id"]}
        requested_seq = frame["terminal_seq"]

        case Map.get(engine.messages, key) do
          %{origin: ^device, terminal: nil} ->
            {:ok, engine, %{"status" => "pending"}}

          %{origin: ^device, terminal: %{seq: seq}} = record when seq == requested_seq ->
            {:ok, engine, %{"receipt" => terminal_receipt(engine, person, key, record)}}

          %{origin: ^device} ->
            failure("stale_target")

          _ ->
            failure("not_found")
        end
    end
  end

  defp terminal_receipt(engine, person, {_, sender, id}, record) do
    terminal = record.terminal

    receipt = %{
      "receipt_version" => 1,
      "relay_instance_id" => engine.relay_instance_id,
      "person_id" => person,
      "sender_id" => sender,
      "message_id" => id,
      "acceptance_seq" => record.accepted_seq,
      "terminal_seq" => terminal.seq,
      "terminal_kind" => terminal_kind(terminal),
      "state" => terminal.state,
      "at" => terminal.at,
      "has_answer" => terminal.answer_present,
      "actor" => %{
        "kind" => Atom.to_string(terminal.actor_kind),
        "id" => Map.get(terminal, :actor_id, terminal.actor),
        "key_fingerprint" => terminal.actor
      },
      "winning_op_id" => terminal.op_id
    }

    receipt =
      if terminal.state == "retracted",
        do: Map.put(receipt, "retracted_at", terminal.at),
        else: receipt

    if terminal.answer_present, do: Map.put(receipt, "answer", terminal.answer), else: receipt
  end

  defp terminal_kind(%{kind: "no_comment"}), do: "no_comment"
  defp terminal_kind(%{answer_present: true}), do: "answer"
  defp terminal_kind(_), do: "lifecycle"

  defp source_bind(engine, %{person: person, key: device}, frame) do
    with :ok <- valid_op(frame),
         :ok <- valid_global_id(frame),
         :ok <- valid_no_selectors(frame),
         :ok <- valid_source_binding_fields(frame),
         :ok <- report_allowed(engine, person, device, frame) do
      semantic =
        Map.take(
          frame,
          ~w(type sender_id message_id acceptance_seq source_instance_id request_id request_revision)
        )

      case receipt(engine, person, device, frame["op_id"], semantic) do
        {:duplicate, reply} -> {:ok, engine, reply}
        :conflict -> failure("operation_conflict")
        :missing -> commit_source_bind(engine, person, device, frame, semantic)
      end
    else
      {:error, :forbidden} -> failure("forbidden")
      {:error, field} -> failure("invalid_field", field)
    end
  end

  defp commit_source_bind(engine, person, device, frame, semantic) do
    key = {person, frame["sender_id"], frame["message_id"]}

    case Map.get(engine.messages, key) do
      %{origin: ^device, accepted_seq: accepted} = record ->
        if accepted != frame["acceptance_seq"] do
          failure("stale_target")
        else
          binding =
            Map.take(frame, ~w(source_instance_id request_id request_revision acceptance_seq))

          case {Map.get(record, :source_binding),
                source_tuple_bound_elsewhere?(engine, key, binding)} do
            {nil, false} ->
              reply = %{
                "source_instance_id" => frame["source_instance_id"],
                "acceptance_seq" => accepted
              }

              next =
                engine
                |> put_message(key, Map.put(record, :source_binding, binding))
                |> save_receipt(person, device, frame["op_id"], semantic, reply)

              {:ok, next, reply}

            {^binding, false} ->
              reply = %{
                "source_instance_id" => frame["source_instance_id"],
                "acceptance_seq" => accepted
              }

              {:ok, save_receipt(engine, person, device, frame["op_id"], semantic, reply), reply}

            _ ->
              failure("source_binding_conflict")
          end
        end

      _ ->
        failure("not_found")
    end
  end

  defp source_tuple_bound_elsewhere?(engine, target, binding) do
    tuple = Map.take(binding, ~w(source_instance_id request_id request_revision))

    Enum.any?(engine.messages, fn {key, record} ->
      key != target and
        match?(%{}, Map.get(record, :source_binding)) and
        Map.take(record.source_binding, ~w(source_instance_id request_id request_revision)) ==
          tuple
    end)
  end

  defp source_result(engine, %{person: person, key: device}, frame) do
    with :ok <- valid_op(frame),
         :ok <- valid_global_id(frame),
         :ok <- valid_no_selectors(frame),
         :ok <- valid_source_result_fields(frame),
         :ok <- report_allowed(engine, person, device, frame) do
      semantic =
        Map.take(
          frame,
          ~w(type sender_id message_id terminal_seq source_instance_id request_id request_revision result_revision status result_ref reason)
        )

      case receipt(engine, person, device, frame["op_id"], semantic) do
        {:duplicate, reply} -> {:ok, engine, reply}
        :conflict -> failure("operation_conflict")
        :missing -> commit_source_result(engine, person, device, frame, semantic)
      end
    else
      {:error, :forbidden} -> failure("forbidden")
      {:error, field} -> failure("invalid_field", field)
    end
  end

  defp commit_source_result(engine, person, device, frame, semantic) do
    key = {person, frame["sender_id"], frame["message_id"]}

    case Map.get(engine.messages, key) do
      %{origin: ^device, terminal: nil} ->
        failure("pending")

      %{origin: ^device, terminal: %{seq: seq}} = record ->
        binding = Map.get(record, :source_binding)

        cond do
          seq != frame["terminal_seq"] ->
            failure("stale_target")

          is_nil(binding) or
              Enum.any?(~w(source_instance_id request_id request_revision), fn field ->
                binding[field] != frame[field]
              end) ->
            failure("source_binding_conflict")

          true ->
            commit_source_result_revision(engine, person, device, key, record, frame, semantic)
        end

      _ ->
        failure("not_found")
    end
  end

  defp commit_source_result_revision(engine, person, device, key, record, frame, semantic) do
    current = Map.get(record, :source_result)
    revision = frame["result_revision"]

    proposed = %{
      status: frame["status"],
      result_ref: frame["result_ref"],
      reason: Map.get(frame, "reason"),
      terminal_seq: frame["terminal_seq"]
    }

    cond do
      current && revision <= current.revision ->
        failure("stale_result")

      current && current.status in ~w(applied refused) &&
          Map.take(current, [:status, :result_ref, :reason, :terminal_seq]) != proposed ->
        failure("receipt_conflict")

      current && current.status == "received" && frame["status"] == "received" &&
          Map.take(current, [:status, :result_ref, :reason, :terminal_seq]) != proposed ->
        failure("receipt_conflict")

      current && current.status in ~w(applied refused) ->
        reply = source_result_reply(current)
        {:ok, save_receipt(engine, person, device, frame["op_id"], semantic, reply), reply}

      true ->
        outcome = %{
          status: frame["status"],
          result_ref: frame["result_ref"],
          reason: Map.get(frame, "reason"),
          terminal_seq: frame["terminal_seq"],
          revision: revision
        }

        reply = source_result_reply(outcome)

        next =
          engine
          |> put_message(key, Map.put(record, :source_result, outcome))
          |> save_receipt(person, device, frame["op_id"], semantic, reply)

        {:ok, next, reply}
    end
  end

  defp source_result_reply(outcome),
    do: %{
      "source_status" => "source_reported",
      "status" => outcome.status,
      "result_revision" => outcome.revision,
      "result_ref" => outcome.result_ref
    }

  defp report_allowed(engine, person, device, frame) do
    source = frame["source_instance_id"]

    case Map.get(engine.source_report_grants, {person, device, source}) do
      %{revoked: false, senders: senders} ->
        if MapSet.member?(senders, frame["sender_id"]), do: :ok, else: {:error, :forbidden}

      _ ->
        {:error, :forbidden}
    end
  end

  defp valid_source_binding_fields(frame) do
    cond do
      not (is_integer(frame["acceptance_seq"]) and frame["acceptance_seq"] > 0) ->
        {:error, "acceptance_seq"}

      true ->
        Enum.find_value(~w(source_instance_id request_id request_revision), :ok, fn field ->
          if bounded_text?(frame[field]), do: false, else: {:error, field}
        end)
    end
  end

  @source_reasons ~w(ineligible_value unmapped_actor source_closed source_revoked stale_revision integrity_refusal source_unavailable other)
  defp valid_source_result_fields(frame) do
    cond do
      not (is_integer(frame["terminal_seq"]) and frame["terminal_seq"] > 0) ->
        {:error, "terminal_seq"}

      not (is_integer(frame["result_revision"]) and frame["result_revision"] > 0) ->
        {:error, "result_revision"}

      frame["status"] not in ~w(received applied refused) ->
        {:error, "status"}

      Map.has_key?(frame, "reason") and frame["reason"] not in @source_reasons ->
        {:error, "reason"}

      true ->
        Enum.find_value(
          ~w(source_instance_id request_id request_revision result_ref),
          :ok,
          fn field ->
            if bounded_text?(frame[field]), do: false, else: {:error, field}
          end
        )
    end
  end

  defp valid_no_selectors(frame) do
    if Enum.any?(
         ~w(person_id device_id agent_id principal_id origin actor),
         &Map.has_key?(frame, &1)
       ),
       do: {:error, "principal"},
       else: :ok
  end

  defp bounded_text?(value), do: is_binary(value) and byte_size(value) in 1..256

  defp subscribe_remote(engine, %{person: person, key: device}, frame) do
    with :ok <- valid_op(frame),
         :ok <- valid_revision(frame),
         :ok <- valid_sender_ids(frame) do
      revision = frame["revision"]
      senders = frame["sender_ids"]
      accepted = Map.get(engine.subscriptions, {person, device}, %{revision: 0})

      semantic = %{
        "type" => "subscribe_remote",
        "revision" => revision,
        "sender_ids" => Enum.sort(Enum.uniq(senders))
      }

      case receipt(engine, person, device, frame["op_id"], semantic) do
        {:duplicate, reply} ->
          {:ok, engine, reply}

        :conflict ->
          failure("operation_conflict")

        :missing when revision <= accepted.revision ->
          failure("stale_subscription")

        :missing ->
          next =
            engine
            |> Map.put(
              :subscriptions,
              Map.put(engine.subscriptions, {person, device}, %{
                revision: revision,
                senders: MapSet.new(senders)
              })
            )
            |> save_receipt(person, device, frame["op_id"], semantic, %{"revision" => revision})

          {:ok, next, %{"revision" => revision}}
      end
    else
      {:error, field} ->
        failure("invalid_field", field)
    end
  end

  defp valid_revision(%{"revision" => revision}) when is_integer(revision) and revision > 0,
    do: :ok

  defp valid_revision(_), do: {:error, "revision"}

  defp valid_sender_ids(%{"sender_ids" => senders}) when is_list(senders) do
    if Enum.all?(senders, &Message.sender_id?/1), do: :ok, else: {:error, "sender_ids"}
  end

  defp valid_sender_ids(_), do: {:error, "sender_ids"}

  defp forward(engine, %{person: person, key: device}, frame) do
    message = Map.get(frame, "message")

    with :ok <- valid_message(message),
         :ok <- valid_op(frame),
         :ok <- valid_conversation_send_fields(frame),
         {:ok, support} <- Conversation.support_from_frame(frame),
         {:ok, files} <- ConversationFiles.normalize(frame, engine, person, device),
         :ok <-
           if(files == [] or is_binary(frame["session_handle"]),
             do: :ok,
             else: {:error, "files"}
           ),
         true <- sender_allowed?(engine, person, device, message["sender_id"]) do
      semantic =
        %{"type" => "forward", "message" => message}
        |> Map.merge(Map.take(frame, ~w(session_handle vendor_session_id)))
        |> Map.put("files", files)
        |> then(fn value -> if support, do: Map.put(value, "support", support), else: value end)

      op = frame["op_id"]

      case receipt(engine, person, device, op, semantic) do
        {:duplicate, reply} ->
          {:ok, engine, reply}

        :conflict ->
          failure("operation_conflict")

        :missing ->
          commit_forward(engine, person, device, op, semantic, message, frame, files, support)
      end
    else
      {:error, field} -> failure("invalid_field", field)
      false -> failure("sender_not_authorized")
    end
  end

  defp commit_forward(engine, person, device, op, semantic, message, frame, files, support) do
    key = {person, message["sender_id"], message["message_id"]}

    case Map.get(engine.messages, key) do
      nil ->
        {next, seq} = next_seq(engine, person)

        record = %{
          message: message,
          origin: device,
          accepted_seq: seq,
          terminal: nil,
          source_binding: nil,
          source_result: nil,
          origin_result: nil,
          send_op_id: op,
          session_handle: Map.get(frame, "session_handle"),
          vendor_session_id: Map.get(frame, "vendor_session_id"),
          support: support,
          revision: 1,
          revisions: %{1 => message},
          files_by_revision: %{1 => files}
        }

        fields =
          if record.session_handle,
            do: %{"seq" => seq, "revision" => 1, "session_handle" => record.session_handle},
            else: %{"seq" => seq}

        next =
          next
          |> put_message(key, record)
          |> put_event(person, seq, :delivery, key)
          |> queue_push_hint(person, seq)
          |> save_receipt(person, device, op, semantic, fields)

        next =
          if record.session_handle do
            {next, feed_entry} = Conversation.on_send(next, person, device, record, key)
            event = next.events[{person, seq}]

            support =
              get_in(next.origin_sessions, [{person, device, record.session_handle}, :support])

            payload = %{
              "event_id" => feed_entry["event_id"],
              "listening_support" => support
            }

            %{
              next
              | events: Map.put(next.events, {person, seq}, Map.put(event, :payload, payload))
            }
          else
            next
          end

        {:ok, next, fields}

      record ->
        if Map.get(frame, "session_handle") != Map.get(record, :session_handle) or
             Map.get(frame, "vendor_session_id") != Map.get(record, :vendor_session_id) or
             (not is_nil(support) and Map.get(record, :support) != support) or
             (Map.has_key?(frame, "session_handle") and
                (record.message != message or
                   Map.get(record, :files_by_revision, %{})[1] != files)) do
          failure("request_conflict")
        else
          reply =
            if Map.get(record, :session_handle),
              do: %{
                "seq" => record.accepted_seq,
                "revision" => Map.get(record, :revision, 1),
                "session_handle" => record.session_handle,
                "duplicate" => true
              },
              else: %{"seq" => record.accepted_seq, "duplicate" => true}

          {:ok, save_receipt(engine, person, device, op, semantic, reply), reply}
        end
    end
  end

  defp terminal(engine, %{person: person, key: actor, kind: kind} = principal, frame, answer?) do
    with :ok <- valid_op(frame),
         :ok <- valid_global_id(frame),
         :ok <- valid_terminal_fields(frame, answer?) do
      semantic =
        terminal_semantic(frame, answer?)
        |> then(fn value ->
          if kind == :agent,
            do: Map.merge(value, Map.take(frame, ~w(acceptance_seq deliberate intent_ref))),
            else: value
        end)

      op = frame["op_id"]

      case receipt(engine, person, actor, op, semantic) do
        {:duplicate, reply} ->
          {:ok, engine, reply}

        :conflict ->
          failure("operation_conflict")

        :missing ->
          case valid_answer_revision(engine, person, frame, answer?) do
            :ok ->
              commit_terminal(
                engine,
                person,
                actor,
                Map.get(principal, :id, actor),
                kind,
                op,
                semantic,
                frame,
                answer?
              )

            {:error, current} ->
              failure("stale_revision", "request_revision", %{"current_revision" => current})
          end
      end
    else
      {:error, field} -> failure("invalid_field", field)
    end
  end

  defp commit_terminal(engine, person, actor, actor_id, kind, op, semantic, frame, answer?) do
    key = {person, frame["sender_id"], frame["message_id"]}

    case Map.get(engine.messages, key) do
      nil ->
        failure("unknown_message")

      %{origin: origin} when not answer? and origin != actor ->
        failure("not_origin")

      %{terminal: %{state: "retracted"} = terminal} when answer? ->
        failure("retracted", nil, retracted_terminal_snapshot(terminal))

      %{terminal: terminal} = record when not is_nil(terminal) ->
        failure("closed", nil, project_terminal(record, key, actor))

      record ->
        {next, seq} = next_seq(engine, person)

        terminal = %{
          state: if(answer?, do: "answered", else: frame["state"]),
          at: if(answer?, do: System.system_time(:millisecond), else: frame["at"]),
          by: if(answer?, do: "answer", else: Map.get(frame, "by")),
          answer_present: answer?,
          answer: if(answer?, do: frame["answer"], else: nil),
          actor: actor,
          actor_id: actor_id,
          actor_kind: kind,
          op_id: op,
          seq: seq
        }

        reply =
          %{"seq" => seq}
          |> then(fn fields ->
            if terminal.state == "retracted",
              do: Map.merge(fields, %{"state" => "retracted", "retracted_at" => terminal.at}),
              else: fields
          end)

        next =
          next
          |> put_message(key, %{record | terminal: terminal})
          |> put_event(person, seq, :lifecycle, key)
          |> then(fn state ->
            if answer? or is_binary(Map.get(record, :session_handle)),
              do: queue_push_hint(state, person, seq),
              else: state
          end)
          |> save_receipt(person, actor, op, semantic, reply)

        {next, terminal_entry} = Conversation.on_terminal(next, person, record, key, terminal)

        next =
          if terminal_entry do
            event = next.events[{person, seq}]

            updated =
              Map.put(event, :payload, %{
                "event_id" => terminal_entry["event_id"],
                "request_revision" => terminal_entry["request_revision"]
              })

            %{next | events: Map.put(next.events, {person, seq}, updated)}
          else
            next
          end

        next =
          if terminal_entry && terminal.state == "retracted" do
            {state, _conversation_seq} =
              append_conversation_fact(next, person, key, %{
                "kind" => "retraction",
                "author" => "origin",
                "event_id" => terminal_entry["event_id"],
                "request_revision" => terminal_entry["request_revision"],
                "session_handle" => record.session_handle,
                "terminal_seq" => seq,
                "terminal_state" => "retracted",
                "retracted_at" => terminal.at
              })

            state
          else
            next
          end

        {:ok, next, reply}
    end
  end

  defp answer_no_comment(
         engine,
         %{person: person, key: actor, kind: :device} = principal,
         request_id,
         request_revision
       ) do
    if Message.nonempty?(request_id) do
      matches =
        for {{^person, _sender, ^request_id} = key, record} <- engine.messages,
            do: {key, record}

      case matches do
        [{key, record}] ->
          case valid_no_comment_revision(engine, person, key, record, request_revision) do
            {:error, current} ->
              failure("stale_revision", "request_revision", %{"current_revision" => current})

            :ok ->
              cond do
                is_nil(record.terminal) ->
                  commit_no_comment(
                    engine,
                    person,
                    actor,
                    Map.get(principal, :id, actor),
                    key,
                    record
                  )

                Map.get(record.terminal, :kind) == "no_comment" ->
                  {:ok, engine,
                   %{
                     "seq" => record.terminal.seq,
                     "terminal_kind" => "no_comment",
                     "duplicate" => true
                   }}

                true ->
                  failure("closed", nil, project_terminal(record, key, actor))
              end
          end

        _ ->
          failure("not_found")
      end
    else
      failure("invalid_field", "request_id")
    end
  end

  defp answer_no_comment(_engine, _principal, _request_id, _request_revision),
    do: failure("forbidden")

  defp valid_no_comment_revision(engine, person, key, record, supplied) do
    if is_nil(record.terminal) do
      frame = %{
        "sender_id" => elem(key, 1),
        "message_id" => elem(key, 2),
        "request_revision" => supplied
      }

      case valid_answer_revision(engine, person, frame, true) do
        :ok ->
          current = Map.get(record, :revision, 1)
          if supplied == current, do: :ok, else: {:error, current}

        {:error, current} ->
          {:error, current}
      end
    else
      :ok
    end
  end

  defp commit_no_comment(engine, person, actor, actor_id, key, record) do
    {next, seq} = next_seq(engine, person)

    terminal = %{
      state: "answered",
      kind: "no_comment",
      at: System.system_time(:millisecond),
      by: "answer_no_comment",
      answer_present: false,
      answer: nil,
      actor: actor,
      actor_id: actor_id,
      actor_kind: :device,
      op_id: nil,
      seq: seq
    }

    next =
      next
      |> put_message(key, %{record | terminal: terminal})
      |> put_event(person, seq, :lifecycle, key)
      |> queue_push_hint(person, seq)

    {next, terminal_entry} = Conversation.on_terminal(next, person, record, key, terminal)

    next =
      if terminal_entry do
        event = next.events[{person, seq}]

        updated =
          Map.put(event, :payload, %{
            "event_id" => terminal_entry["event_id"],
            "request_revision" => terminal_entry["request_revision"]
          })

        %{next | events: Map.put(next.events, {person, seq}, updated)}
      else
        next
      end

    {:ok, next, %{"seq" => seq, "terminal_kind" => "no_comment"}}
  end

  defp valid_message(message) do
    case Message.validate(message) do
      :ok -> :ok
      error -> error
    end
  end

  defp valid_answer_revision(_engine, _person, _frame, false), do: :ok

  defp valid_answer_revision(engine, person, frame, true) do
    key = {person, frame["sender_id"], frame["message_id"]}

    case Map.get(engine.messages, key) do
      %{session_handle: handle, terminal: nil} = record when is_binary(handle) ->
        current = record.revision
        supplied = Map.get(frame, "request_revision")

        if (current == 1 and is_nil(supplied)) or supplied == current,
          do: :ok,
          else: {:error, current}

      _ ->
        :ok
    end
  end

  defp conversation_read(engine, %{person: person, key: actor}, frame) do
    with :ok <- valid_op(frame),
         :ok <- valid_global_id(frame) do
      semantic = Map.take(frame, ~w(type sender_id message_id read_through_conversation_seq))
      op = frame["op_id"]

      case receipt(engine, person, actor, op, semantic) do
        {:duplicate, reply} -> {:ok, engine, reply}
        :conflict -> failure("operation_conflict")
        :missing -> commit_conversation_read(engine, person, actor, frame, semantic)
      end
    else
      {:error, field} -> failure("invalid_field", field)
    end
  end

  defp commit_conversation_read(engine, person, actor, frame, semantic) do
    key = {person, frame["sender_id"], frame["message_id"]}
    position = frame["read_through_conversation_seq"]

    case Map.get(engine.messages, key) do
      nil ->
        failure("not_found")

      %{origin: ^actor} ->
        failure("not_person")

      _record ->
        case Map.get(engine.conversation_events, {person, position}) do
          %{key: ^key, payload: %{"kind" => "reply", "author" => "origin"}} ->
            current = Map.get(engine.chat_read_positions, key, 0)

            if position <= current do
              fields = %{"read_through_conversation_seq" => current}
              next = save_receipt(engine, person, actor, frame["op_id"], semantic, fields)
              {:ok, next, fields}
            else
              {next, seq} = next_conversation_seq(engine, person)
              # Origin-feed IDs are decimal strings; this prefix keeps read events distinct.
              event_id = "read:" <> Integer.to_string(seq)

              payload = %{
                "kind" => "read_position",
                "event_id" => event_id,
                "read_through_conversation_seq" => position
              }

              fields = %{
                "read_through_conversation_seq" => position,
                "seq" => seq,
                "event_id" => event_id
              }

              next =
                next
                |> then(fn state ->
                  %{
                    state
                    | chat_read_positions: Map.put(state.chat_read_positions, key, position)
                  }
                end)
                |> put_conversation_event(person, seq, key, payload)
                |> queue_conversation_hint(person, seq)
                |> save_receipt(person, actor, frame["op_id"], semantic, fields)

              {:ok, next, fields}
            end

          _ ->
            failure("invalid_field", "read_through_conversation_seq")
        end
    end
  end

  defp discussion(engine, %{person: person, key: actor}, frame) do
    with :ok <- valid_op(frame),
         :ok <- valid_global_id(frame),
         :ok <- valid_conversation_text(frame),
         {:ok, files} <- ConversationFiles.normalize(frame, engine, person, actor),
         true <- is_integer(frame["request_revision"]) and frame["request_revision"] >= 1 do
      semantic =
        frame
        |> Map.take(~w(type sender_id message_id request_revision text))
        |> Map.put("files", files)

      op = frame["op_id"]

      case receipt(engine, person, actor, op, semantic) do
        {:duplicate, reply} -> {:ok, engine, reply}
        :conflict -> failure("operation_conflict")
        :missing -> commit_discussion(engine, person, actor, frame, semantic, files)
      end
    else
      {:error, field} -> failure("invalid_field", field)
      false -> failure("invalid_field", "request_revision")
    end
  end

  defp commit_discussion(engine, person, actor, frame, semantic, files) do
    key = {person, frame["sender_id"], frame["message_id"]}

    case Map.get(engine.messages, key) do
      %{session_handle: handle, origin: origin} = record
      when is_binary(handle) and actor != origin ->
        if Conversation.chat_open?(record) do
          if frame["request_revision"] <= record.revision do
            {next, seq} = next_conversation_seq(engine, person)

            entry =
              %{
                "kind" => "discussion",
                "author" => "person",
                "sender_id" => frame["sender_id"],
                "message_id" => frame["message_id"],
                "session_handle" => handle,
                "request_revision" => frame["request_revision"],
                "op_id" => frame["op_id"],
                "text" => frame["text"]
              }
              |> ConversationFiles.put_entry(files)

            {next, feed_entry} = Conversation.append_entry(next, person, origin, entry)
            fields = %{"seq" => seq, "event_id" => feed_entry["event_id"]}

            next =
              next
              |> put_conversation_event(person, seq, key, feed_entry)
              |> queue_conversation_hint(person, seq)
              |> save_receipt(person, actor, frame["op_id"], semantic, fields)

            {:ok, next, fields}
          else
            failure("invalid_field", "request_revision")
          end
        else
          failure("closed")
        end

      %{terminal: terminal} when not is_nil(terminal) ->
        failure("closed")

      %{origin: ^actor} ->
        failure("not_person")

      _ ->
        failure("not_found")
    end
  end

  defp conversation_reply(engine, %{person: person, key: actor}, frame) do
    with :ok <- valid_op(frame),
         :ok <- valid_global_id(frame),
         :ok <- valid_conversation_text(frame),
         {:ok, files} <- ConversationFiles.normalize(frame, engine, person, actor),
         true <- is_integer(frame["in_reply_to"]) and frame["in_reply_to"] > 0,
         true <- is_integer(frame["request_revision"]) and frame["request_revision"] > 0 do
      semantic =
        frame
        |> Map.take(~w(type sender_id message_id in_reply_to request_revision text))
        |> Map.put("files", files)

      case receipt(engine, person, actor, frame["op_id"], semantic) do
        {:duplicate, reply} -> {:ok, engine, reply}
        :conflict -> failure("operation_conflict")
        :missing -> commit_conversation_reply(engine, person, actor, frame, semantic, files)
      end
    else
      {:error, field} -> failure("invalid_field", field)
      false -> failure("invalid_field", "in_reply_to")
    end
  end

  defp commit_conversation_reply(engine, person, actor, frame, semantic, files) do
    key = {person, frame["sender_id"], frame["message_id"]}

    case Map.get(engine.messages, key) do
      %{origin: origin} when origin != actor ->
        failure("not_origin")

      %{origin: ^actor, session_handle: handle} = record when is_binary(handle) ->
        if Conversation.chat_open?(record) do
          with {:ok, %{"kind" => "discussion", "request_revision" => revision} = acted_turn} <-
                 Conversation.person_entry(engine, person, actor, frame["in_reply_to"], key),
               true <- revision == frame["request_revision"],
               {:ok, acted} <-
                 Conversation.act(engine, person, actor, frame["in_reply_to"], frame["op_id"]) do
            {next, seq} = next_conversation_seq(acted, person)

            entry =
              %{
                "kind" => "reply",
                "author" => "origin",
                "sender_id" => frame["sender_id"],
                "message_id" => frame["message_id"],
                "session_handle" => handle,
                "request_revision" => revision,
                "in_reply_to" => Integer.to_string(frame["in_reply_to"]),
                "op_id" => frame["op_id"],
                "text" => frame["text"]
              }
              |> ConversationFiles.put_entry(files)

            {next, feed_entry} = Conversation.append_entry(next, person, actor, entry)
            fields = %{"seq" => seq, "event_id" => feed_entry["event_id"]}

            next =
              next
              |> put_conversation_event(person, seq, key, feed_entry)
              |> queue_conversation_hint(person, seq)
              |> save_receipt(person, actor, frame["op_id"], semantic, fields)

            {next, _acted_seq} =
              append_acted_fact(
                next,
                person,
                key,
                acted_turn["event_id"],
                feed_entry["event_id"],
                revision,
                handle
              )

            {:ok, next, fields}
          else
            false -> failure("stale_revision")
            {:error, :already_acted} -> failure("already_acted")
            _ -> failure("not_found")
          end
        else
          failure("closed")
        end

      %{origin: ^actor, terminal: terminal} when not is_nil(terminal) ->
        failure("closed")

      %{origin: _} ->
        failure("not_origin")

      _ ->
        failure("not_found")
    end
  end

  defp valid_conversation_text(%{"text" => text}) when is_binary(text) do
    if text != "" and String.valid?(text), do: :ok, else: {:error, "text"}
  end

  defp valid_conversation_text(_), do: {:error, "text"}

  defp conversation_revise(engine, %{person: person, key: actor}, frame) do
    with :ok <- valid_op(frame),
         :ok <- valid_global_id(frame),
         :ok <- valid_message(frame["message"]),
         {:ok, files} <- ConversationFiles.normalize(frame, engine, person, actor),
         true <- is_integer(frame["base_revision"]) and frame["base_revision"] > 0,
         true <-
           frame["message"]["sender_id"] == frame["sender_id"] and
             frame["message"]["message_id"] == frame["message_id"] do
      semantic =
        frame
        |> Map.take(~w(type sender_id message_id base_revision in_reply_to message))
        |> Map.put("files", files)

      case receipt(engine, person, actor, frame["op_id"], semantic) do
        {:duplicate, reply} -> {:ok, engine, reply}
        :conflict -> failure("operation_conflict")
        :missing -> commit_conversation_revise(engine, person, actor, frame, semantic, files)
      end
    else
      {:error, field} -> failure("invalid_field", field)
      false -> failure("invalid_field", "base_revision")
    end
  end

  defp commit_conversation_revise(engine, person, actor, frame, semantic, files) do
    key = {person, frame["sender_id"], frame["message_id"]}

    case Map.get(engine.messages, key) do
      %{origin: ^actor, session_handle: handle, terminal: nil} = record
      when is_binary(handle) ->
        if record.revision != frame["base_revision"] do
          failure("stale_revision", "base_revision", %{"current_revision" => record.revision})
        else
          with {:ok, acted} <- maybe_act_person_entry(engine, person, actor, key, frame) do
            {next, seq} = next_conversation_seq(acted, person)
            revision = record.revision + 1
            revisions = Map.get(record, :revisions, %{1 => record.message})

            updated =
              Map.merge(record, %{
                message: frame["message"],
                revision: revision,
                revisions: Map.put(revisions, revision, frame["message"]),
                files_by_revision:
                  Map.put(Map.get(record, :files_by_revision, %{}), revision, files)
              })

            entry =
              %{
                "kind" => "revision",
                "author" => "origin",
                "sender_id" => frame["sender_id"],
                "message_id" => frame["message_id"],
                "session_handle" => handle,
                "request_revision" => revision,
                "op_id" => frame["op_id"],
                "message" => frame["message"]
              }
              |> then(fn entry ->
                if is_integer(frame["in_reply_to"]),
                  do: Map.put(entry, "in_reply_to", Integer.to_string(frame["in_reply_to"])),
                  else: entry
              end)
              |> ConversationFiles.put_entry(files)

            {next, feed_entry} = Conversation.append_entry(next, person, actor, entry)
            fields = %{"seq" => seq, "event_id" => feed_entry["event_id"], "revision" => revision}

            next =
              next
              |> put_message(key, updated)
              |> put_conversation_event(person, seq, key, feed_entry)
              |> queue_conversation_hint(person, seq)
              |> save_receipt(person, actor, frame["op_id"], semantic, fields)

            next =
              if is_integer(frame["in_reply_to"]) do
                acted_turn = engine.origin_feeds[{person, actor}].entries[frame["in_reply_to"]]

                {state, _acted_seq} =
                  append_acted_fact(
                    next,
                    person,
                    key,
                    acted_turn["event_id"],
                    feed_entry["event_id"],
                    revision,
                    handle
                  )

                state
              else
                next
              end

            {:ok, next, fields}
          else
            {:error, reason} -> failure(Atom.to_string(reason))
          end
        end

      %{terminal: terminal} when not is_nil(terminal) ->
        failure("closed")

      %{origin: _} ->
        failure("not_origin")

      _ ->
        failure("not_found")
    end
  end

  defp maybe_act_person_entry(engine, _person, _origin, _key, %{"in_reply_to" => nil}),
    do: {:ok, engine}

  defp maybe_act_person_entry(engine, _person, _origin, _key, frame)
       when not is_map_key(frame, "in_reply_to"),
       do: {:ok, engine}

  defp maybe_act_person_entry(engine, person, origin, key, frame) do
    seq = frame["in_reply_to"]

    with true <- is_integer(seq) and seq > 0,
         {:ok, _entry} <- Conversation.person_entry(engine, person, origin, seq, key) do
      Conversation.act(engine, person, origin, seq, frame["op_id"])
    else
      _ -> {:error, :not_found}
    end
  end

  defp conversation_withdraw(engine, %{person: person, key: actor, kind: kind}, frame) do
    with :ok <- valid_op(frame),
         :ok <- valid_global_id(frame) do
      semantic = Map.take(frame, ~w(type sender_id message_id in_reply_to))

      case receipt(engine, person, actor, frame["op_id"], semantic) do
        {:duplicate, reply} -> {:ok, engine, reply}
        :conflict -> failure("operation_conflict")
        :missing -> commit_conversation_withdraw(engine, person, actor, kind, frame, semantic)
      end
    else
      {:error, field} -> failure("invalid_field", field)
    end
  end

  defp commit_conversation_withdraw(engine, person, actor, kind, frame, semantic) do
    key = {person, frame["sender_id"], frame["message_id"]}

    case Map.get(engine.messages, key) do
      %{origin: ^actor, session_handle: handle, terminal: nil}
      when is_binary(handle) ->
        with {:ok, acted} <- maybe_act_person_entry(engine, person, actor, key, frame) do
          terminal_frame =
            frame
            |> Map.put("state", "retracted")
            |> Map.put("at", System.system_time(:millisecond))
            |> Map.put("by", "origin_withdrawal")

          case commit_terminal(
                 acted,
                 person,
                 actor,
                 actor,
                 kind,
                 frame["op_id"],
                 semantic,
                 terminal_frame,
                 false
               ) do
            {:ok, next, fields} ->
              if is_integer(frame["in_reply_to"]) do
                acted_turn = engine.origin_feeds[{person, actor}].entries[frame["in_reply_to"]]
                feed = next.origin_feeds[{person, actor}]
                acting_turn = feed.entries[feed.high]

                {next, _acted_seq} =
                  append_acted_fact(
                    next,
                    person,
                    key,
                    acted_turn["event_id"],
                    acting_turn["event_id"],
                    next.messages[key].revision,
                    handle
                  )

                {:ok, next, fields}
              else
                {:ok, next, fields}
              end

            other ->
              other
          end
        else
          {:error, reason} -> failure(Atom.to_string(reason))
        end

      %{origin: ^actor, terminal: %{state: "retracted"} = terminal} ->
        failure("already_retracted", nil, retracted_terminal_snapshot(terminal))

      %{origin: origin} when origin != actor ->
        failure("not_origin")

      %{terminal: terminal} when not is_nil(terminal) ->
        failure("closed")

      %{origin: _} ->
        failure("not_origin")

      _ ->
        failure("not_found")
    end
  end

  defp conversation_result(engine, %{person: person, key: actor}, frame) do
    with :ok <- valid_op(frame),
         :ok <- valid_global_id(frame),
         true <- is_integer(frame["in_reply_to"]) and frame["in_reply_to"] > 0,
         true <- frame["status"] in ~w(received applied refused) do
      semantic = Map.take(frame, ~w(type sender_id message_id in_reply_to status))

      case receipt(engine, person, actor, frame["op_id"], semantic) do
        {:duplicate, reply} -> {:ok, engine, reply}
        :conflict -> failure("operation_conflict")
        :missing -> commit_conversation_result(engine, person, actor, frame, semantic)
      end
    else
      {:error, field} -> failure("invalid_field", field)
      false -> failure("invalid_field", "status")
    end
  end

  defp commit_conversation_result(engine, person, actor, frame, semantic) do
    key = {person, frame["sender_id"], frame["message_id"]}

    case Map.get(engine.messages, key) do
      %{origin: ^actor, session_handle: handle, terminal: %{state: "answered"} = terminal} =
          record
      when is_binary(handle) ->
        with {:ok, %{"kind" => "terminal", "terminal_state" => "answered"}} <-
               Conversation.person_entry(engine, person, actor, frame["in_reply_to"], key),
             true <-
               terminal.seq ==
                 terminal_entry_relay_seq(engine, person, actor, frame["in_reply_to"]) do
          prior = Map.get(record, :origin_result)

          cond do
            prior && prior.status in ~w(applied refused) ->
              failure("receipt_conflict")

            prior && prior.status == frame["status"] ->
              failure("receipt_conflict")

            prior && frame["status"] == "received" ->
              failure("receipt_conflict")

            true ->
              with {:ok, acted} <- act_answer_once(engine, person, actor, frame, prior) do
                {next, seq} = next_conversation_seq(acted, person)

                outcome = %{
                  status: frame["status"],
                  result_ref: frame["op_id"],
                  terminal_seq: terminal.seq,
                  revision: if(prior, do: prior.revision + 1, else: 1)
                }

                entry = %{
                  "kind" => "result",
                  "author" => "origin",
                  "sender_id" => frame["sender_id"],
                  "message_id" => frame["message_id"],
                  "session_handle" => handle,
                  "request_revision" => record.revision,
                  "in_reply_to" => Integer.to_string(frame["in_reply_to"]),
                  "op_id" => frame["op_id"],
                  "status" => frame["status"]
                }

                {next, feed_entry} = Conversation.append_entry(next, person, actor, entry)

                fields = %{
                  "seq" => seq,
                  "event_id" => feed_entry["event_id"],
                  "status" => frame["status"],
                  "result_revision" => outcome.revision
                }

                next =
                  next
                  |> put_message(key, Map.put(record, :origin_result, outcome))
                  |> put_conversation_event(person, seq, key, feed_entry)
                  |> queue_conversation_hint(person, seq)
                  |> save_receipt(person, actor, frame["op_id"], semantic, fields)

                next =
                  if is_nil(prior) do
                    acted_turn =
                      engine.origin_feeds[{person, actor}].entries[frame["in_reply_to"]]

                    {state, _acted_seq} =
                      append_acted_fact(
                        next,
                        person,
                        key,
                        acted_turn["event_id"],
                        feed_entry["event_id"],
                        record.revision,
                        handle
                      )

                    state
                  else
                    next
                  end

                {:ok, next, fields}
              else
                {:error, reason} -> failure(Atom.to_string(reason))
              end
          end
        else
          _ -> failure("not_found")
        end

      %{origin: ^actor, terminal: nil} ->
        failure("not_answered")

      %{origin: ^actor} ->
        failure("not_answered")

      %{origin: _} ->
        failure("not_origin")

      _ ->
        failure("not_found")
    end
  end

  defp act_answer_once(engine, _person, _origin, _frame, prior) when not is_nil(prior),
    do: {:ok, engine}

  defp act_answer_once(engine, person, origin, frame, nil),
    do: Conversation.act(engine, person, origin, frame["in_reply_to"], frame["op_id"])

  defp terminal_entry_relay_seq(engine, person, origin, feed_seq) do
    engine.origin_feeds[{person, origin}].entries[feed_seq]["terminal_seq"]
  end

  defp put_conversation_event(engine, person, seq, key, payload) do
    event = %{kind: :conversation, key: key, payload: payload}
    %{engine | conversation_events: Map.put(engine.conversation_events, {person, seq}, event)}
  end

  defp append_conversation_fact(engine, person, key, payload) do
    {next, seq} = next_conversation_seq(engine, person)

    next =
      next
      |> put_conversation_event(person, seq, key, payload)
      |> queue_conversation_hint(person, seq)

    {next, seq}
  end

  defp append_acted_fact(engine, person, key, acted_event_id, acting_event_id, revision, handle) do
    append_conversation_fact(engine, person, key, %{
      "kind" => "acted",
      "author" => "origin",
      "event_id" => acted_event_id,
      "acting_event_id" => acting_event_id,
      "request_revision" => revision,
      "session_handle" => handle
    })
  end

  defp valid_op(%{"op_id" => value}) do
    if Message.uuid_v4?(value), do: :ok, else: {:error, "op_id"}
  end

  defp valid_op(_), do: {:error, "op_id"}

  defp valid_conversation_send_fields(frame) do
    handle = Map.get(frame, "session_handle")
    vendor = Map.get(frame, "vendor_session_id")

    cond do
      is_nil(handle) and is_nil(vendor) and
          not Enum.any?(~w(route cli_version unsupported_reason), &Map.has_key?(frame, &1)) ->
        :ok

      is_nil(handle) ->
        {:error, "session_handle"}

      not Conversation.valid_session?(handle, vendor) ->
        {:error, "session_handle"}

      true ->
        :ok
    end
  end

  defp valid_global_id(frame) do
    cond do
      not Message.sender_id?(frame["sender_id"]) -> {:error, "sender_id"}
      not Message.nonempty?(frame["message_id"]) -> {:error, "message_id"}
      true -> :ok
    end
  end

  defp valid_terminal_fields(frame, true) do
    if Map.has_key?(frame, "answer"), do: :ok, else: {:error, "answer"}
  end

  defp valid_terminal_fields(frame, false) do
    cond do
      frame["state"] not in @terminal_states ->
        {:error, "state"}

      not is_integer(frame["at"]) ->
        {:error, "at"}

      Map.has_key?(frame, "by") and not (is_nil(frame["by"]) or is_binary(frame["by"])) ->
        {:error, "by"}

      true ->
        :ok
    end
  end

  defp terminal_semantic(frame, true) do
    %{
      "type" => "answer",
      "sender_id" => frame["sender_id"],
      "message_id" => frame["message_id"],
      "answer" => frame["answer"]
    }
    |> Map.merge(Map.take(frame, ~w(request_revision)))
  end

  defp terminal_semantic(frame, false) do
    base = %{
      "type" => "lifecycle",
      "sender_id" => frame["sender_id"],
      "message_id" => frame["message_id"],
      "state" => frame["state"],
      "at" => frame["at"]
    }

    if is_binary(Map.get(frame, "by")), do: Map.put(base, "by", frame["by"]), else: base
  end

  defp sender_allowed?(engine, person, device, sender) do
    engine.sender_grants
    |> Map.get({person, device}, MapSet.new())
    |> Enum.any?(fn pattern ->
      pattern == sender or
        (String.ends_with?(pattern, "/") and String.starts_with?(sender, pattern))
    end)
  end

  defp receipt(engine, person, device, op, semantic) do
    case Map.get(engine.receipts, {person, device, op}) do
      nil ->
        :missing

      %{semantic: stored, reply: reply} ->
        if stored == JSON.canonical(semantic),
          do: {:duplicate, Map.put(reply, "duplicate", true)},
          else: :conflict
    end
  end

  defp inbox_operation_semantic(
         engine,
         person,
         device,
         %{"type" => "forward", "message" => message} = frame
       )
       when is_map(message) do
    with :ok <- valid_no_selectors(frame),
         :ok <- valid_message(message),
         :ok <- valid_conversation_send_fields(frame),
         {:ok, support} <- Conversation.support_from_frame(frame),
         {:ok, files} <- ConversationFiles.normalize(frame, engine, person, device) do
      semantic =
        %{"type" => "forward", "message" => message}
        |> Map.merge(Map.take(frame, ~w(session_handle vendor_session_id)))
        |> Map.put("files", files)
        |> then(fn value -> if support, do: Map.put(value, "support", support), else: value end)

      {:ok, semantic}
    else
      {:error, field} -> {:error, field}
    end
  end

  defp inbox_operation_semantic(_engine, _person, _device, %{"type" => "answer"} = frame) do
    with :ok <- valid_no_selectors(frame),
         :ok <- valid_global_id(frame),
         :ok <- valid_terminal_fields(frame, true) do
      {:ok, terminal_semantic(frame, true)}
    else
      {:error, field} -> {:error, field}
    end
  end

  defp inbox_operation_semantic(_engine, _person, _device, %{"type" => "lifecycle"} = frame) do
    with :ok <- valid_no_selectors(frame),
         :ok <- valid_global_id(frame),
         :ok <- valid_terminal_fields(frame, false) do
      {:ok, terminal_semantic(frame, false)}
    else
      {:error, field} -> {:error, field}
    end
  end

  defp inbox_operation_semantic(_engine, _person, _device, %{
         "type" => "subscribe_remote",
         "revision" => revision,
         "sender_ids" => senders
       })
       when is_integer(revision) and is_list(senders),
       do: subscribe_semantic(revision, senders)

  defp inbox_operation_semantic(_engine, _person, _device, %{"type" => "push_register"} = frame),
    do: push_register_semantic(frame)

  defp inbox_operation_semantic(_engine, _person, _device, %{"type" => "push_remove"} = frame) do
    with :ok <- valid_no_selectors(frame),
         :ok <- valid_push_revision(frame) do
      {:ok, %{"type" => "push_remove", "revision" => frame["revision"]}}
    else
      {:error, field} -> {:error, field}
    end
  end

  defp inbox_operation_semantic(_engine, _person, _device, %{"type" => "source_bind"} = frame),
    do: source_semantic(frame, :bind)

  defp inbox_operation_semantic(_engine, _person, _device, %{"type" => "source_result"} = frame),
    do: source_semantic(frame, :result)

  defp inbox_operation_semantic(_engine, _person, _device, _), do: {:error, "type"}

  defp subscribe_semantic(revision, senders) do
    with :ok <- valid_revision(%{"revision" => revision}),
         :ok <- valid_sender_ids(%{"sender_ids" => senders}) do
      {:ok,
       %{
         "type" => "subscribe_remote",
         "revision" => revision,
         "sender_ids" => Enum.sort(Enum.uniq(senders))
       }}
    else
      {:error, field} -> {:error, field}
    end
  end

  defp push_register_semantic(frame) do
    revision = frame["revision"]
    address = frame["address"]

    with :ok <- valid_no_selectors(frame),
         :ok <- valid_push_revision(frame),
         :ok <- valid_push_address(frame),
         :ok <- valid_push_profile(frame) do
      {:ok,
       %{
         "type" => "push_register",
         "revision" => revision,
         "profile_id" => frame["profile_id"],
         "address_kind" => "apns_token",
         "address_sha256" =>
           Base.encode16(:crypto.hash(:sha256, String.downcase(address)), case: :lower)
       }}
    else
      {:error, field} -> {:error, field}
    end
  end

  defp source_semantic(frame, :bind) do
    with :ok <- valid_no_selectors(frame),
         :ok <- valid_global_id(frame),
         :ok <- valid_source_binding_fields(frame) do
      {:ok,
       Map.take(
         frame,
         ~w(type sender_id message_id acceptance_seq source_instance_id request_id request_revision)
       )}
    else
      {:error, field} -> {:error, field}
    end
  end

  defp source_semantic(frame, :result) do
    with :ok <- valid_no_selectors(frame),
         :ok <- valid_global_id(frame),
         :ok <- valid_source_result_fields(frame) do
      {:ok,
       Map.take(
         frame,
         ~w(type sender_id message_id terminal_seq source_instance_id request_id request_revision result_revision status result_ref reason)
       )}
    else
      {:error, field} -> {:error, field}
    end
  end

  defp committed_result(engine, person, device, frame, reply, high) do
    result =
      %{"v" => 1, "type" => "ok"}
      |> Map.merge(reply)
      |> Map.put("duplicate", true)

    entry = %{"op_id" => frame["op_id"], "status" => "committed", "result" => result}

    case frame["type"] do
      type when type in ["answer", "lifecycle"] ->
        with {:ok, terminal} <-
               exact_terminal_projection(engine, person, device, frame, reply["seq"], high) do
          {:ok, Map.put(entry, "terminal", terminal)}
        end

      "forward" ->
        case target_terminal_projection(engine, person, device, frame, high) do
          {:ok, nil} -> {:ok, entry}
          {:ok, terminal} -> {:ok, Map.put(entry, "terminal", terminal)}
          error -> error
        end

      _ ->
        {:ok, entry}
    end
  end

  defp exact_terminal_projection(engine, person, device, frame, seq, high) do
    case Map.get(engine.messages, {person, frame["sender_id"], frame["message_id"]}) do
      %{terminal: %{seq: ^seq}} = record when is_integer(seq) and seq <= high ->
        {:ok, project_terminal(record, {person, frame["sender_id"], frame["message_id"]}, device)}

      _ ->
        {:error, :snapshot_unavailable}
    end
  end

  defp target_terminal_projection(
         engine,
         person,
         device,
         %{"type" => "forward", "message" => message},
         high
       ) do
    target_terminal_projection(engine, person, device, message, high)
  end

  defp target_terminal_projection(
         engine,
         person,
         device,
         %{"sender_id" => sender, "message_id" => message_id},
         high
       ) do
    case Map.get(engine.messages, {person, sender, message_id}) do
      %{terminal: %{seq: seq}} = record when seq <= high ->
        {:ok, project_terminal(record, {person, sender, message_id}, device)}

      nil ->
        {:ok, nil}

      _ ->
        {:ok, nil}
    end
  end

  defp target_terminal_projection(_engine, _person, _device, _frame, _high), do: {:ok, nil}

  defp add_uncommitted_terminal({:ok, entry}, engine, person, device, frame, high) do
    case target_terminal_projection(engine, person, device, frame, high) do
      {:ok, nil} -> {:ok, entry}
      {:ok, terminal} -> {:ok, Map.put(entry, "terminal", terminal)}
      error -> error
    end
  end

  defp add_uncommitted_terminal(error, _engine, _person, _device, _frame, _high), do: error

  defp save_receipt(engine, person, device, op, semantic, reply) do
    item = %{semantic: JSON.canonical(semantic), reply: reply}
    %{engine | receipts: Map.put(engine.receipts, {person, device, op}, item)}
  end

  defp next_seq(engine, person) do
    seq = engine.persons[person] + 1
    {%{engine | persons: Map.put(engine.persons, person, seq)}, seq}
  end

  defp next_conversation_seq(engine, person) do
    seq = Map.get(engine.conversation_highs, person, 0) + 1
    {%{engine | conversation_highs: Map.put(engine.conversation_highs, person, seq)}, seq}
  end

  defp put_message(engine, key, record),
    do: %{engine | messages: Map.put(engine.messages, key, record)}

  defp put_event(engine, person, seq, kind, key),
    do: %{engine | events: Map.put(engine.events, {person, seq}, %{kind: kind, key: key})}

  defp queue_push_hint(engine, person, high) do
    now_ms = System.system_time(:millisecond)

    bindings =
      Map.new(engine.push_bindings, fn
        {{^person, _device} = key, %{active: true} = binding} ->
          if high > Map.get(engine.cursors, key, 0) do
            fresh =
              binding.pending_high == 0 or not is_integer(binding.deadline_at) or
                binding.deadline_at <= now_ms

            # The first unsent request opens a window; a request arriving while
            # the previous batch is still being sent opens the next one.
            opens_window? =
              fresh or
                (binding.state == "sending" and high > binding.pending_high and
                   is_nil(binding.next_at))

            deadline_at =
              if opens_window?, do: now_ms + @push_hint_lifetime_ms, else: binding.deadline_at

            next_at = if opens_window?, do: now_ms + @push_window_ms, else: binding.next_at

            state =
              cond do
                binding.state in ["sending", "blocked_configuration"] -> binding.state
                fresh -> "queued"
                true -> binding.state
              end

            {key,
             %{
               binding
               | pending_high: max(binding.pending_high, high),
                 deadline_at: deadline_at,
                 next_at: next_at,
                 state: state
             }}
          else
            {key, binding}
          end

        {key, binding} ->
          {key, binding}
      end)

    %{engine | push_bindings: bindings}
  end

  defp queue_conversation_hint(engine, person, high) do
    now_ms = System.system_time(:millisecond)

    bindings =
      Map.new(engine.push_bindings, fn
        {{^person, _device} = key, %{active: true} = binding} ->
          if MapSet.member?(engine.conversation_opened, key) and
               high > Map.get(engine.conversation_cursors, key, 0) do
            fresh =
              (binding.pending_high == 0 and binding.pending_conversation_high == 0) or
                not is_integer(binding.deadline_at) or binding.deadline_at <= now_ms

            opens_window? =
              fresh or
                (binding.state == "sending" and high > binding.pending_conversation_high and
                   is_nil(binding.next_at))

            deadline_at =
              if opens_window?, do: now_ms + @push_hint_lifetime_ms, else: binding.deadline_at

            next_at = if opens_window?, do: now_ms + @push_window_ms, else: binding.next_at

            state =
              cond do
                binding.state in ["sending", "blocked_configuration"] -> binding.state
                fresh -> "queued"
                true -> binding.state
              end

            {key,
             %{
               binding
               | pending_conversation_high: max(binding.pending_conversation_high, high),
                 deadline_at: deadline_at,
                 next_at: next_at,
                 state: state
             }}
          else
            {key, binding}
          end

        {key, binding} ->
          {key, binding}
      end)

    %{engine | push_bindings: bindings}
  end

  # Unresolved deliveries gathered since the last push this device accepted
  # (or synced), newest first. One push covers all of them.
  defp push_gathered_records(engine, person, device, binding) do
    floor = max(binding.accepted_high, Map.get(engine.cursors, {person, device}, 0))
    high = binding.pending_high

    if floor < high do
      Range.new(high, floor + 1, -1)
      |> Enum.flat_map(fn seq ->
        case Map.get(engine.events, {person, seq}) do
          %{kind: :delivery, key: key} ->
            case Map.get(engine.messages, key) do
              %{terminal: nil} = record -> [{key, record}]
              _ -> []
            end

          _ ->
            []
        end
      end)
      |> Enum.uniq_by(fn {key, _record} -> key end)
    else
      []
    end
  end

  # The recipient's Inbox count as the server sees it: accepted requests with
  # no explicit action yet. Retracted and answered items are resolved.
  defp inbox_badge(engine, person) do
    Enum.count(engine.messages, fn
      {{^person, _sender, _id}, %{terminal: nil}} -> true
      _ -> false
    end)
  end

  defp grouped_push_alert(gathered) do
    names =
      gathered
      |> Enum.reverse()
      |> Enum.map(fn {{_, sender, _}, record} ->
        case SenderNames.committed(record, sender) do
          %{"name" => name} -> name |> String.split() |> Enum.join(" ")
          nil -> sender
        end
      end)
      |> Enum.uniq()
      |> Enum.join(", ")

    alert = %{"title" => "#{length(gathered)} new requests"}

    {graphemes, remaining} = take_graphemes(names, 160, [])

    if remaining == "" and length(graphemes) <= 160 do
      Map.put(alert, "body", names)
    else
      alert
    end
  end

  defp push_alert({{_, sender, _}, record}) do
    message = record.message
    body = Map.get(message, "body")

    preview =
      if is_binary(body) and String.trim(body) != "" do
        body
      else
        message["title"]
      end

    normalized = preview |> String.split() |> Enum.join(" ")
    {graphemes, _remaining} = take_graphemes(normalized, 161, [])

    {bounded_preview, truncated?} =
      if length(graphemes) > 160 do
        {(Enum.take(graphemes, 159) ++ ["…"]) |> Enum.join(), true}
      else
        {Enum.join(graphemes), false}
      end

    %{"title" => sender, "body" => bounded_preview, "body_truncated" => truncated?}
  end

  defp push_locator({{_, sender, message_id}, record}, person, device, binding_id) do
    acceptance_seq = Map.get(record, :accepted_seq)

    if Message.uuid_v4?(binding_id) and
         Enum.all?([person, device, sender, message_id], &valid_locator_id?/1) and
         is_integer(acceptance_seq) and acceptance_seq > 0 do
      %{
        "v" => 1,
        "binding_id" => binding_id,
        "person_id" => person,
        "device_id" => device,
        "sender_id" => sender,
        "message_id" => message_id,
        "acceptance_seq" => acceptance_seq
      }
    end
  end

  defp valid_locator_id?(value),
    do: is_binary(value) and byte_size(value) > 0 and String.valid?(value)

  defp take_graphemes(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp take_graphemes(rest, remaining, acc) do
    case String.next_grapheme(rest) do
      nil -> {Enum.reverse(acc), ""}
      {grapheme, tail} -> take_graphemes(tail, remaining - 1, [grapheme | acc])
    end
  end

  defp project_event(engine, person, device, seq, projection) do
    %{kind: kind, key: key} = event = engine.events[{person, seq}]
    record = engine.messages[key]

    case kind do
      :delivery ->
        {_, sender, id} = key

        original_message =
          case Map.get(record, :revisions) do
            %{1 => message} -> message
            _ -> record.message
          end

        base = %{
          "v" => 1,
          "type" => "delivery",
          "seq" => seq,
          "sender_id" => sender,
          "message_id" => id,
          "origin" => record.origin
        }

        base =
          if is_binary(Map.get(record, :session_handle)) do
            base
            |> Map.put("request_revision", 1)
            |> Map.put("session_handle", record.session_handle)
            |> Map.merge(Map.get(event, :payload, %{}))
          else
            base
          end

        if device == record.origin do
          base
        else
          subscription = Map.get(engine.subscriptions, {person, device})

          mode =
            if subscription && MapSet.member?(subscription.senders, sender),
              do: "message",
              else: "knock"

          files = Map.get(record, :files_by_revision, %{}) |> Map.get(1, [])

          base
          |> Map.merge(%{"as" => mode, "message" => original_message})
          |> ConversationFiles.put_entry(files)
          |> project_delivery_retraction(record, projection)
        end

      :lifecycle ->
        Map.merge(project_terminal(record, key, device), Map.get(event, :payload, %{}))
    end
  end

  defp project_delivery_retraction(
         delivery,
         %{terminal: %{state: "retracted", at: at}},
         :current
       ) do
    Map.merge(delivery, %{"state" => "retracted", "retracted_at" => at})
  end

  defp project_delivery_retraction(delivery, _record, _projection), do: delivery

  defp project_conversation_event(engine, person, device, seq) do
    event = engine.conversation_events[{person, seq}]
    key = event.key
    record = engine.messages[key]
    {_, sender, id} = key

    base = %{
      "v" => 1,
      "type" => "conversation",
      "conversation_seq" => seq,
      "sender_id" => sender,
      "message_id" => id,
      "origin" => record.origin,
      "kind" => event.payload["kind"],
      "request_revision" => event.payload["request_revision"]
    }

    if event.payload["kind"] == "read_position" do
      base
      |> Map.delete("request_revision")
      |> Map.merge(Map.take(event.payload, ~w(event_id read_through_conversation_seq)))
    else
      if device == record.origin do
        Map.merge(
          base,
          Map.take(
            event.payload,
            ~w(author event_id acting_event_id in_reply_to availability last_detached_at session_handle listening_support unsupported_reason delivered acted file_transfers file status reason op_id terminal_seq terminal_state retracted_at)
          )
        )
      else
        Map.merge(base, Map.drop(event.payload, ~w(sender_id message_id feed_seq)))
      end
    end
  end

  defp project_terminal(record, {_, sender, id}, device) do
    terminal = record.terminal

    frame = %{
      "v" => 1,
      "type" => "lifecycle",
      "seq" => terminal.seq,
      "sender_id" => sender,
      "message_id" => id,
      "origin" => record.origin,
      # Installed clients recognize only the four original lifecycle states.
      # The canonical retraction stays distinct in the origin feed and receipt.
      "state" => if(terminal.state == "retracted", do: "cleared", else: terminal.state),
      "at" => terminal.at
    }

    frame =
      if terminal.state == "retracted",
        do:
          frame
          |> Map.put("terminal_reason", "retracted")
          |> Map.put("retracted_at", terminal.at),
        else: frame

    frame =
      if Map.get(terminal, :kind) == "no_comment",
        do: Map.put(frame, "terminal_kind", "no_comment"),
        else: frame

    frame =
      if is_binary(Map.get(record, :session_handle)),
        do: Map.put(frame, "author", Conversation.terminal_author(record, terminal)),
        else: frame

    frame = if is_binary(terminal.by), do: Map.put(frame, "by", terminal.by), else: frame

    frame =
      case {Map.get(terminal, :kind), terminal.by, terminal.answer_present, terminal.answer} do
        {"no_comment", _, _, _} ->
          Map.put(frame, "answer_outcome", %{"kind" => "no_comment"})

        {_, "answer", true, nil} ->
          Map.put(frame, "answer_outcome", %{"kind" => "seen"})

        {_, "answer", true, answer} when is_binary(answer) ->
          Map.put(frame, "answer_outcome", %{"kind" => "reply", "text" => answer})

        _ ->
          frame
      end

    if terminal.answer_present and device == record.origin,
      do: Map.put(frame, "answer", terminal.answer),
      else: frame
  end

  defp retracted_terminal_snapshot(%{state: "retracted", at: at, seq: seq}) do
    %{"state" => "retracted", "retracted_at" => at, "terminal_seq" => seq}
  end

  defp base(type, nil), do: %{"v" => 1, "type" => type}
  defp base(type, ref), do: %{"v" => 1, "type" => type, "ref" => ref}

  defp error(reason, ref, field \\ nil, terminal \\ nil) do
    base("error", ref)
    |> Map.put("reason", reason)
    |> then(fn frame -> if field, do: Map.put(frame, "field", field), else: frame end)
    |> then(fn frame ->
      case terminal do
        %{"current_revision" => _} -> Map.merge(frame, terminal)
        nil -> frame
        _ -> Map.put(frame, "terminal", terminal)
      end
    end)
  end

  defp failure(reason, field \\ nil, terminal \\ nil), do: {:error, reason, field, terminal}
end
