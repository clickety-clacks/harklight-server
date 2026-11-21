defmodule AttentionServer.ClientAPI.Focus do
  @moduledoc """
  Focus and held shows for the client API v2 (contract rulings round 11 r3;
  focus spec r6 binds unchanged, only the transport changes).

  - `PUT /v2/devices/me/focus` carries today's focus report. Its `ttl_s`
    replaces the session: a report keeps the device connected for `ttl_s`, and
    a lapse with no renewal ends its focus as today's disconnect does. A
    renewal with unchanged fields is liveness only.
  - A held show stays in relay memory, as today, and is never a change-log
    row. It reaches the device in the response to its next focus write and,
    if the device holds an open feed, as an ephemeral `show` event. Both carry
    the same `show_id`, so the client applies it once.

  Shows are `{show_id, device_id, item_id, target, held_ms}`, where `held_ms`
  is the show's age when it is written.

  The feed (C5) calls `subscribe_shows/1` for a device principal and then
  receives `{:client_api_show, device_key, held}` for each show held for it.
  It writes `wire(held, now)` as an id-less `event: show`, so `held_ms` is the
  show's age at the moment it is written (round 11 r4 §4).
  """

  alias AttentionServer.ClientAPI.{Resources, Writes}

  @registry AttentionServer.ChangeLog.Registry

  @doc "Subscribes the calling feed process to shows held for `device_key`."
  @spec subscribe_shows(String.t()) :: :ok
  def subscribe_shows(device_key) when is_binary(device_key) do
    case Registry.register(@registry, {:client_api_show, device_key}, nil) do
      {:ok, _} -> :ok
      {:error, {:already_registered, _}} -> :ok
    end
  end

  @doc false
  # Called by the Relay when it holds a show; sends it to the device's open
  # feeds. With no feed open, or the registry not running, it does nothing.
  def held(engine, device_key, held, _now) do
    if Process.whereis(@registry) do
      Registry.dispatch(@registry, {:client_api_show, device_key}, fn entries ->
        case show(engine, device_key, held, Map.fetch!(held, "accepted_at")) do
          nil ->
            :ok

          show ->
            message = %{show: Map.delete(show, "held_ms"), accepted_at: held["accepted_at"]}
            for {pid, _} <- entries, do: send(pid, {:client_api_show, device_key, message})
        end
      end)
    end

    :ok
  end

  @doc """
  The `show` event data for a held show received by a feed, with `held_ms`
  computed at `now`, the moment the feed writes it.
  """
  def wire(%{show: show, accepted_at: accepted_at}, now) when is_integer(now),
    do: Map.put(show, "held_ms", max(now - accepted_at, 0))

  @doc "The wire form of a held show for `device_key`, or nil if its item is gone."
  def show(engine, device_key, %{"accepted_at" => accepted_at} = held, now) do
    with %{person: person} <- Map.get(engine.devices, device_key),
         {:ok, item_id} <-
           Resources.item_id(engine, {person, held["sender_id"], held["message_id"]}) do
      %{
        "show_id" => show_id(device_key, held),
        "device_id" => device_key,
        "item_id" => item_id,
        "target" => held["target"],
        "held_ms" => max(now - accepted_at, 0)
      }
    else
      _ -> nil
    end
  end

  # Stable for one held show, so both deliveries carry the same id.
  defp show_id(device_key, held) do
    material =
      Enum.join(
        [
          device_key,
          held["sender_id"],
          held["message_id"],
          held["target"],
          held["accepted_at"]
        ],
        <<0>>
      )

    "s" <> Base.url_encode64(binary_part(:crypto.hash(:sha256, material), 0, 18), padding: false)
  end

  @doc false
  # Schedules the lapse of a focus report and returns its token: after
  # `ttl_ms` with no newer report, the device's focus ends. The token carries
  # the timer, so a renewal cancels it (cancel_lapse/1) and only one timer per
  # device is live; a stale timer would do nothing anyway (the token differs).
  def schedule_lapse(relay, device_key, ttl_ms) do
    id = make_ref()

    {:ok, timer} =
      :timer.apply_after(ttl_ms, __MODULE__, :lapse, [relay, device_key, {:lapse, id}])

    {:lapse, id, timer}
  end

  @doc false
  def cancel_lapse({:lapse, _id, timer}) do
    _ = :timer.cancel(timer)
    :ok
  end

  def cancel_lapse(_none), do: :ok

  @doc false
  def lapse(relay, device_key, {:lapse, id}) do
    GenServer.call(relay, {:client_api, Writes, {:focus_lapse, device_key, id}}, 30_000)
  catch
    :exit, _ -> :ok
  end
end
