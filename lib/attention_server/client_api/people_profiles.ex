defmodule AttentionServer.ClientAPI.PeopleProfiles do
  @moduledoc false

  @behaviour AttentionServer.ClientAPI.Routes

  alias AttentionServer.{Engine, Store}
  alias AttentionServer.ClientAPI.{AgentWrites, Errors, Reads, SharedCapability}

  @impl true
  def routes, do: [{"PATCH", ["me", "profile"], :patch_profile}]

  def patch_profile(conn, ctx) do
    with {:ok, %{shared?: true} = capability} <- SharedCapability.parse(conn),
         {:ok, body, conn} <- AgentWrites.json_body(conn, []),
         result <- GenServer.call(ctx.relay, {:client_api, __MODULE__, {ctx.principal, body, capability}}, :infinity) do
      Reads.respond(conn, result)
    else
      {:ok, %{shared?: false}} -> Errors.send_error(conn, "unsupported")
      {:error, token, field, response} -> Errors.send_error(response, token, field: field)
      {:error, token, field} -> Errors.send_error(conn, token, field: field)
    end
  end

  @doc false
  def handle_call({who, body}, from, state), do: handle_call({who, body, %{}}, from, state)

  def handle_call({who, body, capability}, from, state) do
    engine = state.store.engine

    with {:ok, principal} <- Reads.client_principal(engine, who),
         true <- Reads.shared_available?(state, principal.person) and
                   SharedCapability.readable?(state, principal.person, capability) || {:error, :unsupported},
         {:ok, _} <- Engine.owner_revision(engine, principal.person, who.key),
         {:ok, profile} <- checked_profile(body),
         :ok <- available_handle(engine, principal.person, profile) do
      updated = Engine.put_row(engine, :people_profiles, principal.person, profile)
      audit = %{person: principal.person, actor: "profile_owner", action: "person_profile_update"}

      case Store.commit(state.store, updated, audit) do
        {:ok, store} -> Reads.handle_call({:me_profile, who}, from, %{state | store: store})
        _ -> {:reply, {:error, :storage_unavailable}, state}
      end
    else
      error -> {:reply, error, state}
    end
  end

  defp checked_profile(%{"display_name" => _, "handle" => _} = profile) when map_size(profile) == 2 do
    if valid?(profile), do: {:ok, profile}, else: {:error, :invalid_field, "profile"}
  end

  defp checked_profile(_), do: {:error, :invalid_field, "profile"}

  defp available_handle(engine, person, %{"handle" => handle}) do
    if Enum.any?(engine.people_profiles, fn {id, profile} ->
         id != person and profile["handle"] == handle
       end),
      do: {:error, :invalid_field, "handle"},
      else: :ok
  end

  def valid?(%{"display_name" => display, "handle" => handle})
      when is_binary(display) and is_binary(handle) do
    String.valid?(display) and String.trim(display) != "" and Regex.match?(~r/\A[a-z0-9_-]+\z/, handle)
  end

  def valid?(_), do: false

  def for_person(engine, person) do
    Map.get_lazy(engine.people_profiles, person, fn ->
      hosted_label = case Engine.hosted_account(engine, person) do
        %{label: label} -> label
        nil -> nil
      end
      %{"display_name" => hosted_label || "Harklight person", "handle" => nil}
    end)
  end

  def speaker(engine, person) do
    Map.merge(for_person(engine, person), %{"kind" => "person", "id" => person})
  end
end
