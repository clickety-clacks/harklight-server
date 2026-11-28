defmodule AttentionServer.Repository.Postgres.Streams do
  @moduledoc false

  alias AttentionServer.Repository.Postgres.Transaction

  @max_page 500

  def person_page(person, cursor, limit), do: page({person, ""}, cursor, limit, nil, true)

  def agent_page({_, origin} = scope, cursor, limit, session, shared?)
      when is_binary(origin) and origin != "" and is_boolean(shared?) do
    page(scope, cursor, limit, session, shared?)
  end

  # Read the retained floor, head and rows in one statement snapshot. Two
  # statements at READ COMMITTED could see different sides of a retention
  # commit and turn an expired cursor into a silent gap.
  defp page({person, origin}, cursor, limit, session, shared?)
       when is_integer(cursor) and cursor >= 0 and is_integer(limit) and limit in 1..@max_page do
    case Transaction.query(
           """
           WITH bounds AS (
             SELECT coalesce(stream.head, 0) AS head, coalesce(stream.floor, 0) AS floor
             FROM (VALUES (1)) AS anchor(value)
             LEFT JOIN harklight.stream_heads AS stream
               ON stream.person_id = $1 AND stream.origin_key = $2
           )
           SELECT bounds.head, bounds.floor, page.revision, page.resource_kind,
                  page.resource_id, page.session_handle
           FROM bounds LEFT JOIN LATERAL (
             SELECT revision, resource_kind, resource_id, session_handle
             FROM harklight.stream_changes
             WHERE person_id = $1 AND origin_key = $2 AND revision > $3
               AND ($5::text IS NULL OR session_handle = $5)
               AND ($6 OR resource_id NOT LIKE 'shared-%')
             ORDER BY revision LIMIT $4
           ) AS page ON $3 BETWEEN bounds.floor AND bounds.head
           ORDER BY page.revision
           """,
           [person, origin, cursor, limit + 1, session, shared?]
         ) do
      {:ok, %{rows: [[head, floor | _] | _]}}
      when cursor < floor or cursor > head ->
        {:error, :cursor_expired}

      {:ok, %{rows: [[head, _ | _] | _] = rows}} ->
        {changes, rest} =
          rows
          |> Enum.flat_map(fn
            [_, _, nil, _, _, _] -> []
            [_, _, rev, kind, id, session] ->
              [%{rev: rev, kind: kind, resource_id: id, session: session}]
          end)
          |> Enum.split(limit)

        more = rest != []

        position =
          case {origin, more, List.last(changes)} do
            {"", _, nil} -> cursor
            {"", _, last} -> last.rev
            {_, true, last} -> last.rev
            {_, false, _} -> head
          end

        {:ok, changes, more, position}

      {:error, _} = error ->
        error
    end
  end

  def prune(scope, before_ms) when is_integer(before_ms) do
    {person, origin} = scope

    Transaction.run([scope], fn _ ->
      case Transaction.query(
             """
             WITH expired AS (
               DELETE FROM harklight.stream_changes
               WHERE person_id = $1 AND origin_key = $2 AND created_at_ms < $3
               RETURNING revision
             )
             UPDATE harklight.stream_heads
             SET floor = greatest(floor, (SELECT max(revision) FROM expired))
             WHERE person_id = $1 AND origin_key = $2
             RETURNING floor
             """,
             [person, origin, before_ms]
           ) do
        {:ok, %{rows: [[floor]]}} -> {:ok, floor}
        {:error, _} = error -> error
      end
    end)
  end
end
