defmodule AttentionServer.InstallReleaseRollbackTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{Engine, Store}
  alias Exqlite.Sqlite3

  setup do
    dir =
      Path.join(
        System.tmp_dir!(),
        "harklight-install-rollback-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    store_path = Path.join(dir, "relay.sqlite3")
    create_format_21_store!(store_path)
    assert store_format(store_path) == 21
    on_exit(fn -> File.rm_rf!(dir) end)

    %{
      store_path: store_path,
      store_sha256: sha256_file(store_path),
      build_lib: Path.join(Mix.Project.build_path(), "lib"),
      app_ebin: Path.join(Mix.Project.build_path(), "lib/attention_server/ebin"),
      migrator: Path.expand("support/open_store_and_report_format.exs", __DIR__)
    }
  end

  @tag timeout: 30_000
  test "failed candidate readiness restores the exact pre-migration store and prior release", context do
    {output, status} = run_installer("upgrade-failure", context)

    assert status == 0, output
    assert store_format(context.store_path) == 21
  end

  @tag timeout: 30_000
  test "an unavailable optional owner route does not block other server functionality", context do
    {output, status} = run_installer("optional-route", context)

    assert status == 0, output
    assert store_format(context.store_path) == 23
  end

  defp run_installer(mode, context) do
    script = Path.expand("install_release_rollback_test.sh", __DIR__)
    installer = Path.expand("../scripts/install-release.sh", __DIR__)

    env = [
      {"HARKLIGHT_TEST_SOURCE_STORE", context.store_path},
      {"HARKLIGHT_TEST_EXPECTED_STORE_SHA", context.store_sha256},
      {"HARKLIGHT_TEST_APP_EBIN", context.app_ebin},
      {"HARKLIGHT_TEST_ERL_LIBS", context.build_lib},
      {"HARKLIGHT_TEST_MIGRATOR", context.migrator}
    ]

    System.cmd(
      "bash",
      [
        script,
        installer,
        mode,
        context.store_path,
        context.store_sha256,
        context.app_ebin,
        context.build_lib,
        context.migrator
      ],
      env: env,
      stderr_to_stdout: true
    )
  end

  defp create_format_21_store!(path) do
    assert {:ok, initial} = Store.open(path)
    assert {:ok, engine} = Engine.setup_person(initial.engine, "person-a")
    assert :ok = Store.close(initial)

    pre_format_22_engine = Map.delete(engine, :hosted_accounts)
    pre_format_23_engine =
      pre_format_22_engine
      |> Map.delete(:algorithm_choices)
      |> Map.delete(:jev_endpoint)
      |> Map.delete(:jev_model)

    payload =
      :erlang.term_to_binary(
        {:attention_relay_state, 21, pre_format_23_engine},
        [{:compressed, 6}]
      )

    assert {:ok, db} = Sqlite3.open(path)
    assert {:ok, statement} =
             Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ? WHERE id = 1")

    assert :ok =
             Sqlite3.bind(statement, [
               {:blob, payload},
               {:blob, :crypto.hash(:sha256, payload)}
             ])

    assert :done = Sqlite3.step(db, statement)
    assert :ok = Sqlite3.release(db, statement)
    assert :ok = Sqlite3.close(db)
  end

  defp store_format(path) do
    assert {:ok, db} = Sqlite3.open(path)
    assert {:ok, statement} =
             Sqlite3.prepare(db, "SELECT payload, checksum FROM relay_state WHERE id = 1")

    assert {:row, [payload, checksum]} = Sqlite3.step(db, statement)
    assert :ok = Sqlite3.release(db, statement)
    assert :ok = Sqlite3.close(db)
    assert :crypto.hash(:sha256, payload) == checksum
    assert {:attention_relay_state, format, %Engine{}} = :erlang.binary_to_term(payload, [:safe])
    format
  end

  defp sha256_file(path) do
    path
    |> File.read!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end
