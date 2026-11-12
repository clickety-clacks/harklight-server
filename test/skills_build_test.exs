defmodule AttentionServer.SkillsBuildTest do
  use ExUnit.Case, async: true

  test "the package preserves the reviewed hosted guide independently of the shared core" do
    root = Path.expand("..", __DIR__)

    scratch =
      Path.join(System.tmp_dir!(), "harklight-skills-#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf!(scratch) end)
    File.mkdir_p!(Path.join(scratch, "scripts"))

    File.cp!(
      Path.join(root, "scripts/build-skills.sh"),
      Path.join(scratch, "scripts/build-skills.sh")
    )

    File.cp_r!(Path.join(root, "skills"), Path.join(scratch, "skills"))
    output = Path.join(scratch, "output")

    for core <- [File.read!(Path.join(root, "skills/core.md")), "# Updated shared concepts\n"] do
      File.write!(Path.join(scratch, "skills/core.md"), core)
      assert {_, 0} = System.cmd("bash", [Path.join(scratch, "scripts/build-skills.sh"), output])

      assert Enum.sort(Path.wildcard(Path.join(output, "**/*.md"))) ==
               Enum.sort([
                 Path.join(output, "core.md"),
                 Path.join(output, "harklight-agent/SKILL.md"),
                 Path.join(output, "harklight-agent-saas/SKILL.md")
               ])

      assert File.read!(Path.join(output, "core.md")) == core

      assembled = File.read!(Path.join(output, "harklight-agent/SKILL.md"))
      assert String.starts_with?(assembled, "---\nname: harklight-agent\n")
      assert String.ends_with?(assembled, core)
      assert length(:binary.matches(assembled, core)) == 1

      hosted = File.read!(Path.join(output, "harklight-agent-saas/SKILL.md"))
      assert Base.encode16(:crypto.hash(:sha256, hosted), case: :lower) ==
               "b0fd1c441198b3b302e680693c15781d9eebd3c636b236e782cd98c25decd5f9"

      opts =
        AttentionServer.DashboardReadHTTP.init(
          backend: __MODULE__,
          skill_path: Path.join(output, "harklight-agent/SKILL.md")
        )

      response =
        Plug.Test.conn(:get, "/agent-skills/harklight-agent-saas/SKILL.md")
        |> Map.put(:scheme, :https)
        |> AttentionServer.DashboardReadHTTP.call(opts)

      assert response.status == 200
      assert response.resp_body == hosted
      assert Plug.Conn.get_resp_header(response, "content-type") ==
               ["text/plain; charset=utf-8"]
    end
  end
end
