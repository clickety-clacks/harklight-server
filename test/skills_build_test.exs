defmodule AttentionServer.SkillsBuildTest do
  use ExUnit.Case, async: true

  test "the package assembles two self-contained skills and one shared core" do
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

      for skill <- ["harklight-agent", "harklight-agent-saas"] do
        assembled = File.read!(Path.join(output, "#{skill}/SKILL.md"))
        assert String.starts_with?(assembled, "---\nname: #{skill}\n")
        assert String.ends_with?(assembled, core)
        assert length(:binary.matches(assembled, core)) == 1
      end
    end
  end
end
