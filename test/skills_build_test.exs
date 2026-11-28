defmodule AttentionServer.SkillsBuildTest do
  use ExUnit.Case, async: true

  test "the package builds shared-core skills and the Hosted guide follows the TestFlight setting" do
    root = Path.expand("..", __DIR__)

    scratch =
      Path.join(System.tmp_dir!(), "harklight-skills-#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf!(scratch) end)
    File.mkdir_p!(Path.join(scratch, "scripts"))

    File.cp!(
      Path.join(root, "scripts/build-skills.sh"),
      Path.join(scratch, "scripts/build-skills.sh")
    )

    File.cp!(Path.join(root, "mix.exs"), Path.join(scratch, "mix.exs"))
    File.cp_r!(Path.join(root, "skills"), Path.join(scratch, "skills"))
    output = Path.join(scratch, "output")

    for core <- [File.read!(Path.join(root, "skills/core.md")), "# Updated shared concepts\n"] do
      File.write!(Path.join(scratch, "skills/core.md"), core)
      assert {_, 0} = System.cmd("bash", [Path.join(scratch, "scripts/build-skills.sh"), output])

      assert Enum.sort(Path.wildcard(Path.join(output, "**/*.md"))) ==
               Enum.sort([
                 Path.join(output, "core.md"),
                 Path.join(output, "harklight/SKILL.md"),
                 Path.join(output, "harklight-agent/SKILL.md"),
                 Path.join(output, "harklight-agent-saas/SKILL.md")
               ])

      assert File.read!(Path.join(output, "core.md")) == core

      assembled = File.read!(Path.join(output, "harklight-agent/SKILL.md"))
      assert String.starts_with?(assembled, "---\nname: harklight-agent\n")
      assert String.ends_with?(assembled, core)
      assert length(:binary.matches(assembled, core)) == 1

      harklight = File.read!(Path.join(output, "harklight/SKILL.md"))
      server_version = Application.spec(:attention_server, :vsn)

      assert String.starts_with?(
               harklight,
               "---\nname: harklight\nversion: \"#{server_version}\"\n"
             )

      assert harklight =~ "description:"
      assert String.ends_with?(harklight, core)
      assert length(:binary.matches(harklight, core)) == 1

      hosted = File.read!(Path.join(output, "harklight-agent-saas/SKILL.md"))
      assert hosted == File.read!(Path.join(root, "skills/harklight-agent-saas/SKILL.md.in"))
      assert [before_app, after_app] = String.split(hosted, "@HOSTED_APP_INSTALL@")

      settings = [
        backend: __MODULE__,
        hosted: true,
        skill_path: Path.join(output, "harklight-agent/SKILL.md")
      ]

      assert_skill("/agent-skills/harklight/SKILL.md", settings, harklight)

      url =
        URI.to_string(%URI{
          scheme: "https",
          host: "testflight.apple.com",
          path: "/join/test-invitation",
          query: "source=guide&platform=iphone"
        })

      open =
        "Harklight for iPhone is available through Apple's TestFlight app. " <>
          "On your iPhone, open the [Harklight TestFlight invitation](<#{url}>) and follow TestFlight's prompts to install Harklight. " <>
          "If you don't have TestFlight yet, install it from the App Store first."

      for {testflight_settings, paragraph} <- [
            {[], ""},
            {[hosted_testflight_url: url], open},
            {[hosted_testflight_url: "https://example.com/join/test-invitation"], ""}
          ] do
        served_guide =
          assert_skill(
            "/agent-skills/harklight-agent-saas/SKILL.md",
            settings ++ testflight_settings,
            before_app <> paragraph <> after_app
          )

        refute served_guide =~ "public beta"

        if paragraph == "" do
          direction =
            "Use Web for this guide. The compatible iPhone app is not yet available to customers."

          assert length(:binary.matches(served_guide, direction)) == 1
          refute served_guide =~ "@HOSTED_APP_INSTALL@"
          refute served_guide =~ "/account/signup/"
          refute served_guide =~ "Request an invite"
          refute served_guide =~ "TestFlight"
          refute served_guide =~ "App Store"
        end
      end
    end
  end

  defp assert_skill(path, settings, expected) do
    opts = AttentionServer.DashboardReadHTTP.init(settings)

    response =
      Plug.Test.conn(:get, path)
      |> Map.put(:scheme, :https)
      |> AttentionServer.DashboardReadHTTP.call(opts)

    assert response.status == 200
    assert response.resp_body == expected
    assert Plug.Conn.get_resp_header(response, "content-type") == ["text/plain; charset=utf-8"]

    response.resp_body
  end
end
