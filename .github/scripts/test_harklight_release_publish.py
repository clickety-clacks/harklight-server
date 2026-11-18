from __future__ import annotations

import importlib.util
import io
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock


HELPER_PATH = Path(__file__).with_name("harklight_release_publish.py")
HELPER_SPEC = importlib.util.spec_from_file_location("harklight_release_publish", HELPER_PATH)
assert HELPER_SPEC and HELPER_SPEC.loader
HELPER = importlib.util.module_from_spec(HELPER_SPEC)
HELPER_SPEC.loader.exec_module(HELPER)


class RehearsalRecordTests(unittest.TestCase):
    def test_prose_does_not_add_a_rehearsal_field(self):
        annotation = "Release notes\nrehearsal is not needed for this format\nrehearsal: not-required\n"
        form, _, _ = HELPER.parse_rehearsal_record(annotation)
        self.assertEqual(form, "not-required")

    def test_real_mix_version_record_round_trips_through_annotated_tag(self):
        # These are the three real lines from the published v0.1.42 BUILD_INFO.
        mix_version_lines = [
            "Erlang/OTP 27 [erts-15.2.7.13] [source] [64-bit] "
            "[smp:32:32] [ds:32:32:10] [async-threads:1] [jit:ns]",
            "",
            "Mix 1.19.5 (compiled with Erlang/OTP 27)",
        ]
        record_lines = [
            "rehearsal-archive-sha256: " + "a" * 64,
            "rehearsal-source-commit: " + "b" * 40,
            "rehearsal-build-script-sha256: " + "c" * 64,
            *[
                f"rehearsal-toolchain: {line}" if line else "rehearsal-toolchain:"
                for line in mix_version_lines
            ],
        ]
        annotation = "\n".join(record_lines)

        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            subprocess.run(["git", "init", "--quiet"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.name", "Test User"], cwd=repo, check=True)
            subprocess.run(
                ["git", "config", "user.email", "test@example.invalid"], cwd=repo, check=True
            )
            subprocess.run(["git", "config", "tag.gpgSign", "false"], cwd=repo, check=True)
            (repo / "README.md").write_text("test\n", encoding="utf-8")
            subprocess.run(["git", "add", "README.md"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "--quiet", "-m", "test"], cwd=repo, check=True)
            subprocess.run(
                ["git", "tag", "-a", "test-rehearsal-annotation", "-F", "-"],
                cwd=repo,
                input=annotation,
                text=True,
                check=True,
            )
            stored_annotation = subprocess.check_output(
                [
                    "git",
                    "for-each-ref",
                    "--format=%(contents)",
                    "refs/tags/test-rehearsal-annotation",
                ],
                cwd=repo,
                text=True,
            )

        self.assertIn("rehearsal-toolchain:\nrehearsal-toolchain: Mix 1.19.5", stored_annotation)
        form, fields, stored_toolchain_lines = HELPER.parse_rehearsal_record(stored_annotation)
        self.assertEqual(form, "recorded")
        self.assertEqual(fields["rehearsal-source-commit"], "b" * 40)
        self.assertEqual(stored_toolchain_lines, mix_version_lines)
        self.assertEqual(
            HELPER.toolchain_identity(stored_toolchain_lines, "stored annotation"),
            HELPER.toolchain_identity(mix_version_lines, "real Mix version output"),
        )


class ReleaseLookupTests(unittest.TestCase):
    def test_api_reads_use_workflow_token(self):
        with mock.patch.dict(os.environ, {"GH_TOKEN": "test-token"}):
            with mock.patch.object(HELPER.urllib.request, "urlopen", return_value=io.BytesIO(b"{}")) as urlopen:
                self.assertEqual(HELPER.request_json("https://api.github.com/example"), {})
        request = urlopen.call_args.args[0]
        self.assertEqual(request.get_header("Authorization"), "Bearer test-token")

    # GitHub's GET /releases/tags/{tag} returns only published releases, so the
    # workflow's own draft must be found in the authenticated release list.
    def test_published_release_from_tag_lookup_is_used(self):
        published = {"id": 1, "tag_name": "v1.2.3", "draft": False}
        self.assertIs(HELPER.select_release_for_tag("v1.2.3", published, []), published)

    def test_draft_is_found_in_release_list_when_tag_lookup_has_none(self):
        draft = {"id": 7, "tag_name": "v1.2.3", "draft": True}
        listed = [{"id": 6, "tag_name": "v1.2.2", "draft": False}, draft]
        self.assertIs(HELPER.select_release_for_tag("v1.2.3", None, listed), draft)

    def test_no_release_for_tag(self):
        listed = [{"id": 6, "tag_name": "v1.2.2", "draft": False}]
        self.assertIsNone(HELPER.select_release_for_tag("v1.2.3", None, listed))

    def test_two_releases_for_one_tag_are_refused(self):
        listed = [{"id": 7, "tag_name": "v1.2.3", "draft": True}, {"id": 8, "tag_name": "v1.2.3", "draft": True}]
        with self.assertRaises(SystemExit):
            HELPER.select_release_for_tag("v1.2.3", None, listed)


if __name__ == "__main__":
    unittest.main()
