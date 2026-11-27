import copy
import importlib.util
from pathlib import Path
import unittest

script = Path(__file__).resolve().parents[2] / "scripts/history-continuity/verify.py"
spec = importlib.util.spec_from_file_location("history_verify", script)
verify = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify)


class ReleaseHistoryGateTest(unittest.TestCase):
    def setUp(self):
        self.version = "0.2.34"
        self.commit = "a" * 40
        self.pins = dict(server_version=self.version, server_commit=self.commit, fixture_sha256="b" * 64)
        self.proof = {
            "manifest": dict(self.pins, expected=copy.deepcopy(verify.EXPECTED),
                             upgrade=["0.1.49", "0.2.16", self.version]),
            "results": [
                dict(self.pins, platform=platform, client_build="test-build", relaunch="pass",
                     outcome="pass", observations=copy.deepcopy(verify.EXPECTED))
                for platform in ("iPhone", "iPad", "Web")
            ],
        }

    def test_requires_three_matching_connected_results(self):
        verify.release_gate(self.proof, self.version, self.commit)
        self.proof["results"].pop()
        with self.assertRaisesRegex(ValueError, "all required"):
            verify.release_gate(self.proof, self.version, self.commit)

    def test_rejects_server_commit_mismatch(self):
        with self.assertRaisesRegex(ValueError, "commit differs"):
            verify.release_gate(self.proof, self.version, "c" * 40)

    def test_rejects_different_client_fixture(self):
        self.proof["results"][1]["fixture_sha256"] = "d" * 64
        with self.assertRaisesRegex(ValueError, "iPad: fixture_sha256"):
            verify.release_gate(self.proof, self.version, self.commit)

    def test_manifest_cannot_replace_expected_history(self):
        self.proof["manifest"]["expected"]["answer"] = "replacement"
        with self.assertRaisesRegex(ValueError, "expected observations"):
            verify.release_gate(self.proof, self.version, self.commit)

    def test_only_a_unique_well_formed_annotated_tag_pointer_is_accepted(self):
        self.assertEqual(verify.proof_run("Release\nhistory-proof-run: 987\n"), "987")
        for text in ("", "history-proof-run: 0", "history-proof-run: 9\nhistory-proof-run: 8"):
            with self.subTest(text=text), self.assertRaises(ValueError):
                verify.proof_run(text)
