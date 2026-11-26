import importlib.util
from pathlib import Path
import sqlite3
import tempfile
import unittest

script = Path(__file__).resolve().parents[2] / "scripts" / "check-store-compatibility.py"
spec = importlib.util.spec_from_file_location("check_store_compatibility", script)
compatibility = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compatibility)


class StoreCompatibilityTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.store = Path(temp.name) / "relay.sqlite3"
        self.build_info = Path(temp.name) / "BUILD_INFO"
        self.build_info.write_text("relay_store_format=31\nauth_generation=1\n", encoding="utf-8")

    def create_keyed_store(self, store_format=31):
        atom = b"attention_relay_keyed_state"
        anchor = b"\x83h\x03d" + len(atom).to_bytes(2, "big") + atom
        anchor += b"a" + bytes([store_format]) + b"d\x00\x0frelay_record_v1"
        with sqlite3.connect(self.store) as db:
            db.executescript(
                """
                CREATE TABLE relay_records (key BLOB PRIMARY KEY);
                CREATE TABLE relay_record_generation (id INTEGER PRIMARY KEY, generation INTEGER);
                INSERT INTO relay_record_generation VALUES (1, 1);
                CREATE TABLE relay_state (id INTEGER PRIMARY KEY, payload BLOB);
                """
            )
            db.execute("INSERT INTO relay_state VALUES (1, ?)", (anchor,))

    def test_stopped_conversion_must_finish_before_launch(self):
        with sqlite3.connect(self.store) as db:
            db.execute("CREATE TABLE relay_state (id INTEGER PRIMARY KEY)")
        compatibility.check(self.store, self.build_info, allow_conversion=True)
        with self.assertRaisesRegex(ValueError, "must be converted"):
            compatibility.check(self.store, self.build_info)

    def test_compatible_package_accepts_auth_store_and_pre_auth_package_refuses_it(self):
        self.create_keyed_store()
        with sqlite3.connect(self.store) as db:
            db.executescript(
                """
                CREATE TABLE harklight_auth_schema (
                    id INTEGER PRIMARY KEY, minimum_generation INTEGER,
                    device_certificates_disabled INTEGER
                );
                INSERT INTO harklight_auth_schema VALUES (1, 1, 1);
                """
            )
        before = self.store.read_bytes()
        compatibility.check(self.store, self.build_info)
        self.build_info.write_text("relay_store_format=31\nauth_generation=0\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "does not support .* auth generation"):
            compatibility.check(self.store, self.build_info)
        self.assertEqual(self.store.read_bytes(), before)

    def test_package_must_declare_relay_format(self):
        self.create_keyed_store()
        self.build_info.write_text("auth_generation=1\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "does not declare FORMAT31"):
            compatibility.check(self.store, self.build_info)

    def test_format32_accepts_format31_only_for_stopped_conversion(self):
        self.create_keyed_store()
        self.build_info.write_text("relay_store_format=32\nauth_generation=1\n", encoding="utf-8")
        compatibility.check(self.store, self.build_info, allow_conversion=True)
        with self.assertRaisesRegex(ValueError, "does not declare FORMAT31"):
            compatibility.check(self.store, self.build_info)

    def test_format32_refuses_format31_package_even_with_conversion_allowed(self):
        self.create_keyed_store(32)
        with self.assertRaisesRegex(ValueError, "does not declare FORMAT32"):
            compatibility.check(self.store, self.build_info, allow_conversion=True)
        self.build_info.write_text("relay_store_format=32\nauth_generation=1\n", encoding="utf-8")
        compatibility.check(self.store, self.build_info)
