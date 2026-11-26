#!/usr/bin/env python3
"""Check an existing Harklight database against a selected release's BUILD_INFO."""

import argparse
from contextlib import closing
from pathlib import Path
import re
import sqlite3
import sys


def row_store_format(db):
    row = db.execute("SELECT payload FROM relay_state WHERE id = 1").fetchone()
    # The uncompressed Erlang anchor is a three-tuple whose first member is
    # the keyed-state atom and whose second is a small integer format number.
    atom = b"attention_relay_keyed_state"
    for atom_header in (b"d\x00", b"v\x00", b"s", b"w"):
        prefix = b"\x83h\x03" + atom_header + bytes([len(atom)]) + atom + b"a"
        if row and isinstance(row[0], bytes) and row[0].startswith(prefix) and len(row[0]) > len(prefix):
            return row[0][len(prefix)]
    raise ValueError("The row store has invalid format metadata.")


def package_support(build_info):
    values = {}
    for line in build_info.read_text(encoding="utf-8").splitlines():
        key, _, value = line.partition("=")
        if key in {"relay_store_format", "auth_generation"}:
            if key in values or not re.fullmatch(r"0|[1-9][0-9]*", value):
                raise ValueError(f"Invalid package declaration for {key}.")
            values[key] = int(value)
    return values.get("relay_store_format"), values.get("auth_generation", 0)


def check(store, build_info, allow_conversion=False):
    package_format, package_auth = package_support(build_info)
    with closing(sqlite3.connect(store.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        tables = {
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_schema WHERE type = 'table' AND name IN "
                "('relay_state', 'relay_records', 'relay_record_generation', 'harklight_auth_schema')"
            )
        }
        keyed = tables & {"relay_records", "relay_record_generation"}
        if keyed == {"relay_records", "relay_record_generation"}:
            if db.execute("SELECT generation FROM relay_record_generation WHERE id = 1").fetchone() is None:
                raise ValueError("The row store generation is missing.")
            store_format = row_store_format(db)
            if store_format not in (31, 32):
                raise ValueError("The row store format is unsupported.")
            if package_format != store_format and not (allow_conversion and (store_format, package_format) == (31, 32)):
                raise ValueError(f"The selected package does not declare FORMAT{store_format} support.")
        elif keyed or "relay_state" not in tables:
            raise ValueError("The store layout is incomplete or unsupported.")
        elif not allow_conversion or package_format not in (31, 32):
            raise ValueError("The store must be converted before this package starts.")

        if "harklight_auth_schema" not in tables:
            return

        rows = db.execute(
            "SELECT id, minimum_generation, device_certificates_disabled FROM harklight_auth_schema"
        ).fetchall()
        if (
            len(rows) != 1
            or rows[0][0] != 1
            or not isinstance(rows[0][1], int)
            or rows[0][1] < 1
            or rows[0][2] not in (0, 1)
        ):
            raise ValueError("The store has invalid auth compatibility metadata.")
        if package_auth < rows[0][1]:
            raise ValueError("The selected package does not support the store's auth generation.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("store", type=Path)
    parser.add_argument("build_info", type=Path)
    parser.add_argument("--allow-conversion", action="store_true")
    args = parser.parse_args()
    try:
        check(args.store, args.build_info, args.allow_conversion)
    except (OSError, ValueError, sqlite3.Error) as error:
        print(f"Harklight store compatibility check failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
