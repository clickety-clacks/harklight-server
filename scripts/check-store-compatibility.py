#!/usr/bin/env python3
"""Check an existing Harklight database against a selected release's BUILD_INFO."""

import argparse
from contextlib import closing
from pathlib import Path
import re
import sqlite3
import sys


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
                raise ValueError("The FORMAT31 generation row is missing.")
            if package_format != 31:
                raise ValueError("The selected package does not declare FORMAT31 support.")
        elif keyed or "relay_state" not in tables:
            raise ValueError("The store layout is incomplete or unsupported.")
        elif not allow_conversion or package_format != 31:
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
