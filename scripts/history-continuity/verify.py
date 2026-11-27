#!/usr/bin/env python3
"""Verify published predecessor history and require all three release UI results."""

import argparse
import hashlib
import json
from pathlib import Path


PLATFORMS = {"iPhone", "iPad", "Web"}
EXPECTED = {
    "unanswered_item": "i1",
    "answered_item": "i2",
    "answer": "Synthetic answer",
    "discussion_item": "i3",
    "discussion_entry": "e1",
    "discussion": "Synthetic discussion",
}


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def require(condition, explanation):
    if not condition:
        raise ValueError(explanation)


def inventory(directory, version, commit):
    before = load(directory / "predecessor.json")
    after = load(directory / "target.json")
    require(before["stage"] == "0.1.49" and before["format"] == 29, "wrong predecessor package")
    require(after["stage"] == version, "wrong target package")
    require(before["items"] == after["items"], "predecessor message or history changed")
    projected = after["projections"]
    for inbox, detail in (
        ("inbox", "direct_item_reads"),
        ("browser_inbox", "browser_item_reads"),
    ):
        ranks = {item["item_id"]: item["rank"]["section"] for item in projected[inbox]["items"]}
        require(ranks == {"i1": "current", "i2": "resolved", "i3": "current"}, f"{inbox} differs")
        items = projected[detail]
        require(items["answered"]["item"]["item"]["terminal"]["answer"]["text"] == EXPECTED["answer"],
                f"{detail} lost the committed answer")
        chat = items["discussed"]["entries"]["entries"]
        require(len(chat) == 1 and chat[0]["entry_id"] == "e1" and chat[0]["text"] == EXPECTED["discussion"],
                f"{detail} lost the original Chat entry")

    baseline = (directory / "predecessor.json").read_bytes()
    manifest = {"fixture_sha256": hashlib.sha256(baseline).hexdigest(),
                "server_version": version, "server_commit": commit,
                "upgrade": ["0.1.49", "0.2.16", version], "expected": EXPECTED}
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def web_observations(observations):
    current = observations["current"]
    answered = observations["answered"]
    discussed = observations["discussed"]
    return {
        "unanswered_item": current["id"],
        "answered_item": answered["id"],
        "answer": answered["answer"],
        "discussion_item": discussed["id"],
        "discussion_entry": discussed["chat"]["entry_id"],
        "discussion": discussed["chat"]["text"],
    }


def gate(manifest, result_paths):
    seen = set()
    for path in result_paths:
        result = load(path)
        reported_platform = result.get("platform")
        platform = "Web" if reported_platform == "web" else reported_platform
        require(platform in PLATFORMS and platform not in seen, "invalid or repeated client platform")
        seen.add(platform)
        require(result.get("outcome") == "pass", f"{platform}: UI check did not pass")
        require(result.get("client_build"), f"{platform}: exact client build is missing")
        for field in ("fixture_sha256", "server_version", "server_commit"):
            require(result.get(field) == manifest[field], f"{platform}: {field} does not match")
        observations = result["observations"]
        if platform == "Web" and "current" in observations:
            observations = web_observations(observations)
        if platform != "Web":
            require(result.get("relaunch") == "pass", f"{platform}: cold relaunch did not pass")
        require(observations == manifest["expected"], f"{platform}: retained history differs")
    require(seen == PLATFORMS, "passing iPhone, iPad and Web results are all required")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    record = commands.add_parser("inventory")
    record.add_argument("directory", type=Path)
    record.add_argument("version")
    record.add_argument("commit")
    check = commands.add_parser("gate")
    check.add_argument("manifest", type=Path)
    check.add_argument("results", type=Path, nargs="+")
    args = parser.parse_args()
    try:
        if args.command == "inventory":
            inventory(args.directory, args.version, args.commit)
        else:
            gate(load(args.manifest), args.results)
    except (KeyError, TypeError, ValueError, OSError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
